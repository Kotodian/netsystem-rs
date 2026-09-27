use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::{TcpError, TcpInputFlags, TcpSegmentFlags, tcp_header};
use hammer_core::data_plane::{BufferPacketCursor, Frame};
use hammer_plugin_session::{IpSessionEndpoint, IpSessionMain, IpTransportConnectionId};
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, Node, NodeProcessFn, NodeRuntime, TraceFormatter,
    add_packet_trace, format_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::data_plane::set_index_node_error;
use hammer_service::session::{SessionLookup, SessionLookupResult};

use super::{TcpInputNext, write_session_route_opaque};
use crate::protocol::{TcpIpProtocol, TcpIpVersion};
use hammer_service::opaque::NetworkOpaque;

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TcpInputTrace {
    pub version: Option<TcpIpVersion>,
    pub protocol: Option<TcpIpProtocol>,
    pub source_port: Option<u16>,
    pub destination_port: Option<u16>,
    pub flags: u16,
    pub error: Option<u16>,
    pub next: u16,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    name = "tcp4-input",
    next = TcpInputNext,
    init = crate::register_tcp4_input,
    role = internal,
)]
pub struct Tcp4InputNode;

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    name = "tcp6-input",
    next = TcpInputNext,
    init = crate::register_tcp6_input,
    role = internal,
)]
pub struct Tcp6InputNode;

impl Node for Tcp4InputNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_input_process::<true>;
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(TcpInputTrace))
    }
}

impl Node for Tcp6InputNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_input_process::<false>;
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(TcpInputTrace))
    }
}

pub(crate) fn tcp_input_process<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    let handoff_worker = runtime.data_worker_id().ok();
    tcp_input_process_frame::<IS_IP4>(runtime, node_runtime, frame, handoff_worker);
    processed_vectors
}

fn tcp_input_process_frame<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
    handoff_worker: Option<DataWorkerId>,
) -> () {
    let mut output = Frame::<(), u32, ()>::new(0);
    let mut nexts = [0u16; hammer_core::graph::frame::FRAME_VECTOR_CAPACITY];
    for &index in frame.vector_args() {
        prefetch_tcp_input(runtime, &[index]);
        let next = tcp_input_local_next_for_index::<IS_IP4>(runtime, index, handoff_worker);
        let slot = match next {
            Ok(Some(slot)) => slot,
            Ok(None) => continue,
            Err(_) => TcpInputNext::Drop.slot() as u16,
        };
        let count = output.len();
        output.set_vector_count(count + 1);
        output.vector_args_mut()[count] = index;
        nexts[count] = slot;
    }
    let count = output.len();
    if count != 0 {
        runtime.enqueue_to_next(node_runtime, &mut output, &nexts[..count]);
    }
    ()
}

#[inline(always)]
fn tcp_input_local_next_for_index<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    index: u32,
    handoff_worker: Option<DataWorkerId>,
) -> RuntimeResult<Option<u16>> {
    let buffer = runtime.buffer(index);
    let parsed = tcp_input_buffer(&buffer)?;
    next_slot_for_index_with_runtime::<IS_IP4>(runtime, index, parsed, handoff_worker)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpInputError {
    BadLength,
    WrongProtocol {
        version: TcpIpVersion,
        protocol: TcpIpProtocol,
    },
}

#[inline(always)]
fn next_slot_for_index_with_runtime<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    index: u32,
    parsed: Result<
        (
            TcpIpVersion,
            TcpIpProtocol,
            SocketAddr,
            SocketAddr,
            TcpInputFlags,
        ),
        TcpInputError,
    >,
    handoff_worker: Option<DataWorkerId>,
) -> RuntimeResult<Option<u16>> {
    let traced = runtime.buffer(index).trace_handle().is_some();
    let (version, protocol, local, remote, flags) = match parsed {
        Ok(parsed) => parsed,
        Err(TcpInputError::BadLength) => {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::Length,
                None,
                None,
                0,
                traced,
            );
        }
        Err(TcpInputError::WrongProtocol { version, protocol }) => {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::Dispatch,
                Some(version),
                Some(protocol),
                0,
                traced,
            );
        }
    };
    let source_port = remote.port();
    let destination_port = local.port();
    if local.is_ipv4() != IS_IP4 {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Drop,
            TcpError::SegmentInvalid,
            Some(version),
            Some(protocol),
            u16::from(flags.bits()),
            traced,
        );
    }
    let fib_index = hammer_core::buffer_opaque!(runtime.buffer(index) => NetworkOpaque)
        .ip()
        .fib_index()
        .unwrap_or(0);
    let main = crate::TCP_MAIN
        .get()
        .expect("TCP input Node runs after TCP Main initialization");
    let ip_session = IpSessionMain::global()?;
    let connection = match (local, remote) {
        (SocketAddr::V4(local), SocketAddr::V4(remote)) => {
            IpTransportConnectionId::from((fib_index, local, remote, main.protocol()))
        }
        (SocketAddr::V6(local), SocketAddr::V6(remote)) => {
            IpTransportConnectionId::from((fib_index, local, remote, main.protocol()))
        }
        _ => return Ok(Some(TcpInputNext::Drop.slot() as u16)),
    };
    let exact_session = match ip_session.lookup_main().lookup_exact(&connection) {
        SessionLookupResult::Session(handle) => Some(handle),
        SessionLookupResult::HalfOpen(_)
        | SessionLookupResult::WrongThread
        | SessionLookupResult::NotFound => None,
    };
    if let (Some(session), Some(current_worker)) = (exact_session, handoff_worker)
        && session.worker_index != current_worker.slot() as u32
    {
        hammer_core::buffer_opaque!(mut runtime.buffer_mut(index) => NetworkOpaque)
            .set_handoff_source_worker(Some(current_worker.slot() as u16));
        let node = runtime
            .current_node()
            .ok_or(RuntimeError::NodeDispatchContextMissing)?;
        runtime.handoff_index(DataWorkerId::new(session.worker_index), node, index)?;
        return Ok(None);
    }

    let (session_route, listener_pending) =
        session_or_listener_pending_input_entry(runtime, local, remote, flags)?;
    if let Some((session_id, owner, session_next)) = session_route {
        if exact_session.is_some_and(|session| session.session_index != session_id) {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::InvalidConnection,
                Some(version),
                Some(protocol),
                u16::from(flags.bits()),
                traced,
            );
        }
        let slot = session_next.slot() as u16;
        {
            let buffer = runtime.buffer_mut(index);
            buffer.clear_node_error();
            write_session_route_opaque(
                hammer_core::buffer_opaque!(mut buffer => crate::TcpSecondaryOpaque).route_mut(),
                session_id,
                owner,
                session_next,
            );
            if let Some(current_worker) = handoff_worker
                && owner != current_worker
            {
                hammer_core::buffer_opaque!(mut buffer => NetworkOpaque)
                    .set_handoff_source_worker(Some(current_worker.slot() as u16));
            }
        }
        if let Some(current_worker) = handoff_worker
            && owner != current_worker
        {
            if traced {
                add_packet_trace!(
                    runtime,
                    index,
                    TcpInputTrace {
                        version: Some(version),
                        protocol: Some(protocol),
                        source_port: Some(source_port),
                        destination_port: Some(destination_port),
                        flags: u16::from(flags.bits()),
                        error: None,
                        next: slot,
                    },
                )?;
            }
            let node = runtime
                .current_node()
                .ok_or(RuntimeError::NodeDispatchContextMissing)?;
            let target = runtime.nodes().node_next(node, session_next)?;
            runtime.handoff_index(owner, target, index)?;
            return Ok(None);
        }
        return resolve_success_next_with_trace(
            runtime,
            index,
            session_next,
            version,
            protocol,
            source_port,
            destination_port,
            u16::from(flags.bits()),
            traced,
        );
    }

    if exact_session.is_some() {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Drop,
            TcpError::InvalidConnection,
            Some(version),
            Some(protocol),
            u16::from(flags.bits()),
            traced,
        );
    }

    if listener_pending {
        {
            let buffer = runtime.buffer_mut(index);
            buffer.clear_node_error();
            *hammer_core::buffer_opaque!(mut buffer => crate::TcpSecondaryOpaque).route_mut() =
                Default::default();
        }
        return resolve_success_next_with_trace(
            runtime,
            index,
            TcpInputNext::Listen,
            version,
            protocol,
            source_port,
            destination_port,
            u16::from(flags.bits()),
            traced,
        );
    }

    let mut transport = crate::tcp_endpoint_pair(local, remote).0;
    transport.local.fib_index = fib_index;
    let endpoint = IpSessionEndpoint::new(transport, main.protocol());
    let lookup = ip_session.lookup_listener(&endpoint, true);
    let (listener_next, listener_error) = tcp_listener_input_entry(flags);
    if let Some(error) = listener_error {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            listener_next,
            error,
            Some(version),
            Some(protocol),
            u16::from(flags.bits()),
            traced,
        );
    }
    let Some(_) = lookup else {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Punt,
            TcpError::ConnectionClosed,
            Some(version),
            Some(protocol),
            u16::from(flags.bits()),
            traced,
        );
    };
    {
        let buffer = runtime.buffer_mut(index);
        buffer.clear_node_error();
        *hammer_core::buffer_opaque!(mut buffer => crate::TcpSecondaryOpaque).route_mut() =
            Default::default();
    }
    resolve_success_next_with_trace(
        runtime,
        index,
        listener_next,
        version,
        protocol,
        source_port,
        destination_port,
        u16::from(flags.bits()),
        traced,
    )
}

#[inline(always)]
fn resolve_success_next_with_trace(
    runtime: &DataPlaneMain,
    index: u32,
    next_key: TcpInputNext,
    version: TcpIpVersion,
    protocol: TcpIpProtocol,
    source_port: u16,
    destination_port: u16,
    flags: u16,
    traced: bool,
) -> RuntimeResult<Option<u16>> {
    let slot = next_key.slot() as u16;
    if traced {
        add_packet_trace!(
            runtime,
            index,
            TcpInputTrace {
                version: Some(version),
                protocol: Some(protocol),
                source_port: Some(source_port),
                destination_port: Some(destination_port),
                flags,
                error: None,
                next: slot,
            },
        )?;
    }
    Ok(Some(slot))
}

#[inline(always)]
fn resolve_error_next_with_runtime(
    runtime: &mut DataPlaneMain,
    index: u32,
    next_key: TcpInputNext,
    error: TcpError,
    version: Option<TcpIpVersion>,
    protocol: Option<TcpIpProtocol>,
    flags: u16,
    traced: bool,
) -> RuntimeResult<Option<u16>> {
    set_index_node_error(runtime, index, error)?;
    let slot = next_key.slot() as u16;
    if traced {
        add_packet_trace!(
            runtime,
            index,
            TcpInputTrace {
                version,
                protocol,
                source_port: None,
                destination_port: None,
                flags,
                error: Some(error as u16),
                next: slot,
            },
        )?;
    }
    Ok(Some(slot))
}

#[inline(always)]
fn session_or_listener_pending_input_entry(
    runtime: &DataPlaneMain,
    local: SocketAddr,
    remote: SocketAddr,
    flags: TcpInputFlags,
) -> RuntimeResult<(Option<(u32, DataWorkerId, TcpInputNext)>, bool)> {
    // SAFETY: packet input executes on the DataPlaneMain's owning runtime thread.
    let mut worker = crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?
        .worker(runtime.thread_index())?;
    let (route, listener_pending) = worker.lookup.input_route(
        local,
        remote,
        flags.contains(TcpInputFlags::ACK) && !flags.contains(TcpInputFlags::RST),
    );
    Ok((route, listener_pending))
}

#[inline(always)]
fn tcp_listener_input_entry(flags: TcpInputFlags) -> (TcpInputNext, Option<TcpError>) {
    if flags == TcpInputFlags::SYN {
        return (TcpInputNext::Listen, None);
    }
    if flags.contains(TcpInputFlags::RST) {
        return (TcpInputNext::Drop, None);
    }
    if flags.contains(TcpInputFlags::ACK) {
        return (TcpInputNext::Reset, Some(TcpError::AckInvalid));
    }
    if flags.contains(TcpInputFlags::SYN) {
        return (TcpInputNext::Reset, Some(TcpError::AckInvalid));
    }
    (TcpInputNext::Reset, Some(TcpError::ConnectionClosed))
}

#[inline(always)]
fn tcp_input_buffer(
    buffer: &hammer_core::data_plane::Buffer,
) -> RuntimeResult<
    Result<
        (
            TcpIpVersion,
            TcpIpProtocol,
            SocketAddr,
            SocketAddr,
            TcpInputFlags,
        ),
        TcpInputError,
    >,
> {
    tcp_input_parts(
        buffer.current(),
        hammer_core::buffer_opaque!(buffer => NetworkOpaque),
    )
}

#[inline(always)]
fn prefetch_tcp_input(runtime: &DataPlaneMain, indices: &[u32]) {
    let mut read = 0usize;
    while read < indices.len() {
        let index = indices[read];
        runtime.prefetch_read(index);
        let buffer = runtime.buffer(index);
        prefetch_session_route_for_buffer(runtime, buffer);
        read += 1;
    }
}

#[inline(always)]
fn tcp_input_parts(
    current: &[u8],
    network: &NetworkOpaque,
) -> RuntimeResult<
    Result<
        (
            TcpIpVersion,
            TcpIpProtocol,
            SocketAddr,
            SocketAddr,
            TcpInputFlags,
        ),
        TcpInputError,
    >,
> {
    let cursor = network.packet_cursor();
    let Some((version, protocol)) = ip_facts(network) else {
        return Ok(Err(TcpInputError::BadLength));
    };
    if protocol != TcpIpProtocol::Tcp {
        return Ok(Err(TcpInputError::WrongProtocol { version, protocol }));
    }
    if !valid_tcp_cursor(cursor) {
        return Ok(Err(TcpInputError::BadLength));
    }
    let first_len = current.len().min(cursor.packet_len());
    let Some(packet) = current.get(..first_len) else {
        return Ok(Err(TcpInputError::BadLength));
    };
    let Some(network_header) = packet
        .get(cursor.network_header_offset()..cursor.transport_header_offset().min(packet.len()))
    else {
        return Ok(Err(TcpInputError::BadLength));
    };
    let source_ip = source_ip(version, network_header)?;
    let destination_ip = destination_ip(version, network_header)?;
    let Some(transport) = packet.get(cursor.transport_header_offset()..first_len) else {
        return Ok(Err(TcpInputError::BadLength));
    };
    let segment = match tcp_header(transport) {
        Ok(segment) => segment,
        Err(_) => return Ok(Err(TcpInputError::BadLength)),
    };
    Ok(Ok((
        version,
        protocol,
        SocketAddr::new(destination_ip, segment.destination_port()),
        SocketAddr::new(source_ip, segment.source_port()),
        tcp_input_flags(segment.flags()),
    )))
}

#[inline(always)]
fn valid_tcp_cursor(cursor: BufferPacketCursor) -> bool {
    cursor.packet_len() >= cursor.transport_header_offset()
}

#[inline(always)]
fn ip_facts(network: &NetworkOpaque) -> Option<(TcpIpVersion, TcpIpProtocol)> {
    let version = match network.ip().ip_version()? {
        4 => TcpIpVersion::V4,
        6 => TcpIpVersion::V6,
        _ => return None,
    };
    Some((version, TcpIpProtocol::from(network.ip().ip_protocol()?)))
}

#[inline(always)]
fn tcp_input_flags(flags: TcpSegmentFlags) -> TcpInputFlags {
    let mut parsed = TcpInputFlags::empty();
    if flags.contains(TcpSegmentFlags::FIN) {
        parsed |= TcpInputFlags::FIN;
    }
    if flags.contains(TcpSegmentFlags::SYN) {
        parsed |= TcpInputFlags::SYN;
    }
    if flags.contains(TcpSegmentFlags::RST) {
        parsed |= TcpInputFlags::RST;
    }
    if flags.contains(TcpSegmentFlags::ACK) {
        parsed |= TcpInputFlags::ACK;
    }
    parsed
}

#[inline(always)]
fn source_ip(version: TcpIpVersion, packet: &[u8]) -> RuntimeResult<IpAddr> {
    match version {
        TcpIpVersion::V4 => {
            let Some(source) = packet.get(12..16) else {
                return Err(TcpError::Length.into());
            };
            Ok(Ipv4Addr::new(source[0], source[1], source[2], source[3]).into())
        }
        TcpIpVersion::V6 => {
            let Some(source) = packet.get(8..24) else {
                return Err(TcpError::Length.into());
            };
            let bytes: [u8; 16] = source.try_into().map_err(|_| TcpError::Length)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline(always)]
fn destination_ip(version: TcpIpVersion, packet: &[u8]) -> RuntimeResult<IpAddr> {
    match version {
        TcpIpVersion::V4 => {
            let Some(destination) = packet.get(16..20) else {
                return Err(TcpError::Length.into());
            };
            Ok(Ipv4Addr::new(
                destination[0],
                destination[1],
                destination[2],
                destination[3],
            )
            .into())
        }
        TcpIpVersion::V6 => {
            let Some(destination) = packet.get(24..40) else {
                return Err(TcpError::Length.into());
            };
            let bytes: [u8; 16] = destination.try_into().map_err(|_| TcpError::Length)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline(always)]
fn prefetch_session_route_for_buffer(
    runtime: &DataPlaneMain,
    buffer: &hammer_core::data_plane::Buffer,
) {
    let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
    let cursor = network.packet_cursor();
    if !valid_tcp_cursor(cursor) {
        return;
    }
    let current = buffer.current();
    let packet_len = cursor.packet_len().min(current.len());
    let Some(packet) = current.get(..packet_len) else {
        return;
    };
    let Some((version, _)) = ip_facts(network) else {
        return;
    };
    let Some(network_header) = packet
        .get(cursor.network_header_offset()..cursor.transport_header_offset().min(packet.len()))
    else {
        return;
    };
    let (source_ip, destination_ip) = match (
        source_ip(version, network_header),
        destination_ip(version, network_header),
    ) {
        (Ok(source_ip), Ok(destination_ip)) => (source_ip, destination_ip),
        _ => return,
    };
    let local = SocketAddr::new(destination_ip, tcp_destination_port(buffer));
    let remote = SocketAddr::new(source_ip, tcp_source_port(buffer));
    let Some(main) = crate::TCP_MAIN.get() else {
        return;
    };
    if let Ok(worker) = main.worker(runtime.thread_index()) {
        worker.lookup.prefetch_tuple(local, remote);
    }
}

#[inline(always)]
fn tcp_source_port(buffer: &hammer_core::data_plane::Buffer) -> u16 {
    let transport = hammer_core::buffer_opaque!(buffer => NetworkOpaque)
        .packet_cursor()
        .transport_header_offset();
    let current = buffer.current();
    current
        .get(transport..transport + 2)
        .map(|port| u16::from_be_bytes([port[0], port[1]]))
        .unwrap_or(0)
}

#[inline(always)]
fn tcp_destination_port(buffer: &hammer_core::data_plane::Buffer) -> u16 {
    let transport = hammer_core::buffer_opaque!(buffer => NetworkOpaque)
        .packet_cursor()
        .transport_header_offset();
    let current = buffer.current();
    current
        .get(transport + 2..transport + 4)
        .map(|port| u16::from_be_bytes([port[0], port[1]]))
        .unwrap_or(0)
}

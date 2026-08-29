use std::cell::RefCell;
use std::mem::{size_of, transmute};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroU64;
use std::sync::Arc;

use crate::{TcpError, TcpInputFlags, TcpSegmentFlags, tcp_header};
use arc_swap::ArcSwap;
use hammer_core::data_plane::{
    BufferFrame, BufferPacketCursor, Index, NodeHandle, SecondaryOpaque,
};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
    TraceFormatter, add_packet_trace, format_packet_trace,
};
use hammer_runtime::{RuntimeError, RuntimeResult};
use hammer_service::data_plane::set_buffer_node_error;

use super::lookup::{
    TcpIpv4ListenerAddress, TcpIpv6ListenerAddress, TcpLookupSnapshot, TcpLookupValue,
    TcpV4ListenerKey, TcpV6ListenerKey,
};
use super::{TcpInputNext, write_session_route_opaque};
use crate::protocol::{TcpIpProtocol, TcpIpVersion};
use hammer_service::opaque::NetworkOpaque;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct IcmpErrorOpaque {
    icmp_error: Option<NonZeroU64>,
    reserved: [u64; 6],
}

const _: () = assert!(size_of::<IcmpErrorOpaque>() == size_of::<SecondaryOpaque>());

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

#[derive(Clone)]
pub struct TcpInputControlPlane {
    inner: Arc<ArcSwap<TcpLookupSnapshot>>,
}

impl TcpInputControlPlane {
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpLookupSnapshot::default())),
        }
    }

    #[inline]
    pub fn publish_lookup(&self, lookup: TcpLookupSnapshot) -> RuntimeResult<()> {
        self.inner.store(Arc::new(lookup));
        Ok(())
    }

    #[inline]
    pub(crate) fn lookup_listener(&self, local: SocketAddr) -> Option<TcpLookupValue> {
        let snapshot = self.inner.load();
        match local.ip() {
            IpAddr::V4(local_addr) => snapshot.lookup_listener::<TcpIpv4ListenerAddress>(
                TcpV4ListenerKey::new(0, local_addr, local.port()),
            ),
            IpAddr::V6(local_addr) => snapshot.lookup_listener::<TcpIpv6ListenerAddress>(
                TcpV6ListenerKey::new(0, local_addr, local.port()),
            ),
        }
    }

    #[inline]
    pub(crate) fn node(
        &self,
        process: NodeProcessFn,
        handoff: Option<(NodeHandle, DataWorkerId)>,
    ) -> TcpInputNode {
        let mut node =
            TcpInputNode::new(register_tcp_input_runtime(Arc::clone(&self.inner)), process);
        if let Some((handoff, worker)) = handoff {
            node.handoff = Some(handoff);
            node.handoff_worker = Some(worker);
        }
        node
    }
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    name = "tcp-input",
    next = TcpInputNext,
    init = crate::register_tcp_input,
    role = internal,
)]
pub struct TcpInputNode {
    runtime_data: NodeRuntimeData,
    process: NodeProcessFn,
    #[node(default)]
    handoff: Option<NodeHandle>,
    #[node(default)]
    handoff_worker: Option<DataWorkerId>,
}

impl Node for TcpInputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        if sync_tcp_input_runtime(self.runtime_data, self.handoff, self.handoff_worker).is_err() {
            return NodeResult::drop();
        }
        (self.process)(runtime, self.runtime_data, frame)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_packet_trace!(TcpInputTrace))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        self.process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        sync_tcp_input_runtime(self.runtime_data, self.handoff, self.handoff_worker)?;
        Ok(self.runtime_data)
    }
}

#[derive(Clone)]
struct TcpInputRuntime {
    snapshot: Arc<ArcSwap<TcpLookupSnapshot>>,
    handoff: Option<NodeHandle>,
    handoff_worker: Option<DataWorkerId>,
}

thread_local! {
    static TCP_INPUT_RUNTIMES: RefCell<Vec<TcpInputRuntime>> =
        const { RefCell::new(Vec::new()) };
}

fn register_tcp_input_runtime(snapshot: Arc<ArcSwap<TcpLookupSnapshot>>) -> NodeRuntimeData {
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpInputRuntime {
            snapshot,
            handoff: None,
            handoff_worker: None,
        });
        NodeRuntimeData::from_usize(slot).expect("TCP input runtime slot overflow")
    })
}

#[hammer_component_macros::runtime_error(subsystem = "tcp")]
#[derive(Debug, thiserror::Error)]
#[error("TCP input runtime slot {slot} is not registered")]
struct TcpInputSlotInvalid {
    slot: usize,
}

fn tcp_input_runtime(data: NodeRuntimeData) -> RuntimeResult<TcpInputRuntime> {
    let slot = data.usize_word(0)?;
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| RuntimeError::from(TcpInputSlotInvalid { slot }))
    })
}

fn sync_tcp_input_runtime(
    data: NodeRuntimeData,
    handoff: Option<NodeHandle>,
    handoff_worker: Option<DataWorkerId>,
) -> RuntimeResult<()> {
    let slot = data.usize_word(0)?;
    TCP_INPUT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| RuntimeError::from(TcpInputSlotInvalid { slot }))?;
        runtime.handoff = handoff;
        runtime.handoff_worker = handoff_worker;
        Ok(())
    })
}

pub(crate) fn tcp_input_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let state = match tcp_input_runtime(data) {
        Ok(state) => state,
        Err(_) => return NodeResult::drop(),
    };
    let snapshot = state.snapshot.load();
    tcp_input_process_frame(
        runtime,
        frame,
        &snapshot,
        state.handoff,
        state.handoff_worker,
    )
}

fn tcp_input_process_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    snapshot: &TcpLookupSnapshot,
    handoff: Option<NodeHandle>,
    handoff_worker: Option<DataWorkerId>,
) -> NodeResult {
    let width = runtime.preferred_frame_batch_width();
    let mut nexts = Vec::with_capacity(frame.len());
    let _ = frame.rewrite_indices_batched(width, |index| {
        prefetch_tcp_input(runtime, &[index], snapshot);
        match tcp_input_local_next_for_index(runtime, index, snapshot, handoff, handoff_worker) {
            Ok(Some(slot)) => {
                nexts.push(slot);
                Ok(Some(index))
            }
            Ok(None) => Ok(None),
            Err(_) => {
                nexts.push(TcpInputNext::Drop.slot() as u16);
                Ok(Some(index))
            }
        }
    });
    if !nexts.is_empty() {
        runtime.enqueue_to_next(frame, nexts.as_slice());
    }
    NodeResult::drop()
}

#[inline(always)]
fn tcp_input_local_next_for_index(
    runtime: &DataPlaneRuntime,
    index: Index,
    snapshot: &TcpLookupSnapshot,
    handoff: Option<NodeHandle>,
    handoff_worker: Option<DataWorkerId>,
) -> RuntimeResult<Option<u16>> {
    let buffer = runtime.get_buffer(index)?;
    let parsed = tcp_input_buffer(&buffer)?;
    drop(buffer);
    next_slot_for_index_with_runtime(runtime, index, parsed, snapshot, handoff, handoff_worker)
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
fn next_slot_for_index_with_runtime(
    runtime: &DataPlaneRuntime,
    index: Index,
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
    snapshot: &TcpLookupSnapshot,
    handoff: Option<NodeHandle>,
    handoff_worker: Option<DataWorkerId>,
) -> RuntimeResult<Option<u16>> {
    let traced = runtime.get_buffer(index)?.trace_handle().is_some();
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

    let (session_route, listener_pending) =
        session_or_listener_pending_input_entry(runtime, local, remote, flags)?;
    if let Some((session_id, owner, session_next)) = session_route {
        let slot = session_next.slot() as u16;
        {
            let mut buffer = runtime.get_buffer_mut(index)?;
            buffer.clear_node_error();
            write_session_route_opaque(buffer.opaque2_mut(), session_id, owner, session_next);
            if let (Some(_), Some(current_worker)) = (handoff, handoff_worker)
                && owner != current_worker
            {
                unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }
                    .set_handoff_source_worker(Some(current_worker.slot() as u16));
            }
        }
        if let (Some(target), Some(current_worker)) = (handoff, handoff_worker)
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
            runtime.handoff_index(owner, target, index, Some(session_next))?;
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

    if listener_pending {
        {
            let mut buffer = runtime.get_buffer_mut(index)?;
            buffer.clear_node_error();
            buffer.opaque2_mut().clear();
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

    let lookup = lookup_for_packet(snapshot, local, remote);
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
    let Some(_listener) = lookup else {
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
        let mut buffer = runtime.get_buffer_mut(index)?;
        buffer.clear_node_error();
        buffer.opaque2_mut().clear();
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
    runtime: &DataPlaneRuntime,
    index: Index,
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
    runtime: &DataPlaneRuntime,
    index: Index,
    next_key: TcpInputNext,
    error: TcpError,
    version: Option<TcpIpVersion>,
    protocol: Option<TcpIpProtocol>,
    flags: u16,
    traced: bool,
) -> RuntimeResult<Option<u16>> {
    {
        let mut buffer = runtime.get_buffer_mut(index)?;
        set_buffer_node_error(runtime, &mut buffer, error)?;
    }
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
    runtime: &DataPlaneRuntime,
    local: SocketAddr,
    remote: SocketAddr,
    flags: TcpInputFlags,
) -> RuntimeResult<(Option<(u32, DataWorkerId, TcpInputNext)>, bool)> {
    crate::TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?
        .with_worker(runtime, |_, worker| {
            let (route, listener_pending) = worker.lookup.input_route(
                local,
                remote,
                flags.contains(TcpInputFlags::ACK) && !flags.contains(TcpInputFlags::RST),
            );
            Ok((route, listener_pending))
        })
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
    tcp_input_parts(buffer.current(), unsafe {
        transmute::<_, &NetworkOpaque>(buffer.opaque())
    })
}

#[inline(always)]
fn prefetch_tcp_input(runtime: &DataPlaneRuntime, indices: &[Index], lookup: &TcpLookupSnapshot) {
    let mut read = 0usize;
    while read < indices.len() {
        let index = indices[read];
        runtime.prefetch_read(index);
        if let Ok(buffer) = runtime.get_buffer(index) {
            prefetch_lookup_for_buffer(lookup, &buffer);
            prefetch_session_route_for_buffer(runtime, &buffer);
        }
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
fn lookup_for_packet(
    snapshot: &TcpLookupSnapshot,
    local: SocketAddr,
    remote: SocketAddr,
) -> Option<TcpLookupValue> {
    match (local.ip(), remote.ip()) {
        (IpAddr::V4(local_addr), IpAddr::V4(_)) => snapshot
            .lookup_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                0,
                local_addr,
                local.port(),
            )),
        (IpAddr::V6(local_addr), IpAddr::V6(_)) => snapshot
            .lookup_listener::<TcpIpv6ListenerAddress>(TcpV6ListenerKey::new(
                0,
                local_addr,
                local.port(),
            )),
        _ => None,
    }
}

#[inline(always)]
fn prefetch_lookup_for_buffer(
    snapshot: &TcpLookupSnapshot,
    buffer: &hammer_core::data_plane::Buffer,
) {
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
    let cursor = network.packet_cursor();
    if !valid_tcp_cursor(cursor) {
        return;
    }
    let current = buffer.current();
    let packet_len = cursor.packet_len().min(current.len());
    let Some(packet) = current.get(..packet_len) else {
        return;
    };
    let destination_port = tcp_destination_port(buffer);
    let Some((version, _)) = ip_facts(network) else {
        return;
    };
    let Some(network_header) = packet
        .get(cursor.network_header_offset()..cursor.transport_header_offset().min(packet.len()))
    else {
        return;
    };
    match version {
        TcpIpVersion::V4 if network_header.len() >= 20 => {
            let local_addr = Ipv4Addr::new(
                network_header[16],
                network_header[17],
                network_header[18],
                network_header[19],
            );
            snapshot.prefetch_listener::<TcpIpv4ListenerAddress>(TcpV4ListenerKey::new(
                0,
                local_addr,
                destination_port,
            ));
        }
        TcpIpVersion::V6 if network_header.len() >= 40 => {
            let local_addr = Ipv6Addr::from([
                network_header[24],
                network_header[25],
                network_header[26],
                network_header[27],
                network_header[28],
                network_header[29],
                network_header[30],
                network_header[31],
                network_header[32],
                network_header[33],
                network_header[34],
                network_header[35],
                network_header[36],
                network_header[37],
                network_header[38],
                network_header[39],
            ]);
            snapshot.prefetch_listener::<TcpIpv6ListenerAddress>(TcpV6ListenerKey::new(
                0,
                local_addr,
                destination_port,
            ));
        }
        _ => {}
    }
}

#[inline(always)]
fn prefetch_session_route_for_buffer(
    runtime: &DataPlaneRuntime,
    buffer: &hammer_core::data_plane::Buffer,
) {
    let network = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) };
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
    let _ = main.with_worker(runtime, |_, worker| {
        worker.lookup.prefetch_tuple(local, remote);
        Ok(())
    });
}

#[inline(always)]
fn tcp_source_port(buffer: &hammer_core::data_plane::Buffer) -> u16 {
    let transport = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) }
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
    let transport = unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) }
        .packet_cursor()
        .transport_header_offset();
    let current = buffer.current();
    current
        .get(transport + 2..transport + 4)
        .map(|port| u16::from_be_bytes([port[0], port[1]]))
        .unwrap_or(0)
}

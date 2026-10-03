use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::{TcpError, TcpInputFlags, TcpSegmentFlags, TcpState, tcp_header};
use hammer_core::data_plane::{BufferPacketCursor, Frame, NodeNext};
use hammer_plugin_ip::protocol::ip::{IpProtocol, IpVersion};
use hammer_plugin_session::{IpSessionEndpoint, IpSessionMain, IpTransportConnectionId};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime, TraceFormatter};
use hammer_service::opaque::NetworkOpaque;
use hammer_service::session::{SessionLookup, SessionLookupResult, SessionMain};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::{TcpInputNext, read_session_route_opaque, write_session_route_opaque};

// VPP tcp_input.c:1213-1268,1272-1303,1590-1607,1967-2005,2410-2428.
// Input and state nodes retain the received TCP header and connection state.
#[repr(C)]
#[derive(KnownLayout, FromBytes, IntoBytes, Immutable)]
pub(crate) struct TcpReceiveTrace {
    pub(crate) tcp_header: [u8; 20],
    local_ip: [u8; 16],
    remote_ip: [u8; 16],
    pub(crate) connection_index: u32,
    worker_index: u32,
    local_port: u16,
    remote_port: u16,
    pub(crate) state: u8,
    family: u8,
    pub(crate) has_connection: u8,
    pub(crate) header_len: u8,
}

impl TcpReceiveTrace {
    pub(crate) fn record_connection(
        &mut self,
        connection: Option<&crate::connection::TcpConnection>,
    ) {
        let Some(connection) = connection else {
            return;
        };
        self.connection_index = connection.base.connection_index;
        self.worker_index = connection.base.worker_index;
        self.state = connection.state() as u8;
        self.has_connection = 1;
        match connection.base.endpoint {
            IpTransportConnectionId::Ip4 {
                local_address,
                remote_address,
                local_port,
                remote_port,
                ..
            } => {
                self.family = 4;
                self.local_ip[..4].copy_from_slice(&local_address.octets());
                self.remote_ip[..4].copy_from_slice(&remote_address.octets());
                self.local_port = u16::from_be(local_port);
                self.remote_port = u16::from_be(remote_port);
            }
            IpTransportConnectionId::Ip6 {
                local_address,
                remote_address,
                local_port,
                remote_port,
                ..
            } => {
                self.family = 6;
                self.local_ip = local_address.octets();
                self.remote_ip = remote_address.octets();
                self.local_port = u16::from_be(local_port);
                self.remote_port = u16::from_be(remote_port);
            }
        }
    }
}

pub(crate) fn format_tcp_receive_trace(bytes: &[u8]) -> String {
    let (trace, _) = TcpReceiveTrace::ref_from_prefix(bytes)
        .expect("TCP receive trace has its registered layout");
    if usize::from(trace.header_len) < core::mem::size_of::<crate::TcpHeader>() {
        return format!("tcp input truncated header ({} bytes)", trace.header_len);
    }
    let (header, _) = crate::TcpHeader::ref_from_prefix(&trace.tcp_header)
        .expect("TCP receive trace has a full TCP header");
    let states = [
        TcpState::Closed,
        TcpState::Listen,
        TcpState::SynSent,
        TcpState::SynRcvd,
        TcpState::Established,
        TcpState::FinWait1,
        TcpState::FinWait2,
        TcpState::CloseWait,
        TcpState::Closing,
        TcpState::LastAck,
        TcpState::TimeWait,
    ];
    let state = (trace.has_connection != 0).then(|| {
        *states
            .get(usize::from(trace.state))
            .expect("TCP receive trace records a valid connection state")
    });
    let connection = match (trace.has_connection, trace.family) {
        (0, _) => "no tcp connection".to_owned(),
        (_, 4) => format!(
            "[{}:{}][T] {}:{}->{}:{} state {:?}",
            trace.worker_index,
            trace.connection_index,
            Ipv4Addr::new(
                trace.local_ip[0],
                trace.local_ip[1],
                trace.local_ip[2],
                trace.local_ip[3]
            ),
            trace.local_port,
            Ipv4Addr::new(
                trace.remote_ip[0],
                trace.remote_ip[1],
                trace.remote_ip[2],
                trace.remote_ip[3]
            ),
            trace.remote_port,
            state.expect("connected TCP trace has a state"),
        ),
        (_, 6) => format!(
            "[{}:{}][T] {}:{}->{}:{} state {:?}",
            trace.worker_index,
            trace.connection_index,
            Ipv6Addr::from(trace.local_ip),
            trace.local_port,
            Ipv6Addr::from(trace.remote_ip),
            trace.remote_port,
            state.expect("connected TCP trace has a state"),
        ),
        _ => unreachable!("connected TCP trace has an IP family"),
    };
    format!(
        "{connection}\n  tcp {} -> {} seq {} ack {} flags {:?}",
        header.source_port(),
        header.destination_port(),
        header.sequence_number(),
        header.acknowledgment_number(),
        header.flags(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TcpInputDispatch {
    next: TcpInputNext,
    error: Option<TcpError>,
}

impl NodeNext for TcpInputDispatch {
    #[inline(always)]
    fn slot(self) -> u16 {
        u16::try_from(self.next.slot()).expect("TCP input next slot fits u16")
    }
}

// VPP tcp_input.c:3006-3235: unlisted state/flag pairs are dispatch errors.
// TCP_FLAG_{FIN,SYN,RST,ACK} are the only flags used to index the 64 columns.
const TCP_INPUT_DISPATCH: [[TcpInputDispatch; 64]; TcpState::COUNT] = {
    const DISPATCH: TcpInputDispatch = TcpInputDispatch {
        next: TcpInputNext::Drop,
        error: Some(TcpError::Dispatch),
    };
    const RECEIVE: TcpInputDispatch = TcpInputDispatch {
        next: TcpInputNext::RcvProcess,
        error: None,
    };
    const ESTABLISHED: TcpInputDispatch = TcpInputDispatch {
        next: TcpInputNext::Established,
        error: None,
    };
    const INVALID: TcpInputDispatch = TcpInputDispatch {
        next: TcpInputNext::Drop,
        error: Some(TcpError::SegmentInvalid),
    };
    const ACK_INVALID: TcpInputDispatch = TcpInputDispatch {
        next: TcpInputNext::Reset,
        error: Some(TcpError::AckInvalid),
    };
    const CLOSED: TcpInputDispatch = TcpInputDispatch {
        next: TcpInputNext::Drop,
        error: Some(TcpError::ConnectionClosed),
    };
    const CONTROL_MASK: usize = 0x17;
    const FIN: usize = 0x01;
    const SYN: usize = 0x02;
    const RST: usize = 0x04;
    const ACK: usize = 0x10;

    let mut table = [[DISPATCH; 64]; TcpState::COUNT];
    let mut flags = 1;
    while flags < 64 {
        if flags & !CONTROL_MASK == 0 {
            table[TcpState::Listen.index()][flags] = INVALID;
            table[TcpState::SynRcvd.index()][flags] = RECEIVE;
            table[TcpState::Established.index()][flags] = ESTABLISHED;
            table[TcpState::FinWait1.index()][flags] = RECEIVE;
            table[TcpState::Closing.index()][flags] = RECEIVE;
            table[TcpState::LastAck.index()][flags] = RECEIVE;
        }
        flags += 1;
    }
    table[TcpState::Listen.index()][0] = INVALID;
    table[TcpState::Listen.index()][ACK] = ACK_INVALID;
    table[TcpState::Listen.index()][SYN | ACK] = ACK_INVALID;
    table[TcpState::Listen.index()][FIN] = TcpInputDispatch {
        next: TcpInputNext::Reset,
        error: Some(TcpError::SegmentInvalid),
    };
    table[TcpState::Listen.index()][FIN | ACK] = table[TcpState::Listen.index()][FIN];
    table[TcpState::Listen.index()][RST] = TcpInputDispatch {
        next: TcpInputNext::Drop,
        error: Some(TcpError::InvalidConnection),
    };
    table[TcpState::Listen.index()][RST | ACK] = table[TcpState::Listen.index()][RST];
    table[TcpState::Listen.index()][SYN] = TcpInputDispatch {
        next: TcpInputNext::Listen,
        error: None,
    };
    table[TcpState::SynRcvd.index()][0] = INVALID;
    let syn_sent_flags = [SYN | ACK, ACK, RST, RST | ACK, FIN, FIN | ACK];
    let mut index = 0;
    while index < syn_sent_flags.len() {
        table[TcpState::SynSent.index()][syn_sent_flags[index]] = TcpInputDispatch {
            next: TcpInputNext::SynSent,
            error: None,
        };
        index += 1;
    }
    table[TcpState::Established.index()][0] = INVALID;
    table[TcpState::FinWait1.index()][0] = INVALID;
    table[TcpState::Closing.index()][0] = INVALID;
    table[TcpState::LastAck.index()][0] = INVALID;
    let fin_wait2_flags = [FIN, ACK, FIN | ACK, RST, RST | ACK, SYN, SYN | ACK];
    let mut index = 0;
    while index < fin_wait2_flags.len() {
        table[TcpState::FinWait2.index()][fin_wait2_flags[index]] = RECEIVE;
        index += 1;
    }
    let close_wait_flags = [ACK, FIN | ACK, RST, RST | ACK, SYN];
    let mut index = 0;
    while index < close_wait_flags.len() {
        table[TcpState::CloseWait.index()][close_wait_flags[index]] = RECEIVE;
        index += 1;
    }
    table[TcpState::TimeWait.index()][SYN] = table[TcpState::Listen.index()][SYN];
    let time_wait_flags = [FIN, FIN | ACK, RST, RST | ACK, ACK];
    let mut index = 0;
    while index < time_wait_flags.len() {
        table[TcpState::TimeWait.index()][time_wait_flags[index]] = RECEIVE;
        index += 1;
    }
    table[TcpState::Closed.index()][RST] = CLOSED;
    table[TcpState::Closed.index()][RST | ACK] = CLOSED;
    table[TcpState::Closed.index()][ACK] = TcpInputDispatch {
        next: TcpInputNext::Reset,
        error: Some(TcpError::ConnectionClosed),
    };
    table[TcpState::Closed.index()][FIN | ACK] = table[TcpState::Closed.index()][ACK];
    table[TcpState::Closed.index()][SYN] = table[TcpState::Listen.index()][SYN];
    table
};

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

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    name = "tcp4-input-nolookup",
    next = TcpInputNext,
    init = crate::register_tcp4_input_nolookup,
    role = internal,
)]
pub struct Tcp4InputNoLookupNode;

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    name = "tcp6-input-nolookup",
    next = TcpInputNext,
    init = crate::register_tcp6_input_nolookup,
    role = internal,
)]
pub struct Tcp6InputNoLookupNode;

impl Node for Tcp4InputNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_input_process::<true, false>;
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_receive_trace)
    }
}

impl Node for Tcp6InputNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_input_process::<false, false>;
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_receive_trace)
    }
}

impl Node for Tcp4InputNoLookupNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_input_process::<true, true>;
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_receive_trace)
    }
}

impl Node for Tcp6InputNoLookupNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_input_process::<false, true>;
        process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_receive_trace)
    }
}

pub(crate) fn tcp_input_process<const IS_IP4: bool, const NO_LOOKUP: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    // VPP tcp_input.c:2789 and tcp_inlines.h:262-278: input refreshes the
    // worker clock before any packet lookup; Session Queue advances timers.
    let now = SessionMain::global()
        .expect("Session Main initializes before TCP input")
        .now();
    let tcp = crate::TCP_MAIN
        .get()
        .expect("TCP Main initializes before TCP input");
    {
        let mut worker = tcp
            .worker(runtime.thread_index())
            .expect("TCP input executes on its owner Data Worker");
        worker.time_us = now;
        worker.time_tstamp = ((now * 1_000.0) as u64) as u32;
    }
    let processed_vectors = frame.len();
    tcp_input_process_frame::<IS_IP4, NO_LOOKUP>(runtime, node_runtime, frame);
    processed_vectors
}

fn tcp_input_process_frame<const IS_IP4: bool, const NO_LOOKUP: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
) -> () {
    let mut output = Frame::<(), u32, ()>::new(0);
    let mut nexts = [0u16; hammer_core::graph::frame::FRAME_VECTOR_CAPACITY];
    let mut error_counts = [0u16; crate::TCP_ERRORS.len()];
    let mut error_codes = [None; crate::TCP_ERRORS.len()];
    let mut error_mask = 0u64;
    let indices = frame.vector_args();
    let mut position = 0;
    while indices.len() - position >= 4 {
        // VPP tcp_input.c:2797-2855 prefetches the next pair before lookup.
        let ahead0 = indices[position + 2];
        let ahead1 = indices[position + 3];
        prefetch_tcp_input(runtime, ahead0);
        prefetch_tcp_input(runtime, ahead1);

        let index0 = indices[position];
        let mut error0 = None;
        let next0 =
            tcp_input_local_next_for_index::<IS_IP4, NO_LOOKUP>(runtime, index0, &mut error0);
        let slot0 = match next0 {
            Ok(slot) => slot,
            Err(_) => Some(TcpInputNext::Drop.slot() as u16),
        };
        if let Some(slot) = slot0 {
            let count = output.len();
            output.set_vector_count(count + 1);
            output.vector_args_mut()[count] = index0;
            nexts[count] = slot;
        }
        if let Some(error) = error0 {
            let code = error as usize;
            error_counts[code] += 1;
            error_codes[code] = Some(error);
            error_mask |= 1u64 << code;
        }

        let index1 = indices[position + 1];
        let mut error1 = None;
        let next1 =
            tcp_input_local_next_for_index::<IS_IP4, NO_LOOKUP>(runtime, index1, &mut error1);
        let slot1 = match next1 {
            Ok(slot) => slot,
            Err(_) => Some(TcpInputNext::Drop.slot() as u16),
        };
        if let Some(slot) = slot1 {
            let count = output.len();
            output.set_vector_count(count + 1);
            output.vector_args_mut()[count] = index1;
            nexts[count] = slot;
        }
        if let Some(error) = error1 {
            let code = error as usize;
            error_counts[code] += 1;
            error_codes[code] = Some(error);
            error_mask |= 1u64 << code;
        }
        position += 2;
    }
    while position < indices.len() {
        // VPP tcp_input.c:2857-2883 prefetches the next tail packet.
        if let Some(&next) = indices.get(position + 1) {
            prefetch_tcp_input(runtime, next);
        }
        let index = indices[position];
        let mut error = None;
        let next = tcp_input_local_next_for_index::<IS_IP4, NO_LOOKUP>(runtime, index, &mut error);
        if let Some(error) = error {
            let code = error as usize;
            error_counts[code] += 1;
            error_codes[code] = Some(error);
            error_mask |= 1u64 << code;
        }
        let slot = match next {
            Ok(Some(slot)) => slot,
            Ok(None) => {
                position += 1;
                continue;
            }
            Err(_) => TcpInputNext::Drop.slot() as u16,
        };
        let count = output.len();
        output.set_vector_count(count + 1);
        output.vector_args_mut()[count] = index;
        nexts[count] = slot;
        position += 1;
    }
    // VPP tcp_input.c:2786-2890 writes the frame's nonzero error counters once.
    while error_mask != 0 {
        let code = error_mask.trailing_zeros() as usize;
        error_mask &= error_mask - 1;
        runtime
            .record_current_node_error_count(
                error_codes[code].expect("nonzero TCP input error has its typed code"),
                u64::from(error_counts[code]),
            )
            .expect("TCP input registers its error counters");
    }
    let count = output.len();
    if count != 0 {
        if hammer_runtime::unlikely(node_runtime.trace_enabled()) {
            trace_tcp_input_frame(runtime, node_runtime, output.vector_args(), &nexts[..count]);
        }
        runtime.enqueue_to_next(node_runtime, &mut output, &nexts[..count]);
    }
    ()
}

// VPP tcp_input.c:1644-1675,2888-2891. The frame's decisions are final;
// only Buffers marked by an upstream source append records here.
fn trace_tcp_input_frame(
    runtime: &mut DataPlaneMain,
    node: &NodeRuntime,
    indices: &[u32],
    nexts: &[u16],
) {
    let tcp = crate::TCP_MAIN
        .get()
        .expect("TCP Main initializes before input trace");
    let worker = tcp
        .worker(runtime.thread_index())
        .expect("TCP input trace reads its owner worker");
    for (&index, &next) in indices.iter().zip(nexts) {
        if !runtime.buffer(index).trace_handle().is_some() {
            continue;
        }
        let (header_ptr, header_len, connection) = {
            let buffer = runtime.buffer(index);
            let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
            let offset = network.packet_cursor().transport_header_offset();
            let header = buffer.current().get(offset..).unwrap_or(&[]);
            let route = hammer_core::buffer_opaque!(buffer => crate::TcpSecondaryOpaque).route();
            let connection = if next == TcpInputNext::Drop.slot() as u16
                || next == TcpInputNext::Punt.slot() as u16
                || next == TcpInputNext::Reset.slot() as u16
                || route.origin == crate::TcpRouteOrigin::Absent as u8
            {
                None
            } else if route.origin == crate::TcpRouteOrigin::Listener as u8 {
                tcp.listener_connection(route.connection_index)
            } else {
                worker.connection(route.connection_index)
            };
            (
                header.as_ptr(),
                header.len().min(core::mem::size_of::<crate::TcpHeader>()),
                connection,
            )
        };
        if let Some(trace) = runtime.add_trace::<TcpReceiveTrace>(node, index) {
            trace.record_connection(connection);
            trace.header_len = u8::try_from(header_len).expect("TCP base header length fits u8");
            // SAFETY: add_trace changes only the trace pool/Buffer handle;
            // the input Buffer's packet storage remains live and disjoint.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    header_ptr,
                    trace.tcp_header.as_mut_ptr(),
                    header_len,
                );
            }
        }
    }
}

#[inline(always)]
fn prefetch_tcp_input(runtime: &DataPlaneMain, index: u32) {
    // VPP tcp_input.c:2803-2807: STORE the header, LOAD two lines at b->data.
    runtime.prefetch_header_write(index);
    let buffer = runtime.buffer(index);
    // current() starts at data + current_data_offset; VPP prefetches data.
    let data = buffer
        .current()
        .as_ptr()
        .wrapping_offset(-isize::from(buffer.current_data_offset()));
    hammer_infra::prefetch::prefetch_read_l1_bytes(data, 2 * hammer_infra::align::CACHE_LINE);
}

#[inline(always)]
fn tcp_input_local_next_for_index<const IS_IP4: bool, const NO_LOOKUP: bool>(
    runtime: &mut DataPlaneMain,
    index: u32,
    error: &mut Option<TcpError>,
) -> RuntimeResult<Option<u16>> {
    let buffer = runtime.buffer(index);
    let parsed = tcp_input_buffer(&buffer)?;
    next_slot_for_index_with_runtime::<IS_IP4, NO_LOOKUP>(runtime, index, parsed, error)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpInputError {
    BadLength,
    WrongProtocol {
        version: IpVersion,
        protocol: IpProtocol,
    },
}

#[inline(always)]
fn next_slot_for_index_with_runtime<const IS_IP4: bool, const NO_LOOKUP: bool>(
    runtime: &mut DataPlaneMain,
    index: u32,
    parsed: Result<(IpVersion, IpProtocol, SocketAddr, SocketAddr, TcpInputFlags), TcpInputError>,
    classified_error: &mut Option<TcpError>,
) -> RuntimeResult<Option<u16>> {
    let (_, _, local, remote, flags) = match parsed {
        Ok(parsed) => parsed,
        Err(TcpInputError::BadLength) => {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::Length,
                classified_error,
            );
        }
        Err(TcpInputError::WrongProtocol { .. }) => {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::Dispatch,
                classified_error,
            );
        }
    };
    if local.is_ipv4() != IS_IP4 {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Drop,
            TcpError::SegmentInvalid,
            classified_error,
        );
    }
    let main = crate::TCP_MAIN
        .get()
        .expect("TCP input Node runs after TCP Main initialization");
    if NO_LOOKUP {
        // VPP tcp_inlines.h:359-361: the nolookup input reads the connection
        // index already carried by the Buffer, without a tuple lookup.
        let route = {
            let buffer = runtime.buffer(index);
            let opaque = hammer_core::buffer_opaque!(buffer => crate::TcpSecondaryOpaque).route();
            read_session_route_opaque(opaque)
                .map(|(session_id, owner, _)| (session_id, opaque.connection_index, owner))
        };
        let Some((session_id, connection_index, owner)) = route else {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::InvalidConnection,
                classified_error,
            );
        };
        let current_worker = runtime
            .data_worker_id()
            .expect("TCP nolookup input runs on a Data Worker");
        if owner != current_worker {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::WrongThread,
                classified_error,
            );
        }
        let state = {
            let worker = main.worker(runtime.thread_index())?;
            worker
                .connection(connection_index)
                .filter(|connection| {
                    connection.session_id() == session_id && connection.owner_worker() == owner
                })
                .map(|connection| connection.state())
        };
        let Some(state) = state else {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::InvalidConnection,
                classified_error,
            );
        };
        let dispatch = TCP_INPUT_DISPATCH[state.index()][usize::from(flags.bits())];
        {
            let buffer = runtime.buffer_mut(index);
            buffer.clear_node_error();
            write_session_route_opaque(
                hammer_core::buffer_opaque!(mut buffer => crate::TcpSecondaryOpaque).route_mut(),
                session_id,
                connection_index,
                owner,
                dispatch.next,
            );
        }
        if let Some(error) = dispatch.error {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                dispatch,
                error,
                classified_error,
            );
        }
        return Ok(Some(dispatch.slot()));
    }
    let fib_index = hammer_core::buffer_opaque!(runtime.buffer(index) => NetworkOpaque)
        .ip()
        .fib_index()
        .unwrap_or(0);
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
    // VPP tcp_input.c prefetches the established and listener tuple tables
    // before the first lookup; the lookup owner keeps both family tables
    // concrete and performs the family-specific key prefetch here.
    main.worker(runtime.thread_index())?
        .lookup
        .prefetch_tuple(local, remote);
    let exact_session = match ip_session.lookup_main().lookup_exact(&connection) {
        SessionLookupResult::Session(handle) => Some(handle),
        SessionLookupResult::WrongThread => {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::WrongThread,
                classified_error,
            );
        }
        SessionLookupResult::HalfOpen(_) | SessionLookupResult::NotFound => None,
    };
    if let Some(session) = exact_session
        && session.thread_index != runtime.thread_index() - 1
    {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Drop,
            TcpError::WrongThread,
            classified_error,
        );
    }

    let (session_route, listener_pending) = {
        let mut worker = main.worker(runtime.thread_index())?;
        worker.lookup.input_route(
            local,
            remote,
            flags.contains(TcpInputFlags::ACK) && !flags.contains(TcpInputFlags::RST),
        )
    };
    if let Some((session_id, owner)) = session_route {
        if exact_session.is_some_and(|session| {
            session.session_index != session_id || session.thread_index != owner.slot() as u32
        }) {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::InvalidConnection,
                classified_error,
            );
        }
        if owner.slot() as u32 != runtime.thread_index() - 1 {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::WrongThread,
                classified_error,
            );
        }
        let connection_index = {
            // SAFETY: this input node executes on the Session worker's Data Worker.
            let sessions = unsafe { ip_session.session().worker_mut(runtime) }?;
            sessions
                .session(session_id)
                .map(|session| session.connection_index())
        };
        let state = if let Some(connection_index) = connection_index {
            let worker = main.worker(runtime.thread_index())?;
            worker
                .connection(connection_index)
                .map(|connection| (connection_index, connection.state()))
        } else {
            None
        };
        let Some((connection_index, state)) = state else {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                TcpInputNext::Drop,
                TcpError::InvalidConnection,
                classified_error,
            );
        };
        // VPP tcp_input.c:2754-2772: dispatch from the current connection
        // state, not a next node cached when the tuple was published.
        let dispatch = TCP_INPUT_DISPATCH[state.index()][usize::from(flags.bits())];
        {
            let buffer = runtime.buffer_mut(index);
            buffer.clear_node_error();
            write_session_route_opaque(
                hammer_core::buffer_opaque!(mut buffer => crate::TcpSecondaryOpaque).route_mut(),
                session_id,
                connection_index,
                owner,
                dispatch.next,
            );
        }
        if let Some(error) = dispatch.error {
            return resolve_error_next_with_runtime(
                runtime,
                index,
                dispatch,
                error,
                classified_error,
            );
        }
        return Ok(Some(dispatch.slot()));
    }

    if exact_session.is_some() {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Drop,
            TcpError::InvalidConnection,
            classified_error,
        );
    }

    let mut transport = crate::tcp_endpoint_pair(local, remote).0;
    transport.local.fib_index = fib_index;
    let endpoint = IpSessionEndpoint::new(transport, main.protocol());
    let lookup = ip_session.lookup_listener(&endpoint, true);
    let Some(listener) = lookup else {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Reset,
            TcpError::NoListener,
            classified_error,
        );
    };
    let Some(registration) = main.listener_control.listener_for_session(listener) else {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Drop,
            TcpError::InvalidConnection,
            classified_error,
        );
    };
    let Some(connection) = main.listener_connection(registration.lookup_id) else {
        return resolve_error_next_with_runtime(
            runtime,
            index,
            TcpInputNext::Drop,
            TcpError::InvalidConnection,
            classified_error,
        );
    };
    let state = connection.state();
    if listener_pending {
        let buffer = runtime.buffer_mut(index);
        buffer.clear_node_error();
        *hammer_core::buffer_opaque!(mut buffer => crate::TcpSecondaryOpaque).route_mut() =
            crate::TcpRouteOpaque {
                session_raw: 0,
                owner_worker: registration.owner_worker.slot() as u32,
                connection_index: registration.lookup_id,
                next: TcpInputNext::Listen as u8,
                origin: crate::TcpRouteOrigin::Listener as u8,
                reserved: [0; 38],
            };
        return Ok(Some(TcpInputNext::Listen.slot() as u16));
    }
    let dispatch = TCP_INPUT_DISPATCH[state.index()][usize::from(flags.bits())];
    if let Some(error) = dispatch.error {
        return resolve_error_next_with_runtime(runtime, index, dispatch, error, classified_error);
    }
    {
        let buffer = runtime.buffer_mut(index);
        buffer.clear_node_error();
        let route =
            hammer_core::buffer_opaque!(mut buffer => crate::TcpSecondaryOpaque).route_mut();
        *route = if matches!(dispatch.next, TcpInputNext::Listen) {
            crate::TcpRouteOpaque {
                session_raw: 0,
                owner_worker: registration.owner_worker.slot() as u32,
                connection_index: registration.lookup_id,
                next: TcpInputNext::Listen as u8,
                origin: crate::TcpRouteOrigin::Listener as u8,
                reserved: [0; 38],
            }
        } else {
            Default::default()
        };
    }
    Ok(Some(dispatch.slot()))
}

#[inline(always)]
fn resolve_error_next_with_runtime(
    runtime: &mut DataPlaneMain,
    index: u32,
    next_key: impl NodeNext,
    error: TcpError,
    classified_error: &mut Option<TcpError>,
) -> RuntimeResult<Option<u16>> {
    let error_index = runtime.record_current_node_error_count(error, 0)?;
    runtime.buffer_mut(index).set_node_error_index(error_index);
    *classified_error = Some(error);
    Ok(Some(next_key.slot()))
}

#[inline(always)]
fn tcp_input_buffer(
    buffer: &hammer_core::data_plane::Buffer,
) -> RuntimeResult<
    Result<(IpVersion, IpProtocol, SocketAddr, SocketAddr, TcpInputFlags), TcpInputError>,
> {
    tcp_input_parts(
        buffer.current(),
        hammer_core::buffer_opaque!(buffer => NetworkOpaque),
    )
}

#[inline(always)]
fn tcp_input_parts(
    current: &[u8],
    network: &NetworkOpaque,
) -> RuntimeResult<
    Result<(IpVersion, IpProtocol, SocketAddr, SocketAddr, TcpInputFlags), TcpInputError>,
> {
    let cursor = network.packet_cursor();
    let Some((version, protocol)) = ip_facts(network) else {
        return Ok(Err(TcpInputError::BadLength));
    };
    if protocol != IpProtocol::Tcp {
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
fn ip_facts(network: &NetworkOpaque) -> Option<(IpVersion, IpProtocol)> {
    let version = match network.ip().ip_version()? {
        4 => IpVersion::V4,
        6 => IpVersion::V6,
        _ => return None,
    };
    Some((version, IpProtocol::from(network.ip().ip_protocol()?)))
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
fn source_ip(version: IpVersion, packet: &[u8]) -> RuntimeResult<IpAddr> {
    match version {
        IpVersion::V4 => {
            let Some(source) = packet.get(12..16) else {
                return Err(TcpError::Length.into());
            };
            Ok(Ipv4Addr::new(source[0], source[1], source[2], source[3]).into())
        }
        IpVersion::V6 => {
            let Some(source) = packet.get(8..24) else {
                return Err(TcpError::Length.into());
            };
            let bytes: [u8; 16] = source.try_into().map_err(|_| TcpError::Length)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

#[inline(always)]
fn destination_ip(version: IpVersion, packet: &[u8]) -> RuntimeResult<IpAddr> {
    match version {
        IpVersion::V4 => {
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
        IpVersion::V6 => {
            let Some(destination) = packet.get(24..40) else {
                return Err(TcpError::Length.into());
            };
            let bytes: [u8; 16] = destination.try_into().map_err(|_| TcpError::Length)?;
            Ok(Ipv6Addr::from(bytes).into())
        }
    }
}

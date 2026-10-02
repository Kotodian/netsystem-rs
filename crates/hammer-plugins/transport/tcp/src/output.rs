use crate::{TCP_FLAG_FIN, TCP_FLAG_SYN, TcpHeader, TcpState, tcp_header};
use core::hash::Hasher;
use hammer_core::data_plane::{
    BufferPacketCursor, DEFAULT_BUFFER_FRAME_CAPACITY, Frame, NodeId, NodeNext,
};
use hammer_infra::checksum::InternetChecksum;
use hammer_plugin_session::{IpSessionFamily, IpSessionMain, IpTransportConnectionId};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneMain, Node, NodeRuntime, TraceFormatter};
use hammer_service::session::SessionMain;
use hammer_service::session::node::SessionQueueNode;

use super::{TCP_EGRESS_TAG, TCP_MAIN, TcpError, read_tcp_egress_endpoints};
use hammer_service::opaque::NetworkOpaque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};
pub const DEFAULT_TCP_OUTPUT_PAYLOAD_LEN: usize = 1_440;
const TCP_PROTOCOL: u8 = 6;

// VPP tcp_output.c:2226-2252,2305-2316. The TCP header is captured before
// this node prepends the IP header.
#[repr(C)]
#[derive(KnownLayout, FromBytes, IntoBytes, Immutable)]
pub(crate) struct TcpOutputTrace {
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

impl TcpOutputTrace {
    fn record_connection(&mut self, connection: Option<&crate::connection::TcpConnection>) {
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

pub(crate) fn format_tcp_output_trace(bytes: &[u8]) -> String {
    let (trace, _) =
        TcpOutputTrace::ref_from_prefix(bytes).expect("TCP output trace has its registered layout");
    if usize::from(trace.header_len) < core::mem::size_of::<TcpHeader>() {
        return format!("tcp output truncated header ({} bytes)", trace.header_len);
    }
    let (header, _) = TcpHeader::ref_from_prefix(&trace.tcp_header)
        .expect("TCP output trace has a full TCP header");
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
            .expect("TCP output trace records a valid connection state")
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

#[hammer_component_macros::node_next]
pub enum TcpOutputNext {
    Drop,
    #[next("ip4-lookup")]
    Lookup,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::output::register_tcp4_output,
    next = TcpOutputNext,
    role = internal,
    name = "tcp4-output",
)]
#[derive(Clone, Copy)]
pub struct Tcp4OutputNode;

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::output::register_tcp6_output,
    next = TcpOutputNext,
    role = internal,
    name = "tcp6-output",
)]
#[derive(Clone, Copy)]
pub struct Tcp6OutputNode;

pub fn register_tcp4_output(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name(Tcp4OutputNode::NODE_NAME) {
        return Ok(node);
    }
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp4OutputNode::new(), &TcpOutputNext::NEXT_NAMES)?;
    runtime.register_node_errors(node, &crate::TCP_ERRORS)?;
    let session_queue = runtime
        .nodes()
        .node_by_name("session-queue")
        .expect("Session Queue Graph Node must be registered before TCP output");
    let next = SessionQueueNode::compile_output_next(runtime, session_queue, node)?;
    IpSessionMain::global()?.register_transport(
        TCP_MAIN
            .get()
            .expect("TCP Main initializes before output graph")
            .protocol(),
        IpSessionFamily::Ip4,
        next,
        crate::tcp_session_tx,
    );
    Ok(node)
}

pub fn register_tcp6_output(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name(Tcp6OutputNode::NODE_NAME) {
        return Ok(node);
    }
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp6OutputNode::new(), &["drop", "ip6-lookup"])?;
    runtime.register_node_errors(node, &crate::TCP_ERRORS)?;
    let session_queue = runtime
        .nodes()
        .node_by_name("session-queue")
        .expect("Session Queue Graph Node must be registered before TCP output");
    let next = SessionQueueNode::compile_output_next(runtime, session_queue, node)?;
    IpSessionMain::global()?.register_transport(
        TCP_MAIN
            .get()
            .expect("TCP Main initializes before output graph")
            .protocol(),
        IpSessionFamily::Ip6,
        next,
        crate::tcp_session_tx,
    );
    Ok(node)
}

impl Node for Tcp4OutputNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        tcp4_output_node_process(runtime, node_runtime, frame)
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntime> {
        Ok(NodeRuntime::default())
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_output_trace)
    }
}

impl Node for Tcp6OutputNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut hammer_runtime::NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        tcp6_output_node_process(runtime, node_runtime, frame)
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_output_trace)
    }
}

#[hammer_component_macros::node_function(node = Tcp4OutputNode)]
fn tcp4_output_node_process(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    tcp_output_node_process_frame::<true>(runtime, node_runtime, frame);
    processed_vectors
}

#[hammer_component_macros::node_function(node = Tcp6OutputNode)]
fn tcp6_output_node_process(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    tcp_output_node_process_frame::<false>(runtime, node_runtime, frame);
    processed_vectors
}

#[inline(always)]
fn tcp_output_node_process_frame<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut hammer_runtime::NodeRuntime,
    frame: &mut Frame,
) -> () {
    // VPP tcp_output.c:2312 refreshes the worker clock before the output
    // packet loop; only Session Queue advances and dispatches TCP timers.
    let now = SessionMain::global()
        .expect("Session Main initializes before TCP output")
        .now();
    let tcp = TCP_MAIN
        .get()
        .expect("TCP Main initializes before TCP output");
    {
        let mut worker = tcp
            .worker(runtime.thread_index())
            .expect("TCP output executes on its owner Data Worker");
        worker.time_us = now;
        worker.time_tstamp = ((now * 1_000.0) as u64) as u32;
    }
    if hammer_runtime::unlikely(node_runtime.trace_enabled()) {
        trace_tcp_output_frame(runtime, node_runtime, frame.vector_args());
    }
    let mut error_counts = [0u16; crate::TCP_ERRORS.len()];
    let mut error_codes = [None; crate::TCP_ERRORS.len()];
    let mut error_mask = 0u64;
    let indices = frame.vector_args();
    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut position = 0;
    while indices.len() - position >= 4 {
        // VPP tcp_output.c:2314-2363 prefetches the next pair before
        // constructing the current pair's IP headers.
        prefetch_tcp_output(runtime, indices[position + 2]);
        prefetch_tcp_output(runtime, indices[position + 3]);

        let mut error0 = None;
        let next0 = tcp_output_next_for_index::<IS_IP4>(runtime, indices[position], &mut error0)
            .unwrap_or(TcpOutputNext::Drop);
        nexts[position] = NodeNext::slot(next0);
        if let Some(error) = error0 {
            let code = error as usize;
            error_counts[code] += 1;
            error_codes[code] = Some(error);
            error_mask |= 1u64 << code;
        }

        let mut error1 = None;
        let next1 =
            tcp_output_next_for_index::<IS_IP4>(runtime, indices[position + 1], &mut error1)
                .unwrap_or(TcpOutputNext::Drop);
        nexts[position + 1] = NodeNext::slot(next1);
        if let Some(error) = error1 {
            let code = error as usize;
            error_counts[code] += 1;
            error_codes[code] = Some(error);
            error_mask |= 1u64 << code;
        }
        position += 2;
    }
    while position < indices.len() {
        // VPP tcp_output.c:2365-2393 prefetches the next packet in the tail.
        if let Some(&next) = indices.get(position + 1) {
            prefetch_tcp_output(runtime, next);
        }
        let mut error = None;
        let next = tcp_output_next_for_index::<IS_IP4>(runtime, indices[position], &mut error)
            .unwrap_or(TcpOutputNext::Drop);
        nexts[position] = NodeNext::slot(next);
        if let Some(error) = error {
            let code = error as usize;
            error_counts[code] += 1;
            error_codes[code] = Some(error);
            error_mask |= 1u64 << code;
        }
        position += 1;
    }
    let packet_count = indices.len();
    // VPP tcp_output.c:2301-2393 stores only nonzero frame-local counters.
    while error_mask != 0 {
        let code = error_mask.trailing_zeros() as usize;
        error_mask &= error_mask - 1;
        runtime
            .record_current_node_error_count(
                error_codes[code].expect("nonzero TCP error has its typed code"),
                u64::from(error_counts[code]),
            )
            .expect("TCP output registers its error counters");
    }
    runtime.enqueue_to_next(node_runtime, frame, &nexts[..packet_count]);
}

fn trace_tcp_output_frame(runtime: &mut DataPlaneMain, node: &NodeRuntime, indices: &[u32]) {
    let tcp = TCP_MAIN
        .get()
        .expect("TCP Main initializes before TCP output");
    let worker = tcp
        .worker(runtime.thread_index())
        .expect("TCP output traces its owner Data Worker");
    let worker_index = runtime
        .data_worker_id()
        .expect("TCP output executes on a Data Worker")
        .slot() as u32;
    for &index in indices {
        if !runtime.buffer(index).trace_handle().is_some() {
            continue;
        }
        let (header_ptr, header_len, connection) = {
            let buffer = runtime.buffer(index);
            let header = buffer.current();
            let egress = hammer_core::buffer_opaque!(buffer => crate::TcpSecondaryOpaque).egress();
            let connection = if egress.tag == TCP_EGRESS_TAG
                && egress.worker_index == worker_index
                && egress.connection_index != u32::MAX
            {
                worker.connection(egress.connection_index)
            } else {
                None
            };
            (
                header.as_ptr(),
                header.len().min(core::mem::size_of::<TcpHeader>()),
                connection,
            )
        };
        if let Some(trace) = runtime.add_trace::<TcpOutputTrace>(node, index) {
            trace.record_connection(connection);
            trace.header_len = u8::try_from(header_len).expect("TCP base header length fits u8");
            // SAFETY: add_trace mutates the trace pool/Buffer handle while
            // this node still owns disjoint, live TCP packet storage.
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
fn prefetch_tcp_output(runtime: &DataPlaneMain, index: u32) {
    // VPP tcp_output.c:2319-2323: STORE the header and two lines at b->data.
    runtime.prefetch_header_write(index);
    let buffer = runtime.buffer(index);
    let data = buffer
        .current()
        .as_ptr()
        .wrapping_offset(-isize::from(buffer.current_data_offset()));
    hammer_infra::prefetch::prefetch_write_l1_bytes(data, 2 * hammer_infra::align::CACHE_LINE);
}

#[inline(always)]
fn tcp_output_next_for_index<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    index: u32,
    error: &mut Option<TcpError>,
) -> RuntimeResult<TcpOutputNext> {
    let buffer = runtime.buffer(index);
    let header = buffer.current();
    if tcp_header(header).is_err() {
        *error = Some(TcpError::Length);
        return Ok(TcpOutputNext::Drop);
    }
    let tcp_len = buffer
        .current_len()
        .checked_add(buffer.total_len_not_including_first());
    let egress = *hammer_core::buffer_opaque!(buffer => crate::TcpSecondaryOpaque).egress();

    let Some(tcp_len) = tcp_len else {
        *error = Some(TcpError::Length);
        return Ok(TcpOutputNext::Drop);
    };

    if egress.tag != TCP_EGRESS_TAG {
        *error = Some(TcpError::InvalidConnection);
        return Ok(TcpOutputNext::Drop);
    }

    // VPP: tcp_output.c:2305-2401. Normal packets carry the owner-worker
    // connection index; only stateless early control packets use endpoint facts.
    let (local, remote, fib_index) = if egress.connection_index != u32::MAX {
        let worker_index = runtime.data_worker_id()?.slot() as u32;
        if egress.worker_index != worker_index {
            *error = Some(TcpError::InvalidConnection);
            return Ok(TcpOutputNext::Drop);
        }
        let main = TCP_MAIN
            .get()
            .expect("TCP output Node runs after TCP Main initialization");
        let tcp = main.worker(runtime.thread_index())?;
        let Some(connection) = tcp.connection(egress.connection_index) else {
            *error = Some(TcpError::InvalidConnection);
            return Ok(TcpOutputNext::Drop);
        };
        let Some(local) = connection.local() else {
            *error = Some(TcpError::InvalidConnection);
            return Ok(TcpOutputNext::Drop);
        };
        (
            local.ip(),
            connection.remote().ip(),
            connection.base.endpoint.fib_index(),
        )
    } else {
        let Some((local, remote)) = read_tcp_egress_endpoints(&egress) else {
            *error = Some(TcpError::InvalidConnection);
            return Ok(TcpOutputNext::Drop);
        };
        let fib_index = if egress.fib_index == u32::MAX {
            0
        } else {
            egress.fib_index
        };
        (local, remote, fib_index)
    };

    match (local, remote) {
        (IpAddr::V4(src), IpAddr::V4(dst)) if IS_IP4 => {
            let Some(total_len) = tcp_len
                .checked_add(20)
                .and_then(|length| u16::try_from(length).ok())
            else {
                *error = Some(TcpError::Length);
                return Ok(TcpOutputNext::Drop);
            };
            tcp_output_push_ipv4(runtime, index, src, dst, total_len, fib_index)?;
            Ok(TcpOutputNext::Lookup)
        }
        (IpAddr::V6(src), IpAddr::V6(dst)) if !IS_IP4 => {
            let Ok(payload_len) = u16::try_from(tcp_len) else {
                *error = Some(TcpError::Length);
                return Ok(TcpOutputNext::Drop);
            };
            tcp_output_push_ipv6(runtime, index, src, dst, payload_len, fib_index)?;
            Ok(TcpOutputNext::Lookup)
        }
        _ => {
            *error = Some(TcpError::InvalidConnection);
            Ok(TcpOutputNext::Drop)
        }
    }
}

/// VPP `tcp_output_push_ip` → `vlib_buffer_push_ip4(..., is_df=1)`.
#[inline(always)]
pub(crate) fn tcp_output_push_ipv4(
    runtime: &mut DataPlaneMain,
    index: u32,
    src: Ipv4Addr,
    dst: Ipv4Addr,
    total_len: u16,
    fib_index: u32,
) -> RuntimeResult<()> {
    const IPV4_HEADER_LEN: usize = 20;
    let tcp_len = total_len - IPV4_HEADER_LEN as u16;
    let mut checksum = InternetChecksum::default();
    checksum.write(&src.octets());
    checksum.write(&dst.octets());
    checksum.write(&[0, TCP_PROTOCOL]);
    checksum.write(&tcp_len.to_be_bytes());
    set_tcp_checksum(runtime, index, checksum)?;

    let buffer = runtime.buffer_mut(index);
    {
        let header = buffer.push_uninit(IPV4_HEADER_LEN as u8);
        hammer_plugin_ip::write_ipv4_push_header(header, src, dst, TCP_PROTOCOL, total_len, true)?;
    }
    let packet_len = usize::from(total_len);
    let tcp_header_len = tcp_header(&buffer.current()[IPV4_HEADER_LEN..])
        .map(|tcp| tcp.header_len())
        .unwrap_or(20);
    let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
    network.sw_if_index = [u32::MAX, fib_index];
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, IPV4_HEADER_LEN)
            .with_transport_header(IPV4_HEADER_LEN, tcp_header_len)
            .with_transport_payload_offset(IPV4_HEADER_LEN + tcp_header_len),
    );
    network.ip_mut().set_ip_version(Some(4));
    network.ip_mut().set_ip_protocol(Some(6));
    network.ip_mut().set_fib_index(Some(fib_index));
    Ok(())
}

/// VPP `tcp_output_push_ip` IPv6 path (`vlib_buffer_push_ip6_custom`).
#[inline(always)]
pub(crate) fn tcp_output_push_ipv6(
    runtime: &mut DataPlaneMain,
    index: u32,
    src: Ipv6Addr,
    dst: Ipv6Addr,
    payload_len: u16,
    fib_index: u32,
) -> RuntimeResult<()> {
    const IPV6_HEADER_LEN: usize = 40;
    let mut checksum = InternetChecksum::default();
    checksum.write(&src.octets());
    checksum.write(&dst.octets());
    checksum.write(&u32::from(payload_len).to_be_bytes());
    checksum.write(&[0, 0, 0, TCP_PROTOCOL]);
    set_tcp_checksum(runtime, index, checksum)?;

    let buffer = runtime.buffer_mut(index);
    {
        let header = buffer.push_uninit(IPV6_HEADER_LEN as u8);
        hammer_plugin_ip::write_ipv6_push_header(header, src, dst, TCP_PROTOCOL, payload_len)?;
    }
    let packet_len = IPV6_HEADER_LEN + usize::from(payload_len);
    let tcp_header_len = tcp_header(&buffer.current()[IPV6_HEADER_LEN..])
        .map(|tcp| tcp.header_len())
        .unwrap_or(20);
    let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
    network.sw_if_index = [u32::MAX, fib_index];
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, IPV6_HEADER_LEN)
            .with_transport_header(IPV6_HEADER_LEN, tcp_header_len)
            .with_transport_payload_offset(IPV6_HEADER_LEN + tcp_header_len),
    );
    network.ip_mut().set_ip_version(Some(6));
    network.ip_mut().set_ip_protocol(Some(6));
    network.ip_mut().set_fib_index(Some(fib_index));
    Ok(())
}

#[inline(always)]
fn set_tcp_checksum(
    runtime: &mut DataPlaneMain,
    index: u32,
    mut checksum: InternetChecksum,
) -> RuntimeResult<()> {
    {
        let buffer = runtime.buffer_mut(index);
        let (header, _) =
            TcpHeader::mut_from_prefix(buffer.current_mut()).map_err(|_| TcpError::Length)?;
        header.set_checksum(0);
    }
    for buffer in runtime.chain(index) {
        checksum.write(buffer.current());
    }
    let value = checksum.finish() as u16;
    let buffer = runtime.buffer_mut(index);
    let (header, _) =
        TcpHeader::mut_from_prefix(buffer.current_mut()).map_err(|_| TcpError::Length)?;
    header.set_checksum(value);
    Ok(())
}

#[inline]
pub const fn tcp_effective_output_payload_len(peer_max_segment_size: Option<u16>) -> usize {
    match peer_max_segment_size {
        Some(max_segment_size) if max_segment_size != 0 => {
            let max_segment_size = max_segment_size as usize;
            if max_segment_size < DEFAULT_TCP_OUTPUT_PAYLOAD_LEN {
                max_segment_size
            } else {
                DEFAULT_TCP_OUTPUT_PAYLOAD_LEN
            }
        }
        _ => DEFAULT_TCP_OUTPUT_PAYLOAD_LEN,
    }
}

#[inline]
pub const fn tcp_send_goal_size(peer_max_segment_size: Option<u16>) -> usize {
    tcp_effective_output_payload_len(peer_max_segment_size)
}

#[inline]
pub fn tcp_available_send_window(
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    congestion_window: u32,
) -> u32 {
    snd_wnd
        .min(congestion_window)
        .saturating_sub(tcp_inflight_sequence_len(snd_una, snd_nxt))
}

#[inline]
pub fn tcp_payload_len_in_send_window(
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    congestion_window: u32,
    requested_payload_len: usize,
    control_len: u32,
) -> usize {
    let available_payload_len =
        tcp_available_send_window(snd_una, snd_nxt, snd_wnd, congestion_window)
            .saturating_sub(control_len) as usize;
    available_payload_len.min(requested_payload_len)
}

#[inline]
pub const fn tcp_output_sequence_len(flags: u8, payload_len: usize) -> u32 {
    let control_len = ((flags & TCP_FLAG_SYN != 0) as u32) + ((flags & TCP_FLAG_FIN != 0) as u32);
    payload_len as u32 + control_len
}

#[inline]
pub fn tcp_output_next_sequence(sequence: u32, sequence_len: u32) -> u32 {
    let sequence: crate::TcpSeq = sequence.into();
    sequence.advance(sequence_len).raw()
}

#[inline]
fn tcp_inflight_sequence_len(snd_una: u32, snd_nxt: u32) -> u32 {
    if snd_una != 0 && snd_nxt != 0 {
        let snd_una: crate::TcpSeq = snd_una.into();
        let snd_nxt: crate::TcpSeq = snd_nxt.into();
        snd_una.distance_to(snd_nxt)
    } else {
        0
    }
}

#[cfg(test)]
mod opaque_tests {
    use super::*;
    use crate::{TcpCapabilities, TcpSecondaryOpaque, TcpSegment, TcpSegmentFlags};

    // Derived from vnet/tcp/tcp_output.c: tcp_output_push_ip consumes the
    // transport producer's packet metadata to select the IP lookup arc.
    #[test]
    fn segment_metadata_reaches_ip_output() -> RuntimeResult<()> {
        hammer_core::buffer::BufferMain::new(2048, 16, &[0], 1, hammer_infra::PageSize::Default)?;
        let mut runtime = DataPlaneMain::new(hammer_runtime::DataPlaneBufferConfig {
            buffer_slot_capacity: 2048,
            buffer_slots: 16,
            ..Default::default()
        });
        let mut index = 0;
        if runtime.buffer_alloc(core::slice::from_mut(&mut index)) != 1 {
            return Err(hammer_core::error::DataPlaneError::from(
                hammer_core::error::BufferInvariant::PoolExhausted,
            )
            .into());
        }
        let local = "192.0.2.1:1234".parse().unwrap();
        let remote = "192.0.2.2:4321".parse().unwrap();
        {
            let buffer = runtime.buffer_mut(index);
            let opaque = hammer_core::buffer_opaque!(mut buffer => TcpSecondaryOpaque);
            crate::write_session_route_opaque(
                opaque.route_mut(),
                17,
                23,
                hammer_runtime::DataWorkerId::new(0),
                crate::TcpInputNext::Established,
            );
            let (session, worker, next) = crate::read_session_route_opaque(opaque.route()).unwrap();
            assert_eq!(session, 17);
            assert_eq!(opaque.route().connection_index, 23);
            assert_eq!(worker.slot(), 0);
            assert!(matches!(next, crate::TcpInputNext::Established));
            assert_eq!(
                std::ptr::from_ref(opaque.route()).addr(),
                std::ptr::from_ref(opaque.egress()).addr()
            );
            TcpSegment::new(
                local,
                remote,
                1,
                2,
                1024,
                TcpSegmentFlags::ACK,
                TcpCapabilities::default(),
                None,
                None,
                None,
                None,
                0,
            )
            .write_to_buffer(buffer)?;
        }
        let mut error = None;
        assert!(matches!(
            tcp_output_next_for_index::<1, true>(&mut runtime, index, &mut error)?,
            TcpOutputNext::Lookup
        ));
        assert_eq!(error, None);
        {
            let buffer = runtime.buffer(index);
            assert_eq!(&buffer.current()[12..16], &[192, 0, 2, 1]);
            assert_eq!(&buffer.current()[16..20], &[192, 0, 2, 2]);
            let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
            assert_eq!(network.ip().ip_protocol(), Some(6));
            assert_eq!(network.packet_cursor().transport_header_offset(), 20);
        }
        runtime.buffer_free_one(index);
        Ok(())
    }
}

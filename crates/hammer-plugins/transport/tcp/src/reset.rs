use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::{TcpCapabilities, TcpSegmentFlags, tcp_header};
use hammer_core::data_plane::{BufferPacketCursor, Frame, NodeId};
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntime, RuntimeResult, TraceFormatter};
use hammer_service::opaque::{NetworkFlags, NetworkOpaque};

use super::output::{
    TcpOutputTrace, format_tcp_output_trace, tcp_output_push_ipv4, tcp_output_push_ipv6,
};
use super::segment::TcpSegment;

#[hammer_component_macros::node_next]
pub enum TcpResetNext {
    Drop,
    #[next("ip4-lookup")]
    Lookup,
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::reset::register_tcp4_reset,
    next = TcpResetNext,
    role = internal,
    name = "tcp4-reset",
)]
#[derive(Clone, Copy)]
pub struct Tcp4ResetNode;

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::reset::register_tcp6_reset,
    next = TcpResetNext,
    role = internal,
    name = "tcp6-reset",
)]
#[derive(Clone, Copy)]
pub struct Tcp6ResetNode;

pub fn register_tcp4_reset(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp4ResetNode::new(), &TcpResetNext::NEXT_NAMES)
}

pub fn register_tcp6_reset(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .try_register_internal_with_next_names(Tcp6ResetNode::new(), &["drop", "ip6-lookup"])
}

impl Node for Tcp4ResetNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_reset_process::<true>;
        process(runtime, node_runtime, frame)
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_output_trace)
    }
}

impl Node for Tcp6ResetNode {
    #[inline(always)]
    fn process(
        runtime: &mut DataPlaneMain,
        node_runtime: &mut NodeRuntime,
        frame: &mut Frame,
    ) -> usize {
        let process: NodeProcessFn = tcp_reset_process::<false>;
        process(runtime, node_runtime, frame)
    }

    fn node_trace_formatter(&self) -> Option<TraceFormatter> {
        Some(format_tcp_output_trace)
    }
}

fn tcp_reset_process<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    node_runtime: &mut NodeRuntime,
    frame: &mut Frame,
) -> usize {
    let processed_vectors = frame.len();
    let trace_enabled = hammer_runtime::unlikely(node_runtime.trace_enabled());
    hammer_runtime::process_frame!(runtime, node_runtime, frame, |index| {
        let next = tcp_reset_next_for_index::<IS_IP4>(runtime, index)
            .unwrap_or(TcpResetNext::Drop);
        // VPP tcp_output.c:2502-2549 records the completed RST after its
        // TCP and IP headers have been constructed, before next enqueue.
        if trace_enabled
            && matches!(next, TcpResetNext::Lookup)
            && hammer_runtime::unlikely(runtime.buffer(index).trace_handle().is_some())
        {
            let (header_ptr, header_len) = {
                let buffer = runtime.buffer(index);
                let cursor = hammer_core::buffer_opaque!(buffer => NetworkOpaque).packet_cursor();
                let header = buffer
                    .current()
                    .get(cursor.transport_header_offset()..)
                    .unwrap_or(&[]);
                (header.as_ptr(), header.len().min(core::mem::size_of::<crate::TcpHeader>()))
            };
            if let Some(trace) = runtime.add_trace::<TcpOutputTrace>(node_runtime, index) {
                trace.connection_index = u32::MAX;
                trace.has_connection = 0;
                trace.header_len = u8::try_from(header_len)
                    .expect("TCP base header length fits u8");
                // SAFETY: add_trace mutates the trace pool/Buffer handle;
                // the completed reset packet remains live in disjoint storage.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        header_ptr,
                        trace.tcp_header.as_mut_ptr(),
                        header_len,
                    );
                }
            }
        }
        next
    });
    processed_vectors
}

#[inline(always)]
fn tcp_reset_next_for_index<const IS_IP4: bool>(
    runtime: &mut DataPlaneMain,
    index: u32,
) -> RuntimeResult<TcpResetNext> {
    let Some((segment, tcp_offset, source, destination, fib_index, input_interface)) = ({
        let buffer = runtime.buffer(index);
        let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
        let Some(fib_index) = network.ip().fib_index() else {
            return Ok(TcpResetNext::Drop);
        };
        tcp_reset_segment(buffer.current(), network.packet_cursor()).map(
            |(segment, tcp_offset, source, destination)| {
                (
                    segment,
                    tcp_offset,
                    source,
                    destination,
                    fib_index,
                    network.sw_if_index[0],
                )
            },
        )
    }) else {
        return Ok(TcpResetNext::Drop);
    };
    if source.is_ipv4() != IS_IP4 {
        return Ok(TcpResetNext::Drop);
    }

    let next_buffer = runtime.buffer(index).next_buffer_slot();
    if let Some(next_buffer) = next_buffer {
        runtime.buffer_mut(index).set_next_buffer(None);
        runtime.buffer_free_one(next_buffer);
    }
    {
        let buffer = runtime.buffer_mut(index);
        buffer.set_total_len_not_including_first(0)?;
        buffer.advance(tcp_offset as isize);
        buffer.truncate(0)?;
        let header = buffer.push_uninit(segment.header_len() as u8);
        segment.write_header(header)?;
        buffer.clear_node_error();
    }
    match (source, destination) {
        (IpAddr::V4(source), IpAddr::V4(destination)) if IS_IP4 => {
            tcp_output_push_ipv4(runtime, index, source, destination, 40, fib_index)?;
            let buffer = runtime.buffer_mut(index);
            let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
            network.sw_if_index[0] = input_interface;
            network.flags.insert(NetworkFlags::LOCALLY_ORIGINATED);
            Ok(TcpResetNext::Lookup)
        }
        (IpAddr::V6(source), IpAddr::V6(destination)) if !IS_IP4 => {
            tcp_output_push_ipv6(runtime, index, source, destination, 20, fib_index)?;
            let buffer = runtime.buffer_mut(index);
            let network = hammer_core::buffer_opaque!(mut buffer => NetworkOpaque);
            network.sw_if_index[0] = input_interface;
            network.flags.insert(NetworkFlags::LOCALLY_ORIGINATED);
            Ok(TcpResetNext::Lookup)
        }
        _ => unreachable!("TCP reset source and destination share the incoming IP family"),
    }
}

/// VPP: tcp_output.c:570-650, tcp_buffer_make_reset; 2502-2538, tcp46_reset_inline.
#[inline(always)]
fn tcp_reset_segment(
    packet: &[u8],
    cursor: BufferPacketCursor,
) -> Option<(TcpSegment, usize, IpAddr, IpAddr)> {
    let transport_offset = cursor.transport_header_offset();
    let header_end = transport_offset.checked_add(20)?;
    if cursor.network_header_offset() > transport_offset
        || header_end > packet.len()
        || header_end > cursor.packet_len()
        || cursor.transport_payload_offset() > cursor.packet_len()
    {
        return None;
    }
    let version = packet.get(cursor.network_header_offset())? >> 4;
    let network_header = packet.get(cursor.network_header_offset()..transport_offset)?;
    let tcp = tcp_header(packet.get(transport_offset..)?).ok()?;
    if transport_offset.checked_add(tcp.header_len())? != cursor.transport_payload_offset()
        || cursor.transport_payload_offset() > packet.len()
    {
        return None;
    }
    let flags = tcp.flags();
    if flags.contains(TcpSegmentFlags::RST) {
        return None;
    }
    let (source, destination) = match version {
        4 => {
            let source = network_header.get(12..16)?;
            let destination = network_header.get(16..20)?;
            (
                IpAddr::V4(Ipv4Addr::new(source[0], source[1], source[2], source[3])),
                IpAddr::V4(Ipv4Addr::new(
                    destination[0],
                    destination[1],
                    destination[2],
                    destination[3],
                )),
            )
        }
        6 => {
            let source: [u8; 16] = network_header.get(8..24)?.try_into().ok()?;
            let destination: [u8; 16] = network_header.get(24..40)?.try_into().ok()?;
            (
                IpAddr::V6(Ipv6Addr::from(source)),
                IpAddr::V6(Ipv6Addr::from(destination)),
            )
        }
        _ => return None,
    };
    let (sequence, acknowledgment, reset_flags) = if flags.contains(TcpSegmentFlags::ACK) {
        (tcp.acknowledgment_number(), 0, TcpSegmentFlags::RST)
    } else {
        let payload_len = cursor
            .packet_len()
            .checked_sub(cursor.transport_payload_offset())?;
        let sequence_len = payload_len
            .checked_add(usize::from(flags.contains(TcpSegmentFlags::SYN)))?
            .checked_add(usize::from(flags.contains(TcpSegmentFlags::FIN)))?;
        let sequence_len = u32::try_from(sequence_len).ok()?;
        (
            0,
            tcp.sequence_number().wrapping_add(sequence_len),
            TcpSegmentFlags::RST | TcpSegmentFlags::ACK,
        )
    };
    let segment = TcpSegment::new(
        SocketAddr::new(destination, tcp.destination_port()),
        SocketAddr::new(source, tcp.source_port()),
        sequence,
        acknowledgment,
        0,
        reset_flags,
        TcpCapabilities::default(),
        None,
        None,
        None,
        None,
        0,
    );
    Some((segment, header_end, destination, source))
}

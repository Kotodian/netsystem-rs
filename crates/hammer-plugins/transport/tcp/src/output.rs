use crate::{TCP_FLAG_FIN, TCP_FLAG_SYN, tcp_header};
use core::hash::Hasher;
use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, NodeId, NodeState};
use hammer_infra::checksum::InternetChecksum;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneMain, Node, NodeProcessFn, NodeRuntimeData};
use hammer_service::session::node::SessionQueueNode;

use super::{TcpOutputError, read_tcp_egress_endpoints};
use hammer_service::opaque::NetworkOpaque;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
pub const DEFAULT_TCP_OUTPUT_PAYLOAD_LEN: usize = 1_440;
const TCP_CHECKSUM_OFFSET: usize = 16;
const TCP_PROTOCOL: u8 = 6;

#[hammer_component_macros::node_next]
pub enum TcpOutputNext {
    Drop,
    #[next("ip4-lookup")]
    LookupV4,
    #[next("ip6-lookup")]
    LookupV6,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::output::register_tcp_output,
    next = TcpOutputNext,
    role = internal,
)]
#[derive(Clone, Copy)]
pub struct TcpOutputNode;

pub fn register_tcp_output(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    if let Some(node) = runtime.nodes().node_by_name(TcpOutputNode::NODE_NAME) {
        return Ok(node);
    }
    let node = runtime
        .nodes()
        .try_register_internal_with_next_names(TcpOutputNode::new(), &TcpOutputNext::NEXT_NAMES)?;
    let session_queue = runtime
        .nodes()
        .node_by_name("session-queue")
        .expect("Session Queue Graph Node must be registered before TCP output");
    SessionQueueNode::compile_output_next(runtime, session_queue, node)?;
    runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    Ok(node)
}

impl Node for TcpOutputNode {
    #[inline(always)]
    fn process(&mut self, runtime: &mut DataPlaneMain, frame: &mut BufferFrame) -> () {
        tcp_output_node_process_frame::<1>(runtime, frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_output_node_process
    }

    #[inline]
    fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn tcp_output_node_process(
    runtime: &mut DataPlaneMain,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> () {
    tcp_output_node_process_frame::<1>(runtime, frame)
}

#[hammer_component_macros::node_function(node = TcpOutputNode)]
fn tcp_output_node_process_simd<const SIMD_BYTES: usize>(
    runtime: &mut DataPlaneMain,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> () {
    tcp_output_node_process_frame::<SIMD_BYTES>(runtime, frame)
}

fn tcp_output_node_process_frame<const SIMD_BYTES: usize>(
    runtime: &mut DataPlaneMain,
    frame: &mut BufferFrame,
) -> () {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        tcp_output_next_for_index::<SIMD_BYTES>(runtime, index).unwrap_or(TcpOutputNext::Drop)
    })
}

fn tcp_output_next_for_index<const SIMD_BYTES: usize>(
    runtime: &mut DataPlaneMain,
    index: u32,
) -> RuntimeResult<TcpOutputNext> {
    let buffer = runtime.buffer(index);
    let header = buffer.current();
    if tcp_header(header).is_err() {
        let _ = runtime.record_current_node_error(TcpOutputError::NoTcpHeader);
        return Ok(TcpOutputNext::Drop);
    }
    let tcp_len = buffer
        .current_len()
        .checked_add(buffer.total_len_not_including_first());
    let endpoints = read_tcp_egress_endpoints(
        hammer_core::buffer_opaque!(buffer => crate::TcpSecondaryOpaque).egress(),
    );

    let Some(tcp_len) = tcp_len else {
        let _ = runtime.record_current_node_error(TcpOutputError::SegmentTooLong);
        return Ok(TcpOutputNext::Drop);
    };

    let Some((local, remote)) = endpoints else {
        let _ = runtime.record_current_node_error(TcpOutputError::MissingEgressEndpoints);
        return Ok(TcpOutputNext::Drop);
    };

    match (local, remote) {
        (IpAddr::V4(src), IpAddr::V4(dst)) => {
            let Some(total_len) = tcp_len
                .checked_add(20)
                .and_then(|length| u16::try_from(length).ok())
            else {
                let _ = runtime.record_current_node_error(TcpOutputError::SegmentTooLong);
                return Ok(TcpOutputNext::Drop);
            };
            tcp_output_push_ipv4::<SIMD_BYTES>(runtime, index, src, dst, total_len)?;
            Ok(TcpOutputNext::LookupV4)
        }
        (IpAddr::V6(src), IpAddr::V6(dst)) => {
            let Ok(payload_len) = u16::try_from(tcp_len) else {
                let _ = runtime.record_current_node_error(TcpOutputError::SegmentTooLong);
                return Ok(TcpOutputNext::Drop);
            };
            tcp_output_push_ipv6::<SIMD_BYTES>(runtime, index, src, dst, payload_len)?;
            Ok(TcpOutputNext::LookupV6)
        }
        _ => {
            let _ = runtime.record_current_node_error(TcpOutputError::UnsupportedEgress);
            Ok(TcpOutputNext::Drop)
        }
    }
}

/// VPP `tcp_output_push_ip` → `vlib_buffer_push_ip4(..., is_df=1)`.
fn tcp_output_push_ipv4<const SIMD_BYTES: usize>(
    runtime: &mut DataPlaneMain,
    index: u32,
    src: Ipv4Addr,
    dst: Ipv4Addr,
    total_len: u16,
) -> RuntimeResult<()> {
    const IPV4_HEADER_LEN: usize = 20;
    let tcp_len = total_len - IPV4_HEADER_LEN as u16;
    let mut checksum = InternetChecksum::<SIMD_BYTES>::default();
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
    network.sw_if_index = [u32::MAX; 2];
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, IPV4_HEADER_LEN)
            .with_transport_header(IPV4_HEADER_LEN, tcp_header_len)
            .with_transport_payload_offset(IPV4_HEADER_LEN + tcp_header_len),
    );
    network.ip_mut().set_ip_version(Some(4));
    network.ip_mut().set_ip_protocol(Some(6));
    Ok(())
}

/// VPP `tcp_output_push_ip` IPv6 path (`vlib_buffer_push_ip6_custom`).
fn tcp_output_push_ipv6<const SIMD_BYTES: usize>(
    runtime: &mut DataPlaneMain,
    index: u32,
    src: Ipv6Addr,
    dst: Ipv6Addr,
    payload_len: u16,
) -> RuntimeResult<()> {
    const IPV6_HEADER_LEN: usize = 40;
    let mut checksum = InternetChecksum::<SIMD_BYTES>::default();
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
    network.sw_if_index = [u32::MAX; 2];
    network.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, IPV6_HEADER_LEN)
            .with_transport_header(IPV6_HEADER_LEN, tcp_header_len)
            .with_transport_payload_offset(IPV6_HEADER_LEN + tcp_header_len),
    );
    network.ip_mut().set_ip_version(Some(6));
    network.ip_mut().set_ip_protocol(Some(6));
    Ok(())
}

fn set_tcp_checksum<const SIMD_BYTES: usize>(
    runtime: &mut DataPlaneMain,
    index: u32,
    mut checksum: InternetChecksum<SIMD_BYTES>,
) -> RuntimeResult<()> {
    {
        let buffer = runtime.buffer_mut(index);
        buffer.current_mut()[TCP_CHECKSUM_OFFSET..TCP_CHECKSUM_OFFSET + 2].fill(0);
    }
    for buffer in runtime.chain(index) {
        checksum.write(buffer?.current());
    }
    let value = checksum.finish() as u16;
    let buffer = runtime.buffer_mut(index);
    buffer.current_mut()[TCP_CHECKSUM_OFFSET..TCP_CHECKSUM_OFFSET + 2]
        .copy_from_slice(&value.to_be_bytes());
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
        hammer_infra::main_heap::init_default().unwrap();
        hammer_core::buffer::BufferMain::new(2048, 16, &[0], 1, hammer_infra::PageSize::Default)?;
        let mut runtime = DataPlaneMain::new(hammer_runtime::DataPlaneBufferConfig {
            buffer_slot_capacity: 2048,
            buffer_slots: 16,
            ..Default::default()
        });
        let index = runtime.alloc_index()?;
        let local = "192.0.2.1:1234".parse().unwrap();
        let remote = "192.0.2.2:4321".parse().unwrap();
        {
            let buffer = runtime.buffer_mut(index);
            let opaque = hammer_core::buffer_opaque!(mut buffer => TcpSecondaryOpaque);
            crate::write_session_route_opaque(
                opaque.route_mut(),
                17,
                hammer_runtime::DataWorkerId::new(0),
                crate::TcpInputNext::Established,
            );
            let (session, worker, next) = crate::read_session_route_opaque(opaque.route()).unwrap();
            assert_eq!(session, 17);
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
        assert!(matches!(
            tcp_output_next_for_index::<1>(&mut runtime, index)?,
            TcpOutputNext::LookupV4
        ));
        {
            let buffer = runtime.buffer(index);
            assert_eq!(&buffer.current()[12..16], &[192, 0, 2, 1]);
            assert_eq!(&buffer.current()[16..20], &[192, 0, 2, 2]);
            let network = hammer_core::buffer_opaque!(buffer => NetworkOpaque);
            assert_eq!(network.ip().ip_protocol(), Some(6));
            assert_eq!(network.packet_cursor().transport_header_offset(), 20);
        }
        runtime.buffers().drop_index_owned_with_trace(index, |_| {});
        Ok(())
    }
}

#[cfg(test)]
use crate::{TCP_FLAG_ACK, TCP_FLAG_PSH};
use crate::{TCP_FLAG_FIN, TCP_FLAG_SYN, tcp_header};
use core::hash::Hasher;
use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId, NodeState};
use hammer_infra::checksum::InternetChecksum;
#[cfg(test)]
use hammer_runtime::InternalNode;
use hammer_runtime::RuntimeResult;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};
use hammer_service::session::node::SessionQueueNode;

use super::{TcpOutputError, read_tcp_egress_endpoints};
use hammer_service::opaque::NetworkOpaque;
use std::mem::transmute;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
pub const DEFAULT_TCP_OUTPUT_PAYLOAD_LEN: usize = 1_440;
const TCP_CHECKSUM_OFFSET: usize = 16;
const TCP_PROTOCOL: u8 = 6;

#[hammer_component_macros::node_next]
pub enum TcpOutputNext {
    Drop,
    #[next("ip-lookup")]
    Lookup,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::output::register_tcp_output,
    next = TcpOutputNext,
    role = internal,
)]
#[derive(Clone, Copy)]
pub struct TcpOutputNode;

pub fn register_tcp_output(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
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
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
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
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    tcp_output_node_process_frame::<1>(runtime, frame)
}

#[hammer_component_macros::node_function(node = TcpOutputNode)]
fn tcp_output_node_process_simd<const SIMD_BYTES: usize>(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    tcp_output_node_process_frame::<SIMD_BYTES>(runtime, frame)
}

fn tcp_output_node_process_frame<const SIMD_BYTES: usize>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
) -> NodeResult {
    hammer_runtime::process_frame!(runtime, frame, |index| {
        tcp_output_next_for_index::<SIMD_BYTES>(runtime, index).unwrap_or(TcpOutputNext::Drop)
    })
}

fn tcp_output_next_for_index<const SIMD_BYTES: usize>(
    runtime: &DataPlaneRuntime,
    index: Index,
) -> RuntimeResult<TcpOutputNext> {
    let buffer = runtime.get_buffer(index)?;
    let header = buffer.current();
    if tcp_header(header).is_err() {
        let _ = runtime.record_current_node_error(TcpOutputError::NoTcpHeader);
        return Ok(TcpOutputNext::Drop);
    }
    let tcp_len = buffer
        .current_len()
        .checked_add(buffer.total_len_not_including_first());
    let endpoints = read_tcp_egress_endpoints(buffer.opaque2());
    drop(buffer);

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
            Ok(TcpOutputNext::Lookup)
        }
        (IpAddr::V6(src), IpAddr::V6(dst)) => {
            let Ok(payload_len) = u16::try_from(tcp_len) else {
                let _ = runtime.record_current_node_error(TcpOutputError::SegmentTooLong);
                return Ok(TcpOutputNext::Drop);
            };
            tcp_output_push_ipv6::<SIMD_BYTES>(runtime, index, src, dst, payload_len)?;
            Ok(TcpOutputNext::Lookup)
        }
        _ => {
            let _ = runtime.record_current_node_error(TcpOutputError::UnsupportedEgress);
            Ok(TcpOutputNext::Drop)
        }
    }
}

/// VPP `tcp_output_push_ip` → `vlib_buffer_push_ip4(..., is_df=1)`.
fn tcp_output_push_ipv4<const SIMD_BYTES: usize>(
    runtime: &DataPlaneRuntime,
    index: Index,
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

    let mut buffer = runtime.get_buffer_mut(index)?;
    {
        let header = buffer.prepend_mut(IPV4_HEADER_LEN)?;
        hammer_plugin_ip::write_ipv4_push_header(header, src, dst, TCP_PROTOCOL, total_len)?;
    }
    let packet_len = usize::from(total_len);
    let tcp_header_len = tcp_header(&buffer.current()[IPV4_HEADER_LEN..])
        .map(|tcp| tcp.header_len())
        .unwrap_or(20);
    let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
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
    runtime: &DataPlaneRuntime,
    index: Index,
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

    let mut buffer = runtime.get_buffer_mut(index)?;
    {
        let header = buffer.prepend_mut(IPV6_HEADER_LEN)?;
        hammer_plugin_ip::write_ipv6_push_header(header, src, dst, TCP_PROTOCOL, payload_len)?;
    }
    let packet_len = IPV6_HEADER_LEN + usize::from(payload_len);
    let tcp_header_len = tcp_header(&buffer.current()[IPV6_HEADER_LEN..])
        .map(|tcp| tcp.header_len())
        .unwrap_or(20);
    let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
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
    runtime: &DataPlaneRuntime,
    index: Index,
    mut checksum: InternetChecksum<SIMD_BYTES>,
) -> RuntimeResult<()> {
    {
        let mut buffer = runtime.get_buffer_mut(index)?;
        buffer.current_mut()[TCP_CHECKSUM_OFFSET..TCP_CHECKSUM_OFFSET + 2].fill(0);
    }
    for buffer in runtime.chain(index) {
        checksum.write(buffer?.current());
    }
    let value = checksum.finish() as u16;
    let mut buffer = runtime.get_buffer_mut(index)?;
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
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex, OnceLock};

    use crate::{TcpCapabilities, TcpSegmentFlags};
    use hammer_core::data_plane::BufferFrame;
    use hammer_infra::checksum::internet_checksum_parts;
    use hammer_runtime::NodeProcessFn;

    use super::*;
    use crate::segment::TcpSegment;

    #[derive(Default)]
    struct CaptureState {
        packets: Vec<Vec<u8>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states().lock().expect("capture registry");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
            }
        }
    }

    impl Node for CaptureNode {
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }

        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        fn node_runtime_data(&self) -> RuntimeResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl InternalNode for CaptureNode {}

    fn capture_states() -> &'static Mutex<Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> NodeResult {
        let slot = match data.usize_word(0) {
            Ok(s) => s,
            Err(_) => return NodeResult::drop(),
        };
        let state = match capture_states().lock() {
            Ok(states) => match states.get(slot) {
                Some(s) => Arc::clone(s),
                None => return NodeResult::drop(),
            },
            Err(_) => return NodeResult::drop(),
        };
        for &index in frame.pending_indices() {
            let mut packet = Vec::new();
            for buffer in runtime.chain(index) {
                let Ok(buffer) = buffer else {
                    return NodeResult::drop();
                };
                packet.extend_from_slice(buffer.current());
            }
            match state.lock() {
                Ok(mut guard) => guard.packets.push(packet),
                Err(_) => return NodeResult::drop(),
            }
        }
        NodeResult::drop()
    }

    fn output_graph() -> (
        DataPlaneRuntime,
        Arc<Mutex<CaptureState>>,
        Arc<Mutex<CaptureState>>,
        NodeId,
    ) {
        let runtime =
            hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
                buffers: hammer_runtime::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_slots: 8,
                    ..hammer_runtime::DataPlaneBufferConfig::default()
                },
            });
        let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
        let drop_state = Arc::new(Mutex::new(CaptureState::default()));
        let lookup = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
        let drop = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
        let output = runtime.nodes().register_internal(TcpOutputNode::new());
        runtime
            .nodes()
            .set_node_next(output, TcpOutputNext::Drop, drop)
            .expect("wire TCP output drop");
        runtime
            .nodes()
            .set_node_next(output, TcpOutputNext::Lookup, lookup)
            .expect("wire TCP output lookup");
        (runtime, lookup_state, drop_state, output)
    }

    fn send_to_output(runtime: &DataPlaneRuntime, output: NodeId, index: Index) {
        let mut frame = runtime.buffers().get_next_frame(output).expect("frame");
        frame.push_index(index).expect("push index");
        runtime.put_next_frame(frame).expect("put next frame");
    }

    fn test_segment(payload_len: usize) -> TcpSegment {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:443".parse().expect("remote");
        TcpSegment::new(
            local,
            remote,
            100,
            200,
            4096,
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            TcpCapabilities::default(),
            None,
            None,
            None,
            None,
            payload_len,
        )
    }

    #[test]
    fn tcp_output_pushes_ipv4_with_df_then_routes_lookup() {
        // VPP tcp_output_push_ip → vlib_buffer_push_ip4(..., is_df=1):
        // tcp-output pushes IPv4 (always DF) in front of the TCP header, then lookup.
        let (runtime, lookup_state, drop_state, output) = output_graph();
        let index = runtime.alloc_index().expect("buffer");
        runtime.buffers().append(index, b"hello").expect("payload");
        test_segment(5)
            .write_to_buffer(runtime.buffers(), index)
            .expect("write segment");

        send_to_output(&runtime, output, index);
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        assert!(drop_state.lock().unwrap().packets.is_empty());
        let packets = &lookup_state.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        assert_eq!(packet[0], 0x45);
        assert_eq!(
            u16::from_be_bytes([packet[2], packet[3]]) as usize,
            packet.len()
        );
        assert!(u16::from_be_bytes([packet[6], packet[7]]) & 0x4000 != 0);
        assert_eq!(packet[8], 255); // VPP push_ip4 ttl
        assert_eq!(packet[9], 6); // TCP
        assert_eq!(&packet[12..16], &[192, 0, 2, 10]);
        assert_eq!(&packet[16..20], &[198, 51, 100, 20]);
        let tcp = &packet[20..];
        assert_eq!(&tcp[0..2], &[0xc3, 0x50]);
        assert_eq!(&tcp[2..4], &[0x01, 0xbb]);
        assert_eq!(&tcp[4..8], &[0, 0, 0, 100]);
        assert_eq!(&tcp[8..12], &[0, 0, 0, 200]);
        assert_eq!(tcp[12] >> 4, 5);
        assert_eq!(tcp[13] & TCP_FLAG_ACK, TCP_FLAG_ACK);
        assert_eq!(tcp[13] & TCP_FLAG_PSH, TCP_FLAG_PSH);
        assert_eq!(&tcp[14..16], &[0x10, 0x00]);
        assert_eq!(&tcp[20..], b"hello");
        assert_eq!(
            internet_checksum_parts(&[
                &packet[12..16],
                &packet[16..20],
                &[0, TCP_PROTOCOL],
                &(tcp.len() as u16).to_be_bytes(),
                tcp,
            ]),
            0
        );
    }

    #[test]
    fn tcp_output_checksums_payload_across_an_odd_buffer_chain_boundary() {
        let (runtime, lookup_state, drop_state, output) = output_graph();
        let head = runtime.alloc_index().expect("head buffer");
        runtime
            .buffers()
            .append(head, b"abc")
            .expect("head payload");
        test_segment(7)
            .write_to_buffer(runtime.buffers(), head)
            .expect("write segment");
        let tail = runtime
            .alloc_index_with_bytes(b"defg")
            .expect("tail buffer");
        runtime
            .buffers()
            .chain_buffer(head, tail)
            .expect("chain payload");

        send_to_output(&runtime, output, head);
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        assert!(drop_state.lock().unwrap().packets.is_empty());
        let packets = &lookup_state.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        assert_eq!(usize::from(u16::from_be_bytes([packet[2], packet[3]])), 47);
        assert_eq!(&packet[40..], b"abcdefg");
        assert_eq!(
            internet_checksum_parts(&[
                &packet[12..16],
                &packet[16..20],
                &[0, TCP_PROTOCOL],
                &27u16.to_be_bytes(),
                &packet[20..],
            ]),
            0
        );
    }

    #[test]
    fn tcp_output_checksums_ipv6_pseudo_header() {
        let (runtime, lookup_state, drop_state, output) = output_graph();
        let index = runtime.alloc_index().expect("buffer");
        runtime.buffers().append(index, b"hello").expect("payload");
        TcpSegment::new(
            "[2001:db8::10]:50000".parse().expect("local"),
            "[2001:db8::20]:443".parse().expect("remote"),
            100,
            200,
            4096,
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            TcpCapabilities::default(),
            None,
            None,
            None,
            None,
            5,
        )
        .write_to_buffer(runtime.buffers(), index)
        .expect("write segment");

        send_to_output(&runtime, output, index);
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        assert!(drop_state.lock().unwrap().packets.is_empty());
        let packets = &lookup_state.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        assert_eq!(packet[0] >> 4, 6);
        assert_eq!(u16::from_be_bytes([packet[4], packet[5]]), 25);
        assert_eq!(packet[6], TCP_PROTOCOL);
        assert_eq!(packet[7], 255);
        assert_eq!(
            internet_checksum_parts(&[
                &packet[8..24],
                &packet[24..40],
                &25u32.to_be_bytes(),
                &[0, 0, 0, TCP_PROTOCOL],
                &packet[40..],
            ]),
            0
        );
    }

    #[test]
    fn tcp_output_non_tcp_buffer_routes_drop() {
        let (runtime, lookup_state, drop_state, output) = output_graph();
        let index = runtime.alloc_index_with_bytes(b"hello").expect("buffer");

        send_to_output(&runtime, output, index);
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        assert!(lookup_state.lock().unwrap().packets.is_empty());
        assert_eq!(drop_state.lock().unwrap().packets.len(), 1);
    }
}

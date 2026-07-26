use std::mem::transmute;
use std::sync::{Arc, Mutex, OnceLock};

use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, Index, NodeId};
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
use hammer_plugin_tcp::{TcpResetNext, TcpResetNode};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, InternalNode, Node,
    NodeProcessFn, NodeResult, NodeRuntimeData,
};
use hammer_service::opaque::NetworkOpaque;

fn test_runtime_configured(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_slots: usize,
) -> DataPlaneRuntime {
    let config = DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity,
            buffer_slots,
            frame_slots,
            ..DataPlaneBufferConfig::default()
        },
    };
    DataPlaneRuntime::new(config)
}

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
}

struct CaptureNode {
    runtime_data: NodeRuntimeData,
}

impl CaptureNode {
    fn new(state: Arc<Mutex<CaptureState>>) -> Self {
        let mut states = capture_states().lock().expect("capture state registry");
        let slot = states.len();
        states.push(state);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("capture state slot"),
        }
    }
}

impl Node for CaptureNode {
    #[inline(always)]
    fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        capture_process
    }

    #[inline]
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
    let state = {
        let states = capture_states().lock().expect("capture state registry");
        Arc::clone(
            states
                .get(data.usize_word(0).expect("capture state slot"))
                .expect("capture state slot is invalid"),
        )
    };
    for index in frame.pending_indices().iter().copied() {
        let packet = runtime
            .get_buffer(index)
            .expect("capture buffer")
            .current()
            .to_vec();
        state.lock().expect("capture state").packets.push(packet);
    }
    NodeResult::drop()
}

#[test]
fn tcp_reset_ack_segment_replies_with_rst_only() {
    let (runtime, reset, lookup_state, drop_state) = reset_graph();
    let packet = ipv4_tcp_packet(0x10, 1_000, 9_000, &[]);
    schedule_packet(&runtime, reset, &packet);

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

    assert!(drop_state.lock().unwrap().packets.is_empty());
    let reply = lookup_state.lock().unwrap().packets.clone();
    assert_eq!(reply.len(), 1);
    let reply_tcp = etherparse::TcpSlice::from_slice(&reply[0][20..]).expect("parse reply");

    assert!(reply_tcp.rst());
    assert!(!reply_tcp.ack());
    assert_eq!(reply_tcp.sequence_number(), 9_000);
    assert_eq!(reply_tcp.acknowledgment_number(), 0);
    assert_eq!(reply_tcp.source_port(), 80);
    assert_eq!(reply_tcp.destination_port(), 50_000);
}

#[test]
fn tcp_reset_non_ack_segment_replies_with_rst_ack_using_sequence_space() {
    let (runtime, reset, lookup_state, drop_state) = reset_graph();
    let packet = ipv4_tcp_packet(0x02, 1_000, 0, b"hello");
    schedule_packet(&runtime, reset, &packet);

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

    assert!(drop_state.lock().unwrap().packets.is_empty());
    let reply = lookup_state.lock().unwrap().packets.clone();
    assert_eq!(reply.len(), 1);
    let reply_tcp = etherparse::TcpSlice::from_slice(&reply[0][20..]).expect("parse reply");

    assert!(reply_tcp.rst());
    assert!(reply_tcp.ack());
    assert_eq!(reply_tcp.sequence_number(), 0);
    assert_eq!(reply_tcp.acknowledgment_number(), 1_006);
}

#[test]
fn tcp_reset_fin_consumes_sequence_space() {
    let (runtime, reset, lookup_state, drop_state) = reset_graph();
    let packet = ipv4_tcp_packet(0x01, 4_000, 0, b"abc");
    schedule_packet(&runtime, reset, &packet);

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

    assert!(drop_state.lock().unwrap().packets.is_empty());
    let reply = lookup_state.lock().unwrap().packets.clone();
    assert_eq!(reply.len(), 1);
    let reply_tcp = etherparse::TcpSlice::from_slice(&reply[0][20..]).expect("parse reply");

    assert!(reply_tcp.rst());
    assert!(reply_tcp.ack());
    assert_eq!(reply_tcp.acknowledgment_number(), 4_004);
}

#[test]
fn tcp_reset_drops_existing_rst_segment() {
    let (runtime, reset, lookup_state, drop_state) = reset_graph();
    let packet = ipv4_tcp_packet(0x14, 1_000, 9_000, &[]);
    schedule_packet(&runtime, reset, &packet);

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

    assert!(lookup_state.lock().unwrap().packets.is_empty());
    assert_eq!(drop_state.lock().unwrap().packets.len(), 1);
}

#[test]
fn tcp_reset_reply_reverses_ip_tuple_and_addrs() {
    let (runtime, reset, lookup_state, drop_state) = reset_graph();
    let packet = ipv4_tcp_packet(0x10, 1_000, 9_000, &[]);
    schedule_packet(&runtime, reset, &packet);

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

    assert!(drop_state.lock().unwrap().packets.is_empty());
    let reply = lookup_state.lock().unwrap().packets.clone();
    assert_eq!(reply.len(), 1);

    assert_eq!(&reply[0][12..16], &[198, 51, 100, 2]);
    assert_eq!(&reply[0][16..20], &[192, 0, 2, 1]);
}

#[test]
fn tcp_reset_sets_ipv4_dont_fragment_when_pmtu_enabled() {
    let (runtime, reset, lookup_state, _) = reset_graph();
    let packet = ipv4_tcp_packet(0x10, 1_000, 9_000, &[]);
    schedule_packet(&runtime, reset, &packet);

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);

    let reply = lookup_state.lock().unwrap().packets.clone();
    assert_eq!(reply.len(), 1);
    let flags = u16::from_be_bytes([reply[0][6], reply[0][7]]);
    assert_eq!(flags & 0x4000, 0x4000);
}

fn reset_graph() -> (
    DataPlaneRuntime,
    NodeId,
    Arc<Mutex<CaptureState>>,
    Arc<Mutex<CaptureState>>,
) {
    let runtime = test_runtime_configured(512, 8, 4);
    let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
    let drop_state = Arc::new(Mutex::new(CaptureState::default()));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
    let reset = runtime.nodes().register_internal(TcpResetNode::new());
    runtime
        .nodes()
        .set_node_next(reset, TcpResetNext::Drop, drop)
        .expect("wire TCP reset drop");
    runtime
        .nodes()
        .set_node_next(reset, TcpResetNext::Lookup, lookup)
        .expect("wire TCP reset lookup");
    (runtime, reset, lookup_state, drop_state)
}

fn schedule_packet(runtime: &DataPlaneRuntime, reset: NodeId, packet: &[u8]) {
    let mut frame = runtime
        .buffers()
        .get_next_frame(reset)
        .expect("alloc frame");
    let index = runtime
        .alloc_index_with_bytes(packet)
        .expect("alloc packet");
    set_tcp_cursor(runtime, index, packet.len());
    frame.push_index(index).expect("push packet");
    runtime.put_next_frame(frame).expect("schedule reset");
}

fn set_tcp_cursor(runtime: &DataPlaneRuntime, index: Index, packet_len: usize) {
    let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.set_packet_cursor(
        BufferPacketCursor::new()
            .with_packet_len(packet_len)
            .with_network_header(0, 20)
            .with_transport_header(20, 20)
            .with_transport_payload_offset(40),
    );
}

fn ipv4_tcp_packet(flags: u8, sequence: u32, acknowledgment: u32, payload: &[u8]) -> Vec<u8> {
    let packet_len = 20 + 20 + payload.len();
    let total_len = u16::try_from(packet_len).expect("packet length fits");
    let mut packet = vec![0u8; packet_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&total_len.to_be_bytes());
    packet[8] = 64;
    packet[9] = 6;
    packet[12..16].copy_from_slice(&[192, 0, 2, 1]);
    packet[16..20].copy_from_slice(&[198, 51, 100, 2]);
    packet[20..22].copy_from_slice(&50_000u16.to_be_bytes());
    packet[22..24].copy_from_slice(&80u16.to_be_bytes());
    packet[24..28].copy_from_slice(&sequence.to_be_bytes());
    packet[28..32].copy_from_slice(&acknowledgment.to_be_bytes());
    packet[32] = 0x50;
    packet[33] = flags;
    packet[34..36].copy_from_slice(&4096u16.to_be_bytes());
    if !payload.is_empty() {
        packet[40..40 + payload.len()].copy_from_slice(payload);
    }
    let tcp_checksum = ipv4_l4_checksum([192, 0, 2, 1], [198, 51, 100, 2], 6, &packet[20..]);
    packet[36..38].copy_from_slice(&tcp_checksum.to_be_bytes());
    let ip_checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&ip_checksum.to_be_bytes());
    packet
}

fn ipv4_l4_checksum(source: [u8; 4], destination: [u8; 4], protocol: u8, segment: &[u8]) -> u16 {
    let segment_len = (segment.len() as u16).to_be_bytes();
    internet_checksum_parts(&[&source, &destination, &[0, protocol], &segment_len, segment])
}

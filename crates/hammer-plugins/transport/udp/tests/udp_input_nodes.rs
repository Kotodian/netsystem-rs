use std::mem::transmute;
use std::sync::{Arc, Mutex, OnceLock};

use hammer_core::data_plane::{BufferFrame, BufferNodeError, BufferPacketCursor};
use hammer_infra::checksum::internet_checksum_parts;
use hammer_plugin_udp::{UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputTrace};
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::RuntimeResult;
use hammer_runtime::graph::install_packet_graph;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, InternalNode, Node,
    NodeProcessFn, NodeResult, NodeRuntimeData, TraceControlPlane, TraceInputPolicy, TracePolicy,
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

#[test]
fn udp_graph_contribution_initializes_with_existing_drop_node() {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut engine = Engine::new(runtime, RuntimeRegistry::new());
    engine
        .plugin_main_mut()
        .register_builtin_image(hammer_service::registration_image());
    let plugin = hammer_plugin_udp::plugin_module();
    engine
        .plugin_main_mut()
        .register_builtin_image(plugin.registration_image().get());

    // The statically linked UDP image has no IP dependency image in this test,
    // so the full service graph may stop at an unresolved IP next. All image
    // contributions are installed before named-next resolution.
    _ = install_packet_graph(&mut engine);

    assert!(engine.runtime.node_by_name("drop").is_some());
    assert!(engine.runtime.node_by_name("udp-input").is_some());
}

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
    node_errors: Vec<Option<BufferNodeError>>,
    packet_cursors: Vec<BufferPacketCursor>,
}

struct CaptureNode {
    runtime_data: NodeRuntimeData,
}

impl CaptureNode {
    fn new(state: Arc<Mutex<CaptureState>>) -> Self {
        let mut states = capture_states()
            .lock()
            .expect("capture state registry poisoned");
        let slot = states.len();
        states.push(state);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("capture state slot"),
        }
    }
}

impl Node for CaptureNode {
    #[inline(always)]
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
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

fn chain_bytes(
    runtime: &DataPlaneRuntime,
    index: hammer_core::data_plane::Index,
) -> RuntimeResult<Vec<u8>> {
    let mut bytes = Vec::new();
    for buffer in runtime.buffers().chain(index) {
        bytes.extend_from_slice(buffer?.current());
    }
    Ok(bytes)
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
    for index in frame.pending_indices().iter().copied() {
        let packet = match chain_bytes(runtime, index) {
            Ok(bytes) => bytes,
            Err(_) => return NodeResult::drop(),
        };
        let node_error = match runtime.node_error(index) {
            Ok(err) => err,
            Err(_) => return NodeResult::drop(),
        };
        let packet_cursor = match runtime.get_buffer(index) {
            Ok(buffer) => {
                unsafe { transmute::<_, &NetworkOpaque>(buffer.opaque()) }.packet_cursor()
            }
            Err(_) => return NodeResult::drop(),
        };
        match state.lock() {
            Ok(mut guard) => {
                guard.packets.push(packet.into_iter().collect());
                guard.node_errors.push(node_error);
                guard.packet_cursors.push(packet_cursor);
            }
            Err(_) => return NodeResult::drop(),
        }
    }
    NodeResult::drop()
}

fn wire_udp_input(
    runtime: &DataPlaneRuntime,
) -> (
    UdpInputControlPlane,
    hammer_core::data_plane::NodeId,
    Arc<Mutex<CaptureState>>,
    Arc<Mutex<CaptureState>>,
    Arc<Mutex<CaptureState>>,
) {
    let drop_state = Arc::new(Mutex::new(CaptureState::default()));
    let punt_state = Arc::new(Mutex::new(CaptureState::default()));
    let icmp_state = Arc::new(Mutex::new(CaptureState::default()));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&punt_state)));
    let icmp_error = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&icmp_state)));
    let mut control =
        UdpInputControlPlane::new([drop, punt, icmp_error]).with_nodes(runtime.nodes().clone());
    let udp_input = runtime.nodes().register_internal(control.node());
    control
        .attach_consumer(udp_input)
        .expect("attach udp input");
    (control, udp_input, drop_state, punt_state, icmp_state)
}

#[test]
fn udp_input_dispatches_registered_port_by_local_slot() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let echo_state = Arc::new(Mutex::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    let echo_slot = control.register_port(53, echo).expect("register dns port");
    assert!(echo_slot >= UdpInputNext::COUNT as u16);

    let trace_control = TraceControlPlane::new(4);
    trace_control.publish(TracePolicy {
        enabled: true,
        record_capacity: 4,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: udp_input,
            count: 1,
        }]
        .into(),
    });
    runtime.set_trace_control(Some(trace_control.handle()), 2);

    let packet = ipv4_udp_packet(12_345, 53, b"dns");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_marked_packet(&runtime, &mut frame, udp_input, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.lock().unwrap().packets, vec![packet]);
    assert!(echo_state.lock().unwrap().node_errors[0].is_none());
    assert!(drop_state.lock().unwrap().packets.is_empty());
    assert!(punt_state.lock().unwrap().packets.is_empty());
    assert!(icmp_state.lock().unwrap().packets.is_empty());
    assert_eq!(trace_control.drain_completed(), 1);
    let records = trace_control.take_records();
    let entry = &records[0].entries[0];
    assert!(!entry.payload_bytes.is_empty());
    assert!(
        entry
            .format_payload()
            .starts_with(std::any::type_name::<UdpInputTrace>())
    );
}

#[test]
fn udp_input_owns_udp_cursor_initialization() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let echo_state = Arc::new(Mutex::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    control.register_port(53, echo).expect("register dns port");

    let packet = ipv4_udp_packet(12_345, 53, b"dns");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet_with_ip_cursor(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    let echo = echo_state.lock().expect("echo state");
    assert_eq!(echo.packets, vec![packet]);
    assert_eq!(echo.packet_cursors.len(), 1);
    assert_eq!(echo.packet_cursors[0].transport_header_len(), 8);
    assert_eq!(echo.packet_cursors[0].transport_payload_offset(), 28);
    assert!(drop_state.lock().unwrap().packets.is_empty());
    assert!(punt_state.lock().unwrap().packets.is_empty());
    assert!(icmp_state.lock().unwrap().packets.is_empty());
}

#[test]
fn udp_input_rejects_invalid_ipv4_checksum() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (_control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let mut packet = ipv4_udp_packet(12_345, 53, b"dns");
    set_ipv4_udp_checksum(&mut packet);
    packet[27] ^= 1;
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(drop_state.lock().unwrap().packets, vec![packet]);
    assert_eq!(
        drop_state.lock().unwrap().node_errors,
        vec![Some(BufferNodeError::new(
            udp_input,
            UdpInputError::BadChecksum.code()
        ))]
    );
    assert!(punt_state.lock().unwrap().packets.is_empty());
    assert!(icmp_state.lock().unwrap().packets.is_empty());
}

#[test]
fn udp_input_dispatches_valid_ipv4_checksum() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let echo_state = Arc::new(Mutex::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    control.register_port(53, echo).expect("register dns port");

    let mut packet = ipv4_udp_packet(12_345, 53, b"dns");
    set_ipv4_udp_checksum(&mut packet);
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.lock().unwrap().packets, vec![packet]);
    assert!(drop_state.lock().unwrap().packets.is_empty());
    assert!(punt_state.lock().unwrap().packets.is_empty());
    assert!(icmp_state.lock().unwrap().packets.is_empty());
}

#[test]
fn udp_input_rejects_missing_ipv6_checksum() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (_control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let packet = ipv6_udp_packet(12_345, 53, b"dns");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(drop_state.lock().unwrap().packets, vec![packet]);
    assert_eq!(
        drop_state.lock().unwrap().node_errors,
        vec![Some(BufferNodeError::new(
            udp_input,
            UdpInputError::BadChecksum.code()
        ))]
    );
    assert!(punt_state.lock().unwrap().packets.is_empty());
    assert!(icmp_state.lock().unwrap().packets.is_empty());
}

#[test]
fn udp_input_sends_unknown_port_to_icmp_error_next() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (_control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let packet = ipv4_udp_packet(12_345, 9_999, b"miss");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(drop_state.lock().unwrap().packets.is_empty());
    assert!(punt_state.lock().unwrap().packets.is_empty());
    assert_eq!(icmp_state.lock().unwrap().packets, vec![packet]);
    assert_eq!(
        icmp_state.lock().unwrap().node_errors,
        vec![Some(BufferNodeError::new(
            udp_input,
            UdpInputError::UnknownPort.code()
        ))]
    );
}

#[test]
fn udp_input_sends_punt_port_to_punt_next() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    control
        .register_punt_port(1234)
        .expect("register punt port");
    let packet = ipv4_udp_packet(1, 1234, b"punt");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(drop_state.lock().unwrap().packets.is_empty());
    assert!(icmp_state.lock().unwrap().packets.is_empty());
    assert_eq!(punt_state.lock().unwrap().packets, vec![packet]);
    assert!(punt_state.lock().unwrap().node_errors[0].is_none());
}

#[test]
fn udp_input_registers_more_than_sixteen_port_nexts() {
    let runtime = test_runtime_configured(2048, 64, 8);
    let (control, udp_input, _, _, _) = wire_udp_input(&runtime);
    let mut last_slot = UdpInputNext::COUNT as u16 - 1;
    let mut last_state = Arc::new(Mutex::new(CaptureState::default()));
    let mut last_port = 0u16;
    for port in 1..=20u16 {
        let state = Arc::new(Mutex::new(CaptureState::default()));
        let target = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&state)));
        let slot = control
            .register_port(port, target)
            .expect("register port next");
        assert_eq!(slot, last_slot + 1);
        last_slot = slot;
        last_state = state;
        last_port = port;
    }
    assert!(last_slot >= 16);

    let packet = ipv4_udp_packet(7, last_port, b"high-slot");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(last_state.lock().unwrap().packets, vec![packet]);
}

fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, packet: &[u8]) {
    let buffer = runtime
        .alloc_index_with_bytes(packet)
        .expect("alloc packet");
    set_udp_opaque(runtime, buffer, packet, 8);
    frame.push_index(buffer).expect("push packet");
}

fn push_packet_with_ip_cursor(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, packet: &[u8]) {
    let buffer = runtime
        .alloc_index_with_bytes(packet)
        .expect("alloc packet");
    set_udp_opaque(runtime, buffer, packet, 0);
    frame.push_index(buffer).expect("push packet");
}

fn push_marked_packet(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    trace_input: hammer_core::data_plane::NodeId,
    packet: &[u8],
) {
    let buffer = runtime
        .alloc_index_with_bytes(packet)
        .expect("alloc packet");
    runtime
        .try_mark_trace(trace_input, buffer)
        .expect("mark packet");
    set_udp_opaque(runtime, buffer, packet, 8);
    frame.push_index(buffer).expect("push packet");
}

fn set_udp_opaque(
    runtime: &DataPlaneRuntime,
    index: hammer_core::data_plane::Index,
    packet: &[u8],
    transport_header_len: usize,
) {
    let Some(first) = packet.first().copied() else {
        return;
    };
    let (cursor, version, protocol) = match first >> 4 {
        4 => {
            let ihl = usize::from(first & 0x0f) * 4;
            (
                BufferPacketCursor::new()
                    .with_packet_len(packet.len())
                    .with_network_header(0, ihl)
                    .with_transport_header(ihl, transport_header_len)
                    .with_transport_payload_offset(ihl + transport_header_len),
                4u8,
                packet.get(9).copied().unwrap_or(17),
            )
        }
        6 => (
            BufferPacketCursor::new()
                .with_packet_len(packet.len())
                .with_network_header(0, 40)
                .with_transport_header(40, transport_header_len)
                .with_transport_payload_offset(40 + transport_header_len),
            6u8,
            packet.get(6).copied().unwrap_or(17),
        ),
        _ => return,
    };
    let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
    let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
    network.set_packet_cursor(cursor);
    network.ip_mut().set_ip_version(Some(version));
    network.ip_mut().set_ip_protocol(Some(protocol));
}

fn ipv4_udp_packet(source_port: u16, destination_port: u16, payload: &[u8]) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&[10, 0, 0, 1]);
    packet[16..20].copy_from_slice(&[192, 0, 2, 1]);
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[28..].copy_from_slice(payload);
    packet
}

fn ipv6_udp_packet(source_port: u16, destination_port: u16, payload: &[u8]) -> Vec<u8> {
    let payload_len = 8 + payload.len();
    let mut packet = vec![0u8; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = 17;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    packet[24..40].copy_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]);
    packet[40..42].copy_from_slice(&source_port.to_be_bytes());
    packet[42..44].copy_from_slice(&destination_port.to_be_bytes());
    packet[44..46].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[48..].copy_from_slice(payload);
    packet
}

fn set_ipv4_udp_checksum(packet: &mut [u8]) {
    let (ip, udp) = packet.split_at_mut(20);
    udp[6..8].fill(0);
    let checksum = internet_checksum_parts(&[
        &ip[12..16],
        &ip[16..20],
        &[0, 17],
        &(udp.len() as u16).to_be_bytes(),
        udp,
    ]);
    udp[6..8].copy_from_slice(&checksum.to_be_bytes());
}

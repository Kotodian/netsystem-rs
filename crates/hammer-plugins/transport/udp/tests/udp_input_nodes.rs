use std::cell::UnsafeCell;
use std::mem::transmute;
use std::sync::Arc;

use hammer_core::data_plane::{BufferFrame, BufferNodeError, BufferPacketCursor};
use hammer_infra::checksum::internet_checksum_parts;
use hammer_plugin_udp::{
    UdpControlError, UdpInputControlPlane, UdpInputError, UdpInputNext, UdpInputTrace, UdpIpVersion,
};
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::RuntimeResult;
use hammer_runtime::graph::install_packet_graph;
use hammer_runtime::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, InternalNode, Node,
    NodeProcessFn, NodeResult, NodeRuntimeData, RuntimeError, TraceControlPlane, TraceInputPolicy,
    TracePolicy,
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

fn with_main_engine<R>(operation: impl FnOnce() -> R) -> R {
    let mut engine = Engine::new(
        DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
        RuntimeRegistry::new(),
    );
    engine.install_current();
    let result = operation();
    Engine::uninstall_current();
    result
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

#[test]
fn udp_plugin_exports_generic_root_metadata() {
    let module = hammer_plugin_udp::plugin_module();

    assert_eq!(module.metadata().name(), "udp");
}

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
    node_errors: Vec<Option<BufferNodeError>>,
    packet_cursors: Vec<BufferPacketCursor>,
}

struct CaptureCell {
    state: UnsafeCell<CaptureState>,
}

// SAFETY: these graph capture tests run on one thread. The node writes state
// during `run_ready_nodes`, then assertions read it after dispatch completes.
unsafe impl Send for CaptureCell {}
unsafe impl Sync for CaptureCell {}

impl CaptureCell {
    fn new(state: CaptureState) -> Arc<Self> {
        Arc::new(Self {
            state: UnsafeCell::new(state),
        })
    }

    fn borrow(&self) -> &CaptureState {
        unsafe { &*self.state.get() }
    }

    fn borrow_mut(&self) -> &mut CaptureState {
        unsafe { &mut *self.state.get() }
    }
}

struct CaptureNode {
    runtime_data: NodeRuntimeData,
    _state: Arc<CaptureCell>,
}

impl CaptureNode {
    fn new(state: Arc<CaptureCell>) -> Self {
        Self {
            runtime_data: NodeRuntimeData::from_usize(Arc::as_ptr(&state) as usize)
                .expect("capture state pointer"),
            _state: state,
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
    let pointer = match data.usize_word(0) {
        Ok(pointer) => pointer as *const CaptureCell,
        Err(_) => return NodeResult::drop(),
    };
    if pointer.is_null() {
        return NodeResult::drop();
    }
    // SAFETY: the CaptureNode owns `Arc<CaptureCell>` for the graph lifetime
    // and graph dispatch is synchronous in these tests.
    let state = unsafe { &*pointer };
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
        let guard = state.borrow_mut();
        guard.packets.push(packet.into_iter().collect());
        guard.node_errors.push(node_error);
        guard.packet_cursors.push(packet_cursor);
    }
    NodeResult::drop()
}

fn wire_udp_input(
    runtime: &DataPlaneRuntime,
) -> (
    UdpInputControlPlane,
    hammer_core::data_plane::NodeId,
    Arc<CaptureCell>,
    Arc<CaptureCell>,
    Arc<CaptureCell>,
) {
    let drop_state = CaptureCell::new(CaptureState::default());
    let punt_state = CaptureCell::new(CaptureState::default());
    let icmp_state = CaptureCell::new(CaptureState::default());
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&punt_state)));
    let icmp_error = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&icmp_state)));
    let mut control = UdpInputControlPlane::new().with_nodes(runtime.nodes().clone());
    let udp_input = runtime.nodes().register_internal(control.node());
    runtime
        .nodes()
        .set_node_next(udp_input, UdpInputNext::Drop, drop)
        .expect("wire UDP input drop");
    runtime
        .nodes()
        .set_node_next(udp_input, UdpInputNext::Punt, punt)
        .expect("wire UDP input punt");
    runtime
        .nodes()
        .set_node_next(udp_input, UdpInputNext::IcmpError, icmp_error)
        .expect("wire UDP input ICMP error");
    control
        .attach_consumer(udp_input)
        .expect("attach udp input");
    (control, udp_input, drop_state, punt_state, icmp_state)
}

#[test]
fn udp_input_dispatches_registered_port_by_local_slot() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let echo_state = CaptureCell::new(CaptureState::default());
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    let echo_slot =
        with_main_engine(|| control.register_port(53, echo).expect("register dns port"));
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
    runtime.set_trace_control(Some(trace_control.handle()));

    let packet = ipv4_udp_packet(12_345, 53, b"dns");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_marked_packet(&runtime, &mut frame, udp_input, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.borrow().packets, vec![packet]);
    assert!(echo_state.borrow().node_errors[0].is_none());
    assert!(drop_state.borrow().packets.is_empty());
    assert!(punt_state.borrow().packets.is_empty());
    assert!(icmp_state.borrow().packets.is_empty());
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
    let echo_state = CaptureCell::new(CaptureState::default());
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    with_main_engine(|| control.register_port(53, echo).expect("register dns port"));

    let packet = ipv4_udp_packet(12_345, 53, b"dns");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet_with_ip_cursor(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    let echo = echo_state.borrow();
    assert_eq!(echo.packets, vec![packet]);
    assert_eq!(echo.packet_cursors.len(), 1);
    assert_eq!(echo.packet_cursors[0].transport_header_len(), 8);
    assert_eq!(echo.packet_cursors[0].transport_payload_offset(), 28);
    assert!(drop_state.borrow().packets.is_empty());
    assert!(punt_state.borrow().packets.is_empty());
    assert!(icmp_state.borrow().packets.is_empty());
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
    assert_eq!(drop_state.borrow().packets, vec![packet]);
    assert_eq!(
        drop_state.borrow().node_errors,
        vec![Some(BufferNodeError::new(
            udp_input,
            UdpInputError::BadChecksum.code()
        ))]
    );
    assert!(punt_state.borrow().packets.is_empty());
    assert!(icmp_state.borrow().packets.is_empty());
}

#[test]
fn udp_input_dispatches_valid_ipv4_checksum() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let echo_state = CaptureCell::new(CaptureState::default());
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    with_main_engine(|| control.register_port(53, echo).expect("register dns port"));

    let mut packet = ipv4_udp_packet(12_345, 53, b"dns");
    set_ipv4_udp_checksum(&mut packet);
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.borrow().packets, vec![packet]);
    assert!(drop_state.borrow().packets.is_empty());
    assert!(punt_state.borrow().packets.is_empty());
    assert!(icmp_state.borrow().packets.is_empty());
}

#[test]
fn udp_input_dispatches_registered_ipv6_port_by_local_slot() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (control, udp_input, drop_state, punt_state, icmp_state) = wire_udp_input(&runtime);
    let echo_state = CaptureCell::new(CaptureState::default());
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    with_main_engine(|| {
        control
            .register_dst_port(UdpIpVersion::V6, 53, echo)
            .expect("register IPv6 DNS port");
    });

    let mut packet = ipv6_udp_packet(12_345, 53, b"dns");
    set_ipv6_udp_checksum(&mut packet);
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.borrow().packets, vec![packet]);
    assert!(drop_state.borrow().packets.is_empty());
    assert!(punt_state.borrow().packets.is_empty());
    assert!(icmp_state.borrow().packets.is_empty());
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
    assert_eq!(drop_state.borrow().packets, vec![packet]);
    assert_eq!(
        drop_state.borrow().node_errors,
        vec![Some(BufferNodeError::new(
            udp_input,
            UdpInputError::BadChecksum.code()
        ))]
    );
    assert!(punt_state.borrow().packets.is_empty());
    assert!(icmp_state.borrow().packets.is_empty());
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
    assert!(drop_state.borrow().packets.is_empty());
    assert!(punt_state.borrow().packets.is_empty());
    assert_eq!(icmp_state.borrow().packets, vec![packet]);
    assert_eq!(
        icmp_state.borrow().node_errors,
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
    with_main_engine(|| {
        control
            .register_punt_port(1234)
            .expect("register punt port");
    });
    let packet = ipv4_udp_packet(1, 1234, b"punt");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(drop_state.borrow().packets.is_empty());
    assert!(icmp_state.borrow().packets.is_empty());
    assert_eq!(punt_state.borrow().packets, vec![packet]);
    assert!(punt_state.borrow().node_errors[0].is_none());
}

#[test]
fn udp_input_registers_more_than_sixteen_port_nexts() {
    let runtime = test_runtime_configured(2048, 64, 8);
    let (control, udp_input, _, _, _) = wire_udp_input(&runtime);
    let mut last_slot = UdpInputNext::COUNT as u16 - 1;
    let mut last_state = CaptureCell::new(CaptureState::default());
    let mut last_port = 0u16;
    with_main_engine(|| {
        for port in 1..=20u16 {
            let state = CaptureCell::new(CaptureState::default());
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
    });
    assert!(last_slot >= 16);

    let packet = ipv4_udp_packet(7, last_port, b"high-slot");
    let mut frame = runtime
        .buffers()
        .get_next_frame(udp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);
    runtime.put_next_frame(frame).expect("schedule");
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(last_state.borrow().packets, vec![packet]);
}

#[test]
fn udp_port_registration_scopes_ownership_by_address_family() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let (control, _udp_input, _, _, _) = wire_udp_input(&runtime);
    let first = runtime
        .nodes()
        .register_internal(CaptureNode::new(CaptureCell::new(CaptureState::default())));
    let second = runtime
        .nodes()
        .register_internal(CaptureNode::new(CaptureCell::new(CaptureState::default())));

    with_main_engine(|| {
        let v4_slot = control
            .register_dst_port(UdpIpVersion::V4, 443, first)
            .expect("register IPv4 port");
        let v4_shared_slot = control
            .register_dst_port(UdpIpVersion::V4, 443, first)
            .expect("share IPv4 port");
        assert_eq!(v4_shared_slot, v4_slot);

        let v6_slot = control
            .register_dst_port(UdpIpVersion::V6, 443, second)
            .expect("register IPv6 port");
        assert_ne!(v6_slot, v4_slot);

        let error = control
            .register_dst_port(UdpIpVersion::V4, 443, second)
            .expect_err("a different IPv4 owner must conflict");
        let RuntimeError::Subsystem { source, .. } = error else {
            panic!("expected UDP subsystem error");
        };
        let Some(error) = source.downcast_ref::<UdpControlError>() else {
            panic!("expected typed UDP registration error");
        };
        assert!(matches!(
            error,
            UdpControlError::PortConflict {
                version: UdpIpVersion::V4,
                port: 443,
                owner,
                requested_owner,
            } if *owner == first && *requested_owner == second
        ));
    });
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

fn set_ipv6_udp_checksum(packet: &mut [u8]) {
    let (ip, udp) = packet.split_at_mut(40);
    udp[6..8].fill(0);
    let checksum = internet_checksum_parts(&[
        &ip[8..24],
        &ip[24..40],
        &(udp.len() as u32).to_be_bytes(),
        &[0, 0, 0, 17],
        udp,
    ]);
    udp[6..8].copy_from_slice(&checksum.to_be_bytes());
}

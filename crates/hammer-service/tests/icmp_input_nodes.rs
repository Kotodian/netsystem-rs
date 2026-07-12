use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_core::data_plane::{
    BufferFrame, BufferNodeError, BufferPacketCursor, DataPlaneBufferConfig,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
use hammer_infra::vec::Vec as InfraVec;
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, InternalNode, Node, NodeProcessFn, NodeResult,
    NodeRuntimeData, TraceControlPlane, TraceInputPolicy, TracePolicy,
};
use hammer_service::net::{
    IcmpInputControlPlane, IcmpInputError, IcmpInputTrace, IpVersion, NetworkOpaque,
};
use std::mem::transmute;

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
    node_errors: Vec<Option<BufferNodeError>>,
    frame_lens: Vec<usize>,
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
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for CaptureNode {}

fn chain_bytes(
    runtime: &DataPlaneRuntime,
    index: hammer_core::data_plane::Index,
) -> CoreResult<InfraVec<u8>> {
    let mut bytes = InfraVec::new();
    for buffer in runtime.buffers().chain(index) {
        bytes.extend_from_slice(buffer?.current());
    }
    Ok(bytes)
}

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
    match state.lock() {
        Ok(mut guard) => guard.frame_lens.push(frame.pending_len()),
        Err(_) => return NodeResult::drop(),
    }
    for index in frame.pending_indices().iter().copied() {
        let packet = match chain_bytes(runtime, index) {
            Ok(bytes) => bytes,
            Err(_) => return NodeResult::drop(),
        };
        let node_error = match runtime.node_error(index) {
            Ok(err) => err,
            Err(_) => return NodeResult::drop(),
        };
        match state.lock() {
            Ok(mut guard) => {
                guard.packets.push(packet.into_iter().collect());
                guard.node_errors.push(node_error);
            }
            Err(_) => return NodeResult::drop(),
        }
    }
    NodeResult::drop()
}

#[test]
fn icmp_input_dispatches_ipv4_echo_request_by_type() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let echo_state = Arc::new(Mutex::new(CaptureState::default()));
    let punt_state = Arc::new(Mutex::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&punt_state)));
    let control = IcmpInputControlPlane::new(punt);
    control
        .register_type(IpVersion::V4, 8, echo)
        .expect("register echo request");
    let icmp_input = runtime.nodes().register_internal(control.node());
    let trace_control = TraceControlPlane::new(4);
    trace_control.publish(TracePolicy {
        enabled: true,
        record_capacity: 4,
        packet_capacity: 2,
        inputs: vec![TraceInputPolicy {
            node: icmp_input,
            count: 1,
        }]
        .into(),
    });
    runtime.set_trace_control(Some(trace_control.handle()), 2);
    let packet = ipv4_icmp_packet(8, 0, b"echo4");
    let mut frame = runtime
        .buffers()
        .get_next_frame(icmp_input)
        .expect("alloc frame");
    push_marked_packet(&runtime, &mut frame, icmp_input, &packet);

    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.lock().unwrap().packets, vec![packet]);
    assert!(echo_state.lock().unwrap().node_errors[0].is_none());
    assert!(punt_state.lock().unwrap().packets.is_empty());
    assert_eq!(trace_control.drain_completed(), 1);
    let records = trace_control.take_records();
    let trace =
        IcmpInputTrace::decode(&records[0].entries[0].payload_bytes).expect("icmp input trace");
    assert_eq!(trace.version, Some(IpVersion::V4));
    assert_eq!(trace.icmp_type, Some(8));
    assert_eq!(trace.code, Some(0));
    assert_eq!(trace.error, None);
    assert_eq!(trace.next, echo);
}

#[test]
fn icmp_input_dispatches_ipv6_echo_request_by_type() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let echo_state = Arc::new(Mutex::new(CaptureState::default()));
    let punt_state = Arc::new(Mutex::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&punt_state)));
    let control = IcmpInputControlPlane::new(punt);
    control
        .register_type(IpVersion::V6, 128, echo)
        .expect("register echo request");
    let icmp_input = runtime.nodes().register_internal(control.node());
    let packet = ipv6_icmp_packet(128, 0, b"echo6");
    let mut frame = runtime
        .buffers()
        .get_next_frame(icmp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);

    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.lock().unwrap().packets, vec![packet]);
    assert!(echo_state.lock().unwrap().node_errors[0].is_none());
    assert!(punt_state.lock().unwrap().packets.is_empty());
}

#[test]
fn icmp_input_sends_unknown_ipv4_type_to_default_next() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let punt_state = Arc::new(Mutex::new(CaptureState::default()));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&punt_state)));
    let control = IcmpInputControlPlane::new(punt);
    let icmp_input = runtime.nodes().register_internal(control.node());
    let packet = ipv4_icmp_packet(13, 0, b"timestamp");
    let mut frame = runtime
        .buffers()
        .get_next_frame(icmp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);

    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(punt_state.lock().unwrap().packets, vec![packet]);
    assert_eq!(
        punt_state.lock().unwrap().node_errors,
        vec![Some(BufferNodeError::new(
            icmp_input,
            IcmpInputError::UnknownType.code()
        ))]
    );
}

#[test]
fn icmp_input_rejects_ipv6_echo_request_with_nonzero_code() {
    let runtime = test_runtime_configured(2048, 16, 8);
    let echo_state = Arc::new(Mutex::new(CaptureState::default()));
    let punt_state = Arc::new(Mutex::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&echo_state)));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&punt_state)));
    let control = IcmpInputControlPlane::new(punt);
    control
        .register_type(IpVersion::V6, 128, echo)
        .expect("register echo request");
    let icmp_input = runtime.nodes().register_internal(control.node());
    let packet = ipv6_icmp_packet(128, 1, b"bad-code");
    let mut frame = runtime
        .buffers()
        .get_next_frame(icmp_input)
        .expect("alloc frame");
    push_packet(&runtime, &mut frame, &packet);

    runtime.put_next_frame(frame).expect("schedule");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(echo_state.lock().unwrap().packets.is_empty());
    assert_eq!(punt_state.lock().unwrap().packets, vec![packet]);
    assert_eq!(
        punt_state.lock().unwrap().node_errors,
        vec![Some(BufferNodeError::new(
            icmp_input,
            IcmpInputError::BadCode.code()
        ))]
    );
}

fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, packet: &[u8]) {
    let buffer = runtime
        .alloc_index_with_bytes(packet)
        .expect("alloc packet");
    set_ip_cursor(runtime, buffer, packet);
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
    set_ip_cursor(runtime, buffer, packet);
    frame.push_index(buffer).expect("push packet");
}

fn set_ip_cursor(
    runtime: &DataPlaneRuntime,
    index: hammer_core::data_plane::Index,
    packet: &[u8],
) {
    let Some(first) = packet.first().copied() else {
        return;
    };
    let cursor = match first >> 4 {
        4 => {
            let ihl = usize::from(first & 0x0f) * 4;
            BufferPacketCursor::new()
                .with_packet_len(packet.len())
                .with_network_header(0, ihl)
                .with_transport_header(ihl, 8)
                .with_transport_payload_offset(ihl + 8)
        }
        6 => BufferPacketCursor::new()
            .with_packet_len(packet.len())
            .with_network_header(0, 40)
            .with_transport_header(40, 8)
            .with_transport_payload_offset(48),
        _ => return,
    };
    let mut buffer = runtime.get_buffer_mut(index).expect("buffer");
    unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) }.set_packet_cursor(cursor);
}

fn ipv4_icmp_packet(icmp_type: u8, code: u8, payload: &[u8]) -> Vec<u8> {
    let mut packet = ipv4_packet(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 0, 2, 1),
        1,
        8 + payload.len(),
    );
    let icmp = 20;
    packet[icmp] = icmp_type;
    packet[icmp + 1] = code;
    packet[icmp + 4..icmp + 6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[icmp + 6..icmp + 8].copy_from_slice(&1u16.to_be_bytes());
    packet[icmp + 8..].copy_from_slice(payload);
    let checksum = internet_checksum(&packet[icmp..]);
    packet[icmp + 2..icmp + 4].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv6_icmp_packet(icmp_type: u8, code: u8, payload: &[u8]) -> Vec<u8> {
    let source = "2001:db8::1".parse().expect("source");
    let destination = "2001:db8::2".parse().expect("destination");
    let mut packet = ipv6_packet(source, destination, 58, 8 + payload.len());
    let icmp = 40;
    packet[icmp] = icmp_type;
    packet[icmp + 1] = code;
    packet[icmp + 4..icmp + 6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[icmp + 6..icmp + 8].copy_from_slice(&1u16.to_be_bytes());
    packet[icmp + 8..].copy_from_slice(payload);
    let checksum = ipv6_l4_checksum(source, destination, 58, &packet[icmp..]);
    packet[icmp + 2..icmp + 4].copy_from_slice(&checksum.to_be_bytes());
    packet
}

fn ipv4_packet(
    source: Ipv4Addr,
    destination: Ipv4Addr,
    protocol: u8,
    payload_len: usize,
) -> Vec<u8> {
    let total_len = 20 + payload_len;
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[8] = 64;
    packet[9] = protocol;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet
}

fn ipv6_packet(
    source: Ipv6Addr,
    destination: Ipv6Addr,
    protocol: u8,
    payload_len: usize,
) -> Vec<u8> {
    let mut packet = vec![0u8; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = protocol;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    packet
}

fn update_ipv4_header_checksum(packet: &mut [u8]) {
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    internet_checksum_parts(&[
        &source.octets(),
        &destination.octets(),
        &(segment.len() as u32).to_be_bytes(),
        &[0, 0, 0, protocol],
        segment,
    ])
}

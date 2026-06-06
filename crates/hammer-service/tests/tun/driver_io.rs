use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
    RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::vec::Vec;
use hammer_service::tun::{
    DeviceMain, DriverScheduleMode, RealTunInput, RealTunOutput, ScriptedTunIo,
    TunBufferSendResult, TunDeviceEventSource, TunInputDriverNode, TunOutputDriverNode,
};

struct CaptureNode {
    runtime_data: NodeRuntimeData,
}

struct CaptureState {
    metadata: Arc<Mutex<Vec<RouteMetadata>>>,
    packets: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl CaptureNode {
    fn new(metadata: Arc<Mutex<Vec<RouteMetadata>>>, packets: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
        let mut states = capture_states().lock().expect("capture state poisoned");
        let slot = states.len();
        states.push(CaptureState {
            metadata: Arc::clone(&metadata),
            packets: Arc::clone(&packets),
        });
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("capture runtime data"),
        }
    }
}

fn capture_states() -> &'static Mutex<Vec<CaptureState>> {
    static STATES: OnceLock<Mutex<Vec<CaptureState>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

impl Node for CaptureNode {
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "capture node must run through descriptor process",
        ))
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

fn capture_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let slot = data.usize_word(0)?;
    let (metadata, packets) = {
        let states = capture_states()
            .lock()
            .map_err(|_| CoreError::internal("capture state poisoned"))?;
        let state = states
            .get(slot)
            .ok_or_else(|| CoreError::internal("capture state slot is invalid"))?;
        (Arc::clone(&state.metadata), Arc::clone(&state.packets))
    };
    for index in frame.drain_pending() {
        metadata
            .lock()
            .expect("capture metadata poisoned")
            .push(runtime.metadata(index)?);
        packets
            .lock()
            .expect("capture packets poisoned")
            .push(runtime.current(index)?);
        runtime.free_index(index);
    }
    Ok(NodeResult::drop())
}

#[test]
fn readiness_schedules_only_and_input_driver_reads_packets() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let io = ScriptedTunIo::default();
    let packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"rx");
    io.inject(packet.clone());

    let metadata = Arc::new(Mutex::new(Vec::new()));
    let packets = Arc::new(Mutex::new(Vec::new()));
    let capture = runtime.nodes().register_internal(CaptureNode::new(
        Arc::clone(&metadata),
        Arc::clone(&packets),
    ));
    let device_main = DeviceMain::new();
    let input_node = TunInputDriverNode::new(RealTunInput::new(io.clone()), "utun-test", capture)
        .with_interface_index(42);
    let input = runtime.nodes().register_driver(input_node.clone());
    let rx_queue = device_main.register_rx_queue(input, DriverScheduleMode::Interrupt);
    input_node
        .bind_rx_queue(device_main.clone(), rx_queue)
        .expect("bind RX queue");
    let readiness = TunDeviceEventSource::input(device_main.clone(), rx_queue);

    runtime
        .schedule_driver_frame(input, runtime.alloc_frame_index().expect("empty frame"))
        .expect("schedule input without interrupt");
    assert_eq!(runtime.run_ready_nodes().expect("run no-pending input"), 1);
    assert_eq!(io.recv_calls(), 0);

    readiness
        .schedule_readable(&runtime)
        .expect("schedule readable");

    assert_eq!(io.recv_calls(), 0);
    assert!(
        metadata
            .lock()
            .expect("capture metadata poisoned")
            .is_empty()
    );

    assert_eq!(runtime.run_ready_nodes().expect("run input"), 2);
    assert_eq!(io.recv_calls(), 2);
    assert_eq!(
        &*packets.lock().expect("capture packets poisoned"),
        &[packet]
    );
    let guard = metadata.lock().expect("capture metadata poisoned");
    let first = &guard[0];
    assert_eq!(first.inbound, "utun-test");
    assert_eq!(first.ingress_interface, Some(42));
    assert_eq!(
        first.source,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 12345))
    );
    assert_eq!(
        first.destination,
        Some(SocksAddr::ip(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)),
            53
        ))
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn output_driver_writes_packets_and_writable_readiness_drains_pending_tx() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let io = ScriptedTunIo::default();
    io.push_send_result(TunBufferSendResult::Backpressure);
    io.push_send_result(TunBufferSendResult::Complete);
    let packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"tx");
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(RealTunOutput::new(io.clone())));
    let device_main = DeviceMain::new();
    let tx_queue = device_main.register_tx_queue(output);
    let readiness = TunDeviceEventSource::output(device_main, tx_queue);
    let frame = runtime.alloc_frame_index().expect("alloc output frame");
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), &packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("get frame")
        .push_index(index)
        .expect("push packet");

    runtime
        .schedule_driver_frame(output, frame)
        .expect("schedule output");

    assert_eq!(runtime.run_ready_nodes().expect("run output"), 1);
    assert_eq!(io.send_calls(), 1);
    assert!(io.sent().is_empty());
    assert_eq!(runtime.in_use_buffers(), 1);

    readiness
        .schedule_writable(&runtime)
        .expect("schedule writable");

    assert_eq!(io.send_calls(), 1);

    assert_eq!(runtime.run_ready_nodes().expect("run writable"), 1);
    assert_eq!(io.send_calls(), 2);
    let mut expected = Vec::new();
    expected.push(packet);
    assert_eq!(io.sent(), expected);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn input_driver_rejects_rx_packet_that_fills_entire_buffer() {
    let runtime = DataPlaneRuntime::with_capacities(40, 8, 8, 4);
    let io = ScriptedTunIo::default();
    io.inject((0..40).collect());

    let capture = runtime.nodes().register_internal(CaptureNode::new(
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(Mutex::new(Vec::new())),
    ));
    let device_main = DeviceMain::new();
    let input_node = TunInputDriverNode::new(RealTunInput::new(io.clone()), "utun-test", capture);
    let input = runtime.nodes().register_driver(input_node.clone());
    let rx_queue = device_main.register_rx_queue(input, DriverScheduleMode::Interrupt);
    input_node
        .bind_rx_queue(device_main.clone(), rx_queue)
        .expect("bind RX queue");
    let readiness = TunDeviceEventSource::input(device_main, rx_queue);

    readiness
        .schedule_readable(&runtime)
        .expect("schedule readable");
    let err = runtime
        .run_ready_nodes()
        .expect_err("full receive tail must fail");

    assert!(err.to_string().contains("possible truncation"), "{err}");
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn output_driver_drains_pending_tx_before_ring_capacity_admission() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let io = ScriptedTunIo::default();
    io.push_send_result(TunBufferSendResult::Backpressure);
    io.push_send_result(TunBufferSendResult::Complete);
    io.push_send_result(TunBufferSendResult::Complete);
    let first_packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"first");
    let second_packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 8], b"second");
    let output = runtime.nodes().register_driver(TunOutputDriverNode::new(
        RealTunOutput::new(io.clone()).with_tx_ring_capacity(1),
    ));

    schedule_output_packet(&runtime, output, &first_packet);
    assert_eq!(runtime.run_ready_nodes().expect("run first output"), 1);
    assert_eq!(io.send_calls(), 1);
    assert!(io.sent().is_empty());
    assert_eq!(runtime.in_use_buffers(), 1);

    schedule_output_packet(&runtime, output, &second_packet);
    assert_eq!(runtime.run_ready_nodes().expect("run second output"), 1);

    assert_eq!(io.send_calls(), 3);
    assert_eq!(io.sent(), vec![first_packet, second_packet]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn output_driver_keeps_partial_packet_in_tx_ring_until_writable() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let io = ScriptedTunIo::default();
    io.push_send_result(TunBufferSendResult::Partial(12));
    io.push_send_result(TunBufferSendResult::Complete);
    let packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"partial-tx");
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(RealTunOutput::new(io.clone())));
    let device_main = DeviceMain::new();
    let tx_queue = device_main.register_tx_queue(output);
    let readiness = TunDeviceEventSource::output(device_main, tx_queue);
    let frame = runtime.alloc_frame_index().expect("alloc output frame");
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), &packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("get frame")
        .push_index(index)
        .expect("push packet");

    runtime
        .schedule_driver_frame(output, frame)
        .expect("schedule output");

    assert_eq!(runtime.run_ready_nodes().expect("run output"), 1);
    assert_eq!(io.send_calls(), 1);
    assert_eq!(io.sent().len(), 1);
    assert_eq!(&io.sent()[0], &packet[..12]);
    assert_eq!(runtime.in_use_buffers(), 1);

    readiness
        .schedule_writable(&runtime)
        .expect("schedule writable");
    assert_eq!(runtime.run_ready_nodes().expect("run writable"), 1);

    assert_eq!(io.send_calls(), 2);
    assert_eq!(io.sent().len(), 2);
    assert_eq!(&io.sent()[1], &packet[12..]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn output_driver_sends_all_segments_of_chained_packet() {
    let runtime = DataPlaneRuntime::with_capacities(1, 64, 8, 4);
    let io = ScriptedTunIo::default();
    let packet: Vec<u8> = (0..40).collect();
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(RealTunOutput::new(io.clone())));

    schedule_output_packet(&runtime, output, &packet);
    assert_eq!(runtime.run_ready_nodes().expect("run output"), 1);

    assert_eq!(io.send_calls(), 1);
    assert_eq!(io.sent_segment_counts(), vec![40]);
    assert_eq!(io.sent(), vec![packet]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn output_driver_rejects_non_advancing_partial_offset() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let io = ScriptedTunIo::default();
    io.push_send_result(TunBufferSendResult::Partial(0));
    let packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"partial-tx");
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(RealTunOutput::new(io.clone())));

    schedule_output_packet(&runtime, output, &packet);
    let err = runtime
        .run_ready_nodes()
        .expect_err("non-advancing partial offset must fail");

    assert!(
        err.to_string().contains("non-advancing TUN TX partial"),
        "{err}"
    );
    assert_eq!(io.send_calls(), 1);
    assert_eq!(runtime.in_use_buffers(), 1);
}

#[test]
fn output_driver_rejects_out_of_range_partial_offset() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 8, 8, 4);
    let io = ScriptedTunIo::default();
    let packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"partial-tx");
    io.push_send_result(TunBufferSendResult::Partial(packet.len() + 1));
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(RealTunOutput::new(io.clone())));

    schedule_output_packet(&runtime, output, &packet);
    let err = runtime
        .run_ready_nodes()
        .expect_err("out-of-range partial offset must fail");

    assert!(err.to_string().contains("exceeds packet length"), "{err}");
    assert_eq!(io.send_calls(), 1);
    assert_eq!(runtime.in_use_buffers(), 1);
}

fn schedule_output_packet(
    runtime: &DataPlaneRuntime,
    output: hammer_adapter::NodeId,
    packet: &[u8],
) {
    let frame = runtime.alloc_frame_index().expect("alloc output frame");
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("get frame")
        .push_index(index)
        .expect("push packet");
    runtime
        .schedule_driver_frame(output, frame)
        .expect("schedule output");
}

fn ipv4_udp_packet(source: [u8; 4], destination: [u8; 4], payload: &[u8]) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = Vec::with_capacity(total_len);
    packet.extend_from_slice(&[
        0x45,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        64,
        17,
        0,
        0,
        source[0],
        source[1],
        source[2],
        source[3],
        destination[0],
        destination[1],
        destination[2],
        destination[3],
    ]);
    packet.extend_from_slice(&[0x30, 0x39, 0, 53, 0, 0, 0, 0]);
    packet.extend_from_slice(payload);
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet
}

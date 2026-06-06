use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr};
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeResult, RouteMetadata, SocksAddr,
};
use hammer_core::error::CoreResult;
use hammer_infra::vec::Vec;
use hammer_service::tun::{
    RealTunInput, RealTunOutput, TunBufferIo, TunBufferSendResult, TunFdReadiness,
    TunInputDriverNode, TunOutputDriverNode,
};

#[derive(Default)]
struct FakeTunFdIo {
    rx: Vec<Vec<u8>>,
    rx_head: usize,
    recv_calls: usize,
    send_calls: usize,
    send_results: Vec<TunBufferSendResult>,
    send_result_head: usize,
    sent: Vec<Vec<u8>>,
    sent_segment_counts: Vec<usize>,
}

#[derive(Clone, Default)]
struct SharedTunFdIo(Rc<RefCell<FakeTunFdIo>>);

impl SharedTunFdIo {
    fn inject(&self, packet: Vec<u8>) {
        self.0.borrow_mut().rx.push(packet);
    }

    fn push_send_result(&self, result: TunBufferSendResult) {
        self.0.borrow_mut().send_results.push(result);
    }

    fn recv_calls(&self) -> usize {
        self.0.borrow().recv_calls
    }

    fn send_calls(&self) -> usize {
        self.0.borrow().send_calls
    }

    fn sent(&self) -> Vec<Vec<u8>> {
        self.0.borrow().sent.clone()
    }

    fn sent_segment_counts(&self) -> Vec<usize> {
        self.0.borrow().sent_segment_counts.clone()
    }
}

impl TunBufferIo for SharedTunFdIo {
    fn try_recv_buffer(&mut self, buffer: &mut [u8]) -> CoreResult<Option<usize>> {
        let mut inner = self.0.borrow_mut();
        inner.recv_calls += 1;
        if inner.rx_head >= inner.rx.len() {
            return Ok(None);
        }
        let packet = &inner.rx[inner.rx_head];
        let len = packet.len().min(buffer.len());
        buffer[..len].copy_from_slice(&packet[..len]);
        inner.rx_head += 1;
        Ok(Some(len))
    }

    fn try_send_buffer(&mut self, packet: &[u8], offset: usize) -> CoreResult<TunBufferSendResult> {
        self.record_send(&[packet], offset, packet.len())
    }

    fn try_send_buffers(
        &mut self,
        segments: &[&[u8]],
        offset: usize,
        total_len: usize,
    ) -> CoreResult<TunBufferSendResult> {
        self.record_send(segments, offset, total_len)
    }
}

impl SharedTunFdIo {
    fn record_send(
        &mut self,
        segments: &[&[u8]],
        offset: usize,
        total_len: usize,
    ) -> CoreResult<TunBufferSendResult> {
        let mut inner = self.0.borrow_mut();
        inner.send_calls += 1;
        let result = if inner.send_result_head < inner.send_results.len() {
            let result = inner.send_results[inner.send_result_head];
            inner.send_result_head += 1;
            result
        } else {
            TunBufferSendResult::Complete
        };
        inner.sent_segment_counts.push(segments.len());
        match result {
            TunBufferSendResult::Complete => {
                let mut sent = Vec::with_capacity(total_len.saturating_sub(offset));
                extend_segments_from_offset(&mut sent, segments, offset, total_len);
                inner.sent.push(sent);
            }
            TunBufferSendResult::Partial(next_offset) => {
                let take = next_offset.saturating_sub(offset).min(total_len - offset);
                let mut sent = Vec::with_capacity(take);
                extend_segments_from_offset(&mut sent, segments, offset, offset + take);
                inner.sent.push(sent);
            }
            TunBufferSendResult::Backpressure => {}
        }
        Ok(result)
    }
}

fn extend_segments_from_offset(
    out: &mut Vec<u8>,
    segments: &[&[u8]],
    offset: usize,
    end_offset: usize,
) {
    let mut base = 0usize;
    for segment in segments {
        let end = base + segment.len();
        if offset < end && base < end_offset {
            let start_in_segment = offset.saturating_sub(base);
            let end_in_segment = (end_offset - base).min(segment.len());
            out.extend_from_slice(&segment[start_in_segment..end_in_segment]);
        }
        base = end;
    }
}

struct CaptureNode {
    metadata: Rc<RefCell<Vec<RouteMetadata>>>,
    packets: Rc<RefCell<Vec<Vec<u8>>>>,
}

impl Node<TestNode> for CaptureNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.drain_pending() {
            self.metadata.borrow_mut().push(runtime.metadata(index)?);
            self.packets.borrow_mut().push(runtime.current(index)?);
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for CaptureNode {}

enum TestNode {
    TunInput(TunInputDriverNode<RealTunInput<SharedTunFdIo>>),
    TunOutput(TunOutputDriverNode<RealTunOutput<SharedTunFdIo>>),
    Capture(CaptureNode),
}

impl From<TunInputDriverNode<RealTunInput<SharedTunFdIo>>> for TestNode {
    fn from(node: TunInputDriverNode<RealTunInput<SharedTunFdIo>>) -> Self {
        Self::TunInput(node)
    }
}

impl From<TunOutputDriverNode<RealTunOutput<SharedTunFdIo>>> for TestNode {
    fn from(node: TunOutputDriverNode<RealTunOutput<SharedTunFdIo>>) -> Self {
        Self::TunOutput(node)
    }
}

impl From<CaptureNode> for TestNode {
    fn from(node: CaptureNode) -> Self {
        Self::Capture(node)
    }
}

impl Node<TestNode> for TestNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::TunInput(node) => node.process(runtime, frame),
            Self::TunOutput(node) => node.process(runtime, frame),
            Self::Capture(node) => node.process(runtime, frame),
        }
    }
}

#[test]
fn readiness_schedules_only_and_input_driver_reads_packets() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let io = SharedTunFdIo::default();
    let packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"rx");
    io.inject(packet.clone());

    let metadata = Rc::new(RefCell::new(Vec::new()));
    let packets = Rc::new(RefCell::new(Vec::new()));
    let capture = runtime.nodes().register_internal(CaptureNode {
        metadata: Rc::clone(&metadata),
        packets: Rc::clone(&packets),
    });
    let input = runtime.nodes().register_driver(
        TunInputDriverNode::new(RealTunInput::new(io.clone()), "utun-test", capture)
            .with_interface_index(42),
    );
    let readiness = TunFdReadiness::input(input);

    readiness
        .schedule_readable(&runtime)
        .expect("schedule readable");

    assert_eq!(io.recv_calls(), 0);
    assert!(metadata.borrow().is_empty());

    assert_eq!(runtime.run_ready_nodes().expect("run input"), 2);
    assert_eq!(io.recv_calls(), 2);
    assert_eq!(&*packets.borrow(), &[packet]);
    let first = &metadata.borrow()[0];
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
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let io = SharedTunFdIo::default();
    io.push_send_result(TunBufferSendResult::Backpressure);
    io.push_send_result(TunBufferSendResult::Complete);
    let packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"tx");
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(RealTunOutput::new(io.clone())));
    let readiness = TunFdReadiness::output(output);
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
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(40, 8, 8, 4);
    let io = SharedTunFdIo::default();
    io.inject((0..40).collect());

    let capture = runtime.nodes().register_internal(CaptureNode {
        metadata: Rc::new(RefCell::new(Vec::new())),
        packets: Rc::new(RefCell::new(Vec::new())),
    });
    let input = runtime.nodes().register_driver(TunInputDriverNode::new(
        RealTunInput::new(io.clone()),
        "utun-test",
        capture,
    ));
    let readiness = TunFdReadiness::input(input);

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
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let io = SharedTunFdIo::default();
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
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let io = SharedTunFdIo::default();
    io.push_send_result(TunBufferSendResult::Partial(12));
    io.push_send_result(TunBufferSendResult::Complete);
    let packet = ipv4_udp_packet([10, 0, 0, 2], [198, 51, 100, 7], b"partial-tx");
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(RealTunOutput::new(io.clone())));
    let readiness = TunFdReadiness::output(output);
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
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(1, 64, 8, 4);
    let io = SharedTunFdIo::default();
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
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let io = SharedTunFdIo::default();
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
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let io = SharedTunFdIo::default();
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
    runtime: &DataPlaneRuntime<TestNode>,
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

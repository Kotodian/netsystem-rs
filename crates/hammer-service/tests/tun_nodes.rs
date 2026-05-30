use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use hammer_adapter::{
    BufferFrame, BufferPacketCursor, DataPlaneRuntime, InternalNode, Network, Node, NodeId,
    NodeResult, RouteDecision, RouteMetadata, RouteTarget, Router, SocksAddr,
};
use hammer_core::error::CoreResult;
use hammer_core::lifecycle::{Lifecycle, StartStage};
use hammer_service::data_plane::{RouteDispatchNode, RouteMatchNode};
use hammer_service::packet::{IpInputNode, UdpInputNode};
use hammer_service::tun::{MemoryTunDevice, TunInputDriverNode, TunOutputNode};

struct StaticRouter {
    decision: RouteDecision,
    prepare_count: AtomicUsize,
    match_count: AtomicUsize,
}

impl StaticRouter {
    fn new(decision: RouteDecision) -> Self {
        Self {
            decision,
            prepare_count: AtomicUsize::new(0),
            match_count: AtomicUsize::new(0),
        }
    }
}

impl Lifecycle for StaticRouter {
    fn name(&self) -> &str {
        "static-router"
    }

    fn start(&self, _stage: StartStage) -> CoreResult<()> {
        Ok(())
    }

    fn close(&self) -> CoreResult<()> {
        Ok(())
    }
}

impl Router for StaticRouter {
    fn reset_network(&self) {}

    fn match_route(&self, _metadata: &mut RouteMetadata) -> CoreResult<RouteDecision> {
        self.match_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.decision.clone())
    }

    fn prepare_route_metadata(&self, metadata: &mut RouteMetadata) -> CoreResult<()> {
        self.prepare_count.fetch_add(1, Ordering::SeqCst);
        metadata.protocol = "prepared".to_owned();
        Ok(())
    }

    fn sniff_timeout(&self, _metadata: &RouteMetadata) -> Option<Duration> {
        None
    }

    fn should_sniff(&self, _metadata: &RouteMetadata) -> bool {
        false
    }
}

struct CaptureNode {
    next: NodeId,
    metadata: Rc<RefCell<Vec<RouteMetadata>>>,
    cursors: Rc<RefCell<Vec<BufferPacketCursor>>>,
}

impl Node<TestNode> for CaptureNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.pending_indices().iter().copied() {
            self.metadata.borrow_mut().push(runtime.metadata(index)?);
            self.cursors
                .borrow_mut()
                .push(runtime.packet_cursor(index)?);
        }
        Ok(NodeResult::next_current(self.next))
    }
}

impl InternalNode<TestNode> for CaptureNode {}

enum TestNode {
    TunInput(TunInputDriverNode<hammer_service::tun::MemoryTunInput>),
    IpInput(IpInputNode),
    UdpInput(UdpInputNode),
    Capture(CaptureNode),
    RouteMatch(RouteMatchNode<Arc<StaticRouter>>),
    RouteDispatch(RouteDispatchNode),
    TunOutput(TunOutputNode<hammer_service::tun::MemoryTunOutput>),
}

impl From<TunInputDriverNode<hammer_service::tun::MemoryTunInput>> for TestNode {
    fn from(node: TunInputDriverNode<hammer_service::tun::MemoryTunInput>) -> Self {
        Self::TunInput(node)
    }
}

impl From<IpInputNode> for TestNode {
    fn from(node: IpInputNode) -> Self {
        Self::IpInput(node)
    }
}

impl From<UdpInputNode> for TestNode {
    fn from(node: UdpInputNode) -> Self {
        Self::UdpInput(node)
    }
}

impl From<CaptureNode> for TestNode {
    fn from(node: CaptureNode) -> Self {
        Self::Capture(node)
    }
}

impl From<RouteMatchNode<Arc<StaticRouter>>> for TestNode {
    fn from(node: RouteMatchNode<Arc<StaticRouter>>) -> Self {
        Self::RouteMatch(node)
    }
}

impl From<RouteDispatchNode> for TestNode {
    fn from(node: RouteDispatchNode) -> Self {
        Self::RouteDispatch(node)
    }
}

impl From<TunOutputNode<hammer_service::tun::MemoryTunOutput>> for TestNode {
    fn from(node: TunOutputNode<hammer_service::tun::MemoryTunOutput>) -> Self {
        Self::TunOutput(node)
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
            Self::IpInput(node) => node.process(runtime, frame),
            Self::UdpInput(node) => node.process(runtime, frame),
            Self::Capture(node) => node.process(runtime, frame),
            Self::RouteMatch(node) => node.process(runtime, frame),
            Self::RouteDispatch(node) => node.process(runtime, frame),
            Self::TunOutput(node) => node.process(runtime, frame),
        }
    }
}

#[test]
fn tun_driver_node_feeds_frame_and_output_node_writes_packet() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    let packets = vec![
        ipv4_udp_packet([10, 0, 0, 2], 54_321, [198, 51, 100, 7], 53, b"query"),
        ipv4_udp_packet([10, 0, 0, 3], 54_322, [198, 51, 100, 8], 443, b"hello"),
        ipv4_udp_packet([10, 0, 0, 4], 54_323, [198, 51, 100, 9], 853, b"dot"),
    ];
    for packet in &packets {
        device.inject(packet.clone()).expect("inject packet");
    }

    let output = runtime
        .nodes()
        .register_output(TunOutputNode::new(device.output()));
    let captured = Rc::new(RefCell::new(Vec::new()));
    let cursors = Rc::new(RefCell::new(Vec::new()));
    let capture = runtime.nodes().register_internal(CaptureNode {
        next: output,
        metadata: Rc::clone(&captured),
        cursors: Rc::clone(&cursors),
    });
    let udp_input = runtime
        .nodes()
        .register_internal(UdpInputNode::new(capture));
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(udp_input));
    let driver =
        runtime
            .nodes()
            .register_driver(TunInputDriverNode::new(device.input(), "tun0", ip_input));
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 5);
    assert_eq!(device.drain_output_batch_sizes(), vec![3]);
    assert_eq!(device.drain_output(), packets);
    assert_eq!(captured.borrow().len(), 3);
    let metadata = &captured.borrow()[0];
    assert_eq!(metadata.inbound, "tun0");
    assert_eq!(metadata.network, Network::Udp);
    let cursor = cursors.borrow()[0];
    assert_eq!(cursor.network_header_offset(), 0);
    assert_eq!(cursor.network_header_len(), 20);
    assert_eq!(cursor.transport_header_offset(), 20);
    assert_eq!(cursor.transport_header_len(), 8);
    assert_eq!(cursor.transport_payload_offset(), 28);
    assert_eq!(cursor.packet_len(), packets[0].len());
    assert_eq!(
        metadata.source,
        Some(SocksAddr::ip(
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)),
            54_321
        ))
    );
    assert_eq!(
        metadata.destination,
        Some(SocksAddr::ip(
            IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)),
            53
        ))
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tun_driver_routes_through_service_internal_nodes() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    let packets = vec![
        ipv4_udp_packet([10, 0, 0, 2], 12_345, [203, 0, 113, 9], 443, b"hello"),
        ipv4_udp_packet([10, 0, 0, 3], 12_346, [203, 0, 113, 10], 8443, b"world"),
    ];
    for packet in &packets {
        device.inject(packet.clone()).expect("inject packet");
    }

    let output = runtime
        .nodes()
        .register_output(TunOutputNode::new(device.output()));
    let dispatch = runtime
        .nodes()
        .register_internal(RouteDispatchNode::new().with_outbound("tun-output", output));
    let router = Arc::new(StaticRouter::new(RouteDecision::Route {
        target: RouteTarget::Outbound("tun-output".to_owned()),
    }));
    let route_match = runtime
        .nodes()
        .register_internal(RouteMatchNode::new(Arc::clone(&router), dispatch));
    let udp_input = runtime
        .nodes()
        .register_internal(UdpInputNode::new(route_match));
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(udp_input));
    let driver =
        runtime
            .nodes()
            .register_driver(TunInputDriverNode::new(device.input(), "tun0", ip_input));
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 6);
    assert_eq!(device.drain_output_batch_sizes(), vec![2]);
    assert_eq!(device.drain_output(), packets);
    assert_eq!(router.prepare_count.load(Ordering::SeqCst), 2);
    assert_eq!(router.match_count.load(Ordering::SeqCst), 2);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tun_driver_batch_respects_frame_capacity() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 2, 4);
    let device = MemoryTunDevice::new();
    let packets = vec![
        ipv4_udp_packet([10, 0, 0, 2], 20_001, [203, 0, 113, 1], 53, b"a"),
        ipv4_udp_packet([10, 0, 0, 3], 20_002, [203, 0, 113, 2], 53, b"b"),
        ipv4_udp_packet([10, 0, 0, 4], 20_003, [203, 0, 113, 3], 53, b"c"),
    ];
    for packet in &packets {
        device.inject(packet.clone()).expect("inject packet");
    }

    let output = runtime
        .nodes()
        .register_output(TunOutputNode::new(device.output()));
    let driver = runtime
        .nodes()
        .register_driver(TunInputDriverNode::new(device.input(), "tun0", output).with_max_batch(8));
    let first_frame = runtime.alloc_frame_index().expect("alloc first frame");
    let second_frame = runtime.alloc_frame_index().expect("alloc second frame");

    runtime
        .schedule_driver_frame(driver, first_frame)
        .expect("schedule first batch");
    assert_eq!(runtime.run_ready_nodes().expect("run first batch"), 2);

    runtime
        .schedule_driver_frame(driver, second_frame)
        .expect("schedule second batch");
    assert_eq!(runtime.run_ready_nodes().expect("run second batch"), 2);

    assert_eq!(device.drain_output_batch_sizes(), vec![2, 1]);
    assert_eq!(device.drain_output(), packets);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

fn ipv4_udp_packet(
    source: [u8; 4],
    source_port: u16,
    destination: [u8; 4],
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = vec![0; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&source);
    packet[16..20].copy_from_slice(&destination);
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[28..].copy_from_slice(payload);
    packet
}

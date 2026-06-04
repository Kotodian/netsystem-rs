use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr};
use std::rc::Rc;
use std::vec::Vec as StdVec;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Network, Node, NodeId, NodeResult, RouteMetadata,
    SocksAddr, TraceControlPlane, TraceInputPolicy, TracePolicy,
};
use hammer_core::error::CoreResult;
use hammer_core::forwarding::AdjacencyRewrite;
use hammer_infra::vec::Vec;
use hammer_runtime::spawn::DataRuntime;
use hammer_service::data_plane::{FeatureArcControl, next_feature_frame};
use hammer_service::interface::{
    InterfaceControlPlane, InterfaceMtu, InterfaceOutputControlPlane, InterfaceOutputNode,
};
use hammer_service::net::{
    AdjacencyRewriteNode, DpoId, DpoProto, FibTableBuilder, IpInputNext, IpInputNode,
    IpInputTarget, IpInputTrace, IpLookupControlPlane, IpLookupNode, IpProtocol, IpVersion,
};
use hammer_service::tun::{
    MemoryTunDevice, TunInputDriverNode, TunInputTrace, TunOutputDriverNode, TunOutputTrace,
};
use ipnet::Ipv4Net;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FeatureVisit {
    name: &'static str,
    config: Option<StdVec<u8>>,
    ingress_interface: Option<u32>,
}

#[hammer_component_macros::feature_arc]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TestIpUnicastArc {
    TunFeature,
    AlphaFeature,
    BetaFeature,
}

struct ForwardCaptureNode {
    next: NodeId,
    metadata: Rc<RefCell<Vec<RouteMetadata>>>,
}

impl Node<TestNode> for ForwardCaptureNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.pending_indices().iter().copied() {
            self.metadata.borrow_mut().push(runtime.metadata(index)?);
        }
        Ok(NodeResult::next_current(self.next))
    }
}

impl InternalNode<TestNode> for ForwardCaptureNode {}

#[hammer_component_macros::feature(arc = TestIpUnicastArc, id = TunFeature)]
struct CaptureFeatureNode {
    metadata: Rc<RefCell<Vec<RouteMetadata>>>,
}

impl Node<TestNode> for CaptureFeatureNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.pending_indices().iter().copied() {
            self.metadata.borrow_mut().push(runtime.metadata(index)?);
        }
        next_feature_frame(runtime, frame)
    }
}

impl InternalNode<TestNode> for CaptureFeatureNode {}

#[hammer_component_macros::feature(
    arc = TestIpUnicastArc,
    id = AlphaFeature,
    runs_before = [BetaFeature]
)]
struct AlphaFeatureNode {
    visits: Rc<RefCell<StdVec<FeatureVisit>>>,
}

impl Node<TestNode> for AlphaFeatureNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.pending_indices().iter().copied() {
            let metadata = runtime.metadata(index)?;
            self.visits.borrow_mut().push(FeatureVisit {
                name: "alpha",
                config: metadata.feature_config.clone(),
                ingress_interface: metadata.ingress_interface,
            });
        }
        next_feature_frame(runtime, frame)
    }
}

impl InternalNode<TestNode> for AlphaFeatureNode {}

#[hammer_component_macros::feature(
    arc = TestIpUnicastArc,
    id = BetaFeature,
    runs_after = [AlphaFeature]
)]
struct BetaFeatureNode {
    visits: Rc<RefCell<StdVec<FeatureVisit>>>,
}

impl Node<TestNode> for BetaFeatureNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.pending_indices().iter().copied() {
            let metadata = runtime.metadata(index)?;
            self.visits.borrow_mut().push(FeatureVisit {
                name: "beta",
                config: metadata.feature_config.clone(),
                ingress_interface: metadata.ingress_interface,
            });
        }
        next_feature_frame(runtime, frame)
    }
}

impl InternalNode<TestNode> for BetaFeatureNode {}

enum TestNode {
    TunInput(TunInputDriverNode<hammer_service::tun::MemoryTunInput>),
    IpInput(IpInputNode<TestIpUnicastArc>),
    ForwardCapture(ForwardCaptureNode),
    CaptureFeature(CaptureFeatureNode),
    AlphaFeature(AlphaFeatureNode),
    BetaFeature(BetaFeatureNode),
    IpLookup(IpLookupNode),
    AdjacencyRewrite(AdjacencyRewriteNode),
    InterfaceOutput(InterfaceOutputNode),
    TunOutputDriver(TunOutputDriverNode<hammer_service::tun::MemoryTunOutput>),
}

impl From<TunInputDriverNode<hammer_service::tun::MemoryTunInput>> for TestNode {
    fn from(node: TunInputDriverNode<hammer_service::tun::MemoryTunInput>) -> Self {
        Self::TunInput(node)
    }
}

impl From<IpInputNode<TestIpUnicastArc>> for TestNode {
    fn from(node: IpInputNode<TestIpUnicastArc>) -> Self {
        Self::IpInput(node)
    }
}

impl From<ForwardCaptureNode> for TestNode {
    fn from(node: ForwardCaptureNode) -> Self {
        Self::ForwardCapture(node)
    }
}

impl From<CaptureFeatureNode> for TestNode {
    fn from(node: CaptureFeatureNode) -> Self {
        Self::CaptureFeature(node)
    }
}

impl From<AlphaFeatureNode> for TestNode {
    fn from(node: AlphaFeatureNode) -> Self {
        Self::AlphaFeature(node)
    }
}

impl From<BetaFeatureNode> for TestNode {
    fn from(node: BetaFeatureNode) -> Self {
        Self::BetaFeature(node)
    }
}

impl From<IpLookupNode> for TestNode {
    fn from(node: IpLookupNode) -> Self {
        Self::IpLookup(node)
    }
}

impl From<AdjacencyRewriteNode> for TestNode {
    fn from(node: AdjacencyRewriteNode) -> Self {
        Self::AdjacencyRewrite(node)
    }
}

impl From<InterfaceOutputNode> for TestNode {
    fn from(node: InterfaceOutputNode) -> Self {
        Self::InterfaceOutput(node)
    }
}

impl From<TunOutputDriverNode<hammer_service::tun::MemoryTunOutput>> for TestNode {
    fn from(node: TunOutputDriverNode<hammer_service::tun::MemoryTunOutput>) -> Self {
        Self::TunOutputDriver(node)
    }
}

impl Node<TestNode> for TestNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::TunInput(node) => node.process(runtime, frame),
            Self::IpInput(node) => node.process(runtime, frame),
            Self::ForwardCapture(node) => node.process(runtime, frame),
            Self::CaptureFeature(node) => node.process(runtime, frame),
            Self::AlphaFeature(node) => node.process(runtime, frame),
            Self::BetaFeature(node) => node.process(runtime, frame),
            Self::IpLookup(node) => node.process(runtime, frame),
            Self::AdjacencyRewrite(node) => node.process(runtime, frame),
            Self::InterfaceOutput(node) => node.process(runtime, frame),
            Self::TunOutputDriver(node) => node.process(runtime, frame),
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
        .register_driver(TunOutputDriverNode::new(device.output()));
    let captured = Rc::new(RefCell::new(Vec::new()));
    let capture = runtime.nodes().register_internal(ForwardCaptureNode {
        next: output,
        metadata: Rc::clone(&captured),
    });
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            capture, capture, capture, capture, capture, capture, capture,
        )));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tun0", ip_input).with_interface_index(42),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(device.drain_output_batch_sizes(), vec![3]);
    assert_eq!(device.drain_output(), packets);
    assert_eq!(captured.borrow().len(), 3);
    let metadata = &captured.borrow()[0];
    assert_eq!(metadata.inbound, "tun0");
    assert_eq!(metadata.ingress_interface, Some(42));
    assert_eq!(metadata.network, Network::Udp);
    assert_eq!(
        metadata.source,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 0))
    );
    assert_eq!(
        metadata.destination,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 0))
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tun_driver_nodes_record_node_payloads_for_traced_packets() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    let packet = ipv4_udp_packet([10, 0, 0, 2], 54_321, [198, 51, 100, 7], 53, b"query");
    device.inject(packet.clone()).expect("inject packet");

    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(device.output()));
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            output, output, output, output, output, output, output,
        )));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tun0", ip_input).with_interface_index(42),
    );
    let control = TraceControlPlane::new(8);
    control.publish(TracePolicy {
        enabled: true,
        record_capacity: 8,
        packet_capacity: 4,
        inputs: vec![TraceInputPolicy {
            node: driver,
            count: 1,
        }],
    });
    runtime.set_trace_control(Some(control.handle()), 4);
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(control.drain_completed(), 1);
    let records = control.take_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].input_node, driver);
    assert_eq!(records[0].input_node_name, Some("tun-input-driver-node"));
    assert_eq!(records[0].entries.len(), 3);
    assert_eq!(
        records[0].entries[0].node_name,
        Some("tun-input-driver-node")
    );
    assert_eq!(
        TunInputTrace::decode(&records[0].entries[0].payload_bytes).expect("tun input trace"),
        TunInputTrace {
            interface_index: Some(42),
            mode: hammer_service::tun::TunDriverMode::Tun,
            received: 1,
        }
    );
    assert_eq!(records[0].entries[1].node_name, Some("ip-input-node"));
    assert_eq!(
        IpInputTrace::decode(&records[0].entries[1].payload_bytes).expect("ip input trace"),
        IpInputTrace {
            version: Some(IpVersion::V4),
            protocol: Some(IpProtocol::Udp),
            input_target: Some(IpInputTarget::Lookup),
            input_error: Some(hammer_service::net::IpInputError::None),
            packet_len: packet.len(),
            next: output,
        }
    );
    assert_eq!(
        records[0].entries[2].node_name,
        Some("tun-output-driver-node")
    );
    assert_eq!(
        TunOutputTrace::decode(&records[0].entries[2].payload_bytes).expect("tun output trace"),
        TunOutputTrace {
            mode: hammer_service::tun::TunDriverMode::Tun,
            pending: 1,
        }
    );
}

#[test]
fn tap_input_strips_ethernet_header_before_ip_pipeline() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    let ip_packet = ipv4_udp_packet([10, 0, 0, 2], 54_321, [198, 51, 100, 7], 53, b"tap");
    let frame = ethernet_frame(
        [0x02, 0xaa, 0xaa, 0xaa, 0xaa, 0x01],
        [0x02, 0xbb, 0xbb, 0xbb, 0xbb, 0x02],
        0x0800,
        &ip_packet,
    );
    device.inject(frame).expect("inject tap frame");

    let captured = Rc::new(RefCell::new(Vec::new()));
    let capture = runtime.nodes().register_internal(ForwardCaptureNode {
        next: runtime
            .nodes()
            .register_driver(TunOutputDriverNode::new(device.output())),
        metadata: Rc::clone(&captured),
    });
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            capture, capture, capture, capture, capture, capture, capture,
        )));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tap0", ip_input)
            .with_interface_index(7)
            .with_tap(true),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tap driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(device.drain_output(), vec![ip_packet]);
    let metadata = &captured.borrow()[0];
    assert_eq!(metadata.ingress_interface, Some(7));
    assert_eq!(metadata.egress_interface, None);
    assert_eq!(metadata.network, Network::Udp);
    assert_eq!(
        metadata.source,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 0))
    );
    assert_eq!(
        metadata.destination,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 0))
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tap_input_drops_truncated_and_unsupported_ethernet_frames() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    device
        .inject(StdVec::from([0u8; 13]).into())
        .expect("inject truncated");
    device
        .inject(ethernet_frame(
            [0x02, 0xaa, 0xaa, 0xaa, 0xaa, 0x01],
            [0x02, 0xbb, 0xbb, 0xbb, 0xbb, 0x02],
            0x0806,
            b"arp-is-out-of-scope",
        ))
        .expect("inject unsupported");

    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(device.output()).with_tap(true));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tap0", output)
            .with_interface_index(7)
            .with_tap(true),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tap driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert!(device.drain_output().is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tap_output_emits_ethernet_frame_from_tap_metadata() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let input_device = MemoryTunDevice::new();
    let output_device = MemoryTunDevice::new();
    let ip_packet = ipv4_udp_packet([10, 0, 0, 2], 54_321, [198, 51, 100, 7], 53, b"tap-out");
    let input_frame = ethernet_frame(
        [0x02, 0xaa, 0xaa, 0xaa, 0xaa, 0x01],
        [0x02, 0xbb, 0xbb, 0xbb, 0xbb, 0x02],
        0x0800,
        &ip_packet,
    );
    input_device
        .inject(input_frame.clone())
        .expect("inject tap frame");

    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(output_device.output()).with_tap(true));
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            output, output, output, output, output, output, output,
        )));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(input_device.input(), "tap0", ip_input)
            .with_interface_index(7)
            .with_tap(true),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tap driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(output_device.drain_output(), vec![input_frame]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tun_driver_sets_ingress_interface_from_interface_control_handle() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    let packet = ipv4_udp_packet([10, 0, 0, 2], 54_321, [198, 51, 100, 7], 53, b"query");
    device.inject(packet).expect("inject packet");

    let interface_control = InterfaceControlPlane::new();
    let tun0 = interface_control
        .register_interface("tun0")
        .expect("register tun0");
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(device.output()));
    let captured = Rc::new(RefCell::new(Vec::new()));
    let capture = runtime.nodes().register_internal(ForwardCaptureNode {
        next: output,
        metadata: Rc::clone(&captured),
    });
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            capture, capture, capture, capture, capture, capture, capture,
        )));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tun0", ip_input)
            .with_interface_control(interface_control.handle()),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(captured.borrow().len(), 1);
    assert_eq!(captured.borrow()[0].ingress_interface, Some(tun0));
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tun_driver_interface_control_handle_exposes_configured_mtu() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    let packet = ipv4_udp_packet([10, 0, 0, 2], 54_321, [198, 51, 100, 7], 53, b"query");
    device.inject(packet).expect("inject packet");

    let interface_control = InterfaceControlPlane::new();
    let mtu = InterfaceMtu::new(9000, 1500, 1280, 0);
    let tun0 = interface_control
        .register_interface_with_mtu("tun0", mtu)
        .expect("register tun0");
    let handle = interface_control.handle();
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(device.output()));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tun0", output)
            .with_interface_control(handle.clone()),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");
    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(handle.interface_mtu(tun0), Some(mtu));
}

#[test]
fn tun_driver_errors_when_interface_control_cannot_resolve_interface() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let device = MemoryTunDevice::new();
    let packet = ipv4_udp_packet([10, 0, 0, 2], 54_321, [198, 51, 100, 7], 53, b"query");
    device.inject(packet).expect("inject packet");

    let interface_control = InterfaceControlPlane::new();
    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(device.output()));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tun0", output)
            .with_interface_control(interface_control.handle()),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");
    let err = runtime
        .run_ready_nodes()
        .expect_err("missing interface should fail");

    assert!(err.to_string().contains("interface tun0 is not registered"));
}

#[test]
fn tun_driver_routes_through_service_internal_nodes() {
    let data_runtime =
        DataRuntime::new(1, "tun-feature-arc-route-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
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
        .register_driver(TunOutputDriverNode::new(device.output()));
    let mut builder = FibTableBuilder::new(output);
    let adjacency = builder.add_adjacency(DpoProto::IP4, output);
    let load_balance = builder.add_load_balance(
        DpoProto::IP4,
        [DpoId::adjacency(DpoProto::IP4, adjacency, output)],
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
        load_balance,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let mut unicast_features =
        FeatureArcControl::<TestIpUnicastArc>::new().with_data_plane_barrier(barrier);
    let feature_metadata = Rc::new(RefCell::new(Vec::new()));
    let feature = runtime.nodes().register_internal(CaptureFeatureNode {
        metadata: Rc::clone(&feature_metadata),
    });
    unicast_features
        .register_feature::<CaptureFeatureNode>(feature)
        .expect("register feature");
    unicast_features
        .enable_feature::<CaptureFeatureNode>(7)
        .expect("enable feature");
    let mut ip_input_node = IpInputNode::new(IpInputNext::nodes(
        output, output, output, lookup, output, output, output,
    ));
    unicast_features.attach_start(&mut ip_input_node);
    let ip_input = runtime.nodes().register_internal(ip_input_node);
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tun0", ip_input).with_interface_index(7),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 5);
    assert_eq!(device.drain_output_batch_sizes(), vec![2]);
    assert_eq!(device.drain_output(), packets);
    assert_eq!(feature_metadata.borrow().len(), 2);
    assert_eq!(feature_metadata.borrow()[0].ingress_interface, Some(7));
    assert_eq!(feature_metadata.borrow()[1].ingress_interface, Some(7));
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
    data_runtime.shutdown_timeout(std::time::Duration::from_secs(1));
}

#[test]
fn tap_graph_routes_through_adjacency_rewrite_interface_output_and_tap_output() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let input_device = MemoryTunDevice::new();
    let output_device = MemoryTunDevice::new();
    let ip_packet = ipv4_udp_packet([10, 0, 0, 2], 12_345, [203, 0, 113, 9], 443, b"graph-tap");
    let input_frame = ethernet_frame(
        [0x02, 0xaa, 0xaa, 0xaa, 0xaa, 0x01],
        [0x02, 0xbb, 0xbb, 0xbb, 0xbb, 0x02],
        0x0800,
        &ip_packet,
    );
    input_device.inject(input_frame).expect("inject tap frame");
    let rewrite = ethernet_header(
        [0x02, 0xcc, 0xcc, 0xcc, 0xcc, 0x03],
        [0x02, 0xdd, 0xdd, 0xdd, 0xdd, 0x04],
        0x0800,
    );
    let expected = ethernet_frame(
        [0x02, 0xcc, 0xcc, 0xcc, 0xcc, 0x03],
        [0x02, 0xdd, 0xdd, 0xdd, 0xdd, 0x04],
        0x0800,
        &ip_packet,
    );

    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(output_device.output()).with_tap(true));
    let interface_output_control = InterfaceOutputControlPlane::new();
    interface_output_control
        .register_tx(7, output)
        .expect("register tap output");
    let interface_output = runtime
        .nodes()
        .register_internal(interface_output_control.node());
    let lookup_control = IpLookupControlPlane::new(FibTableBuilder::new(interface_output).build());
    let adjacency_rewrite = runtime
        .nodes()
        .register_internal(AdjacencyRewriteNode::new(lookup_control.table_handle()));
    let mut builder = FibTableBuilder::new(interface_output);
    let dpo = builder.add_interface_adjacency_dpo(
        DpoProto::IP4,
        7,
        AdjacencyRewrite::try_new(&rewrite).expect("rewrite fits"),
        adjacency_rewrite,
        interface_output,
    );
    let lb = builder.add_load_balance(DpoProto::IP4, [dpo]);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
        lb,
    );
    lookup_control
        .publish(builder.build())
        .expect("publish fib");
    let lookup = runtime.nodes().register_internal(lookup_control.node());
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            interface_output,
            interface_output,
            interface_output,
            lookup,
            interface_output,
            interface_output,
            interface_output,
        )));
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(input_device.input(), "tap0", ip_input)
            .with_interface_index(7)
            .with_tap(true),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tap graph");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 6);
    assert_eq!(output_device.drain_output(), vec![expected]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn feature_arc_enable_disable_and_end_next_affect_new_packets() {
    let data_runtime =
        DataRuntime::new(1, "tun-feature-arc-toggle-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 4);
    let input_device = MemoryTunDevice::new();
    let default_output_device = MemoryTunDevice::new();
    let override_output_device = MemoryTunDevice::new();

    let default_output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(default_output_device.output()));
    let override_output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(override_output_device.output()));
    let feature_metadata = Rc::new(RefCell::new(Vec::new()));
    let feature = runtime.nodes().register_internal(CaptureFeatureNode {
        metadata: Rc::clone(&feature_metadata),
    });
    let mut unicast_features =
        FeatureArcControl::<TestIpUnicastArc>::new().with_data_plane_barrier(barrier);
    unicast_features
        .register_feature::<CaptureFeatureNode>(feature)
        .expect("register feature");
    unicast_features
        .set_end_node_for_interface(7, override_output)
        .expect("set end node");
    let mut ip_input_node = IpInputNode::new(IpInputNext::nodes(
        default_output,
        default_output,
        default_output,
        default_output,
        default_output,
        default_output,
        default_output,
    ));
    unicast_features.attach_start(&mut ip_input_node);
    let ip_input = runtime.nodes().register_internal(ip_input_node);
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(input_device.input(), "tun0", ip_input).with_interface_index(7),
    );

    let packet_without_feature =
        ipv4_udp_packet([10, 0, 0, 2], 12_345, [203, 0, 113, 9], 443, b"off");
    input_device
        .inject(packet_without_feature.clone())
        .expect("inject disabled packet");
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule disabled packet");
    assert_eq!(runtime.run_ready_nodes().expect("run disabled packet"), 3);
    assert!(feature_metadata.borrow().is_empty());
    assert!(default_output_device.drain_output().is_empty());
    assert_eq!(
        override_output_device.drain_output(),
        vec![packet_without_feature]
    );

    unicast_features
        .enable_feature::<CaptureFeatureNode>(7)
        .expect("enable feature");
    let packet_with_feature = ipv4_udp_packet([10, 0, 0, 3], 12_346, [203, 0, 113, 10], 443, b"on");
    input_device
        .inject(packet_with_feature.clone())
        .expect("inject enabled packet");
    let frame = runtime.alloc_frame_index().expect("alloc second frame");
    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule enabled packet");
    assert_eq!(runtime.run_ready_nodes().expect("run enabled packet"), 4);
    assert_eq!(feature_metadata.borrow().len(), 1);
    assert_eq!(feature_metadata.borrow()[0].ingress_interface, Some(7));
    assert!(default_output_device.drain_output().is_empty());
    assert_eq!(
        override_output_device.drain_output(),
        vec![packet_with_feature]
    );

    unicast_features
        .disable_feature::<CaptureFeatureNode>(7)
        .expect("disable feature");
    let packet_after_disable =
        ipv4_udp_packet([10, 0, 0, 4], 12_347, [203, 0, 113, 11], 443, b"off-again");
    input_device
        .inject(packet_after_disable.clone())
        .expect("inject re-disabled packet");
    let frame = runtime.alloc_frame_index().expect("alloc third frame");
    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule re-disabled packet");
    assert_eq!(
        runtime.run_ready_nodes().expect("run re-disabled packet"),
        3
    );
    assert_eq!(feature_metadata.borrow().len(), 1);
    assert!(default_output_device.drain_output().is_empty());
    assert_eq!(
        override_output_device.drain_output(),
        vec![packet_after_disable]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
    data_runtime.shutdown_timeout(std::time::Duration::from_secs(1));
}

#[test]
fn feature_arc_orders_multiple_features_and_exposes_config_metadata() {
    let data_runtime =
        DataRuntime::new(1, "tun-feature-arc-order-test", 512 * 1024, 2).expect("data runtime");
    let barrier = data_runtime.data_plane_barrier();
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 4);
    let device = MemoryTunDevice::new();
    let packet = ipv4_udp_packet([10, 0, 0, 2], 12_345, [203, 0, 113, 9], 443, b"ordered");
    device.inject(packet.clone()).expect("inject packet");

    let output = runtime
        .nodes()
        .register_driver(TunOutputDriverNode::new(device.output()));
    let visits = Rc::new(RefCell::new(StdVec::new()));
    let beta = runtime.nodes().register_internal(BetaFeatureNode {
        visits: Rc::clone(&visits),
    });
    let alpha = runtime.nodes().register_internal(AlphaFeatureNode {
        visits: Rc::clone(&visits),
    });
    let mut unicast_features =
        FeatureArcControl::<TestIpUnicastArc>::new().with_data_plane_barrier(barrier);
    unicast_features
        .register_feature::<BetaFeatureNode>(beta)
        .expect("register beta");
    unicast_features
        .register_feature::<AlphaFeatureNode>(alpha)
        .expect("register alpha");
    unicast_features
        .enable_feature_with_config::<BetaFeatureNode>(7, b"beta-config".to_vec())
        .expect("enable beta");
    unicast_features
        .enable_feature_with_config::<AlphaFeatureNode>(7, b"alpha-config".to_vec())
        .expect("enable alpha");
    let mut ip_input_node = IpInputNode::new(IpInputNext::nodes(
        output, output, output, output, output, output, output,
    ));
    unicast_features.attach_start(&mut ip_input_node);
    let ip_input = runtime.nodes().register_internal(ip_input_node);
    let driver = runtime.nodes().register_driver(
        TunInputDriverNode::new(device.input(), "tun0", ip_input).with_interface_index(7),
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 5);
    assert_eq!(device.drain_output(), vec![packet]);
    assert_eq!(
        *visits.borrow(),
        vec![
            FeatureVisit {
                name: "alpha",
                config: Some(b"alpha-config".to_vec()),
                ingress_interface: Some(7),
            },
            FeatureVisit {
                name: "beta",
                config: Some(b"beta-config".to_vec()),
                ingress_interface: Some(7),
            },
        ]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
    data_runtime.shutdown_timeout(std::time::Duration::from_secs(1));
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
        .register_driver(TunOutputDriverNode::new(device.output()));
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
    let checksum = ipv4_checksum(&packet[..20]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[22..24].copy_from_slice(&destination_port.to_be_bytes());
    packet[24..26].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[28..].copy_from_slice(payload);
    packet.into()
}

fn ethernet_header(destination: [u8; 6], source: [u8; 6], ethertype: u16) -> Vec<u8> {
    let mut header = Vec::with_capacity(14);
    header.extend_from_slice(&destination);
    header.extend_from_slice(&source);
    header.extend_from_slice(&ethertype.to_be_bytes());
    header
}

fn ethernet_frame(
    destination: [u8; 6],
    source: [u8; 6],
    ethertype: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut frame = ethernet_header(destination, source, ethertype);
    frame.extend_from_slice(payload);
    frame
}

fn ipv4_checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in header.chunks_exact(2) {
        sum += u16::from_be_bytes([chunk[0], chunk[1]]) as u32;
    }
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

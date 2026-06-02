use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr};
use std::rc::Rc;
use std::vec::Vec as StdVec;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Network, Node, NodeId, NodeResult, RouteMetadata,
    SocksAddr,
};
use hammer_core::error::CoreResult;
use hammer_infra::vec::Vec;
use hammer_service::data_plane::{FeatureArcControl, next_feature_frame};
use hammer_service::net::{
    DpoId, DpoProto, FibSnapshotBuilder, IpInputControlPlane, IpInputNext, IpInputNode,
    IpLookupControlPlane, IpLookupNode, IpUnicastArc,
};
use hammer_service::tun::{MemoryTunDevice, TunInputDriverNode, TunOutputDriverNode};
use ipnet::Ipv4Net;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FeatureVisit {
    name: &'static str,
    config: Option<StdVec<u8>>,
    ingress_interface: Option<u32>,
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

#[hammer_component_macros::feature(arc = IpUnicastArc, id = TunFeature)]
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
    arc = IpUnicastArc,
    id = AlphaFeature,
    after = [BetaFeature]
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
    arc = IpUnicastArc,
    id = BetaFeature,
    before = [AlphaFeature]
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
    IpInput(IpInputNode),
    ForwardCapture(ForwardCaptureNode),
    CaptureFeature(CaptureFeatureNode),
    AlphaFeature(AlphaFeatureNode),
    BetaFeature(BetaFeatureNode),
    IpLookup(IpLookupNode),
    TunOutputDriver(TunOutputDriverNode<hammer_service::tun::MemoryTunOutput>),
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
        .register_driver(TunOutputDriverNode::new(device.output()));
    let mut builder = FibSnapshotBuilder::new(output);
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
    let mut unicast_features = FeatureArcControl::<IpUnicastArc>::new();
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
    let mut input_control = IpInputControlPlane::new(IpInputNext::nodes(
        output, output, output, lookup, output, output, output,
    ));
    input_control.set_feature_arc(unicast_features.arc());
    let ip_input = runtime.nodes().register_internal(input_control.node());
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
}

#[test]
fn feature_arc_enable_disable_and_end_next_affect_new_packets() {
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
    let mut unicast_features = FeatureArcControl::<IpUnicastArc>::new();
    unicast_features
        .register_feature::<CaptureFeatureNode>(feature)
        .expect("register feature");
    unicast_features
        .set_end_node_for_interface(7, override_output)
        .expect("set end node");
    let mut input_control = IpInputControlPlane::new(IpInputNext::nodes(
        default_output,
        default_output,
        default_output,
        default_output,
        default_output,
        default_output,
        default_output,
    ));
    input_control.set_feature_arc(unicast_features.arc());
    let ip_input = runtime.nodes().register_internal(input_control.node());
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
}

#[test]
fn feature_arc_orders_multiple_features_and_exposes_config_metadata() {
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
    let mut unicast_features = FeatureArcControl::<IpUnicastArc>::new();
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
    let mut input_control = IpInputControlPlane::new(IpInputNext::nodes(
        output, output, output, output, output, output, output,
    ));
    input_control.set_feature_arc(unicast_features.arc());
    let ip_input = runtime.nodes().register_internal(input_control.node());
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

use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, BufferNodeError, DataPlaneRuntime, InternalNode, Network, Node, NodeId,
    NodeResult, RouteMetadata, SocksAddr,
};
use hammer_core::error::CoreResult;
use hammer_service::data_plane::{DropNode, FeatureArcControl, next_feature_frame};
use hammer_service::net::{
    DpoId, DpoProto, FibSnapshotBuilder, IpInputNext, IpInputNode, IpLocalArc, IpLocalControlPlane,
    IpLocalError, IpLocalNext, IpLocalNode, IpLocalSourceCheck, IpLookupControlPlane, IpLookupNode,
    IpReceiveNode,
};
use ipnet::Ipv4Net;

#[derive(Default)]
struct CaptureState {
    metadata: Vec<RouteMetadata>,
    node_errors: Vec<Option<BufferNodeError>>,
    frame_lens: Vec<usize>,
}

struct CaptureNode {
    state: Rc<RefCell<CaptureState>>,
}

impl CaptureNode {
    fn new(state: Rc<RefCell<CaptureState>>) -> Self {
        Self { state }
    }
}

impl Node<TestNode> for CaptureNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.state.borrow_mut().frame_lens.push(frame.pending_len());
        for index in frame.drain_pending() {
            let metadata = runtime.metadata(index)?;
            let node_error = runtime.node_error(index)?;
            let mut state = self.state.borrow_mut();
            state.metadata.push(metadata);
            state.node_errors.push(node_error);
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for CaptureNode {}

#[hammer_component_macros::feature(arc = IpLocalArc, id = LocalForward)]
struct ForwardNode {
    state: Rc<RefCell<CaptureState>>,
}

impl Node<TestNode> for ForwardNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.state.borrow_mut().frame_lens.push(frame.pending_len());
        for index in frame.pending_indices().iter().copied() {
            let metadata = runtime.metadata(index)?;
            let node_error = runtime.node_error(index)?;
            let mut state = self.state.borrow_mut();
            state.metadata.push(metadata);
            state.node_errors.push(node_error);
        }
        next_feature_frame(runtime, frame)
    }
}

impl InternalNode<TestNode> for ForwardNode {}

enum TestNode {
    Drop(DropNode),
    Capture(CaptureNode),
    Forward(ForwardNode),
    IpInput(IpInputNode),
    IpLookup(IpLookupNode),
    IpLocal(IpLocalNode),
    IpReceive(IpReceiveNode),
}

impl From<DropNode> for TestNode {
    fn from(node: DropNode) -> Self {
        Self::Drop(node)
    }
}

impl From<CaptureNode> for TestNode {
    fn from(node: CaptureNode) -> Self {
        Self::Capture(node)
    }
}

impl From<ForwardNode> for TestNode {
    fn from(node: ForwardNode) -> Self {
        Self::Forward(node)
    }
}

impl From<IpInputNode> for TestNode {
    fn from(node: IpInputNode) -> Self {
        Self::IpInput(node)
    }
}

impl From<IpLookupNode> for TestNode {
    fn from(node: IpLookupNode) -> Self {
        Self::IpLookup(node)
    }
}

impl From<IpLocalNode> for TestNode {
    fn from(node: IpLocalNode) -> Self {
        Self::IpLocal(node)
    }
}

impl From<IpReceiveNode> for TestNode {
    fn from(node: IpReceiveNode) -> Self {
        Self::IpReceive(node)
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
            Self::Drop(node) => node.process(runtime, frame),
            Self::Capture(node) => node.process(runtime, frame),
            Self::Forward(node) => node.process(runtime, frame),
            Self::IpInput(node) => node.process(runtime, frame),
            Self::IpLookup(node) => node.process(runtime, frame),
            Self::IpLocal(node) => node.process(runtime, frame),
            Self::IpReceive(node) => node.process(runtime, frame),
        }
    }
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode<TestNode>,
{
    let _ = node;
}

#[test]
fn ip_local_dispatches_ipv4_and_ipv6_known_protocols() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let graph = LocalGraph::new(&runtime);
    let control = IpLocalControlPlane::new(graph.nexts());
    let local = control.node();
    assert_internal_node(&local);
    let local = runtime.nodes().register_internal(local);

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet(
            Ipv4Addr::new(10, 0, 0, 1),
            10_001,
            Ipv4Addr::new(192, 0, 2, 1),
            53,
            b"udp4",
            true,
        ),
    );
    push_packet(
        &runtime,
        frame,
        &ipv6_tcp_packet(
            "2001:db8::1".parse().expect("source"),
            10_002,
            "2001:db8::2".parse().expect("destination"),
            443,
            b"tcp6",
        ),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_icmp_packet(
            Ipv4Addr::new(10, 0, 0, 3),
            Ipv4Addr::new(192, 0, 2, 3),
            b"icmp4",
        ),
    );
    push_packet(
        &runtime,
        frame,
        &ipv6_icmp_packet(
            "2001:db8::3".parse().expect("source"),
            "2001:db8::4".parse().expect("destination"),
            b"icmp6",
        ),
    );

    assert!(runtime.schedule_frame(local, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(graph.udp_state.borrow().metadata.len(), 1);
    assert_eq!(graph.tcp_state.borrow().metadata.len(), 1);
    assert_eq!(graph.icmp_state.borrow().metadata.len(), 2);
    assert_eq!(graph.punt_state.borrow().metadata.len(), 0);
    assert_metadata(
        &graph.udp_state.borrow().metadata[0],
        Network::Udp,
        Ipv4Addr::new(10, 0, 0, 1).into(),
        10_001,
        Ipv4Addr::new(192, 0, 2, 1).into(),
        53,
    );
    assert_metadata(
        &graph.tcp_state.borrow().metadata[0],
        Network::Tcp,
        "2001:db8::1".parse().expect("source"),
        10_002,
        "2001:db8::2".parse().expect("destination"),
        443,
    );
    assert_metadata(
        &graph.icmp_state.borrow().metadata[0],
        Network::Icmp,
        Ipv4Addr::new(10, 0, 0, 3).into(),
        0,
        Ipv4Addr::new(192, 0, 2, 3).into(),
        0,
    );
    assert_metadata(
        &graph.icmp_state.borrow().metadata[1],
        Network::Icmp,
        "2001:db8::3".parse().expect("source"),
        0,
        "2001:db8::4".parse().expect("destination"),
        0,
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_local_protocol_table_defaults_to_punt_and_allows_registration() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let graph = LocalGraph::new(&runtime);
    let custom_state = Rc::new(RefCell::new(CaptureState::default()));
    let custom = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&custom_state)));
    let control = IpLocalControlPlane::new(graph.nexts());
    let local = runtime.nodes().register_internal(control.node());
    let unknown = ipv4_protocol_packet(
        Ipv4Addr::new(10, 0, 0, 2),
        Ipv4Addr::new(192, 0, 2, 2),
        143,
        b"opaque",
    );

    let first = runtime.alloc_frame_index().expect("alloc first frame");
    push_packet(&runtime, first, &unknown);
    assert!(runtime.schedule_frame(local, first).expect("schedule punt"));
    assert_eq!(runtime.run_ready_nodes().expect("run punt"), 2);
    assert_eq!(graph.punt_state.borrow().metadata.len(), 1);
    assert_eq!(custom_state.borrow().metadata.len(), 0);

    control
        .register_protocol(143, custom)
        .expect("register protocol");
    let second = runtime.alloc_frame_index().expect("alloc second frame");
    push_packet(&runtime, second, &unknown);
    assert!(
        runtime
            .schedule_frame(local, second)
            .expect("schedule custom")
    );
    assert_eq!(runtime.run_ready_nodes().expect("run custom"), 2);
    assert_eq!(custom_state.borrow().metadata.len(), 1);

    control
        .unregister_protocol(143)
        .expect("unregister protocol");
    let third = runtime.alloc_frame_index().expect("alloc third frame");
    push_packet(&runtime, third, &unknown);
    assert!(runtime.schedule_frame(local, third).expect("schedule punt"));
    assert_eq!(runtime.run_ready_nodes().expect("run punt again"), 2);
    assert_eq!(graph.punt_state.borrow().metadata.len(), 2);
    assert_eq!(
        runtime
            .node_error_count(local, IpLocalError::UnknownProtocol.code())
            .expect("unknown counter"),
        2
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_local_routes_fragments_to_reassembly_next() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let graph = LocalGraph::new(&runtime);
    let control = IpLocalControlPlane::new(graph.nexts());
    let local = runtime.nodes().register_internal(control.node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let fragment = ipv4_first_fragment(
        &ipv4_udp_packet(
            Ipv4Addr::new(10, 0, 0, 3),
            10_003,
            Ipv4Addr::new(192, 0, 2, 3),
            53,
            b"fragmented-payload",
            true,
        ),
        8,
    );
    push_packet(&runtime, frame, &fragment);

    assert!(runtime.schedule_frame(local, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(graph.reassembly_state.borrow().metadata.len(), 1);
    assert_eq!(graph.udp_state.borrow().metadata.len(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_local_drops_bad_transport_headers_and_checksums() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let graph = LocalGraph::new(&runtime);
    let control = IpLocalControlPlane::new(graph.nexts());
    let local = runtime.nodes().register_internal(control.node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let mut bad_tcp = ipv4_tcp_packet(
        Ipv4Addr::new(10, 0, 0, 4),
        10_004,
        Ipv4Addr::new(192, 0, 2, 4),
        443,
        b"bad-tcp",
    );
    bad_tcp[20 + 16] ^= 0xff;
    let mut bad_udp = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 5),
        10_005,
        Ipv4Addr::new(192, 0, 2, 5),
        53,
        b"bad-udp",
        true,
    );
    bad_udp[24..26].copy_from_slice(&7u16.to_be_bytes());
    push_packet(&runtime, frame, &bad_tcp);
    push_packet(&runtime, frame, &bad_udp);

    assert!(runtime.schedule_frame(local, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(graph.tcp_state.borrow().metadata.len(), 0);
    assert_eq!(graph.udp_state.borrow().metadata.len(), 0);
    assert_eq!(
        runtime
            .node_error_count(local, IpLocalError::BadChecksum.code())
            .expect("checksum counter"),
        1
    );
    assert_eq!(
        runtime
            .node_error_count(local, IpLocalError::BadTransportHeader.code())
            .expect("header counter"),
        1
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_local_accepts_ipv4_udp_zero_checksum_and_rejects_ipv6_udp_zero_checksum() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let graph = LocalGraph::new(&runtime);
    let control = IpLocalControlPlane::new(graph.nexts());
    let local = runtime.nodes().register_internal(control.node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet(
            Ipv4Addr::new(10, 0, 0, 6),
            10_006,
            Ipv4Addr::new(192, 0, 2, 6),
            53,
            b"zero-ok",
            false,
        ),
    );
    let mut ipv6_zero = ipv6_udp_packet(
        "2001:db8::6".parse().expect("source"),
        10_006,
        "2001:db8::7".parse().expect("destination"),
        53,
        b"zero-bad",
    );
    ipv6_zero[40 + 6..40 + 8].copy_from_slice(&0u16.to_be_bytes());
    push_packet(&runtime, frame, &ipv6_zero);

    assert!(runtime.schedule_frame(local, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(graph.udp_state.borrow().metadata.len(), 1);
    assert_metadata(
        &graph.udp_state.borrow().metadata[0],
        Network::Udp,
        Ipv4Addr::new(10, 0, 0, 6).into(),
        10_006,
        Ipv4Addr::new(192, 0, 2, 6).into(),
        53,
    );
    assert_eq!(
        runtime
            .node_error_count(local, IpLocalError::BadChecksum.code())
            .expect("checksum counter"),
        1
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_local_reverse_fib_source_check_rejects_unusable_source_routes() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 24, 8, 8);
    let graph = LocalGraph::new(&runtime);
    let mut source_builder = FibSnapshotBuilder::new(graph.drop);
    let usable_source_lb = source_builder.add_single_path_load_balance(DpoProto::IP4, graph.udp);
    source_builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("usable source route"),
        usable_source_lb,
    );
    source_builder.add_ip4_drop_route(
        Ipv4Net::new(Ipv4Addr::new(203, 0, 113, 1), 32).expect("drop source route"),
    );
    source_builder.add_ip4_receive_route(
        Ipv4Net::new(Ipv4Addr::new(203, 0, 113, 2), 32).expect("receive source route"),
        graph.udp,
    );
    source_builder.add_ip4_punt_route(
        Ipv4Net::new(Ipv4Addr::new(203, 0, 113, 3), 32).expect("punt source route"),
        graph.punt,
    );
    let source_handle = IpLookupControlPlane::new(source_builder.build()).snapshot_handle();
    let control = IpLocalControlPlane::new(graph.nexts())
        .with_source_check(IpLocalSourceCheck::ReverseFib(source_handle));
    let local = runtime.nodes().register_internal(control.node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    for source in [
        Ipv4Addr::new(198, 51, 100, 8),
        Ipv4Addr::new(203, 0, 113, 250),
        Ipv4Addr::new(203, 0, 113, 1),
        Ipv4Addr::new(203, 0, 113, 2),
        Ipv4Addr::new(203, 0, 113, 3),
    ] {
        push_packet(
            &runtime,
            frame,
            &ipv4_udp_packet(
                source,
                10_007,
                Ipv4Addr::new(192, 0, 2, 7),
                53,
                b"source-check",
                true,
            ),
        );
    }

    assert!(runtime.schedule_frame(local, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(graph.udp_state.borrow().metadata.len(), 1);
    assert_metadata(
        &graph.udp_state.borrow().metadata[0],
        Network::Udp,
        Ipv4Addr::new(198, 51, 100, 8).into(),
        10_007,
        Ipv4Addr::new(192, 0, 2, 7).into(),
        53,
    );
    assert_eq!(
        runtime
            .node_error_count(local, IpLocalError::SourceCheckFailed.code())
            .expect("source check counter"),
        4
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_local_feature_arc_runs_before_end_of_arc_dispatch() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 24, 8, 8);
    let graph = LocalGraph::new(&runtime);
    let control = IpLocalControlPlane::new(graph.nexts());
    let mut features = FeatureArcControl::<IpLocalArc>::new();
    let feature_state = Rc::new(RefCell::new(CaptureState::default()));
    let feature = runtime.nodes().register_internal(ForwardNode {
        state: Rc::clone(&feature_state),
    });
    features
        .register_feature::<ForwardNode>(feature)
        .expect("register local feature");
    features
        .enable_feature::<ForwardNode>(9)
        .expect("enable local feature");
    let mut local_node = control.node();
    features.attach_start(&mut local_node);
    let local = runtime.nodes().register_internal(local_node);

    let good = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 8),
        10_008,
        Ipv4Addr::new(192, 0, 2, 8),
        53,
        b"feature",
        true,
    );
    let mut bad = good.clone();
    bad[26] ^= 0x80;
    let head_frame = runtime.alloc_frame_index().expect("alloc head frame");
    push_packet_on_interface(&runtime, head_frame, &good, 9);
    push_packet_on_interface(&runtime, head_frame, &bad, 9);
    assert!(
        runtime
            .schedule_frame(local, head_frame)
            .expect("schedule head")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run head"), 4);
    assert_eq!(feature_state.borrow().metadata.len(), 1);
    assert_eq!(graph.udp_state.borrow().metadata.len(), 1);
    assert_eq!(
        runtime
            .node_error_count(local, IpLocalError::BadChecksum.code())
            .expect("checksum counter"),
        1
    );

    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_input_lookup_receive_route_reaches_ip_local() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let graph = LocalGraph::new(&runtime);
    let local_control = IpLocalControlPlane::new(graph.nexts());
    let receive = runtime
        .nodes()
        .register_internal(local_control.receive_node());
    let mut builder = FibSnapshotBuilder::new(graph.drop);
    builder.add_ip4_route_dpo(
        Ipv4Net::new(Ipv4Addr::new(192, 0, 2, 9), 32).expect("receive route"),
        DpoId::receive(DpoProto::IP4, receive),
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            graph.drop,
            graph.punt,
            graph.drop,
            lookup,
            graph.drop,
            graph.drop,
            graph.reassembly,
        )));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet(
            Ipv4Addr::new(10, 0, 0, 9),
            10_009,
            Ipv4Addr::new(192, 0, 2, 9),
            53,
            b"receive-local",
            true,
        ),
    );

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(graph.udp_state.borrow().metadata.len(), 1);
    assert_metadata(
        &graph.udp_state.borrow().metadata[0],
        Network::Udp,
        Ipv4Addr::new(10, 0, 0, 9).into(),
        10_009,
        Ipv4Addr::new(192, 0, 2, 9).into(),
        53,
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

struct LocalGraph {
    drop: NodeId,
    punt: NodeId,
    tcp: NodeId,
    udp: NodeId,
    icmp: NodeId,
    reassembly: NodeId,
    punt_state: Rc<RefCell<CaptureState>>,
    tcp_state: Rc<RefCell<CaptureState>>,
    udp_state: Rc<RefCell<CaptureState>>,
    icmp_state: Rc<RefCell<CaptureState>>,
    reassembly_state: Rc<RefCell<CaptureState>>,
}

impl LocalGraph {
    fn new(runtime: &DataPlaneRuntime<TestNode>) -> Self {
        let punt_state = Rc::new(RefCell::new(CaptureState::default()));
        let tcp_state = Rc::new(RefCell::new(CaptureState::default()));
        let udp_state = Rc::new(RefCell::new(CaptureState::default()));
        let icmp_state = Rc::new(RefCell::new(CaptureState::default()));
        let reassembly_state = Rc::new(RefCell::new(CaptureState::default()));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let punt = runtime
            .nodes()
            .register_internal(CaptureNode::new(Rc::clone(&punt_state)));
        let tcp = runtime
            .nodes()
            .register_internal(CaptureNode::new(Rc::clone(&tcp_state)));
        let udp = runtime
            .nodes()
            .register_internal(CaptureNode::new(Rc::clone(&udp_state)));
        let icmp = runtime
            .nodes()
            .register_internal(CaptureNode::new(Rc::clone(&icmp_state)));
        let reassembly = runtime
            .nodes()
            .register_internal(CaptureNode::new(Rc::clone(&reassembly_state)));
        Self {
            drop,
            punt,
            tcp,
            udp,
            icmp,
            reassembly,
            punt_state,
            tcp_state,
            udp_state,
            icmp_state,
            reassembly_state,
        }
    }

    fn nexts(&self) -> [NodeId; IpLocalNext::COUNT] {
        IpLocalNext::nodes(
            self.drop,
            self.punt,
            self.tcp,
            self.udp,
            self.icmp,
            self.reassembly,
        )
    }
}

fn push_packet(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
) {
    push_packet_with_metadata(runtime, frame, RouteMetadata::default(), packet);
}

fn push_packet_on_interface(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    ingress_interface: u32,
) {
    push_packet_with_metadata(
        runtime,
        frame,
        RouteMetadata {
            ingress_interface: Some(ingress_interface),
            ..Default::default()
        },
        packet,
    );
}

fn push_packet_with_metadata(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    metadata: RouteMetadata,
    packet: &[u8],
) {
    let buffer = runtime
        .alloc_index_with_bytes(metadata, packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
}

fn assert_metadata(
    metadata: &RouteMetadata,
    network: Network,
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) {
    assert_eq!(metadata.network, network);
    assert_eq!(metadata.source, Some(SocksAddr::ip(source, source_port)));
    assert_eq!(
        metadata.destination,
        Some(SocksAddr::ip(destination, destination_port))
    );
}

fn ipv4_udp_packet(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    payload: &[u8],
    checksum_enabled: bool,
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 17, 8 + payload.len());
    let udp = 20;
    packet[udp..udp + 2].copy_from_slice(&source_port.to_be_bytes());
    packet[udp + 2..udp + 4].copy_from_slice(&destination_port.to_be_bytes());
    packet[udp + 4..udp + 6].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[udp + 8..].copy_from_slice(payload);
    if checksum_enabled {
        let checksum = ipv4_l4_checksum(source, destination, 17, &packet[udp..]);
        packet[udp + 6..udp + 8].copy_from_slice(&udp_checksum_wire(checksum).to_be_bytes());
    }
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv6_udp_packet(
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv6_packet(source, destination, 17, 8 + payload.len());
    let udp = 40;
    packet[udp..udp + 2].copy_from_slice(&source_port.to_be_bytes());
    packet[udp + 2..udp + 4].copy_from_slice(&destination_port.to_be_bytes());
    packet[udp + 4..udp + 6].copy_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet[udp + 8..].copy_from_slice(payload);
    let checksum = ipv6_l4_checksum(source, destination, 17, &packet[udp..]);
    packet[udp + 6..udp + 8].copy_from_slice(&udp_checksum_wire(checksum).to_be_bytes());
    packet
}

fn ipv4_tcp_packet(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 6, 20 + payload.len());
    write_tcp_segment(&mut packet[20..], source_port, destination_port, payload);
    let checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
    packet[20 + 16..20 + 18].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv6_tcp_packet(
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv6_packet(source, destination, 6, 20 + payload.len());
    write_tcp_segment(&mut packet[40..], source_port, destination_port, payload);
    let checksum = ipv6_l4_checksum(source, destination, 6, &packet[40..]);
    packet[40 + 16..40 + 18].copy_from_slice(&checksum.to_be_bytes());
    packet
}

fn write_tcp_segment(segment: &mut [u8], source_port: u16, destination_port: u16, payload: &[u8]) {
    segment[0..2].copy_from_slice(&source_port.to_be_bytes());
    segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
    segment[12] = 0x50;
    segment[13] = 0x18;
    segment[20..].copy_from_slice(payload);
}

fn ipv4_icmp_packet(source: Ipv4Addr, destination: Ipv4Addr, payload: &[u8]) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 1, 8 + payload.len());
    let icmp = 20;
    packet[icmp] = 8;
    packet[icmp + 4..icmp + 6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[icmp + 6..icmp + 8].copy_from_slice(&1u16.to_be_bytes());
    packet[icmp + 8..].copy_from_slice(payload);
    let checksum = internet_checksum(&packet[icmp..]);
    packet[icmp + 2..icmp + 4].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv6_icmp_packet(source: Ipv6Addr, destination: Ipv6Addr, payload: &[u8]) -> Vec<u8> {
    let mut packet = ipv6_packet(source, destination, 58, 8 + payload.len());
    let icmp = 40;
    packet[icmp] = 128;
    packet[icmp + 4..icmp + 6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[icmp + 6..icmp + 8].copy_from_slice(&1u16.to_be_bytes());
    packet[icmp + 8..].copy_from_slice(payload);
    let checksum = ipv6_l4_checksum(source, destination, 58, &packet[icmp..]);
    packet[icmp + 2..icmp + 4].copy_from_slice(&checksum.to_be_bytes());
    packet
}

fn ipv4_protocol_packet(
    source: Ipv4Addr,
    destination: Ipv4Addr,
    protocol: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, protocol, payload.len());
    packet[20..].copy_from_slice(payload);
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv4_first_fragment(packet: &[u8], payload_len: usize) -> Vec<u8> {
    let mut fragment = packet[..20 + payload_len].to_vec();
    fragment[2..4].copy_from_slice(&((20 + payload_len) as u16).to_be_bytes());
    fragment[6..8].copy_from_slice(&0x2000u16.to_be_bytes());
    update_ipv4_header_checksum(&mut fragment);
    fragment
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
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    let checksum = internet_checksum(&packet[..ihl]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
}

fn ipv4_l4_checksum(source: Ipv4Addr, destination: Ipv4Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len());
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.push(0);
    pseudo.push(protocol);
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(40 + segment.len());
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, protocol]);
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn udp_checksum_wire(checksum: u16) -> u16 {
    if checksum == 0 { 0xffff } else { checksum }
}

fn internet_checksum(bytes: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in bytes.chunks(2) {
        let word = if chunk.len() == 2 {
            u16::from_be_bytes([chunk[0], chunk[1]]) as u32
        } else {
            (chunk[0] as u32) << 8
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}

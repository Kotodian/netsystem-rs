use std::cell::RefCell;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, ForwardingDpoType, ForwardingMetadata, InternalNode, Node,
    NodeResult,
};
use hammer_core::error::CoreResult;
use hammer_core::protocol::ip::IpVersion;
use hammer_service::data_plane::DropNode;
use hammer_service::net::{
    DpoId, FibSnapshotBuilder, IpInputNext, IpInputNode, IpLookupControlPlane, IpLookupNode,
};
use ipnet::{Ipv4Net, Ipv6Net};

#[derive(Default)]
struct SinkState {
    payloads: Vec<Vec<u8>>,
    forwarding: Vec<Option<ForwardingMetadata>>,
    frame_lens: Vec<usize>,
}

struct SinkNode {
    state: Rc<RefCell<SinkState>>,
}

impl Node<TestNode> for SinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        self.state.borrow_mut().frame_lens.push(frame.pending_len());
        for buffer in frame.drain_pending() {
            let metadata = runtime.metadata(buffer)?;
            let payload = runtime.copy_current_chain(buffer)?;
            runtime.free_index(buffer);
            let mut state = self.state.borrow_mut();
            state.forwarding.push(metadata.forwarding);
            state.payloads.push(payload);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for SinkNode {}

struct CorruptCurrentHeaderNode {
    next: hammer_adapter::NodeId,
}

impl Node<TestNode> for CorruptCurrentHeaderNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.pending_indices().iter().copied() {
            runtime.get_buffer_mut(index)?.current_mut()[0] = 0;
        }
        Ok(NodeResult::next_current(self.next))
    }
}

impl InternalNode<TestNode> for CorruptCurrentHeaderNode {}

enum TestNode {
    Sink(SinkNode),
    Drop(DropNode),
    IpInput(IpInputNode),
    IpLookup(IpLookupNode),
    Corrupt(CorruptCurrentHeaderNode),
}

impl From<SinkNode> for TestNode {
    fn from(node: SinkNode) -> Self {
        Self::Sink(node)
    }
}

impl From<DropNode> for TestNode {
    fn from(node: DropNode) -> Self {
        Self::Drop(node)
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

impl From<CorruptCurrentHeaderNode> for TestNode {
    fn from(node: CorruptCurrentHeaderNode) -> Self {
        Self::Corrupt(node)
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
            Self::Sink(node) => node.process(runtime, frame),
            Self::Drop(node) => node.process(runtime, frame),
            Self::IpInput(node) => node.process(runtime, frame),
            Self::IpLookup(node) => node.process(runtime, frame),
            Self::Corrupt(node) => node.process(runtime, frame),
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
fn ip_lookup_node_uses_ipv4_mtrie_longest_prefix_match() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let default_state = Rc::new(RefCell::new(SinkState::default()));
    let specific_state = Rc::new(RefCell::new(SinkState::default()));
    let host_state = Rc::new(RefCell::new(SinkState::default()));
    let default = register_sink(&runtime, &default_state);
    let specific = register_sink(&runtime, &specific_state);
    let host = register_sink(&runtime, &host_state);
    let drop = runtime.nodes().register_internal(DropNode::new());

    let mut builder = FibSnapshotBuilder::new(drop);
    let default_lb = add_single_path(&mut builder, IpVersion::V4, default);
    let specific_lb = add_single_path(&mut builder, IpVersion::V4, specific);
    let host_lb = add_single_path(&mut builder, IpVersion::V4, host);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
        default_lb,
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("specific route"),
        specific_lb,
    );
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 42), 32).expect("host route"),
        host_lb,
    );
    let control = IpLookupControlPlane::new(builder.build());
    let lookup = control.node();
    assert_internal_node(&lookup);
    let lookup = runtime.nodes().register_internal(lookup);

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_001, [203, 0, 113, 7], 53, b"default"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_002, [198, 51, 100, 7], 53, b"specific"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_003, [198, 51, 100, 42], 53, b"host"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_payloads(&default_state, &[b"default".as_slice()]);
    assert_payloads(&specific_state, &[b"specific".as_slice()]);
    assert_payloads(&host_state, &[b"host".as_slice()]);
    assert_frame_lens(&default_state, &[1]);
    assert_frame_lens(&specific_state, &[1]);
    assert_frame_lens(&host_state, &[1]);
    assert_forwarding(&host_state, host_lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_speculative_enqueue_keeps_same_next_in_current_frame() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let state = Rc::new(RefCell::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibSnapshotBuilder::new(drop);
    let lb = add_single_path(&mut builder, IpVersion::V4, sink);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("default route"),
        lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_011, [203, 0, 113, 1], 53, b"one"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_012, [203, 0, 113, 2], 53, b"two"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 10_013, [203, 0, 113, 3], 53, b"three"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_payloads(
        &state,
        &[b"one".as_slice(), b"two".as_slice(), b"three".as_slice()],
    );
    assert_frame_lens(&state, &[3]);
    assert_forwarding(&state, lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_node_uses_ipv6_hash_prefix_order() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let default_state = Rc::new(RefCell::new(SinkState::default()));
    let subnet_state = Rc::new(RefCell::new(SinkState::default()));
    let host_state = Rc::new(RefCell::new(SinkState::default()));
    let default = register_sink(&runtime, &default_state);
    let subnet = register_sink(&runtime, &subnet_state);
    let host = register_sink(&runtime, &host_state);
    let drop = runtime.nodes().register_internal(DropNode::new());

    let mut builder = FibSnapshotBuilder::new(drop);
    let default_lb = add_single_path(&mut builder, IpVersion::V6, default);
    let subnet_lb = add_single_path(&mut builder, IpVersion::V6, subnet);
    let host_lb = add_single_path(&mut builder, IpVersion::V6, host);
    builder.add_ip6_route(
        Ipv6Net::new(Ipv6Addr::UNSPECIFIED, 0).expect("default route"),
        default_lb,
    );
    builder.add_ip6_route(
        Ipv6Net::new("2001:db8:64::".parse().expect("subnet"), 64).expect("subnet route"),
        subnet_lb,
    );
    builder.add_ip6_route(
        Ipv6Net::new("2001:db8:64::42".parse().expect("host"), 128).expect("host route"),
        host_lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());

    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv6_udp_packet("2001:db8::1", 20_001, "2001:db8:ffff::1", 53, b"default"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv6_udp_packet("2001:db8::1", 20_002, "2001:db8:64::7", 53, b"subnet"),
    );
    push_packet(
        &runtime,
        frame,
        &ipv6_udp_packet("2001:db8::1", 20_003, "2001:db8:64::42", 53, b"host"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_payloads(&default_state, &[b"default".as_slice()]);
    assert_payloads(&subnet_state, &[b"subnet".as_slice()]);
    assert_payloads(&host_state, &[b"host".as_slice()]);
    assert_forwarding(&host_state, host_lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_node_sends_miss_to_drop_dpo() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(FibSnapshotBuilder::new(drop).build()).node());
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 30_001, [198, 51, 100, 7], 53, b"miss"),
    );

    assert!(runtime.schedule_frame(lookup, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_control_plane_publish_replaces_forwarding_snapshot() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let first_state = Rc::new(RefCell::new(SinkState::default()));
    let second_state = Rc::new(RefCell::new(SinkState::default()));
    let first = register_sink(&runtime, &first_state);
    let second = register_sink(&runtime, &second_state);
    let drop = runtime.nodes().register_internal(DropNode::new());

    let mut first_builder = FibSnapshotBuilder::new(drop);
    let first_lb = add_single_path(&mut first_builder, IpVersion::V4, first);
    first_builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("first default"),
        first_lb,
    );
    let control = IpLookupControlPlane::new(first_builder.build());
    let lookup = runtime.nodes().register_internal(control.node());

    let first_frame = runtime.alloc_frame_index().expect("alloc first frame");
    push_packet(
        &runtime,
        first_frame,
        &ipv4_udp_packet([10, 0, 0, 1], 50_001, [203, 0, 113, 1], 53, b"first"),
    );
    assert!(
        runtime
            .schedule_frame(lookup, first_frame)
            .expect("schedule")
    );
    assert_eq!(runtime.run_ready_nodes().expect("run first"), 2);
    assert_payloads(&first_state, &[b"first".as_slice()]);

    let mut second_builder = FibSnapshotBuilder::new(drop);
    let second_lb = add_single_path(&mut second_builder, IpVersion::V4, second);
    second_builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).expect("second default"),
        second_lb,
    );
    control.publish(second_builder.build()).expect("publish");

    let second_frame = runtime.alloc_frame_index().expect("alloc second frame");
    push_packet(
        &runtime,
        second_frame,
        &ipv4_udp_packet([10, 0, 0, 1], 50_002, [203, 0, 113, 2], 53, b"second"),
    );
    assert!(
        runtime
            .schedule_frame(lookup, second_frame)
            .expect("schedule")
    );
    assert_eq!(runtime.run_ready_nodes().expect("run second"), 2);
    assert_payloads(&second_state, &[b"second".as_slice()]);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_input_to_lookup_graph_routes_packet_by_fib() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let state = Rc::new(RefCell::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibSnapshotBuilder::new(drop);
    let lb = add_single_path(&mut builder, IpVersion::V4, sink);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("route"),
        lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            drop, drop, drop, lookup, drop, drop, drop,
        )));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 40_001, [198, 51, 100, 17], 853, b"graph"),
    );

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_payloads(&state, &[b"graph".as_slice()]);
    assert_forwarding(&state, lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ip_lookup_uses_ip_input_cursor_without_reparsing_current_header() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let state = Rc::new(RefCell::new(SinkState::default()));
    let sink = register_sink(&runtime, &state);
    let drop = runtime.nodes().register_internal(DropNode::new());
    let mut builder = FibSnapshotBuilder::new(drop);
    let lb = add_single_path(&mut builder, IpVersion::V4, sink);
    builder.add_ip4_route(
        Ipv4Net::new(Ipv4Addr::new(198, 51, 100, 0), 24).expect("route"),
        lb,
    );
    let lookup = runtime
        .nodes()
        .register_internal(IpLookupControlPlane::new(builder.build()).node());
    let corrupt = runtime
        .nodes()
        .register_internal(CorruptCurrentHeaderNode { next: lookup });
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            drop, drop, drop, corrupt, drop, drop, drop,
        )));
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(
        &runtime,
        frame,
        &ipv4_udp_packet([10, 0, 0, 1], 40_011, [198, 51, 100, 17], 853, b"cursor"),
    );

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    {
        let state = state.borrow();
        assert_eq!(state.payloads.len(), 1);
        assert_eq!(&state.payloads[0][28..], b"cursor");
    }
    assert_forwarding(&state, lb.get());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

fn register_sink(
    runtime: &DataPlaneRuntime<TestNode>,
    state: &Rc<RefCell<SinkState>>,
) -> hammer_adapter::NodeId {
    runtime.nodes().register_internal(SinkNode {
        state: Rc::clone(state),
    })
}

fn add_single_path(
    builder: &mut FibSnapshotBuilder,
    version: IpVersion,
    node: hammer_adapter::NodeId,
) -> hammer_service::net::LoadBalanceIndex {
    let adjacency = builder.add_adjacency(version, node);
    builder.add_load_balance([DpoId::adjacency(version, adjacency, node)])
}

fn push_packet(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
) {
    let index = runtime
        .alloc_index_with_bytes(Default::default(), packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");
}

fn assert_payloads(state: &Rc<RefCell<SinkState>>, expected_payloads: &[&[u8]]) {
    let payloads = state
        .borrow()
        .payloads
        .iter()
        .map(|packet| udp_payload(packet).to_vec())
        .collect::<Vec<_>>();
    let expected = expected_payloads
        .iter()
        .map(|payload| payload.to_vec())
        .collect::<Vec<_>>();
    assert_eq!(payloads, expected);
}

fn assert_frame_lens(state: &Rc<RefCell<SinkState>>, expected_lens: &[usize]) {
    assert_eq!(&*state.borrow().frame_lens, expected_lens);
}

fn assert_forwarding(state: &Rc<RefCell<SinkState>>, load_balance_index: u32) {
    let forwarding = state.borrow().forwarding[0].expect("forwarding metadata");
    assert_eq!(forwarding.load_balance_index, load_balance_index);
    assert_eq!(forwarding.dpo_type, ForwardingDpoType::Adjacency);
}

fn udp_payload(packet: &[u8]) -> &[u8] {
    match packet[0] >> 4 {
        4 => {
            let ihl = ((packet[0] & 0x0f) as usize) * 4;
            &packet[ihl + 8..]
        }
        6 => &packet[40 + 8..],
        _ => panic!("test packet is not IP"),
    }
}

fn ipv4_udp_packet(
    source: [u8; 4],
    source_port: u16,
    destination: [u8; 4],
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let total_len = 20 + 8 + payload.len();
    let mut packet = vec![0u8; total_len];
    packet[0] = 0x45;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
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
    packet
}

fn ipv6_udp_packet(
    source: &str,
    source_port: u16,
    destination: &str,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let source = source.parse::<Ipv6Addr>().expect("source addr");
    let destination = destination.parse::<Ipv6Addr>().expect("destination addr");
    let payload_len = 8 + payload.len();
    let mut packet = vec![0u8; 40 + payload_len];
    packet[0] = 0x60;
    packet[4..6].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[6] = 17;
    packet[7] = 64;
    packet[8..24].copy_from_slice(&source.octets());
    packet[24..40].copy_from_slice(&destination.octets());
    packet[40..42].copy_from_slice(&source_port.to_be_bytes());
    packet[42..44].copy_from_slice(&destination_port.to_be_bytes());
    packet[44..46].copy_from_slice(&(payload_len as u16).to_be_bytes());
    packet[48..].copy_from_slice(payload);
    packet
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

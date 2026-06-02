use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr};
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Network, Node, NodeId, NodeResult, RouteMetadata,
    SocksAddr,
};
use hammer_core::error::CoreResult;
use hammer_infra::vec::Vec;
use hammer_service::net::{
    DpoId, DpoProto, FibSnapshotBuilder, IpInputNext, IpInputNode, IpLookupControlPlane,
    IpLookupNode,
};
use hammer_service::tun::{MemoryTunDevice, TunInputDriverNode, TunOutputDriverNode};
use ipnet::Ipv4Net;

struct CaptureNode {
    next: NodeId,
    metadata: Rc<RefCell<Vec<RouteMetadata>>>,
}

impl Node<TestNode> for CaptureNode {
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

impl InternalNode<TestNode> for CaptureNode {}

enum TestNode {
    TunInput(TunInputDriverNode<hammer_service::tun::MemoryTunInput>),
    IpInput(IpInputNode),
    Capture(CaptureNode),
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

impl From<CaptureNode> for TestNode {
    fn from(node: CaptureNode) -> Self {
        Self::Capture(node)
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
            Self::Capture(node) => node.process(runtime, frame),
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
    let capture = runtime.nodes().register_internal(CaptureNode {
        next: output,
        metadata: Rc::clone(&captured),
    });
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            capture, capture, capture, capture, capture, capture, capture,
        )));
    let driver =
        runtime
            .nodes()
            .register_driver(TunInputDriverNode::new(device.input(), "tun0", ip_input));
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
    let ip_input = runtime
        .nodes()
        .register_internal(IpInputNode::new(IpInputNext::nodes(
            output, output, output, lookup, output, output, output,
        )));
    let driver =
        runtime
            .nodes()
            .register_driver(TunInputDriverNode::new(device.input(), "tun0", ip_input));
    let frame = runtime.alloc_frame_index().expect("alloc frame");

    runtime
        .schedule_driver_frame(driver, frame)
        .expect("schedule tun driver");

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_eq!(device.drain_output_batch_sizes(), vec![2]);
    assert_eq!(device.drain_output(), packets);
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

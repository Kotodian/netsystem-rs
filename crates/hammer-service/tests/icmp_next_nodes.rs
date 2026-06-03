use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, BufferNodeError, DataPlaneRuntime, IcmpErrorMetadata, InternalNode, Node,
    NodeResult, RouteMetadata,
};
use hammer_core::error::CoreResult;
use hammer_service::data_plane::DropNode;
use hammer_service::net::{
    IcmpEchoRequestNext, IcmpEchoRequestNode, IcmpErrorNext, IcmpErrorNode, IcmpNodeError,
};

const LOCAL_ORIGINATED_TTL: u8 = 64;

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
    node_errors: Vec<Option<BufferNodeError>>,
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
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.drain_pending() {
            let packet = runtime.copy_current_chain(index)?;
            let node_error = runtime.node_error(index)?;
            let mut state = self.state.borrow_mut();
            state.packets.push(packet.into_iter().collect());
            state.node_errors.push(node_error);
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for CaptureNode {}

enum TestNode {
    Drop(DropNode),
    Capture(CaptureNode),
    Echo(IcmpEchoRequestNode),
    Error(IcmpErrorNode),
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

impl From<IcmpEchoRequestNode> for TestNode {
    fn from(node: IcmpEchoRequestNode) -> Self {
        Self::Echo(node)
    }
}

impl From<IcmpErrorNode> for TestNode {
    fn from(node: IcmpErrorNode) -> Self {
        Self::Error(node)
    }
}

impl Node<TestNode> for TestNode {
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::Drop(node) => node.process(runtime, frame),
            Self::Capture(node) => node.process(runtime, frame),
            Self::Echo(node) => node.process(runtime, frame),
            Self::Error(node) => node.process(runtime, frame),
        }
    }
}

#[test]
fn icmp_echo_request_rewrites_ipv4_request_to_lookup_reply() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let lookup_state = Rc::new(RefCell::new(CaptureState::default()));
    let drop_state = Rc::new(RefCell::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&lookup_state)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&drop_state)));
    let echo =
        runtime
            .nodes()
            .register_internal(IcmpEchoRequestNode::new(IcmpEchoRequestNext::nodes(
                lookup, drop,
            )));
    let packet = ipv4_icmp_echo_packet(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 0, 2, 1),
        8,
        b"echo4-payload",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet, RouteMetadata::default());

    assert!(runtime.schedule_frame(echo, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(drop_state.borrow().packets.is_empty());
    let lookup_state = lookup_state.borrow();
    assert_eq!(lookup_state.packets.len(), 1);
    assert_eq!(lookup_state.node_errors, vec![None]);
    let reply = &lookup_state.packets[0];
    assert_eq!(reply[8], LOCAL_ORIGINATED_TTL);
    assert_eq!(&reply[12..16], &[192, 0, 2, 1]);
    assert_eq!(&reply[16..20], &[10, 0, 0, 1]);
    assert_eq!(reply[20], 0);
    assert_eq!(reply[21], 0);
    assert_eq!(&reply[24..28], &packet[24..28]);
    assert_eq!(&reply[28..], &packet[28..]);
    assert_eq!(internet_checksum(&reply[..20]), 0);
    assert_eq!(internet_checksum(&reply[20..]), 0);
}

#[test]
fn icmp_echo_request_rewrites_ipv6_request_to_lookup_reply() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let lookup_state = Rc::new(RefCell::new(CaptureState::default()));
    let drop_state = Rc::new(RefCell::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&lookup_state)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&drop_state)));
    let echo =
        runtime
            .nodes()
            .register_internal(IcmpEchoRequestNode::new(IcmpEchoRequestNext::nodes(
                lookup, drop,
            )));
    let source = "2001:db8::1".parse().expect("source");
    let destination = "2001:db8::2".parse().expect("destination");
    let packet = ipv6_icmp_echo_packet(source, destination, 128, b"echo6-payload");
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet, RouteMetadata::default());

    assert!(runtime.schedule_frame(echo, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(drop_state.borrow().packets.is_empty());
    let lookup_state = lookup_state.borrow();
    assert_eq!(lookup_state.packets.len(), 1);
    assert_eq!(lookup_state.node_errors, vec![None]);
    let reply = &lookup_state.packets[0];
    assert_eq!(reply[7], LOCAL_ORIGINATED_TTL);
    assert_eq!(&reply[8..24], &destination.octets());
    assert_eq!(&reply[24..40], &source.octets());
    assert_eq!(reply[40], 129);
    assert_eq!(reply[41], 0);
    assert_eq!(&reply[44..48], &packet[44..48]);
    assert_eq!(&reply[48..], &packet[48..]);
    assert_eq!(ipv6_l4_checksum(destination, source, 58, &reply[40..]), 0);
}

#[test]
fn icmp_echo_request_sends_wrong_type_to_drop_with_node_error() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let lookup_state = Rc::new(RefCell::new(CaptureState::default()));
    let drop_state = Rc::new(RefCell::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&lookup_state)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&drop_state)));
    let echo =
        runtime
            .nodes()
            .register_internal(IcmpEchoRequestNode::new(IcmpEchoRequestNext::nodes(
                lookup, drop,
            )));
    let packet = ipv4_icmp_echo_packet(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 0, 2, 1),
        0,
        b"reply",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet, RouteMetadata::default());

    assert!(runtime.schedule_frame(echo, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(lookup_state.borrow().packets.is_empty());
    assert_eq!(drop_state.borrow().packets, vec![packet]);
    assert_eq!(
        drop_state.borrow().node_errors,
        vec![Some(BufferNodeError::new(
            echo,
            IcmpNodeError::WrongType.code()
        ))]
    );
}

#[test]
fn icmp_echo_request_sends_bad_checksum_to_drop_with_node_error() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let lookup_state = Rc::new(RefCell::new(CaptureState::default()));
    let drop_state = Rc::new(RefCell::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&lookup_state)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&drop_state)));
    let echo =
        runtime
            .nodes()
            .register_internal(IcmpEchoRequestNode::new(IcmpEchoRequestNext::nodes(
                lookup, drop,
            )));
    let mut packet = ipv4_icmp_echo_packet(
        Ipv4Addr::new(10, 0, 0, 1),
        Ipv4Addr::new(192, 0, 2, 1),
        8,
        b"bad-checksum",
    );
    packet[22] ^= 0xff;
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet, RouteMetadata::default());

    assert!(runtime.schedule_frame(echo, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(lookup_state.borrow().packets.is_empty());
    assert_eq!(drop_state.borrow().packets, vec![packet]);
    assert_eq!(
        drop_state.borrow().node_errors,
        vec![Some(BufferNodeError::new(
            echo,
            IcmpNodeError::BadChecksum.code()
        ))]
    );
}

#[test]
fn icmp_error_node_synthesizes_ipv4_time_exceeded_to_lookup() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let lookup_state = Rc::new(RefCell::new(CaptureState::default()));
    let drop_state = Rc::new(RefCell::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&lookup_state)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&drop_state)));
    let error = runtime
        .nodes()
        .register_internal(IcmpErrorNode::new(IcmpErrorNext::nodes(drop, lookup)));
    let original = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 10),
        Ipv4Addr::new(198, 51, 100, 1),
        b"expired-hop",
    );
    let metadata = RouteMetadata {
        icmp_error: Some(IcmpErrorMetadata::ipv4_time_exceeded()),
        icmp_error_source: Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9))),
        ..RouteMetadata::default()
    };
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &original, metadata);

    assert!(runtime.schedule_frame(error, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(drop_state.borrow().packets.is_empty());
    let lookup_state = lookup_state.borrow();
    assert_eq!(lookup_state.packets.len(), 1);
    assert_eq!(lookup_state.node_errors, vec![None]);
    let reply = &lookup_state.packets[0];
    assert_eq!(reply[8], LOCAL_ORIGINATED_TTL);
    assert_eq!(reply[9], 1);
    assert_eq!(&reply[12..16], &[203, 0, 113, 9]);
    assert_eq!(&reply[16..20], &[10, 0, 0, 10]);
    assert_eq!(reply[20], 11);
    assert_eq!(reply[21], 0);
    assert_eq!(&reply[24..28], &[0, 0, 0, 0]);
    assert_eq!(&reply[28..], &original[..]);
    assert_eq!(internet_checksum(&reply[..20]), 0);
    assert_eq!(internet_checksum(&reply[20..]), 0);
}

#[test]
fn icmp_error_node_synthesizes_ipv6_packet_too_big_to_lookup() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let lookup_state = Rc::new(RefCell::new(CaptureState::default()));
    let drop_state = Rc::new(RefCell::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&lookup_state)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&drop_state)));
    let error = runtime
        .nodes()
        .register_internal(IcmpErrorNode::new(IcmpErrorNext::nodes(drop, lookup)));
    let source = "2001:db8::10".parse().expect("source");
    let destination = "2001:db8::20".parse().expect("destination");
    let original = ipv6_udp_packet(source, destination, b"too-big");
    let local_source = "2001:db8::ff".parse().expect("local source");
    let metadata = RouteMetadata {
        icmp_error: Some(IcmpErrorMetadata::ipv6_packet_too_big(1280)),
        icmp_error_source: Some(IpAddr::V6(local_source)),
        ..RouteMetadata::default()
    };
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &original, metadata);

    assert!(runtime.schedule_frame(error, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(drop_state.borrow().packets.is_empty());
    let lookup_state = lookup_state.borrow();
    assert_eq!(lookup_state.packets.len(), 1);
    assert_eq!(lookup_state.node_errors, vec![None]);
    let reply = &lookup_state.packets[0];
    assert_eq!(reply[6], 58);
    assert_eq!(reply[7], LOCAL_ORIGINATED_TTL);
    assert_eq!(&reply[8..24], &local_source.octets());
    assert_eq!(&reply[24..40], &source.octets());
    assert_eq!(reply[40], 2);
    assert_eq!(reply[41], 0);
    assert_eq!(&reply[44..48], &1280u32.to_be_bytes());
    assert_eq!(&reply[48..], &original[..]);
    assert_eq!(ipv6_l4_checksum(local_source, source, 58, &reply[40..]), 0);
}

#[test]
fn icmp_error_node_drops_when_metadata_is_missing() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let lookup_state = Rc::new(RefCell::new(CaptureState::default()));
    let drop_state = Rc::new(RefCell::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&lookup_state)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&drop_state)));
    let error = runtime
        .nodes()
        .register_internal(IcmpErrorNode::new(IcmpErrorNext::nodes(drop, lookup)));
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 10),
        Ipv4Addr::new(198, 51, 100, 1),
        b"no-metadata",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet, RouteMetadata::default());

    assert!(runtime.schedule_frame(error, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(lookup_state.borrow().packets.is_empty());
    assert_eq!(drop_state.borrow().packets, vec![packet]);
    assert_eq!(
        drop_state.borrow().node_errors,
        vec![Some(BufferNodeError::new(
            error,
            IcmpNodeError::MissingMetadata.code()
        ))]
    );
}

#[test]
fn icmp_error_node_drops_when_local_source_is_missing() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let lookup_state = Rc::new(RefCell::new(CaptureState::default()));
    let drop_state = Rc::new(RefCell::new(CaptureState::default()));
    let lookup = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&lookup_state)));
    let drop = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&drop_state)));
    let error = runtime
        .nodes()
        .register_internal(IcmpErrorNode::new(IcmpErrorNext::nodes(drop, lookup)));
    let packet = ipv4_udp_packet(
        Ipv4Addr::new(10, 0, 0, 10),
        Ipv4Addr::new(198, 51, 100, 1),
        b"no-source",
    );
    let metadata = RouteMetadata {
        icmp_error: Some(IcmpErrorMetadata::ipv4_time_exceeded()),
        ..RouteMetadata::default()
    };
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet, metadata);

    assert!(runtime.schedule_frame(error, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(lookup_state.borrow().packets.is_empty());
    assert_eq!(drop_state.borrow().packets, vec![packet]);
    assert_eq!(
        drop_state.borrow().node_errors,
        vec![Some(BufferNodeError::new(
            error,
            IcmpNodeError::MissingSource.code()
        ))]
    );
}

fn push_packet(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    metadata: RouteMetadata,
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

fn ipv4_icmp_echo_packet(
    source: Ipv4Addr,
    destination: Ipv4Addr,
    icmp_type: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 1, 8 + payload.len());
    let icmp = 20;
    packet[icmp] = icmp_type;
    packet[icmp + 1] = 0;
    packet[icmp + 4..icmp + 6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[icmp + 6..icmp + 8].copy_from_slice(&1u16.to_be_bytes());
    packet[icmp + 8..].copy_from_slice(payload);
    let checksum = internet_checksum(&packet[icmp..]);
    packet[icmp + 2..icmp + 4].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv6_icmp_echo_packet(
    source: Ipv6Addr,
    destination: Ipv6Addr,
    icmp_type: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv6_packet(source, destination, 58, 8 + payload.len());
    let icmp = 40;
    packet[icmp] = icmp_type;
    packet[icmp + 1] = 0;
    packet[icmp + 4..icmp + 6].copy_from_slice(&0x1234u16.to_be_bytes());
    packet[icmp + 6..icmp + 8].copy_from_slice(&1u16.to_be_bytes());
    packet[icmp + 8..].copy_from_slice(payload);
    let checksum = ipv6_l4_checksum(source, destination, 58, &packet[icmp..]);
    packet[icmp + 2..icmp + 4].copy_from_slice(&checksum.to_be_bytes());
    packet
}

fn ipv4_udp_packet(source: Ipv4Addr, destination: Ipv4Addr, payload: &[u8]) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 17, 8 + payload.len());
    let udp = 20;
    packet[udp..udp + 2].copy_from_slice(&1234u16.to_be_bytes());
    packet[udp + 2..udp + 4].copy_from_slice(&4321u16.to_be_bytes());
    packet[udp + 4..udp + 6].copy_from_slice(&(8 + payload.len() as u16).to_be_bytes());
    packet[udp + 8..].copy_from_slice(payload);
    update_ipv4_header_checksum(&mut packet);
    packet
}

fn ipv6_udp_packet(source: Ipv6Addr, destination: Ipv6Addr, payload: &[u8]) -> Vec<u8> {
    let mut packet = ipv6_packet(source, destination, 17, 8 + payload.len());
    let udp = 40;
    packet[udp..udp + 2].copy_from_slice(&1234u16.to_be_bytes());
    packet[udp + 2..udp + 4].copy_from_slice(&4321u16.to_be_bytes());
    packet[udp + 4..udp + 6].copy_from_slice(&(8 + payload.len() as u16).to_be_bytes());
    packet[udp + 8..].copy_from_slice(payload);
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
    packet[8] = 1;
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
    packet[7] = 1;
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

fn ipv6_l4_checksum(source: Ipv6Addr, destination: Ipv6Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::new();
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.extend_from_slice(&(segment.len() as u32).to_be_bytes());
    pseudo.extend_from_slice(&[0, 0, 0, protocol]);
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
}

fn _metadata_addr(addr: IpAddr) -> hammer_core::SocksAddr {
    hammer_core::SocksAddr::ip(addr, 0)
}

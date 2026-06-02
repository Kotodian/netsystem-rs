use std::cell::RefCell;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::rc::Rc;

use hammer_adapter::{
    BufferFrame, BufferNodeError, DataPlaneRuntime, InternalNode, Node, NodeResult, RouteMetadata,
};
use hammer_core::error::CoreResult;
use hammer_service::data_plane::DropNode;
use hammer_service::net::{IcmpInputControlPlane, IcmpInputError, IcmpInputNode, IpVersion};

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
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
    IcmpInput(IcmpInputNode),
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

impl From<IcmpInputNode> for TestNode {
    fn from(node: IcmpInputNode) -> Self {
        Self::IcmpInput(node)
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
            Self::IcmpInput(node) => node.process(runtime, frame),
        }
    }
}

#[test]
fn icmp_input_dispatches_ipv4_echo_request_by_type() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let echo_state = Rc::new(RefCell::new(CaptureState::default()));
    let punt_state = Rc::new(RefCell::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&echo_state)));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&punt_state)));
    let control = IcmpInputControlPlane::new(punt);
    control
        .register_type(IpVersion::V4, 8, echo)
        .expect("register echo request");
    let icmp_input = runtime.nodes().register_internal(control.node());
    let packet = ipv4_icmp_packet(8, 0, b"echo4");
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet);

    assert!(runtime.schedule_frame(icmp_input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.borrow().packets, vec![packet]);
    assert!(echo_state.borrow().node_errors[0].is_none());
    assert!(punt_state.borrow().packets.is_empty());
}

#[test]
fn icmp_input_dispatches_ipv6_echo_request_by_type() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let echo_state = Rc::new(RefCell::new(CaptureState::default()));
    let punt_state = Rc::new(RefCell::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&echo_state)));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&punt_state)));
    let control = IcmpInputControlPlane::new(punt);
    control
        .register_type(IpVersion::V6, 128, echo)
        .expect("register echo request");
    let icmp_input = runtime.nodes().register_internal(control.node());
    let packet = ipv6_icmp_packet(128, 0, b"echo6");
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet);

    assert!(runtime.schedule_frame(icmp_input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(echo_state.borrow().packets, vec![packet]);
    assert!(echo_state.borrow().node_errors[0].is_none());
    assert!(punt_state.borrow().packets.is_empty());
}

#[test]
fn icmp_input_sends_unknown_ipv4_type_to_default_next() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let punt_state = Rc::new(RefCell::new(CaptureState::default()));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&punt_state)));
    let control = IcmpInputControlPlane::new(punt);
    let icmp_input = runtime.nodes().register_internal(control.node());
    let packet = ipv4_icmp_packet(13, 0, b"timestamp");
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet);

    assert!(runtime.schedule_frame(icmp_input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(punt_state.borrow().packets, vec![packet]);
    assert_eq!(
        punt_state.borrow().node_errors,
        vec![Some(BufferNodeError::new(
            icmp_input,
            IcmpInputError::UnknownType.code()
        ))]
    );
}

#[test]
fn icmp_input_rejects_ipv6_echo_request_with_nonzero_code() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 8);
    let echo_state = Rc::new(RefCell::new(CaptureState::default()));
    let punt_state = Rc::new(RefCell::new(CaptureState::default()));
    let echo = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&echo_state)));
    let punt = runtime
        .nodes()
        .register_internal(CaptureNode::new(Rc::clone(&punt_state)));
    let control = IcmpInputControlPlane::new(punt);
    control
        .register_type(IpVersion::V6, 128, echo)
        .expect("register echo request");
    let icmp_input = runtime.nodes().register_internal(control.node());
    let packet = ipv6_icmp_packet(128, 1, b"bad-code");
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet);

    assert!(runtime.schedule_frame(icmp_input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(echo_state.borrow().packets.is_empty());
    assert_eq!(punt_state.borrow().packets, vec![packet]);
    assert_eq!(
        punt_state.borrow().node_errors,
        vec![Some(BufferNodeError::new(
            icmp_input,
            IcmpInputError::BadCode.code()
        ))]
    );
}

fn push_packet(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
) {
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
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

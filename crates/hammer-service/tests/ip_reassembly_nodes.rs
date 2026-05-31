use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::rc::Rc;
use std::time::{Duration, Instant};

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Network, Node, NodeResult, RouteMetadata,
    SocksAddr,
};
use hammer_core::error::CoreResult;
use hammer_service::net::{IpInputNode, IpReassemblyNode};

struct SinkNode {
    packets: Rc<RefCell<Vec<Vec<u8>>>>,
    metadata: Rc<RefCell<Vec<RouteMetadata>>>,
}

impl Node<TestNode> for SinkNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<TestNode>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.drain_pending() {
            self.packets
                .borrow_mut()
                .push(runtime.copy_current_chain(index)?);
            self.metadata.borrow_mut().push(runtime.metadata(index)?);
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }
}

impl InternalNode<TestNode> for SinkNode {}

enum TestNode {
    IpInput(IpInputNode),
    Reassembly(IpReassemblyNode),
    Sink(SinkNode),
}

impl From<IpInputNode> for TestNode {
    fn from(node: IpInputNode) -> Self {
        Self::IpInput(node)
    }
}

impl From<IpReassemblyNode> for TestNode {
    fn from(node: IpReassemblyNode) -> Self {
        Self::Reassembly(node)
    }
}

impl From<SinkNode> for TestNode {
    fn from(node: SinkNode) -> Self {
        Self::Sink(node)
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
            Self::IpInput(node) => node.process(runtime, frame),
            Self::Reassembly(node) => node.process(runtime, frame),
            Self::Sink(node) => node.process(runtime, frame),
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
fn ip_input_drops_ipv4_options_packets() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 8, 8, 4);
    let packets = Rc::new(RefCell::new(Vec::new()));
    let metadata = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_internal(SinkNode {
        packets: Rc::clone(&packets),
        metadata,
    });
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(sink));
    let input = IpInputNode::new(sink, reassembly);
    assert_internal_node(&input);
    let input = runtime.nodes().register_internal(input);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let packet = ipv4_options_packet();
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), &packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 1);
    assert!(packets.borrow().is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_emits_complete_packet_after_out_of_order_fragments() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 4);
    let packets = Rc::new(RefCell::new(Vec::new()));
    let metadata = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_internal(SinkNode {
        packets: Rc::clone(&packets),
        metadata: Rc::clone(&metadata),
    });
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(sink));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::new(sink, reassembly));
    let original = ipv4_udp_packet(
        [10, 0, 0, 2],
        12_345,
        [198, 51, 100, 7],
        53,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv4_fragments(&original, 100, 16);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &fragments[1]);
    push_packet(&runtime, frame, &fragments[0]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(
        &*packets.borrow(),
        &[ipv4_reassembled_packet(&original, 100)]
    );
    let metadata = metadata.borrow();
    assert_eq!(metadata[0].network, Network::Udp);
    assert_eq!(
        metadata[0].source,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)), 0))
    );
    assert_eq!(
        metadata[0].destination,
        Some(SocksAddr::ip(IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)), 0))
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_ignores_duplicate_covered_fragment() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 4);
    let packets = Rc::new(RefCell::new(Vec::new()));
    let metadata = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_internal(SinkNode {
        packets: Rc::clone(&packets),
        metadata,
    });
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(sink));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::new(sink, reassembly));
    let original = ipv4_udp_packet(
        [10, 0, 0, 3],
        12_346,
        [198, 51, 100, 8],
        443,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv4_fragments(&original, 101, 16);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &fragments[0]);
    push_packet(&runtime, frame, &fragments[0]);
    push_packet(&runtime, frame, &fragments[1]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(
        &*packets.borrow(),
        &[ipv4_reassembled_packet(&original, 101)]
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv4_reassembly_drops_context_on_partial_overlap() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 4);
    let packets = Rc::new(RefCell::new(Vec::new()));
    let metadata = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_internal(SinkNode {
        packets: Rc::clone(&packets),
        metadata,
    });
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(sink));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::new(sink, reassembly));
    let original = ipv4_udp_packet(
        [10, 0, 0, 4],
        12_347,
        [198, 51, 100, 9],
        853,
        b"abcdefghijklmnopqrstuvwxyz012345",
    );
    let first = ipv4_fragment(&original, 102, 0, 24, true);
    let overlap = ipv4_fragment(&original, 102, 16, 16, false);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &first);
    push_packet(&runtime, frame, &overlap);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert!(packets.borrow().is_empty());
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn ipv6_reassembly_emits_complete_packet_after_out_of_order_fragments() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 4);
    let packets = Rc::new(RefCell::new(Vec::new()));
    let metadata = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime.nodes().register_internal(SinkNode {
        packets: Rc::clone(&packets),
        metadata: Rc::clone(&metadata),
    });
    let reassembly = runtime
        .nodes()
        .register_internal(IpReassemblyNode::new(sink));
    let input = runtime
        .nodes()
        .register_internal(IpInputNode::new(sink, reassembly));
    let original = ipv6_udp_packet(
        Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1),
        20_000,
        Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 2),
        53,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv6_fragments(&original, 0x0102_0304, 16);
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &fragments[1]);
    push_packet(&runtime, frame, &fragments[0]);

    assert!(runtime.schedule_frame(input, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_eq!(&*packets.borrow(), &[original]);
    let metadata = metadata.borrow();
    assert_eq!(metadata[0].network, Network::Udp);
    assert_eq!(
        metadata[0].source,
        Some(SocksAddr::ip(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 1)),
            0
        ))
    );
    assert_eq!(
        metadata[0].destination,
        Some(SocksAddr::ip(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 1, 0, 0, 0, 2)),
            0
        ))
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn reassembly_expire_frees_incomplete_fragments() {
    let runtime = DataPlaneRuntime::<TestNode>::with_capacities(2048, 16, 8, 4);
    let packets = Rc::new(RefCell::new(Vec::new()));
    let metadata = Rc::new(RefCell::new(Vec::new()));
    let sink = runtime
        .nodes()
        .register(TestNode::Sink(SinkNode { packets, metadata }));
    let mut reassembly = IpReassemblyNode::new(sink).with_timeout(Duration::from_millis(100));
    let packet = ipv4_udp_packet(
        [10, 0, 0, 5],
        12_348,
        [198, 51, 100, 10],
        53,
        b"abcdefghijklmnopqrstuvwx",
    );
    let fragments = ipv4_fragments(&packet, 103, 16);
    let mut frame = runtime.alloc_pooled_frame().expect("alloc frame");
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), &fragments[0])
        .expect("alloc packet");
    frame.push_index(index).expect("push packet");

    let started = Instant::now();
    reassembly
        .process_at(&runtime, &mut frame, started)
        .expect("process fragment");
    runtime.release_pooled_frame(frame).expect("release frame");

    assert_eq!(runtime.in_use_buffers(), 1);
    assert_eq!(
        reassembly.expire(&runtime, started + Duration::from_millis(101)),
        1
    );
    assert_eq!(runtime.in_use_buffers(), 0);
}

fn push_packet(
    runtime: &DataPlaneRuntime<TestNode>,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
) {
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(index)
        .expect("push packet");
}

fn ipv4_options_packet() -> Vec<u8> {
    let payload = b"option";
    let total_len = 24 + payload.len();
    let mut packet = vec![0; total_len];
    packet[0] = 0x46;
    packet[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
    packet[8] = 64;
    packet[9] = 17;
    packet[12..16].copy_from_slice(&[10, 0, 0, 2]);
    packet[16..20].copy_from_slice(&[198, 51, 100, 7]);
    packet[20..24].copy_from_slice(&[1, 1, 1, 0]);
    packet[24..].copy_from_slice(payload);
    update_ipv4_checksum(&mut packet);
    packet
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
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_fragments(packet: &[u8], identification: u16, split: usize) -> Vec<Vec<u8>> {
    let payload_len = packet.len() - 20;
    vec![
        ipv4_fragment(packet, identification, 0, split, true),
        ipv4_fragment(packet, identification, split, payload_len - split, false),
    ]
}

fn ipv4_reassembled_packet(packet: &[u8], identification: u16) -> Vec<u8> {
    let mut packet = packet.to_vec();
    packet[4..6].copy_from_slice(&identification.to_be_bytes());
    update_ipv4_checksum(&mut packet);
    packet
}

fn ipv4_fragment(
    packet: &[u8],
    identification: u16,
    payload_offset: usize,
    payload_len: usize,
    more_fragments: bool,
) -> Vec<u8> {
    assert_eq!(payload_offset % 8, 0);
    let mut fragment = Vec::with_capacity(20 + payload_len);
    fragment.extend_from_slice(&packet[..20]);
    fragment.extend_from_slice(&packet[20 + payload_offset..20 + payload_offset + payload_len]);
    let fragment_len = fragment.len() as u16;
    fragment[2..4].copy_from_slice(&fragment_len.to_be_bytes());
    fragment[4..6].copy_from_slice(&identification.to_be_bytes());
    let flags_offset = ((payload_offset / 8) as u16) | if more_fragments { 0x2000 } else { 0 };
    fragment[6..8].copy_from_slice(&flags_offset.to_be_bytes());
    update_ipv4_checksum(&mut fragment);
    fragment
}

fn ipv6_udp_packet(
    source: Ipv6Addr,
    source_port: u16,
    destination: Ipv6Addr,
    destination_port: u16,
    payload: &[u8],
) -> Vec<u8> {
    let payload_len = 8 + payload.len();
    let mut packet = vec![0; 40 + payload_len];
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

fn ipv6_fragments(packet: &[u8], identification: u32, split: usize) -> Vec<Vec<u8>> {
    let payload_len = packet.len() - 40;
    vec![
        ipv6_fragment(packet, identification, 0, split, true),
        ipv6_fragment(packet, identification, split, payload_len - split, false),
    ]
}

fn ipv6_fragment(
    packet: &[u8],
    identification: u32,
    payload_offset: usize,
    payload_len: usize,
    more_fragments: bool,
) -> Vec<u8> {
    assert_eq!(payload_offset % 8, 0);
    let mut fragment = Vec::with_capacity(40 + 8 + payload_len);
    fragment.extend_from_slice(&packet[..40]);
    fragment[6] = 44;
    fragment.extend_from_slice(&[
        packet[6],
        0,
        0,
        0,
        (identification >> 24) as u8,
        (identification >> 16) as u8,
        (identification >> 8) as u8,
        identification as u8,
    ]);
    let mut offset_more = ((payload_offset / 8) as u16) << 3;
    if more_fragments {
        offset_more |= 1;
    }
    fragment[42..44].copy_from_slice(&offset_more.to_be_bytes());
    fragment.extend_from_slice(&packet[40 + payload_offset..40 + payload_offset + payload_len]);
    let fragment_payload_len = fragment.len() - 40;
    fragment[4..6].copy_from_slice(&(fragment_payload_len as u16).to_be_bytes());
    fragment
}

fn update_ipv4_checksum(packet: &mut [u8]) {
    let ihl = ((packet[0] & 0x0f) as usize) * 4;
    packet[10] = 0;
    packet[11] = 0;
    let checksum = internet_checksum(&packet[..ihl]);
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

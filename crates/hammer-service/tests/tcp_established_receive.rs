use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferFrame, BufferPacketCursor, DataPlaneRuntime, DataWorkerId, InternalNode, Network, Node,
    NodeId, NodeProcessFn, NodeResult, NodeRuntimeData, RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_service::data_plane::DropNode;
use hammer_service::transport::congestion::BbrController;
use hammer_service::transport::tcp::TcpSessionProtocol;
use hammer_service::transport::tcp::{
    TcpEstablishedNext, TcpEstablishedNode, TcpListenNext, TcpListenNode, TcpOutputNext,
    TcpOutputNode, TcpSynRcvdNext, TcpSynRcvdNode,
};

const LOCAL: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
const REMOTE: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 20);
const LOCAL_PORT: u16 = 443;
const REMOTE_PORT: u16 = 50_001;
const CLIENT_ISN: u32 = 7_000;
const SERVER_ISN: u32 = 1;
const SYN: u8 = 0x02;
const RST: u8 = 0x04;
const ACK: u8 = 0x10;
const FIN: u8 = 0x01;

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
}

struct CaptureNode {
    runtime_data: NodeRuntimeData,
}

impl CaptureNode {
    fn new(state: Arc<Mutex<CaptureState>>) -> Self {
        let mut states = capture_states().lock().expect("capture registry");
        let slot = states.len();
        states.push(state);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
        }
    }
}

impl Node for CaptureNode {
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "capture node must use descriptor process",
        ))
    }

    fn node_process(&self) -> NodeProcessFn {
        capture_process
    }

    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.runtime_data)
    }
}

impl InternalNode for CaptureNode {}

fn capture_states() -> &'static Mutex<Vec<Arc<Mutex<CaptureState>>>> {
    static STATES: OnceLock<Mutex<Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(Vec::new()))
}

fn capture_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = {
        let states = capture_states()
            .lock()
            .map_err(|_| CoreError::internal("capture registry poisoned"))?;
        Arc::clone(
            states
                .get(data.usize_word(0)?)
                .ok_or_else(|| CoreError::internal("capture state missing"))?,
        )
    };
    for index in frame.drain_pending() {
        let packet = runtime.copy_current_chain(index)?;
        state
            .lock()
            .map_err(|_| CoreError::internal("capture poisoned"))?
            .packets
            .push(packet.into_iter().collect());
        runtime.free_index(index);
    }
    Ok(NodeResult::drop())
}

struct Graph {
    runtime: DataPlaneRuntime,
    output_state: Arc<Mutex<CaptureState>>,
    established: NodeId,
}

fn established_graph() -> Graph {
    let runtime = DataPlaneRuntime::with_capacities(4096, 32, 8, 8);
    let handle =
        TcpSessionProtocol::register_queue::<BbrController>(
            DataWorkerId::new(0),
            runtime.packet_buffers().clone(),
        )
            .expect("session queue");
    let output_state = Arc::new(Mutex::new(CaptureState::default()));
    let output_capture = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&output_state)));
    let drop = runtime.nodes().register_internal(DropNode::new());
    let output = runtime.nodes().register_internal(TcpOutputNode::new(
        TcpOutputNext::nodes(drop, output_capture),
        handle,
    ));
    let listen = runtime
        .nodes()
        .register_internal(TcpListenNode::new(handle, TcpListenNext::nodes(output, drop)));
    let syn_rcvd = runtime.nodes().register_internal(TcpSynRcvdNode::new(
        handle,
        TcpSynRcvdNext::nodes(output, drop),
    ));
    let established = runtime.nodes().register_internal(TcpEstablishedNode::new(
        handle,
        TcpEstablishedNext::nodes(output, drop),
    ));

    send_packet(
        &runtime,
        listen,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            CLIENT_ISN,
            0,
            SYN,
            b"",
        ),
    );
    assert_eq!(runtime.run_ready_nodes().expect("run listen"), 3);
    output_state.lock().unwrap().packets.clear();

    send_packet(
        &runtime,
        syn_rcvd,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            CLIENT_ISN + 1,
            SERVER_ISN + 1,
            ACK,
            b"",
        ),
    );
    assert_eq!(runtime.run_ready_nodes().expect("run syn-rcvd"), 3);
    output_state.lock().unwrap().packets.clear();

    Graph {
        runtime,
        output_state,
        established,
    }
}

#[test]
fn tcp_established_in_order_payload_advances_rcv_nxt_and_emits_ack() {
    let graph = established_graph();

    send_packet(
        &graph.runtime,
        graph.established,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            CLIENT_ISN + 1,
            SERVER_ISN + 1,
            ACK,
            b"hello",
        ),
    );

    assert_eq!(graph.runtime.run_ready_nodes().expect("run established"), 3);
    let packets = &graph.output_state.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
    assert_tcp_packet(
        &packets[0],
        LOCAL,
        LOCAL_PORT,
        REMOTE,
        REMOTE_PORT,
        SERVER_ISN + 1,
        CLIENT_ISN + 1 + 5,
        ACK,
    );
}

#[test]
fn tcp_established_processes_payload_from_chained_packet_buffer() {
    let graph = established_graph();

    send_chained_packet(
        &graph.runtime,
        graph.established,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            CLIENT_ISN + 1,
            SERVER_ISN + 1,
            ACK,
            b"hello-chain",
        ),
        40,
    );

    assert_eq!(graph.runtime.run_ready_nodes().expect("run established"), 3);
    let packets = &graph.output_state.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
    assert_tcp_packet(
        &packets[0],
        LOCAL,
        LOCAL_PORT,
        REMOTE,
        REMOTE_PORT,
        SERVER_ISN + 1,
        CLIENT_ISN + 1 + 11,
        ACK,
    );
}

#[test]
fn tcp_established_out_of_window_segment_emits_ack_without_advancing_rcv_nxt() {
    let graph = established_graph();

    send_packet(
        &graph.runtime,
        graph.established,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            CLIENT_ISN + 1 + 16,
            SERVER_ISN + 1,
            ACK,
            b"late",
        ),
    );

    assert_eq!(graph.runtime.run_ready_nodes().expect("run established"), 3);
    let packets = &graph.output_state.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
    assert_tcp_packet(
        &packets[0],
        LOCAL,
        LOCAL_PORT,
        REMOTE,
        REMOTE_PORT,
        SERVER_ISN + 1,
        CLIENT_ISN + 1,
        ACK,
    );
}

#[test]
fn tcp_established_fin_advances_rcv_nxt_and_emits_ack() {
    let graph = established_graph();

    send_packet(
        &graph.runtime,
        graph.established,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            CLIENT_ISN + 1,
            SERVER_ISN + 1,
            FIN | ACK,
            b"",
        ),
    );

    assert_eq!(graph.runtime.run_ready_nodes().expect("run established"), 3);
    let packets = &graph.output_state.lock().unwrap().packets;
    assert_eq!(packets.len(), 1);
    assert_tcp_packet(
        &packets[0],
        LOCAL,
        LOCAL_PORT,
        REMOTE,
        REMOTE_PORT,
        SERVER_ISN + 1,
        CLIENT_ISN + 2,
        ACK,
    );
}

#[test]
fn tcp_established_rst_closes_session() {
    let graph = established_graph();

    send_packet(
        &graph.runtime,
        graph.established,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            CLIENT_ISN + 1,
            SERVER_ISN + 1,
            RST,
            b"",
        ),
    );
    assert_eq!(graph.runtime.run_ready_nodes().expect("run rst"), 1);

    send_packet(
        &graph.runtime,
        graph.established,
        tcp_packet(
            REMOTE,
            REMOTE_PORT,
            LOCAL,
            LOCAL_PORT,
            CLIENT_ISN + 1,
            SERVER_ISN + 1,
            ACK,
            b"again",
        ),
    );
    assert_eq!(graph.runtime.run_ready_nodes().expect("run after rst"), 2);
    assert!(graph.output_state.lock().unwrap().packets.is_empty());
}

fn send_packet(runtime: &DataPlaneRuntime, node: NodeId, packet: Vec<u8>) {
    let frame = runtime.alloc_frame_index().expect("frame");
    let buffer = runtime
        .alloc_index_with_bytes(tcp_metadata(), &packet)
        .expect("packet");
    stamp_tcp_cursor(runtime, buffer, &packet);
    runtime
        .get_frame_mut(frame)
        .expect("frame mut")
        .push_index(buffer)
        .expect("push packet");
    assert!(runtime.schedule_frame(node, frame).expect("schedule"));
}

fn send_chained_packet(runtime: &DataPlaneRuntime, node: NodeId, packet: Vec<u8>, split: usize) {
    let frame = runtime.alloc_frame_index().expect("frame");
    let head = runtime
        .alloc_index_with_bytes(tcp_metadata(), &packet[..split])
        .expect("head packet");
    let tail = runtime
        .alloc_index_with_bytes(tcp_metadata(), &packet[split..])
        .expect("tail packet");
    runtime
        .append_existing_chain(head, tail)
        .expect("append payload chain");
    stamp_tcp_cursor(runtime, head, &packet);
    runtime
        .get_frame_mut(frame)
        .expect("frame mut")
        .push_index(head)
        .expect("push packet");
    assert!(runtime.schedule_frame(node, frame).expect("schedule"));
}

fn tcp_metadata() -> RouteMetadata {
    RouteMetadata {
        network: Network::Tcp,
        source: Some(SocksAddr::ip(IpAddr::V4(REMOTE), REMOTE_PORT)),
        destination: Some(SocksAddr::ip(IpAddr::V4(LOCAL), LOCAL_PORT)),
        ..RouteMetadata::default()
    }
}

fn stamp_tcp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    let header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
    let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    let tcp_header_len = ((packet[header_len + 12] >> 4) as usize) * 4;
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, header_len)
                .with_transport_header(header_len, tcp_header_len)
                .with_transport_payload_offset(header_len + tcp_header_len),
        );
}

fn assert_tcp_packet(
    packet: &[u8],
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
) {
    let _ = source;
    let _ = destination;
    assert_eq!(u16::from_be_bytes([packet[0], packet[1]]), source_port);
    assert_eq!(u16::from_be_bytes([packet[2], packet[3]]), destination_port);
    assert_eq!(
        u32::from_be_bytes([packet[4], packet[5], packet[6], packet[7]]),
        sequence
    );
    assert_eq!(
        u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]),
        acknowledgment
    );
    assert_eq!(packet[13] & flags, flags);
}

fn tcp_packet(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 6, 20 + payload.len());
    write_tcp_segment(
        &mut packet[20..],
        source_port,
        destination_port,
        sequence,
        acknowledgment,
        flags,
        payload,
    );
    let checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
    packet[36..38].copy_from_slice(&checksum.to_be_bytes());
    update_ipv4_header_checksum(&mut packet);
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
    packet[8] = 64;
    packet[9] = protocol;
    packet[12..16].copy_from_slice(&source.octets());
    packet[16..20].copy_from_slice(&destination.octets());
    packet
}

fn write_tcp_segment(
    segment: &mut [u8],
    source_port: u16,
    destination_port: u16,
    sequence: u32,
    acknowledgment: u32,
    flags: u8,
    payload: &[u8],
) {
    segment[0..2].copy_from_slice(&source_port.to_be_bytes());
    segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
    segment[4..8].copy_from_slice(&sequence.to_be_bytes());
    segment[8..12].copy_from_slice(&acknowledgment.to_be_bytes());
    segment[12] = 0x50;
    segment[13] = flags;
    segment[14..16].copy_from_slice(&u16::MAX.to_be_bytes());
    segment[20..].copy_from_slice(payload);
}

fn ipv4_l4_checksum(source: Ipv4Addr, destination: Ipv4Addr, protocol: u8, segment: &[u8]) -> u16 {
    let mut pseudo = Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
    pseudo.extend_from_slice(&source.octets());
    pseudo.extend_from_slice(&destination.octets());
    pseudo.push(0);
    pseudo.push(protocol);
    pseudo.extend_from_slice(&(segment.len() as u16).to_be_bytes());
    pseudo.extend_from_slice(segment);
    internet_checksum(&pseudo)
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
        let word = match chunk {
            [hi, lo] => u16::from_be_bytes([*hi, *lo]) as u32,
            [hi] => u16::from_be_bytes([*hi, 0]) as u32,
            _ => unreachable!(),
        };
        sum += word;
        while sum > 0xffff {
            sum = (sum & 0xffff) + (sum >> 16);
        }
    }
    !(sum as u16)
}

use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferFrame, BufferNodeError, BufferPacketCursor, DataPlaneHandoff, DataPlaneRuntime,
    DataWorkerId, InternalNode, Network, Node, NodeHandle, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_service::data_plane::DropNode;
use hammer_service::net::{IpLocalControlPlane, IpLocalNext};
use hammer_service::transport::tcp::{
    TcpEstablishedNext, TcpEstablishedNode, TcpInputControlPlane, TcpInputError, TcpInputHandoff,
    TcpInputNext, TcpInputNode, TcpListenNext, TcpListenNode, TcpRcvProcessNext, TcpRcvProcessNode,
    TcpResetNext, TcpResetNode, TcpV4ConnectionKey, TcpV4ListenerKey, TcpWorkerOwnedState,
};

const LISTEN_PORT: u16 = 4_43;
const ESTABLISHED_ID: u32 = 37;
const LISTENER_ID: u32 = 11;

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
    metadata: Vec<RouteMetadata>,
    node_errors: Vec<Option<BufferNodeError>>,
    handoff_source_workers: Vec<Option<DataWorkerId>>,
    frame_lens: Vec<usize>,
}

struct CaptureNode {
    runtime_data: NodeRuntimeData,
}

impl CaptureNode {
    fn new(state: Arc<Mutex<CaptureState>>) -> Self {
        let mut states = capture_states()
            .lock()
            .expect("capture state registry poisoned");
        let slot = states.len();
        states.push(state);
        Self {
            runtime_data: NodeRuntimeData::from_usize(slot).expect("capture state slot"),
        }
    }
}

impl Node for CaptureNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        _frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        Err(CoreError::internal(
            "capture node must run through descriptor process",
        ))
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        capture_process
    }

    #[inline]
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
            .map_err(|_| CoreError::internal("capture state registry poisoned"))?;
        Arc::clone(
            states
                .get(data.usize_word(0)?)
                .ok_or_else(|| CoreError::internal("capture state slot is invalid"))?,
        )
    };
    state
        .lock()
        .map_err(|_| CoreError::internal("capture state poisoned"))?
        .frame_lens
        .push(frame.pending_len());
    for index in frame.drain_pending() {
        let packet = runtime.copy_current_chain(index)?;
        let metadata = runtime.metadata(index)?;
        let node_error = runtime.node_error(index)?;
        let handoff_source_worker = runtime.handoff_source_worker(index)?;
        let mut state = state
            .lock()
            .map_err(|_| CoreError::internal("capture state poisoned"))?;
        state.packets.push(packet.into_iter().collect());
        state.metadata.push(metadata);
        state.node_errors.push(node_error);
        state.handoff_source_workers.push(handoff_source_worker);
        runtime.free_index(index);
    }
    Ok(NodeResult::drop())
}

fn assert_internal_node<I>(node: &I)
where
    I: InternalNode,
{
    let _ = node;
}

#[test]
fn ip_local_routes_tcp_packets_into_tcp_input_listen_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = TcpGraph::new(&runtime);
    let local_control = IpLocalControlPlane::new(IpLocalNext::nodes(
        graph.drop,
        graph.punt,
        graph.tcp_input,
        graph.drop,
        graph.drop,
        graph.drop,
    ));
    let local = runtime.nodes().register_internal(local_control.node());
    let packet = ipv4_tcp_packet(
        Ipv4Addr::new(10, 0, 0, 1),
        50_001,
        Ipv4Addr::new(192, 0, 2, 10),
        LISTEN_PORT,
        tcp_flags(false, true, false, false),
        b"syn",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet, RouteMetadata::default());

    assert!(runtime.schedule_frame(local, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 4);
    assert_capture_packets(&graph.listen_state, &[packet]);
    assert!(graph.reset_state.lock().unwrap().packets.is_empty());
    assert!(graph.established_state.lock().unwrap().packets.is_empty());
    let state = graph.listen_state.lock().unwrap();
    assert_metadata(
        &state.metadata[0],
        Ipv4Addr::new(10, 0, 0, 1).into(),
        50_001,
        Ipv4Addr::new(192, 0, 2, 10).into(),
        LISTEN_PORT,
    );
    assert_eq!(state.node_errors, vec![None]);
    drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_input_routes_listen_ack_to_reset_node() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = TcpGraph::new(&runtime);
    let packet = ipv4_tcp_packet(
        Ipv4Addr::new(10, 0, 0, 2),
        50_002,
        Ipv4Addr::new(192, 0, 2, 20),
        LISTEN_PORT,
        tcp_flags(false, false, false, true),
        b"ack",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_tcp_packet(
        &runtime,
        frame,
        &packet,
        Ipv4Addr::new(10, 0, 0, 2).into(),
        50_002,
        Ipv4Addr::new(192, 0, 2, 20).into(),
        LISTEN_PORT,
    );

    assert!(
        runtime
            .schedule_frame(graph.tcp_input, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_capture_packets(&graph.reset_state, &[packet]);
    assert!(graph.listen_state.lock().unwrap().packets.is_empty());
    assert!(graph.established_state.lock().unwrap().packets.is_empty());
    let state = graph.reset_state.lock().unwrap();
    assert_eq!(
        state.node_errors,
        vec![Some(BufferNodeError::new(
            graph.tcp_input,
            TcpInputError::AckInvalid.code(),
        ))]
    );
    assert_metadata(
        &state.metadata[0],
        Ipv4Addr::new(10, 0, 0, 2).into(),
        50_002,
        Ipv4Addr::new(192, 0, 2, 20).into(),
        LISTEN_PORT,
    );
    drop(state);
    assert_eq!(
        runtime
            .node_error_count(graph.tcp_input, TcpInputError::AckInvalid.code())
            .expect("ack invalid counter"),
        1
    );
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_input_prefers_established_connection_lookup() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let graph = TcpGraph::new(&runtime);
    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(0));
    let listener_key = TcpV4ListenerKey::new(0, Ipv4Addr::new(192, 0, 2, 30), LISTEN_PORT);
    let connection_key = TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(192, 0, 2, 30),
        LISTEN_PORT,
        Ipv4Addr::new(198, 51, 100, 30),
        50_030,
    );
    owner.insert_listener_v4(listener_key, LISTENER_ID);
    owner.insert_connection_v4(connection_key, ESTABLISHED_ID);
    graph
        .tcp_control
        .publish_lookup(owner.publish_snapshot())
        .expect("publish lookup snapshot");

    let packet = ipv4_tcp_packet(
        Ipv4Addr::new(203, 0, 113, 99),
        40_000,
        Ipv4Addr::new(203, 0, 113, 100),
        40_001,
        tcp_flags(false, false, false, true),
        b"established",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let metadata = tcp_metadata(
        Ipv4Addr::new(198, 51, 100, 30).into(),
        50_030,
        Ipv4Addr::new(192, 0, 2, 30).into(),
        LISTEN_PORT,
    );
    let buffer = push_packet(&runtime, frame, &packet, metadata);
    stamp_tcp_cursor(&runtime, buffer, &packet);

    assert!(
        runtime
            .schedule_frame(graph.tcp_input, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 3);
    assert_capture_packets(&graph.established_state, &[packet]);
    assert!(graph.listen_state.lock().unwrap().packets.is_empty());
    assert!(graph.reset_state.lock().unwrap().packets.is_empty());
    let state = graph.established_state.lock().unwrap();
    assert_metadata(
        &state.metadata[0],
        Ipv4Addr::new(198, 51, 100, 30).into(),
        50_030,
        Ipv4Addr::new(192, 0, 2, 30).into(),
        LISTEN_PORT,
    );
    assert_eq!(state.node_errors, vec![None]);
    drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_input_handoffs_established_packets_to_owner_worker() {
    const TCP_INPUT_HANDLE: NodeHandle = NodeHandle::new(41);

    let handoff = DataPlaneHandoff::new(2, 8);
    let first_runtime = DataPlaneRuntime::with_handoff(
        DataPlaneRuntime::with_capacities(2048, 16, 8, 8),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second_runtime = DataPlaneRuntime::with_handoff(
        DataPlaneRuntime::with_capacities(2048, 16, 8, 8),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );
    let first_graph =
        TcpGraph::new_with_handoff(&first_runtime, TCP_INPUT_HANDLE, DataWorkerId::new(0));
    let second_graph =
        TcpGraph::new_with_handoff(&second_runtime, TCP_INPUT_HANDLE, DataWorkerId::new(1));

    let mut owner = TcpWorkerOwnedState::new(DataWorkerId::new(1));
    let connection_key = TcpV4ConnectionKey::new(
        0,
        Ipv4Addr::new(192, 0, 2, 31),
        LISTEN_PORT,
        Ipv4Addr::new(198, 51, 100, 31),
        50_031,
    );
    owner.insert_connection_v4(connection_key, ESTABLISHED_ID);
    let snapshot = owner.publish_snapshot();
    first_graph
        .tcp_control
        .publish_lookup(snapshot.clone())
        .expect("publish first snapshot");
    second_graph
        .tcp_control
        .publish_lookup(snapshot)
        .expect("publish second snapshot");

    let packet = ipv4_tcp_packet(
        Ipv4Addr::new(203, 0, 113, 31),
        40_031,
        Ipv4Addr::new(203, 0, 113, 32),
        40_032,
        tcp_flags(false, false, false, true),
        b"handoff",
    );
    let frame = first_runtime.alloc_frame_index().expect("alloc frame");
    let metadata = tcp_metadata(
        Ipv4Addr::new(198, 51, 100, 31).into(),
        50_031,
        Ipv4Addr::new(192, 0, 2, 31).into(),
        LISTEN_PORT,
    );
    let buffer = push_packet(&first_runtime, frame, &packet, metadata);
    stamp_tcp_cursor(&first_runtime, buffer, &packet);

    assert!(
        first_runtime
            .schedule_frame(first_graph.tcp_input, frame)
            .expect("schedule first")
    );

    assert_eq!(first_runtime.run_ready_nodes().expect("run first"), 1);
    assert_eq!(second_runtime.run_ready_nodes().expect("run second"), 3);
    assert!(
        first_graph
            .established_state
            .lock()
            .unwrap()
            .packets
            .is_empty()
    );
    assert_capture_packets(&second_graph.established_state, &[packet]);
    let state = second_graph.established_state.lock().unwrap();
    assert_eq!(
        state.handoff_source_workers,
        vec![Some(DataWorkerId::new(0))]
    );
    assert_metadata(
        &state.metadata[0],
        Ipv4Addr::new(198, 51, 100, 31).into(),
        50_031,
        Ipv4Addr::new(192, 0, 2, 31).into(),
        LISTEN_PORT,
    );
    drop(state);
    assert_eq!(first_runtime.frames_in_use(), 0);
    assert_eq!(second_runtime.frames_in_use(), 0);
    assert_eq!(first_runtime.in_use_buffers(), 0);
    assert_eq!(second_runtime.in_use_buffers(), 0);
}

#[test]
fn tcp_rcv_process_node_passes_packets_to_configured_next() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let sink_state = Arc::new(Mutex::new(CaptureState::default()));
    let sink = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&sink_state)));
    let node = TcpRcvProcessNode::new(TcpRcvProcessNext::nodes(sink));
    assert_internal_node(&node);
    let rcv_process = runtime.nodes().register_internal(node);

    let packet = ipv4_tcp_packet(
        Ipv4Addr::new(198, 51, 100, 40),
        40_040,
        Ipv4Addr::new(192, 0, 2, 40),
        LISTEN_PORT,
        tcp_flags(false, false, false, true),
        b"rcv-process",
    );
    let metadata = tcp_metadata(
        Ipv4Addr::new(198, 51, 100, 40).into(),
        40_040,
        Ipv4Addr::new(192, 0, 2, 40).into(),
        LISTEN_PORT,
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    push_packet(&runtime, frame, &packet, metadata);

    assert!(
        runtime
            .schedule_frame(rcv_process, frame)
            .expect("schedule")
    );

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_capture_packets(&sink_state, &[packet]);
    let state = sink_state.lock().unwrap();
    assert_eq!(state.node_errors, vec![None]);
    assert_metadata(
        &state.metadata[0],
        Ipv4Addr::new(198, 51, 100, 40).into(),
        40_040,
        Ipv4Addr::new(192, 0, 2, 40).into(),
        LISTEN_PORT,
    );
    drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
}

struct TcpGraph {
    drop: NodeId,
    punt: NodeId,
    tcp_input: NodeId,
    tcp_control: TcpInputControlPlane,
    listen_state: Arc<Mutex<CaptureState>>,
    reset_state: Arc<Mutex<CaptureState>>,
    established_state: Arc<Mutex<CaptureState>>,
}

impl TcpGraph {
    fn new(runtime: &DataPlaneRuntime) -> Self {
        Self::new_inner(runtime, None)
    }

    fn new_with_handoff(
        runtime: &DataPlaneRuntime,
        handle: NodeHandle,
        worker: DataWorkerId,
    ) -> Self {
        Self::new_inner(runtime, Some((handle, worker)))
    }

    fn new_inner(runtime: &DataPlaneRuntime, handoff: Option<(NodeHandle, DataWorkerId)>) -> Self {
        let listen_state = Arc::new(Mutex::new(CaptureState::default()));
        let reset_state = Arc::new(Mutex::new(CaptureState::default()));
        let established_state = Arc::new(Mutex::new(CaptureState::default()));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let punt = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::new(Mutex::new(
                CaptureState::default(),
            ))));
        let listen_sink = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&listen_state)));
        let reset_sink = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&reset_state)));
        let established =
            runtime
                .nodes()
                .register_internal(TcpEstablishedNode::new(TcpEstablishedNext::nodes(
                    runtime
                        .nodes()
                        .register_internal(CaptureNode::new(Arc::clone(&established_state))),
                )));
        let listen_node = TcpListenNode::new(TcpListenNext::nodes(listen_sink));
        assert_internal_node(&listen_node);
        let listen = runtime.nodes().register_internal(listen_node);
        let reset_node = TcpResetNode::new(TcpResetNext::nodes(reset_sink));
        assert_internal_node(&reset_node);
        let reset = runtime.nodes().register_internal(reset_node);
        let tcp_control = TcpInputControlPlane::new(TcpInputNext::nodes(
            drop,
            punt,
            listen,
            drop,
            drop,
            established,
            reset,
        ));
        let tcp_node: TcpInputNode = match handoff {
            Some((handle, worker)) => tcp_control
                .node()
                .with_handoff(TcpInputHandoff::new(handle, worker)),
            None => tcp_control.node(),
        };
        assert_internal_node(&tcp_node);
        let tcp_input = match handoff {
            Some((handle, _)) => runtime
                .nodes()
                .register_internal_with_handle(handle, tcp_node)
                .expect("register tcp-input with handle"),
            None => runtime.nodes().register_internal(tcp_node),
        };
        Self {
            drop,
            punt,
            tcp_input,
            tcp_control,
            listen_state,
            reset_state,
            established_state,
        }
    }
}

fn push_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    metadata: RouteMetadata,
) -> hammer_adapter::BufferIndex {
    let buffer = runtime
        .alloc_index_with_bytes(metadata, packet)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
    buffer
}

fn push_tcp_packet(
    runtime: &DataPlaneRuntime,
    frame: hammer_adapter::FrameIndex,
    packet: &[u8],
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) {
    let buffer = runtime
        .alloc_index_with_bytes(
            tcp_metadata(source, source_port, destination, destination_port),
            packet,
        )
        .expect("alloc packet");
    stamp_tcp_cursor(runtime, buffer, packet);
    runtime
        .get_frame_mut(frame)
        .expect("mutate frame")
        .push_index(buffer)
        .expect("push packet");
}

fn stamp_tcp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    let (packet_len, network_header_len) = match packet.first().map(|byte| byte >> 4) {
        Some(4) => {
            let header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
            let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
            (packet_len, header_len)
        }
        _ => panic!("TCP test packet must be IPv4"),
    };
    let tcp_offset = network_header_len;
    let tcp_header_len = ((packet[tcp_offset + 12] >> 4) as usize) * 4;
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, network_header_len)
                .with_transport_header(tcp_offset, tcp_header_len)
                .with_transport_payload_offset(tcp_offset + tcp_header_len),
        );
}

fn tcp_metadata(
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) -> RouteMetadata {
    RouteMetadata {
        network: Network::Tcp,
        source: Some(SocksAddr::ip(source, source_port)),
        destination: Some(SocksAddr::ip(destination, destination_port)),
        ..RouteMetadata::default()
    }
}

fn assert_capture_packets(state: &Arc<Mutex<CaptureState>>, expected: &[Vec<u8>]) {
    assert_eq!(state.lock().unwrap().packets, expected);
}

fn assert_metadata(
    metadata: &RouteMetadata,
    source: IpAddr,
    source_port: u16,
    destination: IpAddr,
    destination_port: u16,
) {
    assert_eq!(metadata.network, Network::Tcp);
    assert_eq!(metadata.source, Some(SocksAddr::ip(source, source_port)));
    assert_eq!(
        metadata.destination,
        Some(SocksAddr::ip(destination, destination_port))
    );
}

fn ipv4_tcp_packet(
    source: Ipv4Addr,
    source_port: u16,
    destination: Ipv4Addr,
    destination_port: u16,
    flags: u8,
    payload: &[u8],
) -> Vec<u8> {
    let mut packet = ipv4_packet(source, destination, 6, 20 + payload.len());
    write_tcp_segment(
        &mut packet[20..],
        source_port,
        destination_port,
        flags,
        payload,
    );
    let checksum = ipv4_l4_checksum(source, destination, 6, &packet[20..]);
    packet[20 + 16..20 + 18].copy_from_slice(&checksum.to_be_bytes());
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
    packet[4..6].copy_from_slice(&0x1234u16.to_be_bytes());
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
    flags: u8,
    payload: &[u8],
) {
    segment[0..2].copy_from_slice(&source_port.to_be_bytes());
    segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
    segment[12] = 0x50;
    segment[13] = flags;
    segment[20..].copy_from_slice(payload);
}

fn tcp_flags(fin: bool, syn: bool, rst: bool, ack: bool) -> u8 {
    u8::from(fin) | (u8::from(syn) << 1) | (u8::from(rst) << 2) | (u8::from(ack) << 4)
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

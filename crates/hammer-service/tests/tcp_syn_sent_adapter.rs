use std::net::{IpAddr, Ipv4Addr};
use std::sync::{Arc, Mutex, OnceLock};

use hammer_adapter::{
    BufferFrame, BufferPacketCursor, DataPlaneRuntime, InternalNode, Network, Node, NodeProcessFn,
    NodeResult, NodeRuntimeData, RouteMetadata, SocksAddr,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_service::transport::tcp::syn_sent::{
    TcpSynSentBackend, TcpSynSentControlPlane, TcpSynSentObservation, TcpSynSentRegistration,
};
use hammer_service::transport::tcp::{TcpState, TcpSynSentNext};

const CONNECTION_ID: u32 = 144;

#[derive(Default)]
struct CaptureState {
    packets: Vec<Vec<u8>>,
    metadata: Vec<RouteMetadata>,
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
    for index in frame.drain_pending() {
        let packet = runtime.copy_current_chain(index)?;
        let metadata = runtime.metadata(index)?;
        let mut state = state
            .lock()
            .map_err(|_| CoreError::internal("capture state poisoned"))?;
        state.packets.push(packet.into_iter().collect());
        state.metadata.push(metadata);
        runtime.free_index(index);
    }
    Ok(NodeResult::drop())
}

#[derive(Default)]
struct RecordingTcpSynSentBackend {
    observations: Arc<Mutex<Vec<TcpSynSentObservation>>>,
}

impl TcpSynSentBackend for RecordingTcpSynSentBackend {
    fn observe_syn_ack(&self, observation: TcpSynSentObservation) -> CoreResult<()> {
        self.observations
            .lock()
            .map_err(|_| CoreError::internal("syn-sent observations poisoned"))?
            .push(observation);
        Ok(())
    }
}

#[test]
fn tcp_syn_sent_node_observes_matching_syn_ack_via_backend() {
    let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
    let capture_state = Arc::new(Mutex::new(CaptureState::default()));
    let sink = runtime
        .nodes()
        .register_internal(CaptureNode::new(Arc::clone(&capture_state)));
    let backend = Arc::new(RecordingTcpSynSentBackend::default());
    let control = TcpSynSentControlPlane::new(backend.clone(), TcpSynSentNext::nodes(sink));
    control
        .publish_connections([TcpSynSentRegistration::v4(
            CONNECTION_ID,
            hammer_service::transport::tcp::TcpV4PendingConnectionKey::new(
                0,
                40_144,
                Ipv4Addr::new(198, 51, 100, 44),
                443,
            ),
        )])
        .expect("publish syn-sent connection");
    let syn_sent = runtime.nodes().register_internal(control.node());

    let packet = ipv4_tcp_packet(
        Ipv4Addr::new(198, 51, 100, 44),
        443,
        Ipv4Addr::new(192, 0, 2, 44),
        40_144,
        tcp_flags(false, true, false, true),
        b"syn-ack",
    );
    let frame = runtime.alloc_frame_index().expect("alloc frame");
    let buffer = push_packet(
        &runtime,
        frame,
        &packet,
        tcp_metadata(
            Ipv4Addr::new(198, 51, 100, 44).into(),
            443,
            Ipv4Addr::new(192, 0, 2, 44).into(),
            40_144,
        ),
    );
    stamp_tcp_cursor(&runtime, buffer, &packet);

    assert!(runtime.schedule_frame(syn_sent, frame).expect("schedule"));

    assert_eq!(runtime.run_ready_nodes().expect("run nodes"), 2);
    assert_eq!(
        *backend.observations.lock().unwrap(),
        vec![TcpSynSentObservation::new(
            CONNECTION_ID,
            "198.51.100.44:443".parse().expect("remote"),
            "192.0.2.44:40144".parse().expect("local"),
            TcpState::SynSent,
            TcpState::Established,
        )]
    );
    let state = capture_state.lock().unwrap();
    assert_eq!(state.packets, vec![packet]);
    assert_eq!(
        state.metadata,
        vec![tcp_metadata(
            Ipv4Addr::new(198, 51, 100, 44).into(),
            443,
            Ipv4Addr::new(192, 0, 2, 44).into(),
            40_144,
        )]
    );
    drop(state);
    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
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

fn stamp_tcp_cursor(
    runtime: &DataPlaneRuntime,
    buffer: hammer_adapter::BufferIndex,
    packet: &[u8],
) {
    let header_len = ((*packet.first().expect("IPv4 header") & 0x0f) as usize) * 4;
    let packet_len = u16::from_be_bytes([packet[2], packet[3]]) as usize;
    let tcp_offset = header_len;
    let tcp_header_len = ((packet[tcp_offset + 12] >> 4) as usize) * 4;
    runtime
        .get_buffer_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(
            BufferPacketCursor::new()
                .with_packet_len(packet_len)
                .with_network_header(0, header_len)
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

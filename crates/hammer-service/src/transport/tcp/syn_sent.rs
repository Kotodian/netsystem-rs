use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpSegmentFlags, TcpSeq, TcpState};

use crate::session::SessionQueueHandle;

use super::TcpSessionProtocol;
use super::output::{TCP_FLAG_ACK, tcp_output_packet_flags};
use super::segment::parse_tcp_packet;

#[hammer_component_macros::node_next]
pub enum TcpSynSentNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpSynSentNext)]
pub struct TcpSynSentNode {
    #[node(default)]
    session_queue: Option<SessionQueueHandle>,
}

impl TcpSynSentNode {
    #[inline]
    pub fn with_session_queue(mut self, handle: SessionQueueHandle) -> Self {
        self.session_queue = Some(handle);
        self
    }
}

impl Node for TcpSynSentNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        tcp_syn_sent_frame(runtime, frame, self.session_queue, next)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_syn_sent_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        self.session_queue
            .map(SessionQueueHandle::runtime_data)
            .ok_or_else(|| CoreError::internal("tcp syn-sent node missing session queue"))
    }
}

fn tcp_syn_sent_process(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpSynSentNode::runtime_nexts(runtime)?;
    tcp_syn_sent_frame(runtime, frame, Some(SessionQueueHandle::new(data)), next)
}

fn tcp_syn_sent_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: Option<SessionQueueHandle>,
    next: [NodeId; TcpSynSentNext::COUNT],
) -> CoreResult<NodeResult> {
    let session_queue = session_queue
        .ok_or_else(|| CoreError::internal("tcp syn-sent node missing session queue"))?;
    let output = next[TcpSynSentNext::Output as usize];
    let drop_next = next[TcpSynSentNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_syn_sent_index(runtime, index, session_queue, output, &mut next_frames) {
            Ok(()) => Ok(None),
            Err(_) => {
                next_frames.enqueue(runtime, drop_next, index)?;
                Ok(None)
            }
        }
    })?;
    next_frames.schedule(runtime)?;
    Ok(NodeResult::drop())
}

fn tcp_syn_sent_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: SessionQueueHandle,
    output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()> {
    let packet = parse_tcp_packet(runtime, index)?;
    let acknowledgment = packet
        .acknowledgment
        .ok_or_else(|| CoreError::internal("syn-sent packet missing ACK"))?;
    let mut output_index = None;
    let mut remove_session = None;
    let result = TcpSessionProtocol::with_queue(session_queue, |queue| {
        let session_id = queue
            .session_id_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp syn-sent session is missing"))?;
        let connection = queue
            .session_state_mut(session_id)
            .ok_or_else(|| CoreError::internal("tcp syn-sent state is missing"))?;
        if connection.state() != TcpState::SynSent {
            return Err(CoreError::internal(
                "tcp syn-sent received non-syn-sent session",
            ));
        }
        if !packet.flags.contains(TcpSegmentFlags::ACK)
            || !tcp_syn_sent_ack_acceptable(connection.iss(), connection.snd_nxt(), acknowledgment)
        {
            return Err(CoreError::internal("syn-sent ACK is unacceptable"));
        }
        if packet.flags.contains(TcpSegmentFlags::RST) {
            remove_session = Some(session_id);
            return Ok(());
        }
        if !packet.flags.contains(TcpSegmentFlags::SYN) {
            return Err(CoreError::internal("syn-sent segment missing SYN"));
        }
        connection.apply_peer_handshake_capabilities(packet.capabilities);
        connection.set_sequence_state(
            connection.iss(),
            packet.sequence,
            acknowledgment,
            connection.snd_nxt(),
            connection.effective_send_window(u32::from(packet.advertised_window)),
            TcpSeq::new(packet.sequence).advance(1).raw(),
            connection.rcv_wnd(),
        );
        connection.set_state(TcpState::Established);
        let record = tcp_output_packet_flags(connection, packet.local, &[], TCP_FLAG_ACK)?;
        let allocated = record.alloc_finalized_header_buffer(runtime)?;
        output_index = Some(allocated);
        let indexed = connection.clone();
        queue.index_session(session_id, &indexed);
        Ok(())
    });
    if let Err(error) = result {
        if let Some(output_index) = output_index.take() {
            runtime.free_index(output_index);
        }
        return Err(error);
    }
    if let Some(session_id) = remove_session {
        let result = TcpSessionProtocol::with_queue(session_queue, |queue| {
            queue.close_session(session_id)?;
            Ok(())
        });
        if let Err(error) = result {
            if let Some(output_index) = output_index.take() {
                runtime.free_index(output_index);
            }
            return Err(error);
        }
    }
    if let Some(output_index) = output_index.take()
        && let Err(error) = next_frames.enqueue(runtime, output, output_index)
    {
        runtime.free_index(output_index);
        return Err(error);
    }
    runtime.free_index(index);
    Ok(())
}

#[inline]
fn tcp_syn_sent_ack_acceptable(iss: u32, snd_nxt: u32, acknowledgment: u32) -> bool {
    TcpSeq::new(acknowledgment).after(TcpSeq::new(iss))
        && !TcpSeq::new(acknowledgment).after(TcpSeq::new(snd_nxt))
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_adapter::{
        BufferPacketCursor, DataWorkerId, InternalNode, Network, NodeId, RouteMetadata, SocksAddr,
    };
    use hammer_core::error::CoreError;
    use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
    use hammer_runtime::app::{AppCqeKind, AppOpId, AppRingHandle};

    use crate::data_plane::DropNode;
    use crate::session::SessionQueueHandle;
    use crate::transport::tcp::connection::TcpConnectionState;

    use super::*;

    const LOCAL: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
    const REMOTE: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 20);
    const LOCAL_PORT: u16 = 50_001;
    const REMOTE_PORT: u16 = 443;
    const CLIENT_ISN: u32 = 7_000;
    const SERVER_ISN: u32 = 11_000;
    const SYN: u8 = 0x02;
    const RST: u8 = 0x04;
    const ACK: u8 = 0x10;

    #[derive(Default)]
    struct CaptureState {
        packets: std::vec::Vec<std::vec::Vec<u8>>,
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

    fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
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

    #[test]
    fn valid_syn_ack_emits_final_ack_and_establishes() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let handle = TcpSessionProtocol::register_queue(
            DataWorkerId::new(0),
            runtime.packet_buffers().clone(),
        )
        .expect("session queue");
        let session_id = insert_syn_sent_session(handle);
        let output_state = Arc::new(Mutex::new(CaptureState::default()));
        let output = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&output_state)));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let syn_sent = runtime.nodes().register_internal(
            TcpSynSentNode::new(TcpSynSentNext::nodes(output, drop)).with_session_queue(handle),
        );

        send_packet(
            &runtime,
            syn_sent,
            tcp_packet(
                REMOTE,
                REMOTE_PORT,
                LOCAL,
                LOCAL_PORT,
                SERVER_ISN,
                CLIENT_ISN + 1,
                SYN | ACK,
            ),
        );

        assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 2);
        let packets = &output_state.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        assert_tcp_packet(
            &packets[0],
            LOCAL,
            LOCAL_PORT,
            REMOTE,
            REMOTE_PORT,
            CLIENT_ISN + 1,
            SERVER_ISN + 1,
            ACK,
        );
        TcpSessionProtocol::with_queue(handle, |queue| {
            let session = queue
                .session_state(session_id)
                .expect("syn-sent session should remain installed");
            assert_eq!(session.state(), TcpState::Established);
            assert_eq!(session.snd_una(), CLIENT_ISN + 1);
            assert_eq!(session.rcv_nxt(), SERVER_ISN + 1);
            Ok(())
        })
        .expect("inspect session");
    }

    #[test]
    fn rst_closes_half_open_session() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let handle = TcpSessionProtocol::register_queue(
            DataWorkerId::new(0),
            runtime.packet_buffers().clone(),
        )
        .expect("session queue");
        let session_id = insert_syn_sent_session(handle);
        let app_ring = AppRingHandle::new(4, 4);
        let app_op = AppOpId::new(8_001);
        TcpSessionProtocol::with_queue(handle, |queue| {
            assert!(queue.bind_session_app_ring(session_id, app_op, app_ring.clone()));
            Ok(())
        })
        .expect("bind app ring");
        let output_state = Arc::new(Mutex::new(CaptureState::default()));
        let output = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&output_state)));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let syn_sent = runtime.nodes().register_internal(
            TcpSynSentNode::new(TcpSynSentNext::nodes(output, drop)).with_session_queue(handle),
        );

        send_packet(
            &runtime,
            syn_sent,
            tcp_packet(
                REMOTE,
                REMOTE_PORT,
                LOCAL,
                LOCAL_PORT,
                SERVER_ISN,
                CLIENT_ISN + 1,
                RST | ACK,
            ),
        );

        assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 1);
        assert!(output_state.lock().unwrap().packets.is_empty());
        let completion = app_ring.pop_completion().expect("closed completion");
        match completion.kind() {
            AppCqeKind::Closed { op } => assert_eq!(*op, Some(app_op)),
            other => panic!("expected closed completion, got {other:?}"),
        }
        TcpSessionProtocol::with_queue(handle, |queue| {
            assert!(queue.session_state(session_id).is_none());
            assert!(
                queue
                    .session_id_by_tuple(local_addr(), remote_addr())
                    .is_none()
            );
            Ok(())
        })
        .expect("inspect queue");
    }

    fn insert_syn_sent_session(handle: SessionQueueHandle) -> crate::session::SessionId {
        TcpSessionProtocol::with_queue(handle, |queue| {
            let connection = TcpConnectionState::new(
                Some(TcpConnectionId::new(1)),
                DataWorkerId::new(0),
                TcpState::SynSent,
                LOCAL_PORT,
                Some(local_addr()),
                remote_addr(),
            );
            let session_id = queue.insert_session(connection);
            {
                let connection = queue
                    .session_state_mut(session_id)
                    .expect("inserted session");
                connection.set_sequence_state(
                    CLIENT_ISN,
                    0,
                    CLIENT_ISN,
                    CLIENT_ISN + 1,
                    u16::MAX as u32,
                    0,
                    u16::MAX as u32,
                );
            }
            let indexed = queue
                .session_state(session_id)
                .expect("inserted session")
                .clone();
            queue.index_session(session_id, &indexed);
            Ok(session_id)
        })
        .expect("insert syn-sent session")
    }

    fn local_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(LOCAL), LOCAL_PORT)
    }

    fn remote_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(REMOTE), REMOTE_PORT)
    }

    fn send_packet(runtime: &DataPlaneRuntime, node: NodeId, packet: std::vec::Vec<u8>) {
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
        assert_eq!(&packet[12..16], &source.octets());
        assert_eq!(&packet[16..20], &destination.octets());
        assert_eq!(u16::from_be_bytes([packet[20], packet[21]]), source_port);
        assert_eq!(
            u16::from_be_bytes([packet[22], packet[23]]),
            destination_port
        );
        assert_eq!(
            u32::from_be_bytes([packet[24], packet[25], packet[26], packet[27]]),
            sequence
        );
        assert_eq!(
            u32::from_be_bytes([packet[28], packet[29], packet[30], packet[31]]),
            acknowledgment
        );
        assert_eq!(packet[33] & flags, flags);
    }

    fn tcp_packet(
        source: Ipv4Addr,
        source_port: u16,
        destination: Ipv4Addr,
        destination_port: u16,
        sequence: u32,
        acknowledgment: u32,
        flags: u8,
    ) -> std::vec::Vec<u8> {
        let mut packet = ipv4_packet(source, destination, 6, 20);
        write_tcp_segment(
            &mut packet[20..],
            source_port,
            destination_port,
            sequence,
            acknowledgment,
            flags,
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
    ) -> std::vec::Vec<u8> {
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
    ) {
        segment[0..2].copy_from_slice(&source_port.to_be_bytes());
        segment[2..4].copy_from_slice(&destination_port.to_be_bytes());
        segment[4..8].copy_from_slice(&sequence.to_be_bytes());
        segment[8..12].copy_from_slice(&acknowledgment.to_be_bytes());
        segment[12] = 0x50;
        segment[13] = flags;
        segment[14..16].copy_from_slice(&u16::MAX.to_be_bytes());
    }

    fn ipv4_l4_checksum(
        source: Ipv4Addr,
        destination: Ipv4Addr,
        protocol: u8,
        segment: &[u8],
    ) -> u16 {
        let mut pseudo = std::vec::Vec::with_capacity(12 + segment.len() + (segment.len() & 1));
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
}

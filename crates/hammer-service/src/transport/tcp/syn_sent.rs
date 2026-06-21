use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeNextFrames, NodeProcessFn,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};

use crate::transport::congestion::CongestionController;
use super::segment::parse_tcp_packet;
use super::TcpQueueHandle;

#[hammer_component_macros::node_next]
pub enum TcpSynSentNext {
    Output,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpSynSentNext)]
pub struct TcpSynSentNode<C: CongestionController + 'static> {
    session_queue: TcpQueueHandle<C>,
}

impl<C> Node for TcpSynSentNode<C>
where
    C: CongestionController + 'static,
{
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
        tcp_syn_sent_process::<C>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_syn_sent_process<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let next = TcpSynSentNode::<C>::runtime_nexts(runtime)?;
    tcp_syn_sent_frame::<C>(runtime, frame, TcpQueueHandle::new(data), next)
}

fn tcp_syn_sent_frame<C>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: TcpQueueHandle<C>,
    next: [NodeId; TcpSynSentNext::COUNT],
) -> CoreResult<NodeResult>
where
    C: CongestionController + 'static,
{
    let tcp_output = next[TcpSynSentNext::Output as usize];
    let drop_next = next[TcpSynSentNext::Drop as usize];
    let mut next_frames = NodeNextFrames::default();
    frame.rewrite_indices_batched(runtime.preferred_frame_batch_width(), |index| {
        match tcp_syn_sent_index(runtime, index, session_queue, tcp_output, &mut next_frames) {
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

fn tcp_syn_sent_index<C>(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    session_queue: TcpQueueHandle<C>,
    tcp_output: NodeId,
    next_frames: &mut NodeNextFrames,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let packet = parse_tcp_packet(runtime, index)?;
    let mut release_input = true;
    let mut tx_index = None;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let (session_id, _, _) = queue
            .pending_route_by_tuple(packet.local, packet.remote)
            .ok_or_else(|| CoreError::internal("tcp syn-sent session is missing"))?;
        let (control, acked_tx_len, established_with_payload) = {
            let connection = queue
                .session_mut(session_id)
                .ok_or_else(|| CoreError::internal("tcp syn-sent session is missing"))?;
            let previous_snd_una = connection.snd_una();
            let previous_state = connection.state();
            let control = connection.receive_open_reply(&packet)?;
            (
                control,
                connection.take_acked_tx_len(previous_snd_una),
                previous_state == crate::transport::tcp::TcpState::SynSent
                    && connection.state() == crate::transport::tcp::TcpState::Established
                    && packet.payload_len != 0,
            )
        };
        if acked_tx_len != 0 {
            queue.release_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if let Some(cookie) = packet
            .fast_open_cookie
            .as_deref()
            .filter(|cookie| !cookie.is_empty())
        {
            queue.aux_mut().remember_fast_open_cookie(
                packet.local,
                packet.remote,
                cookie,
                packet.capabilities.max_segment_size,
            );
        }
        if established_with_payload {
            runtime.packet_buffers().advance(index, packet.payload_offset)?;
            runtime.packet_buffers().truncate_chain(index, packet.payload_len)?;
            let enqueue = queue.enqueue_rx(session_id, index, 0, false)?;
            if enqueue.delivered_len != 0 {
                queue.mark_ready(session_id);
            }
            release_input = false;
        };
        queue.refresh_session_route(session_id)?;
        if let Some(segment) = control {
            let allocated = runtime.packet_buffers().alloc_index(Default::default())?;
            if let Err(error) = segment.write_to_buffer(runtime, allocated) {
                runtime.free_index(allocated);
                return Err(error);
            }
            tx_index = Some(allocated);
        }
        Ok(())
    };
    if let Err(error) = result {
        if let Some(tx_index) = tx_index.take() {
            runtime.free_index(tx_index);
        }
        return Err(error);
    }
    if let Some(tx_index) = tx_index.take()
        && let Err(error) = next_frames.enqueue(runtime, tcp_output, tx_index)
    {
        runtime.free_index(tx_index);
        return Err(error);
    }
    if release_input {
        runtime.free_index(index);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_adapter::{
        BufferPacketCursor, DataWorkerId, InternalNode, Network, NodeId, RouteMetadata, SocksAddr,
    };
    use hammer_core::error::CoreError;
    use hammer_runtime::app::{AppCqeKind, AppOpId, AppRingHandle};

    use crate::data_plane::DropNode;
    use crate::transport::congestion::BbrController;
    use crate::transport::tcp::{
        TcpInputNext, TcpQueueHandle, register_tcp_session_queue,
    };
    use crate::transport::tcp::output::{TcpOutputNext, TcpOutputNode};

    use super::*;

    const LOCAL: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
    const REMOTE: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 20);
    const LOCAL_PORT: u16 = 50_001;
    const REMOTE_PORT: u16 = 443;
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
        let handle = register_tcp_session_queue::<BbrController>(
            DataWorkerId::new(0),
            runtime.packet_buffers().clone(),
        )
        .expect("register queue");
        let (session_id, client_isn) = open_client_session(handle);
        let app_ring = AppRingHandle::new(4, 4);
        let app_op = AppOpId::new(8_000);
        {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            assert!(queue.bind_session_app_ring(session_id, app_op, app_ring.clone()));
        }
        let output_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&output_state)));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let output = runtime
            .nodes()
            .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, capture)));
        let syn_sent = runtime
            .nodes()
            .register_internal(TcpSynSentNode::new(handle, TcpSynSentNext::nodes(output, drop)));

        send_packet(
            &runtime,
            syn_sent,
            tcp_packet(
                REMOTE,
                REMOTE_PORT,
                LOCAL,
                LOCAL_PORT,
                SERVER_ISN,
                client_isn + 1,
                SYN | ACK,
            ),
        );

        assert_eq!(runtime.run_ready_nodes().expect("run syn-sent"), 3);
        let packets = &output_state.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        assert_tcp_packet(
            &packets[0],
            LOCAL,
            LOCAL_PORT,
            REMOTE,
            REMOTE_PORT,
            client_isn + 1,
            SERVER_ISN + 1,
            ACK,
        );
        {
            let queue = handle.borrow_mut().expect("tcp queue");
            let connection = queue.session(session_id).expect("tcp session is missing");
            assert_eq!(connection.state(), crate::transport::tcp::TcpState::Established);
            assert_eq!(connection.snd_una(), client_isn + 1);
            assert_eq!(connection.rcv_nxt(), SERVER_ISN + 1);
            assert_eq!(
                queue.session_route_by_tuple(local_addr(), remote_addr()),
                Some((session_id, DataWorkerId::new(0), TcpInputNext::Established))
            );
            assert_eq!(
                queue.pending_route_by_tuple(local_addr(), remote_addr()),
                None
            );
        }
        let completion = app_ring.pop_completion().expect("connected completion");
        match completion.kind() {
            AppCqeKind::Connected { op } => assert_eq!(*op, app_op),
            other => panic!("expected connected completion, got {other:?}"),
        }
    }

    #[test]
    fn rst_closes_pending_syn_sent_session() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let handle = register_tcp_session_queue::<BbrController>(
            DataWorkerId::new(0),
            runtime.packet_buffers().clone(),
        )
        .expect("register queue");
        let (session_id, client_isn) = open_client_session(handle);
        let app_ring = AppRingHandle::new(4, 4);
        let app_op = AppOpId::new(8_001);
        {
            let mut queue = handle.borrow_mut().expect("tcp queue");
            assert!(queue.bind_session_app_ring(session_id, app_op, app_ring.clone()));
        }
        let output_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&output_state)));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let output = runtime
            .nodes()
            .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, capture)));
        let syn_sent = runtime
            .nodes()
            .register_internal(TcpSynSentNode::new(handle, TcpSynSentNext::nodes(output, drop)));

        send_packet(
            &runtime,
            syn_sent,
            tcp_packet(
                REMOTE,
                REMOTE_PORT,
                LOCAL,
                LOCAL_PORT,
                SERVER_ISN,
                client_isn + 1,
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
        {
            let queue = handle.borrow_mut().expect("tcp queue");
            assert!(queue.session(session_id).is_none());
            assert!(
                queue
                    .session_route_by_tuple(local_addr(), remote_addr())
                    .is_none()
            );
            assert!(
                queue
                    .pending_route_by_tuple(local_addr(), remote_addr())
                    .is_none()
            );
        }
    }

    fn open_client_session(
        handle: TcpQueueHandle<BbrController>,
    ) -> (crate::session::SessionId, u32) {
        let mut queue = handle.borrow_mut().expect("tcp queue");
        let session_id = queue
            .connect(local_addr(), remote_addr())
            .expect("connect session");
        let connection = queue.session(session_id).expect("tcp session is missing");
        assert_eq!(connection.state(), crate::transport::tcp::TcpState::SynSent);
        let initial_sequence = connection.iss();
        (session_id, initial_sequence)
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

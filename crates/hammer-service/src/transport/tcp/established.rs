use crate::session::runtime::RxDelivery;
use hammer_core::data_plane::{
    BufferFrame, Index, NodeId, NodeNext, DEFAULT_BUFFER_FRAME_CAPACITY,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::segment::Segment;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};

use super::segment::tcp_packet;
use super::{TcpNodeError, TcpWorker, publish_tcp_connection, read_session_id};
use crate::session::SessionQueueHandle;
use crate::session::app::SessionAppRuntimeCreate;
use crate::session::runtime::SessionDriverRuntime;
use crate::transport::congestion::CongestionController;

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    #[next("tcp-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::transport::tcp::established::register_tcp_established,
    name = "tcp-established",
    next = TcpEstablishedNext,
    role = internal,
)]
pub struct TcpEstablishedNode<C: CongestionController + 'static, Seg: Segment> {
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
}

pub fn register_tcp_established(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime
        .nodes()
        .node_by_name("tcp-established")
        .ok_or_else(|| CoreError::internal("TCP worker graph is not registered"))
}

impl<C, Seg> Node for TcpEstablishedNode<C, Seg>
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    #[inline(always)]
    fn process(&mut self, runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        tcp_established_frame(runtime, frame, self.session_queue)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_established_process::<C, Seg>
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.session_queue.runtime_data())
    }
}

fn tcp_established_process<C, Seg>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    tcp_established_frame::<C, Seg>(
        runtime,
        frame,
        SessionQueueHandle::<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>::new(data),
    )
}

fn tcp_established_frame<C, Seg>(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
) -> NodeResult
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let input_len = frame.len();
    debug_assert!(input_len <= DEFAULT_BUFFER_FRAME_CAPACITY);
    let mut inputs =
        [core::mem::MaybeUninit::<Index>::uninit(); DEFAULT_BUFFER_FRAME_CAPACITY];
    for (offset, &index) in frame.indices().iter().enumerate() {
        inputs[offset].write(index);
    }
    frame.discard_prefix(input_len);

    let mut nexts = [0u16; DEFAULT_BUFFER_FRAME_CAPACITY];
    let mut out_len = 0usize;
    for offset in 0..input_len {
        let index = unsafe { inputs[offset].assume_init() };
        if tcp_established_index(
            runtime,
            index,
            session_queue,
            frame,
            &mut nexts,
            &mut out_len,
        )
        .is_err()
        {
            let _ = emit_local(
                runtime,
                frame,
                &mut nexts,
                &mut out_len,
                TcpEstablishedNext::Drop,
                index,
            );
        }
    }
    if out_len != 0 {
        runtime.enqueue_to_next(frame, &nexts[..out_len]);
    }
    NodeResult::drop()
}

#[inline]
fn emit_local(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
    next: TcpEstablishedNext,
    index: Index,
) -> CoreResult<()> {
    if *out_len == DEFAULT_BUFFER_FRAME_CAPACITY {
        runtime.enqueue_to_next(frame, &nexts[..*out_len]);
        *out_len = 0;
    }
    nexts[*out_len] = NodeNext::slot(next);
    frame.push_index(index)?;
    *out_len += 1;
    debug_assert_eq!(*out_len, frame.len());
    Ok(())
}

fn tcp_established_index<C, Seg>(
    runtime: &DataPlaneRuntime,
    index: Index,
    session_queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
    out_frame: &mut BufferFrame,
    nexts: &mut [u16; DEFAULT_BUFFER_FRAME_CAPACITY],
    out_len: &mut usize,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
    Seg: Segment,
    crate::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let packet = tcp_packet(runtime, index)?;
    let mut tx_segment = None;
    let result = {
        let mut queue = session_queue.borrow_mut()?;
        let session_id = read_session_id(runtime, index)?.ok_or_else(|| {
            let _ = runtime
                .record_current_node_error(TcpNodeError::EstablishedSessionRouteMissing.code());
            TcpNodeError::EstablishedSessionRouteMissing
        })?;
        // Warm the session pool slot cacheline before the `session_mut`
        // borrow; the `receive_established`/`accept_payload` work below gives
        // the prefetch lead time.
        queue.prefetch_session(session_id);
        let (
            control,
            acked_tx_len,
            ack_advanced,
            accept_payload,
            accepted_sequence,
            duplicate_payload,
        ) = {
            let (_, connection_index) = queue
                .sessions()
                .session_transport(session_id)
                .ok_or(TcpNodeError::EstablishedSessionMissing)?;
            let worker = &mut queue.transports_mut().0;
            let crate::transport::tcp::worker::TcpWorker {
                connections,
                timers,
                ..
            } = worker;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            let previous_snd_una = connection.snd_una();
            let control = connection.receive_established_with_timers(
                connection_index,
                timers,
                &packet,
                std::time::Instant::now(),
            )?;
            let accept_payload = connection.accept_payload(&packet);
            let duplicate_payload = accept_payload.is_none() && packet.payload_len != 0;
            (
                control,
                connection.take_acked_tx_len(previous_snd_una),
                connection.snd_una() != previous_snd_una,
                accept_payload,
                packet.sequence,
                duplicate_payload,
            )
        };
        if acked_tx_len != 0 {
            queue.ack_tx_up_to(session_id, acked_tx_len as usize)?;
        }
        if ack_advanced && queue.app().pending_send_len(session_id)?.is_some() {
            queue.mark_session_ready(session_id);
        }
        let mut immediate_ack = false;
        if let Some((trim, offset)) = accept_payload {
            let accepted_len = packet.payload_len.saturating_sub(trim) as u32;
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset.saturating_add(trim) as isize)?;
                buffer.truncate(accepted_len as usize)?;
            }
            let delivery = queue.enqueue_rx(session_id, index, offset, false)?;
            let (_, connection_index) = queue
                .sessions()
                .session_transport(session_id)
                .ok_or(TcpNodeError::EstablishedSessionMissing)?;
            let worker = &mut queue.transports_mut().0;
            let crate::transport::tcp::worker::TcpWorker {
                connections,
                timers,
                ..
            } = worker;
            let connection = connections.get_mut(connection_index).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            let rx_available = match delivery {
                RxDelivery::NotAccepted { rx_available }
                | RxDelivery::InOrder { rx_available, .. }
                | RxDelivery::OutOfOrder { rx_available, .. } => rx_available as usize,
            };
            connection.receive_payload(accepted_sequence, trim as u32, delivery);
            let clean_in_order = trim == 0
                && offset == 0
                && matches!(
                    delivery,
                    RxDelivery::InOrder {
                        accepted,
                        promoted,
                        ..
                    } if promoted == 0 && accepted.get() == accepted_len
                );
            immediate_ack = if clean_in_order {
                connection.on_clean_in_order_payload(connection_index, timers)?
            } else {
                true
            };
            if matches!(delivery, RxDelivery::InOrder { .. }) {
                queue.mark_session_ready(session_id);
            }
            match delivery {
                RxDelivery::NotAccepted { .. } => {}
                RxDelivery::InOrder {
                    accepted, promoted, ..
                } => {
                    if accepted.get() != accepted_len || promoted != 0 {
                        immediate_ack = true;
                    }
                }
                RxDelivery::OutOfOrder { accepted, .. } => {
                    if accepted.get() != accepted_len {
                        immediate_ack = true;
                    }
                }
            }
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.set_rcv_wnd(rx_available);
        } else if duplicate_payload {
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            let sequence = packet.sequence;
            let end_sequence = sequence.advance(packet.payload_len as u32);
            connection.observe_duplicate_payload(sequence, end_sequence);
            immediate_ack = true;
        }

        if immediate_ack {
            let connection = queue.session_mut(session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            tx_segment = Some(connection.control_segment(
                packet.local,
                packet.remote,
                hammer_core::protocol::tcp::TcpSegmentFlags::ACK,
                None,
                hammer_core::protocol::tcp::TcpCapabilities::default(),
            ));
        }
        publish_tcp_connection(&mut queue, session_id)?;
        if tx_segment.is_none() {
            tx_segment = control;
        }
        Ok(())
    };
    if let Err(error) = result {
        return Err(error);
    }
    if let Some(segment) = tx_segment.take() {
        let allocated = runtime.buffers().alloc_index()?;
        segment.write_to_buffer(runtime.buffers(), allocated)?;
        emit_local(
            runtime,
            out_frame,
            nexts,
            out_len,
            TcpEstablishedNext::Output,
            allocated,
        )?;
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_core::data_plane::{BufferFrame, NodeId};
    use hammer_core::error::CoreResult;
    use hammer_core::protocol::tcp::{TcpCapabilities, TcpConnectionId, TcpSegmentFlags};
    use hammer_infra::checksum::{internet_checksum, internet_checksum_parts};
    use hammer_infra::pool::Index as PoolIndex;
    use hammer_infra::segment::Local;
    use hammer_runtime::{
        DataPlaneRuntime, DataWorkerId, InternalNode, Node, NodeProcessFn, NodeResult,
        NodeRuntimeData,
    };

    use super::*;
    use crate::data_plane::DropNode;
    use crate::net::NetworkOpaque;
    use crate::session::runtime::SessionDriverRuntime;
    use crate::session::{SessionId, SessionQueueHandle};
    use crate::transport::congestion::BbrController;
    use crate::transport::tcp::output::{TcpOutputNext, TcpOutputNode};
    use crate::transport::tcp::tcp_control_cursor;
    use crate::transport::tcp::{
        TCP_FLAG_ACK, TcpConnection, TcpInputNext, TcpWorker, publish_tcp_connection,
        write_session_route_opaque,
    };

    const LOCAL_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
    const REMOTE_IP: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 20);
    const LOCAL_PORT: u16 = 443;
    const REMOTE_PORT: u16 = 50_001;

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
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
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
    ) -> NodeResult {
        let state = match capture_states().lock() {
            Ok(states) => {
                let slot = match data.usize_word(0) {
                    Ok(slot) => slot,
                    Err(_) => return NodeResult::drop(),
                };
                match states.get(slot) {
                    Some(state) => Arc::clone(state),
                    None => return NodeResult::drop(),
                }
            }
            Err(_) => return NodeResult::drop(),
        };
        for &index in frame.pending_indices() {
            let packet = match runtime.get_buffer(index) {
                Ok(buffer) => buffer.current().to_vec(),
                Err(_) => continue,
            };
            match state.lock() {
                Ok(mut state) => state.packets.push(packet),
                Err(_) => continue,
            }
        }
        NodeResult::drop()
    }

    fn local_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(LOCAL_IP), LOCAL_PORT)
    }

    fn remote_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(REMOTE_IP), REMOTE_PORT)
    }

    fn install_established_runtime(
        runtime: &DataPlaneRuntime,
    ) -> (
        NodeId,
        SessionQueueHandle<SessionDriverRuntime<(TcpWorker<BbrController>, ()), Local, PoolIndex>>,
        Arc<Mutex<CaptureState>>,
        Arc<Mutex<CaptureState>>,
    ) {
        let worker = DataWorkerId::new(0);
        let driver = SessionDriverRuntime::new(
            worker,
            runtime.buffers().clone(),
            (TcpWorker::<BbrController>::new(worker), ()),
        );
        let handle = crate::session::node::register_session_queue(driver).expect("register queue");

        let output_state = Arc::new(Mutex::new(CaptureState::default()));
        let drop_state = Arc::new(Mutex::new(CaptureState::default()));
        let capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&output_state)));
        let drop_capture = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
        let drop = runtime.nodes().register_internal(DropNode::new());
        let output = runtime
            .nodes()
            .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, capture)));
        let established = runtime.nodes().register_internal(TcpEstablishedNode::new(
            handle,
            TcpEstablishedNext::nodes(output, drop_capture),
        ));
        (established, handle, output_state, drop_state)
    }

    fn open_established_session(
        handle: SessionQueueHandle<
            SessionDriverRuntime<(TcpWorker<BbrController>, ()), Local, PoolIndex>,
        >,
    ) -> (SessionId, u32, u32) {
        let mut queue = handle.borrow_mut().expect("tcp queue");
        let session_id = queue
            .insert_session_with_id(|session_id| {
                TcpConnection::established_with_sack_for_test(
                    Some(TcpConnectionId::new(session_id.get())),
                    DataWorkerId::new(0),
                    LOCAL_PORT,
                    Some(local_addr()),
                    remote_addr(),
                )
            })
            .expect("insert established");
        publish_tcp_connection(&mut queue, session_id).expect("publish");
        let connection = queue.session(session_id).expect("session");
        (
            session_id,
            connection.rcv_nxt(),
            connection.snd_nxt(),
        )
    }

    fn send_to_established(
        runtime: &DataPlaneRuntime,
        established: NodeId,
        session_id: Option<SessionId>,
        packet: std::vec::Vec<u8>,
    ) {
        let mut frame = runtime.buffers().get_next_frame(established).expect("frame");
        let buffer = runtime.alloc_index_with_bytes(&packet).expect("packet");
        let cursor = tcp_control_cursor(&packet).expect("cursor");
        let mut data_buffer = runtime.get_buffer_mut(buffer).expect("buffer mut");
        let network =
            unsafe { std::mem::transmute::<_, &mut NetworkOpaque>(data_buffer.opaque_mut()) };
        network.set_packet_cursor(cursor);
        let ip_version = (packet[0] >> 4) as u8;
        network.ip_mut().set_ip_version(Some(ip_version));
        network.ip_mut().set_ip_protocol(Some(6));
        if let Some(session_id) = session_id {
            write_session_route_opaque(
                data_buffer.opaque2_mut(),
                session_id,
                DataWorkerId::new(0),
                TcpInputNext::Established,
            );
        }
        drop(data_buffer);
        frame.push_index(buffer).expect("push packet");
        runtime.put_next_frame(frame).expect("put next frame");
    }

    fn data_packet(sequence: u32, acknowledgment: u32, payload: &[u8]) -> std::vec::Vec<u8> {
        tcp_ipv4_packet(
            remote_addr(),
            local_addr(),
            hammer_core::protocol::tcp::TcpSegmentHeader {
                source_port: remote_addr().port(),
                destination_port: local_addr().port(),
                sequence_number: sequence,
                acknowledgment_number: acknowledgment,
                flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
                advertised_window: u16::MAX,
                capabilities: TcpCapabilities::default(),
                timestamp: None,
                fast_open_cookie: None,
            },
            payload,
        )
        .expect("data packet")
    }

    fn tcp_ipv4_packet(
        local: SocketAddr,
        remote: SocketAddr,
        header: hammer_core::protocol::tcp::TcpSegmentHeader<'_>,
        payload: &[u8],
    ) -> Result<std::vec::Vec<u8>, hammer_core::protocol::tcp::TcpError> {
        let mut tcp = [0u8; 60];
        let tcp_header_len =
            hammer_core::protocol::tcp::write_tcp_segment_header(&mut tcp, header, None)?;
        let tcp_len = tcp_header_len
            .checked_add(payload.len())
            .ok_or(hammer_core::protocol::tcp::TcpError::Dispatch)?;
        let (IpAddr::V4(local_ip), IpAddr::V4(remote_ip)) = (local.ip(), remote.ip()) else {
            return Err(hammer_core::protocol::tcp::TcpError::SegmentInvalid);
        };
        let packet_len = 20usize.checked_add(tcp_len).ok_or(hammer_core::protocol::tcp::TcpError::Dispatch)?;
        let total_len = u16::try_from(packet_len).map_err(|_| hammer_core::protocol::tcp::TcpError::Length)?;
        let mut packet = std::vec![0u8; packet_len];
        packet[0] = 0x45;
        packet[2] = (total_len >> 8) as u8;
        packet[3] = total_len as u8;
        packet[8] = 64;
        packet[9] = 6;
        packet[12..16].copy_from_slice(&local_ip.octets());
        packet[16..20].copy_from_slice(&remote_ip.octets());
        packet[20..20 + tcp_header_len].copy_from_slice(&tcp[..tcp_header_len]);
        if !payload.is_empty() {
            packet[20 + tcp_header_len..].copy_from_slice(payload);
        }
        let tcp_len_bytes = [(tcp_len as u16 >> 8) as u8, tcp_len as u8];
        let tcp_checksum = internet_checksum_parts(&[
            &local_ip.octets(),
            &remote_ip.octets(),
            &[0, 6],
            &tcp_len_bytes,
            &packet[20..],
        ]);
        packet[36] = (tcp_checksum >> 8) as u8;
        packet[37] = tcp_checksum as u8;
        let ip_checksum = internet_checksum(&packet[..20]);
        packet[10] = (ip_checksum >> 8) as u8;
        packet[11] = ip_checksum as u8;
        Ok(packet)
    }

    fn tcp_flags(packet: &[u8]) -> u8 {
        packet[13]
    }

    fn tcp_acknowledgment(packet: &[u8]) -> u32 {
        u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]])
    }

    #[test]
    fn in_order_payload_advances_receive_window() {
        let runtime = DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
            buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                buffer_slot_capacity: 2048,
                buffer_slots: 32,
                frame_slots: 8,
                ..hammer_core::data_plane::DataPlaneBufferConfig::default()
            },
        });
        let (established, handle, output_state, _) = install_established_runtime(&runtime);
        let (session_id, rcv_nxt, snd_nxt) = open_established_session(handle);
        let rx_before = {
            let queue = handle.borrow_mut().expect("tcp queue");
            queue
                .app()
                .rx_available_len(session_id)
                .expect("rx capacity")
        };

        send_to_established(
            &runtime,
            established,
            Some(session_id),
            data_packet(rcv_nxt, snd_nxt, b"hello"),
        );
        assert!(runtime.run_ready_nodes().expect("run established") >= 1);

        let queue = handle.borrow_mut().expect("tcp queue");
        let connection = queue.session(session_id).expect("session");
        assert_eq!(connection.rcv_nxt(), rcv_nxt + 5);
        assert_eq!(
            queue.app().rx_available_len(session_id).expect("rx capacity"),
            rx_before - 5
        );
        // Delayed ACK: first clean segment does not emit an immediate ACK.
        assert!(output_state.lock().expect("output").packets.is_empty());
    }

    #[test]
    fn duplicate_segment_triggers_immediate_ack() {
        let runtime = DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
            buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                buffer_slot_capacity: 2048,
                buffer_slots: 32,
                frame_slots: 8,
                ..hammer_core::data_plane::DataPlaneBufferConfig::default()
            },
        });
        let (established, handle, output_state, _) = install_established_runtime(&runtime);
        let (session_id, rcv_nxt, snd_nxt) = open_established_session(handle);

        send_to_established(
            &runtime,
            established,
            Some(session_id),
            data_packet(rcv_nxt, snd_nxt, b"hello"),
        );
        assert!(runtime.run_ready_nodes().expect("run first") >= 1);

        send_to_established(
            &runtime,
            established,
            Some(session_id),
            data_packet(rcv_nxt, snd_nxt, b"hello"),
        );
        for _ in 0..8 {
            let _ = runtime.run_ready_nodes().expect("drain");
            if !output_state.lock().expect("output").packets.is_empty() {
                break;
            }
        }

        let queue = handle.borrow_mut().expect("tcp queue");
        let connection = queue.session(session_id).expect("session");
        assert_eq!(connection.rcv_nxt(), rcv_nxt + 5);
        let packets = output_state.lock().expect("output");
        assert_eq!(packets.packets.len(), 1);
        assert_eq!(tcp_flags(&packets.packets[0]) & TCP_FLAG_ACK, TCP_FLAG_ACK);
        assert_eq!(tcp_acknowledgment(&packets.packets[0]), rcv_nxt + 5);
    }

    #[test]
    fn segment_without_session_is_discarded_without_side_effects() {
        let runtime = DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
            buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                buffer_slot_capacity: 2048,
                buffer_slots: 16,
                frame_slots: 8,
                ..hammer_core::data_plane::DataPlaneBufferConfig::default()
            },
        });
        let (established, handle, output_state, _) = install_established_runtime(&runtime);
        let (session_id, rcv_nxt, snd_nxt) = open_established_session(handle);

        send_to_established(
            &runtime,
            established,
            None,
            data_packet(rcv_nxt, snd_nxt, b"x"),
        );
        let _ = runtime.run_ready_nodes().expect("run established");
        let _ = runtime.run_ready_nodes().expect("drain");

        let queue = handle.borrow_mut().expect("tcp queue");
        let connection = queue.session(session_id).expect("session");
        assert_eq!(connection.rcv_nxt(), rcv_nxt);
        assert!(output_state.lock().expect("output").packets.is_empty());
    }
}

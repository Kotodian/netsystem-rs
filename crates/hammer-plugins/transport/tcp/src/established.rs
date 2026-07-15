use crate::{insert_tcp_session, mark_tcp_session_ready, tcp_session, tcp_session_mut};
use hammer_core::data_plane::{
    BufferFrame, DEFAULT_BUFFER_FRAME_CAPACITY, Index, NodeId, NodeNext,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::pool::Index as PoolIndex;
use hammer_runtime::app::SessionSegment;
use hammer_runtime::{DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData};
use hammer_service::session::runtime::RxDelivery;

use super::segment::tcp_packet;
use super::{TcpNodeError, TcpWorker, publish_tcp_connection, read_session_id};
use hammer_service::session::SessionQueueHandle;
use hammer_service::session::app::SessionAppRuntimeCreate;
use hammer_service::session::runtime::SessionDriverRuntime;
use hammer_service::transport::congestion::CongestionController;

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    #[next("tcp-output")]
    Output,
    Drop,
}

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = crate::established::register_tcp_established,
    name = "tcp-established",
    next = TcpEstablishedNext,
    role = internal,
)]
pub struct TcpEstablishedNode<C: CongestionController + 'static, Seg: SessionSegment> {
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
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let input_len = frame.len();
    debug_assert!(input_len <= DEFAULT_BUFFER_FRAME_CAPACITY);
    let mut inputs = [core::mem::MaybeUninit::<Index>::uninit(); DEFAULT_BUFFER_FRAME_CAPACITY];
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
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
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
            let crate::worker::TcpWorker {
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
            mark_tcp_session_ready(&mut *queue, session_id);
        }
        let mut immediate_ack = false;
        if let Some((trim, offset)) = accept_payload {
            let accepted_len = packet.payload_len.saturating_sub(trim) as u32;
            {
                let mut buffer = runtime.buffers().get_buffer_mut(index)?;
                buffer.advance(packet.payload_offset.saturating_add(trim) as isize)?;
                buffer.truncate(accepted_len as usize)?;
            }
            let delivery = queue.enqueue_rx(
                session_id,
                index,
                offset,
                packet
                    .flags
                    .contains(hammer_core::protocol::tcp::TcpSegmentFlags::URG),
            )?;
            let (_, connection_index) = queue
                .sessions()
                .session_transport(session_id)
                .ok_or(TcpNodeError::EstablishedSessionMissing)?;
            let worker = &mut queue.transports_mut().0;
            let crate::worker::TcpWorker {
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
                mark_tcp_session_ready(&mut *queue, session_id);
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
            let connection = tcp_session_mut(&mut *queue, session_id).ok_or_else(|| {
                let _ = runtime
                    .record_current_node_error(TcpNodeError::EstablishedSessionMissing.code());
                TcpNodeError::EstablishedSessionMissing
            })?;
            connection.set_rcv_wnd(rx_available);
        } else if duplicate_payload {
            let connection = tcp_session_mut(&mut *queue, session_id).ok_or_else(|| {
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
            let connection = tcp_session_mut(&mut *queue, session_id).ok_or_else(|| {
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

    use std::mem::transmute;

    use hammer_core::data_plane::{BufferFrame, BufferPacketCursor, NodeId};
    use hammer_core::error::CoreResult;
    use hammer_core::protocol::ip::write_ipv4_push_header;
    use hammer_core::protocol::tcp::{
        TcpCapabilities, TcpConnectionId, TcpSegmentFlags, TcpSegmentHeader,
    };
    use hammer_infra::pool::Index as PoolIndex;
    use hammer_infra::segment::Local;
    use hammer_runtime::app::{
        AppSession, AppSessionConfig, SessionEvt, SessionEvtFlags, SessionEvtType, SessionHandle,
    };
    use hammer_runtime::{
        DataPlaneRuntime, DataWorkerId, InternalNode, Node, NodeProcessFn, NodeResult,
        NodeRuntimeData,
    };

    use super::*;
    use crate::output::{TcpOutputNext, TcpOutputNode};
    use crate::{
        TcpConnection, TcpInputNext, TcpState, TcpWorker, publish_tcp_connection,
        write_session_route_opaque,
    };
    use hammer_service::data_plane::DropNode;
    use hammer_service::opaque::NetworkOpaque;
    use hammer_service::session::runtime::SessionDriverRuntime;
    use hammer_service::session::{SessionId, SessionQueueHandle};
    use hammer_service::transport::congestion::BbrController;

    const LOCAL_IP: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
    const REMOTE_IP: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 20);
    const LOCAL_PORT: u16 = 443;
    const REMOTE_PORT: u16 = 50_001;

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

    fn capture_states() -> &'static Mutex<Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(Vec::new()))
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
                Ok(mut state) => state.packets.push(packet.into()),
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
        let handle =
            hammer_service::session::node::register_session_queue(driver).expect("register queue");

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
        let session_id = insert_tcp_session(&mut *queue, |session_id| {
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
        let connection = tcp_session(&*queue, session_id).expect("session");
        (session_id, connection.rcv_nxt(), connection.snd_nxt())
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

        let index = runtime.alloc_index().expect("buffer");
        runtime.buffers().append(index, b"hello").expect("payload");
        {
            let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
            {
                let tcp = buffer.prepend_mut(20).expect("tcp header");
                TcpSegmentHeader {
                    source_port: REMOTE_PORT,
                    destination_port: LOCAL_PORT,
                    sequence_number: rcv_nxt,
                    acknowledgment_number: snd_nxt,
                    flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
                    advertised_window: u16::MAX,
                    urgent_pointer: 0,
                    capabilities: TcpCapabilities::default(),
                    timestamp: None,
                    fast_open_cookie: None,
                }
                .write_to_buffer(tcp, None)
                .expect("write tcp");
            }
            {
                let ip = buffer.prepend_mut(20).expect("ip header");
                write_ipv4_push_header(ip, REMOTE_IP, LOCAL_IP, 6, 45).expect("write ip");
            }
            let packet_len = buffer.current().len();
            let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
            network.set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, 20)
                    .with_transport_header(20, 20)
                    .with_transport_payload_offset(40),
            );
            network.ip_mut().set_ip_version(Some(4));
            network.ip_mut().set_ip_protocol(Some(6));
            write_session_route_opaque(
                buffer.opaque2_mut(),
                session_id,
                DataWorkerId::new(0),
                TcpInputNext::Established,
            );
        }
        let mut frame = runtime
            .buffers()
            .get_next_frame(established)
            .expect("frame");
        frame.push_index(index).expect("push");
        runtime.put_next_frame(frame).expect("put");
        assert!(runtime.run_ready_nodes().expect("run established") >= 1);

        let queue = handle.borrow_mut().expect("tcp queue");
        let connection = tcp_session(&*queue, session_id).expect("session");
        assert_eq!(connection.rcv_nxt(), rcv_nxt + 5);
        assert_eq!(
            queue
                .app()
                .rx_available_len(session_id)
                .expect("rx capacity"),
            rx_before - 5
        );
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

        for _ in 0..2 {
            let index = runtime.alloc_index().expect("buffer");
            runtime.buffers().append(index, b"hello").expect("payload");
            {
                let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
                {
                    let tcp = buffer.prepend_mut(20).expect("tcp header");
                    TcpSegmentHeader {
                        source_port: REMOTE_PORT,
                        destination_port: LOCAL_PORT,
                        sequence_number: rcv_nxt,
                        acknowledgment_number: snd_nxt,
                        flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
                        advertised_window: u16::MAX,
                        urgent_pointer: 0,
                        capabilities: TcpCapabilities::default(),
                        timestamp: None,
                        fast_open_cookie: None,
                    }
                    .write_to_buffer(tcp, None)
                    .expect("write tcp");
                }
                {
                    let ip = buffer.prepend_mut(20).expect("ip header");
                    write_ipv4_push_header(ip, REMOTE_IP, LOCAL_IP, 6, 45).expect("write ip");
                }
                let packet_len = buffer.current().len();
                let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
                network.set_packet_cursor(
                    BufferPacketCursor::new()
                        .with_packet_len(packet_len)
                        .with_network_header(0, 20)
                        .with_transport_header(20, 20)
                        .with_transport_payload_offset(40),
                );
                network.ip_mut().set_ip_version(Some(4));
                network.ip_mut().set_ip_protocol(Some(6));
                write_session_route_opaque(
                    buffer.opaque2_mut(),
                    session_id,
                    DataWorkerId::new(0),
                    TcpInputNext::Established,
                );
            }
            let mut frame = runtime
                .buffers()
                .get_next_frame(established)
                .expect("frame");
            frame.push_index(index).expect("push");
            runtime.put_next_frame(frame).expect("put");
            let _ = runtime.run_ready_nodes().expect("run");
        }
        for _ in 0..8 {
            let _ = runtime.run_ready_nodes().expect("drain");
            if !output_state.lock().expect("output").packets.is_empty() {
                break;
            }
        }

        let queue = handle.borrow_mut().expect("tcp queue");
        let connection = tcp_session(&*queue, session_id).expect("session");
        assert_eq!(connection.rcv_nxt(), rcv_nxt + 5);
        let packets = output_state.lock().expect("output");
        assert_eq!(packets.packets.len(), 1);
        let tcp = &packets.packets[0][20..];
        let wire = hammer_core::protocol::tcp::tcp_header(tcp).expect("tcp hdr");
        assert!(wire.flags().contains(TcpSegmentFlags::ACK));
        assert_eq!(wire.acknowledgment_number(), rcv_nxt + 5);
    }

    #[test]
    fn peer_fin_moves_established_to_close_wait_and_acks() {
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

        let index = runtime.alloc_index().expect("buffer");
        {
            let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
            {
                let tcp = buffer.prepend_mut(20).expect("tcp header");
                TcpSegmentHeader {
                    source_port: REMOTE_PORT,
                    destination_port: LOCAL_PORT,
                    sequence_number: rcv_nxt,
                    acknowledgment_number: snd_nxt,
                    flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
                    advertised_window: u16::MAX,
                    urgent_pointer: 0,
                    capabilities: TcpCapabilities::default(),
                    timestamp: None,
                    fast_open_cookie: None,
                }
                .write_to_buffer(tcp, None)
                .expect("write tcp");
            }
            {
                let ip = buffer.prepend_mut(20).expect("ip header");
                write_ipv4_push_header(ip, REMOTE_IP, LOCAL_IP, 6, 40).expect("write ip");
            }
            let packet_len = buffer.current().len();
            let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
            network.set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, 20)
                    .with_transport_header(20, 20)
                    .with_transport_payload_offset(40),
            );
            network.ip_mut().set_ip_version(Some(4));
            network.ip_mut().set_ip_protocol(Some(6));
            write_session_route_opaque(
                buffer.opaque2_mut(),
                session_id,
                DataWorkerId::new(0),
                TcpInputNext::Established,
            );
        }
        let mut frame = runtime
            .buffers()
            .get_next_frame(established)
            .expect("frame");
        frame.push_index(index).expect("push");
        runtime.put_next_frame(frame).expect("put");
        for _ in 0..8 {
            let _ = runtime.run_ready_nodes().expect("drain");
            if !output_state.lock().expect("output").packets.is_empty() {
                break;
            }
        }

        let queue = handle.borrow_mut().expect("tcp queue");
        let connection = tcp_session(&*queue, session_id).expect("session");
        assert_eq!(connection.state(), TcpState::CloseWait);
        assert_eq!(connection.rcv_nxt(), rcv_nxt + 1);
        let packets = output_state.lock().expect("output");
        assert_eq!(packets.packets.len(), 1);
        let wire = hammer_core::protocol::tcp::tcp_header(&packets.packets[0][20..]).expect("tcp");
        assert!(wire.flags().contains(TcpSegmentFlags::ACK));
        assert_eq!(wire.acknowledgment_number(), rcv_nxt + 1);
    }

    #[test]
    fn out_of_order_peer_fin_does_not_leave_established() {
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

        let index = runtime.alloc_index().expect("buffer");
        {
            let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
            {
                let tcp = buffer.prepend_mut(20).expect("tcp header");
                TcpSegmentHeader {
                    source_port: REMOTE_PORT,
                    destination_port: LOCAL_PORT,
                    sequence_number: rcv_nxt.wrapping_add(1),
                    acknowledgment_number: snd_nxt,
                    flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
                    advertised_window: u16::MAX,
                    urgent_pointer: 0,
                    capabilities: TcpCapabilities::default(),
                    timestamp: None,
                    fast_open_cookie: None,
                }
                .write_to_buffer(tcp, None)
                .expect("write tcp");
            }
            {
                let ip = buffer.prepend_mut(20).expect("ip header");
                write_ipv4_push_header(ip, REMOTE_IP, LOCAL_IP, 6, 40).expect("write ip");
            }
            let packet_len = buffer.current().len();
            let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
            network.set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, 20)
                    .with_transport_header(20, 20)
                    .with_transport_payload_offset(40),
            );
            network.ip_mut().set_ip_version(Some(4));
            network.ip_mut().set_ip_protocol(Some(6));
            write_session_route_opaque(
                buffer.opaque2_mut(),
                session_id,
                DataWorkerId::new(0),
                TcpInputNext::Established,
            );
        }
        let mut frame = runtime
            .buffers()
            .get_next_frame(established)
            .expect("frame");
        frame.push_index(index).expect("push");
        runtime.put_next_frame(frame).expect("put");
        let _ = runtime.run_ready_nodes().expect("run established");
        let _ = runtime.run_ready_nodes().expect("drain");

        let queue = handle.borrow_mut().expect("tcp queue");
        let connection = tcp_session(&*queue, session_id).expect("session");
        assert_eq!(connection.state(), TcpState::Established);
        assert_eq!(connection.rcv_nxt(), rcv_nxt);
        assert!(output_state.lock().expect("output").packets.is_empty());
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

        let index = runtime.alloc_index().expect("buffer");
        runtime.buffers().append(index, b"x").expect("payload");
        {
            let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
            {
                let tcp = buffer.prepend_mut(20).expect("tcp header");
                TcpSegmentHeader {
                    source_port: REMOTE_PORT,
                    destination_port: LOCAL_PORT,
                    sequence_number: rcv_nxt,
                    acknowledgment_number: snd_nxt,
                    flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
                    advertised_window: u16::MAX,
                    urgent_pointer: 0,
                    capabilities: TcpCapabilities::default(),
                    timestamp: None,
                    fast_open_cookie: None,
                }
                .write_to_buffer(tcp, None)
                .expect("write tcp");
            }
            {
                let ip = buffer.prepend_mut(20).expect("ip header");
                write_ipv4_push_header(ip, REMOTE_IP, LOCAL_IP, 6, 41).expect("write ip");
            }
            let packet_len = buffer.current().len();
            let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
            network.set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, 20)
                    .with_transport_header(20, 20)
                    .with_transport_payload_offset(40),
            );
            network.ip_mut().set_ip_version(Some(4));
            network.ip_mut().set_ip_protocol(Some(6));
        }
        let mut frame = runtime
            .buffers()
            .get_next_frame(established)
            .expect("frame");
        frame.push_index(index).expect("push");
        runtime.put_next_frame(frame).expect("put");
        let _ = runtime.run_ready_nodes().expect("run established");
        let _ = runtime.run_ready_nodes().expect("drain");

        let queue = handle.borrow_mut().expect("tcp queue");
        let connection = tcp_session(&*queue, session_id).expect("session");
        assert_eq!(connection.rcv_nxt(), rcv_nxt);
        assert!(output_state.lock().expect("output").packets.is_empty());
    }

    #[test]
    fn urg_segment_marks_session_rx_event_flag() {
        let runtime = DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
            buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                buffer_slot_capacity: 2048,
                buffer_slots: 32,
                frame_slots: 8,
                ..hammer_core::data_plane::DataPlaneBufferConfig::default()
            },
        });
        let (established, handle, _, _) = install_established_runtime(&runtime);
        let (session_id, rcv_nxt, snd_nxt) = open_established_session(handle);
        let app = Arc::new(
            AppSession::new_in_segment(
                Local::default(),
                AppSessionConfig::new(256, 16),
                SessionHandle::new(session_id.pool_index().slot(), 0),
                handle
                    .borrow_mut()
                    .expect("tcp queue")
                    .app()
                    .tx_evt_q()
                    .clone(),
            )
            .expect("app session"),
        );
        handle
            .borrow_mut()
            .expect("tcp queue")
            .app_mut()
            .attach_session(session_id, Arc::clone(&app));
        app.want_rx_notification();

        let index = runtime.alloc_index().expect("buffer");
        runtime.buffers().append(index, b"urg").expect("payload");
        {
            let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
            {
                let tcp = buffer.prepend_mut(20).expect("tcp header");
                TcpSegmentHeader {
                    source_port: REMOTE_PORT,
                    destination_port: LOCAL_PORT,
                    sequence_number: rcv_nxt,
                    acknowledgment_number: snd_nxt,
                    flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH | TcpSegmentFlags::URG,
                    advertised_window: u16::MAX,
                    urgent_pointer: 3,
                    capabilities: TcpCapabilities::default(),
                    timestamp: None,
                    fast_open_cookie: None,
                }
                .write_to_buffer(tcp, None)
                .expect("write tcp");
            }
            {
                let ip = buffer.prepend_mut(20).expect("ip header");
                write_ipv4_push_header(ip, REMOTE_IP, LOCAL_IP, 6, 43).expect("write ip");
            }
            let packet_len = buffer.current().len();
            let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
            network.set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, 20)
                    .with_transport_header(20, 20)
                    .with_transport_payload_offset(40),
            );
            network.ip_mut().set_ip_version(Some(4));
            network.ip_mut().set_ip_protocol(Some(6));
            write_session_route_opaque(
                buffer.opaque2_mut(),
                session_id,
                DataWorkerId::new(0),
                TcpInputNext::Established,
            );
        }
        let mut frame = runtime
            .buffers()
            .get_next_frame(established)
            .expect("frame");
        frame.push_index(index).expect("push");
        runtime.put_next_frame(frame).expect("put");
        assert!(runtime.run_ready_nodes().expect("run established") >= 1);

        let mut out = [SessionEvt::io(0, SessionEvtType::Close)];
        assert_eq!(app.poll_events(&mut out), 1);
        assert_eq!(out[0].evt_type, SessionEvtType::RxEnq);
        assert!(out[0].flags().contains(SessionEvtFlags::URGENT));
    }

    #[test]
    fn non_urg_segment_leaves_session_rx_event_flags_clear() {
        let runtime = DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
            buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                buffer_slot_capacity: 2048,
                buffer_slots: 32,
                frame_slots: 8,
                ..hammer_core::data_plane::DataPlaneBufferConfig::default()
            },
        });
        let (established, handle, _, _) = install_established_runtime(&runtime);
        let (session_id, rcv_nxt, snd_nxt) = open_established_session(handle);
        let app = Arc::new(
            AppSession::new_in_segment(
                Local::default(),
                AppSessionConfig::new(256, 16),
                SessionHandle::new(session_id.pool_index().slot(), 0),
                handle
                    .borrow_mut()
                    .expect("tcp queue")
                    .app()
                    .tx_evt_q()
                    .clone(),
            )
            .expect("app session"),
        );
        handle
            .borrow_mut()
            .expect("tcp queue")
            .app_mut()
            .attach_session(session_id, Arc::clone(&app));
        app.want_rx_notification();

        let index = runtime.alloc_index().expect("buffer");
        runtime.buffers().append(index, b"ok").expect("payload");
        {
            let mut buffer = runtime.get_buffer_mut(index).expect("buffer mut");
            {
                let tcp = buffer.prepend_mut(20).expect("tcp header");
                TcpSegmentHeader {
                    source_port: REMOTE_PORT,
                    destination_port: LOCAL_PORT,
                    sequence_number: rcv_nxt,
                    acknowledgment_number: snd_nxt,
                    flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
                    advertised_window: u16::MAX,
                    urgent_pointer: 0,
                    capabilities: TcpCapabilities::default(),
                    timestamp: None,
                    fast_open_cookie: None,
                }
                .write_to_buffer(tcp, None)
                .expect("write tcp");
            }
            {
                let ip = buffer.prepend_mut(20).expect("ip header");
                write_ipv4_push_header(ip, REMOTE_IP, LOCAL_IP, 6, 42).expect("write ip");
            }
            let packet_len = buffer.current().len();
            let network = unsafe { transmute::<_, &mut NetworkOpaque>(buffer.opaque_mut()) };
            network.set_packet_cursor(
                BufferPacketCursor::new()
                    .with_packet_len(packet_len)
                    .with_network_header(0, 20)
                    .with_transport_header(20, 20)
                    .with_transport_payload_offset(40),
            );
            network.ip_mut().set_ip_version(Some(4));
            network.ip_mut().set_ip_protocol(Some(6));
            write_session_route_opaque(
                buffer.opaque2_mut(),
                session_id,
                DataWorkerId::new(0),
                TcpInputNext::Established,
            );
        }
        let mut frame = runtime
            .buffers()
            .get_next_frame(established)
            .expect("frame");
        frame.push_index(index).expect("push");
        runtime.put_next_frame(frame).expect("put");
        assert!(runtime.run_ready_nodes().expect("run established") >= 1);

        let mut out = [SessionEvt::io(0, SessionEvtType::Close)];
        assert_eq!(app.poll_events(&mut out), 1);
        assert_eq!(out[0].evt_type, SessionEvtType::RxEnq);
        assert!(out[0].flags().is_empty());
        assert!(!out[0].flags().contains(SessionEvtFlags::URGENT));
    }
}

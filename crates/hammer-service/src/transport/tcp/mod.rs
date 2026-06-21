use std::cell::RefMut;
use std::marker::PhantomData;

pub use hammer_core::protocol::tcp::TcpState;

use hammer_adapter::{DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, NodeRuntimeData};
use hammer_core::error::CoreResult;

use crate::session::{
    SessionId, SessionQueueHandle, SessionQueueNext,
    node::SessionQueueDispatchFn,
    runtime::{SessionDriverRuntime, dispatch_session_queue_once_at},
};
use crate::transport::congestion::CongestionController;

pub mod congestion;
pub mod connection;
pub mod established;
pub mod input;
pub mod listen;
pub mod lookup;
pub mod output;
pub mod rcv_process;
pub mod recovery;
pub mod reply;
pub mod reset;
pub mod segment;
pub mod state;
pub mod syn_sent;

pub use connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TCP_MAX_RETRANSMIT_TIMEOUT, TCP_MIN_RETRANSMIT_TIMEOUT,
    TcpConnection, TcpConnectionOptionState, TcpConnectionTimerKind, TcpRetransmitTimeoutState,
};
pub use established::{TcpEstablishedNext, TcpEstablishedNode};
pub use input::{TcpInputControlPlane, TcpInputNode, TcpInputTrace};
pub use listen::{TcpListenNext, TcpListenNode};
pub use output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TCP_FLAG_SYN,
    TcpOutputNext, TcpOutputNode,
};
pub use rcv_process::{TcpRcvProcessNext, TcpRcvProcessNode};
pub use recovery::{TcpRecoveryAck, TcpRecoveryState};
pub use reply::{
    TcpControlFlags, queue_tcp_control_packet, synthesize_ipv4_tcp_control, tcp_control_metadata,
};
pub use reset::{TcpResetNext, TcpResetNode};
pub use state::TcpInputFlags;
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};

pub(crate) use lookup::TcpWorkerOwnedState;

#[derive(Debug, PartialEq, Eq)]
pub struct TcpQueueHandle<C>
where
    C: CongestionController + 'static,
{
    runtime_data: NodeRuntimeData,
    _controller: PhantomData<fn() -> C>,
}

impl<C> TcpQueueHandle<C>
where
    C: CongestionController + 'static,
{
    #[inline]
    pub(crate) const fn new(runtime_data: NodeRuntimeData) -> Self {
        Self {
            runtime_data,
            _controller: PhantomData,
        }
    }

    #[inline]
    pub(crate) const fn runtime_data(self) -> NodeRuntimeData {
        self.runtime_data
    }

    #[inline]
    pub(crate) fn borrow_mut(
        self,
    ) -> CoreResult<RefMut<'static, SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>>>
    {
        SessionQueueHandle::<SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>>::new(
            self.runtime_data,
        )
        .borrow_mut()
    }
}

impl<C> Copy for TcpQueueHandle<C> where C: CongestionController + 'static {}

impl<C> Clone for TcpQueueHandle<C>
where
    C: CongestionController + 'static,
{
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

pub(crate) fn tcp_session_queue_dispatch_fn<C>() -> SessionQueueDispatchFn
where
    C: CongestionController + 'static,
{
    tcp_session_queue_dispatch::<C>
}

fn tcp_session_queue_dispatch<C>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: std::time::Instant,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let mut driver = SessionQueueHandle::<
        SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
    >::new(data)
    .borrow_mut()?;
    dispatch_session_queue_once_at(runtime, &mut driver, now, output_next, output)?;
    Ok(())
}

impl<C> SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>
where
    C: CongestionController + 'static,
{
    pub(crate) fn connect(
        &mut self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> CoreResult<SessionId> {
        let owner = self.worker();
        let initial_sequence = self.aux_mut().next_initial_sequence(local, remote);
        let session_id = self.insert_session_with_id(|session_id: SessionId| {
            let connection_id = hammer_core::protocol::tcp::TcpConnectionId::new(session_id.get());
            let mut connection =
                TcpConnection::new(Some(connection_id), owner, local.port(), Some(local), remote);
            connection.connect_state(initial_sequence);
            connection
        });
        self.aux_mut().remember_pending_open(
            session_id,
            Some(local),
            remote,
            owner,
            TcpInputNext::SynSent,
        );
        if let Some(connection) = self.session_mut(session_id) {
            connection.tcp_timer_set(TcpConnectionTimerKind::RETRANSMIT);
        }
        self.mark_ready(session_id);
        Ok(session_id)
    }

    pub(crate) fn refresh_session_route(&mut self, session_id: SessionId) -> CoreResult<()> {
        let state = self
            .session(session_id)
            .ok_or_else(|| hammer_core::error::CoreError::internal("tcp session is missing"))?
            .clone();
        let completed_active_open = state
            .local()
            .and_then(|local| self.pending_route_by_tuple(local, state.remote()))
            .is_some_and(|(pending_session_id, _, _)| pending_session_id == session_id)
            && state.state() == TcpState::Established;
        self.aux_mut().forget_session(session_id);
        self.aux_mut().forget_pending_open(session_id);
        match state.state() {
            TcpState::Closed => {
                let _ = self.close_session(session_id)?;
                return Ok(());
            }
            TcpState::SynSent => self.aux_mut().remember_pending_open(
                session_id,
                state.local(),
                state.remote(),
                state.owner_worker(),
                state.next_node(),
            ),
            _ => self.aux_mut().remember_session(
                session_id,
                state.connection_id(),
                state.local(),
                state.remote(),
                state.owner_worker(),
                state.next_node(),
            ),
        }
        if completed_active_open
            && let Some(op) = self.session_app_op(session_id)
        {
            self.app().complete_connected(op)?;
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn session_route_by_tuple(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.aux().session_route_by_tuple(local, remote)
    }

    #[inline]
    pub(crate) fn pending_route_by_tuple(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.aux().pending_route_by_tuple(local, remote)
    }
}

pub(crate) fn register_tcp_session_queue<C>(
    worker: DataWorkerId,
    buffers: DataPlaneBuffers,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    let handle = crate::session::node::register_session_queue(
        SessionDriverRuntime::<TcpConnection<C>, TcpWorkerOwnedState>::new(
            worker,
            buffers,
            TcpWorkerOwnedState::new(worker),
        ),
    )?;
    Ok(TcpQueueHandle::new(handle.runtime_data()))
}

pub(crate) fn register_tcp_session_queue_with_connection_for_test<C>(
    worker: DataWorkerId,
    buffers: DataPlaneBuffers,
    connection: TcpConnection<C>,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    let mut driver = SessionDriverRuntime::new(worker, buffers, TcpWorkerOwnedState::new(worker));
    let session_id = driver.insert_session_with_id(|_| connection.clone());
    driver.refresh_session_route(session_id)?;
    let handle = crate::session::node::register_session_queue(driver)?;
    Ok(TcpQueueHandle::new(handle.runtime_data()))
}

#[cfg(test)]
pub(crate) fn register_session_queue_for_test<C>(
    queue: SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
) -> CoreResult<TcpQueueHandle<C>>
where
    C: CongestionController + 'static,
{
    let handle = crate::session::node::register_session_queue(queue)?;
    Ok(TcpQueueHandle::new(handle.runtime_data()))
}

#[hammer_component_macros::node_next]
pub enum TcpInputNext {
    Drop,
    Punt,
    Listen,
    RcvProcess,
    SynSent,
    Established,
    Reset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpInputError {
    BadLength,
    WrongProtocol,
    AckInvalid,
    ConnectionClosed,
}

impl TcpInputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Instant;

    use hammer_adapter::{
        BufferFrame, DataPlaneRuntime, DataWorkerId, InternalNode, Node, NodeId, NodeProcessFn,
        NodeRegistration, NodeResult, NodeRuntimeData,
    };
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::protocol::tcp::{
        TcpConnectionId, TcpSackBlock, TcpSegmentFlags, TcpSegmentView,
    };
    use hammer_runtime::app::{AppRingHandle, AppSendData};

    use super::*;
    use crate::session::SessionId;
    use crate::session::runtime::{
        SessionDriverRuntime, dispatch_session_queue_for_ticks, dispatch_session_queue_pending,
    };
    use crate::transport::congestion::BbrController;

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
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> CoreResult<NodeResult> {
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

    impl InternalNode for CaptureNode {
        fn node_registration(&self) -> NodeRegistration
        where
            Self: Sized,
        {
            NodeRegistration::Plain
        }
    }

    fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let slot = data.usize_word(0)?;
        let state = {
            let states = capture_states().lock().expect("capture registry");
            Arc::clone(
                states
                    .get(slot)
                    .ok_or_else(|| CoreError::internal("capture slot is invalid"))?,
            )
        };
        let mut state = state.lock().expect("capture state");
        for index in frame.drain_pending() {
            let packet = runtime.copy_current_chain(index)?;
            state.packets.push(packet.to_vec());
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    fn tcp_output_graph(
        runtime: &DataPlaneRuntime,
    ) -> (NodeId, Arc<Mutex<CaptureState>>, Arc<Mutex<CaptureState>>) {
        let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
        let drop_state = Arc::new(Mutex::new(CaptureState::default()));
        let lookup = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
        let drop = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
        let output = runtime
            .nodes()
            .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, lookup)));
        (output, lookup_state, drop_state)
    }

    fn established_tcp_connection() -> TcpConnection<BbrController> {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        TcpConnection::established_with_sack_for_test(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    fn enqueue_app_send(
        driver: &mut SessionDriverRuntime<TcpConnection<BbrController>, TcpWorkerOwnedState>,
        ring: &AppRingHandle,
        session_id: SessionId,
        bytes: &[u8],
    ) {
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(bytes).expect("data"))
            .try_into()
            .expect("transfer");
        driver.app_mut().push_pending_send(session_id, send);
        driver.mark_ready(session_id);
    }

    fn dispatch_tcp_session_queue(
        runtime: &DataPlaneRuntime,
        driver: &mut SessionDriverRuntime<TcpConnection<BbrController>, TcpWorkerOwnedState>,
        output_node: NodeId,
    ) {
        let next = crate::session::SessionQueueNext::from_node(output_node);
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");
        dispatch_session_queue_pending(
            runtime,
            driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch tcp session queue");
        output.schedule(runtime).expect("schedule output");
        assert_eq!(runtime.run_ready_nodes().expect("run tcp output"), 2);
    }

    fn expire_tcp_timers(
        runtime: &DataPlaneRuntime,
        driver: &mut SessionDriverRuntime<TcpConnection<BbrController>, TcpWorkerOwnedState>,
        ticks: u32,
        output_node: NodeId,
    ) {
        let next = crate::session::SessionQueueNext::from_node(output_node);
        let step = dispatch_session_queue_for_ticks(runtime, driver, ticks, next)
            .expect("dispatch session queue for timer ticks");
        assert!(step.expired_timers != 0);
        assert_eq!(runtime.run_ready_nodes().expect("run timer output"), 2);
    }

    fn tcp_segment_payload(packet: &[u8]) -> &[u8] {
        let segment = TcpSegmentView::parse(packet).expect("tcp segment");
        &packet[segment.header_len()..]
    }

    #[test]
    fn session_tcp_tlp_retransmits_latest_retained_payload() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 32, 8, 8);
        let (output_node, lookup_state, drop_state) = tcp_output_graph(&runtime);
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            runtime.packet_buffers().clone(),
            TcpWorkerOwnedState::new(DataWorkerId::new(0)),
        );
        let session_id = driver.insert_session(established_tcp_connection());
        driver
            .refresh_session_route(session_id)
            .expect("refresh session route");
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");

        enqueue_app_send(&mut driver, &ring, session_id, b"first");
        dispatch_tcp_session_queue(&runtime, &mut driver, output_node);
        lookup_state.lock().expect("lookup").packets.clear();

        enqueue_app_send(&mut driver, &ring, session_id, b"second");
        dispatch_tcp_session_queue(&runtime, &mut driver, output_node);
        lookup_state.lock().expect("lookup").packets.clear();
        drop_state.lock().expect("drop").packets.clear();

        expire_tcp_timers(&runtime, &mut driver, 20, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        let segment = TcpSegmentView::parse(packet).expect("tcp segment");
        assert_eq!(
            segment.flags() & (TcpSegmentFlags::ACK | TcpSegmentFlags::PSH),
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH
        );
        assert_eq!(tcp_segment_payload(packet), b"second");
    }

    #[test]
    fn session_tcp_rack_retransmits_sacked_gap_payload() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 32, 8, 8);
        let (output_node, lookup_state, drop_state) = tcp_output_graph(&runtime);
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            runtime.packet_buffers().clone(),
            TcpWorkerOwnedState::new(DataWorkerId::new(0)),
        );
        let session_id = driver.insert_session(established_tcp_connection());
        driver
            .refresh_session_route(session_id)
            .expect("refresh session route");
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");

        enqueue_app_send(&mut driver, &ring, session_id, b"first");
        dispatch_tcp_session_queue(&runtime, &mut driver, output_node);
        let second_left_edge = driver.session(session_id).expect("connection").snd_nxt();
        lookup_state.lock().expect("lookup").packets.clear();

        enqueue_app_send(&mut driver, &ring, session_id, b"second");
        dispatch_tcp_session_queue(&runtime, &mut driver, output_node);
        let connection = driver.session(session_id).expect("connection");
        let acknowledgment = connection.snd_una();
        let second_right_edge = connection.snd_nxt();
        lookup_state.lock().expect("lookup").packets.clear();
        drop_state.lock().expect("drop").packets.clear();

        let timers = driver
            .session_mut(session_id)
            .expect("connection")
            .receive_ack(
                acknowledgment,
                u16::MAX,
                &[TcpSackBlock {
                    left_edge: second_left_edge,
                    right_edge: second_right_edge,
                }],
            );
        if timers.contains(TcpConnectionTimerKind::RACK) {
            driver
                .arm_timer_ticks(
                    session_id,
                    TcpConnectionTimerKind::RACK
                        .session_timer_token()
                        .expect("rack timer token"),
                    6,
                )
                .expect("arm rack timer");
        }

        expire_tcp_timers(&runtime, &mut driver, 6, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        let segment = TcpSegmentView::parse(packet).expect("tcp segment");
        assert_eq!(
            segment.flags() & (TcpSegmentFlags::ACK | TcpSegmentFlags::PSH),
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH
        );
        assert_eq!(tcp_segment_payload(packet), b"first");
    }
}

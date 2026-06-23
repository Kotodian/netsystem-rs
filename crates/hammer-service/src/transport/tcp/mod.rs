use std::cell::RefMut;
use std::marker::PhantomData;

pub use hammer_core::protocol::tcp::{TcpSeq, TcpState};

use hammer_adapter::{DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, NodeRuntimeData};
use hammer_core::error::{CoreError, CoreResult};

use crate::session::{
    SessionId, SessionQueueHandle, SessionQueueNext,
    node::SessionQueueDispatchFn,
    node::SessionQueueOutput,
    protocol::SessionQueueControlContext,
    runtime::{SessionDriverRuntime, SessionQueueProtocol, dispatch_session_queue_once_at},
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
mod sack;
pub mod segment;
pub mod state;
pub mod syn_sent;

pub use connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TCP_MAX_RETRANSMIT_TIMEOUT, TCP_MIN_RETRANSMIT_TIMEOUT,
    TCP_TIMER_DELAYED_ACK, TCP_TIMER_PERSIST, TCP_TIMER_RACK, TCP_TIMER_RETRANSMIT,
    TCP_TIMER_TIME_WAIT, TCP_TIMER_TLP, TcpConnection, TcpConnectionOptionState,
    TcpRetransmitTimeoutState,
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
pub use reply::{TcpControlFlags, synthesize_ipv4_tcp_control, tcp_control_cursor};
pub use reset::{TcpResetNext, TcpResetNode};
use segment::TcpSegment;
pub use state::TcpInputFlags;
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};

pub(crate) use lookup::TcpWorkerOwnedState;

fn enqueue_tcp_segment(
    runtime: &DataPlaneRuntime,
    output_next: SessionQueueNext,
    output: &mut SessionQueueOutput,
    segment: TcpSegment,
) -> CoreResult<()> {
    let index = runtime.packet_buffers().alloc_index()?;
    if let Err(error) = segment.write_to_buffer(runtime.packet_buffers(), index) {
        runtime.free_index(index);
        return Err(error);
    }
    if let Err(error) = output.enqueue(runtime, output_next.node(), index) {
        runtime.free_index(index);
        return Err(error);
    }
    Ok(())
}

fn refresh_tcp_timer<C>(
    connection: &TcpConnection<C>,
    context: &mut SessionQueueControlContext,
    timer_id: u32,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let session = context.session_id().pool_index();
    let timers = context.timer_wheel();
    if connection.timer_is_active(timer_id) {
        let Some(ticks) = connection.timer_ticks(timer_id) else {
            let _ = timers.cancel_timer(session.slot(), session.generation(), timer_id);
            return Ok(());
        };
        timers
            .update_timer(session.slot(), session.generation(), timer_id, ticks)
            .map_err(|_| CoreError::internal("tcp timer update failed"))?;
    } else {
        let _ = timers.cancel_timer(session.slot(), session.generation(), timer_id);
    }
    Ok(())
}

fn refresh_tcp_timers<C>(
    connection: &TcpConnection<C>,
    context: &mut SessionQueueControlContext,
    timer_mask: u16,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    for timer_id in 0..crate::transport::tcp::connection::TCP_TIMER_COUNT {
        if (timer_mask & (1u16 << timer_id)) != 0 || connection.timer_is_active(timer_id) {
            refresh_tcp_timer(connection, context, timer_id)?;
        }
    }
    Ok(())
}

fn refresh_tcp_timers_for_session<C>(
    driver: &mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
    session_id: SessionId,
    timer_mask: u16,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let connection = driver
        .session(session_id)
        .ok_or_else(|| CoreError::internal("tcp session is missing"))?
        as *const TcpConnection<C>;
    let timers = driver.timers_mut() as *mut _;
    let ready = driver.ready_mut_ptr();
    let buffers = driver.buffers() as *const _;
    let mut context = SessionQueueControlContext::new(
        timers,
        ready,
        buffers,
        session_id,
        driver.app().pending_send_head(session_id).is_some(),
    );
    // SAFETY: `connection` is an immutable pointer captured before building the
    // control context. `refresh_tcp_timers` only reads the connection while the
    // context mutates timer-wheel state and does not alias the session entry.
    refresh_tcp_timers(unsafe { &*connection }, &mut context, timer_mask)
}

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

pub struct TcpCloseHarness<C: CongestionController + 'static> {
    driver: SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>,
}

impl<C> TcpCloseHarness<C>
where
    C: CongestionController + 'static,
{
    pub fn session(&self, session_id: SessionId) -> Option<&TcpConnection<C>> {
        self.driver.session(session_id)
    }

    pub fn session_route_by_tuple(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
        self.driver.session_route_by_tuple(local, remote)
    }

    pub fn drive_fin_ack_to_time_wait(&mut self, session_id: SessionId) -> CoreResult<()> {
        let (local, remote, ack) = {
            let connection = self
                .driver
                .session(session_id)
                .ok_or_else(|| hammer_core::error::CoreError::internal("tcp session is missing"))?;
            (
                connection.local().expect("local"),
                connection.remote(),
                connection.snd_nxt(),
            )
        };
        let packet = crate::transport::tcp::segment::TcpPacket {
            local: remote,
            remote: local,
            sequence: 7_000.into(),
            acknowledgment: Some(ack.into()),
            advertised_window: u16::MAX,
            flags: hammer_core::protocol::tcp::TcpSegmentFlags::FIN
                | hammer_core::protocol::tcp::TcpSegmentFlags::ACK,
            capabilities: hammer_core::protocol::tcp::TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let queue_ptr: *mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState> =
            &mut self.driver;
        unsafe {
            let connection = (*queue_ptr)
                .session_mut(session_id)
                .ok_or_else(|| hammer_core::error::CoreError::internal("tcp session is missing"))?;
            connection.on_session_close();
            let _ = connection.receive_close_side(&packet)?;
        }
        self.driver.refresh_session_route(session_id)?;
        Ok(())
    }

    pub fn receive_duplicate_fin(&mut self, session_id: SessionId) -> CoreResult<Option<TcpSegment>> {
        let (local, remote, sequence) = {
            let connection = self
                .driver
                .session(session_id)
                .ok_or_else(|| hammer_core::error::CoreError::internal("tcp session is missing"))?;
            (
                connection.local().expect("local"),
                connection.remote(),
                connection.rcv_nxt().wrapping_sub(1),
            )
        };
        let packet = crate::transport::tcp::segment::TcpPacket {
            local: remote,
            remote: local,
            sequence: sequence.into(),
            acknowledgment: Some(0.into()),
            advertised_window: u16::MAX,
            flags: hammer_core::protocol::tcp::TcpSegmentFlags::FIN
                | hammer_core::protocol::tcp::TcpSegmentFlags::ACK,
            capabilities: hammer_core::protocol::tcp::TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let queue_ptr: *mut SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState> =
            &mut self.driver;
        unsafe {
            let connection = (*queue_ptr)
                .session_mut(session_id)
                .ok_or_else(|| hammer_core::error::CoreError::internal("tcp session is missing"))?;
            connection.receive_close_side(&packet)
        }
    }

    pub fn expire_time_wait(&mut self, session_id: SessionId) -> CoreResult<()> {
        let connection = self
            .driver
            .session_mut(session_id)
            .ok_or_else(|| hammer_core::error::CoreError::internal("tcp session is missing"))?;
        connection.timer_set(TCP_TIMER_TIME_WAIT);
        connection.timer_expire(TCP_TIMER_TIME_WAIT);
        let _ = connection.on_tcp_timer_expiry(TCP_TIMER_TIME_WAIT);
        self.driver.refresh_session_route(session_id)?;
        Ok(())
    }
}

pub fn closing_session_for_test<C>() -> (
    TcpCloseHarness<C>,
    SessionId,
    std::net::SocketAddr,
    std::net::SocketAddr,
)
where
    C: CongestionController + 'static,
{
    let local: std::net::SocketAddr = "192.0.2.10:443".parse().expect("local");
    let remote: std::net::SocketAddr = "198.51.100.20:50001".parse().expect("remote");
    let mut driver = SessionDriverRuntime::new(
        DataWorkerId::new(0),
        hammer_adapter::DataPlaneBuffers::with_capacities(2048, 4, 4, 4),
        TcpWorkerOwnedState::new(DataWorkerId::new(0)),
    );
    let session_id = driver.insert_session_with_id(|session_id: SessionId| {
        TcpConnection::established_for_time_wait_test(
            Some(hammer_core::protocol::tcp::TcpConnectionId::new(
                session_id.get(),
            )),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    });
    driver
        .refresh_session_route(session_id)
        .expect("refresh session route");
    (TcpCloseHarness { driver }, session_id, local, remote)
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

impl<C> SessionQueueProtocol<TcpWorkerOwnedState> for TcpConnection<C>
where
    C: CongestionController + 'static,
{
    fn tx_offset(&self, _: &SessionQueueControlContext) -> CoreResult<usize> {
        let start = if self.state() == TcpState::SynSent {
            self.iss()
        } else {
            self.snd_una()
        };
        usize::try_from(TcpSeq::from(start).distance_to(self.tx_payload_sequence()))
            .map_err(|_| CoreError::internal("tcp tx offset exceeds usize"))
    }

    fn handle_expired_timer(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut SessionQueueControlContext,
        timer_id: u32,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<bool> {
        self.timer_expire(timer_id);
        let control = self.on_tcp_timer_expiry(timer_id);
        refresh_tcp_timer(self, context, timer_id)?;
        if let Some(segment) = control {
            if segment.payload_len() == 0 {
                enqueue_tcp_segment(runtime, output_next, output, segment)?;
            } else {
                context.mark_ready();
            }
        }
        Ok(self.state() == TcpState::Closed)
    }

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut SessionQueueControlContext,
        close_requested: bool,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<bool> {
        if close_requested {
            self.on_session_close();
        }
        if let Some(segment) = self.on_tcp_ready(context.has_pending_tx()) {
            enqueue_tcp_segment(runtime, output_next, output, segment)?;
        }
        refresh_tcp_timers(self, context, u16::MAX)?;
        Ok(self.state() == TcpState::Closed)
    }

    fn tx_payload_len(
        &mut self,
        _: &mut SessionQueueControlContext,
        _: usize,
        pending_len: usize,
        now: std::time::Instant,
    ) -> CoreResult<usize> {
        Ok(self.tx_payload_budget(pending_len, now))
    }

    fn prepare_tx(
        &mut self,
        context: &mut SessionQueueControlContext,
        index: hammer_adapter::BufferIndex,
        _: usize,
        payload_len: usize,
        _: std::time::Instant,
    ) -> CoreResult<()> {
        let segment = self.tx_segment(payload_len)?;
        segment.write_to_buffer(context.buffers(), index)
    }

    fn cancel_tx(&mut self, _: &mut TcpWorkerOwnedState, _: hammer_adapter::BufferIndex) {}

    fn commit_tx(
        &mut self,
        context: &mut SessionQueueControlContext,
        _: hammer_adapter::BufferIndex,
        _: usize,
        payload_len: usize,
        now: std::time::Instant,
    ) -> CoreResult<()> {
        let timer_mask = self.commit_payload_tx(payload_len, now)?;
        refresh_tcp_timers(self, context, timer_mask | (1u16 << TCP_TIMER_RETRANSMIT))?;
        Ok(())
    }
}

impl<C> SessionDriverRuntime<TcpConnection<C>, TcpWorkerOwnedState>
where
    C: CongestionController + 'static,
{
    fn close_tcp_session(&mut self, session_id: SessionId) -> CoreResult<Option<TcpConnection<C>>> {
        self.aux_mut().forget_session(session_id);
        self.aux_mut().forget_pending_open(session_id);
        self.close_session(session_id)
    }

    pub(crate) fn connect(
        &mut self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> CoreResult<SessionId> {
        let owner = self.worker();
        let initial_sequence = self.aux_mut().next_initial_sequence(local, remote);
        let cached_fast_open =
            self.aux()
                .fast_open_cookie(local, remote)
                .map(|(cookie, max_segment_size)| {
                    let mut copied = [0u8; 16];
                    let len = cookie.len().min(copied.len());
                    copied[..len].copy_from_slice(&cookie[..len]);
                    (copied, len as u8, max_segment_size)
                });
        let session_id = self.insert_session_with_id(|session_id: SessionId| {
            let connection_id = hammer_core::protocol::tcp::TcpConnectionId::new(session_id.get());
            let mut connection = TcpConnection::new(
                Some(connection_id),
                owner,
                local.port(),
                Some(local),
                remote,
            );
            if let Some((cookie, len, max_segment_size)) = cached_fast_open {
                let mut capabilities = connection.local_capabilities();
                capabilities.fast_open = true;
                if max_segment_size.is_some() {
                    capabilities.max_segment_size = max_segment_size;
                }
                let _ = connection.set_local_capabilities(capabilities);
                connection.set_fast_open_cookie(Some(&cookie[..usize::from(len)]));
            }
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
            connection.timer_set(TCP_TIMER_RETRANSMIT);
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
                let _ = self.close_tcp_session(session_id)?;
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
        if completed_active_open && let Some(op) = self.session_app_op(session_id) {
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
    let handle = crate::session::node::register_session_queue(SessionDriverRuntime::<
        TcpConnection<C>,
        TcpWorkerOwnedState,
    >::new(
        worker,
        buffers,
        TcpWorkerOwnedState::new(worker),
    ))?;
    Ok(TcpQueueHandle::new(handle.runtime_data()))
}

#[cfg(test)]
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
        TcpCapabilities, TcpConnectionId, TcpSackBlock, TcpSegmentFlags, TcpSeq,
    };
    use hammer_runtime::app::{AppOpId, AppRingHandle, AppSendData, AppSqe};

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
    ) -> usize {
        let next: crate::session::SessionQueueNext = output_node.into();
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
        runtime.run_ready_nodes().expect("run tcp output")
    }

    fn expire_tcp_timers(
        runtime: &DataPlaneRuntime,
        driver: &mut SessionDriverRuntime<TcpConnection<BbrController>, TcpWorkerOwnedState>,
        ticks: u32,
        output_node: NodeId,
    ) {
        let next: crate::session::SessionQueueNext = output_node.into();
        let _ = dispatch_session_queue_for_ticks(runtime, driver, ticks, next)
            .expect("dispatch session queue for timer ticks");
        let _ = dispatch_session_queue_for_ticks(runtime, driver, 0, next)
            .expect("dispatch session queue follow-up");
        let _ = runtime.run_ready_nodes().expect("run timer output");
    }

    fn tcp_segment_payload(packet: &[u8]) -> &[u8] {
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        &packet[segment.header_len()..]
    }

    fn tcp_ack_number(packet: &[u8]) -> u32 {
        etherparse::TcpSlice::from_slice(packet)
            .expect("tcp segment")
            .acknowledgment_number()
    }

    #[test]
    fn session_tcp_tlp_retransmits_latest_session_tx_payload() {
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
        assert_eq!(
            dispatch_tcp_session_queue(&runtime, &mut driver, output_node),
            2
        );
        lookup_state.lock().expect("lookup").packets.clear();

        enqueue_app_send(&mut driver, &ring, session_id, b"second");
        assert_eq!(
            dispatch_tcp_session_queue(&runtime, &mut driver, output_node),
            2
        );
        lookup_state.lock().expect("lookup").packets.clear();
        drop_state.lock().expect("drop").packets.clear();

        expire_tcp_timers(&runtime, &mut driver, 20, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        assert_eq!(
            tcp_flags(&segment) & (TcpSegmentFlags::ACK | TcpSegmentFlags::PSH),
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH
        );
        assert_eq!(tcp_segment_payload(packet), b"second");
    }

    #[test]
    fn session_tcp_delayed_ack_timer_emits_ack_after_first_clean_payload() {
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

        let (local, remote, sequence, acknowledgment) = {
            let connection = driver.session(session_id).expect("connection");
            (
                connection.local().expect("local"),
                connection.remote(),
                connection.rcv_nxt(),
                connection.snd_nxt(),
            )
        };
        let payload = runtime
            .packet_buffers()
            .alloc_index()
            .expect("payload");
        runtime
            .packet_buffers()
            .append(payload, b"hello")
            .expect("payload bytes");
        let packet = crate::transport::tcp::segment::TcpPacket {
            local: remote,
            remote: local,
            sequence: sequence.into(),
            acknowledgment: Some(acknowledgment.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 5,
        };
        let control = {
            let connection = driver.session_mut(session_id).expect("connection");
            let (control, _) = connection.receive_established(&packet).expect("receive data");
            assert!(connection.accept_payload(&packet).is_some());
            assert!(!connection.on_clean_in_order_payload());
            control
        };
        {
            let mut buffer = runtime
                .packet_buffers()
                .get_buffer_mut(payload)
                .expect("payload buffer");
            buffer
                .advance(packet.payload_offset)
                .expect("advance payload");
            buffer
                .truncate_chain(packet.payload_len)
                .expect("truncate payload");
        }
        let enqueue = driver
            .enqueue_rx(session_id, payload, 0, false)
            .expect("enqueue rx");
        {
            let connection = driver.session_mut(session_id).expect("connection");
            connection.receive_payload(
                packet.sequence,
                0,
                enqueue.delivered_len,
                enqueue.newest_ooo_start,
                enqueue.newest_ooo_len,
            );
        }
        if enqueue.delivered_len != 0 {
            driver.mark_ready(session_id);
        }
        refresh_tcp_timers_for_session(
            &mut driver,
            session_id,
            1u16 << TCP_TIMER_DELAYED_ACK,
        )
        .expect("refresh delayed ack timer");

        assert!(control.is_none());
        assert!(drop_state.lock().expect("drop").packets.is_empty());
        assert!(lookup_state.lock().expect("lookup").packets.is_empty());

        expire_tcp_timers(&runtime, &mut driver, 1, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        assert_eq!(tcp_flags(&segment), TcpSegmentFlags::ACK);
        assert_eq!(tcp_segment_payload(packet), b"");
        assert_eq!(tcp_ack_number(packet), sequence.wrapping_add(5));
    }

    #[test]
    fn session_tcp_persist_timer_emits_one_byte_probe_from_session_tx() {
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

        enqueue_app_send(&mut driver, &ring, session_id, b"hello");
        assert_eq!(
            dispatch_tcp_session_queue(&runtime, &mut driver, output_node),
            2
        );
        lookup_state.lock().expect("lookup").packets.clear();
        drop_state.lock().expect("drop").packets.clear();

        let (local, remote, sequence, acknowledgment) = {
            let connection = driver.session(session_id).expect("connection");
            (
                connection.local().expect("local"),
                connection.remote(),
                connection.rcv_nxt(),
                connection.snd_una(),
            )
        };
        {
            let connection = driver.session_mut(session_id).expect("connection");
            let packet = crate::transport::tcp::segment::TcpPacket {
                local: remote,
                remote: local,
                sequence: sequence.into(),
                acknowledgment: Some(acknowledgment.into()),
                advertised_window: 0,
                flags: TcpSegmentFlags::ACK,
                capabilities: TcpCapabilities::default(),
                sack_blocks: hammer_infra::vec::Vec::new(),
                timestamp: None,
                fast_open_cookie: None,
                ip_ecn: None,
                payload_offset: 0,
                payload_len: 0,
            };
            let _ = connection.receive_established(&packet).expect("receive zero-window ack");
        }
        driver.mark_ready(session_id);
        assert_eq!(
            dispatch_tcp_session_queue(&runtime, &mut driver, output_node),
            0
        );
        let connection = driver.session(session_id).expect("connection");
        assert_eq!(connection.snd_wnd(), 0);
        assert!(connection.timer_is_active(TCP_TIMER_PERSIST));

        expire_tcp_timers(&runtime, &mut driver, 5, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        assert_eq!(
            tcp_flags(&segment) & (TcpSegmentFlags::ACK | TcpSegmentFlags::PSH),
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH
        );
        assert_eq!(tcp_segment_payload(packet), b"h");
        assert!(driver.has_session_tx(session_id));
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
        assert_eq!(
            dispatch_tcp_session_queue(&runtime, &mut driver, output_node),
            2
        );
        let second_left_edge = driver.session(session_id).expect("connection").snd_nxt();
        lookup_state.lock().expect("lookup").packets.clear();

        enqueue_app_send(&mut driver, &ring, session_id, b"second");
        assert_eq!(
            dispatch_tcp_session_queue(&runtime, &mut driver, output_node),
            2
        );
        let (local, remote, acknowledgment, second_right_edge, rcv_nxt) = {
            let connection = driver.session(session_id).expect("connection");
            (
                connection.local().expect("local"),
                connection.remote(),
                connection.snd_una(),
                connection.snd_nxt(),
                connection.rcv_nxt(),
            )
        };
        lookup_state.lock().expect("lookup").packets.clear();
        drop_state.lock().expect("drop").packets.clear();

        let timers = driver
            .session_mut(session_id)
            .expect("connection")
            .receive_ack(
                &crate::transport::tcp::segment::TcpPacket {
                    local: remote,
                    remote: local,
                    sequence: rcv_nxt.into(),
                    acknowledgment: Some(acknowledgment.into()),
                    advertised_window: u16::MAX,
                    flags: TcpSegmentFlags::ACK,
                    capabilities: TcpCapabilities::default(),
                    sack_blocks: hammer_infra::vec::Vec::new(),
                    timestamp: None,
                    fast_open_cookie: None,
                    ip_ecn: None,
                    payload_offset: 0,
                    payload_len: 0,
                },
                acknowledgment,
                u16::MAX,
                &[TcpSackBlock {
                    left_edge: TcpSeq::from(second_left_edge),
                    right_edge: TcpSeq::from(second_right_edge),
                }],
            );
        if (timers & (1u16 << TCP_TIMER_RACK)) != 0 {
            let session = session_id.pool_index();
            driver
                .timers_mut()
                .arm_timer(session.slot(), session.generation(), TCP_TIMER_RACK, 6)
                .expect("arm rack timer");
        }

        expire_tcp_timers(&runtime, &mut driver, 6, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        assert_eq!(
            tcp_flags(&segment) & (TcpSegmentFlags::ACK | TcpSegmentFlags::PSH),
            TcpSegmentFlags::ACK | TcpSegmentFlags::PSH
        );
        assert_eq!(tcp_segment_payload(packet), b"first");
    }

    #[test]
    fn session_tcp_out_of_order_payload_is_retained_and_ack_advertises_sack() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 32, 8, 8);
        let (_, _, drop_state) = tcp_output_graph(&runtime);
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
        let op = AppOpId::new(7);
        assert!(driver.bind_session_app_ring(session_id, op, ring.clone()));

        let (local, remote, acknowledgment, sequence) = {
            let connection = driver.session(session_id).expect("connection");
            (
                connection.local().expect("local"),
                connection.remote(),
                connection.snd_nxt(),
                connection.rcv_nxt().wrapping_add(2),
            )
        };
        let payload = runtime.packet_buffers().alloc_index().expect("payload");
        runtime
            .packet_buffers()
            .append(payload, b"world")
            .expect("payload bytes");
        let packet = crate::transport::tcp::segment::TcpPacket {
            local: remote,
            remote: local,
            sequence: sequence.into(),
            acknowledgment: Some(acknowledgment.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 5,
        };
        let ack = {
            let (control, trim, offset) = {
                let connection = driver.session_mut(session_id).expect("connection");
                let (control, _) = connection.receive_established(&packet).expect("receive data");
                let (trim, offset) = connection.accept_payload(&packet).expect("accept payload");
                (control, trim, offset)
            };
            {
                let mut buffer = runtime
                    .packet_buffers()
                    .get_buffer_mut(payload)
                    .expect("payload buffer");
                buffer
                    .advance(packet.payload_offset.saturating_add(trim))
                    .expect("advance payload");
                buffer
                    .truncate_chain(packet.payload_len.saturating_sub(trim))
                    .expect("truncate payload");
            }
            let enqueue = driver
                .enqueue_rx(session_id, payload, offset, false)
                .expect("enqueue rx");
            let connection = driver.session_mut(session_id).expect("connection");
            connection.receive_payload(
                packet.sequence,
                trim as u32,
                enqueue.delivered_len,
                enqueue.newest_ooo_start,
                enqueue.newest_ooo_len,
            );
            assert_eq!(enqueue.delivered_len, 0);
            control.unwrap_or_else(|| {
                connection.control_segment(
                    &packet,
                    hammer_core::protocol::tcp::TcpSegmentFlags::ACK,
                    None,
                )
            })
        };

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let mut header = [0u8; 64];
        let header_len = ack.write_header(&mut header).expect("write ack");
        let segment = etherparse::TcpSlice::from_slice(&header[..header_len]).expect("tcp segment");
        assert_eq!(tcp_flags(&segment), TcpSegmentFlags::ACK);
        let options = hammer_core::protocol::tcp::tcp_options_from_bytes(segment.options());
        assert_eq!(
            options.sack_blocks,
            vec![TcpSackBlock {
                left_edge: sequence.into(),
                right_edge: sequence.wrapping_add(5).into(),
            }]
        );

        let gap_closer = runtime.packet_buffers().alloc_index().expect("gap closer");
        runtime
            .packet_buffers()
            .append(gap_closer, b"ab")
            .expect("gap closer bytes");
        let gap_packet = crate::transport::tcp::segment::TcpPacket {
            local: remote,
            remote: local,
            sequence: sequence.wrapping_sub(2).into(),
            acknowledgment: Some(acknowledgment.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 2,
        };
        {
            let (trim, offset) = {
                let connection = driver.session_mut(session_id).expect("connection");
                let _ = connection
                    .receive_established(&gap_packet)
                    .expect("receive gap closer");
                connection.accept_payload(&gap_packet).expect("accept gap closer")
            };
            {
                let mut buffer = runtime
                    .packet_buffers()
                    .get_buffer_mut(gap_closer)
                    .expect("gap closer buffer");
                buffer
                    .advance(gap_packet.payload_offset.saturating_add(trim))
                    .expect("advance gap closer");
                buffer
                    .truncate_chain(gap_packet.payload_len.saturating_sub(trim))
                    .expect("truncate gap closer");
            }
            let enqueue = driver
                .enqueue_rx(session_id, gap_closer, offset, false)
                .expect("enqueue gap closer");
            let connection = driver.session_mut(session_id).expect("connection");
            connection.receive_payload(
                gap_packet.sequence,
                trim as u32,
                enqueue.delivered_len,
                enqueue.newest_ooo_start,
                enqueue.newest_ooo_len,
            );
            if enqueue.delivered_len != 0 {
                driver.mark_ready(session_id);
            }
        }

        ring.push_test_submission(AppSqe::recv(None, op, 64))
            .expect("queue first recv");
        driver.flush_session_rx(session_id).expect("flush first recv");
        ring.push_test_submission(AppSqe::recv(None, op, 64))
            .expect("queue second recv");
        driver.flush_session_rx(session_id).expect("flush second recv");
        let completions = ring.take_test_completions(4);
        assert_eq!(completions.len(), 2);
    }

    #[test]
    fn session_tcp_syn_data_ack_releases_session_tx() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 32, 8, 8);
        let (output_node, lookup_state, drop_state) = tcp_output_graph(&runtime);
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            runtime.packet_buffers().clone(),
            TcpWorkerOwnedState::new(DataWorkerId::new(0)),
        );
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        let session_id = driver.insert_session_with_id(|session_id: SessionId| {
            let mut connection = TcpConnection::new(
                Some(TcpConnectionId::new(session_id.get())),
                DataWorkerId::new(0),
                local.port(),
                Some(local),
                remote,
            );
            let _ = connection.set_local_capabilities(TcpCapabilities {
                max_segment_size: Some(1460),
                window_scale: None,
                sack: true,
                timestamps: false,
                ecn: false,
                accurate_ecn: false,
                fast_open: true,
            });
            connection.set_fast_open_cookie(Some(&[1, 2, 3, 4]));
            connection.connect_state(1000);
            connection
        });
        driver
            .refresh_session_route(session_id)
            .expect("refresh session route");
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");

        enqueue_app_send(&mut driver, &ring, session_id, b"hello");
        dispatch_tcp_session_queue(&runtime, &mut driver, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        assert!(driver.has_session_tx(session_id));
        let connection = driver.session(session_id).expect("connection");
        assert_eq!(connection.state(), TcpState::SynSent);
        let acknowledgment = connection.snd_nxt();
        let packet = &lookup_state.lock().expect("lookup").packets[0];
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        let options = hammer_core::protocol::tcp::tcp_options_from_bytes(segment.options());
        assert_eq!(tcp_segment_payload(packet), b"hello");
        assert_eq!(options.fast_open_cookie.as_deref(), Some(&[1, 2, 3, 4][..]));

        let connection = driver.session_mut(session_id).expect("connection");
        let packet = crate::transport::tcp::segment::TcpPacket {
            local: remote,
            remote: local,
            sequence: 7_000.into(),
            acknowledgment: Some(acknowledgment.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::SYN | TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities {
                max_segment_size: Some(1460),
                window_scale: None,
                sack: true,
                timestamps: false,
                ecn: false,
                accurate_ecn: false,
                fast_open: true,
            },
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let previous_snd_una = connection.snd_una();
        let _ = connection
            .receive_open_reply(&packet)
            .expect("receive syn-ack");
        let acked = connection.take_acked_tx_len(previous_snd_una);
        assert_eq!(acked, 5);
        driver
            .release_tx_up_to(session_id, acked as usize)
            .expect("release tx");

        assert!(!driver.has_session_tx(session_id));
        assert_eq!(
            driver.session(session_id).expect("connection").state(),
            TcpState::Established
        );
    }
}

#[cfg(test)]
fn tcp_flags(segment: &etherparse::TcpSlice<'_>) -> hammer_core::protocol::tcp::TcpSegmentFlags {
    let mut flags = hammer_core::protocol::tcp::TcpSegmentFlags::empty();
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::NS,
        segment.ns(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::FIN,
        segment.fin(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::SYN,
        segment.syn(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::RST,
        segment.rst(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::PSH,
        segment.psh(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::ACK,
        segment.ack(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::URG,
        segment.urg(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::ECE,
        segment.ece(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::CWR,
        segment.cwr(),
    );
    flags
}

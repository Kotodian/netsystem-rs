use std::cell::RefCell;
use std::net::SocketAddr;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{
    TcpConnectionId, TcpSegmentFlags, TcpSegmentHeader, TcpSeq, TcpState,
};
#[cfg(test)]
use hammer_runtime::app::{AppOpId, AppRingHandle};

use super::connection::TcpConnection;
use super::segment::alloc_tcp_segment;
use super::state_machine::Closed;
use super::{
    TcpConnectionState, TcpConnectionTimerKind, TcpPendingIndex, TcpSessionConnectionIndex,
};
use crate::session::node::{
    SessionQueueDispatchFn, SessionQueueHandle, SessionQueueNext, SessionQueueOutput,
    register_session_queue, with_session_queue,
};
use crate::session::runtime::{
    SessionDriverRuntime, SessionEntry, SessionQueueProtocol, SessionStateFactory,
    dispatch_session_queue_once_at,
};
#[cfg(test)]
use crate::session::runtime::{SessionQueueStep, dispatch_session_queue_for_ticks};
use crate::session::{
    SessionAppCloseSubmission, SessionAppSendSubmission, SessionId, SessionProtocolContext,
    SessionTimerExpiry, SessionTimerToken,
};

const TCP_ACTIVE_OPEN_TIMER_TICKS: u64 = 2;

thread_local! {
    static TCP_SESSION_QUEUES: RefCell<hammer_infra::vec::Vec<TcpSessionQueue>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

pub(crate) struct TcpSessionQueue {
    driver: SessionDriverRuntime<TcpConnectionState>,
    protocol: TcpSessionProtocol,
}

impl TcpSessionQueue {
    #[inline]
    pub(crate) fn new(worker: DataWorkerId, buffers: DataPlaneBuffers) -> Self {
        Self {
            driver: SessionDriverRuntime::new(worker, buffers),
            protocol: TcpSessionProtocol::new(worker),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn with_timer_clock(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
    ) -> Self {
        Self {
            driver: SessionDriverRuntime::with_timer_clock(
                worker,
                buffers,
                timer_tick_duration,
                last_timer_tick,
            ),
            protocol: TcpSessionProtocol::new(worker),
        }
    }

    #[inline]
    pub(crate) fn worker(&self) -> DataWorkerId {
        self.protocol.worker()
    }

    #[inline]
    pub(crate) fn insert_session(&mut self, connection: TcpConnectionState) -> SessionId {
        self.driver.insert_session(connection)
    }

    #[inline]
    pub(crate) fn insert_session_with_id<F>(&mut self, f: F) -> SessionId
    where
        F: SessionStateFactory<TcpConnectionState>,
    {
        self.driver.insert_session_with_id(f)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn session(
        &self,
        session_id: SessionId,
    ) -> Option<&SessionEntry<TcpConnectionState>> {
        self.driver.session(session_id)
    }

    #[inline]
    pub(crate) fn session_state(&self, session_id: SessionId) -> Option<&TcpConnectionState> {
        self.driver.session_state(session_id)
    }

    pub(crate) fn take_connection<S>(
        &mut self,
        session_id: SessionId,
    ) -> CoreResult<TcpConnection<S>>
    where
        TcpConnection<S>: TryFrom<TcpConnectionState, Error = CoreError>,
    {
        self.driver
            .session_state(session_id)
            .ok_or_else(|| CoreError::internal("tcp session is missing"))?
            .clone()
            .try_into()
    }

    pub(crate) fn put_connection<C>(&mut self, session_id: SessionId, connection: C)
    where
        C: Into<TcpConnectionState>,
    {
        let replaced = self
            .driver
            .replace_session_state(session_id, connection.into());
        debug_assert!(replaced.is_some());
    }

    #[inline]
    pub(crate) fn close_session(
        &mut self,
        session_id: SessionId,
    ) -> CoreResult<Option<SessionEntry<TcpConnectionState>>> {
        let closed = self.driver.close_session(session_id)?;
        if closed.is_some() {
            self.protocol.remove_session_index(session_id);
        }
        Ok(closed)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn bind_session_app_ring(
        &mut self,
        session_id: SessionId,
        op: AppOpId,
        ring: AppRingHandle,
    ) -> bool {
        self.driver.bind_session_app_ring(session_id, op, ring)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn app_mut(&mut self) -> &mut crate::session::SessionAppRuntime {
        self.driver.app_mut()
    }

    #[inline]
    pub(crate) fn mark_session_ready(&mut self, session_id: SessionId) {
        self.driver.mark_ready(session_id);
    }

    #[inline]
    pub(crate) fn index_session(&mut self, session_id: SessionId, connection: &TcpConnectionState) {
        self.protocol.index_session(session_id, connection);
    }

    #[inline]
    pub(crate) fn index_pending(&mut self, id: SessionId, connection: &TcpConnectionState) {
        self.protocol.index_pending(id, connection);
    }

    #[inline]
    pub(crate) fn remove_pending_index(&mut self, session_id: SessionId) {
        self.protocol.remove_pending_index(session_id);
    }

    #[inline]
    pub(crate) fn session_id_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<SessionId> {
        self.protocol.session_id_by_tuple(local, remote)
    }

    #[inline]
    pub(crate) fn pending_id_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<SessionId> {
        self.protocol.pending_id_by_tuple(local, remote)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn session_id_by_connection_id(
        &self,
        connection_id: TcpConnectionId,
    ) -> Option<SessionId> {
        self.protocol.session_id_by_connection_id(connection_id)
    }

    #[inline]
    pub(crate) fn arm_retransmit_timer(
        &mut self,
        session_id: SessionId,
        ticks: u64,
    ) -> CoreResult<()> {
        let mut context = SessionProtocolContext::new(&mut self.driver);
        TcpSessionProtocol::arm_retransmit_timer(&mut context, session_id, ticks)
    }

    #[inline]
    pub(crate) fn cancel_retransmit_timer(&mut self, session_id: SessionId) -> bool {
        let mut context = SessionProtocolContext::new(&mut self.driver);
        TcpSessionProtocol::cancel_retransmit_timer(&mut context, session_id)
    }

    #[inline]
    pub(crate) fn enqueue_rx(
        &mut self,
        session_id: SessionId,
        index: BufferIndex,
        fin: bool,
    ) -> CoreResult<bool> {
        self.driver.enqueue_rx(session_id, index, fin)
    }

    pub(crate) fn complete_connected(&mut self, session_id: SessionId) -> CoreResult<()> {
        let Some(op) = self
            .driver
            .session(session_id)
            .and_then(|entry| entry.app_op())
        else {
            return Ok(());
        };
        self.driver.app().complete_connected(op)
    }

    pub(crate) fn connect(
        &mut self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> CoreResult<SessionId> {
        let iss = self.protocol.next_initial_sequence(local, remote);
        let closed: TcpConnection<Closed> =
            TcpConnection::new(None, self.worker(), local.port(), Some(local), remote);
        let syn_sent = closed.connect(iss);
        let connection = syn_sent.into();

        let session_id = self.insert_session(connection);
        let indexed = self
            .session_state(session_id)
            .ok_or_else(|| CoreError::internal("inserted tcp session is missing"))?
            .clone();
        self.index_pending(session_id, &indexed);
        self.arm_retransmit_timer(session_id, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
        self.mark_session_ready(session_id);
        Ok(session_id)
    }

    #[cfg(test)]
    pub(crate) fn dispatch_for_ticks(
        &mut self,
        runtime: &DataPlaneRuntime,
        timer_ticks: u32,
        output_next: SessionQueueNext,
    ) -> CoreResult<SessionQueueStep> {
        dispatch_session_queue_for_ticks(
            runtime,
            &mut self.driver,
            &mut self.protocol,
            timer_ticks,
            output_next,
        )
    }

    pub(crate) fn dispatch_once_at(
        &mut self,
        runtime: &DataPlaneRuntime,
        now: Instant,
        output_next: SessionQueueNext,
    ) -> CoreResult<()> {
        dispatch_session_queue_once_at(
            runtime,
            &mut self.driver,
            &mut self.protocol,
            now,
            output_next,
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn expire_timers_for_test(&mut self, ticks: u32) -> CoreResult<usize> {
        self.driver
            .poll_once_for_ticks(ticks)
            .map(|step| step.expired_timers)
    }
}

impl SessionQueueProtocol<TcpConnectionState> for TcpSessionProtocol {
    fn handle_timer_expiry(
        &mut self,
        driver: &mut SessionDriverRuntime<TcpConnectionState>,
        expiry: SessionTimerExpiry,
    ) -> CoreResult<()> {
        if expiry.token() == TcpSessionProtocol::RETRANSMIT_TIMER_TOKEN {
            if let Some(connection) = driver.session_state_mut(expiry.session_id()) {
                connection.tcp_timer_expire(TcpConnectionTimerKind::Retransmit);
            }
        }
        driver.mark_ready(expiry.session_id());
        Ok(())
    }

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        driver: &mut SessionDriverRuntime<TcpConnectionState>,
        session_id: SessionId,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<()> {
        let syn_output = {
            let Some(connection) = driver.session_state_mut(session_id) else {
                return Ok(());
            };
            let retransmit_syn =
                connection.tcp_timer_dispatch_pending(TcpConnectionTimerKind::Retransmit);
            let first_syn = connection.snd_una() == connection.iss()
                && connection.snd_nxt() == TcpSeq::new(connection.iss()).advance(1).raw();
            if connection.state() != TcpState::SynSent || (!retransmit_syn && !first_syn) {
                None
            } else {
                let local = connection.local().ok_or_else(|| {
                    hammer_core::error::CoreError::internal(
                        "syn-sent tcp session missing local address",
                    )
                })?;
                if retransmit_syn {
                    connection.observe_retransmit_timeout();
                }
                connection.tcp_timer_set(TcpConnectionTimerKind::Retransmit);
                Some((
                    local,
                    connection.remote(),
                    connection.iss(),
                    connection.rcv_nxt(),
                    connection.advertised_receive_window(connection.rcv_wnd()),
                    connection.local_capabilities(),
                ))
            }
        };
        if let Some((local, remote, sequence, acknowledgment, window, capabilities)) = syn_output {
            let metadata = hammer_adapter::RouteMetadata {
                network: hammer_adapter::Network::Tcp,
                source: Some(hammer_adapter::SocksAddr::ip(local.ip(), local.port())),
                destination: Some(hammer_adapter::SocksAddr::ip(remote.ip(), remote.port())),
                ..hammer_adapter::RouteMetadata::default()
            };
            let index = alloc_tcp_segment(
                driver.buffers(),
                metadata,
                TcpSegmentHeader {
                    source_port: local.port(),
                    destination_port: remote.port(),
                    sequence_number: sequence,
                    acknowledgment_number: acknowledgment,
                    flags: TcpSegmentFlags::SYN,
                    advertised_window: window,
                    capabilities,
                },
            )?;
            output.enqueue(runtime, output_next.node(), index)?;
        }
        if driver
            .session_state(session_id)
            .is_some_and(|connection| connection.state() == TcpState::SynSent)
        {
            driver.arm_timer_ticks(
                session_id,
                TcpSessionProtocol::RETRANSMIT_TIMER_TOKEN,
                TCP_ACTIVE_OPEN_TIMER_TICKS,
            )?;
        }
        if driver
            .session(session_id)
            .and_then(|entry| entry.app_op())
            .is_none()
        {
            return Ok(());
        }
        driver.app_mut().drain_submissions()
    }
}

pub struct TcpSessionProtocol {
    worker: DataWorkerId,
    index: TcpSessionConnectionIndex,
    pending_index: TcpPendingIndex,
    next_iss: u32,
}

impl TcpSessionProtocol {
    pub const RETRANSMIT_TIMER_TOKEN: SessionTimerToken = SessionTimerToken::new(1);

    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            index: TcpSessionConnectionIndex::empty(),
            pending_index: TcpPendingIndex::empty(),
            next_iss: 81_000,
        }
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn index(&self) -> &TcpSessionConnectionIndex {
        &self.index
    }

    #[inline]
    pub fn index_mut(&mut self) -> &mut TcpSessionConnectionIndex {
        &mut self.index
    }

    #[inline]
    pub fn index_session(&mut self, session_id: SessionId, connection: &TcpConnectionState) {
        self.index.upsert(session_id, connection);
    }

    #[inline]
    pub fn index_pending(&mut self, id: SessionId, connection: &TcpConnectionState) {
        self.pending_index.upsert(id, connection);
    }

    #[inline]
    pub fn session_id_by_tuple(&self, local: SocketAddr, remote: SocketAddr) -> Option<SessionId> {
        self.index.lookup_by_tuple(local, remote)
    }

    #[inline]
    pub fn pending_id_by_tuple(&self, local: SocketAddr, remote: SocketAddr) -> Option<SessionId> {
        self.pending_index.lookup_by_tuple(local, remote)
    }

    #[inline]
    pub fn session_id_by_connection_id(&self, connection_id: TcpConnectionId) -> Option<SessionId> {
        self.index.lookup_by_connection_id(connection_id)
    }

    #[inline]
    pub fn remove_session_index(&mut self, session_id: SessionId) {
        self.index.remove_session(session_id);
    }

    #[inline]
    pub fn remove_pending_index(&mut self, id: SessionId) {
        self.pending_index.remove(id);
    }

    pub fn next_initial_sequence(&mut self, local: SocketAddr, remote: SocketAddr) -> u32 {
        let mut value = self.next_iss;
        value ^= u32::from(local.port()) << 16 | u32::from(remote.port());
        value ^= match (local.ip(), remote.ip()) {
            (std::net::IpAddr::V4(local), std::net::IpAddr::V4(remote)) => {
                u32::from(local) ^ u32::from(remote).rotate_left(13)
            }
            (std::net::IpAddr::V6(local), std::net::IpAddr::V6(remote)) => {
                let local = u128::from(local);
                let remote = u128::from(remote);
                (local as u32) ^ ((local >> 64) as u32) ^ (remote as u32).rotate_left(7)
            }
            _ => 0x9e37_79b9,
        };
        self.next_iss = self.next_iss.wrapping_add(64_099);
        value.max(1)
    }

    #[inline]
    pub fn mark_session_ready(
        &mut self,
        context: &mut SessionProtocolContext<'_, TcpConnectionState>,
        session_id: SessionId,
    ) {
        context.mark_ready(session_id);
    }

    #[inline]
    pub fn arm_retransmit_timer(
        context: &mut SessionProtocolContext<'_, TcpConnectionState>,
        session_id: SessionId,
        ticks: u64,
    ) -> CoreResult<()> {
        context.arm_timer_ticks(session_id, Self::RETRANSMIT_TIMER_TOKEN, ticks)?;
        let Some(connection) = context.session_state_mut(session_id) else {
            return Ok(());
        };
        connection.tcp_timer_set(TcpConnectionTimerKind::Retransmit);
        Ok(())
    }

    #[inline]
    pub fn cancel_retransmit_timer(
        context: &mut SessionProtocolContext<'_, TcpConnectionState>,
        session_id: SessionId,
    ) -> bool {
        if let Some(connection) = context.session_state_mut(session_id) {
            connection.tcp_timer_reset(TcpConnectionTimerKind::Retransmit);
        }
        context.cancel_timer(session_id, Self::RETRANSMIT_TIMER_TOKEN)
    }

    #[inline]
    pub fn take_drained_sends(
        context: &mut SessionProtocolContext<'_, TcpConnectionState>,
    ) -> hammer_infra::vec::Vec<SessionAppSendSubmission> {
        context.app_mut().take_drained_sends()
    }

    #[inline]
    pub fn take_drained_closes(
        context: &mut SessionProtocolContext<'_, TcpConnectionState>,
    ) -> hammer_infra::vec::Vec<SessionAppCloseSubmission> {
        context.app_mut().take_drained_closes()
    }

    #[inline]
    pub fn register_queue(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
    ) -> CoreResult<SessionQueueHandle> {
        register_session_queue(&TCP_SESSION_QUEUES, TcpSessionQueue::new(worker, buffers))
    }

    #[inline]
    pub fn connect(
        handle: SessionQueueHandle,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> CoreResult<SessionId> {
        Self::with_queue(handle, |queue: &mut TcpSessionQueue| {
            queue.connect(local, remote)
        })
    }

    #[inline]
    pub fn session_queue_dispatch_fn() -> SessionQueueDispatchFn {
        tcp_session_queue_dispatch
    }

    #[inline]
    pub(crate) fn with_queue<R, F>(handle: SessionQueueHandle, f: F) -> CoreResult<R>
    where
        F: crate::session::node::SessionQueueAccess<TcpSessionQueue, R>,
    {
        with_session_queue(&TCP_SESSION_QUEUES, handle, f)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn register_queue_for_test(
        queue: TcpSessionQueue,
    ) -> CoreResult<SessionQueueHandle> {
        register_session_queue(&TCP_SESSION_QUEUES, queue)
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn register_queue_with_connection_for_test(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        connection: TcpConnectionState,
    ) -> CoreResult<SessionQueueHandle> {
        let mut queue = TcpSessionQueue::new(worker, buffers);
        let session_id = queue.insert_session(connection);
        let indexed = queue
            .session_state(session_id)
            .expect("inserted tcp test connection")
            .clone();
        queue.index_session(session_id, &indexed);
        Self::register_queue_for_test(queue)
    }
}

fn tcp_session_queue_dispatch(
    runtime: &DataPlaneRuntime,
    handle: SessionQueueHandle,
    output_next: SessionQueueNext,
    now: Instant,
) -> CoreResult<()> {
    TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
        queue.dispatch_once_at(runtime, now, output_next)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hammer_adapter::{
        BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeId, NodeProcessFn, NodeResult,
        NodeRuntimeData,
    };
    use hammer_core::error::CoreError;
    use hammer_core::protocol::tcp::{
        TcpCapabilities, TcpConnectionId, TcpSegmentFlags, TcpSegmentView, TcpState,
        tcp_options_from_bytes,
    };
    use hammer_runtime::app::{AppCqeKind, AppOpId, AppRingHandle, AppSqe, AppUserData};

    use super::*;
    use crate::session::SessionQueueNode;
    use std::sync::{Arc, Mutex, OnceLock};

    const ACTIVE_OPEN_ISS: u32 = 81_000;

    #[inline]
    const fn unused_output_next() -> SessionQueueNext {
        SessionQueueNext::from_node(NodeId::new(0))
    }

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

    fn tcp_connection() -> TcpConnectionState {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        TcpConnectionState::established_for_test(
            Some(TcpConnectionId::new(7001)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    fn syn_sent_connection(
        worker: DataWorkerId,
        local: SocketAddr,
        remote: SocketAddr,
        iss: u32,
        capabilities: TcpCapabilities,
    ) -> TcpConnectionState {
        let closed: TcpConnection<Closed> =
            TcpConnection::new(None, worker, local.port(), Some(local), remote);
        let mut syn_sent = closed.connect(iss);
        syn_sent.set_local_capabilities(capabilities);
        syn_sent.into()
    }

    fn install_connecting_session(
        queue: &mut TcpSessionQueue,
        connection: TcpConnectionState,
    ) -> CoreResult<SessionId> {
        let session_id = queue.insert_session(connection);
        let indexed = queue
            .session_state(session_id)
            .expect("inserted connecting session")
            .clone();
        queue.index_pending(session_id, &indexed);
        queue.arm_retransmit_timer(session_id, TCP_ACTIVE_OPEN_TIMER_TICKS)?;
        queue.mark_session_ready(session_id);
        Ok(session_id)
    }

    #[test]
    fn tcp_active_open_creates_syn_sent_session_and_emits_syn() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let worker = DataWorkerId::new(0);
        let handle = TcpSessionProtocol::register_queue_for_test(TcpSessionQueue::new(
            worker,
            runtime.packet_buffers().clone(),
        ))
        .expect("register queue");
        let local: SocketAddr = "192.0.2.10:50001".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        let capture = Arc::new(Mutex::new(CaptureState::default()));
        let output = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let queue_driver = SessionQueueNode::new().expect("session queue node");
        queue_driver
            .attach_queue(
                handle,
                SessionQueueNext::from_node(output),
                TcpSessionProtocol::session_queue_dispatch_fn(),
            )
            .expect("attach tcp queue");
        let session_queue = runtime.nodes().register_driver(queue_driver);

        let session_id = TcpSessionProtocol::connect(handle, local, remote).expect("active open");
        let active_open_iss =
            TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
                let session = queue
                    .session_state(session_id)
                    .expect("active open session exists before SYN output");
                Ok(session.iss())
            })
            .expect("read active-open iss");

        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule session queue");
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 2);

        let packets = &capture.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        assert_tcp_syn(
            &packets[0],
            local,
            remote,
            active_open_iss,
            TcpCapabilities::default(),
        );

        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            let session = queue
                .session_state(session_id)
                .expect("active open session remains installed");
            assert_eq!(session.state(), TcpState::SynSent);
            assert_eq!(session.snd_una(), active_open_iss);
            assert_eq!(session.snd_nxt(), active_open_iss + 1);
            assert_eq!(session.rcv_nxt(), 0);
            assert!(session.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
            assert_eq!(queue.session_id_by_tuple(local, remote), None);
            assert_eq!(queue.pending_id_by_tuple(local, remote), Some(session_id));
            Ok(())
        })
        .expect("inspect active open");
    }

    #[test]
    fn tcp_active_open_retransmit_timer_reemits_syn() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let worker = DataWorkerId::new(0);
        let handle =
            TcpSessionProtocol::register_queue_for_test(TcpSessionQueue::with_timer_clock(
                worker,
                runtime.packet_buffers().clone(),
                Duration::from_millis(10),
                Instant::now(),
            ))
            .expect("register queue");
        let local: SocketAddr = "192.0.2.10:50002".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        let capture = Arc::new(Mutex::new(CaptureState::default()));
        let output = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let queue_driver = SessionQueueNode::new().expect("session queue node");
        queue_driver
            .attach_queue(
                handle,
                SessionQueueNext::from_node(output),
                TcpSessionProtocol::session_queue_dispatch_fn(),
            )
            .expect("attach tcp queue");
        let session_queue = runtime.nodes().register_driver(queue_driver);

        let session_id = TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            install_connecting_session(
                queue,
                syn_sent_connection(
                    worker,
                    local,
                    remote,
                    ACTIVE_OPEN_ISS,
                    TcpCapabilities::default(),
                ),
            )
        })
        .expect("active open");

        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule session queue");
        assert_eq!(runtime.run_ready_nodes().expect("run first syn"), 2);
        capture.lock().unwrap().packets.clear();

        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            queue
                .expire_timers_for_test(1)
                .expect("expire before timer");
            Ok(())
        })
        .expect("expire before timer");
        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule no retransmit");
        assert_eq!(runtime.run_ready_nodes().expect("run empty"), 1);
        assert!(capture.lock().unwrap().packets.is_empty());

        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            queue.expire_timers_for_test(1).expect("expire retransmit");
            Ok(())
        })
        .expect("expire retransmit");
        runtime
            .schedule_empty_frame(session_queue)
            .expect("schedule retransmit");
        assert_eq!(runtime.run_ready_nodes().expect("run retransmit"), 2);

        let packets = &capture.lock().unwrap().packets;
        assert_eq!(packets.len(), 1);
        assert_tcp_syn(
            &packets[0],
            local,
            remote,
            ACTIVE_OPEN_ISS,
            TcpCapabilities::default(),
        );
        TcpSessionProtocol::with_queue(handle, |queue: &mut TcpSessionQueue| {
            let session = queue.session_state(session_id).expect("session after rxt");
            assert!(session.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
            Ok(())
        })
        .expect("inspect retransmit session");
    }

    #[test]
    fn tcp_session_queue_indexes_pool_session_ids_and_drains_ready_app_close() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let ring = AppRingHandle::new(4, 4);
        let op = AppOpId::new(7001);
        let session_id = queue.insert_session(tcp_connection());
        let state = queue
            .session_state(session_id)
            .expect("session state from pool")
            .clone();

        queue.index_session(session_id, &state);
        assert!(queue.bind_session_app_ring(session_id, op, ring.clone()));
        queue.mark_session_ready(session_id);
        ring.try_push_submission(AppSqe::close(Some(AppUserData::new(11)), op))
            .expect("push close sqe");

        queue
            .dispatch_for_ticks(&runtime, 0, unused_output_next())
            .expect("dispatch queue");

        let closes = queue.app_mut().take_drained_closes();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].session_id, session_id);
        assert_eq!(closes[0].op, op);
    }

    #[test]
    fn tcp_session_queue_resolves_app_submission_session_from_descriptor_op() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let ring = AppRingHandle::new(4, 4);
        let first_op = AppOpId::new(7001);
        let second_op = AppOpId::new(7002);
        let first_session_id = queue.insert_session(tcp_connection());
        let second_session_id = queue.insert_session(tcp_connection());

        assert!(queue.bind_session_app_ring(first_session_id, first_op, ring.clone()));
        assert!(queue.bind_session_app_ring(second_session_id, second_op, ring.clone()));
        queue.mark_session_ready(first_session_id);
        ring.try_push_submission(AppSqe::close(Some(AppUserData::new(22)), second_op))
            .expect("push second close sqe");

        queue
            .dispatch_for_ticks(&runtime, 0, unused_output_next())
            .expect("dispatch queue");

        let closes = queue.app_mut().take_drained_closes();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].session_id, second_session_id);
        assert_eq!(closes[0].op, second_op);
    }

    #[test]
    fn tcp_session_queue_delivers_payload_to_pending_recv_cqe() {
        let worker = DataWorkerId::new(0);
        let buffers = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, buffers.packet_buffers().clone());
        let ring = AppRingHandle::new(4, 4);
        let op = AppOpId::new(7_101);
        let session_id = queue.insert_session(tcp_connection());
        assert!(queue.bind_session_app_ring(session_id, op, ring.clone()));
        ring.try_push_submission(AppSqe::recv(Some(AppUserData::new(33)), op, 32))
            .expect("push recv sqe");
        let buffer = buffers
            .alloc_index_with_bytes(Default::default(), b"tcp:hello")
            .expect("recv buffer");
        buffers.advance(buffer, 4).expect("advance to payload");

        queue
            .enqueue_rx(session_id, buffer, false)
            .expect("enqueue rx");

        let completion = ring.pop_completion().expect("recv completion");
        assert_eq!(completion.user_data(), Some(AppUserData::new(33)));
        match completion.kind() {
            AppCqeKind::Recv {
                op: completed_op,
                fin,
                ..
            } => {
                assert_eq!(*completed_op, op);
                assert!(!fin);
            }
            other => panic!("expected recv completion, got {other:?}"),
        }
        let recv = completion.into_recv().expect("recv cqe");
        assert_eq!(
            recv.copy_current().expect("recv payload"),
            b"hello".to_vec()
        );
        recv.release();
    }

    #[test]
    fn tcp_session_queue_retransmit_timer_can_be_armed_and_cancelled() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let session_id = queue.insert_session(tcp_connection());

        queue
            .arm_retransmit_timer(session_id, 1)
            .expect("arm retransmit timer");
        assert!(
            queue
                .session_state(session_id)
                .expect("armed session")
                .tcp_timer_is_active(TcpConnectionTimerKind::Retransmit)
        );
        queue
            .dispatch_for_ticks(&runtime, 1, unused_output_next())
            .expect("expire timer");
        assert!(
            !queue
                .session_state(session_id)
                .expect("expired session")
                .tcp_timer_is_live(TcpConnectionTimerKind::Retransmit)
        );

        queue
            .arm_retransmit_timer(session_id, 1)
            .expect("rearm retransmit timer");
        assert!(queue.cancel_retransmit_timer(session_id));
        queue
            .dispatch_for_ticks(&runtime, 1, unused_output_next())
            .expect("dispatch cancelled timer");
        assert!(
            !queue
                .session_state(session_id)
                .expect("cancelled session")
                .tcp_timer_is_live(TcpConnectionTimerKind::Retransmit)
        );
    }

    #[test]
    fn tcp_session_queue_rearmed_retransmit_timer_keeps_new_active_timer() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::with_capacities(64, 4, 4, 4);
        let mut queue = TcpSessionQueue::new(worker, runtime.packet_buffers().clone());
        let session_id = queue.insert_session(tcp_connection());

        queue
            .arm_retransmit_timer(session_id, 1)
            .expect("arm first retransmit timer");

        queue.expire_timers_for_test(1).expect("expire first timer");
        queue
            .arm_retransmit_timer(session_id, 4)
            .expect("rearm retransmit timer before expiry dispatch");

        queue
            .dispatch_for_ticks(&runtime, 0, unused_output_next())
            .expect("dispatch stale expiry");

        let connection = queue.session_state(session_id).expect("rearmed session");
        assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
        assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::Retransmit));
    }

    fn assert_tcp_syn(
        packet: &[u8],
        local: SocketAddr,
        remote: SocketAddr,
        sequence: u32,
        expected: TcpCapabilities,
    ) {
        let segment = TcpSegmentView::parse(packet).expect("tcp segment");
        assert_eq!(segment.source_port(), local.port());
        assert_eq!(segment.destination_port(), remote.port());
        assert_eq!(segment.sequence_number(), sequence);
        assert!(segment.flags().contains(TcpSegmentFlags::SYN));
        let options = tcp_options_from_bytes(segment.options());
        assert_eq!(options.capabilities, expected);
    }
}

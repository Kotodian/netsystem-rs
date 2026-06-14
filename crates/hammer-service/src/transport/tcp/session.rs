use std::cell::RefCell;
use std::net::SocketAddr;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataWorkerId};
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::TcpConnectionId;
#[cfg(test)]
use hammer_runtime::app::{AppOpId, AppRingHandle};

use super::{TcpConnectionState, TcpConnectionTimerKind, TcpSessionConnectionIndex};
use crate::session::node::{
    SessionQueueDispatchFn, SessionQueueHandle, SessionQueueNext, register_session_queue,
    with_session_queue,
};
use crate::session::runtime::{
    SessionDriverRuntime, SessionEntry, SessionQueueProtocol, dispatch_session_queue_once_at,
};
#[cfg(test)]
use crate::session::runtime::{SessionQueueStep, dispatch_session_queue_for_ticks};
use crate::session::{
    SessionAppCloseSubmission, SessionAppSendSubmission, SessionId, SessionProtocolContext,
    SessionTimerExpiry, SessionTimerToken,
};

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

    #[inline]
    pub(crate) fn session_state_mut(
        &mut self,
        session_id: SessionId,
    ) -> Option<&mut TcpConnectionState> {
        self.driver.session_state_mut(session_id)
    }

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
    pub(crate) fn session_id_by_tuple(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<SessionId> {
        self.protocol.session_id_by_tuple(local, remote)
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

    #[cfg(test)]
    pub(crate) fn dispatch_for_ticks(
        &mut self,
        timer_ticks: u32,
        output_next: SessionQueueNext,
    ) -> CoreResult<SessionQueueStep> {
        dispatch_session_queue_for_ticks(
            &mut self.driver,
            &mut self.protocol,
            timer_ticks,
            output_next,
        )
    }

    pub(crate) fn dispatch_once_at(
        &mut self,
        now: Instant,
        output_next: SessionQueueNext,
    ) -> CoreResult<()> {
        dispatch_session_queue_once_at(&mut self.driver, &mut self.protocol, now, output_next)?;
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
        if expiry.token() == TcpSessionProtocol::RETRANSMIT_TIMER_TOKEN
            && let Some(connection) = driver.session_state_mut(expiry.session_id())
        {
            connection.tcp_timer_expire(TcpConnectionTimerKind::Retransmit);
        }
        driver.mark_ready(expiry.session_id());
        Ok(())
    }

    fn handle_ready_session(
        &mut self,
        driver: &mut SessionDriverRuntime<TcpConnectionState>,
        session_id: SessionId,
        _output_next: SessionQueueNext,
    ) -> CoreResult<()> {
        if let Some(connection) = driver.session_state_mut(session_id) {
            let _ = connection.tcp_timer_dispatch_pending(TcpConnectionTimerKind::Retransmit);
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
}

impl TcpSessionProtocol {
    pub const RETRANSMIT_TIMER_TOKEN: SessionTimerToken = SessionTimerToken::new(1);

    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            index: TcpSessionConnectionIndex::empty(),
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
    pub fn session_id_by_tuple(&self, local: SocketAddr, remote: SocketAddr) -> Option<SessionId> {
        self.index.lookup_by_tuple(local, remote)
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
    pub fn session_queue_dispatch_fn() -> SessionQueueDispatchFn {
        tcp_session_queue_dispatch
    }

    #[inline]
    pub(crate) fn with_queue<R>(
        handle: SessionQueueHandle,
        f: impl FnOnce(&mut TcpSessionQueue) -> CoreResult<R>,
    ) -> CoreResult<R> {
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
    handle: SessionQueueHandle,
    output_next: SessionQueueNext,
    now: Instant,
) -> CoreResult<()> {
    TcpSessionProtocol::with_queue(handle, |queue| {
        queue.dispatch_once_at(now, output_next)?;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hammer_adapter::DataPlaneRuntime;
    use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
    use hammer_runtime::app::{AppCqeKind, AppOpId, AppRingHandle, AppSqe, AppUserData};

    use super::*;

    const TEST_OUTPUT_NEXT: SessionQueueNext = SessionQueueNext::from_slot(0);

    fn tcp_connection() -> TcpConnectionState {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        TcpConnectionState::new(
            Some(TcpConnectionId::new(7001)),
            DataWorkerId::new(0),
            TcpState::Established,
            local.port(),
            Some(local),
            remote,
        )
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
            .dispatch_for_ticks(0, TEST_OUTPUT_NEXT)
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
            .dispatch_for_ticks(0, TEST_OUTPUT_NEXT)
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
            .dispatch_for_ticks(1, TEST_OUTPUT_NEXT)
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
            .dispatch_for_ticks(1, TEST_OUTPUT_NEXT)
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
            .dispatch_for_ticks(0, TEST_OUTPUT_NEXT)
            .expect("dispatch stale expiry");

        let connection = queue.session_state(session_id).expect("rearmed session");
        assert!(connection.tcp_timer_is_active(TcpConnectionTimerKind::Retransmit));
        assert!(!connection.tcp_timer_is_pending(TcpConnectionTimerKind::Retransmit));
    }
}

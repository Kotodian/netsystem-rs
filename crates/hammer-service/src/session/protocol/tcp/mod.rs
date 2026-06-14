use std::cell::RefCell;
use std::net::SocketAddr;

use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_runtime::app::{AppOpId, AppRingHandle};

use crate::session::node::{
    SessionQueueHandle, register_session_queue_runtime, with_session_queue_runtime,
};
use crate::session::worker::{SessionQueueProgram, SessionQueueRuntime};
use crate::session::{
    SessionAppCloseSubmission, SessionAppSendSubmission, SessionId, SessionProtocolContext,
    SessionTimerExpiry, SessionTimerToken,
};
use crate::transport::tcp::TcpLookupId;

use self::state::{TcpSessionIndex, TcpSessionState};

pub mod node;
pub mod state;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpTimerDispatch {
    pub session_id: SessionId,
    pub token: SessionTimerToken,
}

thread_local! {
    static TCP_SESSION_QUEUE_RUNTIMES: RefCell<hammer_infra::vec::Vec<SessionQueueRuntime<TcpSessionProtocol>>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

pub struct TcpSessionProtocol {
    worker: DataWorkerId,
    index: TcpSessionIndex,
    expired_timers: hammer_infra::vec::Vec<TcpTimerDispatch>,
}

impl TcpSessionProtocol {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self {
            worker,
            index: TcpSessionIndex::empty(),
            expired_timers: hammer_infra::vec::Vec::new(),
        }
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn index(&self) -> &TcpSessionIndex {
        &self.index
    }

    #[inline]
    pub fn index_mut(&mut self) -> &mut TcpSessionIndex {
        &mut self.index
    }

    #[inline]
    pub fn index_session(&mut self, session_id: SessionId, session: &TcpSessionState) {
        self.index.upsert(session_id, session);
    }

    #[inline]
    pub fn session_id_by_tuple(&self, local: SocketAddr, remote: SocketAddr) -> Option<SessionId> {
        self.index.lookup_by_tuple(local, remote)
    }

    #[inline]
    pub fn session_id_by_lookup_id(&self, lookup_id: TcpLookupId) -> Option<SessionId> {
        self.index.lookup_by_lookup_id(lookup_id)
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
        context: &mut SessionProtocolContext<'_, TcpSessionState>,
        session_id: SessionId,
    ) {
        context.mark_ready(session_id);
    }

    #[inline]
    pub fn bind_session_app_ring(
        &mut self,
        context: &mut SessionProtocolContext<'_, TcpSessionState>,
        session_id: SessionId,
        op: AppOpId,
        ring: AppRingHandle,
    ) -> bool {
        context.bind_session_app_ring(session_id, op, ring)
    }

    #[inline]
    pub fn take_drained_sends(
        context: &mut SessionProtocolContext<'_, TcpSessionState>,
    ) -> hammer_infra::vec::Vec<SessionAppSendSubmission> {
        context.app_mut().take_drained_sends()
    }

    #[inline]
    pub fn take_drained_closes(
        context: &mut SessionProtocolContext<'_, TcpSessionState>,
    ) -> hammer_infra::vec::Vec<SessionAppCloseSubmission> {
        context.app_mut().take_drained_closes()
    }

    #[inline]
    pub fn take_expired_timers(&mut self) -> hammer_infra::vec::Vec<TcpTimerDispatch> {
        self.expired_timers.drain(..).collect()
    }

    #[inline]
    pub(crate) fn register_queue(worker: DataWorkerId) -> CoreResult<SessionQueueHandle> {
        register_session_queue_runtime(
            &TCP_SESSION_QUEUE_RUNTIMES,
            SessionQueueRuntime::new(worker, Self::new(worker)),
        )
    }

    #[inline]
    pub(crate) fn with_queue<R>(
        handle: SessionQueueHandle,
        f: impl FnOnce(&mut SessionQueueRuntime<Self>) -> CoreResult<R>,
    ) -> CoreResult<R> {
        with_session_queue_runtime(&TCP_SESSION_QUEUE_RUNTIMES, handle, f)
    }
}

impl SessionQueueProgram for TcpSessionProtocol {
    type Session = TcpSessionState;

    fn handle_timer_expiry(
        &mut self,
        context: &mut SessionProtocolContext<'_, Self::Session>,
        expiry: SessionTimerExpiry,
    ) -> CoreResult<()> {
        self.expired_timers.push(TcpTimerDispatch {
            session_id: expiry.session_id(),
            token: expiry.token(),
        });
        context.mark_ready(expiry.session_id());
        Ok(())
    }

    fn handle_ready(
        &mut self,
        context: &mut SessionProtocolContext<'_, Self::Session>,
        session_id: SessionId,
    ) -> CoreResult<()> {
        let Some(op) = context.session_app_op(session_id) else {
            return Ok(());
        };
        context.app_mut().drain_submissions_for_op(op)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use hammer_core::protocol::tcp::{TcpConnectionId, TcpState};
    use hammer_runtime::app::{AppOpId, AppRingHandle, AppSqe, AppUserData};

    use super::*;

    fn tcp_session() -> TcpSessionState {
        let local: SocketAddr = "192.0.2.10:50000".parse().expect("local");
        let remote: SocketAddr = "198.51.100.10:443".parse().expect("remote");
        TcpSessionState::new(
            7,
            Some(TcpConnectionId::new(7001)),
            DataWorkerId::new(0),
            TcpState::Established,
            local.port(),
            Some(local),
            remote,
        )
    }

    #[test]
    fn tcp_session_protocol_indexes_pool_session_ids_and_drains_ready_app_close() {
        let worker = DataWorkerId::new(0);
        let mut runtime = SessionQueueRuntime::new(worker, TcpSessionProtocol::new(worker));
        let ring = AppRingHandle::new(4, 4);
        let op = AppOpId::new(7001);
        let session_id = runtime.insert_session(tcp_session());
        let state = runtime
            .session_state(session_id)
            .expect("session state from pool")
            .clone();

        runtime.program_mut().index_session(session_id, &state);
        assert_eq!(
            runtime.program_mut().session_id_by_lookup_id(7),
            Some(session_id)
        );
        assert!(runtime.bind_session_app_ring(session_id, op, ring.clone()));
        runtime.sessions_mut().mark_ready(session_id);
        ring.try_push_submission(AppSqe::close(Some(AppUserData::new(11)), op))
            .expect("push close sqe");

        runtime.run_once_for_ticks(0).expect("run queue");

        let closes = runtime.app_mut().take_drained_closes();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].session_id, session_id);
        assert_eq!(closes[0].op, op);
    }

    #[test]
    fn tcp_session_protocol_resolves_app_submission_session_from_descriptor_op() {
        let worker = DataWorkerId::new(0);
        let mut runtime = SessionQueueRuntime::new(worker, TcpSessionProtocol::new(worker));
        let ring = AppRingHandle::new(4, 4);
        let first_op = AppOpId::new(7001);
        let second_op = AppOpId::new(7002);
        let first_session_id = runtime.insert_session(tcp_session());
        let second_session_id = runtime.insert_session(tcp_session());

        assert!(runtime.bind_session_app_ring(first_session_id, first_op, ring.clone()));
        assert!(runtime.bind_session_app_ring(second_session_id, second_op, ring.clone()));
        runtime.sessions_mut().mark_ready(first_session_id);
        ring.try_push_submission(AppSqe::close(Some(AppUserData::new(22)), second_op))
            .expect("push second close sqe");

        runtime.run_once_for_ticks(0).expect("run queue");

        let closes = runtime.app_mut().take_drained_closes();
        assert_eq!(closes.len(), 1);
        assert_eq!(closes[0].session_id, second_session_id);
        assert_eq!(closes[0].op, second_op);
    }
}

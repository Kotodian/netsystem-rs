use std::cell::RefCell;

use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;

use crate::session::node::{
    SessionQueueHandle, register_session_queue_runtime, with_session_queue_runtime,
};
use crate::session::worker::{SessionQueueProgram, SessionQueueRuntime};
use crate::session::{SessionId, SessionProtocolContext, SessionTimerExpiry};

pub mod node;
pub mod state;

thread_local! {
    static TCP_SESSION_QUEUE_RUNTIMES: RefCell<hammer_infra::vec::Vec<SessionQueueRuntime<TcpSessionProtocol>>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

pub struct TcpSessionProtocol {
    worker: DataWorkerId,
}

impl TcpSessionProtocol {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self { worker }
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
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
    fn handle_timer_expiry(
        &mut self,
        _context: &mut SessionProtocolContext<'_>,
        expiry: SessionTimerExpiry,
    ) -> CoreResult<()> {
        let _ = expiry;
        Ok(())
    }

    fn handle_ready(
        &mut self,
        _context: &mut SessionProtocolContext<'_>,
        session_id: SessionId,
    ) -> CoreResult<()> {
        let _ = session_id;
        Ok(())
    }
}

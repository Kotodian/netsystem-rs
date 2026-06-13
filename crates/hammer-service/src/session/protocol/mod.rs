pub mod tcp;

use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;

use crate::session::{SessionId, SessionTimerToken, WorkerSessionRuntime};

pub struct SessionProtocolContext<'a> {
    worker: DataWorkerId,
    runtime: &'a mut WorkerSessionRuntime,
}

impl<'a> SessionProtocolContext<'a> {
    #[inline]
    pub fn new(worker: DataWorkerId, runtime: &'a mut WorkerSessionRuntime) -> Self {
        Self { worker, runtime }
    }

    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: SessionId) {
        self.runtime.mark_ready(session_id);
    }

    #[inline]
    pub fn arm_timer_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.runtime.arm_timer_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        self.runtime.cancel_timer(session_id, token)
    }
}

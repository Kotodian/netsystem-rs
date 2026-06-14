pub mod tcp;

use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;
use hammer_infra::pool::Pool;
use hammer_runtime::app::{AppOpId, AppRingHandle};

use crate::session::{
    SessionAppRuntime, SessionId, SessionTimerToken, WorkerSessionRuntime,
    worker::SessionEntry,
};

pub struct SessionProtocolContext<'a, S> {
    worker: DataWorkerId,
    runtime: &'a mut WorkerSessionRuntime,
    entries: &'a mut Pool<SessionEntry<S>>,
    app: &'a mut SessionAppRuntime,
}

impl<'a, S> SessionProtocolContext<'a, S> {
    #[inline]
    pub(crate) fn new(
        worker: DataWorkerId,
        runtime: &'a mut WorkerSessionRuntime,
        entries: &'a mut Pool<SessionEntry<S>>,
        app: &'a mut SessionAppRuntime,
    ) -> Self {
        Self {
            worker,
            runtime,
            entries,
            app,
        }
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

    #[inline]
    pub fn session_state(&self, session_id: SessionId) -> Option<&S> {
        self.entries
            .get(session_id.pool_index())
            .map(SessionEntry::state)
    }

    #[inline]
    pub fn session_state_mut(&mut self, session_id: SessionId) -> Option<&mut S> {
        self.entries
            .get_mut(session_id.pool_index())
            .map(SessionEntry::state_mut)
    }

    #[inline]
    pub fn session_app_op(&self, session_id: SessionId) -> Option<AppOpId> {
        self.entries
            .get(session_id.pool_index())
            .and_then(SessionEntry::app_op)
    }

    #[inline]
    pub fn bind_session_app_ring(
        &mut self,
        session_id: SessionId,
        op: AppOpId,
        ring: AppRingHandle,
    ) -> bool {
        let Some(entry) = self.entries.get_mut(session_id.pool_index()) else {
            return false;
        };
        entry.bind_app_op(op);
        self.app.bind_ring(session_id, op, ring);
        true
    }

    #[inline]
    pub fn app(&self) -> &SessionAppRuntime {
        self.app
    }

    #[inline]
    pub fn app_mut(&mut self) -> &mut SessionAppRuntime {
        self.app
    }
}

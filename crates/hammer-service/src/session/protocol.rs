use hammer_adapter::DataWorkerId;
use hammer_core::error::CoreResult;
use hammer_runtime::app::{AppOpId, AppRingHandle};

use crate::session::{
    SessionAppRuntime, SessionId, SessionTimerToken, runtime::SessionDriverRuntime,
};

pub struct SessionProtocolContext<'a, S> {
    driver: &'a mut SessionDriverRuntime<S>,
}

impl<'a, S> SessionProtocolContext<'a, S> {
    #[inline]
    pub(crate) fn new(driver: &'a mut SessionDriverRuntime<S>) -> Self {
        Self { driver }
    }

    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.driver.worker()
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: SessionId) {
        self.driver.mark_ready(session_id);
    }

    #[inline]
    pub fn arm_timer_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.driver.arm_timer_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        self.driver.cancel_timer(session_id, token)
    }

    #[inline]
    pub fn session_state(&self, session_id: SessionId) -> Option<&S> {
        self.driver.session_state(session_id)
    }

    #[inline]
    pub fn session_state_mut(&mut self, session_id: SessionId) -> Option<&mut S> {
        self.driver.session_state_mut(session_id)
    }

    #[inline]
    pub fn session_app_op(&self, session_id: SessionId) -> Option<AppOpId> {
        self.driver
            .session(session_id)
            .and_then(|entry| entry.app_op())
    }

    #[inline]
    pub fn bind_session_app_ring(
        &mut self,
        session_id: SessionId,
        op: AppOpId,
        ring: AppRingHandle,
    ) -> bool {
        self.driver.bind_session_app_ring(session_id, op, ring)
    }

    #[inline]
    pub fn app(&self) -> &SessionAppRuntime {
        self.driver.app()
    }

    #[inline]
    pub fn app_mut(&mut self) -> &mut SessionAppRuntime {
        self.driver.app_mut()
    }
}

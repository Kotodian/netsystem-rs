use hammer_core::data_plane::DataPlaneBuffers;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;

use crate::session::{SessionId, runtime::WorkerSessionRuntime};

pub(in crate::session) struct SessionWorkScheduler {
    schedule_pending: *mut bool,
    sessions: *mut WorkerSessionRuntime,
    current_session_id: SessionId,
}

impl SessionWorkScheduler {
    /// SAFETY: `schedule_pending` must point to the current session entry's
    /// scheduling bit, and `sessions` must point to the owning
    /// `WorkerSessionRuntime`. Neither pointer may alias the live protocol
    /// state borrow for this session.
    #[inline]
    pub(in crate::session) const unsafe fn new(
        schedule_pending: *mut bool,
        sessions: *mut WorkerSessionRuntime,
        current_session_id: SessionId,
    ) -> Self {
        Self {
            schedule_pending,
            sessions,
            current_session_id,
        }
    }

    #[inline]
    fn mark_ready(&mut self) {
        unsafe {
            let schedule_pending = &mut *self.schedule_pending;
            if *schedule_pending {
                return;
            }
            *schedule_pending = true;
            (*self.sessions).schedule_session_work(self.current_session_id);
        }
    }
}

pub struct SessionQueueControlContext {
    timer_wheel: *mut TimerWheel1t2w2048sl<u32>,
    scheduler: SessionWorkScheduler,
    buffers: *const DataPlaneBuffers,
    current_session_id: SessionId,
    has_pending_tx: std::cell::Cell<bool>,
}

impl SessionQueueControlContext {
    #[inline]
    pub(in crate::session) fn new(
        timer_wheel: *mut TimerWheel1t2w2048sl<u32>,
        scheduler: SessionWorkScheduler,
        buffers: *const DataPlaneBuffers,
        current_session_id: SessionId,
        has_pending_tx: bool,
    ) -> Self {
        Self {
            timer_wheel,
            scheduler,
            buffers,
            current_session_id,
            has_pending_tx: std::cell::Cell::new(has_pending_tx),
        }
    }

    #[inline]
    pub fn buffers(&self) -> &DataPlaneBuffers {
        unsafe { &*self.buffers }
    }

    #[inline]
    pub fn timer_wheel(&mut self) -> &mut TimerWheel1t2w2048sl<u32> {
        unsafe { &mut *self.timer_wheel }
    }

    #[inline]
    pub fn mark_ready(&mut self) {
        self.scheduler.mark_ready();
    }

    #[inline]
    pub const fn session_id(&self) -> SessionId {
        self.current_session_id
    }

    #[inline]
    pub fn has_pending_tx(&self) -> bool {
        self.has_pending_tx.get()
    }

    #[inline]
    pub fn refresh_has_pending_tx(&self, value: bool) {
        self.has_pending_tx.set(value);
    }
}

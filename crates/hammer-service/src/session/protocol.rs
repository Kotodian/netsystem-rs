use hammer_adapter::DataPlaneBuffers;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use std::ptr::NonNull;

use crate::session::SessionId;

pub(crate) type ScheduleSessionWorkFn = unsafe fn(*mut (), SessionId);

pub struct SessionQueueControlContext {
    timer_wheel: *mut TimerWheel1t2w2048sl<u32>,
    scheduler: NonNull<()>,
    schedule_session_work: ScheduleSessionWorkFn,
    buffers: *const DataPlaneBuffers,
    current_session_id: SessionId,
    has_pending_tx: std::cell::Cell<bool>,
}

impl SessionQueueControlContext {
    #[inline]
    pub(in crate::session) fn new(
        timer_wheel: *mut TimerWheel1t2w2048sl<u32>,
        scheduler: NonNull<()>,
        schedule_session_work: ScheduleSessionWorkFn,
        buffers: *const DataPlaneBuffers,
        current_session_id: SessionId,
        has_pending_tx: bool,
    ) -> Self {
        Self {
            timer_wheel,
            scheduler,
            schedule_session_work,
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
        unsafe { (self.schedule_session_work)(self.scheduler.as_ptr(), self.current_session_id) };
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

use crate::session::ready::SessionReadyQueue;
use hammer_adapter::DataPlaneBuffers;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;

use crate::session::SessionId;

pub struct SessionQueueControlContext {
    timer_wheel: *mut TimerWheel1t2w2048sl<u32>,
    ready: *mut SessionReadyQueue,
    buffers: *const DataPlaneBuffers,
    current_session_id: SessionId,
    has_pending_tx: std::cell::Cell<bool>,
}

impl SessionQueueControlContext {
    #[inline]
    pub fn new(
        timer_wheel: *mut TimerWheel1t2w2048sl<u32>,
        ready: *mut SessionReadyQueue,
        buffers: *const DataPlaneBuffers,
        current_session_id: SessionId,
        has_pending_tx: bool,
    ) -> Self {
        Self {
            timer_wheel,
            ready,
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
        if self.ready.is_null() {
            return;
        }
        unsafe { &mut *self.ready }.mark_ready(self.current_session_id);
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

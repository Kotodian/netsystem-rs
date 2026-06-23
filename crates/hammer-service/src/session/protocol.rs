use crate::session::ready::SessionReadyQueue;
use hammer_adapter::DataPlaneBuffers;
use hammer_infra::timer_wheel::TimerWheel2t1w2048;

use crate::session::SessionId;

pub(crate) struct SessionQueueControlContext {
    timer_wheel: *mut TimerWheel2t1w2048<u32>,
    ready: *mut SessionReadyQueue,
    buffers: *const DataPlaneBuffers,
    current_session_id: SessionId,
    has_pending_tx: bool,
}

impl SessionQueueControlContext {
    #[inline]
    pub(crate) fn new(
        timer_wheel: *mut TimerWheel2t1w2048<u32>,
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
            has_pending_tx,
        }
    }

    #[inline]
    pub(crate) fn buffers(&self) -> &DataPlaneBuffers {
        unsafe { &*self.buffers }
    }

    #[inline]
    pub(crate) fn timer_wheel(&mut self) -> &mut TimerWheel2t1w2048<u32> {
        unsafe { &mut *self.timer_wheel }
    }

    #[inline]
    pub(crate) fn mark_ready(&mut self) {
        if self.ready.is_null() {
            return;
        }
        unsafe { &mut *self.ready }.mark_ready(self.current_session_id);
    }

    #[inline]
    pub(crate) const fn session_id(&self) -> SessionId {
        self.current_session_id
    }

    #[inline]
    pub(crate) const fn has_pending_tx(&self) -> bool {
        self.has_pending_tx
    }
}

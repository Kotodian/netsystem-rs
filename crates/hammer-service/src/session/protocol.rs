use std::marker::PhantomData;

use hammer_adapter::DataPlaneBuffers;
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_runtime::app::AppOpId;

use crate::session::{
    SessionId, SessionTimerToken,
    runtime::{SessionRxQueue, WorkerSessionRuntime},
};

pub(crate) struct SessionQueueControlContext<'a, A> {
    sessions: *mut WorkerSessionRuntime,
    app: *mut crate::session::SessionAppRuntime,
    buffers: *const DataPlaneBuffers,
    rx: *mut Pool<SessionRxQueue>,
    rx_index: *mut FlatHashTable<u64, PoolIndex>,
    aux: *mut A,
    current_session_id: SessionId,
    current_app_op: Option<AppOpId>,
    _marker: PhantomData<&'a mut A>,
}

impl<'a, A> SessionQueueControlContext<'a, A> {
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        sessions: *mut WorkerSessionRuntime,
        app: *mut crate::session::SessionAppRuntime,
        buffers: *const DataPlaneBuffers,
        rx: *mut Pool<SessionRxQueue>,
        rx_index: *mut FlatHashTable<u64, PoolIndex>,
        aux: *mut A,
        current_session_id: SessionId,
        current_app_op: Option<AppOpId>,
    ) -> Self {
        Self {
            sessions,
            app,
            buffers,
            rx,
            rx_index,
            aux,
            current_session_id,
            current_app_op,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn arm_timer_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        unsafe { &mut *self.sessions }.arm_timer_ticks(session_id, token, ticks)
    }

    #[inline]
    pub(crate) fn buffers(&self) -> &DataPlaneBuffers {
        unsafe { &*self.buffers }
    }

    #[inline]
    pub(crate) fn flush_session_rx(&mut self, session_id: SessionId) -> CoreResult<()> {
        if session_id != self.current_session_id {
            return Err(CoreError::internal(
                "session rx flush must target current session",
            ));
        }
        let key = session_id.get();
        let rx_index = unsafe { &mut *self.rx_index };
        let Some(index) = rx_index.lookup(&key) else {
            return Ok(());
        };
        let Some(op) = self.current_app_op else {
            return Ok(());
        };
        let rx = unsafe { &mut *self.rx };
        let app = unsafe { &mut *self.app };
        let buffers = self.buffers().clone();
        loop {
            let current = {
                let queue = rx
                    .get_mut(index)
                    .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
                queue.front().copied()
            };
            let Some(current) = current else {
                break;
            };
            if current.offset != 0 {
                break;
            }
            let delivered = app.complete_recv(op, buffers.clone(), current.index, current.fin)?;
            if !delivered {
                break;
            }
            let queue = rx
                .get_mut(index)
                .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
            let _ = queue
                .pop_front()
                .ok_or_else(|| CoreError::internal("session rx buffer is missing"))?;
            if queue.is_empty() {
                rx_index.remove(&key);
                let _ = rx.remove(index);
                break;
            }
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn aux_mut(&mut self) -> &mut A {
        unsafe { &mut *self.aux }
    }

    #[inline]
    pub(crate) const fn session_id(&self) -> SessionId {
        self.current_session_id
    }
}

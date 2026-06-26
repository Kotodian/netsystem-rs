use std::sync::Arc;

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::CoreResult;
use hammer_runtime::app::{AppOpId, SessionAppBoundary};

use crate::session::SessionId;
use hammer_infra::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionAppCloseSubmission {
    session_id: SessionId,
    op: AppOpId,
}

impl SessionAppCloseSubmission {
    #[inline]
    pub(crate) const fn new(session_id: SessionId, op: AppOpId) -> Self {
        Self { session_id, op }
    }

    #[inline]
    pub(crate) const fn session_id(self) -> SessionId {
        self.session_id
    }
}

#[derive(Debug)]
pub struct SessionAppRuntime {
    buffers: DataPlaneBuffers,
    boundary: Option<Arc<SessionAppBoundary>>,
}

impl SessionAppRuntime {
    #[inline]
    pub fn new(buffers: DataPlaneBuffers) -> Self {
        Self {
            buffers,
            boundary: None,
        }
    }

    #[inline]
    pub fn complete_recv(
        &self,
        _op: AppOpId,
        _buffers: DataPlaneBuffers,
        _index: BufferIndex,
        _fin: bool,
    ) -> CoreResult<bool> {
        let _ = (&self.buffers, &self.boundary);
        Ok(true)
    }

    #[inline]
    pub fn complete_closed(&self, _op: AppOpId) -> CoreResult<()> {
        Ok(())
    }

    #[inline]
    pub fn complete_connected(&self, _op: AppOpId) -> CoreResult<()> {
        Ok(())
    }

    pub fn drain_submissions(&mut self) -> CoreResult<()> {
        Ok(())
    }

    #[inline]
    pub(crate) fn take_drained_closes(&mut self) -> Vec<SessionAppCloseSubmission> {
        Vec::new()
    }

    pub(crate) fn release_pending_send_bytes(
        &mut self,
        _session_id: SessionId,
        _len: usize,
    ) -> CoreResult<bool> {
        Ok(false)
    }

    pub(crate) fn pending_send_len(&self, _session_id: SessionId) -> CoreResult<Option<usize>> {
        Ok(None)
    }

    #[inline]
    pub(crate) fn pending_send_head(&self, _session_id: SessionId) -> Option<BufferIndex> {
        None
    }

    #[inline]
    pub(crate) fn free_pending_send(&mut self, _session_id: SessionId) {}

    pub(crate) fn take_ready_tx_sessions(&mut self, _out: &mut Vec<SessionId>) {}

    pub(crate) fn take_ready_sessions(&mut self, _out: &mut Vec<SessionId>) {}

    #[inline]
    #[cfg(test)]
    pub(crate) fn has_pending_send(&self, _session_id: SessionId) -> bool {
        false
    }
}

impl Default for SessionAppRuntime {
    #[inline]
    fn default() -> Self {
        Self::new(DataPlaneBuffers::with_buffer_capacity(2048, 1))
    }
}

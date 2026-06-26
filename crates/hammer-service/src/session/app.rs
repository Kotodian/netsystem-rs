use std::sync::Arc;

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::svm_msg_q::SessionEvtType;
use hammer_infra::vec::Vec;
use hammer_runtime::app::AppSession;

use crate::session::{SessionId, SessionReadyQueue};

#[derive(Debug)]
pub struct SessionAppRuntime {
    buffers: DataPlaneBuffers,
    sessions: FlatHashTable<u64, Arc<AppSession>>,
    ready_tx: SessionReadyQueue,
    ready_rx: SessionReadyQueue,
}

impl SessionAppRuntime {
    #[inline]
    pub fn new(buffers: DataPlaneBuffers) -> Self {
        Self {
            buffers,
            sessions: FlatHashTable::new(),
            ready_tx: SessionReadyQueue::new(),
            ready_rx: SessionReadyQueue::new(),
        }
    }

    pub(crate) fn attach_session(&mut self, session_id: SessionId, session: Arc<AppSession>) {
        self.sessions.insert(session_id.get(), session);
    }

    pub(crate) fn detach_session(&mut self, session_id: SessionId) -> Option<Arc<AppSession>> {
        self.ready_tx.take(session_id);
        self.ready_rx.take(session_id);
        self.sessions.remove(&session_id.get())
    }

    pub(crate) fn connected(&self, session_id: SessionId) -> CoreResult<()> {
        let Some(session) = self.sessions.lookup(&session_id.get()) else {
            return Ok(());
        };
        session
            .push_event(SessionEvtType::Connect)
            .map_err(CoreError::from)
    }

    pub(crate) fn closed(&self, session_id: SessionId) -> CoreResult<()> {
        let Some(session) = self.sessions.lookup(&session_id.get()) else {
            return Ok(());
        };
        session
            .push_event(SessionEvtType::Close)
            .map_err(CoreError::from)
    }

    pub(crate) fn mark_tx_ready(&mut self, session_id: SessionId) {
        self.ready_tx.mark_ready(session_id);
    }

    pub(crate) fn mark_rx_ready(&mut self, session_id: SessionId) {
        self.ready_rx.mark_ready(session_id);
    }

    pub fn poll_tx_fifo_ready(&mut self) -> CoreResult<()> {
        for (key, session) in self.sessions.iter() {
            let session_id = SessionId::from_raw(key);
            if session.tx_fifo().max_dequeue() != 0 {
                self.ready_tx.mark_ready(session_id);
            }
        }
        Ok(())
    }

    pub(crate) fn release_pending_send_bytes(
        &mut self,
        session_id: SessionId,
        len: usize,
    ) -> CoreResult<bool> {
        let Some(session) = self.sessions.lookup(&session_id.get()) else {
            return Ok(false);
        };
        let dropped = session.drop_tx_acked(len).map_err(CoreError::from)?;
        Ok(dropped != 0)
    }

    pub(crate) fn pending_send_len(&self, session_id: SessionId) -> CoreResult<Option<usize>> {
        Ok(self
            .sessions
            .lookup(&session_id.get())
            .map(|session| session.tx_fifo().max_dequeue())
            .filter(|len| *len != 0))
    }

    #[inline]
    pub(crate) fn has_pending_send(&self, session_id: SessionId) -> bool {
        self.pending_send_len(session_id).ok().flatten().is_some()
    }

    pub(crate) fn copy_tx_to_buffer(
        &self,
        session_id: SessionId,
        tx_offset: usize,
        payload_len: usize,
        index: BufferIndex,
    ) -> CoreResult<()> {
        let session = self
            .sessions
            .lookup(&session_id.get())
            .ok_or_else(|| CoreError::internal("app session is missing"))?;
        let written = session
            .tx_fifo()
            .peek_slices(tx_offset, payload_len, |first, second| {
                if !first.is_empty() {
                    self.buffers.append(index, first)?;
                }
                if !second.is_empty() {
                    self.buffers.append(index, second)?;
                }
                CoreResult::Ok(first.len() + second.len())
            })
            .ok_or_else(|| CoreError::internal("app session tx fifo ended early"))??;
        if written != payload_len {
            return Err(CoreError::internal("app session tx fifo ended early"));
        }
        Ok(())
    }

    pub(crate) fn enqueue_rx(
        &self,
        session_id: SessionId,
        buffers: DataPlaneBuffers,
        index: BufferIndex,
    ) -> CoreResult<bool> {
        let Some(session) = self.sessions.lookup(&session_id.get()) else {
            return Ok(false);
        };
        let mut total = 0usize;
        let mut wrote = 0usize;
        let mut current = Some(index);
        while let Some(current_index) = current {
            let buffer = buffers.get_buffer(current_index)?;
            let chunk = buffer.current();
            total += chunk.len();
            if wrote == total - chunk.len() {
                wrote += session.enqueue_rx(chunk).map_err(CoreError::from)?;
            }
            current = buffers.buffers().next_buffer(current_index)?;
        }
        buffers.free_index(index);
        Ok(wrote == total)
    }

    pub(crate) fn free_pending_send(&mut self, session_id: SessionId) {
        if let Some(session) = self.sessions.lookup(&session_id.get()) {
            let _ = session.drop_tx_acked(session.tx_fifo().max_dequeue());
        }
    }

    pub(crate) fn take_ready_tx_sessions(&mut self) -> Vec<SessionId> {
        self.ready_tx.take_ready_sessions()
    }

    pub(crate) fn take_ready_sessions(&mut self) -> Vec<SessionId> {
        self.ready_rx.take_ready_sessions()
    }
}

impl Default for SessionAppRuntime {
    #[inline]
    fn default() -> Self {
        Self::new(DataPlaneBuffers::with_buffer_capacity(2048, 1))
    }
}

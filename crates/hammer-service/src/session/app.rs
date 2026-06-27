use std::sync::Arc;

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::map::FlatHashTable;
use hammer_infra::ring::LockFreeRing;
use hammer_infra::msg_queue::SessionEvtType;
use hammer_infra::vec::Vec;
use hammer_runtime::app::AppSession;

use crate::session::{SessionId, SessionReadyQueue};

#[derive(Debug)]
pub struct SessionAppRuntime {
    buffers: DataPlaneBuffers,
    sessions: FlatHashTable<u64, Arc<AppSession>>,
    sessions_by_index: Vec<Option<SessionId>>,
    tx_evt_q: Arc<LockFreeRing<u32>>,
}

impl SessionAppRuntime {
    #[inline]
    pub fn new(
        session_capacity: usize,
        buffers: DataPlaneBuffers,
        tx_evt_q: Arc<LockFreeRing<u32>>,
    ) -> Self {
        Self {
            buffers,
            sessions: FlatHashTable::new(),
            sessions_by_index: Vec::from_elem_copy(session_capacity, None),
            tx_evt_q,
        }
    }

    pub(crate) fn attach_session(&mut self, session_id: SessionId, session: Arc<AppSession>) {
        self.sessions.insert(session_id.get(), session);
        let slot = session_id.pool_index().slot() as usize;
        if let Some(entry) = self.sessions_by_index.get_mut(slot) {
            *entry = Some(session_id);
        }
    }

    pub(crate) fn detach_session(&mut self, session_id: SessionId) -> Option<Arc<AppSession>> {
        let slot = session_id.pool_index().slot() as usize;
        if let Some(entry) = self.sessions_by_index.get_mut(slot) {
            *entry = None;
        }
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
            .peek_segments(tx_offset, payload_len, |first, second| {
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
        for buffer in buffers.chain(index) {
            let buffer = buffer?;
            let chunk = buffer.current();
            total += chunk.len();
            if wrote == total - chunk.len() {
                wrote += session.enqueue_rx(chunk).map_err(CoreError::from)?;
            }
        }
        buffers.free_index(index);
        Ok(wrote == total)
    }

    pub(crate) fn free_pending_send(&mut self, session_id: SessionId) {
        if let Some(session) = self.sessions.lookup(&session_id.get()) {
            let _ = session.drop_tx_acked(session.tx_fifo().max_dequeue());
        }
    }

    pub(crate) fn drain_tx_events_to(&self, ready: &mut SessionReadyQueue) -> usize {
        let mut scheduled = 0usize;
        let mut batch = [0u32; 64];
        loop {
            let count = self.tx_evt_q.dequeue_batch(&mut batch);
            if count == 0 {
                break;
            }
            for session_index in batch[..count].iter().copied() {
                let Some(Some(session_id)) = self.sessions_by_index.get(session_index as usize)
                else {
                    continue;
                };
                let Some(session) = self.sessions.lookup(&session_id.get()) else {
                    continue;
                };
                session.clear_tx_event();
                ready.mark_ready(*session_id);
                scheduled += 1;
            }
            if count < batch.len() {
                break;
            }
        }
        scheduled
    }
}

impl Default for SessionAppRuntime {
    #[inline]
    fn default() -> Self {
        let tx_evt_q =
            Arc::new(LockFreeRing::with_capacity(2048).expect("default tx event queue capacity"));
        Self::new(
            1024,
            DataPlaneBuffers::with_buffer_capacity(2048, 1),
            tx_evt_q,
        )
    }
}

use std::fmt;
use std::sync::Arc;

use hammer_adapter::{BufferIndex, DataPlaneBuffers};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo::Fifo;
use hammer_infra::map::FlatHashTable;
use hammer_infra::msg_queue::{MsgQueue, SessionEvt, SessionEvtType};
use hammer_infra::segment::{Local, Segment, Svm};
use hammer_infra::vec::Vec;
use hammer_runtime::app::AppSession;

use crate::session::{SessionId, SessionReadyQueue};

pub struct SessionAppRuntime<S: Segment> {
    buffers: DataPlaneBuffers,
    sessions: FlatHashTable<u64, Arc<AppSession<S>>>,
    sessions_by_index: Vec<Option<SessionId>>,
    tx_evt_q: Arc<MsgQueue<S>>,
    worker_index: usize,
    seg: S,
}

impl<S: Segment> fmt::Debug for SessionAppRuntime<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionAppRuntime")
            .field("buffers", &self.buffers)
            .field("sessions", &self.sessions)
            .field("sessions_by_index", &self.sessions_by_index)
            .field("msg_queue_slots", &self.sessions_by_index.len())
            .finish_non_exhaustive()
    }
}

impl<S: Segment> SessionAppRuntime<S> {
    #[inline]
    pub fn new(
        session_capacity: usize,
        buffers: DataPlaneBuffers,
        tx_evt_q: Arc<MsgQueue<S>>,
        worker_index: usize,
        seg: S,
    ) -> Self {
        Self {
            buffers,
            sessions: FlatHashTable::new(),
            sessions_by_index: Vec::from_elem_copy(session_capacity, None),
            tx_evt_q,
            worker_index,
            seg,
        }
    }

    /// Shared runtime-side TX event queue.  Every app session created via
    /// `attach_session_local_with_runtime_tx` shares this same queue, so the
    /// dataplane side can drain all sessions' TX events from one ring.
    #[inline]
    pub fn tx_evt_q(&self) -> &Arc<MsgQueue<S>> {
        &self.tx_evt_q
    }

    pub fn attach_session(&mut self, session_id: SessionId, session: Arc<AppSession<S>>) {
        self.sessions.insert(session_id.get(), session);
        let slot = session_id.pool_index().slot() as usize;
        if let Some(entry) = self.sessions_by_index.get_mut(slot) {
            *entry = Some(session_id);
        }
    }

    pub(crate) fn detach_session(&mut self, session_id: SessionId) -> Option<Arc<AppSession<S>>> {
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
        let handle = session.session_handle();
        session
            .push_event(SessionEvtType::Connect)
            .map_err(CoreError::from)?;
        hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
            worker.wake_evt(handle);
        });
        Ok(())
    }

    pub(crate) fn closed(&self, session_id: SessionId) -> CoreResult<()> {
        let Some(session) = self.sessions.lookup(&session_id.get()) else {
            return Ok(());
        };
        let handle = session.session_handle();
        session
            .push_event(SessionEvtType::Close)
            .map_err(CoreError::from)?;
        hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
            worker.wake_evt(handle);
        });
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
        let handle = session.session_handle();
        let dropped = session.drop_tx_acked(len).map_err(CoreError::from)?;
        if dropped != 0 {
            hammer_runtime::app::with_current_app_worker(handle.worker_index() as usize, |worker| {
                worker.wake_tx(handle);
            });
        }
        Ok(dropped != 0)
    }

    pub fn pending_send_len(&self, session_id: SessionId) -> CoreResult<Option<usize>> {
        Ok(self
            .sessions
            .lookup(&session_id.get())
            .map(|session| session.tx_fifo().max_dequeue())
            .filter(|len| *len != 0))
    }

    pub(crate) fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        self.sessions
            .lookup(&session_id.get())
            .map(|session| session.rx_fifo().max_enqueue())
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

    pub(crate) fn copy_rx_from_buffer(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
    ) -> CoreResult<usize> {
        let handle = self.sessions.lookup(&session_id.get()).map(|s| s.session_handle());
        let wrote = match handle {
            Some(h) => {
                let Some(session) = self.sessions.lookup(&session_id.get()) else {
                    return Ok(0);
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
                if wrote > 0 {
                    hammer_runtime::app::with_current_app_worker(
                        h.worker_index() as usize,
                        |worker| {
                            worker.wake_rx(h);
                        },
                    );
                }
                wrote
            }
            None => 0,
        };
        Ok(wrote)
    }

    pub(crate) fn copy_rx_from_buffer_ooo(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
        offset: u32,
    ) -> CoreResult<(u32, Option<u32>, u32)> {
        let Some(session) = self.sessions.lookup(&session_id.get()) else {
            buffers.free_index(index);
            return Ok((0, None, 0));
        };
        let mut total_len = 0u32;
        let mut delivered = 0u32;
        for buf in buffers.chain(index) {
            let buf = buf?;
            let current = buf.current();
            let chunk_offset = offset
                .checked_add(total_len)
                .ok_or_else(|| CoreError::internal("ooo rx offset overflow"))?;
            let result = session
                .rx_fifo()
                .enqueue_ooo(chunk_offset, current)
                .map_err(|_| CoreError::internal("ooo enqueue failed"))?;
            delivered = delivered.wrapping_add(result.delivered);
            total_len = total_len
                .checked_add(current.len() as u32)
                .ok_or_else(|| CoreError::internal("ooo rx buffer length overflow"))?;
        }
        buffers.free_index(index);
        Ok((delivered, Some(offset), total_len))
    }

    pub(crate) fn free_pending_send(&mut self, session_id: SessionId) {
        if let Some(session) = self.sessions.lookup(&session_id.get()) {
            let _ = session.drop_tx_acked(session.tx_fifo().max_dequeue());
        }
    }

    pub(crate) fn drain_tx_events_to(&self, ready: &mut SessionReadyQueue) -> usize {
        let mut scheduled = 0usize;
        let mut batch = [SessionEvt {
            session_index: 0,
            evt_type: SessionEvtType::Connect,
        }; 64];
        loop {
            self.tx_evt_q.drain();
            let count = self.tx_evt_q.dequeue_batch(&mut batch);
            if count == 0 {
                break;
            }
            for evt in batch[..count].iter() {
                let Some(Some(session_id)) = self.sessions_by_index.get(evt.session_index as usize)
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

impl SessionAppRuntime<Local> {
    #[inline]
    pub fn default_local() -> Self {
        let tx_evt_q = Arc::new(
            MsgQueue::<Local>::with_capacity(2048).expect("default tx event queue capacity"),
        );
        Self::new(
            1024,
            DataPlaneBuffers::with_buffer_capacity(2048, 1),
            tx_evt_q,
            0,
            Local::default(),
        )
    }

    pub(crate) fn create_app_session(
        &self,
        handle: hammer_runtime::app::SessionHandle,
        config: hammer_runtime::app::AppSessionConfig,
        tx_evt_q: Arc<MsgQueue<Local>>,
    ) -> CoreResult<Arc<AppSession<Local>>> {
        hammer_runtime::app::with_current_app_worker(self.worker_index, |worker| {
            worker
                .attach_session_local_with_runtime_tx(handle, config, tx_evt_q)
                .map_err(CoreError::from)
        })
    }

    fn notify_rx(&self, session: &AppSession<Local>) {
        let handle = session.session_handle();
        hammer_runtime::app::with_current_app_worker(self.worker_index, |w| w.wake_rx(handle));
    }

    fn notify_tx(&self, session: &AppSession<Local>) {
        let handle = session.session_handle();
        hammer_runtime::app::with_current_app_worker(self.worker_index, |w| w.wake_tx(handle));
    }

    fn notify_evt(&self, session: &AppSession<Local>) {
        let handle = session.session_handle();
        hammer_runtime::app::with_current_app_worker(self.worker_index, |w| w.wake_evt(handle));
    }
}

impl SessionAppRuntime<Svm> {
    pub(crate) fn create_app_session(
        &self,
        handle: hammer_runtime::app::SessionHandle,
        config: hammer_runtime::app::AppSessionConfig,
        tx_evt_q: Arc<MsgQueue<Svm>>,
    ) -> CoreResult<Arc<AppSession<Svm>>> {
        let rx_fifo = Arc::new(
            Fifo::<Svm>::new(self.seg.clone(), config.fifo_capacity)
                .map_err(|e| CoreError::internal(format!("svm rx fifo: {e:?}")))?,
        );
        let tx_fifo = Arc::new(
            Fifo::<Svm>::new(self.seg.clone(), config.fifo_capacity)
                .map_err(|e| CoreError::internal(format!("svm tx fifo: {e:?}")))?,
        );
        let evt_q_ring = config
            .evt_q_capacity
            .saturating_add(1)
            .next_power_of_two()
            .max(2);
        let evt_q = Arc::new(
            MsgQueue::<Svm>::new(self.seg.clone(), evt_q_ring)
                .map_err(|e| CoreError::internal(format!("svm evt_q: {e:?}")))?,
        );

        let session = Arc::new(AppSession::<Svm>::from_parts(
            rx_fifo, tx_fifo, evt_q, tx_evt_q, handle,
        ));
        Ok(session)
    }

    fn notify_rx(&self, session: &AppSession<Svm>) {
        session.evt_q().fire();
    }

    fn notify_tx(&self, session: &AppSession<Svm>) {
        session.tx_evt_q().fire();
    }

    fn notify_evt(&self, session: &AppSession<Svm>) {
        session.evt_q().fire();
    }
}

use std::fmt;
use std::sync::Arc;

use hammer_core::data_plane::{DataPlaneBuffers, Index};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::{Local, Segment, Svm};
use hammer_runtime::app::{
    AppSession, SessionEventQueue, SessionEvt, SessionEvtType, SessionMsgQueue, SessionSegment,
};

use crate::session::SessionId;

#[derive(Debug, thiserror::Error)]
enum SessionAppRuntimeError {
    #[error("session TX event queue allocation failed")]
    TxEventQueue,
}

impl From<SessionAppRuntimeError> for CoreError {
    fn from(error: SessionAppRuntimeError) -> Self {
        CoreError::lifecycle("session app", error.to_string())
    }
}

#[inline]
fn checked_add_ooo_accepted(total: u32, accepted: u32) -> CoreResult<u32> {
    total
        .checked_add(accepted)
        .ok_or_else(|| CoreError::internal("ooo rx accepted length overflow"))
}

pub struct SessionAppRuntime<S: SessionSegment> {
    buffers: DataPlaneBuffers,
    session_slots: Vec<Option<(SessionId, Arc<AppSession<S>>)>>,
    tx_evt_q: Arc<S::EventQueue>,
    worker_index: usize,
    seg: S,
}

impl<S: SessionSegment> fmt::Debug for SessionAppRuntime<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SessionAppRuntime")
            .field("buffers", &self.buffers)
            .field("session_slots", &self.session_slots)
            .field("msg_queue_slots", &self.session_slots.len())
            .finish_non_exhaustive()
    }
}

impl<S: SessionSegment> SessionAppRuntime<S> {
    #[inline]
    pub fn new(
        session_capacity: usize,
        buffers: DataPlaneBuffers,
        tx_evt_q: Arc<S::EventQueue>,
        worker_index: usize,
        seg: S,
    ) -> Self {
        Self {
            buffers,
            session_slots: vec![None; session_capacity],
            tx_evt_q,
            worker_index,
            seg,
        }
    }

    /// Shared runtime-side TX event queue.  Every app session created via
    /// `attach_session_local_with_runtime_tx` shares this same queue, so the
    /// dataplane side can drain all sessions' TX events from one ring.
    #[inline]
    pub fn tx_evt_q(&self) -> &Arc<S::EventQueue> {
        &self.tx_evt_q
    }

    pub fn attach_session(&mut self, session_id: SessionId, session: Arc<AppSession<S>>) {
        let slot = session_id.pool_index().slot() as usize;
        self.session_slots[slot] = Some((session_id, session));
    }

    pub fn detach_session(&mut self, session_id: SessionId) -> Option<Arc<AppSession<S>>> {
        let slot = session_id.pool_index().slot() as usize;
        let entry = self.session_slots.get_mut(slot)?;
        if entry
            .as_ref()
            .is_some_and(|(stored, _)| *stored == session_id)
        {
            return entry.take().map(|(_, session)| session);
        }
        None
    }

    #[inline(always)]
    fn session(&self, session_id: SessionId) -> Option<&Arc<AppSession<S>>> {
        let slot = session_id.pool_index().slot() as usize;
        self.session_slots
            .get(slot)?
            .as_ref()
            .and_then(|(stored, session)| (*stored == session_id).then_some(session))
    }

    pub fn connected(&self, session_id: SessionId) -> CoreResult<()>
    where
        Self: SessionAppRuntimeCreate<S>,
    {
        let Some(session) = self.session(session_id) else {
            return Ok(());
        };
        session
            .push_event(SessionEvtType::Connect)
            .map_err(CoreError::from)?;
        self.notify_evt(&session);
        Ok(())
    }

    pub fn closed(&self, session_id: SessionId) -> CoreResult<()>
    where
        Self: SessionAppRuntimeCreate<S>,
    {
        let Some(session) = self.session(session_id) else {
            return Ok(());
        };
        session
            .push_event(SessionEvtType::Close)
            .map_err(CoreError::from)?;
        self.notify_evt(&session);
        Ok(())
    }

    pub fn discard_acked_tx_bytes(&mut self, session_id: SessionId, len: usize) -> CoreResult<bool>
    where
        Self: SessionAppRuntimeCreate<S>,
    {
        let Some(session) = self.session(session_id) else {
            return Ok(false);
        };
        let dropped = session.drop_tx_acked(len).map_err(CoreError::from)?;
        if dropped != 0 {
            self.notify_tx(&session);
        }
        Ok(dropped != 0)
    }

    pub fn pending_send_len(&self, session_id: SessionId) -> CoreResult<Option<usize>> {
        Ok(self
            .session(session_id)
            .map(|session| session.tx_fifo().max_dequeue())
            .filter(|len| *len != 0))
    }

    pub fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        self.session(session_id)
            .map(|session| session.rx_fifo().max_enqueue())
    }

    #[inline]
    pub fn has_pending_send(&self, session_id: SessionId) -> bool {
        self.pending_send_len(session_id).ok().flatten().is_some()
    }

    pub fn copy_tx_to_buffer(
        &self,
        session_id: SessionId,
        tx_offset: usize,
        payload_len: usize,
        index: Index,
    ) -> CoreResult<()> {
        let session = self
            .session(session_id)
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

    pub fn copy_rx_from_buffer(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: Index,
        urgent: bool,
    ) -> CoreResult<(u32, u32)>
    where
        Self: SessionAppRuntimeCreate<S>,
    {
        let Some(session) = self.session(session_id) else {
            return Ok((0, 0));
        };
        let mut total = 0u32;
        let mut accepted = 0u32;
        let mut promoted = 0u32;
        let mut urgent_pending = urgent;
        for buffer in buffers.chain(index) {
            let buffer = buffer?;
            let chunk = buffer.current();
            let chunk_len = u32::try_from(chunk.len())
                .map_err(|_| CoreError::internal("rx buffer length overflow"))?;
            if accepted == total {
                let rx_available_before = session.rx_fifo().max_enqueue();
                let flags = if urgent_pending {
                    urgent_pending = false;
                    hammer_runtime::app::SessionEvtFlags::URGENT
                } else {
                    hammer_runtime::app::SessionEvtFlags::empty()
                };
                let wrote = session
                    .enqueue_rx_with_flags(chunk, flags)
                    .map_err(CoreError::from)?;
                let accepted_now = wrote.min(chunk.len()).min(rx_available_before);
                let promoted_now = wrote.saturating_sub(accepted_now);
                accepted = accepted
                    .checked_add(accepted_now as u32)
                    .ok_or_else(|| CoreError::internal("rx accepted length overflow"))?;
                promoted = promoted
                    .checked_add(promoted_now as u32)
                    .ok_or_else(|| CoreError::internal("rx promoted length overflow"))?;
            }
            total = total
                .checked_add(chunk_len)
                .ok_or_else(|| CoreError::internal("rx buffer length overflow"))?;
        }
        if accepted != 0 || promoted != 0 {
            self.notify_rx(session);
        }
        Ok((accepted, promoted))
    }

    pub fn copy_rx_from_buffer_ooo(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: Index,
        offset: u32,
    ) -> CoreResult<(u32, Option<(u32, u32)>)> {
        let Some(session) = self.session(session_id) else {
            return Ok((0, None));
        };
        let mut total_len = 0u32;
        let mut accepted = 0u32;
        let mut newest_start: Option<u32> = None;
        let mut newest_end: Option<u32> = None;
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
            accepted = checked_add_ooo_accepted(accepted, result.accepted)?;
            if let Some(start) = result.start {
                let end = start
                    .checked_add(result.len)
                    .ok_or_else(|| CoreError::internal("ooo span end overflow"))?;
                newest_start = Some(match newest_start {
                    Some(existing) => existing.min(start),
                    None => start,
                });
                newest_end = Some(match newest_end {
                    Some(existing) => existing.max(end),
                    None => end,
                });
            }
            total_len = total_len
                .checked_add(current.len() as u32)
                .ok_or_else(|| CoreError::internal("ooo rx buffer length overflow"))?;
        }
        let newest = match (newest_start, newest_end) {
            (Some(start), Some(end)) => Some((
                start,
                end.checked_sub(start)
                    .ok_or_else(|| CoreError::internal("ooo span length underflow"))?,
            )),
            _ => None,
        };
        Ok((accepted, newest))
    }

    pub fn discard_all_tx_bytes_for_session(&mut self, session_id: SessionId) {
        if let Some(session) = self.session(session_id) {
            let _ = session.drop_tx_acked(session.tx_fifo().max_dequeue());
        }
    }

    pub fn drain_tx_events_to(
        &self,
        mut dispatch_event: impl FnMut(SessionId, SessionEvtType),
    ) -> usize {
        let mut scheduled = 0usize;
        let mut batch = [SessionEvt::io(0, SessionEvtType::Connect); 64];
        let worker_index = self.worker_index as u32;
        loop {
            self.tx_evt_q.drain();
            let count = self.tx_evt_q.dequeue_batch(&mut batch);
            if count == 0 {
                break;
            }
            for evt in batch[..count].iter() {
                if evt.evt_type == SessionEvtType::Close && evt.worker_index() != worker_index {
                    continue;
                }
                let Some(Some((session_id, session))) =
                    self.session_slots.get(evt.session_index() as usize)
                else {
                    continue;
                };
                if evt.evt_type == SessionEvtType::TxDeq {
                    session.clear_tx_event();
                    scheduled += 1;
                }
                dispatch_event(*session_id, evt.evt_type);
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
    pub fn default_local() -> CoreResult<Self> {
        let tx_evt_q: Arc<SessionMsgQueue> = Arc::new(
            SessionMsgQueue::with_cfg(2048, 1024)
                .map_err(|_| SessionAppRuntimeError::TxEventQueue)?,
        );
        let mut config = hammer_core::config::Config::default();
        config.worker.buffer.slot_bytes = 2048;
        config.worker.buffer.slots_per_numa = 1;
        config.worker.buffer.frame_pool_size =
            hammer_core::data_plane::DEFAULT_BUFFER_FRAME_POOL_SIZE;
        let runtime = hammer_runtime::new_worker_runtime(&config)?;
        Ok(Self::new(
            1024,
            runtime.buffers().clone(),
            tx_evt_q,
            0,
            Local::default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::pool::Index as PoolIndex;
    use hammer_runtime::app::{AppSessionConfig, SessionEvt, SessionEvtType, SessionHandle};
    use std::sync::Mutex;

    #[test]
    fn checked_add_ooo_accepted_rejects_overflow() {
        let error =
            checked_add_ooo_accepted(u32::MAX, 1).expect_err("overflow must return an error");
        assert_eq!(error.to_string(), "ooo rx accepted length overflow");
    }

    #[test]
    fn checked_add_ooo_accepted_accumulates_without_overflow() {
        assert_eq!(
            checked_add_ooo_accepted(7, 11).expect("sum without overflow"),
            18
        );
    }

    #[test]
    fn drain_drops_events_for_unmapped_session_slot() {
        let app = SessionAppRuntime::<Local>::default_local().expect("app runtime");
        app.tx_evt_q()
            .enqueue_ctrl(SessionEvt::ctrl(3, 0, SessionEvtType::Close))
            .expect("enqueue close");
        app.tx_evt_q()
            .enqueue_io(SessionEvt::io(3, SessionEvtType::TxDeq))
            .expect("enqueue txdeq");

        let dispatched = Mutex::new(Vec::new());
        let scheduled = app.drain_tx_events_to(|session_id, evt_type| {
            dispatched
                .lock()
                .expect("dispatched")
                .push((session_id.get(), evt_type));
        });

        assert_eq!(scheduled, 0);
        assert!(dispatched.lock().expect("dispatched").is_empty());
    }

    #[test]
    fn drain_drops_close_when_worker_index_mismatches() {
        let mut app = SessionAppRuntime::<Local>::default_local().expect("app runtime");
        let session_id = SessionId::from(PoolIndex::new(5, 1));
        let session = Arc::new(
            AppSession::new_in_segment(
                Local::default(),
                AppSessionConfig::new(64, 4),
                SessionHandle::new(5, 0),
                app.tx_evt_q().clone(),
            )
            .expect("app session"),
        );
        app.attach_session(session_id, session);
        app.tx_evt_q()
            .enqueue_ctrl(SessionEvt::ctrl(5, 9, SessionEvtType::Close))
            .expect("enqueue close for wrong worker");

        let dispatched = Mutex::new(Vec::new());
        let scheduled = app.drain_tx_events_to(|session_id, evt_type| {
            dispatched
                .lock()
                .expect("dispatched")
                .push((session_id.get(), evt_type));
        });

        assert_eq!(scheduled, 0);
        assert!(dispatched.lock().expect("dispatched").is_empty());
    }

    #[test]
    fn drain_dispatches_close_with_matching_session_handle() {
        let mut app = SessionAppRuntime::<Local>::default_local().expect("app runtime");
        let session_id = SessionId::from(PoolIndex::new(5, 1));
        let session = Arc::new(
            AppSession::new_in_segment(
                Local::default(),
                AppSessionConfig::new(64, 4),
                SessionHandle::new(5, 0),
                app.tx_evt_q().clone(),
            )
            .expect("app session"),
        );
        app.attach_session(session_id, session);
        app.tx_evt_q()
            .enqueue_ctrl(SessionEvt::ctrl(5, 0, SessionEvtType::Close))
            .expect("enqueue close");

        let dispatched = Mutex::new(Vec::new());
        app.drain_tx_events_to(|id, evt_type| {
            dispatched
                .lock()
                .expect("dispatched")
                .push((id.get(), evt_type));
        });

        assert_eq!(
            *dispatched.lock().expect("dispatched"),
            vec![(session_id.get(), SessionEvtType::Close)]
        );
    }
}

impl SessionAppRuntime<Svm> {}

pub trait SessionAppRuntimeCreate<S: SessionSegment> {
    fn create_app_session(
        &self,
        handle: hammer_runtime::app::SessionHandle,
        config: hammer_runtime::app::AppSessionConfig,
        tx_evt_q: Arc<S::EventQueue>,
    ) -> CoreResult<Arc<AppSession<S>>>;

    fn notify_rx(&self, session: &AppSession<S>);
    fn notify_tx(&self, session: &AppSession<S>);
    fn notify_evt(&self, session: &AppSession<S>);
}

impl SessionAppRuntimeCreate<Local> for SessionAppRuntime<Local> {
    fn create_app_session(
        &self,
        handle: hammer_runtime::app::SessionHandle,
        config: hammer_runtime::app::AppSessionConfig,
        tx_evt_q: Arc<SessionMsgQueue>,
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

impl SessionAppRuntimeCreate<Svm> for SessionAppRuntime<Svm> {
    fn create_app_session(
        &self,
        handle: hammer_runtime::app::SessionHandle,
        config: hammer_runtime::app::AppSessionConfig,
        tx_evt_q: Arc<SessionMsgQueue<Svm>>,
    ) -> CoreResult<Arc<AppSession<Svm>>> {
        let rx_fifo = Arc::new(
            Fifo::<Svm>::new(self.seg.clone(), config.fifo_capacity)
                .map_err(|e| CoreError::internal(format!("svm rx fifo: {e:?}")))?,
        );
        let tx_fifo = Arc::new(
            Fifo::<Svm>::new(self.seg.clone(), config.fifo_capacity)
                .map_err(|e| CoreError::internal(format!("svm tx fifo: {e:?}")))?,
        );
        let ring_nitems = config.evt_q_capacity.max(1) as u32;
        let q_nitems = (config.evt_q_capacity + 1).next_power_of_two().max(2) as u32;
        let layout = SessionMsgQueue::<Svm>::layout_bytes(q_nitems, ring_nitems)
            .map_err(|_| CoreError::internal("invalid svm evt_q layout"))?;
        let evt_off = self.seg.alloc(layout, 64);
        let evt_q = Arc::new(
            unsafe {
                SessionMsgQueue::<Svm>::init_at(self.seg.clone(), evt_off, q_nitems, ring_nitems)
            }
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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Weak};

use hammer_core::data_plane::{DataPlaneBuffers, Index};
use hammer_infra::align::align_up;
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, AppSessionConfig, AppSessionError, SessionEventQueue, SessionEvt, SessionEvtType,
    SessionHandle, SessionMsgQueue, SessionMsgQueueError, SessionOffsets,
};
use hammer_runtime::attach::{AppSessionPublication, AppSessionPublisher};
use hammer_runtime::{RuntimeError, RuntimeResult};

use crate::session::SessionId;
use crate::session::error::SessionError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AppWorkerError {
    #[error("session TX event queue configuration rejected: {error:?}")]
    TxEventQueue { error: SessionMsgQueueError },
    #[error("session TX event queue layout exceeds addressable memory")]
    TxEventQueueLayoutOverflow,
    #[error("session TX event queue segment capacity exhausted")]
    TxEventQueueSegmentExhausted,
    #[error("failed to create shared listener session segment")]
    SessionSegment {
        #[source]
        source: std::io::Error,
    },
    #[error("session segment layout exceeds addressable memory")]
    SessionSegmentSizeOverflow,
    #[error("listener segment cannot hold the session layout")]
    SessionSegmentExhausted,
}

impl From<AppWorkerError> for RuntimeError {
    fn from(error: AppWorkerError) -> Self {
        RuntimeError::subsystem("session app", error)
    }
}

pub struct AppWorker {
    /// Hot per-packet lookup array: 16 bytes per occupied slot so
    /// `session()`/`drain_tx_events_to` stay cache-dense. Cold attach
    /// metadata lives in `attach_slots` at the same index.
    session_slots: Vec<Option<SessionSlot>>,
    /// Cold accept/detach-only metadata; never read on the packet path.
    attach_slots: Vec<AttachSlot>,
    tx_evt_q: Arc<SessionMsgQueue>,
    worker_index: usize,
    listeners: HashMap<u32, SegmentManager>,
    attach: Option<AppWorkerAttach>,
}

#[derive(Clone)]
struct SessionSlot {
    session_id: SessionId,
    session: Arc<AppSession>,
}

#[derive(Clone, Default)]
struct AttachSlot {
    listener: Option<u32>,
    publication: Option<AppSessionPublication>,
}

pub(crate) struct AppWorkerAttach {
    publisher: AppSessionPublisher,
    tx_event_segment: Segment,
    tx_event_offset: u64,
}

impl AppWorkerAttach {
    pub(crate) fn new(
        publisher: AppSessionPublisher,
        tx_event_segment: Segment,
        tx_event_offset: u64,
    ) -> Self {
        Self {
            publisher,
            tx_event_segment,
            tx_event_offset,
        }
    }
}

struct SegmentManager {
    segments: Vec<Option<Segment>>,
    allocations: HashMap<u64, SessionAllocation>,
    retired: Vec<SessionAllocation>,
    shared_name_prefix: Option<String>,
    next_segment: usize,
}

struct CreatedAppSession {
    session: Arc<AppSession>,
    segment: Segment,
    offsets: SessionOffsets,
}

struct SessionAllocation {
    segment: usize,
    offset: u64,
    bytes: usize,
    session: Weak<AppSession>,
}

impl SegmentManager {
    fn new(shared_name_prefix: Option<String>) -> Self {
        Self {
            segments: Vec::new(),
            allocations: HashMap::new(),
            retired: Vec::new(),
            shared_name_prefix,
            next_segment: 0,
        }
    }

    fn create_session(
        &mut self,
        handle: SessionHandle,
        config: AppSessionConfig,
        tx_evt_q: Arc<SessionMsgQueue>,
        tx_event_offset: u64,
    ) -> RuntimeResult<CreatedAppSession> {
        let mut index = 0;
        while index < self.retired.len() {
            if self.retired[index].session.strong_count() == 0 {
                let allocation = self.retired.swap_remove(index);
                self.deallocate(allocation);
            } else {
                index += 1;
            }
        }

        let fifo_bytes = align_up(
            Fifo::layout_bytes(config.fifo_capacity).map_err(|_| {
                AppSessionError::RxFifoCapacityInvalid {
                    capacity: config.fifo_capacity,
                }
            })?,
            64,
        );
        let ring_nitems = config.evt_q_capacity.max(1) as u32;
        let q_nitems = (config.evt_q_capacity + 1).next_power_of_two().max(2) as u32;
        let event_queue_bytes = align_up(
            SessionMsgQueue::layout_bytes(q_nitems, ring_nitems).map_err(|_| {
                AppSessionError::EventQueueCapacityInvalid {
                    capacity: config.evt_q_capacity,
                }
            })?,
            64,
        );
        let session_bytes = fifo_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(event_queue_bytes))
            .ok_or(AppWorkerError::SessionSegmentSizeOverflow)?;
        let allocation = self
            .segments
            .iter()
            .enumerate()
            .find_map(|(segment, entry)| {
                entry.as_ref().and_then(|storage| {
                    storage
                        .alloc(session_bytes, 64)
                        .map(|offset| (segment, offset, storage.clone()))
                })
            });
        let (segment_index, offset, segment) = match allocation {
            Some(allocation) => allocation,
            None => {
                let segment_bytes = (1024 * 1024).max(
                    session_bytes
                        .checked_add(128)
                        .ok_or(AppWorkerError::SessionSegmentSizeOverflow)?,
                );
                let segment = if let Some(prefix) = &self.shared_name_prefix {
                    let name = format!("{prefix}-{}", self.next_segment);
                    self.next_segment += 1;
                    Segment::shared(&name, segment_bytes)
                        .map_err(|source| AppWorkerError::SessionSegment { source })?
                } else {
                    Segment::local(segment_bytes)
                };
                let offset = segment
                    .alloc(session_bytes, 64)
                    .ok_or(AppWorkerError::SessionSegmentExhausted)?;
                let segment_index = match self.segments.iter().position(Option::is_none) {
                    Some(index) => {
                        self.segments[index] = Some(segment.clone());
                        index
                    }
                    None => {
                        self.segments.push(Some(segment.clone()));
                        self.segments.len() - 1
                    }
                };
                (segment_index, offset, segment)
            }
        };
        let tx_fifo_offset = offset + fifo_bytes as u64;
        let event_queue_offset = tx_fifo_offset + fifo_bytes as u64;
        let session = (|| {
            let mut rx_fifo =
                unsafe { Fifo::init_at(segment.clone(), offset, config.fifo_capacity) }.map_err(
                    |_| AppSessionError::RxFifoCapacityInvalid {
                        capacity: config.fifo_capacity,
                    },
                )?;
            rx_fifo.enable_ooo();
            let tx_fifo =
                unsafe { Fifo::init_at(segment.clone(), tx_fifo_offset, config.fifo_capacity) }
                    .map_err(|_| AppSessionError::TxFifoCapacityInvalid {
                        capacity: config.fifo_capacity,
                    })?;
            let event_queue = unsafe {
                SessionMsgQueue::init_at_with_signal(
                    segment.clone(),
                    event_queue_offset,
                    q_nitems,
                    ring_nitems,
                )
            }
            .map_err(|_| AppSessionError::EventQueueCapacityInvalid {
                capacity: config.evt_q_capacity,
            })?;
            Ok::<_, AppSessionError>(Arc::new(AppSession::from_parts(
                Arc::new(rx_fifo),
                Arc::new(tx_fifo),
                Arc::new(event_queue),
                tx_evt_q,
                handle,
            )))
        })();
        let session = match session {
            Ok(session) => session,
            Err(error) => {
                segment.free(offset, session_bytes);
                return Err(error.into());
            }
        };
        self.allocations.insert(
            handle.raw(),
            SessionAllocation {
                segment: segment_index,
                offset,
                bytes: session_bytes,
                session: Arc::downgrade(&session),
            },
        );
        Ok(CreatedAppSession {
            session,
            segment,
            offsets: SessionOffsets {
                rx_fifo_off: offset,
                tx_fifo_off: tx_fifo_offset,
                evt_q_off: event_queue_offset,
                tx_evt_q_off: tx_event_offset,
            },
        })
    }

    fn release_session(&mut self, handle: hammer_runtime::app::SessionHandle) {
        let Some(allocation) = self.allocations.remove(&handle.raw()) else {
            return;
        };
        if allocation.session.strong_count() == 0 {
            self.deallocate(allocation);
        } else {
            self.retired.push(allocation);
        }
    }

    fn deallocate(&mut self, allocation: SessionAllocation) {
        let segment_index = allocation.segment;
        let entry = self.segments[segment_index]
            .as_ref()
            .expect("session allocation refers to a live listener segment");
        entry.free(allocation.offset, allocation.bytes);
        let segment_in_use = self
            .allocations
            .values()
            .chain(self.retired.iter())
            .any(|allocation| allocation.segment == segment_index);
        if segment_index != 0 && !segment_in_use {
            self.segments[segment_index] = None;
        }
    }
}

impl fmt::Debug for AppWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AppWorker")
            .field("session_capacity", &self.session_slots.len())
            .field(
                "active_sessions",
                &self.session_slots.iter().flatten().count(),
            )
            .finish_non_exhaustive()
    }
}

impl AppWorker {
    #[inline]
    pub fn new(
        session_capacity: usize,
        tx_evt_q: Arc<SessionMsgQueue>,
        worker_index: usize,
    ) -> Self {
        Self {
            session_slots: vec![None; session_capacity],
            attach_slots: vec![AttachSlot::default(); session_capacity],
            tx_evt_q,
            worker_index,
            listeners: HashMap::new(),
            attach: None,
        }
    }

    pub(crate) fn with_attach(
        session_capacity: usize,
        tx_evt_q: Arc<SessionMsgQueue>,
        worker_index: usize,
        attach: AppWorkerAttach,
    ) -> Self {
        Self {
            session_slots: vec![None; session_capacity],
            attach_slots: vec![AttachSlot::default(); session_capacity],
            tx_evt_q,
            worker_index,
            listeners: HashMap::new(),
            attach: Some(attach),
        }
    }

    /// Shared runtime-side TX event queue.  Every app session created via
    /// `attach_session_local_with_runtime_tx` shares this same queue, so the
    /// dataplane side can drain all sessions' TX events from one ring.
    #[inline]
    pub fn tx_evt_q(&self) -> &Arc<SessionMsgQueue> {
        &self.tx_evt_q
    }

    pub fn attach_session(&mut self, session_id: SessionId, session: Arc<AppSession>) {
        let slot = session_id.pool_index().slot() as usize;
        self.session_slots[slot] = Some(SessionSlot {
            session_id,
            session,
        });
    }

    pub fn detach_session(&mut self, session_id: SessionId) -> Option<Arc<AppSession>> {
        let index = session_id.pool_index().slot() as usize;
        let entry = self.session_slots.get_mut(index)?;
        if entry
            .as_ref()
            .is_some_and(|slot| slot.session_id == session_id)
        {
            let slot = entry.take()?;
            let attach_slot = std::mem::take(&mut self.attach_slots[index]);
            if let Some(listener) = attach_slot.listener
                && let Some(manager) = self.listeners.get_mut(&listener)
            {
                manager.release_session(slot.session.session_handle());
            }
            return Some(slot.session);
        }
        None
    }

    #[inline(always)]
    fn session(&self, session_id: SessionId) -> Option<&Arc<AppSession>> {
        self.session_slots
            .get(session_id.pool_index().slot() as usize)?
            .as_ref()
            .and_then(|slot| (slot.session_id == session_id).then_some(&slot.session))
    }

    pub fn connected(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        let index = session_id.pool_index().slot() as usize;
        let Some(session) = self.session(session_id).cloned() else {
            return Ok(());
        };
        session
            .push_event(SessionEvtType::Connect)
            .map_err(RuntimeError::from)?;
        self.notify_evt(&session);
        let attach_slot = &mut self.attach_slots[index];
        if let (Some(publication), Some(attach)) = (attach_slot.publication.as_ref(), &self.attach)
        {
            attach.publisher.try_publish(publication)?;
            attach_slot.publication = None;
        }
        Ok(())
    }

    pub fn closed(&self, session_id: SessionId) -> RuntimeResult<()> {
        let Some(session) = self.session(session_id) else {
            return Ok(());
        };
        session
            .push_event(SessionEvtType::Close)
            .map_err(RuntimeError::from)?;
        self.notify_evt(&session);
        Ok(())
    }

    pub fn discard_acked_tx_bytes(
        &mut self,
        session_id: SessionId,
        len: usize,
    ) -> RuntimeResult<bool> {
        let Some(session) = self.session(session_id) else {
            return Ok(false);
        };
        let dropped = session.drop_tx_acked(len).map_err(RuntimeError::from)?;
        if dropped != 0 {
            self.notify_tx(&session);
        }
        Ok(dropped != 0)
    }

    pub fn pending_send_len(&self, session_id: SessionId) -> RuntimeResult<Option<usize>> {
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
    pub(crate) fn request_rx_dequeue_notification(&self, session_id: SessionId) {
        if let Some(session) = self.session(session_id) {
            session.rx_fifo().want_deq_notification();
        }
    }

    #[inline]
    pub fn has_pending_send(&self, session_id: SessionId) -> bool {
        self.pending_send_len(session_id).ok().flatten().is_some()
    }

    pub fn copy_tx_to_buffer(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        tx_offset: usize,
        payload_len: usize,
        index: Index,
    ) -> RuntimeResult<()> {
        let session = self
            .session(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        let written = session
            .tx_fifo()
            .peek_segments(tx_offset, payload_len, |first, second| {
                if !first.is_empty() {
                    buffers.append(index, first)?;
                }
                if !second.is_empty() {
                    buffers.append(index, second)?;
                }
                RuntimeResult::Ok(first.len() + second.len())
            })
            .ok_or(SessionError::TxFifoRangeInvalid {
                session_id,
                tx_offset,
                payload_len,
            })??;
        if written != payload_len {
            return Err(SessionError::TxFifoRangeInvalid {
                session_id,
                tx_offset,
                payload_len,
            }
            .into());
        }
        Ok(())
    }

    pub fn copy_rx_from_buffer(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: Index,
        urgent: bool,
    ) -> RuntimeResult<(u32, u32)> {
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
                .map_err(|_| SessionError::RxLengthOverflow { session_id })?;
            if accepted == total {
                let rx_available_before = session.rx_fifo().max_enqueue();
                if chunk.len() >= rx_available_before {
                    session.rx_fifo().want_deq_notification();
                }
                let flags = if urgent_pending {
                    urgent_pending = false;
                    hammer_runtime::app::SessionEvtFlags::URGENT
                } else {
                    hammer_runtime::app::SessionEvtFlags::empty()
                };
                let wrote = session
                    .enqueue_rx_with_flags(chunk, flags)
                    .map_err(RuntimeError::from)?;
                let accepted_now = wrote.min(chunk.len()).min(rx_available_before);
                let promoted_now = wrote.saturating_sub(accepted_now);
                accepted = accepted
                    .checked_add(accepted_now as u32)
                    .ok_or(SessionError::RxLengthOverflow { session_id })?;
                promoted = promoted
                    .checked_add(promoted_now as u32)
                    .ok_or(SessionError::RxLengthOverflow { session_id })?;
            }
            total = total
                .checked_add(chunk_len)
                .ok_or(SessionError::RxLengthOverflow { session_id })?;
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
    ) -> RuntimeResult<(u32, Option<(u32, u32)>)> {
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
            let chunk_offset = offset.checked_add(total_len).ok_or({
                SessionError::RxOutOfOrderOffsetOverflow {
                    session_id,
                    offset,
                    buffered_len: total_len,
                }
            })?;
            let result = session
                .rx_fifo()
                .enqueue_ooo(chunk_offset, current)
                .map_err(|source| SessionError::RxOutOfOrderEnqueue {
                    session_id,
                    offset: chunk_offset,
                    source,
                })?;
            accepted = accepted
                .checked_add(result.accepted)
                .ok_or(SessionError::RxLengthOverflow { session_id })?;
            if let Some(start) = result.start {
                let end = start
                    .checked_add(result.len)
                    .ok_or(SessionError::OooSpanInvalid { session_id })?;
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
                .ok_or(SessionError::RxLengthOverflow { session_id })?;
        }
        let newest = match (newest_start, newest_end) {
            (Some(start), Some(end)) => Some((
                start,
                end.checked_sub(start)
                    .ok_or(SessionError::OooSpanInvalid { session_id })?,
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
                let Some(Some(slot)) = self.session_slots.get(evt.session_index() as usize) else {
                    continue;
                };
                if evt.evt_type == SessionEvtType::TxDeq {
                    slot.session.clear_tx_event();
                    scheduled += 1;
                }
                dispatch_event(slot.session_id, evt.evt_type);
            }
            if count < batch.len() {
                break;
            }
        }
        scheduled
    }
}

impl AppWorker {
    #[inline]
    pub fn default_local() -> RuntimeResult<Self> {
        let tx_evt_q: Arc<SessionMsgQueue> = Arc::new(
            SessionMsgQueue::with_cfg(2048, 1024)
                .map_err(|error| AppWorkerError::TxEventQueue { error })?,
        );
        Ok(Self::new(1024, tx_evt_q, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::pool::Index as PoolIndex;
    use hammer_runtime::app::{AppSessionConfig, SessionEvt, SessionEvtType, SessionHandle};
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};
    use std::sync::Mutex;

    #[test]
    fn drain_drops_events_for_unmapped_session_slot() {
        let app = AppWorker::default_local().expect("app worker");
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
        let mut app = AppWorker::default_local().expect("app worker");
        let session_id = SessionId::from(PoolIndex::new(5, 1));
        let session = Arc::new(
            AppSession::new_in_segment(
                Segment::default(),
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
        let mut app = AppWorker::default_local().expect("app worker");
        let session_id = SessionId::from(PoolIndex::new(5, 1));
        let session = Arc::new(
            AppSession::new_in_segment(
                Segment::default(),
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

    #[test]
    fn filling_rx_fifo_arms_app_dequeue_before_rx_notification() {
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
        let mut app = AppWorker::default_local().expect("app worker");
        let session_id = SessionId::from(PoolIndex::new(5, 1));
        let session = Arc::new(
            AppSession::new_in_segment(
                Segment::default(),
                AppSessionConfig::new(64, 4),
                SessionHandle::new(5, 0),
                app.tx_evt_q().clone(),
            )
            .expect("app session"),
        );
        app.attach_session(session_id, Arc::clone(&session));
        let index = runtime
            .alloc_index_with_bytes(&[0xab; 64])
            .expect("RX buffer");

        let delivery = app
            .copy_rx_from_buffer(session_id, runtime.buffers(), index, false)
            .expect("copy RX");
        assert_eq!(delivery, (64, 0));
        assert_eq!(session.consume_rx(1), 1);

        let event = app.tx_evt_q().dequeue().expect("RX dequeue event");
        assert_eq!(event.evt_type, SessionEvtType::RxDeq);
        assert_eq!(event.session_index(), 5);
    }

    #[test]
    fn listener_sessions_share_one_segment_manager() {
        let mut app = AppWorker::default_local().expect("app worker");
        let config = AppSessionConfig::new(64, 4);
        for slot in 0..2 {
            let session_id = SessionId::from(PoolIndex::new(slot, 1));
            let handle = SessionHandle::new(slot, 0);
            let session = app
                .create_app_session(7, handle, config, app.tx_evt_q().clone())
                .expect("listener session");
            app.attach_session(session_id, session);
        }

        assert_eq!(app.listeners.len(), 1);
        assert_eq!(app.listeners[&7].segments.len(), 1);
    }

    #[test]
    fn listeners_have_independent_segment_managers() {
        let mut app = AppWorker::default_local().expect("app worker");
        let config = AppSessionConfig::new(64, 4);
        for (slot, listener) in [(0, 7), (1, 9)] {
            let session_id = SessionId::from(PoolIndex::new(slot, 1));
            let handle = SessionHandle::new(slot, 0);
            let session = app
                .create_app_session(listener, handle, config, app.tx_evt_q().clone())
                .expect("listener session");
            app.attach_session(session_id, session);
        }

        assert_eq!(app.listeners.len(), 2);
        assert_eq!(app.listeners[&7].segments.len(), 1);
        assert_eq!(app.listeners[&9].segments.len(), 1);
    }

    #[test]
    fn detached_session_storage_waits_for_last_session_reference() {
        let mut app = AppWorker::default_local().expect("app worker");
        let config = AppSessionConfig::new(64, 4);
        let session_id = SessionId::from(PoolIndex::new(0, 1));
        let handle = SessionHandle::new(0, 0);
        let session = app
            .create_app_session(7, handle, config, app.tx_evt_q().clone())
            .expect("listener session");
        app.attach_session(session_id, Arc::clone(&session));

        let detached = app.detach_session(session_id).expect("detached session");
        assert_eq!(app.listeners[&7].retired.len(), 1);
        drop(detached);
        drop(session);

        let replacement = app
            .create_app_session(7, handle, config, app.tx_evt_q().clone())
            .expect("replacement session");
        assert!(app.listeners[&7].retired.is_empty());
        assert_eq!(app.listeners[&7].segments.len(), 1);
        drop(replacement);
    }

    #[test]
    fn listener_manager_keeps_segment_zero_and_releases_empty_growth_segment() {
        let app = AppWorker::default_local().expect("app worker");
        let mut manager = SegmentManager::new(None);
        let config = AppSessionConfig::new(524_288, 4);
        let segment_zero_handle = SessionHandle::new(0, 0);
        let growth_segment_handle = SessionHandle::new(1, 0);
        let segment_zero_session = manager
            .create_session(segment_zero_handle, config, app.tx_evt_q().clone(), 0)
            .expect("session in segment zero");
        let growth_segment_session = manager
            .create_session(growth_segment_handle, config, app.tx_evt_q().clone(), 0)
            .expect("session in growth segment");

        assert_eq!(manager.segments.len(), 2);
        drop(growth_segment_session);
        manager.release_session(growth_segment_handle);
        assert!(manager.segments[1].is_none());

        drop(segment_zero_session);
        manager.release_session(segment_zero_handle);
        assert!(manager.segments[0].is_some());
    }
}

impl AppWorker {
    pub fn create_app_session(
        &mut self,
        listener: u32,
        handle: SessionHandle,
        config: AppSessionConfig,
        tx_evt_q: Arc<SessionMsgQueue>,
    ) -> RuntimeResult<Arc<AppSession>> {
        let shared_name_prefix = self
            .attach
            .as_ref()
            .map(|_| format!("hammer-session-w{}-l{listener}", self.worker_index));
        let tx_event_offset = self
            .attach
            .as_ref()
            .map_or(0, |attach| attach.tx_event_offset);
        let created = self
            .listeners
            .entry(listener)
            .or_insert_with(|| SegmentManager::new(shared_name_prefix))
            .create_session(handle, config, tx_evt_q, tx_event_offset)?;
        let slot = handle.session_index() as usize;
        let publication = if let Some(attach) = &self.attach {
            match AppSessionPublication::new(
                Arc::clone(&created.session),
                created.segment,
                attach.tx_event_segment.clone(),
                created.offsets,
            ) {
                Ok(publication) => Some(publication),
                Err(error) => {
                    if let Some(manager) = self.listeners.get_mut(&listener) {
                        manager.release_session(handle);
                    }
                    drop(created.session);
                    return Err(error);
                }
            }
        } else {
            None
        };
        self.attach_slots[slot] = AttachSlot {
            listener: Some(listener),
            publication,
        };
        Ok(created.session)
    }

    fn notify_rx(&self, session: &AppSession) {
        session.evt_q().fire();
    }

    fn notify_tx(&self, session: &AppSession) {
        session.tx_evt_q().fire();
    }

    fn notify_evt(&self, session: &AppSession) {
        session.evt_q().fire();
    }
}

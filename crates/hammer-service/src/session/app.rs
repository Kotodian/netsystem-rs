use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Weak};

use hammer_infra::align::align_up;
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, AppSessionConfig, AppSessionError, ApplicationId, SessionEventQueue,
    SessionEvtType, SessionHandle, SessionMsgQueue, SessionMsgQueueError, SessionOffsets,
};
use hammer_runtime::attach::{AppSessionPublication, AppSessionPublisher};
use hammer_runtime::{RuntimeError, RuntimeResult};

use crate::session::SessionId;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AppWorkerError {
    #[error("session message queue configuration rejected: {error:?}")]
    SessionEventQueue { error: SessionMsgQueueError },
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
    /// Hot per-packet lookup array. Cold accept/detach-only metadata lives in
    /// `attach_slots` at the same index.
    session_slots: Vec<Option<SessionSlot>>,
    /// Cold accept/detach-only metadata; never read on the packet path.
    attach_slots: Vec<AttachSlot>,
    worker_index: usize,
    segments_by_owner: HashMap<u64, SegmentManager>,
    publisher: Option<AppSessionPublisher>,
}

#[derive(Clone)]
struct SessionSlot {
    session_id: SessionId,
    app_session: Arc<AppSession>,
}

#[derive(Clone, Default)]
struct AttachSlot {
    allocation_owner: Option<u64>,
    publication: Option<AppSessionPublication>,
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
        app_rx_mq: Arc<SessionMsgQueue>,
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
                app_rx_mq,
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
        worker_index: usize,
        publisher: Option<AppSessionPublisher>,
    ) -> Self {
        Self {
            session_slots: vec![None; session_capacity],
            attach_slots: vec![AttachSlot::default(); session_capacity],
            worker_index,
            segments_by_owner: HashMap::new(),
            publisher,
        }
    }

    pub(crate) fn attach_session(&mut self, session_id: SessionId, app_session: Arc<AppSession>) {
        let slot = session_id.pool_index().slot() as usize;
        self.session_slots[slot] = Some(SessionSlot {
            session_id,
            app_session,
        });
    }

    pub(crate) fn detach_session(&mut self, session_id: SessionId) -> Option<Arc<AppSession>> {
        let index = session_id.pool_index().slot() as usize;
        let entry = self.session_slots.get_mut(index)?;
        if entry
            .as_ref()
            .is_some_and(|slot| slot.session_id == session_id)
        {
            let slot = entry.take()?;
            let app_session_index = slot.app_session.session_handle().session_index() as usize;
            let attach_slot = std::mem::take(&mut self.attach_slots[app_session_index]);
            if let Some(owner) = attach_slot.allocation_owner
                && let Some(manager) = self.segments_by_owner.get_mut(&owner)
            {
                manager.release_session(slot.app_session.session_handle());
            }
            return Some(slot.app_session);
        }
        None
    }

    pub(crate) fn discard_app_session(&mut self, session: &AppSession) {
        let index = session.session_handle().session_index() as usize;
        let attach_slot = std::mem::take(&mut self.attach_slots[index]);
        if let Some(owner) = attach_slot.allocation_owner
            && let Some(manager) = self.segments_by_owner.get_mut(&owner)
        {
            manager.release_session(session.session_handle());
        }
    }

    #[inline(always)]
    pub(crate) fn app_session(&self, session_id: SessionId) -> Option<&Arc<AppSession>> {
        self.session_slots
            .get(session_id.pool_index().slot() as usize)?
            .as_ref()
            .and_then(|slot| (slot.session_id == session_id).then_some(&slot.app_session))
    }

    pub fn connected(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        let Some(session) = self.app_session(session_id).cloned() else {
            return Ok(());
        };
        session
            .push_control_event(SessionEvtType::Connect)
            .map_err(RuntimeError::from)?;
        self.notify_evt(&session);
        let attach_slot = &mut self.attach_slots[session.session_handle().session_index() as usize];
        if let (Some(publication), Some(publisher)) =
            (attach_slot.publication.as_ref(), &self.publisher)
        {
            publisher.try_publish(publication)?;
            attach_slot.publication = None;
        }
        Ok(())
    }

    pub fn disconnected(&self, session_id: SessionId) -> RuntimeResult<()> {
        let Some(session) = self.app_session(session_id) else {
            return Ok(());
        };
        session
            .push_control_event(SessionEvtType::Disconnected)
            .map_err(RuntimeError::from)?;
        self.notify_evt(&session);
        Ok(())
    }

    pub fn reset(&self, session_id: SessionId) -> RuntimeResult<()> {
        let Some(session) = self.app_session(session_id) else {
            return Ok(());
        };
        session
            .push_control_event(SessionEvtType::Reset)
            .map_err(RuntimeError::from)?;
        self.notify_evt(&session);
        Ok(())
    }

    pub fn transport_closed(&self, session_id: SessionId) -> RuntimeResult<()> {
        let Some(session) = self.app_session(session_id) else {
            return Ok(());
        };
        session
            .push_control_event(SessionEvtType::TransportClosed)
            .map_err(RuntimeError::from)?;
        self.notify_evt(&session);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_infra::pool::Index as PoolIndex;
    use hammer_runtime::app::{AppSessionConfig, SessionHandle, SessionMsgQueue};

    fn app_rx_mq() -> Arc<SessionMsgQueue> {
        Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("Application Rx MQ"))
    }

    #[test]
    fn listener_sessions_share_one_segment_manager() {
        let mut app = AppWorker::new(1024, 0, None);
        let config = AppSessionConfig::new(64, 4);
        (0..2).for_each(|slot| {
            let session_id = SessionId::from(PoolIndex::new(slot, 1));
            let handle = SessionHandle::new(slot, 0);
            let session = app
                .create_app_session(7, None, handle, config, app_rx_mq())
                .expect("listener session");
            app.attach_session(session_id, session);
        });

        assert_eq!(app.segments_by_owner.len(), 1);
        assert_eq!(app.segments_by_owner[&7].segments.len(), 1);
    }

    #[test]
    fn listeners_have_independent_segment_managers() {
        let mut app = AppWorker::new(1024, 0, None);
        let config = AppSessionConfig::new(64, 4);
        [(0, 7), (1, 9)].into_iter().for_each(|(slot, listener)| {
            let session_id = SessionId::from(PoolIndex::new(slot, 1));
            let handle = SessionHandle::new(slot, 0);
            let session = app
                .create_app_session(listener, None, handle, config, app_rx_mq())
                .expect("listener session");
            app.attach_session(session_id, session);
        });

        assert_eq!(app.segments_by_owner.len(), 2);
        assert_eq!(app.segments_by_owner[&7].segments.len(), 1);
        assert_eq!(app.segments_by_owner[&9].segments.len(), 1);
    }

    #[test]
    fn detached_session_storage_waits_for_last_session_reference() {
        let mut app = AppWorker::new(1024, 0, None);
        let config = AppSessionConfig::new(64, 4);
        let session_id = SessionId::from(PoolIndex::new(0, 1));
        let handle = SessionHandle::new(0, 0);
        let session = app
            .create_app_session(7, None, handle, config, app_rx_mq())
            .expect("listener session");
        app.attach_session(session_id, Arc::clone(&session));

        let detached = app.detach_session(session_id).expect("detached session");
        assert_eq!(app.segments_by_owner[&7].retired.len(), 1);
        drop(detached);
        drop(session);

        let replacement = app
            .create_app_session(7, None, handle, config, app_rx_mq())
            .expect("replacement session");
        assert!(app.segments_by_owner[&7].retired.is_empty());
        assert_eq!(app.segments_by_owner[&7].segments.len(), 1);
        drop(replacement);
    }

    #[test]
    fn listener_manager_keeps_segment_zero_and_releases_empty_growth_segment() {
        let app = AppWorker::new(1024, 0, None);
        let mut manager = SegmentManager::new(None);
        let config = AppSessionConfig::new(524_288, 4);
        let segment_zero_handle = SessionHandle::new(0, 0);
        let growth_segment_handle = SessionHandle::new(1, 0);
        let segment_zero_session = manager
            .create_session(segment_zero_handle, config, app_rx_mq())
            .expect("session in segment zero");
        let growth_segment_session = manager
            .create_session(growth_segment_handle, config, app_rx_mq())
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
    pub(crate) fn create_app_session(
        &mut self,
        allocation_owner: u64,
        application: Option<ApplicationId>,
        handle: SessionHandle,
        config: AppSessionConfig,
        app_rx_mq: Arc<SessionMsgQueue>,
    ) -> RuntimeResult<Arc<AppSession>> {
        let shared_name_prefix = self
            .publisher
            .as_ref()
            .map(|_| format!("hammer-session-w{}-o{allocation_owner}", self.worker_index));
        let created = self
            .segments_by_owner
            .entry(allocation_owner)
            .or_insert_with(|| SegmentManager::new(shared_name_prefix))
            .create_session(handle, config, app_rx_mq)?;
        let slot = handle.session_index() as usize;
        if self.attach_slots.len() <= slot {
            self.attach_slots.resize_with(slot + 1, AttachSlot::default);
        }
        let publication = if let (Some(_), Some(application)) = (&self.publisher, application) {
            match AppSessionPublication::new(
                Arc::clone(&created.session),
                application,
                created.segment,
                created.offsets,
            ) {
                Ok(publication) => Some(publication),
                Err(error) => {
                    if let Some(manager) = self.segments_by_owner.get_mut(&allocation_owner) {
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
            allocation_owner: Some(allocation_owner),
            publication,
        };
        Ok(created.session)
    }

    fn notify_evt(&self, session: &AppSession) {
        session.evt_q().fire();
    }
}

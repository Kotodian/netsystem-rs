use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use hammer_infra::align::align_up;
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, AppSessionConfig, AppSessionError, ApplicationId, SessionAcceptedMsg,
    SessionConnectedMsg, SessionEventQueue, SessionEvtType, SessionHandle, SessionMsgQueue,
    SessionMsgQueueError, SessionOffsets,
};
use hammer_runtime::attach::{AppSessionPublication, AppSessionPublisher};
use hammer_runtime::{AttachError, RuntimeError, RuntimeResult};

use crate::session::SessionId;

/// Global physical-segment counter mirroring VPP's `smm->seg_name_counter`
/// (third_party/vpp/src/vnet/session/segment_manager.c:181-182): every shared
/// segment gets a unique name across SegmentManager instances, so independent
/// workers owning the same allocation owner never shm_open the same name.
static SEG_NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

#[hammer_component_macros::runtime_error(subsystem = "session app")]
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
    #[error("session allocation refers to a missing listener segment")]
    SessionSegmentMissing,
    #[error("App Session CONNECTED publication is unavailable")]
    ConnectedPublicationUnavailable,
    #[error("App Session ACCEPTED publication is unavailable")]
    AcceptedPublicationUnavailable,
}

pub struct AppWorker {
    /// Hot per-packet lookup array. Cold accept/detach-only metadata lives in
    /// `attach_slots` at the same index.
    session_slots: Vec<Option<SessionSlot>>,
    /// Cold accept/detach-only metadata; never read on the packet path.
    attach_slots: Vec<AttachSlot>,
    pending_accepted: std::collections::VecDeque<SessionId>,
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
    connect_pending: bool,
    connect_event_sent: bool,
    accepted_pending: bool,
    accepted_event_sent: bool,
    accepted_retry_queued: bool,
}

struct SegmentManager {
    segments: Vec<Option<Segment>>,
    allocations: HashMap<u64, SessionAllocation>,
    retired: Vec<SessionAllocation>,
    shared_name_prefix: Option<String>,
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
                if let Err(error) = self.deallocate(allocation) {
                    tracing::warn!(%error, "retired session allocation cleanup deferred");
                }
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
                    let name =
                        format!("{prefix}-{:x}", SEG_NAME_COUNTER.fetch_add(1, Ordering::Relaxed));
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

    /// VPP session_free is void best-effort cleanup (session.c:258-265): a
    /// missing segment entry is logged, never fatal to the detach that
    /// triggered the release.
    fn deallocate(&mut self, allocation: SessionAllocation) -> Result<(), AppWorkerError> {
        let segment_index = allocation.segment;
        let Some(entry) = self.segments[segment_index].as_ref() else {
            return Err(AppWorkerError::SessionSegmentMissing);
        };
        entry.free(allocation.offset, allocation.bytes);
        let segment_in_use = self
            .allocations
            .values()
            .chain(self.retired.iter())
            .any(|allocation| allocation.segment == segment_index);
        if segment_index != 0 && !segment_in_use {
            self.segments[segment_index] = None;
        }
        Ok(())
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
            pending_accepted: std::collections::VecDeque::with_capacity(session_capacity),
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

    /// Generation-safe lookup of the AppSession's allocation owner from its
    /// AttachSlot, used to inherit the parent worker for peer-opened children.
    #[inline(always)]
    pub(crate) fn session_allocation_owner(&self, session_id: SessionId) -> Option<u64> {
        let session = self.app_session(session_id)?;
        self.attach_slots
            .get(session.session_handle().session_index() as usize)?
            .allocation_owner
    }

    pub fn connected(&mut self, session_id: SessionId) -> RuntimeResult<bool> {
        let Some(session) = self.app_session(session_id).cloned() else {
            return Ok(true);
        };
        let index = session.session_handle().session_index() as usize;
        self.attach_slots[index].connect_pending = true;
        self.try_connected(&session)
    }

    fn try_connected(&mut self, session: &Arc<AppSession>) -> RuntimeResult<bool> {
        let index = session.session_handle().session_index() as usize;
        if !self.attach_slots[index].connect_event_sent {
            match session.push_control_event(SessionEvtType::Connect) {
                Ok(()) => {
                    self.attach_slots[index].connect_event_sent = true;
                    self.notify_evt(session);
                }
                Err(AppSessionError::EventQueueFull { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        }
        if self.publisher.is_none() {
            // No publisher: the CONNECT publication can never be delivered
            // (VPP app_worker_free tears the app worker down); drop it and
            // report success so no impossible retry is queued.
            self.attach_slots[index].publication = None;
            self.attach_slots[index].connect_pending = false;
            return Ok(true);
        }
        let publisher = self.publisher.clone();
        if let Some(publisher) = publisher
            && let Some(publication) = self.attach_slots[index].publication.as_ref()
        {
            match publisher.try_publish(publication) {
                Ok(()) => self.attach_slots[index].publication = None,
                Err(RuntimeError::Attach(AttachError::PublicationQueueFull)) => {}
                Err(error) => return Err(error),
            }
        }
        let publication_accepted = self.attach_slots[index].publication.is_none();
        if self.attach_slots[index].connect_event_sent && publication_accepted {
            self.attach_slots[index].connect_pending = false;
        }
        Ok(publication_accepted)
    }

    pub fn accepted(&mut self, session_id: SessionId) -> RuntimeResult<bool> {
        let Some(session) = self.app_session(session_id).cloned() else {
            return Ok(true);
        };
        let index = session.session_handle().session_index() as usize;
        let retry_queued = self.attach_slots[index].accepted_retry_queued;
        self.attach_slots[index].accepted_pending = true;
        let published = self.try_accepted(&session)?;
        if !published && !retry_queued {
            self.pending_accepted.push_back(session_id);
            self.attach_slots[index].accepted_retry_queued = true;
        }
        Ok(published)
    }

    fn try_accepted(&mut self, session: &Arc<AppSession>) -> RuntimeResult<bool> {
        let index = session.session_handle().session_index() as usize;
        if !self.attach_slots[index].accepted_event_sent {
            match session.push_control_event(SessionEvtType::Accepted) {
                Ok(()) => {
                    self.attach_slots[index].accepted_event_sent = true;
                    self.notify_evt(session);
                }
                // VPP backpressure (app_worker_mq_is_congested): the event
                // could not be enqueued, so publish nothing and report failure
                // so the retry round re-attempts both event and publication.
                Err(AppSessionError::EventQueueFull { .. }) => return Ok(false),
                Err(error) => return Err(error.into()),
            }
        }
        if self.publisher.is_none() {
            // No publisher: the ACCEPTED publication can never be delivered
            // (VPP app_worker_free tears the app worker down); drop it and
            // report success so no impossible retry is queued.
            self.attach_slots[index].publication = None;
            self.attach_slots[index].accepted_pending = false;
            self.attach_slots[index].accepted_retry_queued = false;
            return Ok(true);
        }
        let publisher = self.publisher.clone();
        if let Some(publisher) = publisher
            && let Some(publication) = self.attach_slots[index].publication.as_ref()
        {
            match publisher.try_publish(publication) {
                Ok(()) => self.attach_slots[index].publication = None,
                Err(RuntimeError::Attach(AttachError::PublicationQueueFull)) => {}
                Err(error) => return Err(error),
            }
        }
        let publication_accepted = self.attach_slots[index].publication.is_none();
        if self.attach_slots[index].accepted_event_sent && publication_accepted {
            self.attach_slots[index].accepted_pending = false;
        }
        Ok(publication_accepted)
    }

    pub(crate) fn pending_connected_sessions(&self) -> Vec<SessionId> {
        self.session_slots
            .iter()
            .flatten()
            .filter_map(|slot| {
                let index = slot.app_session.session_handle().session_index() as usize;
                self.attach_slots
                    .get(index)
                    .filter(|attach_slot| attach_slot.connect_pending)
                    .map(|_| slot.session_id)
            })
            .collect()
    }

    pub(crate) fn has_pending_connected_sessions(&self) -> bool {
        self.session_slots.iter().flatten().any(|slot| {
            let index = slot.app_session.session_handle().session_index() as usize;
            self.attach_slots
                .get(index)
                .is_some_and(|attach_slot| attach_slot.connect_pending)
        })
    }

    pub(crate) fn next_pending_accepted(&mut self) -> Option<SessionId> {
        let session_id = self.pending_accepted.pop_front()?;
        // Derive the attach slot from the live session handle, matching every
        // other accepted_retry_queued operation: VPP keys events by session
        // index, not by the SessionId pool slot.
        if let Some(session) = self.app_session(session_id) {
            let index = session.session_handle().session_index() as usize;
            if let Some(slot) = self.attach_slots.get_mut(index) {
                slot.accepted_retry_queued = false;
            }
        }
        Some(session_id)
    }

    pub(crate) fn has_pending_accepted_sessions(&self) -> bool {
        !self.pending_accepted.is_empty()
    }

    /// Re-attempts the ACCEPTED publication for a Session that
    /// [`next_pending_accepted`](Self::next_pending_accepted) popped because
    /// the publisher queue was full. Returns `true` when the publication
    /// completed; on failure the Session is re-queued (bounded deque, one
    /// retry per round, no scans) and `false` is returned so the poll loop
    /// stops retrying this round.
    pub(crate) fn retry_pending_accepted(&mut self, session_id: SessionId) -> RuntimeResult<bool> {
        let Some(session) = self.app_session(session_id).cloned() else {
            return Ok(true);
        };
        let published = self.try_accepted(&session)?;
        if !published {
            let index = session.session_handle().session_index() as usize;
            self.pending_accepted.push_back(session_id);
            self.attach_slots[index].accepted_retry_queued = true;
        }
        Ok(published)
    }

    pub(crate) fn set_connected(
        &mut self,
        session_id: SessionId,
        connected: SessionConnectedMsg,
    ) -> RuntimeResult<()> {
        let session = self.app_session(session_id).cloned().ok_or_else(|| {
            RuntimeError::from(AppSessionError::NotFound {
                app_worker: self.worker_index,
                session: session_id.get(),
            })
        })?;
        let index = session.session_handle().session_index() as usize;
        let slot = self
            .attach_slots
            .get_mut(index)
            .ok_or(AppWorkerError::ConnectedPublicationUnavailable)?;
        if self.publisher.is_none() {
            return Err(AppWorkerError::ConnectedPublicationUnavailable.into());
        }
        let publication = slot
            .publication
            .as_mut()
            .ok_or(AppWorkerError::ConnectedPublicationUnavailable)?;
        publication.set_connected(connected);
        Ok(())
    }

    pub(crate) fn set_accepted(
        &mut self,
        session_id: SessionId,
        accepted: SessionAcceptedMsg,
    ) -> RuntimeResult<()> {
        let session = self.app_session(session_id).cloned().ok_or_else(|| {
            RuntimeError::from(AppSessionError::NotFound {
                app_worker: self.worker_index,
                session: session_id.get(),
            })
        })?;
        let index = session.session_handle().session_index() as usize;
        let slot = self
            .attach_slots
            .get_mut(index)
            .ok_or(AppWorkerError::AcceptedPublicationUnavailable)?;
        if self.publisher.is_none() {
            return Err(AppWorkerError::AcceptedPublicationUnavailable.into());
        }
        let publication = slot
            .publication
            .as_mut()
            .ok_or(AppWorkerError::AcceptedPublicationUnavailable)?;
        publication.set_accepted(accepted)?;
        Ok(())
    }

    pub(crate) fn accepted_message(&self, session_id: SessionId) -> Option<SessionAcceptedMsg> {
        let session = self.app_session(session_id)?;
        let index = session.session_handle().session_index() as usize;
        let slot = self.attach_slots.get(index)?;
        slot.publication.as_ref()?.accepted_message()
    }

    pub(crate) fn publish_connect_failed(
        &self,
        application: ApplicationId,
        message: SessionConnectedMsg,
    ) -> RuntimeResult<bool> {
        let Some(publisher) = self.publisher.as_ref() else {
            return Ok(true);
        };
        match publisher.try_publish_connect_failure(application, message) {
            Ok(()) => Ok(true),
            Err(RuntimeError::Attach(AttachError::PublicationQueueFull)) => Ok(false),
            Err(error) => Err(error),
        }
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
    use hammer_runtime::app::{AppSessionConfig, SessionFlags, SessionHandle, SessionMsgQueue};
    use std::error::Error;

    fn app_rx_mq() -> Arc<SessionMsgQueue> {
        Arc::new(SessionMsgQueue::with_cfg(64, 64).expect("Application Rx MQ"))
    }

    #[test]
    fn accepted_session_notifies_application_once() {
        let mut app = AppWorker::new(1024, 0, None);
        let session_id = SessionId::from(PoolIndex::new(0, 1));
        let session = app
            .create_app_session(
                7,
                None,
                SessionHandle::new(0, 0),
                AppSessionConfig::new(64, 4),
                app_rx_mq(),
            )
            .expect("accepted Application Session");
        app.attach_session(session_id, Arc::clone(&session));

        assert!(app.accepted(session_id).expect("accepted notification"));
        assert!(app.accepted(session_id).expect("accepted retry"));
        assert_eq!(
            session
                .evt_q()
                .dequeue()
                .expect("dequeue")
                .map(|event| event.evt_type),
            Some(SessionEvtType::Accepted)
        );
        assert!(session.evt_q().dequeue().expect("dequeue").is_none());
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

    #[test]
    fn independent_shared_segment_managers_with_same_prefix_create_sessions() {
        // Independent managers (SessionWorker and ApplicationMain) can own the
        // same allocation owner and therefore the same shared prefix. Names
        // come from one global counter (VPP segment_manager.c:181-182); a
        // manager-local counter would make both shm_open the same name, and
        // macOS shm_open fails with EINVAL when the name exists.
        let prefix = format!("hs-wtest-{}", std::process::id());
        let config = AppSessionConfig::new(64, 4);
        let mut manager_a = SegmentManager::new(Some(prefix.clone()));
        let mut manager_b = SegmentManager::new(Some(prefix));
        manager_a
            .create_session(SessionHandle::new(0, 0), config, app_rx_mq())
            .expect("first manager session");
        manager_b
            .create_session(SessionHandle::new(1, 0), config, app_rx_mq())
            .expect("second manager session shares the prefix");
    }

    #[test]
    fn release_session_with_missing_segment_is_best_effort() {
        let mut manager = SegmentManager::new(None);
        let config = AppSessionConfig::new(64, 4);
        let handle = SessionHandle::new(0, 0);
        let created = manager
            .create_session(handle, config, app_rx_mq())
            .expect("session allocation");
        drop(created.session);

        // Corrupt the segment table so the allocation refers to a missing
        // segment entry. VPP session_free is void best-effort cleanup
        // (session.c:258-265); Hammer must degrade to a logged warning,
        // never panic the detach path.
        manager.segments[0] = None;
        manager.release_session(handle);
        assert!(manager.allocations.is_empty());
    }

    #[test]
    fn retry_pending_accepted_requeues_while_publisher_queue_is_full() {
        let socket_path = format!("/tmp/hammer-app-retry-accepted-{}.sock", std::process::id());
        let server = hammer_runtime::attach::AppServer::bind(&socket_path, 1)
            .expect("bind App server with a single publication slot");
        let mut app = AppWorker::new(1024, 0, Some(server.publisher()));
        let session_a = app
            .create_app_session(
                7,
                Some(hammer_runtime::app::ApplicationId::new(0, 0)),
                SessionHandle::new(0, 0),
                AppSessionConfig::new(64, 4),
                app_rx_mq(),
            )
            .expect("first accepted Application Session");
        app.attach_session(
            SessionId::from(PoolIndex::new(0, 1)),
            Arc::clone(&session_a),
        );
        assert!(
            app.accepted(SessionId::from(PoolIndex::new(0, 1)))
                .expect("first accepted publication")
        );

        let session_b = app
            .create_app_session(
                8,
                Some(hammer_runtime::app::ApplicationId::new(0, 0)),
                SessionHandle::new(1, 0),
                AppSessionConfig::new(64, 4),
                app_rx_mq(),
            )
            .expect("second accepted Application Session");
        app.attach_session(
            SessionId::from(PoolIndex::new(1, 1)),
            Arc::clone(&session_b),
        );
        // The single publication slot is occupied by session A, so session B
        // is re-queued, and a retry round re-queues it again while the queue
        // stays full.
        assert!(
            !app.accepted(SessionId::from(PoolIndex::new(1, 1)))
                .expect("second publication blocked")
        );
        assert!(app.has_pending_accepted_sessions());
        let pending = app
            .next_pending_accepted()
            .expect("pending accepted Session");
        assert_eq!(pending, SessionId::from(PoolIndex::new(1, 1)));
        assert!(
            !app.retry_pending_accepted(pending)
                .expect("retry stays blocked")
        );
        assert!(app.has_pending_accepted_sessions());
        let requeued = app.next_pending_accepted().expect("re-queued Session");
        assert_eq!(requeued, SessionId::from(PoolIndex::new(1, 1)));
    }

    #[test]
    fn accepted_event_queue_full_retries_event_and_publication() {
        let socket_path = format!(
            "/tmp/hammer-app-accepted-evtq-full-{}.sock",
            std::process::id()
        );
        let server = hammer_runtime::attach::AppServer::bind(&socket_path, 1)
            .expect("bind App server with a single publication slot");
        let mut app = AppWorker::new(8, 0, Some(server.publisher()));
        let handle = SessionHandle::new(0, 0);
        let session_id = SessionId::from(PoolIndex::new(0, 1));
        let session = app
            .create_app_session(
                0x17,
                Some(hammer_runtime::app::ApplicationId::new(0, 0)),
                handle,
                AppSessionConfig::new(64, 4),
                app_rx_mq(),
            )
            .expect("accepted Application Session");
        app.attach_session(session_id, Arc::clone(&session));
        app.set_accepted(
            session_id,
            SessionAcceptedMsg::new(0, handle, handle, SessionFlags::empty()),
        )
        .expect("set accepted publication");

        // Fill the CTRL event queue so the ACCEPTED enqueue is rejected.
        std::iter::repeat_with(|| session.push_control_event(SessionEvtType::Connect))
            .take_while(Result::is_ok)
            .for_each(std::mem::drop);

        // Queue-full must not publish the descriptor: the retry round retries
        // both the ACCEPTED event enqueue and the publication.
        assert!(
            !app.accepted(session_id)
                .expect("queue-full accepted is deferred")
        );
        assert!(app.has_pending_accepted_sessions());
        assert!(
            app.accepted_message(session_id).is_some(),
            "publication descriptor must not be consumed while the event queue is full"
        );
        assert!(
            !app.accepted(session_id)
                .expect("no duplicate retry while queued")
        );

        // Drain the queue; the retry round then delivers event and publication.
        assert!(session.evt_q().dequeue().expect("dequeue").is_some());
        let pending = app
            .next_pending_accepted()
            .expect("pending accepted Session");
        assert_eq!(pending, session_id);
        assert!(
            app.retry_pending_accepted(pending)
                .expect("retry after drain")
        );
        assert!(!app.has_pending_accepted_sessions());
        assert!(app.accepted_message(session_id).is_none());
        let mut delivered = Vec::new();
        while let Some(event) = session.evt_q().dequeue().expect("dequeue") {
            delivered.push(event.evt_type);
        }
        assert!(
            delivered.contains(&SessionEvtType::Accepted),
            "retry must deliver the ACCEPTED control event"
        );
    }

    #[test]
    fn retry_slot_is_derived_from_session_handle_index() {
        let socket_path = format!("/tmp/hammer-app-retry-slot-{}.sock", std::process::id());
        let server = hammer_runtime::attach::AppServer::bind(&socket_path, 1)
            .expect("bind App server with a single publication slot");
        let mut app = AppWorker::new(8, 0, Some(server.publisher()));

        // Session A occupies the single publication slot so Session B is
        // deferred into the retry queue.
        let session_a = app
            .create_app_session(
                0x18,
                Some(hammer_runtime::app::ApplicationId::new(0, 0)),
                SessionHandle::new(0, 0),
                AppSessionConfig::new(64, 4),
                app_rx_mq(),
            )
            .expect("first accepted Application Session");
        app.attach_session(
            SessionId::from(PoolIndex::new(0, 1)),
            Arc::clone(&session_a),
        );
        assert!(
            app.accepted(SessionId::from(PoolIndex::new(0, 1)))
                .expect("first accepted publication")
        );

        // The handle session_index (5) deliberately differs from the SessionId
        // pool slot (3): accepted_retry_queued must be tracked under the
        // session handle index, as VPP keys events by session index.
        let handle = SessionHandle::new(5, 0);
        let session_id = SessionId::from(PoolIndex::new(3, 1));
        let session = app
            .create_app_session(
                0x18,
                Some(hammer_runtime::app::ApplicationId::new(0, 0)),
                handle,
                AppSessionConfig::new(64, 4),
                app_rx_mq(),
            )
            .expect("second accepted Application Session");
        app.attach_session(session_id, Arc::clone(&session));
        app.set_accepted(
            session_id,
            SessionAcceptedMsg::new(0, handle, handle, SessionFlags::empty()),
        )
        .expect("set accepted publication");

        assert!(
            !app.accepted(session_id)
                .expect("blocked accepted publication")
        );
        assert!(
            app.attach_slots[5].accepted_retry_queued,
            "retry queued under the session handle index"
        );
        let pending = app
            .next_pending_accepted()
            .expect("pending accepted Session");
        assert_eq!(pending, session_id);
        assert!(
            !app.attach_slots[5].accepted_retry_queued,
            "retry flag cleared under the session handle index"
        );
    }

    #[test]
    fn set_connected_and_accepted_unavailable_without_publisher() {
        let mut app = AppWorker::new(8, 0, None);
        let session_id = SessionId::from(PoolIndex::new(0, 1));
        let handle = SessionHandle::new(0, 0);
        let session = app
            .create_app_session(
                7,
                Some(hammer_runtime::app::ApplicationId::new(0, 0)),
                handle,
                AppSessionConfig::new(64, 4),
                app_rx_mq(),
            )
            .expect("Application Session");
        app.attach_session(session_id, Arc::clone(&session));

        let error = app
            .set_connected(session_id, SessionConnectedMsg::new(0, Ok(handle)))
            .expect_err("CONNECTED publication is unavailable without a publisher");
        assert!(matches!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<AppWorkerError>()),
            Some(AppWorkerError::ConnectedPublicationUnavailable)
        ));
        let error = app
            .set_accepted(
                session_id,
                SessionAcceptedMsg::new(0, handle, handle, SessionFlags::empty()),
            )
            .expect_err("ACCEPTED publication is unavailable without a publisher");
        assert!(matches!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<AppWorkerError>()),
            Some(AppWorkerError::AcceptedPublicationUnavailable)
        ));
    }

    #[test]
    fn set_connected_and_accepted_unavailable_without_publication() {
        let socket_path = format!("/tmp/hammer-app-set-connected-{}.sock", std::process::id());
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let mut app = AppWorker::new(8, 0, Some(server.publisher()));
        let session_id = SessionId::from(PoolIndex::new(0, 1));
        let handle = SessionHandle::new(0, 0);
        let session = app
            .create_app_session(7, None, handle, AppSessionConfig::new(64, 4), app_rx_mq())
            .expect("listener Session without publication");
        app.attach_session(session_id, Arc::clone(&session));

        let error = app
            .set_connected(session_id, SessionConnectedMsg::new(0, Ok(handle)))
            .expect_err("CONNECTED publication is unavailable without a publication");
        assert!(matches!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<AppWorkerError>()),
            Some(AppWorkerError::ConnectedPublicationUnavailable)
        ));
        let error = app
            .set_accepted(
                session_id,
                SessionAcceptedMsg::new(0, handle, handle, SessionFlags::empty()),
            )
            .expect_err("ACCEPTED publication is unavailable without a publication");
        assert!(matches!(
            error
                .source()
                .and_then(|source| source.downcast_ref::<AppWorkerError>()),
            Some(AppWorkerError::AcceptedPublicationUnavailable)
        ));
    }

    #[test]
    fn no_pending_retry_when_publisher_absent() {
        let socket_path = format!("/tmp/hammer-app-no-publisher-{}.sock", std::process::id());
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let mut app = AppWorker::new(8, 0, Some(server.publisher()));
        let session_id = SessionId::from(PoolIndex::new(0, 1));
        let handle = SessionHandle::new(0, 0);
        let session = app
            .create_app_session(
                7,
                Some(hammer_runtime::app::ApplicationId::new(0, 0)),
                handle,
                AppSessionConfig::new(64, 4),
                app_rx_mq(),
            )
            .expect("Application Session");
        app.attach_session(session_id, Arc::clone(&session));
        // The worker loses its publisher (app detach); a pending ACCEPTED or
        // CONNECT publication can then never be delivered, so it must not be
        // queued for an impossible retry.
        app.publisher = None;
        assert!(app.accepted(session_id).expect("accepted"));
        assert!(!app.has_pending_accepted_sessions());
        assert!(app.connected(session_id).expect("connected"));
        assert!(!app.has_pending_connected_sessions());
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
        // Physical shm name only: `Segment::shared` unlinks it immediately and
        // descriptors carry the mapping. Owner grouping in `segments_by_owner`
        // is unchanged, so keep the name compact enough for shm_open limits.
        let shared_name_prefix = self
            .publisher
            .as_ref()
            .map(|_| format!("hs-w{:x}-o{allocation_owner:x}", self.worker_index));
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
            connect_pending: false,
            connect_event_sent: false,
            accepted_pending: false,
            accepted_event_sent: false,
            accepted_retry_queued: false,
        };
        Ok(created.session)
    }

    fn notify_evt(&self, session: &AppSession) {
        session.evt_q().fire();
    }
}

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::marker::PhantomPinned;
use std::mem::size_of;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::bitmap::Bitmap;
use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo_segment::{FifoSegmentError, SvmFifoSegment, SvmFifoSegmentConfig};
use hammer_infra::svm::msg_queue::{SvmMsgQ, SvmMsgQConfig, SvmMsgQError, SvmMsgQRingConfig};
use hammer_infra::svm::queue::SvmQueueConditionalWait;
use hammer_infra::svm::ssvm::{SsvmConfig, SsvmPrivate, SsvmSegmentBackend};
use hammer_infra::sync::SpinLock;
use hammer_runtime::DataPlaneMain;
use hammer_runtime::config::worker::worker_count;

use super::core::{
    Session, SessionControlData, SessionEvent, SessionEventType, SessionHandle, SessionState,
    SessionWorker,
};
use super::error::SessionError;
use super::segment_manager::{
    SegmentManagerError, SegmentManagerFlags, SegmentManagerMain, SegmentManagerProperties,
};

/// Failures owned by the new service Application path. VPP error categories:
/// `foreach_session_error`, session_types.h:519-561.
#[hammer_component_macros::runtime_error(subsystem = "session application")]
#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    #[error("application {application} is already attached")]
    ApplicationAttached { application: u32 },
    #[error("application namespace {namespace} is invalid")]
    InvalidNamespace { namespace: u32 },
    #[error("application worker {worker} does not belong to application {application}")]
    InvalidApplicationWorker { application: u32, worker: u32 },
    #[error("application worker {worker} does not exist for application {application}")]
    WorkerMissing { application: u32, worker: u32 },
    #[error("application listener {listener} does not exist")]
    NoListener { listener: u32 },
    #[error("application listener {listener} is already listening")]
    AlreadyListening { listener: u32 },
    #[error("application listener {listener} already includes worker {worker}")]
    ListenerWorkerAttached { listener: u32, worker: u32 },
    #[error("application listener {listener} belongs to another application")]
    Owner { listener: u32 },
    #[error("application listener has no accepting worker")]
    NoAcceptingWorker,
    #[error("application FIFO segment creation failed")]
    SegmentCreate {
        #[source]
        source: FifoSegmentError,
    },
    #[error("application FIFO segment has no space for the FIFO pair")]
    SegmentNoSpace,
    #[error("application event message allocation failed")]
    MessageAllocation {
        #[source]
        source: SvmMsgQError,
    },
    #[error("application callback rejected the session")]
    CallbackRejected,
    #[error("builtin application {application} callback failed")]
    BuiltinCallback {
        application: u32,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("application listener {listener} segment cleanup failed")]
    ListenerSegmentCleanup {
        listener: u32,
        #[source]
        source: FifoSegmentError,
    },
    #[error("application {application} event queue detach failed")]
    EventQueueDetach {
        application: u32,
        #[source]
        source: SvmMsgQError,
    },
    #[error("Application attach requires a Data Worker")]
    NoDataWorkers,
    #[error("Application event queue capacity {capacity} is invalid")]
    EventQueueCapacityInvalid { capacity: u32 },
    #[error("Application {application} still owns listeners")]
    ListenersActive { application: u32 },
    #[error("builtin Application requires an RX callback")]
    BuiltinRxMissing,
}

/// VPP: `app_worker_add_event` and MQ congestion handling,
/// application_worker.c:493-517,934-967; session_input.c:80-113.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplicationEventResult {
    Queued,
    Deferred,
    QueueFull,
    LockUnavailable,
}

bitflags::bitflags! {
    /// VPP: `foreach_app_options_flags`, application_interface.h:196-222.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct ApplicationFlags: u32 {
        const ACCEPT_REDIRECT = 1 << 0;
        const ADD_SEGMENT = 1 << 1;
        const BUILTIN = 1 << 2;
        const TRANSPORT_APP = 1 << 3;
        const PROXY = 1 << 4;
        const GLOBAL_SCOPE = 1 << 5;
        const LOCAL_SCOPE = 1 << 6;
        const MQ_EVENTFD = 1 << 7;
        const BUILTIN_MEMFD = 1 << 8;
        const HUGE_PAGE = 1 << 9;
        const ORIGINAL_DESTINATION = 1 << 10;
        const EVENT_COLLECTOR = 1 << 11;
        const NO_DUMP_SEGMENTS = 1 << 12;
    }
}

/// VPP: `session_cleanup_ntf_t`, session_types.h:173-177.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionCleanup {
    Transport,
    Session,
}

impl TryFrom<u8> for SessionCleanup {
    type Error = SessionError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        [Self::Transport, Self::Session]
            .get(value as usize)
            .copied()
            .ok_or(SessionError::Invalid)
    }
}

/// Attach input. Each event slot is copied directly into its Application.
/// VPP: `app_init_args_t`, application_interface.h:77-84;
/// `application_alloc_and_init`, application.c:664-703.
pub struct ApplicationConfig {
    pub namespace: u32,
    pub flags: ApplicationFlags,
    pub name: String,
    pub segment: SegmentManagerProperties,
    pub add_segment: fn(u32, u64) -> Result<(), ApplicationError>,
    pub del_segment: fn(u32, u64) -> Result<(), ApplicationError>,
    pub accepted: fn(&mut Session) -> Result<(), ApplicationError>,
    pub connected:
        fn(u32, u64, Option<&mut Session>, Option<SessionError>) -> Result<(), ApplicationError>,
    pub disconnected: fn(&mut Session),
    pub reset: fn(&mut Session),
    pub transport_closed: Option<fn(&mut Session)>,
    pub cleanup: Option<fn(&mut Session, SessionCleanup)>,
    pub half_open_cleanup: Option<fn(&mut Session)>,
    pub migrated: Option<fn(&mut Session, SessionHandle)>,
    pub listened:
        Option<fn(u32, u32, SessionHandle, Option<SessionError>) -> Result<(), ApplicationError>>,
    pub unlistened: Option<fn(u32, SessionHandle, u32, Option<SessionError>)>,
    pub builtin_rx: Option<fn(&mut Session)>,
    pub builtin_tx: Option<fn(&mut Session)>,
}

bitflags::bitflags! {
    /// VPP: `app_rx_mq_flags_t`, application.h:103-107.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct ApplicationRxMqFlags: u8 {
        const PENDING = 1 << 0;
        const POSTPONED = 1 << 1;
    }
}

/// VPP: `app_rx_mq_elt_t`, application.h:109-117.
pub struct ApplicationRxMq<'app> {
    pub next: u32,
    pub previous: u32,
    pub queue: &'app SvmMsgQ,
    pub file_index: usize,
    pub application: u32,
    pub flags: ApplicationRxMqFlags,
}

/// VPP: `app_worker_map_t`, application.h:82-85.
pub struct ApplicationWorkerMap {
    pub worker: u32,
}

/// Service-owned Application record. Callback fields are direct event slots.
/// VPP: `application_t`, application.h:119-161;
/// `session_cb_vft_t`, application_interface.h:15-59.
pub struct Application<'app> {
    index: u32,
    flags: ApplicationFlags,
    add_segment: fn(u32, u64) -> Result<(), ApplicationError>,
    del_segment: fn(u32, u64) -> Result<(), ApplicationError>,
    accepted: fn(&mut Session) -> Result<(), ApplicationError>,
    connected:
        fn(u32, u64, Option<&mut Session>, Option<SessionError>) -> Result<(), ApplicationError>,
    disconnected: fn(&mut Session),
    reset: fn(&mut Session),
    transport_closed: Option<fn(&mut Session)>,
    cleanup: Option<fn(&mut Session, SessionCleanup)>,
    half_open_cleanup: Option<fn(&mut Session)>,
    migrated: Option<fn(&mut Session, SessionHandle)>,
    listened:
        Option<fn(u32, u32, SessionHandle, Option<SessionError>) -> Result<(), ApplicationError>>,
    unlistened: Option<fn(u32, SessionHandle, u32, Option<SessionError>)>,
    builtin_rx: Option<fn(&mut Session)>,
    builtin_tx: Option<fn(&mut Session)>,
    segment: SegmentManagerProperties,
    worker_maps: Pool<ApplicationWorkerMap>,
    name: String,
    namespace: u32,
    listeners: Pool<u32>,
    workers: Pool<u32>,
    rx_mqs: Vec<ApplicationRxMq<'app>>,
    rx_mq_segment: SvmFifoSegment,
    pin: PhantomPinned,
}

impl Application<'_> {
    #[inline(always)]
    pub const fn index(&self) -> u32 {
        self.index
    }

    #[inline(always)]
    pub const fn flags(&self) -> ApplicationFlags {
        self.flags
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    pub const fn namespace(&self) -> u32 {
        self.namespace
    }

    /// VPP: `application_get_worker`, application.h:316-327.
    #[inline(always)]
    pub fn worker(&self, worker_map: u32) -> Option<u32> {
        self.worker_maps.get(worker_map).map(|entry| entry.worker)
    }

    #[inline(always)]
    pub fn app_rx_mq(&self, worker: u32) -> Option<&ApplicationRxMq<'_>> {
        self.rx_mqs.get(worker as usize)
    }
}

/// VPP: `app_listener_t`, application.h:88-101.
pub struct ApplicationListener {
    index: u32,
    application: u32,
    workers: Bitmap,
    accept_rotor: AtomicU32,
    global_session: Option<SessionHandle>,
    local_session: Option<SessionHandle>,
    worker_sessions: Vec<u32>,
    opaque: u64,
}

impl ApplicationListener {
    fn new(application: u32, opaque: u64) -> Self {
        Self {
            index: u32::MAX,
            application,
            workers: Bitmap::new(),
            accept_rotor: AtomicU32::new(0),
            global_session: None,
            local_session: None,
            worker_sessions: Vec::new(),
            opaque,
        }
    }

    #[inline(always)]
    pub const fn index(&self) -> u32 {
        self.index
    }

    #[inline(always)]
    pub const fn application(&self) -> u32 {
        self.application
    }

    #[inline(always)]
    pub const fn opaque(&self) -> u64 {
        self.opaque
    }

    #[inline(always)]
    pub const fn handle(&self) -> Option<SessionHandle> {
        match self.global_session {
            Some(handle) => Some(handle),
            None => self.local_session,
        }
    }

    #[inline]
    pub fn select_worker(&self) -> Result<u32, ApplicationError> {
        // VPP: `app_listener_select_worker`, application.c:172-185. The
        // bitmap is barrier-published; only the independent rotor is updated
        // concurrently by accepting Session workers.
        let mut observed = self.accept_rotor.load(Ordering::Acquire);
        loop {
            let worker = self
                .workers
                .next_set((observed as usize).saturating_add(1))
                .or_else(|| self.workers.first_set())
                .ok_or(ApplicationError::NoAcceptingWorker)?;
            let worker = u32::try_from(worker).expect("Application worker-map index fits u32");
            match self.accept_rotor.compare_exchange_weak(
                observed,
                worker,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(worker),
                Err(current) => observed = current,
            }
        }
    }

    #[inline(always)]
    pub const fn global_session(&self) -> Option<SessionHandle> {
        self.global_session
    }

    #[inline(always)]
    pub const fn local_session(&self) -> Option<SessionHandle> {
        self.local_session
    }

    pub fn attach_session(
        &mut self,
        global: Option<SessionHandle>,
        local: Option<SessionHandle>,
    ) -> Result<(), ApplicationError> {
        if self.global_session.is_some() || self.local_session.is_some() {
            return Err(ApplicationError::AlreadyListening {
                listener: self.index,
            });
        }
        self.global_session = global;
        self.local_session = local;
        Ok(())
    }

    pub fn attach_worker(&mut self, worker: u32) -> Result<(), ApplicationError> {
        if !self.workers.set(worker as usize) {
            return Err(ApplicationError::ListenerWorkerAttached {
                listener: self.index,
                worker,
            });
        }
        Ok(())
    }

    pub fn detach_worker(&mut self, worker: u32) -> Result<(), ApplicationError> {
        if !self.workers.clear(worker as usize) {
            return Err(ApplicationError::WorkerMissing {
                application: self.application,
                worker,
            });
        }
        Ok(())
    }
}

/// VPP: `app_worker_t`, application.h:32-81; `app_worker_alloc`,
/// application_worker.c:15-34. The event queue is owned by the worker's
/// connects segment manager, not by this record.
#[repr(C)]
pub struct AppWorker<'segment> {
    cacheline0: CacheLineAlignMark,
    worker_index: u32,
    worker_map_index: u32,
    application: u32,
    event_queue: &'segment SvmMsgQ,
    connects_segment_manager: u32,
    listeners: HashMap<SessionHandle, u32>,
    half_open: Pool<SessionHandle>,
    api_client: u32,
    mq_congested: AtomicU32,
    app_is_builtin: bool,
    events_by_worker: Vec<UnsafeCell<Vec<SessionEvent>>>,
    worker_mq_congested: Vec<UnsafeCell<bool>>,
}

// SAFETY: event and worker-congestion slots are constructed before publication;
// the corresponding Session worker exclusively mutates its own slot. Main
// Thread changes the remaining fields only while WorkerBarrier stops readers.
unsafe impl Sync for AppWorker<'_> {}

impl<'segment> AppWorker<'segment> {
    /// VPP: `application_alloc_worker_and_init`, application.c:986-1020.
    pub fn init(
        application: u32,
        worker_index: u32,
        worker_map_index: u32,
        connects_segment_manager: u32,
        event_queue: &'segment SvmMsgQ,
        api_client: u32,
        app_is_builtin: bool,
    ) -> Self {
        let count = worker_count() as usize;
        Self {
            cacheline0: CacheLineAlignMark,
            worker_index,
            worker_map_index,
            application,
            event_queue,
            connects_segment_manager,
            listeners: HashMap::new(),
            half_open: Pool::new(),
            api_client,
            mq_congested: AtomicU32::new(0),
            app_is_builtin,
            events_by_worker: (0..count).map(|_| UnsafeCell::new(Vec::new())).collect(),
            worker_mq_congested: (0..count).map(|_| UnsafeCell::new(false)).collect(),
        }
    }

    #[inline(always)]
    pub const fn index(&self) -> u32 {
        self.worker_index
    }

    #[inline(always)]
    pub const fn application(&self) -> u32 {
        self.application
    }

    #[inline(always)]
    pub const fn worker_map_index(&self) -> u32 {
        self.worker_map_index
    }

    #[inline(always)]
    pub const fn connects_segment_manager(&self) -> u32 {
        self.connects_segment_manager
    }

    #[inline(always)]
    pub const fn event_queue(&self) -> &'segment SvmMsgQ {
        self.event_queue
    }

    /// VPP: `app_worker_add_event`, application_worker.c:934-951.
    /// The first event schedules this app worker on its Session worker.
    pub fn add_event(&self, session: &Session, event: SessionEventType) {
        let worker = session.handle().worker_index;
        let slot = self
            .events_by_worker
            .get(worker as usize)
            .expect("Application event worker entry is preconstructed");
        // SAFETY: this Session worker is the sole executor of its event slot.
        let events = unsafe { &mut *slot.get() };
        events.push(SessionEvent::from((event, session.handle().session_index)));
    }

    /// VPP: `app_worker_add_event_custom`, application_worker.c:954-967.
    pub fn add_event_custom(&self, thread_index: u32, event: &SessionEvent) {
        let slot = self
            .events_by_worker
            .get(thread_index as usize)
            .expect("Application event worker entry is preconstructed");
        // SAFETY: the selected Session worker exclusively mutates its slot.
        unsafe { &mut *slot.get() }.push(*event);
    }

    #[inline(always)]
    /// # Safety
    /// The caller must be the sole executor of the selected Session worker,
    /// or hold WorkerBarrier while that worker is stopped.
    pub unsafe fn events(&self, worker: u32) -> Option<&mut Vec<SessionEvent>> {
        // SAFETY: the caller owns this worker's execution or holds the barrier.
        self.events_by_worker
            .get(worker as usize)
            .map(|slot| unsafe { &mut *slot.get() })
    }

    /// VPP: `app_worker_flush_events_inline`, session_input.c:80-389. The
    /// initial event count is bounded; a callback can append later events to
    /// the same worker slot without invalidating an outstanding Vec borrow.
    #[inline(always)]
    pub fn flush_events(
        &self,
        application: &Application<'_>,
        runtime: &mut DataPlaneMain,
        session_worker: &mut SessionWorker,
    ) -> Result<bool, ApplicationError> {
        let worker = session_worker.worker_index();
        let slot = self
            .events_by_worker
            .get(worker as usize)
            .expect("AppWorker event slots cover every Session worker");
        // SAFETY: this Session worker exclusively executes its own slot.
        let count = unsafe { (&*slot.get()).len().min(128) };
        let mut consumed = 0;
        let mut callback_error = None;
        let mut mq_blocked = false;
        while consumed < count {
            // SAFETY: this worker exclusively executes its slot. No borrow of
            // the Vec survives the callback, which may enqueue another event.
            let event = unsafe { (&*slot.get())[consumed] };
            let event_type = SessionEventType::try_from(event.event_type)
                .expect("AppWorker received a registered Session event type");
            // VPP session_types.h:476-492: these fields overlay as_u64[2].
            let payload = [
                u64::from(event.session_index) | (u64::from(event.worker_index) << 32),
                event.rpc_sequence,
            ];
            let handle = if matches!(
                event_type,
                SessionEventType::BuiltinRx
                    | SessionEventType::TxMain
                    | SessionEventType::Bound
                    | SessionEventType::UnlistenReply
            ) {
                event.session_handle()
            } else {
                SessionHandle {
                    worker_index: worker,
                    session_index: event.session_index(),
                }
            };
            let valid_session =
                session_worker
                    .session(handle.session_index)
                    .is_some_and(|session| {
                        session.handle() == handle
                            && session.application_worker() == Some(self.worker_index)
                    });
            if !self.app_is_builtin
                && matches!(event_type, SessionEventType::Rx | SessionEventType::Tx)
            {
                if self.event_queue.is_full() {
                    mq_blocked = true;
                    break;
                }
                let mut producer = match self.event_queue.producer(SvmQueueConditionalWait::Nowait)
                {
                    Ok(producer) => producer,
                    Err(SvmMsgQError::LockBusy) => {
                        mq_blocked = true;
                        break;
                    }
                    Err(source) => {
                        callback_error = Some(ApplicationError::MessageAllocation { source });
                        break;
                    }
                };
                let descriptor = match producer.alloc_msg_on_ring(0) {
                    Ok(descriptor) => descriptor,
                    Err(SvmMsgQError::QueueFull | SvmMsgQError::RingFull { .. }) => {
                        mq_blocked = true;
                        break;
                    }
                    Err(source) => {
                        callback_error = Some(ApplicationError::MessageAllocation { source });
                        break;
                    }
                };
                producer
                    .write(descriptor, &event)
                    .expect("IO ring element matches the Session event layout");
                producer
                    .add(descriptor)
                    .expect("locked, non-full Application MQ accepts allocated descriptor");
                if event_type == SessionEventType::Rx && valid_session {
                    session_worker
                        .session_mut(handle.session_index)
                        .expect("validated RX Session remains allocated")
                        .clear_rx_event();
                }
            } else {
                match event_type {
                    SessionEventType::Rx | SessionEventType::BuiltinRx => {
                        if valid_session {
                            let session = session_worker
                                .session_mut(handle.session_index)
                                .expect("validated RX Session remains allocated");
                            session.clear_rx_event();
                            if session.rx_ready() {
                                if let Some(callback) = application.builtin_rx {
                                    callback(session);
                                }
                            }
                        }
                    }
                    SessionEventType::Tx | SessionEventType::TxMain => {
                        if valid_session {
                            if let Some(callback) = application.builtin_tx {
                                let session = session_worker
                                    .session_mut(handle.session_index)
                                    .expect("validated TX Session remains allocated");
                                callback(session);
                            }
                        }
                    }
                    SessionEventType::Accepted => {
                        if valid_session {
                            let state = session_worker
                                .session(handle.session_index)
                                .expect("validated accepted Session remains allocated")
                                .load_state();
                            let accepted = {
                                let session = session_worker
                                    .session_mut(handle.session_index)
                                    .expect("validated accepted Session remains allocated");
                                (application.accepted)(session).is_ok()
                            };
                            if !accepted {
                                if let Some(session) =
                                    session_worker.session_mut(handle.session_index)
                                {
                                    session.detach_application();
                                }
                            } else if matches!(
                                state,
                                SessionState::TransportClosing
                                    | SessionState::Closing
                                    | SessionState::AppClosed
                                    | SessionState::TransportClosed
                                    | SessionState::Closed
                                    | SessionState::TransportDeleted
                            ) {
                                // VPP: session_input.c:157-195. The accept callback
                                // may have set Ready after transport close began.
                                let session = session_worker
                                    .session_mut(handle.session_index)
                                    .expect("accepted Session remains allocated after callback");
                                let app_closed = session.app_closed()
                                    || matches!(
                                        state,
                                        SessionState::AppClosed
                                            | SessionState::Closed
                                            | SessionState::TransportDeleted
                                    )
                                    || matches!(
                                        session.load_state(),
                                        SessionState::AppClosed
                                            | SessionState::Closed
                                            | SessionState::TransportDeleted
                                    );
                                if !app_closed {
                                    session.store_state(state);
                                }
                                let rx_pending = session
                                    .rx_fifo()
                                    .is_some_and(|fifo| fifo.max_dequeue() != 0);
                                if rx_pending {
                                    if let Some(callback) = application.builtin_rx {
                                        callback(session);
                                    }
                                }
                                if !app_closed {
                                    (application.disconnected)(session);
                                }
                            } else if let Some(session) =
                                session_worker.session_mut(handle.session_index)
                            {
                                session.set_rx_ready(true);
                            }
                        }
                    }
                    SessionEventType::Connected => {
                        let connected = (application.connected)(
                            self.worker_index,
                            payload[1] >> 32,
                            if valid_session {
                                session_worker.session_mut(handle.session_index)
                            } else {
                                None
                            },
                            None,
                        )
                        .is_ok();
                        if !connected {
                            if valid_session {
                                if let Some(session) =
                                    session_worker.session_mut(handle.session_index)
                                {
                                    session.detach_application();
                                }
                            }
                        } else if valid_session {
                            if let Some(session) = session_worker.session_mut(handle.session_index)
                            {
                                session.set_rx_ready(true);
                            }
                        }
                    }
                    SessionEventType::Disconnected | SessionEventType::Reset => {
                        if valid_session {
                            if let Some(session) = session_worker.session_mut(handle.session_index)
                            {
                                session.set_rx_ready(false);
                            }
                            if !session_worker
                                .session(handle.session_index)
                                .expect("validated Session remains allocated")
                                .app_closed()
                            {
                                // VPP session_input.c:221-237 suppresses repeated
                                // close notifications after the app requests close.
                                if event_type == SessionEventType::Disconnected {
                                    let session = session_worker
                                        .session_mut(handle.session_index)
                                        .expect("validated disconnect Session remains allocated");
                                    (application.disconnected)(session);
                                } else {
                                    let session = session_worker
                                        .session_mut(handle.session_index)
                                        .expect("validated reset Session remains allocated");
                                    (application.reset)(session);
                                }
                            }
                        }
                    }
                    SessionEventType::TransportClosed => {
                        if valid_session {
                            if let Some(callback) = application.transport_closed {
                                let session = session_worker
                                    .session_mut(handle.session_index)
                                    .expect("validated transport-closed Session remains allocated");
                                callback(session);
                            }
                        }
                    }
                    SessionEventType::Cleanup => {
                        let cleanup = SessionCleanup::try_from((payload[0] >> 32) as u8)
                            .expect("Session cleanup event has a registered stage");
                        if valid_session {
                            if let Some(callback) = application.cleanup {
                                let session = session_worker
                                    .session_mut(handle.session_index)
                                    .expect("validated cleanup Session remains allocated");
                                callback(session, cleanup);
                            }
                            if cleanup == SessionCleanup::Session {
                                session_worker
                                    .session_mut(handle.session_index)
                                    .expect("Application cleanup retains its Session")
                                    .detach_application();
                            } else {
                                session_worker
                                    .finish_transport_cleanup(runtime, handle)
                                    .expect("registered transport completes its cleanup after the Application callback");
                            }
                        }
                        // VPP session_input.c:282-305 skips the application
                        // callback after refusal, but still frees the Session.
                        // The handle and owner check also exclude a reused
                        // pool slot or a Session attached to another worker.
                        if cleanup == SessionCleanup::Session
                            && session_worker
                                .session_from_handle(handle)
                                .is_some_and(|session| {
                                    session.application_worker().is_none()
                                        || session.application_worker() == Some(self.worker_index)
                                })
                        {
                            session_worker
                                .cleanup(handle)
                                .expect("validated Session remains allocated until cleanup");
                        }
                    }
                    SessionEventType::HalfCleanup => {
                        if valid_session {
                            if let Some(callback) = application.half_open_cleanup {
                                let session = session_worker
                                    .session_mut(handle.session_index)
                                    .expect("validated half-open Session remains allocated");
                                callback(session);
                            }
                        }
                    }
                    SessionEventType::Migrated => {
                        if valid_session {
                            if let Some(callback) = application.migrated {
                                let destination = SessionHandle {
                                    session_index: payload[1] as u32,
                                    worker_index: (payload[1] >> 32) as u32,
                                };
                                let session = session_worker
                                    .session_mut(handle.session_index)
                                    .expect("validated migrated Session remains allocated");
                                callback(session, destination);
                            }
                        }
                    }
                    SessionEventType::Bound => {
                        if let Some(callback) = application.listened {
                            if let Err(source) =
                                callback(self.worker_index, (payload[1] >> 32) as u32, handle, None)
                            {
                                callback_error = Some(source);
                                break;
                            }
                        }
                    }
                    SessionEventType::UnlistenReply => {
                        if let Some(callback) = application.unlistened {
                            callback(self.worker_index, handle, (payload[1] >> 32) as u32, None);
                        }
                    }
                    SessionEventType::AppAddSegment | SessionEventType::AppDelSegment => {
                        let callback = if event_type == SessionEventType::AppAddSegment {
                            application.add_segment
                        } else {
                            application.del_segment
                        };
                        if let Err(source) = callback(self.worker_index, payload[1]) {
                            callback_error = Some(source);
                            break;
                        }
                    }
                    _ => panic!("AppWorker received unsupported Session event {event_type:?}"),
                }
            }
            // VPP: `session_close`, session.c:1540-1574, and
            // `session_program_transport_ctrl_evt`, session.c:223-245.
            // The callback's mutable Session borrow ends before the local
            // worker control list receives the Close request.
            if valid_session
                && session_worker
                    .session_mut(handle.session_index)
                    .is_some_and(Session::take_close_request)
            {
                session_worker.program_close(runtime, handle);
            }
            consumed += 1;
        }
        // SAFETY: only this Session worker mutates its event slot.
        let events = unsafe { &mut *slot.get() };
        events.drain(..consumed);
        let pending = !events.is_empty();
        let congestion = self
            .worker_mq_congested
            .get(worker as usize)
            .expect("AppWorker congestion slots cover every Session worker");
        // SAFETY: this worker exclusively mutates its congestion flag.
        let congestion = unsafe { &mut *congestion.get() };
        if mq_blocked && !*congestion {
            *congestion = true;
            self.mq_congested.fetch_add(1, Ordering::AcqRel);
        } else if !mq_blocked && *congestion {
            *congestion = false;
            self.mq_congested.fetch_sub(1, Ordering::AcqRel);
        }
        if let Some(source) = callback_error {
            return Err(source);
        }
        Ok(pending)
    }
}

/// VPP: `app_main_t`, application.h:205-238. Name keys borrow from pinned
/// Application records and are removed before those records are freed.
pub struct ApplicationMain<'app> {
    application_by_name: UnsafeCell<HashMap<&'app str, u32>>,
    application_by_api_client: UnsafeCell<HashMap<u32, u32>>,
    listeners: UnsafeCell<Pool<ApplicationListener>>,
    applications: UnsafeCell<Pool<Pin<Box<Application<'app>>>>>,
    workers: UnsafeCell<Pool<AppWorker<'app>>>,
    pending_rx_mq_heads: UnsafeCell<Vec<u32>>,
    pending_connects: SpinLock<Vec<u32>>,
    burst_connects: UnsafeCell<Vec<SessionControlData>>,
    connect_data: UnsafeCell<Pool<SessionControlData>>,
    worker_count: u32,
}

// SAFETY: mutation is Main Thread work before worker launch or under the
// existing WorkerBarrier. Session workers read only the record selected for
// their event and never retain a borrow across barrier acknowledgement.
unsafe impl Sync for ApplicationMain<'_> {}

static APP_MAIN: OnceLock<ApplicationMain<'static>> = OnceLock::new();

impl ApplicationMain<'static> {
    /// VPP: `application_init`, application.c:2090-2106.
    pub fn init(worker_count: u32) -> Result<(), ApplicationError> {
        let main = Self {
            application_by_name: UnsafeCell::new(HashMap::new()),
            application_by_api_client: UnsafeCell::new(HashMap::new()),
            listeners: UnsafeCell::new(Pool::new()),
            applications: UnsafeCell::new(Pool::new()),
            workers: UnsafeCell::new(Pool::new()),
            pending_rx_mq_heads: UnsafeCell::new(vec![u32::MAX; worker_count as usize]),
            pending_connects: SpinLock::new(Vec::new()),
            burst_connects: UnsafeCell::new(Vec::new()),
            connect_data: UnsafeCell::new(Pool::new()),
            worker_count,
        };
        assert!(
            APP_MAIN.set(main).is_ok(),
            "Application Main initializes once"
        );
        Ok(())
    }

    #[inline(always)]
    pub fn global() -> Option<&'static Self> {
        APP_MAIN.get()
    }
}

impl<'app> ApplicationMain<'app> {
    /// VPP: `app_worker_init_accepted`/`app_worker_accept_notify`,
    /// application_worker.c:493-517,593-598. A congested Application MQ
    /// declines this accept without attaching FIFOs or scheduling an event.
    pub fn init_accepted(
        &self,
        runtime: &hammer_runtime::DataPlaneMain,
        session_worker: &mut SessionWorker,
        listener: u32,
        session: SessionHandle,
    ) -> Result<ApplicationEventResult, ApplicationError> {
        // SAFETY: listener publication/removal requires WorkerBarrier, so
        // workers may read the installed record without a listener-table lock.
        let listeners = unsafe { &*self.listeners.get() };
        let record = listeners
            .get(listener)
            .ok_or(ApplicationError::NoListener { listener })?;
        let listening_session = record
            .handle()
            .expect("an accepting listener retains a listening Session");
        assert_eq!(
            session_worker
                .session_from_handle(session)
                .expect("transport-created Session remains allocated")
                .listener_handle(),
            listening_session,
            "accepted Session belongs to this Application listener"
        );
        let worker_map = record.select_worker()?;
        let application = record.application();
        // SAFETY: Application detach also requires WorkerBarrier; this worker
        // owns the current execution interval.
        let application_record = unsafe { self.application(application) }
            .expect("Application listener retains its owning Application");
        let worker =
            application_record
                .worker(worker_map)
                .ok_or(ApplicationError::WorkerMissing {
                    application,
                    worker: worker_map,
                })?;
        // SAFETY: AppWorker pool entries are barrier-published and remain live
        // while their ApplicationListener accepts Sessions.
        let app_worker =
            unsafe { self.worker(worker) }.expect("accepting worker map retains its AppWorker");
        if app_worker.mq_congested.load(Ordering::Acquire) != 0 {
            return Ok(ApplicationEventResult::Deferred);
        }
        let manager = *app_worker
            .listeners
            .get(&listening_session)
            .expect("accepting AppWorker retains its listener Segment Manager");
        let segment_main = SegmentManagerMain::global()
            .expect("Segment Manager Main remains initialized while accepting");
        // SAFETY: listener detach waits for WorkerBarrier before removing the
        // manager, and this Session worker exclusively owns its FIFO slice.
        let segment = unsafe { segment_main.get(manager) }
            .expect("accepting listener Segment Manager remains allocated");
        session_worker
            .attach_application(session, worker, segment)
            .map_err(|source| match source {
                SegmentManagerError::SegmentCreate { source }
                | SegmentManagerError::FifoAllocation { source }
                | SegmentManagerError::EventQueueAllocation { source } => {
                    ApplicationError::SegmentCreate { source }
                }
                SegmentManagerError::SegmentNoSpace => ApplicationError::SegmentNoSpace,
                SegmentManagerError::InvalidWorker { .. }
                | SegmentManagerError::Detached
                | SegmentManagerError::InvalidEventQueueCapacity { .. }
                | SegmentManagerError::ActiveFifos => {
                    panic!("installed listener Segment Manager accepts this Session worker")
                }
            })?;
        let accepted = session_worker
            .session_from_handle(session)
            .expect("accepted Session remains allocated after FIFO attachment");
        app_worker.add_event(accepted, SessionEventType::Accepted);
        session_worker.program_app_worker(runtime, app_worker.index());
        Ok(ApplicationEventResult::Queued)
    }

    /// VPP: `app_listener_alloc`, application.c:24-38. The accepting-worker
    /// bitmap remains empty until `app_worker_start_listen` succeeds.
    pub fn allocate_listener(
        &self,
        application: u32,
        worker_map: u32,
        opaque: u64,
    ) -> Result<u32, ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application listener allocation requires Main Thread and stopped Data Workers"
        );
        let applications = unsafe { &mut *self.applications.get() };
        let application_record = applications
            .get_mut(application)
            .expect("listener allocation requires an attached Application");
        let application_record = unsafe { application_record.as_mut().get_unchecked_mut() };
        if application_record.worker_maps.get(worker_map).is_none() {
            return Err(ApplicationError::WorkerMissing {
                application,
                worker: worker_map,
            });
        }
        let listeners = unsafe { &mut *self.listeners.get() };
        let listener = listeners.insert(ApplicationListener::new(application, opaque));
        listeners
            .get_mut(listener)
            .expect("inserted Application listener remains allocated")
            .index = listener;
        application_record.listeners.insert(listener);
        Ok(listener)
    }

    /// VPP: `app_worker_listen_sep`, application_worker.c:230-322.
    pub fn attach_listener_session(
        &self,
        session_main: &super::core::SessionMain,
        listener: u32,
        global: Option<SessionHandle>,
        local: Option<SessionHandle>,
    ) -> Result<(), ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application listener publication requires Main Thread and stopped Data Workers"
        );
        assert!(
            global.is_some() || local.is_some(),
            "Application listener publication requires a listening Session"
        );
        unsafe { &mut *self.listeners.get() }
            .get_mut(listener)
            .ok_or(ApplicationError::NoListener { listener })?
            .attach_session(global, local)?;
        session_main.attach_listener(listener, global, local);
        Ok(())
    }

    /// VPP: `app_worker_start_listen`, application_worker.c:338-384.
    pub fn attach_listener_worker(
        &self,
        listener: u32,
        worker_map: u32,
    ) -> Result<(), ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application listener worker attach requires Main Thread and stopped Data Workers"
        );
        let listeners = unsafe { &mut *self.listeners.get() };
        let record = listeners
            .get(listener)
            .ok_or(ApplicationError::NoListener { listener })?;
        if record.workers.is_set(worker_map as usize) {
            return Err(ApplicationError::ListenerWorkerAttached {
                listener,
                worker: worker_map,
            });
        }
        let global = record.global_session;
        let local = record.local_session;
        assert!(
            global.is_some() || local.is_some(),
            "Application listener Session must exist before worker attach"
        );
        let application = record.application;
        let applications = unsafe { &*self.applications.get() };
        let application_record = applications
            .get(application)
            .expect("Application listener retains its owning Application")
            .as_ref()
            .get_ref();
        let worker = application_record
            .worker_maps
            .get(worker_map)
            .ok_or(ApplicationError::WorkerMissing {
                application,
                worker: worker_map,
            })?
            .worker;
        let backend = if application_record.flags.contains(ApplicationFlags::BUILTIN)
            && !application_record
                .flags
                .contains(ApplicationFlags::BUILTIN_MEMFD)
        {
            SsvmSegmentBackend::Private
        } else {
            SsvmSegmentBackend::Memfd
        };
        let segment_main = SegmentManagerMain::global()
            .expect("Segment Manager Main initializes before Application listeners");
        let manager = segment_main
            .allocate(
                worker,
                application_record.segment,
                self.worker_count,
                backend,
                application_record
                    .flags
                    .contains(ApplicationFlags::HUGE_PAGE),
                SegmentManagerFlags::LISTENER,
            )
            .map_err(|error| match error {
                SegmentManagerError::SegmentCreate { source }
                | SegmentManagerError::FifoAllocation { source }
                | SegmentManagerError::EventQueueAllocation { source } => {
                    ApplicationError::SegmentCreate { source }
                }
                SegmentManagerError::SegmentNoSpace => ApplicationError::SegmentNoSpace,
                SegmentManagerError::InvalidEventQueueCapacity { capacity } => {
                    ApplicationError::EventQueueCapacityInvalid { capacity }
                }
                SegmentManagerError::Detached
                | SegmentManagerError::InvalidWorker { .. }
                | SegmentManagerError::ActiveFifos => {
                    panic!("new listener Segment Manager cannot already be in use")
                }
            })?;
        let workers = unsafe { &mut *self.workers.get() };
        let app_worker = workers
            .get_mut(worker)
            .expect("Application worker mapping retains its AppWorker");
        assert_eq!(app_worker.application, application);
        if let Some(session) = global {
            app_worker.listeners.insert(session, manager);
        }
        if let Some(session) = local {
            app_worker.listeners.insert(session, manager);
        }
        listeners
            .get_mut(listener)
            .expect("Application listener remains allocated during attach")
            .attach_worker(worker_map)
            .expect("validated listener worker was not previously attached");
        Ok(())
    }

    /// VPP: `app_worker_stop_listen`, application_worker.c:466-490.
    pub fn detach_listener_worker(
        &self,
        listener: u32,
        worker_map: u32,
    ) -> Result<(), ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application listener worker detach requires Main Thread and stopped Data Workers"
        );
        let listeners = unsafe { &mut *self.listeners.get() };
        let record = listeners
            .get(listener)
            .ok_or(ApplicationError::NoListener { listener })?;
        if !record.workers.is_set(worker_map as usize) {
            return Ok(());
        }
        let global = record.global_session;
        let local = record.local_session;
        let application = record.application;
        let applications = unsafe { &*self.applications.get() };
        let application_record = applications
            .get(application)
            .expect("Application listener retains its owning Application")
            .as_ref()
            .get_ref();
        let worker = application_record
            .worker_maps
            .get(worker_map)
            .expect("attached listener worker remains mapped")
            .worker;
        let workers = unsafe { &mut *self.workers.get() };
        let app_worker = workers
            .get_mut(worker)
            .expect("attached listener worker remains allocated");
        let handle = global.or(local).expect("attached listener has a Session");
        let manager = *app_worker
            .listeners
            .get(&handle)
            .expect("attached listener retains its Segment Manager");
        let segment_main = SegmentManagerMain::global()
            .expect("Segment Manager Main remains initialized during unlisten");
        segment_main
            .detach(manager)
            .expect("a fully initialized listener Segment Manager can detach")
            .expect("listener Segment Manager remains allocated until unlisten");
        if let Some(session) = global {
            app_worker.listeners.remove(&session);
        }
        if let Some(session) = local {
            app_worker.listeners.remove(&session);
        }
        listeners
            .get_mut(listener)
            .expect("Application listener remains allocated during detach")
            .detach_worker(worker_map)
            .expect("attached listener worker remains in its accepting bitmap");
        Ok(())
    }

    /// VPP: `app_listener_cleanup`, application.c:146-170. The concrete
    /// transport owner stops the listening connection before this call.
    pub fn remove_listener(
        &self,
        session_main: &super::core::SessionMain,
        listener: u32,
    ) -> Result<(), ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application listener removal requires Main Thread and stopped Data Workers"
        );
        let (application, global, local) = {
            let listeners = unsafe { &*self.listeners.get() };
            let record = listeners
                .get(listener)
                .ok_or(ApplicationError::NoListener { listener })?;
            (
                record.application,
                record.global_session,
                record.local_session,
            )
        };
        loop {
            let worker = unsafe { &*self.listeners.get() }
                .get(listener)
                .expect("Application listener remains allocated during worker detach")
                .workers
                .first_set();
            let Some(worker) = worker else {
                break;
            };
            self.detach_listener_worker(listener, worker as u32)?;
        }
        if let Some(session) = global {
            session_main
                .cleanup_listening_session(session)
                .expect("Application listener retains its global listening Session");
        }
        if let Some(session) = local
            && Some(session) != global
        {
            session_main
                .cleanup_listening_session(session)
                .expect("Application listener retains its local listening Session");
        }
        let applications = unsafe { &mut *self.applications.get() };
        let application_record = applications
            .get_mut(application)
            .expect("Application listener retains its owning Application");
        let application_record = unsafe { application_record.as_mut().get_unchecked_mut() };
        let slot = application_record
            .listeners
            .iter()
            .find_map(|(slot, &index)| (index == listener).then_some(slot))
            .expect("Application retains its listener pool entry");
        application_record.listeners.remove(slot);
        unsafe { &mut *self.listeners.get() }
            .remove(listener)
            .expect("Application listener remains allocated until removal");
        Ok(())
    }

    /// VPP: `application_alloc_worker_and_init`, application.c:986-1020.
    /// The new worker and its Segment Manager are published together while
    /// Data Workers are stopped.
    pub fn attach_worker(
        &self,
        application: u32,
        api_client: u32,
    ) -> Result<Option<u32>, ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application Worker attach requires Main Thread and stopped Data Workers"
        );
        // SAFETY: Main Thread owns the Application pool while workers stop.
        let applications = unsafe { &mut *self.applications.get() };
        let Some(application_record) = applications.get_mut(application) else {
            return Ok(None);
        };
        let application_record = unsafe { application_record.as_mut().get_unchecked_mut() };
        let backend = if application_record.flags.contains(ApplicationFlags::BUILTIN)
            && !application_record
                .flags
                .contains(ApplicationFlags::BUILTIN_MEMFD)
        {
            SsvmSegmentBackend::Private
        } else {
            SsvmSegmentBackend::Memfd
        };
        let segment_main = SegmentManagerMain::global()
            .expect("Segment Manager Main initializes before Application Worker attach");
        let manager = segment_main
            .allocate(
                u32::MAX,
                application_record.segment,
                self.worker_count,
                backend,
                application_record
                    .flags
                    .contains(ApplicationFlags::HUGE_PAGE),
                SegmentManagerFlags::CONNECTS,
            )
            .map_err(|error| match error {
                SegmentManagerError::SegmentCreate { source }
                | SegmentManagerError::FifoAllocation { source }
                | SegmentManagerError::EventQueueAllocation { source } => {
                    ApplicationError::SegmentCreate { source }
                }
                SegmentManagerError::SegmentNoSpace => ApplicationError::SegmentNoSpace,
                SegmentManagerError::InvalidEventQueueCapacity { capacity } => {
                    ApplicationError::EventQueueCapacityInvalid { capacity }
                }
                SegmentManagerError::Detached
                | SegmentManagerError::InvalidWorker { .. }
                | SegmentManagerError::ActiveFifos => {
                    panic!("new Segment Manager cannot already be in use")
                }
            })?;
        // SAFETY: this new manager remains owned by the application worker
        // until detach; the barrier excludes readers during publication.
        let queue: &'app SvmMsgQ = unsafe {
            &*(segment_main
                .get(manager)
                .expect("new Segment Manager remains allocated")
                .event_queue() as *const SvmMsgQ)
        };
        let workers = unsafe { &mut *self.workers.get() };
        let worker_map = application_record
            .worker_maps
            .insert(ApplicationWorkerMap { worker: u32::MAX });
        let worker = workers.insert(AppWorker::init(
            application,
            u32::MAX,
            worker_map,
            manager,
            queue,
            api_client,
            application_record.flags.contains(ApplicationFlags::BUILTIN),
        ));
        workers
            .get_mut(worker)
            .expect("inserted AppWorker")
            .worker_index = worker;
        application_record
            .worker_maps
            .get_mut(worker_map)
            .expect("inserted worker mapping")
            .worker = worker;
        application_record.workers.insert(worker);
        segment_main.assign_owner(manager, worker);
        if !application_record.flags.contains(ApplicationFlags::BUILTIN) {
            unsafe { &mut *self.application_by_api_client.get() }.insert(api_client, application);
        }
        Ok(Some(worker_map))
    }

    /// # Safety
    /// Caller owns this AppWorker's Session worker or holds WorkerBarrier;
    /// detach must not run until the borrow ends.
    #[inline(always)]
    pub unsafe fn worker(&self, worker: u32) -> Option<&AppWorker<'app>> {
        // SAFETY: the caller preserves the pool entry lifetime.
        unsafe { &*self.workers.get() }.get(worker)
    }

    /// VPP: `session_wrk_flush_events`, session_input.c:350-388. A Session
    /// worker visits only AppWorkers whose first event set its pending bit.
    pub fn flush_worker_events(
        &self,
        runtime: &mut DataPlaneMain,
        session_worker: &mut SessionWorker,
    ) -> Result<bool, ApplicationError> {
        let mut pending = session_worker.pending_app_workers().first_set();
        while let Some(index) = pending {
            let worker = index as u32;
            // SAFETY: this Session worker owns the selected event slot, and
            // application detach clears pending bits under WorkerBarrier.
            let app_worker = unsafe { self.worker(worker) }
                .expect("pending AppWorker remains allocated until its events are flushed");
            // SAFETY: the AppWorker retains its Application identity while
            // worker execution excludes Main Thread detach.
            let application = unsafe { self.application(app_worker.application) }
                .expect("pending AppWorker retains its owning Application");
            if !app_worker.flush_events(application, runtime, session_worker)? {
                session_worker.clear_pending_app_worker(worker);
            }
            pending = session_worker.pending_app_workers().next_set(index);
        }
        Ok(session_worker.pending_app_workers().first_set().is_some())
    }

    /// VPP: `vnet_app_worker_add_del` and `app_worker_free`,
    /// application.c:1023-1074; application_worker.c:44-126.
    pub fn detach_worker(
        &self,
        application: u32,
        worker_map: u32,
    ) -> Result<Option<()>, ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application Worker detach requires Main Thread and stopped Data Workers"
        );
        // SAFETY: Main Thread owns these pools while Data Workers stop.
        let applications = unsafe { &mut *self.applications.get() };
        let Some(application_record) = applications.get_mut(application) else {
            return Ok(None);
        };
        let application_record = unsafe { application_record.as_mut().get_unchecked_mut() };
        if !application_record.listeners.is_empty() {
            return Err(ApplicationError::ListenersActive { application });
        }
        let Some(worker) = application_record
            .worker_maps
            .get(worker_map)
            .map(|entry| entry.worker)
        else {
            return Ok(None);
        };
        let workers = unsafe { &mut *self.workers.get() };
        let (manager, api_client) = {
            let Some(mut app_worker) = workers.remove(worker) else {
                panic!("Application {application} references missing AppWorker {worker}");
            };
            for events in &mut app_worker.events_by_worker {
                events.get_mut().clear();
            }
            let session_main = super::core::SessionMain::global()
                .expect("Session Main initializes before Application Worker detach");
            for worker_index in 0..self.worker_count {
                session_main.clear_pending_app_worker(worker_index, worker);
            }
            (app_worker.connects_segment_manager, app_worker.api_client)
        };
        SegmentManagerMain::global()
            .expect("Segment Manager Main remains initialized")
            .detach(manager)
            .expect("a fully initialized Segment Manager can detach");
        let slot = application_record
            .workers
            .iter()
            .find_map(|(slot, &index)| (index == worker).then_some(slot))
            .expect("Application owns its AppWorker pool entry");
        application_record.workers.remove(slot);
        application_record.worker_maps.remove(worker_map);
        if !application_record.flags.contains(ApplicationFlags::BUILTIN) {
            unsafe { &mut *self.application_by_api_client.get() }.remove(&api_client);
        }
        Ok(Some(()))
    }

    /// VPP: `application_alloc_and_init`, application.c:664-767.
    pub fn attach(&self, config: ApplicationConfig) -> Result<u32, ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application attach requires Main Thread and stopped Data Workers"
        );
        if self.worker_count == 0 {
            return Err(ApplicationError::NoDataWorkers);
        }
        if config.flags.contains(ApplicationFlags::BUILTIN) && config.builtin_rx.is_none() {
            return Err(ApplicationError::BuiltinRxMissing);
        }
        // VPP: `application_alloc_and_init`, application.c:699-708.
        let mut flags = config.flags;
        if !flags.intersects(ApplicationFlags::GLOBAL_SCOPE | ApplicationFlags::LOCAL_SCOPE) {
            flags.insert(ApplicationFlags::GLOBAL_SCOPE);
        }
        // SAFETY: the Main Thread owns mutation while the barrier excludes
        // Session worker readers of the name table and Application pool.
        let names = unsafe { &mut *self.application_by_name.get() };
        if let Some(&application) = names.get(config.name.as_str()) {
            return Err(ApplicationError::ApplicationAttached { application });
        }
        let q_nitems = config
            .segment
            .event_queue_size
            .checked_next_power_of_two()
            .ok_or(ApplicationError::EventQueueCapacityInvalid {
                capacity: config.segment.event_queue_size,
            })?;
        if q_nitems == 0 {
            return Err(ApplicationError::EventQueueCapacityInvalid { capacity: 0 });
        }
        let backend = if config.flags.contains(ApplicationFlags::BUILTIN)
            && !config.flags.contains(ApplicationFlags::BUILTIN_MEMFD)
        {
            SsvmSegmentBackend::Private
        } else {
            SsvmSegmentBackend::Memfd
        };
        let mapping = Arc::new(
            SsvmPrivate::server_init_fifo_segment(&SsvmConfig {
                backend,
                name: format!(
                    "hammer-app-rx-{:x}",
                    SEG_NAME_COUNTER.fetch_add(1, Ordering::Relaxed)
                ),
                size: config.segment.segment_size,
                requested_va: 0,
                huge_page: config.flags.contains(ApplicationFlags::HUGE_PAGE),
                attach_timeout: Duration::from_secs(1),
            })
            .map_err(|source| ApplicationError::SegmentCreate {
                source: FifoSegmentError::Ssvm { source },
            })?,
        );
        let mut rx_mq_segment = SvmFifoSegment::new(
            mapping,
            SvmFifoSegmentConfig {
                slices: self.worker_count,
                ..SvmFifoSegmentConfig::default()
            },
        )
        .map_err(|source| ApplicationError::SegmentCreate { source })?;
        let ring = [SvmMsgQRingConfig::new(
            config.segment.event_queue_size,
            size_of::<SessionEvent>() as u32,
        )];
        let queue_config = SvmMsgQConfig {
            consumer_pid: std::process::id() as i32,
            q_nitems,
            rings: &ring,
        };
        for worker in 0..self.worker_count {
            rx_mq_segment
                .allocate_message_queue(worker, &queue_config)
                .map_err(|source| ApplicationError::SegmentCreate { source })?;
        }
        let mut application = Box::pin(Application {
            index: u32::MAX,
            flags,
            add_segment: config.add_segment,
            del_segment: config.del_segment,
            accepted: config.accepted,
            connected: config.connected,
            disconnected: config.disconnected,
            reset: config.reset,
            transport_closed: config.transport_closed,
            cleanup: config.cleanup,
            half_open_cleanup: config.half_open_cleanup,
            migrated: config.migrated,
            listened: config.listened,
            unlistened: config.unlistened,
            builtin_rx: config.builtin_rx,
            builtin_tx: config.builtin_tx,
            segment: config.segment,
            worker_maps: Pool::new(),
            name: config.name,
            namespace: config.namespace,
            listeners: Pool::new(),
            workers: Pool::new(),
            rx_mqs: Vec::with_capacity(self.worker_count as usize),
            rx_mq_segment,
            pin: PhantomPinned,
        });
        for worker in 0..self.worker_count {
            let queue = application
                .as_ref()
                .get_ref()
                .rx_mq_segment
                .message_queue(worker)
                .expect("Application RX MQ allocated for every worker")
                as *const SvmMsgQ;
            // SAFETY: the queue Vec is fully built before publication and is
            // never extended afterward. Pin keeps its owning Application at
            // a fixed address; detach drops rx_mqs before the segment.
            let queue: &'app SvmMsgQ = unsafe { &*queue };
            unsafe { application.as_mut().get_unchecked_mut() }
                .rx_mqs
                .push(ApplicationRxMq {
                    next: u32::MAX,
                    previous: u32::MAX,
                    queue,
                    file_index: usize::MAX,
                    application: u32::MAX,
                    flags: ApplicationRxMqFlags::empty(),
                });
        }
        // SAFETY: only Main Thread mutates the pool under the worker barrier.
        let applications = unsafe { &mut *self.applications.get() };
        let index = applications.insert(application);
        let pinned = applications
            .get_mut(index)
            .expect("inserted Application remains in the pool");
        let entry = unsafe { pinned.as_mut().get_unchecked_mut() };
        entry.index = index;
        for mq in &mut entry.rx_mqs {
            mq.application = index;
        }
        // SAFETY: Application is pinned, name is immutable, and detach
        // removes this key before freeing the Application allocation.
        let name: &'app str = unsafe { &*(entry.name.as_str() as *const str) };
        names.insert(name, index);
        if let Err(source) = self.attach_worker(index, u32::MAX) {
            self.detach(index)
                .expect("Application without workers or listeners can be detached");
            return Err(source);
        }
        Ok(index)
    }

    /// VPP: `application_free`, application.c:771-861.
    pub fn detach(&self, application: u32) -> Result<Option<()>, ApplicationError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application detach requires Main Thread and stopped Data Workers"
        );
        // SAFETY: the barrier excludes all worker readers while Main Thread
        // removes the name key before the pinned owner is released.
        let worker_maps: Vec<u32> = {
            let applications = unsafe { &*self.applications.get() };
            let Some(entry) = applications.get(application) else {
                return Ok(None);
            };
            let entry = entry.as_ref().get_ref();
            if !entry.listeners.is_empty() {
                return Err(ApplicationError::ListenersActive { application });
            }
            entry.worker_maps.iter().map(|(index, _)| index).collect()
        };
        for worker_map in worker_maps {
            self.detach_worker(application, worker_map)?;
        }
        let applications = unsafe { &mut *self.applications.get() };
        let entry = applications
            .get(application)
            .expect("Application remains allocated during worker detach")
            .as_ref()
            .get_ref();
        let names = unsafe { &mut *self.application_by_name.get() };
        assert_eq!(names.remove(entry.name.as_str()), Some(application));
        applications
            .remove(application)
            .expect("Application remains allocated until detach");
        Ok(Some(()))
    }

    /// # Safety
    /// The caller must hold the worker's execution ownership or stop all
    /// Data Workers before borrowing an Application record.
    #[inline(always)]
    pub unsafe fn application(&self, application: u32) -> Option<&Application<'app>> {
        // SAFETY: guaranteed by the caller; detach waits for the barrier.
        unsafe { &*self.applications.get() }
            .get(application)
            .map(|entry| entry.as_ref().get_ref())
    }

    /// VPP: `application_t.ns_index`, application.h:139-151.
    #[inline(always)]
    pub fn namespace(&self, application: u32) -> Option<u32> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application namespace lookup requires Main Thread and stopped Data Workers"
        );
        unsafe { &*self.applications.get() }
            .get(application)
            .map(|entry| entry.as_ref().get_ref().namespace)
    }

    /// VPP: `app_listener_get`, application.h:227-229. The Main Thread keeps
    /// the listener pool stable while its Session lookup identity is updated.
    #[inline]
    pub fn listener(&self, listener: u32) -> Option<&ApplicationListener> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application listener lookup requires Main Thread and stopped Data Workers"
        );
        unsafe { &*self.listeners.get() }.get(listener)
    }

    /// VPP: `app_listener_get_w_handle`, application.c:78-85.
    #[inline]
    pub fn listener_for_session(
        &self,
        session_main: &super::core::SessionMain,
        session: SessionHandle,
    ) -> Option<u32> {
        let listener = session_main.application_listener(session)?;
        unsafe { &*self.listeners.get() }
            .get(listener)
            .map(|_| listener)
    }

    #[inline]
    pub fn lookup_name(&self, name: &str) -> Option<u32> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Application name lookup requires Main Thread and stopped Data Workers"
        );
        // SAFETY: the Main Thread owns mutation while workers are stopped.
        unsafe { &*self.application_by_name.get() }
            .get(name)
            .copied()
    }
}

/// Global physical-segment counter mirroring VPP's `smm->seg_name_counter`
/// (third_party/vpp/src/vnet/session/segment_manager.c:181-182): every shared
/// segment gets a unique name across SegmentManager instances, so independent
/// workers owning the same allocation owner never shm_open the same name.
static SEG_NAME_COUNTER: AtomicU64 = AtomicU64::new(0);

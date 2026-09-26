use std::cell::{Cell, UnsafeCell};
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::os::fd::BorrowedFd;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use hammer_core::data_plane::NodeId;
use hammer_infra::bitmap::Bitmap;
use hammer_infra::pool::Pool;
use hammer_infra::segment::Segment;
use hammer_infra::svm::fifo_segment::FifoSegmentError;
use hammer_infra::svm::ssvm::{SsvmError, SsvmSegmentBackend};
use hammer_infra::sync::SpinLock;
use hammer_runtime::app::{SessionMsgQueue, SessionMsgQueueError};
use hammer_runtime::attach::{ApplicationMqPublication, ExtConfigStore};
use hammer_runtime::{
    AttachError, DataWorkerId, FILE_MAIN, File, FileFunctions, NodeMain, RuntimeError,
    RuntimeResult, SessionConnectEndpoint,
};
use hashbrown::HashTable;
use thiserror::Error;

use super::core::{SessionEvent, SessionHandle};
use super::protocol::ApplicationCallbacks;
use super::segment_manager::{SegmentManager, SegmentManagerMain, SegmentManagerProperties};

static APP_MQ_SEGMENT_COUNTER: AtomicU64 = AtomicU64::new(0);

// VPP: foreach_app_options_flags, application_interface.h:196-215.
bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct ApplicationFlags: u32 {
        const ACCEPT_REDIRECT = 1 << 0;
        const ADD_SEGMENT = 1 << 1;
        const IS_BUILTIN = 1 << 2;
        const IS_TRANSPORT_APP = 1 << 3;
        const IS_PROXY = 1 << 4;
        const USE_GLOBAL_SCOPE = 1 << 5;
        const USE_LOCAL_SCOPE = 1 << 6;
        const EVT_MQ_USE_EVENTFD = 1 << 7;
        const MEMFD_FOR_BUILTIN = 1 << 8;
        const USE_HUGE_PAGE = 1 << 9;
        const GET_ORIGINAL_DST = 1 << 10;
        const EVT_COLLECTOR = 1 << 11;
        const NO_DUMP_SEGMENTS = 1 << 12;
    }
}

// VPP: app_init_args_t, application_interface.h:76-82;
// vnet_application_attach, application.c:1112-1199.
pub struct ApplicationConfig {
    pub namespace: u32,
    pub flags: ApplicationFlags,
    pub name: String,
    pub segment: SegmentManagerProperties,
    pub callbacks: ApplicationCallbacks,
    pub api_client: u32,
}

// VPP: app_worker_t, application.h:32-81. The Application owner, not the
// runtime Session worker, owns these application-to-Session facts.
#[repr(C)]
pub struct AppWorker {
    cacheline0: hammer_infra::align::CacheLineAlignMark,
    worker_index: u32,
    worker_map_index: u32,
    application: u32,
    connects_segment_manager: u32,
    listeners: HashMap<SessionHandle, u32>,
    half_open: Pool<SessionHandle>,
    api_client: u32,
    app_is_builtin: bool,
    events_by_worker: Vec<UnsafeCell<Vec<SessionEvent>>>,
    mq_congested: AtomicU8,
    worker_mq_congested: Vec<UnsafeCell<bool>>,
    detached_segment_managers: SpinLock<Vec<u32>>,
}

impl AppWorker {
    // VPP: application_alloc_worker_and_init, application.c:986-1074;
    // application_worker.c:15-44. Hammer has no thread-zero dataplane slot.
    pub fn new(
        application: u32,
        worker_map_index: u32,
        connects_segment_manager: u32,
        api_client: u32,
        app_is_builtin: bool,
        worker_count: usize,
    ) -> Self {
        let mut events_by_worker = Vec::with_capacity(worker_count);
        let mut worker_mq_congested = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            events_by_worker.push(UnsafeCell::new(Vec::new()));
            worker_mq_congested.push(UnsafeCell::new(false));
        }
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
            worker_index: u32::MAX,
            worker_map_index,
            application,
            connects_segment_manager,
            listeners: HashMap::new(),
            half_open: Pool::new(),
            api_client,
            app_is_builtin,
            events_by_worker,
            mq_congested: AtomicU8::new(0),
            worker_mq_congested,
            detached_segment_managers: SpinLock::new(Vec::new()),
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

    // VPP: app_worker_t.event_queue, application.h:39-43;
    // segment_manager_event_queue, segment_manager.c:1035-1040.
    pub fn event_queue<'a>(
        &self,
        managers: &'a SegmentManagerMain,
    ) -> Option<&'a hammer_infra::svm::msg_queue::SvmMsgQ> {
        managers.get(self.connects_segment_manager)?.event_queue()
    }
}

pub struct Application {
    index: u32,
    flags: ApplicationFlags,
    name: String,
    namespace: u32,
    api_client: u32,
    segment: SegmentManagerProperties,
    worker_maps: Pool<u32>,
    listeners: Vec<u32>,
    connections: Vec<u32>,
    mq_resources: Option<ApplicationMqResources>,
    session_callbacks: ApplicationCallbacks,
}

impl Application {
    #[inline]
    fn new(config: ApplicationConfig) -> Self {
        Self {
            index: u32::MAX,
            flags: config.flags,
            name: config.name,
            namespace: config.namespace,
            api_client: config.api_client,
            segment: config.segment,
            worker_maps: Pool::new(),
            listeners: Vec::new(),
            connections: Vec::new(),
            mq_resources: None,
            session_callbacks: config.callbacks,
        }
    }

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
}

pub struct ApplicationListener {
    index: u32,
    application: u32,
    workers: Bitmap,
    accept_rotor: u32,
    global_session: Option<SessionHandle>,
    local_session: Option<SessionHandle>,
    handle: Option<SessionHandle>,
    worker_sessions: Vec<u32>,
    app: Option<u32>,
    opaque: Option<u64>,
}

pub(crate) struct ApplicationConnection {
    protocol: u8,
    context: u64,
    endpoint: SessionConnectEndpoint,
    connect_state: AtomicU8,
}

impl ApplicationListener {
    #[inline(always)]
    pub const fn index(&self) -> u32 {
        self.index
    }

    #[inline]
    pub const fn application(&self) -> u32 {
        self.application
    }

    #[inline]
    pub const fn handle(&self) -> Option<SessionHandle> {
        self.handle
    }

    // VPP: app_listener_select_worker, application.c:172-185.
    #[inline]
    pub(crate) fn select_worker(&mut self) -> u32 {
        let worker = self
            .workers
            .next_set(self.accept_rotor as usize)
            .or_else(|| self.workers.first_set())
            .expect("ApplicationListener must have an accepting worker");
        self.accept_rotor = u32::try_from(worker).expect("worker index fits u32");
        u32::try_from(worker).expect("worker index fits u32")
    }

    #[inline]
    pub const fn global_session(&self) -> Option<SessionHandle> {
        self.global_session
    }

    #[inline]
    pub const fn local_session(&self) -> Option<SessionHandle> {
        self.local_session
    }

    #[inline]
    pub(crate) const fn app(&self) -> Option<u32> {
        self.app
    }

    #[inline]
    pub(crate) const fn opaque(&self) -> Option<u64> {
        self.opaque
    }
}

impl ApplicationConnection {
    #[inline]
    pub(crate) const fn application(&self) -> u32 {
        self.endpoint.application
    }

    #[inline]
    pub(crate) const fn context(&self) -> u64 {
        self.context
    }

    #[inline]
    pub(crate) const fn app(&self) -> Option<u32> {
        self.endpoint.app
    }

    #[inline]
    pub(crate) const fn opaque(&self) -> Option<u64> {
        self.endpoint.opaque
    }

    #[inline]
    pub(crate) fn server_name(&self) -> Option<&str> {
        self.endpoint.server_name.as_deref()
    }

    #[inline]
    pub(crate) fn connect_endpoint(&self, connection: u32) -> (u8, SessionConnectEndpoint) {
        let mut endpoint = self.endpoint.clone();
        endpoint.connection = Some(connection);
        (self.protocol, endpoint)
    }
}

/// Per-Application message queues, one for every Data Worker.
///
/// The queues are owned by `ApplicationMain` and are published to Data
/// Workers at attach time. Every app-to-session event uses the queue selected
/// by its Application and Data Worker; there is no shared worker fallback.
#[deprecated(note = "ADR-0040: migrate private RX MQ ownership into Application")]
pub struct ApplicationMqResources {
    segment: Segment,
    workers: Box<[Box<ApplicationWorkerMq>]>,
    offsets: Box<[u64]>,
    ext_config: Option<ExtConfigStore>,
}

pub(super) struct ApplicationWorkerMq {
    application: u32,
    worker: DataWorkerId,
    queue: Arc<SessionMsgQueue>,
    app_session_input: NodeId,
    file: Option<u32>,
    pending: Cell<bool>,
}

impl ApplicationWorkerMq {
    #[inline]
    pub(super) fn queue(&self) -> &Arc<SessionMsgQueue> {
        &self.queue
    }

    #[inline]
    pub(super) fn clear_pending(&self) {
        self.pending.set(false);
    }
}

impl ApplicationMqResources {
    pub(crate) fn create_local(
        application: u32,
        worker_count: usize,
        capacity: usize,
    ) -> Result<Self, ApplicationError> {
        Self::create(application, worker_count, capacity, false)
    }

    pub(crate) fn create_external(
        application: u32,
        worker_count: usize,
        capacity: usize,
    ) -> Result<Self, ApplicationError> {
        Self::create(application, worker_count, capacity, true)
    }

    fn create(
        application: u32,
        worker_count: usize,
        capacity: usize,
        shared: bool,
    ) -> Result<Self, ApplicationError> {
        if capacity < APP_MQ_CAPACITY_MIN {
            return Err(ApplicationError::MqCapacityInvalid { capacity });
        }
        if worker_count == 0 {
            return Err(ApplicationError::MqWorkerCountZero);
        }
        let q_nitems = capacity.next_power_of_two().max(2) as u32;
        let ring_nitems = capacity.max(1) as u32;
        let queue_bytes = SessionMsgQueue::layout_bytes(q_nitems, ring_nitems)
            .map_err(|source| ApplicationError::MqLayout { source })?;
        let segment_bytes = queue_bytes
            .checked_mul(worker_count)
            .and_then(|bytes| bytes.checked_add(APP_MQ_SEGMENT_HEADROOM))
            .ok_or(ApplicationError::MqLayoutOverflow)?;
        let segment = if shared {
            let name = format!(
                "hammer-app-rx-mq-{}-{}",
                std::process::id(),
                APP_MQ_SEGMENT_COUNTER.fetch_add(1, Ordering::Relaxed)
            );
            Segment::shared(&name, segment_bytes)
                .map_err(|source| ApplicationError::MqSegmentCreate { source })?
        } else {
            Segment::local(segment_bytes)
        };

        let app_session_input = *super::APP_SESSION_INPUT_NODE
            .get()
            .ok_or(ApplicationError::MqInputNodeMissing)?;
        let mut workers = Vec::with_capacity(worker_count);
        let mut offsets = Vec::with_capacity(worker_count);
        for worker in 0..worker_count {
            let offset = segment
                .alloc(queue_bytes, 64)
                .ok_or(ApplicationError::MqSegmentExhausted)?;
            // SAFETY: the segment has enough bytes at `offset` for the queue,
            // and this is the only initializer for this offset.
            let queue = unsafe {
                SessionMsgQueue::init_at_with_signal(segment.clone(), offset, q_nitems, ring_nitems)
            }
            .map_err(|source| ApplicationError::MqInit { source })?;
            workers.push(Box::new(ApplicationWorkerMq {
                application,
                worker: DataWorkerId::new(worker as u32),
                queue: Arc::new(queue),
                app_session_input,
                file: None,
                pending: Cell::new(false),
            }));
            offsets.push(offset);
        }
        // The bounded ext-config store (QUIC/TLS Session control data) lives
        // in the Rx MQ segment from the headroom and is published to the
        // Application with the queues; the Application allocates one fixed
        // chunk per connect and the daemon reads and frees it exactly once
        // (VPP ext_config uword ownership, session_node.c:80-100).
        let ext_config = segment
            .alloc(ExtConfigStore::layout_bytes(), 64)
            .map(|offset| {
                // SAFETY: the segment has enough bytes at `offset` for the
                // whole store layout, and this is the only initializer for
                // this offset.
                unsafe { ExtConfigStore::init_at(segment.clone(), offset as usize) }
            })
            .ok_or(ApplicationError::MqSegmentExhausted)?;
        Ok(Self {
            segment,
            workers: workers.into_boxed_slice(),
            offsets: offsets.into_boxed_slice(),
            ext_config: Some(ext_config),
        })
    }

    /// The bounded ext-config store for this Application's Session control
    /// data, when the segment carried headroom for one.
    pub(crate) fn ext_config_store(&self) -> Option<ExtConfigStore> {
        self.ext_config.clone()
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    #[inline]
    pub(crate) fn queue(&self, worker: DataWorkerId) -> Option<&Arc<SessionMsgQueue>> {
        self.workers.get(worker.slot()).map(|entry| &entry.queue)
    }

    #[inline]
    pub(super) fn worker(&self, worker: DataWorkerId) -> Option<&ApplicationWorkerMq> {
        self.workers.get(worker.slot()).map(Box::as_ref)
    }

    pub(crate) fn publication(&self) -> Result<ApplicationMqPublication, ApplicationError> {
        ApplicationMqPublication::new(
            self.segment.clone(),
            self.workers
                .iter()
                .map(|entry| Arc::clone(&entry.queue))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            self.offsets.clone(),
            self.ext_config
                .as_ref()
                .map(|store| store.offset() as u64)
                .unwrap_or(0),
        )
        .map_err(|source| ApplicationError::MqPublication { source })
    }

    fn install(&mut self) -> RuntimeResult<()> {
        for worker in &mut self.workers {
            let signal_read_fd = worker
                .queue
                .read_fd()
                .ok_or(AttachError::SessionSignalMissing)?;
            // SAFETY: the queue retains the original read endpoint while
            // FileMain owns this independent duplicated descriptor.
            let signal_read = unsafe { BorrowedFd::borrow_raw(signal_read_fd) }
                .try_clone_to_owned()
                .map_err(|source| AttachError::SessionSignalDuplicate { source })?;
            let mut file = File::new(
                signal_read,
                format!("app rx mq {:?}", worker.application),
                worker.as_ref() as *const ApplicationWorkerMq as usize as u64,
                FileFunctions {
                    read: Some(schedule_application_mq),
                    ..FileFunctions::default()
                },
            );
            file.set_polling_thread_index(worker.worker.thread_index());
            match FILE_MAIN
                .get()
                .expect("FileMain is initialized before Application attach")
                .add(file)
            {
                Ok(file) => worker.file = Some(file),
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn uninstall(&mut self) -> RuntimeResult<()> {
        let mut first_error = None;
        for worker in &mut self.workers {
            let Some(file) = worker.file else {
                continue;
            };
            match FILE_MAIN
                .get()
                .expect("FileMain remains initialized through Application detach")
                .delete(file)
            {
                Ok(true) => worker.file = None,
                Ok(false) => {
                    first_error.get_or_insert(RuntimeError::FileIndexInvalid { index: file });
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

fn schedule_application_mq(graph: &mut NodeMain, file: &mut File) -> RuntimeResult<()> {
    let entry = file.private_data() as usize as *const ApplicationWorkerMq;
    assert!(
        !entry.is_null(),
        "Application MQ File retains its owner entry"
    );
    // SAFETY: ApplicationMqResources stores each entry in a Box whose address
    // is stable until its File registration is deleted under WorkerBarrier.
    let entry = unsafe { &*entry };
    if !entry.pending.replace(true) {
        // SAFETY: FileMain invokes this callback on the File's assigned runtime thread.
        let mut sessions =
            unsafe { super::runtime::session_main().worker(file.polling_thread_index()) }?;
        sessions.schedule_application_mq(entry.application);
    }
    graph.mark_interrupt_pending(entry.app_session_input)?;
    Ok(())
}

const CONNECTION_CONNECTING: u8 = 0;
const CONNECTION_CONNECTED: u8 = 1;

/// Main Thread authority for Application identity and lifetime.
pub struct ApplicationMain {
    applications: UnsafeCell<Pool<Application>>,
    listeners: UnsafeCell<Pool<ApplicationListener>>,
    connections: UnsafeCell<Pool<ApplicationConnection>>,
    workers: UnsafeCell<Pool<AppWorker>>,
    application_by_api_client: UnsafeCell<HashMap<u32, u32>>,
    application_by_name: UnsafeCell<HashTable<(NonNull<str>, u32)>>,
    name_hasher: RandomState,
}

/// The process-global Application authority, published by `application_init`.
pub static APPLICATION_MAIN: OnceLock<ApplicationMain> = OnceLock::new();

impl ApplicationMain {
    // VPP: vnet_application_attach/application_alloc_and_init,
    // application.c:664-767,1112-1199. All fallible segment work completes
    // before the Application and its callback table become visible.
    pub fn attach(&self, mut config: ApplicationConfig) -> Result<u32, ApplicationError> {
        self.ensure_main_thread()?;
        if !config
            .flags
            .intersects(ApplicationFlags::USE_GLOBAL_SCOPE | ApplicationFlags::USE_LOCAL_SCOPE)
        {
            config.flags.insert(ApplicationFlags::USE_GLOBAL_SCOPE);
        }
        if config.flags.contains(ApplicationFlags::IS_BUILTIN)
            && !config.flags.contains(ApplicationFlags::MEMFD_FOR_BUILTIN)
        {
            config.segment.segment_backend = SsvmSegmentBackend::Private;
        }
        config.segment.add_segment |= config.flags.contains(ApplicationFlags::ADD_SEGMENT);
        config.segment.use_huge_page |= config.flags.contains(ApplicationFlags::USE_HUGE_PAGE);
        config.segment.no_dump_segments |=
            config.flags.contains(ApplicationFlags::NO_DUMP_SEGMENTS);
        config.segment.use_mq_eventfd |=
            config.flags.contains(ApplicationFlags::EVT_MQ_USE_EVENTFD);
        if config.flags.contains(ApplicationFlags::EVT_MQ_USE_EVENTFD)
            && config.segment.segment_backend != SsvmSegmentBackend::Memfd
        {
            return Err(ApplicationError::SegmentConfigInvalid);
        }
        if config.name.is_empty() && config.api_client == u32::MAX {
            return Err(ApplicationError::SegmentConfigInvalid);
        }
        if (!config.name.is_empty() && self.lookup_name(&config.name).is_some())
            || (config.api_client != u32::MAX
                && !config.flags.contains(ApplicationFlags::IS_BUILTIN)
                && unsafe { &*self.application_by_api_client.get() }
                    .contains_key(&config.api_client))
        {
            return Err(ApplicationError::AlreadyAttached);
        }

        let mut manager = SegmentManager::new(config.segment)?;
        let worker_count = usize::from(config.segment.n_slices);
        let install = || {
            let applications = unsafe { &mut *self.applications.get() };
            let workers = unsafe { &mut *self.workers.get() };
            let names = unsafe { &mut *self.application_by_name.get() };
            let api_clients = unsafe { &mut *self.application_by_api_client.get() };

            let api_client = config.api_client;
            let builtin = config.flags.contains(ApplicationFlags::IS_BUILTIN);
            let application = applications.insert(Application::new(config));
            let entry = applications
                .get_mut(application)
                .expect("new Application is present");
            entry.index = application;

            let worker = workers.insert(AppWorker::new(
                application,
                0,
                u32::MAX,
                api_client,
                builtin,
                worker_count,
            ));
            let worker_entry = workers.get_mut(worker).expect("new AppWorker is present");
            worker_entry.worker_index = worker;
            let worker_map = entry.worker_maps.insert(worker);
            worker_entry.worker_map_index = worker_map;
            manager.set_owner(worker);
            worker_entry.connects_segment_manager = SegmentManagerMain::global()
                .expect("SegmentManagerMain initializes with ApplicationMain")
                .insert(manager);

            if !entry.name.is_empty() {
                let name = entry.name.as_str();
                let hash = self.name_hasher.hash_one(name);
                names.insert_unique(hash, (NonNull::from(name), application), |(key, _)| {
                    self.name_hasher.hash_one(unsafe { key.as_ref() })
                });
            }
            if !builtin && api_client != u32::MAX {
                api_clients.insert(api_client, application);
            }
            application
        };
        Ok(match hammer_runtime::barrier::global() {
            Some(_) => hammer_runtime::worker_thread_barrier_sync!({ install() }),
            None => install(),
        })
    }

    // VPP: application_alloc_worker_and_init/vnet_app_worker_add_del,
    // application.c:986-1074.
    pub fn attach_worker(
        &self,
        application: u32,
        api_client: u32,
    ) -> Result<u32, ApplicationError> {
        self.ensure_main_thread()?;
        let entry = self
            .application(application)
            .ok_or(ApplicationError::Missing { application })?;
        if api_client != u32::MAX
            && unsafe { &*self.application_by_api_client.get() }.contains_key(&api_client)
        {
            return Err(ApplicationError::AlreadyAttached);
        }
        let mut manager = SegmentManager::new(entry.segment)?;
        let worker_count = usize::from(entry.segment.n_slices);
        let builtin = entry.flags.contains(ApplicationFlags::IS_BUILTIN);
        let install = || {
            let applications = unsafe { &mut *self.applications.get() };
            let workers = unsafe { &mut *self.workers.get() };
            let api_clients = unsafe { &mut *self.application_by_api_client.get() };
            let entry = applications
                .get_mut(application)
                .expect("Application remains live during worker attach");
            let worker = workers.insert(AppWorker::new(
                application,
                u32::MAX,
                u32::MAX,
                api_client,
                builtin,
                worker_count,
            ));
            let worker_entry = workers.get_mut(worker).expect("new AppWorker is present");
            worker_entry.worker_index = worker;
            let worker_map = entry.worker_maps.insert(worker);
            worker_entry.worker_map_index = worker_map;
            manager.set_owner(worker);
            worker_entry.connects_segment_manager = SegmentManagerMain::global()
                .expect("SegmentManagerMain initializes with ApplicationMain")
                .insert(manager);
            if api_client != u32::MAX {
                api_clients.insert(api_client, application);
            }
            worker_map
        };
        Ok(match hammer_runtime::barrier::global() {
            Some(_) => hammer_runtime::worker_thread_barrier_sync!({ install() }),
            None => install(),
        })
    }

    // VPP: application_lookup_name, application.c:321-330.
    pub fn lookup_name(&self, name: &str) -> Option<&Application> {
        let hash = self.name_hasher.hash_one(name);
        let names = unsafe { &*self.application_by_name.get() };
        let (_, index) = names.find(hash, |(key, _)| unsafe { key.as_ref() == name })?;
        unsafe { &*self.applications.get() }.get(*index)
    }

    #[inline]
    pub fn application(&self, application: u32) -> Option<&Application> {
        unsafe { &*self.applications.get() }.get(application)
    }

    #[inline]
    pub fn worker(&self, worker: u32) -> Option<&AppWorker> {
        unsafe { &*self.workers.get() }.get(worker)
    }

    #[inline]
    pub fn worker_map(&self, application: u32, worker_map: u32) -> Option<&AppWorker> {
        let worker = *self.application(application)?.worker_maps.get(worker_map)?;
        self.worker(worker)
    }

    // VPP: app_listener_alloc, application.c:36-62; vnet_listen,
    // application.c:1277-1320. Listener mutation requires main/control barrier.
    pub fn allocate_listener(
        &self,
        application: u32,
        worker: u32,
    ) -> Result<u32, ApplicationError> {
        self.ensure_main_thread()?;
        let app = self
            .application(application)
            .ok_or(ApplicationError::Missing { application })?;
        if app.worker_maps.get(worker).is_none() {
            return Err(ApplicationError::WorkerMissing {
                application,
                worker,
            });
        }
        let listener = ApplicationListener {
            index: u32::MAX,
            application,
            workers: Bitmap::new(),
            accept_rotor: 0,
            global_session: None,
            local_session: None,
            handle: None,
            worker_sessions: Vec::new(),
            app: None,
            opaque: None,
        };
        Ok(hammer_runtime::worker_thread_barrier_sync!({
            // SAFETY: main/control holds the worker barrier for publication.
            let applications = unsafe { &mut *self.applications.get() };
            let listeners = unsafe { &mut *self.listeners.get() };
            let index = listeners.insert(listener);
            listeners
                .get_mut(index)
                .expect("inserted Application listener remains live")
                .index = index;
            applications
                .get_mut(application)
                .expect("validated Application remains live")
                .listeners
                .push(index);
            index
        }))
    }

    // VPP: app_worker_listen_sep, application_worker.c:230-312. The global
    // listening Session, when present, is the returned listener handle.
    pub fn attach_listener_session(
        &self,
        listener: u32,
        global: Option<SessionHandle>,
        local: Option<SessionHandle>,
    ) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        assert!(
            global.is_some() || local.is_some(),
            "Application listener requires a listening Session"
        );
        hammer_runtime::worker_thread_barrier_sync!({
            // SAFETY: main/control holds the barrier while publishing handles.
            let listeners = unsafe { &mut *self.listeners.get() };
            let entry = listeners
                .get_mut(listener)
                .ok_or(ApplicationError::ListenerMissing { listener })?;
            assert!(
                entry.handle.is_none(),
                "Application listener Session handles are attached once"
            );
            entry.global_session = global;
            entry.local_session = local;
            entry.handle = global.or(local);
            Ok(())
        })
    }

    // VPP: app_worker_start_listen, application_worker.c:338-377. The
    // listener segment manager is created before publishing the worker bit.
    pub fn attach_listener_worker(
        &self,
        listener: u32,
        worker_map: u32,
    ) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        let listener_entry = self
            .listener(listener)
            .map_err(|_| ApplicationError::ListenerMissing { listener })?;
        let application = listener_entry.application;
        let app = self
            .application(application)
            .ok_or(ApplicationError::Missing { application })?;
        let worker_index =
            *app.worker_maps
                .get(worker_map)
                .ok_or(ApplicationError::WorkerMissing {
                    application,
                    worker: worker_map,
                })?;
        let global = listener_entry.global_session;
        let local = listener_entry.local_session;
        if global.is_none() && local.is_none() {
            return Err(ApplicationError::ListenerMissing { listener });
        }
        let mut manager = SegmentManager::new(app.segment)?;
        manager.set_owner(worker_index);
        manager.mark_listener();
        hammer_runtime::worker_thread_barrier_sync!({
            // SAFETY: main/control holds the barrier while publishing the
            // worker listener map, segment manager and accepting bit.
            let listeners = unsafe { &mut *self.listeners.get() };
            let workers = unsafe { &mut *self.workers.get() };
            let listener_entry = listeners
                .get_mut(listener)
                .ok_or(ApplicationError::ListenerMissing { listener })?;
            if listener_entry.workers.is_set(worker_map) {
                return Err(ApplicationError::ListenerWorkerAttached {
                    listener,
                    worker: worker_map,
                });
            }
            let manager_index = SegmentManagerMain::global()
                .expect("SegmentManagerMain initializes with ApplicationMain")
                .insert(manager);
            let worker_entry = workers
                .get_mut(worker_index)
                .expect("Application worker mapping remains live");
            if let Some(session) = global {
                worker_entry.listeners.insert(session, manager_index);
            }
            if let Some(session) = local {
                worker_entry.listeners.insert(session, manager_index);
            }
            listener_entry.workers.set(worker_map);
            Ok(())
        })
    }

    // VPP: app_worker_stop_listen, application_worker.c:408-440.
    pub fn detach_listener_worker(
        &self,
        listener: u32,
        worker_map: u32,
    ) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        hammer_runtime::worker_thread_barrier_sync!({
            // SAFETY: main/control holds the barrier while removing the
            // worker listener map and segment manager.
            let listeners = unsafe { &mut *self.listeners.get() };
            let workers = unsafe { &mut *self.workers.get() };
            let listener_entry = listeners
                .get_mut(listener)
                .ok_or(ApplicationError::ListenerMissing { listener })?;
            if !listener_entry.workers.is_set(worker_map) {
                return Ok(());
            }
            let application = listener_entry.application;
            let worker_index = *unsafe { &*self.applications.get() }
                .get(application)
                .ok_or(ApplicationError::Missing { application })?
                .worker_maps
                .get(worker_map)
                .ok_or(ApplicationError::WorkerMissing {
                    application,
                    worker: worker_map,
                })?;
            let worker_entry = workers
                .get_mut(worker_index)
                .expect("Application worker mapping remains live");
            let manager_index = listener_entry
                .global_session
                .or(listener_entry.local_session)
                .and_then(|session| worker_entry.listeners.get(&session).copied())
                .expect("listener worker owns its listener segment manager");
            if SegmentManagerMain::global()
                .expect("SegmentManagerMain initializes with ApplicationMain")
                .get(manager_index)
                .expect("listener segment manager remains live")
                .active_fifo_count()
                != 0
            {
                return Err(ApplicationError::SegmentBusy {
                    manager: manager_index,
                });
            }
            if let Some(session) = listener_entry.global_session {
                worker_entry.listeners.remove(&session);
            }
            if let Some(session) = listener_entry.local_session {
                worker_entry.listeners.remove(&session);
            }
            SegmentManagerMain::global()
                .expect("SegmentManagerMain initializes with ApplicationMain")
                .remove(manager_index)?;
            listener_entry.workers.clear(worker_map);
            Ok(())
        })
    }

    // VPP: app_listener_get_session/app_listener_get, application.c:70-84.
    pub fn listener_for_session(&self, session: SessionHandle) -> Option<u32> {
        let listeners = unsafe { &*self.listeners.get() };
        listeners.iter().find_map(|(index, listener)| {
            (listener.global_session == Some(session) || listener.local_session == Some(session))
                .then_some(index)
        })
    }

    // VPP: app_listener_cleanup, application.c:145-170. This is the new
    // application-owned listener removal path.
    pub fn remove_listener(&self, listener: u32) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        hammer_runtime::worker_thread_barrier_sync!({
            let listeners = unsafe { &mut *self.listeners.get() };
            let applications = unsafe { &mut *self.applications.get() };
            let workers = unsafe { &mut *self.workers.get() };
            let (application, global_session, local_session, attached_worker) = listeners
                .get(listener)
                .map(|entry| {
                    (
                        entry.application,
                        entry.global_session,
                        entry.local_session,
                        entry.workers.first_set(),
                    )
                })
                .ok_or(ApplicationError::ListenerMissing { listener })?;
            if let Some(worker) = attached_worker {
                return Err(ApplicationError::ListenerWorkerAttached {
                    listener,
                    worker: worker as u32,
                });
            }
            let app = applications
                .get_mut(application)
                .ok_or(ApplicationError::Missing { application })?;
            app.listeners.retain(|index| *index != listener);
            for (_, worker) in app.worker_maps.iter() {
                if let Some(worker_entry) = workers.get_mut(*worker) {
                    worker_entry.listeners.retain(|session, _| {
                        global_session != Some(*session) && local_session != Some(*session)
                    });
                }
            }
            listeners
                .remove(listener)
                .ok_or(ApplicationError::ListenerMissing { listener })?;
            Ok(())
        })
    }

    #[inline(always)]
    pub fn namespace(&self, application: u32) -> Option<u32> {
        self.application(application).map(Application::namespace)
    }

    /// Initializes and publishes the process-global Application authority.
    pub fn init() -> Result<(), RuntimeError> {
        SegmentManagerMain::init();
        let main = Self {
            applications: UnsafeCell::new(Pool::new()),
            listeners: UnsafeCell::new(Pool::new()),
            connections: UnsafeCell::new(Pool::new()),
            workers: UnsafeCell::new(Pool::new()),
            application_by_api_client: UnsafeCell::new(HashMap::new()),
            application_by_name: UnsafeCell::new(HashTable::new()),
            name_hasher: RandomState::new(),
        };
        assert!(
            APPLICATION_MAIN.set(main).is_ok(),
            "Application initialization callback executes once"
        );
        Ok(())
    }

    /// Returns the published process-global Application authority.
    pub fn global() -> Result<&'static Self, RuntimeError> {
        APPLICATION_MAIN
            .get()
            .ok_or(RuntimeError::PluginStateNotInitialized {
                plugin: "application",
            })
    }
}

/// Returns the published process-global Application authority.
#[inline]
pub fn application_main() -> &'static ApplicationMain {
    ApplicationMain::global().expect("ApplicationMain is initialized before Application use")
}

const APP_MQ_CAPACITY_MIN: usize = 128;
const APP_MQ_SEGMENT_HEADROOM: usize = 1 << 20;

// SAFETY: mutable state access is restricted to the GlobalMain Main control path.
// Data Workers may read published listener and connection entries; their
// mutation or removal occurs only while WorkerBarrier stops those readers.
unsafe impl Send for ApplicationMain {}
// SAFETY: worker reads follow the publication contract above. Each MQ pending
// Cell is accessed only by the File callback and input Node on its assigned
// Data Worker; the connecting/connected transition uses its dedicated atomic.
unsafe impl Sync for ApplicationMain {}

impl ApplicationMain {
    /// Publishes one owner-defined Session App callback table.
    ///
    /// The callback policy belongs to the Application authority, matching
    /// VPP's application callback table. Session workers only resolve the
    /// selected numeric slot while dispatching an exact Session.
    #[deprecated(note = "ADR-0040: supply ApplicationCallbacks during attach")]
    pub fn register_session_app(
        &self,
        application: u32,
        callbacks: ApplicationCallbacks,
    ) -> Result<u32, ApplicationError> {
        self.ensure_active(application)?;
        self.ensure_main_thread()?;
        let applications = unsafe { &mut *self.applications.get() };
        let mut register = || {
            let entry = applications
                .get_mut(application)
                .ok_or(ApplicationError::Missing { application })?;
            if !entry.session_callbacks.name.is_empty() {
                return Err(ApplicationError::SessionAppAlreadyRegistered {
                    name: callbacks.name,
                });
            }
            entry.session_callbacks = callbacks;
            Ok(0)
        };
        let barrier = hammer_runtime::barrier::global();
        match barrier {
            Some(barrier) if barrier.is_pending() => register(),
            Some(barrier) => barrier.sync(register),
            None => register(),
        }
    }

    #[inline]
    pub(crate) fn session_callbacks(
        &self,
        application: u32,
        app: u32,
    ) -> Option<ApplicationCallbacks> {
        if app != 0 {
            return None;
        }
        let applications = unsafe { &*self.applications.get() };
        applications
            .get(application)
            .map(|entry| entry.session_callbacks)
    }

    #[deprecated(note = "ADR-0040: attach with ApplicationConfig")]
    pub fn attach_legacy(&self) -> Result<u32, ApplicationError> {
        self.attach_entry()
    }

    /// Attaches an external Application and creates one private Session
    /// Message Queue for every Data Worker.
    #[deprecated(note = "ADR-0040: attach with ApplicationConfig")]
    pub fn attach_external(
        &self,
        worker_count: usize,
        mq_capacity: usize,
    ) -> Result<u32, ApplicationError> {
        self.attach_with_mq(worker_count, mq_capacity, true)
    }

    /// Attaches a local Application and creates one private Session Message
    /// Queue for every Data Worker.
    #[deprecated(note = "ADR-0040: attach with ApplicationConfig")]
    pub fn attach_local(
        &self,
        worker_count: usize,
        mq_capacity: usize,
    ) -> Result<u32, ApplicationError> {
        self.attach_with_mq(worker_count, mq_capacity, false)
    }

    /// Attaches an external Application using the current runtime Session
    /// configuration.
    #[deprecated(note = "ADR-0040: attach with ApplicationConfig")]
    pub fn attach_external_with_runtime(&self) -> Result<u32, ApplicationError> {
        self.attach_with_runtime(true)
    }

    /// Attaches a local Application using the current runtime Session
    /// configuration.
    #[deprecated(note = "ADR-0040: attach with ApplicationConfig")]
    pub fn attach_local_with_runtime(&self) -> Result<u32, ApplicationError> {
        self.attach_with_runtime(false)
    }

    fn attach_with_runtime(&self, shared: bool) -> Result<u32, ApplicationError> {
        self.ensure_main_thread()?;
        let worker_count = hammer_runtime::config::worker::worker_count();
        let mq_capacity = super::session_config().app_mq_capacity;
        self.attach_with_mq(worker_count, mq_capacity, shared)
    }

    fn attach_entry(&self) -> Result<u32, ApplicationError> {
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published Application records.
        let applications = unsafe { &mut *self.applications.get() };
        let segment = SegmentManagerProperties::new(
            hammer_runtime::config::worker::worker_count() as u32,
            SsvmSegmentBackend::Private,
        )?;
        let entry = Application::new(ApplicationConfig {
            namespace: u32::MAX,
            flags: ApplicationFlags::empty(),
            name: String::new(),
            segment,
            callbacks: ApplicationCallbacks::default(),
            api_client: u32::MAX,
        });
        let barrier = hammer_runtime::barrier::global();
        let application = match barrier {
            Some(barrier) if barrier.is_pending() => applications.insert(entry),
            Some(barrier) => barrier.sync(|| applications.insert(entry)),
            None => applications.insert(entry),
        };
        applications
            .get_mut(application)
            .expect("inserted Application remains allocated")
            .index = application;
        Ok(application)
    }

    fn attach_with_mq(
        &self,
        worker_count: usize,
        mq_capacity: usize,
        shared: bool,
    ) -> Result<u32, ApplicationError> {
        let application = self.attach_entry()?;
        let resources = if shared {
            ApplicationMqResources::create_external(application, worker_count, mq_capacity)
        } else {
            ApplicationMqResources::create_local(application, worker_count, mq_capacity)
        };
        let mut resources = match resources {
            Ok(resources) => resources,
            Err(error) => {
                self.remove_application(application);
                return Err(error);
            }
        };
        let mut install = || resources.install();
        let install_result = match hammer_runtime::barrier::global() {
            Some(barrier) if barrier.is_pending() => install(),
            Some(barrier) => barrier.sync(install),
            None => install(),
        };
        match install_result {
            Ok(()) => {
                if let Err((error, mut resources)) = self.store_mq_resources(application, resources)
                {
                    let mut uninstall = || resources.uninstall();
                    let cleanup_result = match hammer_runtime::barrier::global() {
                        Some(barrier) if barrier.is_pending() => uninstall(),
                        Some(barrier) => barrier.sync(uninstall),
                        None => uninstall(),
                    };
                    if let Err(cleanup_error) = cleanup_result {
                        tracing::error!(%cleanup_error, "failed to roll back Application Session MQ files");
                        std::mem::forget(resources);
                    }
                    self.remove_application(application);
                    return Err(error);
                }
                Ok(application)
            }
            Err(source) => {
                let mut uninstall = || resources.uninstall();
                let cleanup_result = match hammer_runtime::barrier::global() {
                    Some(barrier) if barrier.is_pending() => uninstall(),
                    Some(barrier) => barrier.sync(uninstall),
                    None => uninstall(),
                };
                if let Err(cleanup_error) = cleanup_result {
                    tracing::error!(%cleanup_error, "failed to roll back Application Session MQ files");
                    std::mem::forget(resources);
                }
                self.remove_application(application);
                Err(ApplicationError::MqInstall { source })
            }
        }
    }

    fn store_mq_resources(
        &self,
        application: u32,
        resources: ApplicationMqResources,
    ) -> Result<(), (ApplicationError, ApplicationMqResources)> {
        if let Err(error) = self.ensure_main_thread() {
            return Err((error, resources));
        }
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published Application records.
        let applications = unsafe { &mut *self.applications.get() };
        let barrier = hammer_runtime::barrier::global();
        let store = || {
            let Some(entry) = applications.get_mut(application) else {
                return Err((ApplicationError::Missing { application }, resources));
            };
            if entry.mq_resources.is_some() {
                return Err((
                    ApplicationError::MqAlreadyAttached { application },
                    resources,
                ));
            }
            entry.mq_resources = Some(resources);
            Ok(())
        };
        match barrier {
            Some(barrier) if barrier.is_pending() => store(),
            Some(barrier) => barrier.sync(store),
            None => store(),
        }
    }

    fn remove_application(&self, application: u32) {
        self.ensure_main_thread()
            .expect("Application removal stays on the Main Thread");
        // SAFETY: this path runs on Main Thread while the barrier excludes
        // Data Worker readers of Application records.
        let applications = unsafe { &mut *self.applications.get() };
        let barrier = hammer_runtime::barrier::global();
        let removed = match barrier {
            Some(barrier) if barrier.is_pending() => applications.remove(application),
            Some(barrier) => barrier.sync(|| applications.remove(application)),
            None => applications.remove(application),
        };
        removed.expect("Application remains allocated until attach cleanup completes");
    }

    /// Returns the runtime-neutral MQ publication used by the attach server.
    #[deprecated(note = "ADR-0040: use the Application-owned SVM event queue")]
    pub fn application_mq_publication(
        &self,
        application: u32,
    ) -> Result<ApplicationMqPublication, ApplicationError> {
        let resources = self
            .applications()?
            .get(application)
            .and_then(|application| application.mq_resources.as_ref())
            .ok_or(ApplicationError::Missing { application })?;
        resources.publication()
    }

    /// Returns the stored Rx MQ resources of `application`.
    pub(crate) fn application_mq(
        &self,
        application: u32,
    ) -> Result<&ApplicationMqResources, ApplicationError> {
        self.applications()?
            .get(application)
            .and_then(|application| application.mq_resources.as_ref())
            .ok_or(ApplicationError::Missing { application })
    }

    pub fn contains(&self, application: u32) -> Result<bool, ApplicationError> {
        Ok(self.applications()?.contains_key(application))
    }

    pub fn detach(&self, application: u32) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        let applications = unsafe { &*self.applications.get() };
        if !applications.contains_key(application) {
            return Err(ApplicationError::Missing { application });
        }
        let detach = || -> Result<(), ApplicationError> {
            if unsafe { &*self.applications.get() }
                .get(application)
                .is_some_and(|entry| entry.mq_resources.is_some())
            {
                super::runtime::session_main()
                    .application_detached(application)
                    .map_err(|source| ApplicationError::MqDetachFailed { source })?;
            }
            // SAFETY: Main Thread owns Application resource mutation and the
            // WorkerBarrier excludes File callbacks and Session worker access.
            let applications = unsafe { &mut *self.applications.get() };
            let listeners = unsafe { &mut *self.listeners.get() };
            let connections = unsafe { &mut *self.connections.get() };
            let workers = unsafe { &mut *self.workers.get() };
            let names = unsafe { &mut *self.application_by_name.get() };
            let api_clients = unsafe { &mut *self.application_by_api_client.get() };
            let managers = SegmentManagerMain::global()
                .expect("SegmentManagerMain initializes with ApplicationMain");
            let (listener_indexes, connection_indexes, worker_indexes) = {
                let entry = applications
                    .get_mut(application)
                    .ok_or(ApplicationError::Missing { application })?;
                let worker_indexes = entry
                    .worker_maps
                    .iter()
                    .map(|(_, worker)| *worker)
                    .collect::<Vec<_>>();
                for worker in &worker_indexes {
                    let manager = workers
                        .get(*worker)
                        .expect("Application worker mapping remains live")
                        .connects_segment_manager;
                    if managers
                        .get(manager)
                        .expect("Application worker segment manager remains live")
                        .active_fifo_count()
                        != 0
                    {
                        return Err(ApplicationError::SegmentBusy { manager });
                    }
                }
                if let Some(resources) = entry.mq_resources.as_mut() {
                    resources
                        .uninstall()
                        .map_err(|source| ApplicationError::MqDetachFailed { source })?;
                }
                (
                    std::mem::take(&mut entry.listeners),
                    std::mem::take(&mut entry.connections),
                    worker_indexes,
                )
            };
            let entry = applications
                .get(application)
                .expect("Application remains live during detach");
            if !entry.name.is_empty() {
                let name = entry.name.as_str();
                let hash = self.name_hasher.hash_one(name);
                names
                    .find_entry(hash, |(key, index)| {
                        *index == application && unsafe { key.as_ref() == name }
                    })
                    .expect("attached Application name remains indexed")
                    .remove();
            }
            if !entry.flags.contains(ApplicationFlags::IS_BUILTIN) && entry.api_client != u32::MAX {
                api_clients.remove(&entry.api_client);
            }
            for index in listener_indexes {
                listeners.remove(index);
            }
            for index in connection_indexes {
                connections.remove(index);
            }
            for worker in worker_indexes {
                let entry = workers
                    .remove(worker)
                    .expect("Application worker remains live during detach");
                managers
                    .remove(entry.connects_segment_manager)
                    .expect("validated empty segment manager can be removed");
            }
            applications
                .remove(application)
                .ok_or(ApplicationError::Missing { application })?;
            Ok(())
        };
        match hammer_runtime::barrier::global() {
            Some(_) => hammer_runtime::worker_thread_barrier_sync!({ detach() }),
            None => detach(),
        }?;

        Ok(())
    }

    #[deprecated(note = "ADR-0040: use ApplicationListener ownership and worker attachment")]
    pub fn register_listener(
        &self,
        application: u32,
        app: Option<u32>,
        opaque: Option<u64>,
    ) -> Result<u32, ApplicationError> {
        self.ensure_active(application)?;
        let listener = ApplicationListener {
            index: u32::MAX,
            application,
            workers: Bitmap::new(),
            accept_rotor: 0,
            global_session: None,
            local_session: None,
            handle: None,
            worker_sessions: Vec::new(),
            app,
            opaque,
        };
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published listener records.
        let applications = unsafe { &mut *self.applications.get() };
        let listeners = unsafe { &mut *self.listeners.get() };
        let barrier = hammer_runtime::barrier::global();
        let register = || {
            let index = listeners.insert(listener);
            listeners
                .get_mut(index)
                .expect("inserted ApplicationListener remains allocated")
                .index = index;
            applications
                .get_mut(application)
                .expect("active Application remains allocated")
                .listeners
                .push(index);
            index
        };
        Ok(match barrier {
            Some(barrier) if barrier.is_pending() => register(),
            Some(barrier) => barrier.sync(register),
            None => register(),
        })
    }

    #[deprecated(note = "ADR-0040: use ApplicationListener and transport-owned connection setup")]
    pub fn register_connection(
        &self,
        protocol: u8,
        context: u64,
        endpoint: SessionConnectEndpoint,
    ) -> Result<u32, ApplicationError> {
        let application = endpoint.application;
        self.ensure_active(application)?;
        let connection = ApplicationConnection {
            protocol,
            context,
            endpoint,
            connect_state: AtomicU8::new(CONNECTION_CONNECTING),
        };
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published connection records.
        let applications = unsafe { &mut *self.applications.get() };
        let connections = unsafe { &mut *self.connections.get() };
        let barrier = hammer_runtime::barrier::global();
        let register = || {
            // Reap connected entries ahead of the primary insert. VPP
            // session_free is void best-effort cleanup (session.c:258-265):
            // a vanished entry is logged, never fatal to the insert.
            let connected = connections
                .iter()
                .filter_map(|(index, connection)| {
                    (connection.connect_state.load(Ordering::Acquire) == CONNECTION_CONNECTED)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            for index in connected {
                if let Some(removed) = connections.remove(index) {
                    if let Some(application_entry) = applications.get_mut(removed.application()) {
                        application_entry
                            .connections
                            .retain(|entry| *entry != index);
                    }
                }
            }
            let index = connections.insert(connection);
            applications
                .get_mut(application)
                .expect("active Application remains allocated")
                .connections
                .push(index);
            index
        };
        Ok(match barrier {
            Some(barrier) if barrier.is_pending() => register(),
            Some(barrier) => barrier.sync(register),
            None => register(),
        })
    }

    pub(crate) fn connection(
        &self,
        connection: u32,
    ) -> Result<&ApplicationConnection, ApplicationError> {
        // SAFETY: workers only read published entries. Main Thread mutation is
        // synchronized by the worker barrier.
        let connections = unsafe { &*self.connections.get() };
        connections
            .get(connection)
            .ok_or(ApplicationError::ConnectionMissing { connection })
    }

    pub(super) fn worker_mq(
        &self,
        application: u32,
        worker: DataWorkerId,
    ) -> Option<&ApplicationWorkerMq> {
        // SAFETY: Application MQ entries stay allocated until Main Thread
        // removes the owner record while WorkerBarrier stops every Data Worker.
        unsafe { &*self.applications.get() }
            .get(application)?
            .mq_resources
            .as_ref()?
            .worker(worker)
    }

    pub(crate) fn mark_connected(&self, connection: u32) -> Result<(), ApplicationError> {
        let entry = self.connection(connection)?;
        entry
            .connect_state
            .compare_exchange(
                CONNECTION_CONNECTING,
                CONNECTION_CONNECTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| ApplicationError::ConnectionAlreadyConnected { connection })?;
        Ok(())
    }

    #[deprecated(note = "ADR-0040: use ApplicationListener and transport-owned connection cleanup")]
    pub fn remove_connection(
        &self,
        application: u32,
        connection: u32,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published connection records.
        let applications = unsafe { &mut *self.applications.get() };
        let connections = unsafe { &mut *self.connections.get() };
        let barrier = hammer_runtime::barrier::global();
        let mut remove = || {
            let index = connection;
            let entry = connections
                .get(index)
                .ok_or(ApplicationError::ConnectionMissing { connection })?;
            if entry.application() != application {
                return Err(ApplicationError::ConnectionNotOwned {
                    application,
                    connection,
                });
            }
            connections
                .remove(index)
                .ok_or(ApplicationError::ConnectionMissing { connection })?;
            applications
                .get_mut(application)
                .expect("active Application remains allocated")
                .connections
                .retain(|entry| *entry != connection);
            Ok(())
        };
        match barrier {
            Some(barrier) if barrier.is_pending() => remove(),
            Some(barrier) => barrier.sync(remove),
            None => remove(),
        }
    }

    #[deprecated(
        note = "ADR-0040: use remove_listener(listener) after IP lookup and transport stop"
    )]
    pub fn remove_listener_legacy(
        &self,
        application: u32,
        listener_id: u32,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        if self.listener(listener_id)?.application != application {
            return Err(ApplicationError::ListenerNotOwned {
                application,
                listener: listener_id,
            });
        }
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published listener records.
        let applications = unsafe { &mut *self.applications.get() };
        let listeners = unsafe { &mut *self.listeners.get() };
        let barrier = hammer_runtime::barrier::global();
        let mut remove = || {
            let index = listener_id;
            if !listeners.contains_key(index) {
                return Err(ApplicationError::ListenerMissing {
                    listener: listener_id,
                });
            }
            let entry = listeners
                .get(index)
                .ok_or(ApplicationError::ListenerMissing {
                    listener: listener_id,
                })?;
            if entry.application != application {
                return Err(ApplicationError::ListenerNotOwned {
                    application,
                    listener: listener_id,
                });
            }
            drop(listeners.remove(index));
            applications
                .get_mut(application)
                .expect("active Application remains allocated")
                .listeners
                .retain(|entry| *entry != listener_id);
            Ok(())
        };
        match barrier {
            Some(barrier) if barrier.is_pending() => remove(),
            Some(barrier) => barrier.sync(remove),
            None => remove(),
        }
    }

    /// Publishes the opaque fact carried by one Application listener while the
    /// owning Main Thread holds the worker barrier.
    #[deprecated(note = "ADR-0040: use the listener-owned opaque fact")]
    pub fn update_listener_opaque(
        &self,
        application: u32,
        listener_id: u32,
        opaque: Option<u64>,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published listener records.
        let listeners = unsafe { &mut *self.listeners.get() };
        let barrier = hammer_runtime::barrier::global();
        let mut update = || {
            let index = listener_id;
            if !listeners.contains_key(index) {
                return Err(ApplicationError::ListenerMissing {
                    listener: listener_id,
                });
            }
            let listener = listeners
                .get_mut(index)
                .ok_or(ApplicationError::ListenerMissing {
                    listener: listener_id,
                })?;
            if listener.application != application {
                return Err(ApplicationError::ListenerNotOwned {
                    application,
                    listener: listener_id,
                });
            }
            listener.opaque = opaque;
            Ok(())
        };
        match barrier {
            Some(barrier) if barrier.is_pending() => update(),
            Some(barrier) => barrier.sync(update),
            None => update(),
        }
    }

    pub fn listener(&self, listener: u32) -> Result<&ApplicationListener, ApplicationError> {
        // SAFETY: production callers are the Main Thread or a Data Worker that
        // participates in `barrier`; listener mutation stops every Data Worker.
        let listeners = unsafe { &*self.listeners.get() };
        listeners
            .get(listener)
            .ok_or(ApplicationError::ListenerMissing { listener })
    }

    fn ensure_active(&self, application: u32) -> Result<(), ApplicationError> {
        if self.applications()?.contains_key(application) {
            Ok(())
        } else {
            Err(ApplicationError::Missing { application })
        }
    }

    fn applications(&self) -> Result<&Pool<Application>, ApplicationError> {
        self.ensure_main_thread()?;
        // SAFETY: the Main control check above confines all state access to the
        // control path; immutable access does not overlap a mutable call there.
        Ok(unsafe { &*self.applications.get() })
    }

    fn ensure_main_thread(&self) -> Result<(), ApplicationError> {
        hammer_runtime::ensure_main_thread().map_err(|_| ApplicationError::WrongThread)
    }
}

#[hammer_component_macros::runtime_error(subsystem = "application")]
#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("Application name or API client is already attached")]
    AlreadyAttached,
    #[error("Application segment needs 1..255 Data Worker slices, got {worker_count}")]
    SegmentSliceCount { worker_count: u32 },
    #[error("Application segment configuration is invalid")]
    SegmentConfigInvalid,
    #[error("Application segment mapping failed")]
    SegmentMapping {
        #[source]
        source: SsvmError,
    },
    #[error("Application FIFO segment initialization failed")]
    SegmentInit {
        #[source]
        source: FifoSegmentError,
    },
    #[error("Application segment has no space for requested FIFO pairs")]
    SegmentNoSpace,
    #[error("Application segment manager {manager} is missing")]
    SegmentMissing { manager: u32 },
    #[error("Application segment manager {manager} still owns FIFOs")]
    SegmentBusy { manager: u32 },
    #[error("Application {application:?} is not attached")]
    Missing { application: u32 },
    #[error("Session App `{name}` is already registered")]
    SessionAppAlreadyRegistered { name: &'static str },
    #[error("Application state is owned by another thread")]
    WrongThread,
    #[error("per-Application MQ capacity {capacity} is below the minimum 128")]
    MqCapacityInvalid { capacity: usize },
    #[error("per-Application MQ requires at least one Data Worker")]
    MqWorkerCountZero,
    #[error("Application Session MQ input node is not registered")]
    MqInputNodeMissing,
    #[error("per-Application MQ layout rejected: {source:?}")]
    MqLayout {
        #[source]
        source: SessionMsgQueueError,
    },
    #[error("per-Application MQ layout exceeds addressable memory")]
    MqLayoutOverflow,
    #[error("failed to create per-Application MQ segment")]
    MqSegmentCreate {
        #[source]
        source: std::io::Error,
    },
    #[error("per-Application MQ segment cannot hold the queue")]
    MqSegmentExhausted,
    #[error("per-Application MQ initialisation rejected: {source:?}")]
    MqInit {
        #[source]
        source: SessionMsgQueueError,
    },
    #[error("per-Application MQ file registration failed")]
    MqInstall {
        #[source]
        source: RuntimeError,
    },
    #[error("per-Application MQ worker detach cleanup failed")]
    MqDetachFailed {
        #[source]
        source: RuntimeError,
    },
    #[error("per-Application MQ publication is invalid")]
    MqPublication {
        #[source]
        source: AttachError,
    },
    #[error("Application {application:?} already owns per-Application MQ resources")]
    MqAlreadyAttached { application: u32 },
    #[error("Application listener {listener:?} is not registered")]
    ListenerMissing { listener: u32 },
    #[error("Application {application:?} has no worker mapping {worker:?}")]
    WorkerMissing { application: u32, worker: u32 },
    #[error("Application listener {listener:?} already has worker mapping {worker:?}")]
    ListenerWorkerAttached { listener: u32, worker: u32 },
    #[error("Application listener {listener:?} is not owned by Application {application:?}")]
    ListenerNotOwned { application: u32, listener: u32 },
    #[error("Application connection {connection:?} is not registered")]
    ConnectionMissing { connection: u32 },
    #[error("Application connection {connection:?} is not owned by Application {application:?}")]
    ConnectionNotOwned { application: u32, connection: u32 },
    #[error("Application connection {connection:?} was already connected")]
    ConnectionAlreadyConnected { connection: u32 },
}

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use hammer_infra::pool::Pool;
use hammer_infra::segment::Segment;
use hammer_runtime::GlobalMain;
use hammer_runtime::app::{SessionMsgQueue, SessionMsgQueueError};
use hammer_runtime::attach::{ApplicationMqPublication, ExtConfigStore};
use hammer_runtime::{AttachError, DataWorkerId, RuntimeError};
use thiserror::Error;

use super::config::Session;
use super::protocol::SessionAppVft;

static APP_MQ_SEGMENT_COUNTER: AtomicU64 = AtomicU64::new(0);

struct ApplicationState {
    applications: Pool<Application>,
    listeners: Pool<ApplicationListener>,
    connections: Pool<ApplicationConnection>,
}

struct Application {
    workers: Vec<DataWorkerId>,
    listeners: Vec<u32>,
    connections: Vec<u32>,
    mq_resources: Option<ApplicationMqResources>,
    session_callbacks: Option<SessionAppVft>,
}

impl Application {
    #[inline]
    fn new(worker_count: usize) -> Self {
        Self {
            workers: (0..worker_count)
                .map(|worker| DataWorkerId::new(worker as u32))
                .collect(),
            listeners: Vec::new(),
            connections: Vec::new(),
            mq_resources: None,
            session_callbacks: None,
        }
    }
}

pub(crate) struct ApplicationListener {
    application: u32,
    app: Option<u32>,
    opaque: Option<u64>,
}

pub(crate) struct ApplicationConnection {
    application: u32,
    context: u64,
    app: Option<u32>,
    opaque: Option<u64>,
    server_name: Option<String>,
    connect_state: AtomicU8,
}

impl ApplicationListener {
    #[inline]
    pub(crate) const fn application(&self) -> u32 {
        self.application
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
        self.application
    }

    #[inline]
    pub(crate) const fn context(&self) -> u64 {
        self.context
    }

    #[inline]
    pub(crate) const fn app(&self) -> Option<u32> {
        self.app
    }

    #[inline]
    pub(crate) const fn opaque(&self) -> Option<u64> {
        self.opaque
    }

    #[inline]
    pub(crate) fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }
}

/// Per-Application message queues, one for every Data Worker.
///
/// The queues are owned by `ApplicationMain` and are published to Data
/// Workers at attach time. Every app-to-session event uses the queue selected
/// by its Application and Data Worker; there is no shared worker fallback.
pub struct ApplicationMqResources {
    segment: Segment,
    queues: Box<[Arc<SessionMsgQueue>]>,
    offsets: Box<[u64]>,
    ext_config: Option<ExtConfigStore>,
}

impl ApplicationMqResources {
    pub(crate) fn create_local(
        worker_count: usize,
        capacity: usize,
    ) -> Result<Self, ApplicationError> {
        Self::create(worker_count, capacity, false)
    }

    pub(crate) fn create_external(
        worker_count: usize,
        capacity: usize,
    ) -> Result<Self, ApplicationError> {
        Self::create(worker_count, capacity, true)
    }

    fn create(
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

        let mut queues = Vec::with_capacity(worker_count);
        let mut offsets = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let offset = segment
                .alloc(queue_bytes, 64)
                .ok_or(ApplicationError::MqSegmentExhausted)?;
            // SAFETY: the segment has enough bytes at `offset` for the queue,
            // and this is the only initializer for this offset.
            let queue = unsafe {
                SessionMsgQueue::init_at_with_signal(segment.clone(), offset, q_nitems, ring_nitems)
            }
            .map_err(|source| ApplicationError::MqInit { source })?;
            queues.push(Arc::new(queue));
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
            queues: queues.into_boxed_slice(),
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
        self.queues.len()
    }

    #[inline]
    pub(crate) fn queue(&self, worker: DataWorkerId) -> Option<&Arc<SessionMsgQueue>> {
        self.queues.get(worker.slot())
    }

    pub(crate) fn publication(&self) -> Result<ApplicationMqPublication, ApplicationError> {
        ApplicationMqPublication::new(
            self.segment.clone(),
            self.queues.clone(),
            self.offsets.clone(),
            self.ext_config
                .as_ref()
                .map(|store| store.offset() as u64)
                .unwrap_or(0),
        )
        .map_err(|source| ApplicationError::MqPublication { source })
    }
}

const CONNECTION_CONNECTING: u8 = 0;
const CONNECTION_CONNECTED: u8 = 1;

/// Main Thread authority for Application identity and lifetime.
pub struct ApplicationMain {
    state: UnsafeCell<ApplicationState>,
}

/// The process-global Application authority, published by `application_init`.
pub static APPLICATION_MAIN: OnceLock<ApplicationMain> = OnceLock::new();

impl ApplicationMain {
    /// Initializes and publishes the process-global Application authority.
    pub fn init() -> Result<(), RuntimeError> {
        let main = Self {
            state: UnsafeCell::new(ApplicationState {
                applications: Pool::new(),
                listeners: Pool::new(),
                connections: Pool::new(),
            }),
        };
        APPLICATION_MAIN
            .set(main)
            .map_err(|_| RuntimeError::PluginStateNotInitialized {
                plugin: "application",
            })?;
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
// SAFETY: worker reads follow the publication contract above, and the
// connecting/connected transition changes only its dedicated atomic state.
unsafe impl Sync for ApplicationMain {}

impl ApplicationMain {
    /// Publishes one owner-defined Session App callback table.
    ///
    /// The callback policy belongs to the Application authority, matching
    /// VPP's application callback table. Session workers only resolve the
    /// selected numeric slot while dispatching an exact Session.
    pub fn register_session_app(
        &self,
        application: u32,
        vft: SessionAppVft,
    ) -> Result<u32, ApplicationError> {
        self.ensure_active(application)?;
        self.ensure_main_thread()?;
        let state = unsafe { &mut *self.state.get() };
        let mut register = || {
            let entry = state
                .applications
                .get_mut(application)
                .ok_or(ApplicationError::Missing { application })?;
            if entry.session_callbacks.is_some() {
                return Err(ApplicationError::SessionAppAlreadyRegistered { name: vft.name });
            }
            entry.session_callbacks = Some(vft);
            Ok(0)
        };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        match barrier {
            Some(barrier) if barrier.is_pending() => register(),
            Some(barrier) => barrier.sync(register),
            None => register(),
        }
    }

    #[inline]
    pub(crate) fn session_callbacks(&self, application: u32, app: u32) -> Option<SessionAppVft> {
        if app != 0 {
            return None;
        }
        let state = unsafe { &*self.state.get() };
        state
            .applications
            .get(application)
            .and_then(|entry| entry.session_callbacks)
    }

    pub fn attach(&self) -> Result<u32, ApplicationError> {
        self.attach_with_worker_count(0)
    }

    /// Attaches an external Application and creates one private Session
    /// Message Queue for every Data Worker.
    pub fn attach_external(
        &self,
        worker_count: usize,
        mq_capacity: usize,
    ) -> Result<u32, ApplicationError> {
        self.attach_with_mq(worker_count, mq_capacity, true)
    }

    /// Attaches a local Application and creates one private Session Message
    /// Queue for every Data Worker.
    pub fn attach_local(
        &self,
        worker_count: usize,
        mq_capacity: usize,
    ) -> Result<u32, ApplicationError> {
        self.attach_with_mq(worker_count, mq_capacity, false)
    }

    /// Attaches an external Application using the current runtime Session
    /// configuration.
    pub fn attach_external_with_runtime(&self) -> Result<u32, ApplicationError> {
        self.attach_with_runtime(true)
    }

    /// Attaches a local Application using the current runtime Session
    /// configuration.
    pub fn attach_local_with_runtime(&self) -> Result<u32, ApplicationError> {
        self.attach_with_runtime(false)
    }

    fn attach_with_runtime(&self, shared: bool) -> Result<u32, ApplicationError> {
        self.ensure_main_thread()?;
        let (worker_count, mq_capacity) =
            GlobalMain::with_current(|engine| -> Result<(usize, usize), ApplicationError> {
                let session = engine
                    .registry
                    .get::<Session>()
                    .expect("Session configuration is published before Application attach");
                Ok((engine.configured_worker_count(), session.app_mq_capacity))
            })
            .expect("main GlobalMain is installed before Application attach")?;
        self.attach_with_mq(worker_count, mq_capacity, shared)
    }

    fn attach_with_worker_count(&self, worker_count: usize) -> Result<u32, ApplicationError> {
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published Application records.
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let application = match barrier {
            Some(barrier) if barrier.is_pending() => {
                state.applications.insert(Application::new(worker_count))
            }
            Some(barrier) => {
                barrier.sync(|| state.applications.insert(Application::new(worker_count)))
            }
            None => state.applications.insert(Application::new(worker_count)),
        };
        Ok(application)
    }

    fn attach_with_mq(
        &self,
        worker_count: usize,
        mq_capacity: usize,
        shared: bool,
    ) -> Result<u32, ApplicationError> {
        let application = self.attach_with_worker_count(worker_count)?;
        let resources = if shared {
            ApplicationMqResources::create_external(worker_count, mq_capacity)
        } else {
            ApplicationMqResources::create_local(worker_count, mq_capacity)
        };
        let resources = match resources {
            Ok(resources) => resources,
            Err(error) => {
                self.remove_application(application);
                return Err(error);
            }
        };
        let install_result = GlobalMain::with_current(|engine| {
            let main = super::runtime::session_main();
            main.install_application_mqs(engine, application, &resources)
                .map_err(|source| ApplicationError::MqInstall { source })
        });
        match install_result {
            Some(Ok(())) => {
                if let Err(error) = self.store_mq_resources(application, resources) {
                    self.remove_application(application);
                    return Err(error);
                }
                Ok(application)
            }
            Some(Err(error)) => {
                self.remove_application(application);
                Err(error)
            }
            None => {
                self.remove_application(application);
                panic!("main GlobalMain is installed before Application MQ attach")
            }
        }
    }

    fn store_mq_resources(
        &self,
        application: u32,
        resources: ApplicationMqResources,
    ) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published Application records.
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let store = || {
            let entry = state
                .applications
                .get_mut(application)
                .ok_or(ApplicationError::Missing { application })?;
            if entry.mq_resources.is_some() {
                return Err(ApplicationError::MqAlreadyAttached { application });
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
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let removed = match barrier {
            Some(barrier) if barrier.is_pending() => state.applications.remove(application),
            Some(barrier) => barrier.sync(|| state.applications.remove(application)),
            None => state.applications.remove(application),
        };
        removed.expect("Application remains allocated until attach cleanup completes");
    }

    /// Returns the runtime-neutral MQ publication used by the attach server.
    pub fn application_mq_publication(
        &self,
        application: u32,
    ) -> Result<ApplicationMqPublication, ApplicationError> {
        let resources = self
            .state()?
            .applications
            .get(application)
            .and_then(|application| application.mq_resources.as_ref())
            .ok_or(ApplicationError::Missing { application })?;
        resources.publication()
    }

    /// Runs `operation` against the stored Rx MQ resources of `application`.
    ///
    /// The control path uses this to read and free one ext-config chunk
    /// owned by the Application (see `ExtConfigStore`).
    pub(crate) fn with_application_mq<R>(
        &self,
        application: u32,
        operation: impl FnOnce(&ApplicationMqResources) -> Result<R, ApplicationError>,
    ) -> Result<R, ApplicationError> {
        let resources = self
            .state()?
            .applications
            .get(application)
            .and_then(|application| application.mq_resources.as_ref())
            .ok_or(ApplicationError::Missing { application })?;
        operation(resources)
    }

    pub fn contains(&self, application: u32) -> Result<bool, ApplicationError> {
        Ok(self.state()?.applications.contains_key(application))
    }

    pub fn detach(&self, application: u32) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        let state = unsafe { &*self.state.get() };
        if !state.applications.contains_key(application) {
            return Err(ApplicationError::Missing { application });
        }

        match GlobalMain::with_current(|engine| -> Result<(), ApplicationError> {
            let sessions = super::runtime::session_main();
            sessions
                .application_detached(engine, application)
                .map_err(|source| ApplicationError::MqDetachFailed { source })
        }) {
            Some(result) => result,
            None => Ok(()),
        }?;

        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published Application records.
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let mut detach = || -> Result<(), ApplicationError> {
            let (listener_indexes, connection_indexes) = {
                let entry = state
                    .applications
                    .get_mut(application)
                    .ok_or(ApplicationError::Missing { application })?;
                (
                    std::mem::take(&mut entry.listeners),
                    std::mem::take(&mut entry.connections),
                )
            };
            for index in listener_indexes {
                state.listeners.remove(index);
            }
            for index in connection_indexes {
                state.connections.remove(index);
            }
            state
                .applications
                .remove(application)
                .ok_or(ApplicationError::Missing { application })?;
            Ok(())
        };
        match barrier {
            Some(barrier) if barrier.is_pending() => detach(),
            Some(barrier) => barrier.sync(detach),
            None => detach(),
        }?;

        Ok(())
    }

    pub fn register_listener(
        &self,
        application: u32,
        app: Option<u32>,
        opaque: Option<u64>,
    ) -> Result<u32, ApplicationError> {
        self.ensure_active(application)?;
        let listener = ApplicationListener {
            application,
            app,
            opaque,
        };
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published listener records.
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let register = || {
            let index = state.listeners.insert(listener);
            state
                .applications
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

    pub fn register_connection(
        &self,
        application: u32,
        context: u64,
        server_name: Option<String>,
        app: Option<u32>,
        opaque: Option<u64>,
    ) -> Result<u32, ApplicationError> {
        self.ensure_active(application)?;
        let connection = ApplicationConnection {
            application,
            context,
            app,
            opaque,
            server_name,
            connect_state: AtomicU8::new(CONNECTION_CONNECTING),
        };
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published connection records.
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let register = || {
            // Reap connected entries ahead of the primary insert. VPP
            // session_free is void best-effort cleanup (session.c:258-265):
            // a vanished entry is logged, never fatal to the insert.
            let connected = state
                .connections
                .iter()
                .filter_map(|(index, connection)| {
                    (connection.connect_state.load(Ordering::Acquire) == CONNECTION_CONNECTED)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            for index in connected {
                if let Some(removed) = state.connections.remove(index) {
                    if let Some(application_entry) = state.applications.get_mut(removed.application)
                    {
                        application_entry
                            .connections
                            .retain(|entry| *entry != index);
                    }
                }
            }
            let index = state.connections.insert(connection);
            state
                .applications
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

    pub(crate) fn with_connection<R>(
        &self,
        connection: u32,
        operation: impl FnOnce(&ApplicationConnection) -> R,
    ) -> Result<R, ApplicationError> {
        // SAFETY: workers only read published entries. Main Thread mutation is
        // synchronized by the worker barrier.
        let state = unsafe { &*self.state.get() };
        let index = connection;
        state
            .connections
            .get(index)
            .map(operation)
            .ok_or(ApplicationError::ConnectionMissing { connection })
    }

    pub(crate) fn mark_connected(&self, connection: u32) -> Result<(), ApplicationError> {
        let entry =
            self.with_connection(connection, |entry| entry as *const ApplicationConnection)?;
        // SAFETY: the entry remains published until Main Thread observes the
        // connected state under the worker barrier and reaps it.
        let entry = unsafe { &*entry };
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

    pub fn remove_connection(
        &self,
        application: u32,
        connection: u32,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published connection records.
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let mut remove = || {
            let index = connection;
            let entry = state
                .connections
                .get(index)
                .ok_or(ApplicationError::ConnectionMissing { connection })?;
            if entry.application != application {
                return Err(ApplicationError::ConnectionNotOwned {
                    application,
                    connection,
                });
            }
            state
                .connections
                .remove(index)
                .ok_or(ApplicationError::ConnectionMissing { connection })?;
            state
                .applications
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

    pub fn reclaim_connection(
        &self,
        application: u32,
        connection: u32,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published connection records.
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let mut reclaim = || {
            let index = connection;
            let entry = state
                .connections
                .get(index)
                .ok_or(ApplicationError::ConnectionMissing { connection })?;
            if entry.application != application {
                return Err(ApplicationError::ConnectionNotOwned {
                    application,
                    connection,
                });
            }
            if entry.connect_state.load(Ordering::Acquire) != CONNECTION_CONNECTED {
                return Err(ApplicationError::ConnectionNotConnected { connection });
            }
            state
                .connections
                .remove(index)
                .ok_or(ApplicationError::ConnectionMissing { connection })?;
            state
                .applications
                .get_mut(application)
                .expect("active Application remains allocated")
                .connections
                .retain(|entry| *entry != connection);
            Ok(())
        };
        match barrier {
            Some(barrier) if barrier.is_pending() => reclaim(),
            Some(barrier) => barrier.sync(reclaim),
            None => reclaim(),
        }
    }

    pub fn remove_listener(
        &self,
        application: u32,
        listener_id: u32,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        self.with_listener(listener_id, |listener| {
            if listener.application != application {
                return Err(ApplicationError::ListenerNotOwned {
                    application,
                    listener: listener_id,
                });
            }
            Ok(())
        })??;
        self.ensure_main_thread()?;
        // SAFETY: Main Thread mutation is synchronized by the barrier below;
        // Data Workers only read published listener records.
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let mut remove = || {
            let index = listener_id;
            if !state.listeners.contains_key(index) {
                return Err(ApplicationError::ListenerMissing {
                    listener: listener_id,
                });
            }
            let entry = state
                .listeners
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
            drop(state.listeners.remove(index));
            state
                .applications
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
        let state = unsafe { &mut *self.state.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        let mut update = || {
            let index = listener_id;
            if !state.listeners.contains_key(index) {
                return Err(ApplicationError::ListenerMissing {
                    listener: listener_id,
                });
            }
            let listener =
                state
                    .listeners
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

    pub(crate) fn with_listener<R>(
        &self,
        listener: u32,
        operation: impl FnOnce(&ApplicationListener) -> R,
    ) -> Result<R, ApplicationError> {
        // SAFETY: production callers are the Main Thread or a Data Worker that
        // participates in `barrier`; listener mutation stops every Data Worker.
        let state = unsafe { &*self.state.get() };
        let index = listener;
        if !state.listeners.contains_key(index) {
            return Err(ApplicationError::ListenerMissing { listener });
        }
        Ok(operation(
            state
                .listeners
                .get(index)
                .ok_or(ApplicationError::ListenerMissing { listener })?,
        ))
    }

    fn ensure_active(&self, application: u32) -> Result<(), ApplicationError> {
        if self.state()?.applications.contains_key(application) {
            Ok(())
        } else {
            Err(ApplicationError::Missing { application })
        }
    }

    fn state(&self) -> Result<&ApplicationState, ApplicationError> {
        self.ensure_main_thread()?;
        // SAFETY: the Main control check above confines all state access to the
        // control path; immutable access does not overlap a mutable call there.
        Ok(unsafe { &*self.state.get() })
    }

    fn ensure_main_thread(&self) -> Result<(), ApplicationError> {
        match GlobalMain::with_current(|engine| engine.ensure_main_thread()) {
            Some(Ok(())) => Ok(()),
            Some(Err(_)) | None => Err(ApplicationError::WrongThread),
        }
    }
}

#[hammer_component_macros::runtime_error(subsystem = "application")]
#[derive(Debug, Error)]
pub enum ApplicationError {
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
    #[error("per-Application MQ worker installation failed")]
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
    #[error("Application listener {listener:?} is not owned by Application {application:?}")]
    ListenerNotOwned { application: u32, listener: u32 },
    #[error("Application connection {connection:?} is not registered")]
    ConnectionMissing { connection: u32 },
    #[error("Application connection {connection:?} is not owned by Application {application:?}")]
    ConnectionNotOwned { application: u32, connection: u32 },
    #[error("Application connection {connection:?} was already connected")]
    ConnectionAlreadyConnected { connection: u32 },
    #[error("Application connection {connection:?} is not connected")]
    ConnectionNotConnected { connection: u32 },
}

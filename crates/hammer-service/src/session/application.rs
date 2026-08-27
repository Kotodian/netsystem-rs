use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread::{self, ThreadId};

use hammer_infra::pool::Pool;
use hammer_infra::segment::Segment;
use hammer_runtime::Engine;
use hammer_runtime::app::{SessionAppRegistration, SessionMsgQueue, SessionMsgQueueError};
use hammer_runtime::attach::{ApplicationMqPublication, ExtConfigStore};
use hammer_runtime::{AttachError, DataWorkerId, RuntimeError};
use thiserror::Error;

use super::config::Session;

struct ApplicationState {
    applications: Pool<()>,
    listeners: Pool<ApplicationListener>,
    connections: Pool<ApplicationConnection>,
    mq_resources: Vec<Option<ApplicationMqResources>>,
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
    application: u32,
    segment: Segment,
    queues: Box<[Arc<SessionMsgQueue>]>,
    offsets: Box<[u64]>,
    ext_config: Option<ExtConfigStore>,
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
            let name = format!("hammer-app-rx-mq-{}-{}", std::process::id(), application);
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
            application,
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
    owner: ThreadId,
    state: UnsafeCell<ApplicationState>,
    session_apps: Box<[SessionAppRegistration]>,
}

const APP_MQ_CAPACITY_MIN: usize = 128;
const APP_MQ_SEGMENT_HEADROOM: usize = 1 << 20;

// SAFETY: mutable state access is restricted to the creating Main Thread.
// Data Workers may read published listener and connection entries; their
// mutation or removal occurs only while WorkerBarrier stops those readers.
// ApplicationRegistration remains !Send.
unsafe impl Send for ApplicationMain {}
// SAFETY: worker reads follow the publication contract above, and the
// connecting/connected transition changes only its dedicated atomic state.
unsafe impl Sync for ApplicationMain {}

impl ApplicationMain {
    pub fn new(capacity: usize) -> Arc<Self> {
        Self::with_session_apps(capacity, [])
    }

    pub fn with_session_apps(
        capacity: usize,
        session_apps: impl IntoIterator<Item = SessionAppRegistration>,
    ) -> Arc<Self> {
        Arc::new(Self {
            owner: thread::current().id(),
            state: UnsafeCell::new(ApplicationState {
                applications: Pool::with_capacity(capacity),
                listeners: Pool::with_capacity(capacity),
                connections: Pool::with_capacity(capacity),
                mq_resources: std::iter::repeat_with(|| None).take(capacity).collect(),
            }),
            session_apps: session_apps
                .into_iter()
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    pub fn register_local(self: &Arc<Self>) -> Result<ApplicationRegistration, ApplicationError> {
        let application = match Engine::with_current(|engine| engine.registry.get::<Session>()) {
            Some(Some(_)) => self.attach_local_with_runtime()?,
            _ => self.attach()?,
        };
        Ok(ApplicationRegistration {
            main: Arc::clone(self),
            application: Some(application),
            thread_bound: PhantomData,
        })
    }

    pub fn attach(&self) -> Result<u32, ApplicationError> {
        self.with_state_mut(|state| state.applications.insert(()))
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
        let (worker_count, mq_capacity) =
            Engine::with_current(|engine| -> Result<(usize, usize), ApplicationError> {
                let session = engine
                    .registry
                    .get::<Session>()
                    .ok_or(ApplicationError::SessionMainMissing)?;
                Ok((engine.configured_worker_count(), session.app_mq_capacity))
            })
            .ok_or(ApplicationError::SessionMainMissing)??;
        self.attach_with_mq(worker_count, mq_capacity, shared)
    }

    #[cfg(test)]
    pub(crate) fn attach_local_for_test(
        &self,
        worker_count: usize,
        mq_capacity: usize,
    ) -> Result<u32, ApplicationError> {
        let application = self.attach()?;
        let resources =
            ApplicationMqResources::create_local(application, worker_count, mq_capacity)?;
        self.store_mq_resources(application, resources)?;
        Ok(application)
    }

    fn attach_with_mq(
        &self,
        worker_count: usize,
        mq_capacity: usize,
        shared: bool,
    ) -> Result<u32, ApplicationError> {
        let application = self.attach()?;
        let resources = if shared {
            ApplicationMqResources::create_external(application, worker_count, mq_capacity)
        } else {
            ApplicationMqResources::create_local(application, worker_count, mq_capacity)
        }?;
        let install_result = Engine::with_current(|engine| {
            let main = engine
                .registry
                .get::<super::runtime::SessionMain>()
                .ok_or(ApplicationError::SessionMainMissing)?;
            main.install_application_mqs(engine, application, &resources)
                .map_err(|source| ApplicationError::MqInstall { source })
        });
        match install_result {
            Some(Ok(())) => {
                self.store_mq_resources(application, resources)?;
                Ok(application)
            }
            Some(Err(error)) => Err(error),
            None => Err(ApplicationError::SessionMainMissing),
        }
    }

    fn store_mq_resources(
        &self,
        application: u32,
        resources: ApplicationMqResources,
    ) -> Result<(), ApplicationError> {
        self.with_state_mut(|state| {
            let slot = application as usize;
            if slot >= state.mq_resources.len() {
                state.mq_resources.resize_with(slot + 1, || None);
            }
            if state.mq_resources[slot].is_some() {
                return Err(ApplicationError::MqAlreadyAttached { application });
            }
            state.mq_resources[slot] = Some(resources);
            Ok(())
        })?
    }

    /// Returns the runtime-neutral MQ publication used by the attach server.
    pub fn application_mq_publication(
        &self,
        application: u32,
    ) -> Result<ApplicationMqPublication, ApplicationError> {
        let resources = self
            .state()?
            .mq_resources
            .get(application as usize)
            .and_then(Option::as_ref)
            .filter(|resources| resources.application == application)
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
            .mq_resources
            .get(application as usize)
            .and_then(Option::as_ref)
            .filter(|resources| resources.application == application)
            .ok_or(ApplicationError::Missing { application })?;
        operation(resources)
    }

    pub fn contains(&self, application: u32) -> Result<bool, ApplicationError> {
        Ok(self.state()?.applications.contains_key(application))
    }

    pub fn detach(&self, application: u32) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        self.with_state_mut(|state| -> Result<(), ApplicationError> {
            if !state.applications.contains_key(application) {
                return Err(ApplicationError::Missing { application });
            }
            Ok(())
        })??;

        match Engine::with_current(|engine| -> Result<(), ApplicationError> {
            let sessions = engine
                .registry
                .get::<super::runtime::SessionMain>()
                .ok_or(ApplicationError::SessionMainMissing)?;
            sessions
                .application_detached(engine, application)
                .map_err(|source| ApplicationError::MqDetachFailed { source })
        }) {
            Some(result) => result,
            None => Ok(()),
        }?;

        self.with_state_mut(|state| -> Result<(), ApplicationError> {
            if let Some(slot) = state.mq_resources.get_mut(application as usize)
                && slot
                    .as_ref()
                    .is_some_and(|resources| resources.application == application)
            {
                *slot = None;
            }
            let listener_indexes = state
                .listeners
                .iter()
                .filter_map(|(index, listener)| {
                    (listener.application == application).then_some(index)
                })
                .collect::<Vec<_>>();
            for index in listener_indexes {
                state.listeners.remove(index);
            }
            let connection_indexes = state
                .connections
                .iter()
                .filter_map(|(index, connection)| {
                    (connection.application == application).then_some(index)
                })
                .collect::<Vec<_>>();
            for index in connection_indexes {
                state.connections.remove(index);
            }
            state.applications.remove(application);
            Ok(())
        })??;

        Ok(())
    }

    pub fn register_listener(
        &self,
        application: u32,
        app: Option<u32>,
        opaque: Option<u64>,
    ) -> Result<u32, ApplicationError> {
        self.ensure_active(application)?;
        self.validate_session_app(app)?;
        let listener = ApplicationListener {
            application,
            app,
            opaque,
        };
        self.with_state_mut(|state| state.listeners.insert(listener))
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
        self.validate_session_app(app)?;
        let connection = ApplicationConnection {
            application,
            context,
            app,
            opaque,
            server_name,
            connect_state: AtomicU8::new(CONNECTION_CONNECTING),
        };
        self.with_state_mut(|state| {
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
                drop(state.connections.remove(index));
            }
            let index = state.connections.insert(connection);
            index
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
        self.with_state_mut(|state| {
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
            Ok(())
        })?
    }

    pub fn reclaim_connection(
        &self,
        application: u32,
        connection: u32,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        self.with_state_mut(|state| {
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
            Ok(())
        })?
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
        self.with_state_mut(|state| {
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
            Ok(())
        })?
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
        self.with_state_mut(|state| {
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
        })?
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

    pub(crate) fn session_app(
        &self,
        name: &str,
    ) -> Result<SessionAppRegistration, ApplicationError> {
        let mut found = None;
        for entry in self.session_apps.iter().copied() {
            if entry.name() != name {
                continue;
            }
            if found.is_some() {
                return Err(ApplicationError::SessionAppDuplicate {
                    name: name.to_owned(),
                });
            }
            found = Some(entry);
        }
        found.ok_or_else(|| ApplicationError::SessionAppMissing {
            name: name.to_owned(),
        })
    }

    pub fn session_app_id(&self, name: &str) -> Result<u32, ApplicationError> {
        self.session_app(name).map(|_| {
            self.session_apps
                .iter()
                .position(|entry| entry.name() == name)
                .map(|index| index as u32)
                .expect("resolved Session App remains in the registration list")
        })
    }

    pub(crate) fn session_app_registration(&self, app: u32) -> Option<SessionAppRegistration> {
        self.session_apps.get(app as usize).copied()
    }

    fn validate_session_app(&self, app: Option<u32>) -> Result<(), ApplicationError> {
        let Some(app) = app else {
            return Ok(());
        };
        if (app as usize) < self.session_apps.len() {
            Ok(())
        } else {
            Err(ApplicationError::SessionAppUnregistered { app })
        }
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
        // SAFETY: the owner check above confines all state access to one thread;
        // immutable access does not overlap a mutable call on that thread.
        Ok(unsafe { &*self.state.get() })
    }

    fn with_state_mut<R>(
        &self,
        operation: impl FnOnce(&mut ApplicationState) -> R,
    ) -> Result<R, ApplicationError> {
        self.ensure_main_thread()?;
        // SAFETY: the owner check confines mutation to Main Thread, and the
        // worker barrier stops every listener reader during the operation.
        let state = unsafe { &mut *self.state.get() };
        let barrier = Engine::with_current(|engine| engine.worker_barrier());
        Ok(match barrier {
            Some(barrier) if barrier.is_pending() => operation(state),
            Some(barrier) => barrier.sync(|| operation(state)),
            None => operation(state),
        })
    }

    fn ensure_main_thread(&self) -> Result<(), ApplicationError> {
        if thread::current().id() == self.owner {
            Ok(())
        } else {
            Err(ApplicationError::WrongThread)
        }
    }
}

/// Local Application capability. Dropping it detaches the Application.
pub struct ApplicationRegistration {
    main: Arc<ApplicationMain>,
    application: Option<u32>,
    thread_bound: PhantomData<Rc<()>>,
}

impl ApplicationRegistration {
    #[inline]
    pub fn application(&self) -> u32 {
        self.application
            .expect("live Application registration retains its identity")
    }

    pub fn detach(mut self) -> Result<(), ApplicationError> {
        let application = self
            .application
            .take()
            .expect("live Application registration retains its identity");
        self.main.detach(application)
    }
}

impl Drop for ApplicationRegistration {
    fn drop(&mut self) {
        let Some(application) = self.application.take() else {
            return;
        };
        self.main
            .detach(application)
            .expect("Local Application registration is detached on its owning Main Thread");
    }
}

#[hammer_component_macros::runtime_error(subsystem = "application")]
#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("Application {application:?} is not attached")]
    Missing { application: u32 },
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
    #[error("Session Main is not ready for per-Application MQ attach")]
    SessionMainMissing,
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
    #[error("Session App `{name}` is not registered")]
    SessionAppMissing { name: String },
    #[error("Session App `{name}` is registered more than once")]
    SessionAppDuplicate { name: String },
    #[error("Session App id {app:?} is not registered")]
    SessionAppUnregistered { app: u32 },
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

#[cfg(test)]
mod tests {
    use hammer_runtime::DataWorkerId;

    use super::{ApplicationError, ApplicationMain, ApplicationMqResources};

    fn register_connection(main: &ApplicationMain, application: u32) -> u32 {
        main.register_connection(application, 0, None, None, None)
            .expect("register Application connection")
    }

    #[test]
    fn local_mq_resources_create_one_signal_queue_per_data_worker() {
        let main = ApplicationMain::new(2);
        let application = main.attach().expect("attach Application");
        let resources =
            ApplicationMqResources::create_local(application, 2, 2048).expect("MQ resources");

        assert_eq!(resources.worker_count(), 2);
        for worker in [DataWorkerId::new(0), DataWorkerId::new(1)] {
            let queue = resources.queue(worker).expect("worker MQ");
            assert!(queue.read_fd().is_some());
            assert!(queue.write_fd().is_some());
        }
    }

    #[test]
    fn attach_local_without_runtime_does_not_publish_mq_resources() {
        let main = ApplicationMain::new(1);
        let error = main
            .attach_local(1, 128)
            .expect_err("attach requires Session Main");

        assert!(matches!(error, ApplicationError::SessionMainMissing));
        let state = main.state().expect("read Application state");
        assert!(state.mq_resources.iter().all(Option::is_none));
    }

    #[test]
    fn detach_releases_application_mq_resources() {
        let main = ApplicationMain::new(1);
        let application = main
            .attach_local_for_test(1, 128)
            .expect("attach local Application");

        main.detach(application).expect("detach local Application");

        let state = main.state().expect("read Application state");
        assert!(state.applications.is_empty());
        assert!(state.mq_resources.iter().all(Option::is_none));
    }

    #[test]
    fn reclaimed_connection_slot_is_reused_by_next_connection() {
        let main = ApplicationMain::new(1);
        let application = main.attach().expect("attach Application");
        let connection = register_connection(&main, application);

        main.mark_connected(connection)
            .expect("mark Application connection connected");
        main.reclaim_connection(application, connection)
            .expect("reclaim connected Application connection");

        assert_eq!(
            main.state()
                .expect("read Application state")
                .connections
                .len(),
            0
        );
        let replacement = register_connection(&main, application);
        assert_eq!(connection, replacement);
    }

    #[test]
    fn register_connection_reclaims_connected_slot_before_next_insert() {
        let main = ApplicationMain::new(1);
        let application = main.attach().expect("attach Application");
        let connected = register_connection(&main, application);

        main.mark_connected(connected)
            .expect("mark Application connection connected");
        let replacement = register_connection(&main, application);

        assert_eq!(connected, replacement);
        assert!(main.with_connection(connected, |_| ()).is_ok());
    }

    #[test]
    fn reclamation_uses_exact_connect_state_identity_in_any_order() {
        let main = ApplicationMain::new(4);
        let application = main.attach().expect("attach Application");
        let first = register_connection(&main, application);
        let second = register_connection(&main, application);

        main.mark_connected(second)
            .expect("mark second Application connection connected");
        main.mark_connected(first)
            .expect("mark first Application connection connected");
        main.reclaim_connection(application, second)
            .expect("reclaim second Application connection");
        main.reclaim_connection(application, first)
            .expect("reclaim first Application connection");

        assert_eq!(
            main.state()
                .expect("read Application state")
                .connections
                .len(),
            0
        );
    }

    #[test]
    fn connecting_connection_is_not_reclaimed() {
        let main = ApplicationMain::new(1);
        let application = main.attach().expect("attach Application");
        let connection = register_connection(&main, application);

        let error = main
            .reclaim_connection(application, connection)
            .expect_err("connecting Application connection is not reclaimable");
        assert!(matches!(
            error,
            ApplicationError::ConnectionNotConnected { connection: rejected }
                if rejected == connection
        ));
    }

    #[test]
    fn reused_connection_index_refers_to_replacement() {
        let main = ApplicationMain::new(1);
        let application = main.attach().expect("attach Application");
        let first = register_connection(&main, application);

        main.mark_connected(first)
            .expect("mark first Application connection connected");
        main.reclaim_connection(application, first)
            .expect("reclaim first Application connection");
        let replacement = register_connection(&main, application);
        assert_eq!(first, replacement);

        let error = main
            .reclaim_connection(application, first)
            .expect_err("replacement Application connection is still connecting");
        assert!(matches!(
            error,
            ApplicationError::ConnectionNotConnected { connection: rejected }
                if rejected == first
        ));
    }

    #[test]
    fn detach_removes_connecting_and_connected_connections() {
        let main = ApplicationMain::new(2);
        let application = main.attach().expect("attach Application");
        let connecting = register_connection(&main, application);
        let connected = register_connection(&main, application);

        main.mark_connected(connected)
            .expect("mark Application connection connected");
        main.detach(application)
            .expect("detach Application with connecting and connected connections");

        let state = main.state().expect("read Application state");
        assert!(state.applications.is_empty());
        assert!(state.connections.is_empty());
        assert!(matches!(
            main.reclaim_connection(application, connecting),
            Err(ApplicationError::Missing { application: missing })
                if missing == application
        ));
    }
}

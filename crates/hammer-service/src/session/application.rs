use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread::{self, ThreadId};

use hammer_infra::pool::{Index, Pool};
use hammer_infra::segment::Segment;
use hammer_runtime::Engine;
use hammer_runtime::app::{
    AppSessionPolicy, AppSessionProtocolEntry, ApplicationConnectionId, ApplicationId,
    ApplicationListenerId, SessionMsgQueue, SessionMsgQueueError,
};
use hammer_runtime::{DataWorkerId, RuntimeError};
use thiserror::Error;

use super::config::Session;

struct ApplicationState {
    applications: Pool<()>,
    listeners: Pool<ApplicationListener>,
    connections: Pool<ApplicationConnection>,
    mq_resources: Vec<Option<ApplicationMqResources>>,
}

pub(crate) struct ApplicationListener {
    application: ApplicationId,
    protocols: Box<[ApplicationProtocol]>,
}

pub(crate) struct ApplicationConnection {
    application: ApplicationId,
    protocols: Box<[ApplicationProtocol]>,
    server_name: Option<String>,
    completion: AtomicU8,
}

#[derive(Clone, Copy)]
pub(crate) struct ApplicationProtocol {
    entry: AppSessionProtocolEntry,
    id: Option<u64>,
}

impl ApplicationListener {
    #[inline]
    pub(crate) const fn application(&self) -> ApplicationId {
        self.application
    }

    #[inline]
    pub(crate) fn protocols(&self) -> &[ApplicationProtocol] {
        &self.protocols
    }
}

impl ApplicationProtocol {
    #[inline]
    pub(crate) const fn entry(self) -> AppSessionProtocolEntry {
        self.entry
    }

    #[inline]
    pub(crate) const fn id(self) -> Option<u64> {
        self.id
    }
}

impl ApplicationConnection {
    #[inline]
    pub(crate) const fn application(&self) -> ApplicationId {
        self.application
    }

    #[inline]
    pub(crate) fn protocols(&self) -> &[ApplicationProtocol] {
        &self.protocols
    }

    #[inline]
    pub(crate) fn server_name(&self) -> Option<&str> {
        self.server_name.as_deref()
    }
}

/// Per-Application message queues, one for every Data Worker.
///
/// The queues are owned by `ApplicationMain` and are published to Data
/// Workers at attach time. The shared worker TX queue remains available as a
/// fallback for callers that have not opted into per-Application MQs.
pub struct ApplicationMqResources {
    application: ApplicationId,
    segment: Segment,
    queues: Box<[Arc<SessionMsgQueue>]>,
    offsets: Box<[u64]>,
}

impl ApplicationMqResources {
    pub(crate) fn create_local(
        application: ApplicationId,
        worker_count: usize,
        capacity: usize,
    ) -> Result<Self, ApplicationError> {
        Self::create(application, worker_count, capacity, false)
    }

    pub(crate) fn create_external(
        application: ApplicationId,
        worker_count: usize,
        capacity: usize,
    ) -> Result<Self, ApplicationError> {
        Self::create(application, worker_count, capacity, true)
    }

    fn create(
        application: ApplicationId,
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
                application.raw()
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
        Ok(Self {
            application,
            segment,
            queues: queues.into_boxed_slice(),
            offsets: offsets.into_boxed_slice(),
        })
    }

    #[inline]
    pub fn worker_count(&self) -> usize {
        self.queues.len()
    }

    #[inline]
    pub(crate) fn queue(&self, worker: DataWorkerId) -> Option<&Arc<SessionMsgQueue>> {
        self.queues.get(worker.slot())
    }

    #[inline]
    pub(crate) fn segment(&self) -> &Segment {
        &self.segment
    }

    #[inline]
    pub(crate) fn offset(&self, worker: DataWorkerId) -> Option<u64> {
        self.offsets.get(worker.slot()).copied()
    }
}

const CONNECTION_PENDING: u8 = 0;
const CONNECTION_COMPLETED: u8 = 1;

/// Main Thread authority for Application identity and lifetime.
pub struct ApplicationMain {
    owner: ThreadId,
    state: UnsafeCell<ApplicationState>,
    protocols: Box<[AppSessionProtocolEntry]>,
}

const APP_MQ_CAPACITY_MIN: usize = 128;
const APP_MQ_SEGMENT_HEADROOM: usize = 1 << 20;

// SAFETY: every state access verifies the creating Main Thread before
// dereferencing `state`; ApplicationRegistration is !Send and Binary API calls
// execute on that same thread.
unsafe impl Send for ApplicationMain {}
// SAFETY: shared references can be retained by RuntimeRegistry, but methods do
// not touch state from any thread other than `owner`.
unsafe impl Sync for ApplicationMain {}

impl ApplicationMain {
    pub fn new(capacity: usize) -> Arc<Self> {
        Self::with_protocols(capacity, [])
    }

    pub fn with_protocols(
        capacity: usize,
        protocols: impl IntoIterator<Item = AppSessionProtocolEntry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            owner: thread::current().id(),
            state: UnsafeCell::new(ApplicationState {
                applications: Pool::with_capacity(capacity),
                listeners: Pool::with_capacity(capacity),
                connections: Pool::with_capacity(capacity),
                mq_resources: std::iter::repeat_with(|| None).take(capacity).collect(),
            }),
            protocols: protocols.into_iter().collect::<Vec<_>>().into_boxed_slice(),
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

    pub fn attach(&self) -> Result<ApplicationId, ApplicationError> {
        self.with_state_mut(|state| {
            state.applications.insert(()).map(application_id).ok_or(
                ApplicationError::CapacityExhausted {
                    capacity: state.applications.capacity(),
                },
            )
        })?
    }

    /// Attaches an external Application and creates one private Session
    /// Message Queue for every Data Worker.
    pub fn attach_external(
        &self,
        worker_count: usize,
        mq_capacity: usize,
    ) -> Result<ApplicationId, ApplicationError> {
        self.attach_with_mq(worker_count, mq_capacity, true)
    }

    /// Attaches a local Application and creates one private Session Message
    /// Queue for every Data Worker.
    pub fn attach_local(
        &self,
        worker_count: usize,
        mq_capacity: usize,
    ) -> Result<ApplicationId, ApplicationError> {
        self.attach_with_mq(worker_count, mq_capacity, false)
    }

    /// Attaches an external Application using the current runtime Session
    /// configuration.
    pub fn attach_external_with_runtime(&self) -> Result<ApplicationId, ApplicationError> {
        self.attach_with_runtime(true)
    }

    /// Attaches a local Application using the current runtime Session
    /// configuration.
    pub fn attach_local_with_runtime(&self) -> Result<ApplicationId, ApplicationError> {
        self.attach_with_runtime(false)
    }

    fn attach_with_runtime(&self, shared: bool) -> Result<ApplicationId, ApplicationError> {
        let (worker_count, mq_capacity) = Engine::with_current(|engine| {
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
    ) -> Result<ApplicationId, ApplicationError> {
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
    ) -> Result<ApplicationId, ApplicationError> {
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
                if let Err(primary) = self.store_mq_resources(application, resources) {
                    if let Err(cleanup) = self.rollback_attach(application) {
                        return Err(cleanup);
                    }
                    return Err(primary);
                }
                Ok(application)
            }
            Some(Err(primary)) => {
                if let Err(cleanup) = self.rollback_attach(application) {
                    return Err(cleanup);
                }
                Err(primary)
            }
            None => {
                let primary = ApplicationError::SessionMainMissing;
                if let Err(cleanup) = self.rollback_attach(application) {
                    return Err(cleanup);
                }
                Err(primary)
            }
        }
    }

    fn store_mq_resources(
        &self,
        application: ApplicationId,
        resources: ApplicationMqResources,
    ) -> Result<(), ApplicationError> {
        self.with_state_mut(|state| {
            let slot = application.slot() as usize;
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

    pub(crate) fn mq_worker(
        &self,
        application: ApplicationId,
        worker: DataWorkerId,
    ) -> Option<(Arc<SessionMsgQueue>, Option<Segment>, u64)> {
        // SAFETY: MQ resources are published under the worker barrier and are
        // only read by Data Workers during session construction.
        let state = unsafe { &*self.state.get() };
        let resources = state
            .mq_resources
            .get(application.slot() as usize)?
            .as_ref()
            .filter(|resources| resources.application == application)?;
        let queue = resources.queue(worker)?.clone();
        let offset = resources.offset(worker)?;
        Some((queue, Some(resources.segment().clone()), offset))
    }

    fn rollback_attach(&self, application: ApplicationId) -> Result<(), ApplicationError> {
        self.detach(application)
    }

    pub fn contains(&self, application: ApplicationId) -> Result<bool, ApplicationError> {
        Ok(self
            .state()?
            .applications
            .get(application_index(application))
            .is_some())
    }

    pub fn detach(&self, application: ApplicationId) -> Result<(), ApplicationError> {
        self.ensure_main_thread()?;
        let index = application_index(application);
        self.with_state_mut(|state| {
            state
                .applications
                .get(index)
                .ok_or(ApplicationError::Missing { application })?;
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

        self.with_state_mut(|state| {
            if let Some(slot) = state.mq_resources.get_mut(application.slot() as usize)
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
                state
                    .listeners
                    .remove(index)
                    .expect("selected Application listener remains present until removal");
            }
            let connection_indexes = state
                .connections
                .iter()
                .filter_map(|(index, connection)| {
                    (connection.application == application).then_some(index)
                })
                .collect::<Vec<_>>();
            for index in connection_indexes {
                state
                    .connections
                    .remove(index)
                    .expect("selected Application connection remains present until removal");
            }
            state
                .applications
                .remove(index)
                .expect("detaching Application remains allocated until removal");
            Ok(())
        })??;

        Ok(())
    }

    pub fn register_listener(
        &self,
        application: ApplicationId,
        policy: &AppSessionPolicy,
    ) -> Result<ApplicationListenerId, ApplicationError> {
        self.ensure_active(application)?;
        let listener = self.resolve_listener(application, policy)?;
        self.with_state_mut(|state| {
            state
                .listeners
                .insert(listener)
                .map(application_listener_id)
                .ok_or(ApplicationError::ListenerCapacityExhausted {
                    capacity: state.listeners.capacity(),
                })
        })?
    }

    pub fn register_connection(
        &self,
        application: ApplicationId,
        server_name: Option<String>,
        policy: &AppSessionPolicy,
    ) -> Result<ApplicationConnectionId, ApplicationError> {
        self.ensure_active(application)?;
        let protocols = self.resolve_policy(policy)?;
        let connection = ApplicationConnection {
            application,
            protocols,
            server_name,
            completion: AtomicU8::new(CONNECTION_PENDING),
        };
        self.with_state_mut(|state| {
            state
                .connections
                .insert(connection)
                .map(application_connection_id)
                .ok_or(ApplicationError::ConnectionCapacityExhausted {
                    capacity: state.connections.capacity(),
                })
        })?
    }

    pub(crate) fn with_connection<R>(
        &self,
        connection: ApplicationConnectionId,
        operation: impl FnOnce(&ApplicationConnection) -> R,
    ) -> Result<R, ApplicationError> {
        // SAFETY: workers only read published entries. Main Thread mutation is
        // synchronized by the worker barrier.
        let state = unsafe { &*self.state.get() };
        state
            .connections
            .get(application_connection_index(connection))
            .map(operation)
            .ok_or(ApplicationError::ConnectionMissing { connection })
    }

    pub(crate) fn complete_connection(
        &self,
        connection: ApplicationConnectionId,
    ) -> Result<(), ApplicationError> {
        let entry =
            self.with_connection(connection, |entry| entry as *const ApplicationConnection)?;
        // SAFETY: the entry remains published until Main Thread observes the
        // completed state under the worker barrier and reaps it.
        let entry = unsafe { &*entry };
        entry
            .completion
            .compare_exchange(
                CONNECTION_PENDING,
                CONNECTION_COMPLETED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map_err(|_| ApplicationError::ConnectionAlreadyCompleted { connection })?;
        Ok(())
    }

    pub(crate) fn remove_connection(
        &self,
        application: ApplicationId,
        connection: ApplicationConnectionId,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        self.with_state_mut(|state| {
            let entry = state
                .connections
                .get(application_connection_index(connection))
                .ok_or(ApplicationError::ConnectionMissing { connection })?;
            if entry.application != application {
                return Err(ApplicationError::ConnectionNotOwned {
                    application,
                    connection,
                });
            }
            state
                .connections
                .remove(application_connection_index(connection))
                .expect("validated Application connection remains present until removal");
            Ok(())
        })?
    }

    pub(crate) fn reclaim_connection(
        &self,
        application: ApplicationId,
        connection: ApplicationConnectionId,
    ) -> Result<(), ApplicationError> {
        self.ensure_active(application)?;
        self.with_state_mut(|state| {
            let entry = state
                .connections
                .get(application_connection_index(connection))
                .ok_or(ApplicationError::ConnectionMissing { connection })?;
            if entry.application != application {
                return Err(ApplicationError::ConnectionNotOwned {
                    application,
                    connection,
                });
            }
            if entry.completion.load(Ordering::Acquire) != CONNECTION_COMPLETED {
                return Err(ApplicationError::ConnectionNotCompleted { connection });
            }
            state
                .connections
                .remove(application_connection_index(connection))
                .expect(
                    "validated completed Application connection remains present until reclamation",
                );
            Ok(())
        })?
    }

    pub fn remove_listener(
        &self,
        application: ApplicationId,
        listener_id: ApplicationListenerId,
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
            let entry = state
                .listeners
                .get(application_listener_index(listener_id))
                .ok_or(ApplicationError::ListenerMissing {
                    listener: listener_id,
                })?;
            if entry.application != application {
                return Err(ApplicationError::ListenerNotOwned {
                    application,
                    listener: listener_id,
                });
            }
            state
                .listeners
                .remove(application_listener_index(listener_id))
                .expect("validated Application listener remains present until removal");
            Ok(())
        })?
    }

    pub(crate) fn with_listener<R>(
        &self,
        listener: ApplicationListenerId,
        operation: impl FnOnce(&ApplicationListener) -> R,
    ) -> Result<R, ApplicationError> {
        // SAFETY: production callers are the Main Thread or a Data Worker that
        // participates in `barrier`; listener mutation stops every Data Worker.
        let state = unsafe { &*self.state.get() };
        state
            .listeners
            .get(application_listener_index(listener))
            .map(operation)
            .ok_or(ApplicationError::ListenerMissing { listener })
    }

    fn resolve_listener(
        &self,
        application: ApplicationId,
        policy: &AppSessionPolicy,
    ) -> Result<ApplicationListener, ApplicationError> {
        let protocols = self.resolve_policy(policy)?;
        Ok(ApplicationListener {
            application,
            protocols,
        })
    }

    fn resolve_policy(
        &self,
        policy: &AppSessionPolicy,
    ) -> Result<Box<[ApplicationProtocol]>, ApplicationError> {
        let mut protocols = Vec::with_capacity(policy.protocols().len());
        for selection in policy.protocols() {
            let entry = unique_protocol(&self.protocols, selection.protocol())?;
            protocols.push(ApplicationProtocol {
                entry,
                id: selection.id(),
            });
        }
        Ok(protocols.into_boxed_slice())
    }

    fn ensure_active(&self, application: ApplicationId) -> Result<(), ApplicationError> {
        if self
            .state()?
            .applications
            .get(application_index(application))
            .is_some()
        {
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
            Some(barrier) => barrier.sync(state, operation),
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
    application: Option<ApplicationId>,
    thread_bound: PhantomData<Rc<()>>,
}

impl ApplicationRegistration {
    #[inline]
    pub fn application(&self) -> ApplicationId {
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

#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error("Application capacity {capacity} is exhausted")]
    CapacityExhausted { capacity: usize },
    #[error("Application {application:?} is not attached")]
    Missing { application: ApplicationId },
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
    #[error("Application {application:?} already owns per-Application MQ resources")]
    MqAlreadyAttached { application: ApplicationId },
    #[error("App Session protocol `{protocol}` is not registered")]
    ProtocolMissing { protocol: String },
    #[error("App Session protocol `{protocol}` is registered more than once")]
    ProtocolDuplicate { protocol: String },
    #[error("Application listener capacity {capacity} is exhausted")]
    ListenerCapacityExhausted { capacity: usize },
    #[error("Application connection capacity {capacity} is exhausted")]
    ConnectionCapacityExhausted { capacity: usize },
    #[error("Application listener {listener:?} is not registered")]
    ListenerMissing { listener: ApplicationListenerId },
    #[error("Application listener {listener:?} is not owned by Application {application:?}")]
    ListenerNotOwned {
        application: ApplicationId,
        listener: ApplicationListenerId,
    },
    #[error("Application connection {connection:?} is not registered")]
    ConnectionMissing { connection: ApplicationConnectionId },
    #[error("Application connection {connection:?} is not owned by Application {application:?}")]
    ConnectionNotOwned {
        application: ApplicationId,
        connection: ApplicationConnectionId,
    },
    #[error("Application connection {connection:?} was already completed")]
    ConnectionAlreadyCompleted { connection: ApplicationConnectionId },
    #[error("Application connection {connection:?} is not completed")]
    ConnectionNotCompleted { connection: ApplicationConnectionId },
}

#[inline]
fn application_id(index: Index) -> ApplicationId {
    ApplicationId::new(index.slot(), index.generation())
}

#[inline]
fn application_index(application: ApplicationId) -> Index {
    Index::new(application.slot(), application.generation())
}

#[inline]
fn application_listener_id(index: Index) -> ApplicationListenerId {
    ApplicationListenerId::new(index.slot(), index.generation())
}

#[inline]
fn application_listener_index(listener: ApplicationListenerId) -> Index {
    Index::new(listener.slot(), listener.generation())
}

#[inline]
fn application_connection_id(index: Index) -> ApplicationConnectionId {
    ApplicationConnectionId::new(index.slot(), index.generation())
}

#[inline]
fn application_connection_index(connection: ApplicationConnectionId) -> Index {
    Index::new(connection.slot(), connection.generation())
}

fn unique_protocol(
    protocols: &[AppSessionProtocolEntry],
    name: &str,
) -> Result<AppSessionProtocolEntry, ApplicationError> {
    let mut found = None;
    for protocol in protocols.iter().copied() {
        if protocol.registration().name() != name {
            continue;
        }
        if found.is_some() {
            return Err(ApplicationError::ProtocolDuplicate {
                protocol: name.to_owned(),
            });
        }
        found = Some(protocol);
    }
    found.ok_or_else(|| ApplicationError::ProtocolMissing {
        protocol: name.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use hammer_runtime::DataWorkerId;
    use hammer_runtime::app::{APP_SESSION_POLICY_VERSION, AppSessionPolicy, SessionEventQueue};

    use super::{
        ApplicationConnectionId, ApplicationError, ApplicationId, ApplicationMain,
        ApplicationMqResources,
    };

    fn policy() -> AppSessionPolicy {
        AppSessionPolicy::new(APP_SESSION_POLICY_VERSION, []).expect("direct App Session policy")
    }

    fn register_connection(
        main: &ApplicationMain,
        application: ApplicationId,
    ) -> ApplicationConnectionId {
        main.register_connection(application, None, &policy())
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
    fn attach_local_without_runtime_rolls_back_identity_and_mq_resources() {
        let main = ApplicationMain::new(1);
        let error = main
            .attach_local(1, 128)
            .expect_err("attach requires Session Main");

        assert!(matches!(error, ApplicationError::SessionMainMissing));
        let state = main.state().expect("read Application state");
        assert!(state.applications.is_empty());
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
    fn completed_connection_reclaims_exact_identity_without_later_connect() {
        let main = ApplicationMain::new(1);
        let application = main.attach().expect("attach Application");
        let connection = register_connection(&main, application);

        main.complete_connection(connection)
            .expect("complete Application connection");
        main.reclaim_connection(application, connection)
            .expect("reclaim completed Application connection");

        assert_eq!(
            main.state()
                .expect("read Application state")
                .connections
                .len(),
            0
        );
        let replacement = register_connection(&main, application);
        assert_ne!(connection, replacement);
    }

    #[test]
    fn reclamation_uses_exact_completion_identity_in_any_order() {
        let main = ApplicationMain::new(4);
        let application = main.attach().expect("attach Application");
        let first = register_connection(&main, application);
        let second = register_connection(&main, application);

        main.complete_connection(second)
            .expect("complete second Application connection");
        main.complete_connection(first)
            .expect("complete first Application connection");
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
    fn pending_connection_is_not_reclaimed() {
        let main = ApplicationMain::new(1);
        let application = main.attach().expect("attach Application");
        let connection = register_connection(&main, application);

        let error = main
            .reclaim_connection(application, connection)
            .expect_err("pending Application connection is not reclaimable");
        assert!(matches!(
            error,
            ApplicationError::ConnectionNotCompleted { connection: rejected }
                if rejected == connection
        ));
    }

    #[test]
    fn stale_connection_identity_cannot_reclaim_replacement() {
        let main = ApplicationMain::new(1);
        let application = main.attach().expect("attach Application");
        let first = register_connection(&main, application);

        main.complete_connection(first)
            .expect("complete first Application connection");
        main.reclaim_connection(application, first)
            .expect("reclaim first Application connection");
        let replacement = register_connection(&main, application);
        assert_ne!(first, replacement);

        let error = main
            .reclaim_connection(application, first)
            .expect_err("stale Application connection identity is rejected");
        assert!(matches!(
            error,
            ApplicationError::ConnectionMissing { connection: rejected }
                if rejected == first
        ));
    }

    #[test]
    fn detach_removes_pending_and_completed_connections() {
        let main = ApplicationMain::new(2);
        let application = main.attach().expect("attach Application");
        let pending = register_connection(&main, application);
        let completed = register_connection(&main, application);

        main.complete_connection(completed)
            .expect("complete Application connection");
        main.detach(application)
            .expect("detach Application with pending and completed connections");

        let state = main.state().expect("read Application state");
        assert!(state.applications.is_empty());
        assert!(state.connections.is_empty());
        assert!(matches!(
            main.reclaim_connection(application, pending),
            Err(ApplicationError::Missing { application: missing })
                if missing == application
        ));
    }
}

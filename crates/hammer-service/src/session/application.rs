use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::thread::{self, ThreadId};

use hammer_infra::pool::{Index, Pool};
use hammer_runtime::Engine;
use hammer_runtime::app::{
    AppSessionPolicy, AppSessionProtocolEntry, ApplicationConnectionId, ApplicationId,
    ApplicationListenerId, ORDERED_RELIABLE_BYTE_STREAM,
};
use thiserror::Error;

struct ApplicationState {
    applications: Pool<()>,
    listeners: Pool<ApplicationListener>,
    connections: Pool<ApplicationConnection>,
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

const CONNECTION_PENDING: u8 = 0;
const CONNECTION_COMPLETED: u8 = 1;

/// Main Thread authority for Application identity and lifetime.
pub struct ApplicationMain {
    owner: ThreadId,
    state: UnsafeCell<ApplicationState>,
    protocols: Box<[AppSessionProtocolEntry]>,
}

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
            }),
            protocols: protocols.into_iter().collect::<Vec<_>>().into_boxed_slice(),
        })
    }

    pub fn register_local(self: &Arc<Self>) -> Result<ApplicationRegistration, ApplicationError> {
        let application = self.attach()?;
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

        notify_session_runtime(application);
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
        self.reap_connections()?;
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
        let mut semantics = ORDERED_RELIABLE_BYTE_STREAM;
        let mut lower_name = "session";
        for selection in policy.protocols() {
            let entry = unique_protocol(&self.protocols, selection.protocol())?;
            let registration = entry.registration();
            if semantics != registration.lower() {
                return Err(ApplicationError::SemanticsMismatch {
                    lower: lower_name,
                    provides: semantics,
                    upper: registration.name(),
                    requires: registration.lower(),
                });
            }
            protocols.push(ApplicationProtocol {
                entry,
                id: selection.id(),
            });
            semantics = registration.upper();
            lower_name = registration.name();
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

    fn reap_connections(&self) -> Result<(), ApplicationError> {
        self.with_state_mut(|state| {
            let completed = state
                .connections
                .iter()
                .filter_map(|(index, connection)| {
                    (connection.completion.load(Ordering::Acquire) == CONNECTION_COMPLETED)
                        .then_some(index)
                })
                .collect::<Vec<_>>();
            for index in completed {
                state
                    .connections
                    .remove(index)
                    .expect("completed Application connection remains present until reap");
            }
        })
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

fn notify_session_runtime(application: ApplicationId) {
    Engine::with_current(|engine| {
        let Some(sessions) = engine.registry.get::<super::runtime::SessionMain>() else {
            return;
        };
        sessions.application_detached(engine, application);
    });
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
    #[error("App Session protocol `{protocol}` is not registered")]
    ProtocolMissing { protocol: String },
    #[error("App Session protocol `{protocol}` is registered more than once")]
    ProtocolDuplicate { protocol: String },
    #[error(
        "`{lower}` provides `{provides}` App Session semantics but `{upper}` requires `{requires}`"
    )]
    SemanticsMismatch {
        lower: &'static str,
        provides: &'static str,
        upper: &'static str,
        requires: &'static str,
    },
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

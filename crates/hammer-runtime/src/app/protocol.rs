use hammer_infra::fifo::Fifo;
use hammer_infra::pool::{Index, Pool};
use hammer_infra::thread_owned::{ThreadOwned, ThreadOwnedError};
use thiserror::Error;

use crate::{DataWorkerId, RuntimeError, RuntimeResult};

use super::{ApplicationId, SessionHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppSessionProtocolRole {
    Client,
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct AppSessionProtocolConnectionId(u64);

impl AppSessionProtocolConnectionId {
    #[inline]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self((slot as u64) | ((generation as u64) << 32))
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }
}

/// One concrete protocol operating between two adjacent App Sessions.
pub trait AppSessionProtocol: Sized + Send + 'static {
    const CONNECTION_CAPACITY: usize = 1_024;

    fn create(
        _: Option<ApplicationId>,
        _: AppSessionProtocolRole,
        _: Option<u64>,
        _: Option<&str>,
    ) -> RuntimeResult<Self> {
        Err(RuntimeError::from(
            AppSessionProtocolConnectionError::CreationUnsupported,
        ))
    }

    fn ingress(
        &mut self,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)>;

    fn egress(
        &mut self,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)>;

    /// Reports whether this connection may expose its upper Session.
    #[inline]
    fn ready(&self) -> bool {
        true
    }
}

#[hammer_component_macros::runtime_error(subsystem = "app-session-protocol")]
#[derive(Debug, Error)]
enum AppSessionProtocolConnectionError {
    #[error("App Session protocol worker {worker} is outside worker count {worker_count}")]
    WorkerOutOfRange { worker: usize, worker_count: usize },
    #[error(
        "App Session protocol connection storage was initialized for {actual} workers, not {expected}"
    )]
    WorkerCountMismatch { expected: usize, actual: usize },
    #[error("App Session protocol connection capacity {capacity} is exhausted on worker {worker}")]
    CapacityExhausted { worker: usize, capacity: usize },
    #[error("App Session protocol connection {connection:?} is not owned by worker {worker}")]
    Missing {
        worker: usize,
        connection: AppSessionProtocolConnectionId,
    },
    #[error("App Session protocol connections for worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: ThreadOwnedError,
    },
    #[error("App Session protocol does not provide registered connection construction")]
    CreationUnsupported,
}

#[doc(hidden)]
pub struct AppSessionProtocolConnections<P> {
    workers: Box<[ThreadOwned<Pool<AppSessionProtocolConnection<P>>>]>,
    capacity: usize,
}

struct AppSessionProtocolConnection<P> {
    protocol: P,
    session_handle: SessionHandle,
    app_session_handle: SessionHandle,
    ready: bool,
}

impl<P> AppSessionProtocolConnections<P>
where
    P: Send,
{
    pub fn new(worker_count: usize, capacity: usize) -> Self {
        Self {
            workers: (0..worker_count)
                .map(|_| ThreadOwned::new())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            capacity,
        }
    }

    pub fn insert(
        &self,
        worker: DataWorkerId,
        worker_count: usize,
        protocol: P,
        session_handle: SessionHandle,
        app_session_handle: SessionHandle,
    ) -> RuntimeResult<AppSessionProtocolConnectionId> {
        let connections = self.worker(worker, worker_count)?;
        match connections.with_mut(|_| ()) {
            Ok(()) => {}
            Err(ThreadOwnedError::NotInstalled) => {
                if let Err(connections) = connections.install(Pool::with_capacity(self.capacity)) {
                    drop(connections);
                }
            }
            Err(source) => return Err(protocol_worker_access(worker, source)),
        }
        connections
            .with_mut(|connections| {
                connections
                    .insert(AppSessionProtocolConnection {
                        protocol,
                        session_handle,
                        app_session_handle,
                        ready: false,
                    })
                    .map(protocol_connection_id)
                    .ok_or(AppSessionProtocolConnectionError::CapacityExhausted {
                        worker: worker.slot(),
                        capacity: connections.capacity(),
                    })
            })
            .map_err(|source| protocol_worker_access(worker, source))?
            .map_err(RuntimeError::from)
    }

    pub fn with_mut<R>(
        &self,
        worker: DataWorkerId,
        connection: AppSessionProtocolConnectionId,
        operation: impl FnOnce(&mut P) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let connections = self.worker(worker, self.workers.len())?;
        Ok(connections
            .with_mut(|connections| {
                let protocol = connections
                    .get_mut(protocol_connection_index(connection))
                    .ok_or(AppSessionProtocolConnectionError::Missing {
                        worker: worker.slot(),
                        connection,
                    })?;
                operation(&mut protocol.protocol)
                    .map_err(ProtocolConnectionOperationError::Operation)
            })
            .map_err(|source| protocol_worker_access(worker, source))??)
    }

    pub fn sessions(
        &self,
        worker: DataWorkerId,
        connection: AppSessionProtocolConnectionId,
    ) -> RuntimeResult<(SessionHandle, SessionHandle)> {
        let connections = self.worker(worker, self.workers.len())?;
        connections
            .with_mut(|connections| {
                connections
                    .get(protocol_connection_index(connection))
                    .map(|connection| (connection.session_handle, connection.app_session_handle))
                    .ok_or(AppSessionProtocolConnectionError::Missing {
                        worker: worker.slot(),
                        connection,
                    })
            })
            .map_err(|source| protocol_worker_access(worker, source))?
            .map_err(RuntimeError::from)
    }

    pub fn claim_ready(
        &self,
        worker: DataWorkerId,
        connection: AppSessionProtocolConnectionId,
    ) -> RuntimeResult<bool>
    where
        P: AppSessionProtocol,
    {
        let connections = self.worker(worker, self.workers.len())?;
        connections
            .with_mut(
                |connections| -> Result<bool, AppSessionProtocolConnectionError> {
                    let connection = connections
                        .get_mut(protocol_connection_index(connection))
                        .ok_or(AppSessionProtocolConnectionError::Missing {
                            worker: worker.slot(),
                            connection,
                        })?;
                    if connection.ready || !connection.protocol.ready() {
                        return Ok(false);
                    }
                    connection.ready = true;
                    Ok(true)
                },
            )
            .map_err(|source| protocol_worker_access(worker, source))?
            .map_err(RuntimeError::from)
    }

    pub fn remove(&self, worker: DataWorkerId, connection: AppSessionProtocolConnectionId) {
        let connections = self
            .workers
            .get(worker.slot())
            .expect("App Session protocol connection worker exists");
        connections
            .with_mut(|connections| {
                connections
                    .remove(protocol_connection_index(connection))
                    .map(drop)
                    .expect("App Session protocol connection is removed exactly once");
            })
            .expect("App Session protocol connection is removed by its owning Data Worker");
    }

    fn worker(
        &self,
        worker: DataWorkerId,
        worker_count: usize,
    ) -> RuntimeResult<&ThreadOwned<Pool<AppSessionProtocolConnection<P>>>> {
        if self.workers.len() != worker_count {
            return Err(RuntimeError::from(
                AppSessionProtocolConnectionError::WorkerCountMismatch {
                    expected: self.workers.len(),
                    actual: worker_count,
                },
            ));
        }
        self.workers.get(worker.slot()).ok_or_else(|| {
            RuntimeError::from(AppSessionProtocolConnectionError::WorkerOutOfRange {
                worker: worker.slot(),
                worker_count,
            })
        })
    }
}

enum ProtocolConnectionOperationError {
    Missing(AppSessionProtocolConnectionError),
    Operation(RuntimeError),
}

impl From<AppSessionProtocolConnectionError> for ProtocolConnectionOperationError {
    fn from(error: AppSessionProtocolConnectionError) -> Self {
        Self::Missing(error)
    }
}

impl From<ProtocolConnectionOperationError> for RuntimeError {
    fn from(error: ProtocolConnectionOperationError) -> Self {
        match error {
            ProtocolConnectionOperationError::Missing(error) => RuntimeError::from(error),
            ProtocolConnectionOperationError::Operation(error) => error,
        }
    }
}

fn protocol_worker_access(worker: DataWorkerId, source: ThreadOwnedError) -> RuntimeError {
    RuntimeError::from(AppSessionProtocolConnectionError::WorkerAccess {
        worker: worker.slot(),
        source,
    })
}

/// Static identity declared by one App Session protocol implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppSessionProtocolRegistration {
    name: &'static str,
}

impl AppSessionProtocolRegistration {
    #[inline]
    pub const fn new(name: &'static str) -> Self {
        Self { name }
    }

    #[inline]
    pub const fn name(self) -> &'static str {
        self.name
    }
}

/// One component-macro-generated App Session protocol registration.
#[derive(Clone, Copy)]
pub struct AppSessionProtocolEntry {
    registration: AppSessionProtocolRegistration,
    create: fn(
        DataWorkerId,
        usize,
        Option<ApplicationId>,
        AppSessionProtocolRole,
        Option<u64>,
        Option<&str>,
        SessionHandle,
        SessionHandle,
    ) -> RuntimeResult<AppSessionProtocolConnectionId>,
    ingress: fn(
        DataWorkerId,
        AppSessionProtocolConnectionId,
        &Fifo,
        &Fifo,
    ) -> RuntimeResult<(usize, usize)>,
    egress: fn(
        DataWorkerId,
        AppSessionProtocolConnectionId,
        &Fifo,
        &Fifo,
    ) -> RuntimeResult<(usize, usize)>,
    claim_ready: fn(DataWorkerId, AppSessionProtocolConnectionId) -> RuntimeResult<bool>,
    sessions: fn(
        DataWorkerId,
        AppSessionProtocolConnectionId,
    ) -> RuntimeResult<(SessionHandle, SessionHandle)>,
    destroy: fn(DataWorkerId, AppSessionProtocolConnectionId),
}

impl AppSessionProtocolEntry {
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        name: &'static str,
        create: fn(
            DataWorkerId,
            usize,
            Option<ApplicationId>,
            AppSessionProtocolRole,
            Option<u64>,
            Option<&str>,
            SessionHandle,
            SessionHandle,
        ) -> RuntimeResult<AppSessionProtocolConnectionId>,
        ingress: fn(
            DataWorkerId,
            AppSessionProtocolConnectionId,
            &Fifo,
            &Fifo,
        ) -> RuntimeResult<(usize, usize)>,
        egress: fn(
            DataWorkerId,
            AppSessionProtocolConnectionId,
            &Fifo,
            &Fifo,
        ) -> RuntimeResult<(usize, usize)>,
        claim_ready: fn(DataWorkerId, AppSessionProtocolConnectionId) -> RuntimeResult<bool>,
        sessions: fn(
            DataWorkerId,
            AppSessionProtocolConnectionId,
        ) -> RuntimeResult<(SessionHandle, SessionHandle)>,
        destroy: fn(DataWorkerId, AppSessionProtocolConnectionId),
    ) -> Self {
        Self {
            registration: AppSessionProtocolRegistration::new(name),
            create,
            ingress,
            egress,
            claim_ready,
            sessions,
            destroy,
        }
    }

    #[inline]
    pub const fn registration(self) -> AppSessionProtocolRegistration {
        self.registration
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        self,
        worker: DataWorkerId,
        worker_count: usize,
        application: Option<ApplicationId>,
        role: AppSessionProtocolRole,
        id: Option<u64>,
        server_name: Option<&str>,
        session_handle: SessionHandle,
        app_session_handle: SessionHandle,
    ) -> RuntimeResult<AppSessionProtocolConnectionId> {
        (self.create)(
            worker,
            worker_count,
            application,
            role,
            id,
            server_name,
            session_handle,
            app_session_handle,
        )
    }

    pub fn ingress(
        self,
        worker: DataWorkerId,
        connection: AppSessionProtocolConnectionId,
        lower_rx_fifo: &Fifo,
        upper_rx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        (self.ingress)(worker, connection, lower_rx_fifo, upper_rx_fifo)
    }

    pub fn egress(
        self,
        worker: DataWorkerId,
        connection: AppSessionProtocolConnectionId,
        upper_tx_fifo: &Fifo,
        lower_tx_fifo: &Fifo,
    ) -> RuntimeResult<(usize, usize)> {
        (self.egress)(worker, connection, upper_tx_fifo, lower_tx_fifo)
    }

    #[inline]
    pub fn claim_ready(
        self,
        worker: DataWorkerId,
        connection: AppSessionProtocolConnectionId,
    ) -> RuntimeResult<bool> {
        (self.claim_ready)(worker, connection)
    }

    #[inline]
    pub fn sessions(
        self,
        worker: DataWorkerId,
        connection: AppSessionProtocolConnectionId,
    ) -> RuntimeResult<(SessionHandle, SessionHandle)> {
        (self.sessions)(worker, connection)
    }

    pub fn destroy(self, worker: DataWorkerId, connection: AppSessionProtocolConnectionId) {
        (self.destroy)(worker, connection)
    }
}

#[inline]
fn protocol_connection_id(index: Index) -> AppSessionProtocolConnectionId {
    AppSessionProtocolConnectionId::new(index.slot(), index.generation())
}

#[inline]
fn protocol_connection_index(connection: AppSessionProtocolConnectionId) -> Index {
    Index::new(connection.slot(), connection.generation())
}

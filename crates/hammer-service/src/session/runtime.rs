use std::cell::UnsafeCell;
use std::num::NonZeroU32;
use std::os::fd::BorrowedFd;
use std::sync::Arc;
use std::thread::{self, ThreadId};
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, DataPlaneBuffers, Index as BufferIndex, NodeState};
use hammer_infra::align::{CacheLine, align_up};
use hammer_infra::fifo::Fifo;
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::segment::Segment;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{
    AppSession, AppSessionConfig, AppSessionError, AppSessionProtocolConnectionId,
    AppSessionProtocolEntry, AppSessionProtocolRole, ApplicationConnectionId, ApplicationId,
    ApplicationListenerId, SessionEventQueue, SessionEvt, SessionEvtFlags, SessionEvtType,
    SessionHandle, SessionMsgQueue,
};
use hammer_runtime::attach::AppSessionPublisher;
use hammer_runtime::{
    AttachError, RuntimeError, RuntimeResult, SessionConnectEndpoint, SessionConnectionId,
    SessionListenEndpoint, SessionListenerId, SessionTransportRegistration,
};
use hammer_runtime::{DataPlaneRuntime, DataWorkerId, Engine, File, FileFunctions};

use crate::session::app::{AppWorkerAttach, AppWorkerError};
use crate::session::application::{ApplicationMain, ApplicationProtocol};
use crate::session::error::{SessionError, SessionQueueError};
use crate::session::node::{
    AppSessionInputNode, SESSION_QUEUE_IO_BUDGET, SessionQueueTransportDispatch,
};
use crate::session::state::SessionState;
use crate::session::{AppWorker, SessionId, SessionQueueNext};

const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;
const DEFAULT_SESSION_TX_EVENT_CAPACITY: usize = 2048;
const DEFAULT_TX_DISPATCH_BUDGET: usize = 64;
const PROTOCOL_ADVANCE_BUDGET: usize = 64;

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionTransportId(u8);

impl SessionTransportId {
    #[inline]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionControlEvent {
    Close(SessionId),
    TransportClosed(SessionId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OooSpan {
    start: u32,
    len: NonZeroU32,
}

impl OooSpan {
    #[inline]
    pub const fn new(start: u32, len: NonZeroU32) -> Self {
        Self { start, len }
    }

    #[inline]
    pub const fn start(self) -> u32 {
        self.start
    }

    #[inline]
    pub const fn len(self) -> NonZeroU32 {
        self.len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RxDelivery {
    NotAccepted {
        rx_available: u32,
    },
    InOrder {
        accepted: NonZeroU32,
        promoted: u32,
        rx_available: u32,
    },
    OutOfOrder {
        accepted: NonZeroU32,
        newest: OooSpan,
        rx_available: u32,
    },
}

const _: () = assert!(core::mem::size_of::<RxDelivery>() <= 24);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionQueueStep {
    pub scheduled_sessions: usize,
}

#[derive(Clone, Copy)]
enum SessionType<Index> {
    Transport {
        transport: SessionTransportId,
        state: SessionState<Index>,
    },
    AppSessionProtocol {
        protocol: AppSessionProtocolEntry,
        connection: AppSessionProtocolConnectionId,
    },
}

#[derive(Clone, Copy)]
enum SessionApplication {
    AppSessionProtocol {
        protocol: AppSessionProtocolEntry,
        connection: AppSessionProtocolConnectionId,
    },
    External(ApplicationId),
}

struct SessionEntry<Index> {
    session_type: Option<SessionType<Index>>,
    application: Option<SessionApplication>,
    rx_fifo: Arc<Fifo>,
    tx_fifo: Arc<Fifo>,
    schedule_pending: bool,
}

impl<Index: Copy + Eq> SessionEntry<Index> {
    #[inline]
    fn creating_transport(
        transport: SessionTransportId,
        rx_fifo: Arc<Fifo>,
        tx_fifo: Arc<Fifo>,
    ) -> Self {
        Self {
            session_type: Some(SessionType::Transport {
                transport,
                state: SessionState::creating(),
            }),
            application: None,
            rx_fifo,
            tx_fifo,
            schedule_pending: false,
        }
    }

    #[inline]
    fn unbound(rx_fifo: Arc<Fifo>, tx_fifo: Arc<Fifo>) -> Self {
        Self {
            session_type: None,
            application: None,
            rx_fifo,
            tx_fifo,
            schedule_pending: false,
        }
    }
}

pub struct SessionWorker<Index> {
    worker: DataWorkerId,
    worker_count: usize,
    entries: Pool<SessionEntry<Index>>,
    listener_main: Option<Arc<SessionMain>>,
    applications: Option<Arc<ApplicationMain>>,
    app: AppWorker,
    session_evt_q: Arc<SessionMsgQueue>,
    app_session_config: AppSessionConfig,
    pub(crate) transport_dispatches: Vec<SessionQueueTransportDispatch>,
    session_work: Vec<SessionId>,
    session_work_scratch: Vec<SessionId>,
    app_rx_events: FifoQueue<SessionId>,
    control_events: FifoQueue<SessionControlEvent>,
    readiness_file: Option<PoolIndex>,
}

pub struct SessionMain {
    workers: Box<[CacheLine<ThreadOwned<SessionWorker<PoolIndex>>>]>,
    owner: ThreadId,
    listeners: UnsafeCell<Pool<SessionListener>>,
    applications: Arc<ApplicationMain>,
}

pub(super) struct SessionListener {
    application: ApplicationId,
    application_listener: ApplicationListenerId,
    transport: SessionTransportRegistration,
}

impl SessionListener {
    #[inline]
    pub(super) const fn application(&self) -> ApplicationId {
        self.application
    }

    pub(super) const fn application_listener(&self) -> ApplicationListenerId {
        self.application_listener
    }
}

// SAFETY: Main Thread publishes listener state under the worker barrier; Data
// Workers only read the immutable entry selected by their transport callback.
unsafe impl Send for SessionMain {}
// SAFETY: `listeners` mutation is confined to `owner` and synchronized by the
// worker barrier before a Data Worker may observe it.
unsafe impl Sync for SessionMain {}

fn session_worker_error(error: SessionQueueError) -> RuntimeError {
    RuntimeError::subsystem("session", error)
}

#[inline]
fn session_listener_id(index: PoolIndex) -> SessionListenerId {
    SessionListenerId::new(index.slot(), index.generation())
}

#[inline]
fn session_listener_index(listener: SessionListenerId) -> PoolIndex {
    PoolIndex::new(listener.slot(), listener.generation())
}

impl SessionMain {
    pub(super) fn applications(&self) -> &ApplicationMain {
        &self.applications
    }

    pub fn new(worker_count: usize, applications: Arc<ApplicationMain>) -> Self {
        let workers = (0..worker_count)
            .map(|_| CacheLine::new(ThreadOwned::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            workers,
            owner: thread::current().id(),
            listeners: UnsafeCell::new(Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY)),
            applications,
        }
    }

    pub fn listen(
        &self,
        application_listener: ApplicationListenerId,
        transport: SessionTransportRegistration,
        endpoint: SessionListenEndpoint,
    ) -> RuntimeResult<SessionListenerId> {
        let application = self
            .applications
            .with_listener(application_listener, |listener| listener.application())
            .map_err(|source| RuntimeError::subsystem("application", source))?;
        let listener = self.with_listeners_mut(|listeners| {
            listeners
                .insert(SessionListener {
                    application,
                    application_listener,
                    transport,
                })
                .map(session_listener_id)
                .ok_or(SessionError::ListenerCapacityExhausted {
                    capacity: listeners.capacity(),
                })
        })??;
        let Some(start_listen) = transport.start_listen() else {
            self.with_listeners_mut(|listeners| {
                listeners
                    .remove(session_listener_index(listener))
                    .expect("new Session listener remains present until rollback");
            })?;
            return Err(SessionError::TransportListenUnsupported {
                transport: transport.name(),
            }
            .into());
        };
        if let Err(error) = start_listen(listener, endpoint) {
            self.with_listeners_mut(|listeners| {
                listeners
                    .remove(session_listener_index(listener))
                    .expect("new Session listener remains present until rollback");
            })?;
            return Err(error);
        }
        Ok(listener)
    }

    pub fn unlisten(&self, listener: SessionListenerId) -> RuntimeResult<()> {
        let transport = self.with_listener(listener, |entry| entry.transport)?;
        let Some(stop_listen) = transport.stop_listen() else {
            return Err(SessionError::TransportListenUnsupported {
                transport: transport.name(),
            }
            .into());
        };
        stop_listen(listener)?;
        self.with_listeners_mut(|listeners| {
            listeners
                .remove(session_listener_index(listener))
                .ok_or(SessionError::ListenerMissing { listener })
                .map(drop)
        })??;
        Ok(())
    }

    pub fn connect(
        &self,
        connection: ApplicationConnectionId,
        transport: SessionTransportRegistration,
        endpoint: SessionConnectEndpoint,
    ) -> RuntimeResult<SessionConnectionId> {
        let Some(connect) = transport.connect() else {
            return Err(SessionError::TransportConnectUnsupported {
                transport: transport.name(),
            }
            .into());
        };
        let session_connection = SessionConnectionId::from_raw(connection.raw());
        connect(session_connection, endpoint)?;
        Ok(session_connection)
    }

    pub(super) fn with_listener<R>(
        &self,
        listener: SessionListenerId,
        operation: impl FnOnce(&SessionListener) -> R,
    ) -> RuntimeResult<R> {
        // SAFETY: Data Workers read a listener only after Main Thread has
        // published it through the worker barrier.
        let listeners = unsafe { &*self.listeners.get() };
        listeners
            .get(session_listener_index(listener))
            .map(operation)
            .ok_or_else(|| SessionError::ListenerMissing { listener }.into())
    }

    fn with_listeners_mut<R>(
        &self,
        operation: impl FnOnce(&mut Pool<SessionListener>) -> R,
    ) -> RuntimeResult<R> {
        if thread::current().id() != self.owner {
            return Err(SessionError::ListenerControlWrongThread.into());
        }
        // SAFETY: only the Main Thread mutates this pool while Data Workers
        // are stopped by the barrier below.
        let listeners = unsafe { &mut *self.listeners.get() };
        let barrier = Engine::with_current(|engine| engine.worker_barrier());
        Ok(match barrier {
            Some(barrier) => barrier.sync(listeners, operation),
            None => operation(listeners),
        })
    }

    fn worker(
        &self,
        worker: DataWorkerId,
    ) -> RuntimeResult<&ThreadOwned<SessionWorker<PoolIndex>>> {
        self.workers
            .get(worker.slot())
            .map(|slot| &**slot)
            .ok_or_else(|| {
                session_worker_error(SessionQueueError::WorkerOutOfRange {
                    worker: worker.slot(),
                })
            })
    }

    pub fn with_worker_mut<R>(
        &self,
        runtime: &DataPlaneRuntime,
        operation: impl FnOnce(&mut SessionWorker<PoolIndex>) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let thread_index = runtime.thread_index();
        let worker = thread_index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or_else(|| {
                session_worker_error(SessionQueueError::WorkerUnavailable { thread_index })
            })?;
        self.worker(worker)?.with_mut(operation).map_err(|source| {
            session_worker_error(SessionQueueError::WorkerAccess {
                worker: worker.slot(),
                source,
            })
        })?
    }

    pub(crate) fn application_detached(
        self: &Arc<Self>,
        engine: &Engine,
        application: ApplicationId,
    ) {
        let listeners = {
            // SAFETY: this call runs on the Main Thread. Listener removal
            // below stops Data Workers through the same barrier as publish.
            let listeners = unsafe { &*self.listeners.get() };
            listeners
                .iter()
                .filter_map(|(index, listener)| {
                    (listener.application() == application).then_some(session_listener_id(index))
                })
                .collect::<Vec<_>>()
        };
        for listener in listeners {
            self.unlisten(listener)
                .expect("detached Application listener is removed before worker cleanup");
        }
        for worker_slot in 0..self.workers.len() {
            let worker = DataWorkerId::new(worker_slot as u32);
            loop {
                let main = Arc::clone(self);
                match engine.schedule_on_worker(worker, move || {
                    main.worker(worker)
                        .expect("scheduled Application detach targets an existing Session worker")
                        .with_mut(|sessions| {
                            sessions.application_detached(application);
                            sessions.signal_queue();
                        })
                        .expect("scheduled Application detach runs on its Session worker");
                }) {
                    Ok(()) => break,
                    Err(RuntimeError::WorkerControlQueueFull { .. }) => {
                        std::thread::yield_now();
                    }
                    Err(
                        RuntimeError::WorkerControlUnavailable { .. }
                        | RuntimeError::WorkerControlClosed { .. },
                    ) => break,
                    Err(error) => {
                        panic!(
                            "Application detach could not notify Session worker {worker_slot}: {error}"
                        );
                    }
                }
            }
        }
    }
}

pub fn install_session_worker(
    main: &Arc<SessionMain>,
    engine: &mut Engine,
    app_session_input: hammer_core::data_plane::NodeId,
    session_queue: hammer_core::data_plane::NodeId,
    mut worker: SessionWorker<PoolIndex>,
) -> RuntimeResult<()> {
    worker.set_listener_main(Arc::clone(main));
    let session_queue_data =
        hammer_runtime::NodeRuntimeData::from_usize(Arc::as_ptr(main) as usize)?;
    let input_data = AppSessionInputNode::worker_runtime_data(session_queue_data, session_queue);
    engine.set_worker_node_runtime_data(app_session_input, input_data)?;
    engine.set_worker_node_runtime_data(session_queue, session_queue_data)?;
    engine
        .runtime
        .nodes()
        .set_node_state(app_session_input, NodeState::Interrupt)?;
    worker.install_queue_readiness(engine, app_session_input)?;
    let worker_id = worker.worker();
    let slot = main.worker(worker_id)?;
    if let Err(mut worker) = slot.install(worker) {
        worker.remove_queue_readiness(engine)?;
        return Err(session_worker_error(
            SessionQueueError::WorkerAlreadyInstalled {
                worker: worker_id.slot(),
            },
        ));
    }
    Ok(())
}

impl<Index: Copy + Eq> SessionWorker<Index> {
    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn app_session_config(&self) -> AppSessionConfig {
        self.app_session_config
    }

    #[inline]
    pub fn queue_signal_descriptor(&self) -> Option<std::os::fd::RawFd> {
        self.app.tx_evt_q().read_fd()
    }

    #[inline]
    pub fn signal_queue(&self) {
        self.app.tx_evt_q().fire();
    }

    #[cfg(test)]
    pub(crate) fn local_app(&self) -> &AppWorker {
        &self.app
    }

    #[cfg(test)]
    pub(crate) fn session_fifos(&self, session_id: SessionId) -> Option<(&Arc<Fifo>, &Arc<Fifo>)> {
        self.entries
            .get(session_id.pool_index())
            .map(|entry| (&entry.rx_fifo, &entry.tx_fifo))
    }

    /// Registers the external app message-queue signal with the worker FileMain.
    ///
    /// The queue retains its original endpoint. FileMain owns a duplicated
    /// descriptor and must remove it before this worker is replaced.
    pub fn install_queue_readiness(
        &mut self,
        engine: &mut Engine,
        app_session_input: hammer_core::data_plane::NodeId,
    ) -> RuntimeResult<()> {
        self.remove_queue_readiness(engine)?;
        let Some(signal_read_fd) = self.app.tx_evt_q().read_fd() else {
            return Ok(());
        };
        // SAFETY: the queue retains its original read endpoint while FileMain
        // owns this independent duplicated descriptor.
        let signal_read = unsafe { BorrowedFd::borrow_raw(signal_read_fd) }
            .try_clone_to_owned()
            .map_err(|source| AttachError::SessionSignalDuplicate { source })?;
        engine.data_worker_id()?;
        let file = engine.file_main_mut().add(File::new(
            signal_read,
            "app-to-session queue signal".to_owned(),
            u64::from(app_session_input.slot()),
            FileFunctions {
                read: Some(schedule_app_session_input),
                ..FileFunctions::default()
            },
        ))?;
        self.readiness_file = Some(file);
        Ok(())
    }

    /// Cancels session queue readiness before the queue itself is released.
    pub fn remove_queue_readiness(&mut self, engine: &mut Engine) -> RuntimeResult<()> {
        let Some(file) = self.readiness_file else {
            return Ok(());
        };
        let _ = engine.file_main_mut().delete(file)?;
        self.readiness_file = None;
        Ok(())
    }

    #[inline]
    pub fn session_transport(&self, session_id: SessionId) -> Option<(SessionTransportId, Index)> {
        let entry = self.entries.get(session_id.pool_index())?;
        let SessionType::Transport { transport, state } = entry.session_type? else {
            return None;
        };
        Some((transport, state.transport_index()?))
    }

    #[inline]
    pub fn has_session(&self, session_id: SessionId) -> bool {
        self.entries.contains_key(session_id.pool_index())
    }

    #[inline]
    pub fn prefetch_session(&self, session_id: SessionId) {
        self.entries.prefetch_slot(session_id.pool_index());
    }

    pub fn stream_accept(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        listener: SessionListenerId,
    ) -> RuntimeResult<SessionId> {
        let main = self
            .listener_main
            .as_ref()
            .cloned()
            .ok_or(SessionError::ListenerMainMissing)?;
        let application_listener =
            main.with_listener(listener, SessionListener::application_listener)?;
        main.applications
            .with_listener(application_listener, |policy| {
                self.construct_stream_sessions(
                    transport,
                    index,
                    listener.raw(),
                    policy.application(),
                    AppSessionProtocolRole::Server,
                    policy.protocols(),
                    None,
                )
            })
            .map_err(|source| RuntimeError::subsystem("application", source))?
    }

    pub fn stream_connect(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        connection: SessionConnectionId,
    ) -> RuntimeResult<SessionId> {
        let application_connection = ApplicationConnectionId::from_raw(connection.raw());
        let applications = self
            .applications
            .as_ref()
            .cloned()
            .ok_or(SessionError::ApplicationMainMissingForConnection { connection })?;
        let session_id = applications
            .with_connection(application_connection, |policy| {
                self.construct_stream_sessions(
                    transport,
                    index,
                    application_connection.raw(),
                    policy.application(),
                    AppSessionProtocolRole::Client,
                    policy.protocols(),
                    policy.server_name(),
                )
            })
            .map_err(|source| RuntimeError::subsystem("application", source))??;
        if let Err(source) = applications.complete_connection(application_connection) {
            self.remove_session(session_id)?;
            return Err(RuntimeError::subsystem("application", source));
        }
        Ok(session_id)
    }

    fn construct_stream_sessions(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        allocation_owner: u64,
        application: ApplicationId,
        role: AppSessionProtocolRole,
        protocols: &[ApplicationProtocol],
        server_name: Option<&str>,
    ) -> RuntimeResult<SessionId> {
        let (rx_fifo, tx_fifo) = self.create_local_fifos()?;
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            transport, rx_fifo, tx_fifo,
        ))?;
        let mut session_ids = Vec::with_capacity(protocols.len() + 1);
        session_ids.push(session_id);
        for _ in protocols {
            let (rx_fifo, tx_fifo) = match self.create_local_fifos() {
                Ok(fifos) => fifos,
                Err(error) => {
                    self.rollback_stream_sessions(&session_ids, None);
                    return Err(error);
                }
            };
            let session = match self.insert_session_entry(SessionEntry::unbound(rx_fifo, tx_fifo)) {
                Ok(session_id) => session_id,
                Err(error) => {
                    self.rollback_stream_sessions(&session_ids, None);
                    return Err(error);
                }
            };
            session_ids.push(session);
        }
        let application_session_id = *session_ids
            .last()
            .expect("validated App Session policy creates one external Session");
        let application_session = match self.app.create_app_session(
            allocation_owner,
            Some(application),
            self.session_handle(application_session_id),
            self.app_session_config,
            Arc::clone(self.app.tx_evt_q()),
        ) {
            Ok(session) => session,
            Err(error) => {
                self.rollback_stream_sessions(&session_ids, None);
                return Err(error);
            }
        };
        {
            let entry = self
                .entries
                .get_mut(application_session_id.pool_index())
                .expect("new external Session remains installed during construction");
            entry.rx_fifo = Arc::clone(application_session.rx_fifo());
            entry.tx_fifo = Arc::clone(application_session.tx_fifo());
        }
        for (protocol, sessions) in protocols.iter().copied().zip(session_ids.windows(2)) {
            let session = sessions[0];
            let app_session = sessions[1];
            let connection = match protocol.entry().create(
                self.worker,
                self.worker_count,
                Some(application),
                role,
                protocol.id(),
                server_name,
                self.session_handle(session),
                self.session_handle(app_session),
            ) {
                Ok(connection) => connection,
                Err(error) => {
                    self.rollback_stream_sessions(&session_ids, Some(application_session.as_ref()));
                    return Err(error);
                }
            };
            let entry = self
                .entries
                .get_mut(session.pool_index())
                .expect("new Session remains installed during construction");
            entry.application = Some(SessionApplication::AppSessionProtocol {
                protocol: protocol.entry(),
                connection,
            });
            let entry = self
                .entries
                .get_mut(app_session.pool_index())
                .expect("new App Session remains installed during construction");
            entry.session_type = Some(SessionType::AppSessionProtocol {
                protocol: protocol.entry(),
                connection,
            });
        }
        self.entries
            .get_mut(application_session_id.pool_index())
            .expect("new external Session remains installed during construction")
            .application = Some(SessionApplication::External(application));
        self.finish_transport_creation(session_id, index);
        self.app
            .attach_session(application_session_id, application_session);
        Ok(session_id)
    }

    fn create_local_fifos(&self) -> RuntimeResult<(Arc<Fifo>, Arc<Fifo>)> {
        let fifo_bytes = align_up(
            Fifo::layout_bytes(self.app_session_config.fifo_capacity).map_err(|_| {
                AppSessionError::RxFifoCapacityInvalid {
                    capacity: self.app_session_config.fifo_capacity,
                }
            })?,
            64,
        );
        let lower_segment_bytes = fifo_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(128))
            .ok_or(AppWorkerError::SessionSegmentSizeOverflow)?;
        let segment = Segment::local(lower_segment_bytes);
        let mut rx_fifo = Fifo::new(segment.clone(), self.app_session_config.fifo_capacity)
            .map_err(|_| AppSessionError::RxFifoCapacityInvalid {
                capacity: self.app_session_config.fifo_capacity,
            })?;
        rx_fifo.enable_ooo();
        let tx_fifo = Fifo::new(segment, self.app_session_config.fifo_capacity).map_err(|_| {
            AppSessionError::TxFifoCapacityInvalid {
                capacity: self.app_session_config.fifo_capacity,
            }
        })?;
        Ok((Arc::new(rx_fifo), Arc::new(tx_fifo)))
    }

    fn rollback_stream_sessions(
        &mut self,
        session_ids: &[SessionId],
        application_session: Option<&AppSession>,
    ) {
        if let Some(session) = application_session {
            self.app.discard_app_session(session);
        }
        for session_id in session_ids.iter().rev().copied() {
            let Some(entry) = self.entries.remove(session_id.pool_index()) else {
                continue;
            };
            if let Some(SessionType::AppSessionProtocol {
                protocol,
                connection,
            }) = entry.session_type
            {
                protocol.destroy(self.worker, connection);
            }
        }
    }

    fn insert_session_entry(&mut self, entry: SessionEntry<Index>) -> RuntimeResult<SessionId> {
        self.entries
            .insert(entry)
            .map(SessionId::from)
            .ok_or_else(|| {
                SessionError::CapacityExhausted {
                    capacity: self.entries.capacity(),
                }
                .into()
            })
    }

    #[inline]
    fn session_handle(&self, session_id: SessionId) -> SessionHandle {
        SessionHandle::new(session_id.pool_index().slot(), self.worker.slot() as u32)
    }

    #[inline]
    fn session_from_handle(&self, handle: SessionHandle) -> Option<SessionId> {
        (handle.worker_index() == self.worker.slot() as u32)
            .then(|| self.entries.index_at_slot(handle.session_index()))
            .flatten()
            .map(SessionId::from)
    }

    fn protocol_sessions(
        &self,
        protocol: AppSessionProtocolEntry,
        connection: AppSessionProtocolConnectionId,
    ) -> RuntimeResult<Option<(SessionId, SessionId)>> {
        let (session_handle, app_session_handle) = protocol.sessions(self.worker, connection)?;
        Ok(self
            .session_from_handle(session_handle)
            .zip(self.session_from_handle(app_session_handle)))
    }

    fn finish_transport_creation(&mut self, session_id: SessionId, index: Index) {
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .expect("new Session entry remains installed until creation completes");
        let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
            panic!("new transport Session retains its session type");
        };
        *state = state
            .finish_creation(index)
            .expect("new Session entry remains in Creating state until creation completes");
    }

    pub fn connection_published(&mut self, session_id: SessionId) -> RuntimeResult<bool> {
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .ok_or(SessionError::SessionMissing { session_id })?;
        let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
            return Err(SessionError::SessionMissing { session_id }.into());
        };
        let (next, initial) = state
            .on_connection_published()
            .ok_or(SessionError::PublicationRejected { session_id })?;
        *state = next;
        Ok(initial)
    }

    pub fn rollback_session_creation(
        &mut self,
        session_id: SessionId,
    ) -> RuntimeResult<Option<Index>> {
        let entry = self
            .entries
            .get(session_id.pool_index())
            .ok_or(SessionError::SessionMissing { session_id })?;
        let Some(SessionType::Transport { state, .. }) = entry.session_type else {
            return Err(SessionError::SessionMissing { session_id }.into());
        };
        let index = state
            .rollback_index()
            .map_err(|_| SessionError::RollbackRejected { session_id })?;
        self.remove_session(session_id)?;
        Ok(index)
    }

    pub fn insert_session_for_test(
        &mut self,
        transport: SessionTransportId,
        index: Index,
    ) -> SessionId {
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach test Application");
        let policy = hammer_runtime::app::AppSessionPolicy::new(
            hammer_runtime::app::APP_SESSION_POLICY_VERSION,
            [],
        )
        .expect("direct App Session policy is valid");
        let application_listener = applications
            .register_listener(application, &policy)
            .expect("register test Application listener");
        let main = Arc::new(SessionMain::new(self.worker_count, applications));
        self.set_listener_main(Arc::clone(&main));
        let listener = main
            .listen(
                application_listener,
                SessionTransportRegistration::new(
                    "test-session",
                    Some(|_, _| Ok(())),
                    Some(|_| Ok(())),
                    None,
                ),
                SessionListenEndpoint::new(
                    "127.0.0.1:0".parse().expect("test endpoint"),
                    self.worker,
                ),
            )
            .expect("register test Session listener");
        let session_id = self
            .stream_accept(transport, index, listener)
            .expect("accept test session");
        self.connection_published(session_id)
            .expect("publish session connection");
        self.connected(session_id).expect("connect session");
        session_id
    }

    fn application_detached(&mut self, application: ApplicationId) {
        let sessions = self
            .entries
            .iter()
            .filter_map(|(index, entry)| {
                matches!(
                    entry.application,
                    Some(SessionApplication::External(owner)) if owner == application
                )
                .then_some(SessionId::from(index))
            })
            .collect::<Vec<_>>();
        for session_id in sessions {
            self.control_events
                .push_back(SessionControlEvent::Close(session_id));
        }
    }

    pub fn notify_transport_closed(
        &mut self,
        session_id: SessionId,
        index: Index,
    ) -> RuntimeResult<()> {
        let notify_application =
            self.entries
                .get_mut(session_id.pool_index())
                .is_some_and(|entry| {
                    let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut()
                    else {
                        return false;
                    };
                    state.on_transport_close(index)
                });
        if notify_application {
            self.notify_application_closed(session_id)?;
        }
        Ok(())
    }

    pub fn notify_transport_deleted(
        &mut self,
        session_id: SessionId,
        index: Index,
    ) -> RuntimeResult<()> {
        let remove = self
            .entries
            .get_mut(session_id.pool_index())
            .is_some_and(|entry| {
                let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
                    return false;
                };
                state.on_transport_deleted(index)
            });
        if remove {
            self.remove_session(session_id)?;
        }
        Ok(())
    }

    fn close_transport_session(&mut self, session_id: SessionId) -> RuntimeResult<bool> {
        let (remove, disconnect) = self
            .entries
            .get_mut(session_id.pool_index())
            .map(|entry| {
                let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
                    return (false, false);
                };
                let disconnect = matches!(
                    state,
                    SessionState::Created(_)
                        | SessionState::Published(_)
                        | SessionState::Active(_)
                        | SessionState::TransportClosed(_)
                );
                (state.on_app_close(), disconnect)
            })
            .unwrap_or((false, false));
        if remove {
            self.remove_session(session_id)?;
        }
        Ok(disconnect)
    }

    fn remove_session(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        let Some(entry) = self.entries.remove(session_id.pool_index()) else {
            return Ok(());
        };
        if matches!(entry.application, Some(SessionApplication::External(_))) {
            drop(self.app.detach_session(session_id));
        }
        let Some(SessionApplication::AppSessionProtocol {
            protocol,
            connection,
        }) = entry.application
        else {
            return Ok(());
        };
        let (_, app_session_handle) = protocol.sessions(self.worker, connection)?;
        let app_session = self.session_from_handle(app_session_handle);
        protocol.destroy(self.worker, connection);
        if let Some(app_session) = app_session {
            self.remove_session(app_session)?;
        }
        Ok(())
    }

    fn notify_application_closed(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        let application = self
            .entries
            .get(session_id.pool_index())
            .and_then(|entry| entry.application);
        match application {
            Some(SessionApplication::AppSessionProtocol {
                protocol,
                connection,
            }) => {
                let Some((_, app_session)) = self.protocol_sessions(protocol, connection)? else {
                    return Ok(());
                };
                self.control_events
                    .push_back(SessionControlEvent::TransportClosed(app_session));
                Ok(())
            }
            Some(SessionApplication::External(_)) => self.app.closed(session_id),
            None => Ok(()),
        }
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: SessionId) {
        let Some(entry) = self.entries.get_mut(session_id.pool_index()) else {
            return;
        };
        if entry.schedule_pending {
            return;
        }
        entry.schedule_pending = true;
        self.session_work.push(session_id);
    }

    #[inline]
    pub fn schedule_disconnect(&mut self, session_id: SessionId) {
        self.control_events
            .push_back(SessionControlEvent::Close(session_id));
    }

    pub fn poll_app(&mut self) -> RuntimeResult<usize> {
        let session_evt_q = Arc::clone(&self.session_evt_q);
        let mut queue_error = None;
        let handled = self.app.drain_tx_events_to(|evt| {
            if queue_error.is_some() {
                return;
            }
            let result = match evt.evt_type {
                SessionEvtType::Connect | SessionEvtType::Close => session_evt_q.enqueue_ctrl(evt),
                SessionEvtType::RxEnq
                | SessionEvtType::RxDeq
                | SessionEvtType::TxEnq
                | SessionEvtType::TxDeq => session_evt_q.enqueue_io(evt),
                SessionEvtType::ProtocolOutput => return,
            };
            if let Err(error) = result {
                queue_error = Some(error);
            }
        });
        if let Some(error) = queue_error {
            return Err(AppWorkerError::SessionEventQueue { error }.into());
        }
        Ok(handled)
    }

    pub(crate) fn poll_session_events(&mut self) -> RuntimeResult<usize> {
        let mut batch = [SessionEvt::io(0, SessionEvtType::Connect); 64];
        let mut handled = 0usize;
        while handled < SESSION_QUEUE_IO_BUDGET {
            let remaining = SESSION_QUEUE_IO_BUDGET - handled;
            let batch_count = remaining.min(batch.len());
            let count = self.session_evt_q.dequeue_batch(&mut batch[..batch_count]);
            if count == 0 {
                return Ok(handled);
            }
            for event in &batch[..count] {
                self.dispatch_session_event(*event)?;
            }
            handled += count;
            if count < batch_count {
                return Ok(handled);
            }
        }
        Ok(handled)
    }

    fn dispatch_session_event(&mut self, event: SessionEvt) -> RuntimeResult<()> {
        let Some(index) = self.entries.index_at_slot(event.session_index()) else {
            return Ok(());
        };
        let session_id = SessionId::from(index);
        match event.evt_type {
            SessionEvtType::RxEnq | SessionEvtType::TxDeq | SessionEvtType::Connect => {
                self.dispatch_application(session_id, event.evt_type)
            }
            SessionEvtType::RxDeq | SessionEvtType::TxEnq | SessionEvtType::ProtocolOutput => {
                self.dispatch_session_type(session_id, event.evt_type)
            }
            SessionEvtType::Close => {
                self.control_events
                    .push_back(SessionControlEvent::Close(session_id));
                Ok(())
            }
        }
    }

    fn dispatch_application(
        &mut self,
        session_id: SessionId,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let application = self
            .entries
            .get(session_id.pool_index())
            .and_then(|entry| entry.application);
        match application {
            Some(SessionApplication::AppSessionProtocol {
                protocol,
                connection,
            }) => match event {
                SessionEvtType::RxEnq => self.process_protocol_ingress(protocol, connection),
                SessionEvtType::TxDeq | SessionEvtType::Connect => {
                    self.process_protocol_egress(protocol, connection)
                }
                _ => Ok(()),
            },
            Some(SessionApplication::External(_)) => match event {
                SessionEvtType::RxEnq | SessionEvtType::TxDeq => {
                    let Some(session) = self.app.app_session(session_id) else {
                        return Ok(());
                    };
                    session.push_event(event).map_err(RuntimeError::from)
                }
                SessionEvtType::Connect => self.app.connected(session_id),
                _ => Ok(()),
            },
            None => Ok(()),
        }
    }

    fn dispatch_session_type(
        &mut self,
        session_id: SessionId,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let session_type = self
            .entries
            .get(session_id.pool_index())
            .and_then(|entry| entry.session_type);
        match session_type {
            Some(SessionType::Transport { .. }) => match event {
                SessionEvtType::RxDeq => {
                    self.app_rx_events.push_back(session_id);
                    Ok(())
                }
                SessionEvtType::TxEnq | SessionEvtType::ProtocolOutput => {
                    if let Some(entry) = self.entries.get(session_id.pool_index()) {
                        entry.tx_fifo.unset_event();
                    }
                    self.mark_ready(session_id);
                    Ok(())
                }
                _ => Ok(()),
            },
            Some(SessionType::AppSessionProtocol {
                protocol,
                connection,
            }) => match event {
                SessionEvtType::RxDeq => self.process_protocol_ingress(protocol, connection),
                SessionEvtType::TxEnq | SessionEvtType::ProtocolOutput => {
                    self.process_protocol_egress(protocol, connection)
                }
                _ => Ok(()),
            },
            None => Ok(()),
        }
    }

    fn process_protocol_ingress(
        &mut self,
        protocol: AppSessionProtocolEntry,
        connection: AppSessionProtocolConnectionId,
    ) -> RuntimeResult<()> {
        let Some((session_id, app_session_id)) = self.protocol_sessions(protocol, connection)?
        else {
            return Ok(());
        };
        let mut progressed = false;
        let mut exhausted = true;
        for _ in 0..PROTOCOL_ADVANCE_BUDGET {
            let (consumed, produced) = {
                let Some(session) = self.entries.get(session_id.pool_index()) else {
                    return Ok(());
                };
                let Some(app_session) = self.entries.get(app_session_id.pool_index()) else {
                    return Ok(());
                };
                protocol.ingress(
                    self.worker,
                    connection,
                    session.rx_fifo.as_ref(),
                    app_session.rx_fifo.as_ref(),
                )?
            };
            self.publish_rx_dequeue(session_id, consumed)?;
            self.publish_rx_enqueue(app_session_id, produced)?;
            if consumed == 0 && produced == 0 {
                exhausted = false;
                break;
            }
            progressed = true;
        }
        if let Some(session) = self.entries.get(session_id.pool_index())
            && session.rx_fifo.max_dequeue() == 0
        {
            session.rx_fifo.unset_event();
        }
        if progressed {
            self.enqueue_session_event(SessionEvt::io(
                app_session_id.pool_index().slot(),
                SessionEvtType::ProtocolOutput,
            ))?;
        }
        if exhausted {
            self.enqueue_session_event(SessionEvt::io(
                session_id.pool_index().slot(),
                SessionEvtType::RxEnq,
            ))?;
        }
        self.publish_protocol_ready(protocol, connection, app_session_id)
    }

    fn process_protocol_egress(
        &mut self,
        protocol: AppSessionProtocolEntry,
        connection: AppSessionProtocolConnectionId,
    ) -> RuntimeResult<()> {
        let Some((session_id, app_session_id)) = self.protocol_sessions(protocol, connection)?
        else {
            return Ok(());
        };
        let mut exhausted = true;
        for _ in 0..PROTOCOL_ADVANCE_BUDGET {
            let (consumed, produced) = {
                let Some(session) = self.entries.get(session_id.pool_index()) else {
                    return Ok(());
                };
                let Some(app_session) = self.entries.get(app_session_id.pool_index()) else {
                    return Ok(());
                };
                protocol.egress(
                    self.worker,
                    connection,
                    app_session.tx_fifo.as_ref(),
                    session.tx_fifo.as_ref(),
                )?
            };
            self.publish_tx_dequeue(app_session_id, consumed)?;
            self.publish_tx_enqueue(session_id, produced)?;
            if consumed == 0 && produced == 0 {
                exhausted = false;
                break;
            }
        }
        if let Some(app_session) = self.entries.get(app_session_id.pool_index())
            && app_session.tx_fifo.max_dequeue() == 0
        {
            app_session.tx_fifo.unset_event();
        }
        if exhausted {
            self.enqueue_session_event(SessionEvt::io(
                app_session_id.pool_index().slot(),
                SessionEvtType::ProtocolOutput,
            ))?;
        }
        self.publish_protocol_ready(protocol, connection, app_session_id)
    }

    fn publish_protocol_ready(
        &mut self,
        protocol: AppSessionProtocolEntry,
        connection: AppSessionProtocolConnectionId,
        app_session_id: SessionId,
    ) -> RuntimeResult<()> {
        if !protocol.claim_ready(self.worker, connection)? {
            return Ok(());
        }
        self.dispatch_application(app_session_id, SessionEvtType::Connect)
    }

    fn publish_rx_enqueue(&self, session_id: SessionId, produced: usize) -> RuntimeResult<()> {
        self.publish_rx_enqueue_with_flags(session_id, produced, SessionEvtFlags::empty())
    }

    fn publish_rx_enqueue_with_flags(
        &self,
        session_id: SessionId,
        produced: usize,
        flags: SessionEvtFlags,
    ) -> RuntimeResult<()> {
        if produced == 0 {
            return Ok(());
        }
        let urgent = flags.contains(SessionEvtFlags::URGENT);
        let notify = self
            .entries
            .get(session_id.pool_index())
            .is_some_and(|entry| entry.rx_fifo.set_event() || urgent);
        if !notify {
            return Ok(());
        }
        let external = self
            .entries
            .get(session_id.pool_index())
            .is_some_and(|entry| {
                matches!(entry.application, Some(SessionApplication::External(_)))
            });
        if external {
            if let Some(session) = self.app.app_session(session_id) {
                session
                    .push_event_with_flags(SessionEvtType::RxEnq, flags)
                    .map_err(RuntimeError::from)?;
            }
            return Ok(());
        }
        if let Err(error) = self.enqueue_session_event(SessionEvt::io_with_flags(
            session_id.pool_index().slot(),
            SessionEvtType::RxEnq,
            flags,
        )) {
            if let Some(entry) = self.entries.get(session_id.pool_index()) {
                entry.rx_fifo.unset_event();
            }
            return Err(error);
        }
        Ok(())
    }

    fn publish_rx_dequeue(&self, session_id: SessionId, consumed: usize) -> RuntimeResult<()> {
        let notify = self
            .entries
            .get(session_id.pool_index())
            .is_some_and(|entry| entry.rx_fifo.needs_deq_notification(consumed));
        if notify {
            self.enqueue_session_event(SessionEvt::io(
                session_id.pool_index().slot(),
                SessionEvtType::RxDeq,
            ))?;
        }
        Ok(())
    }

    fn publish_tx_enqueue(&self, session_id: SessionId, produced: usize) -> RuntimeResult<()> {
        if produced == 0 {
            return Ok(());
        }
        let notify = self
            .entries
            .get(session_id.pool_index())
            .is_some_and(|entry| entry.tx_fifo.set_event());
        if notify {
            if let Err(error) = self.enqueue_session_event(SessionEvt::io(
                session_id.pool_index().slot(),
                SessionEvtType::TxEnq,
            )) {
                if let Some(entry) = self.entries.get(session_id.pool_index()) {
                    entry.tx_fifo.unset_event();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn publish_tx_dequeue(&self, session_id: SessionId, consumed: usize) -> RuntimeResult<()> {
        let external = self
            .entries
            .get(session_id.pool_index())
            .is_some_and(|entry| {
                matches!(entry.application, Some(SessionApplication::External(_)))
            });
        if external {
            if let Some(session) = self.app.app_session(session_id) {
                session
                    .publish_tx_dequeue(consumed)
                    .map_err(RuntimeError::from)?;
            }
            return Ok(());
        }
        let notify = self
            .entries
            .get(session_id.pool_index())
            .is_some_and(|entry| entry.tx_fifo.needs_deq_notification(consumed));
        if notify {
            self.enqueue_session_event(SessionEvt::io(
                session_id.pool_index().slot(),
                SessionEvtType::TxDeq,
            ))?;
        }
        Ok(())
    }

    fn enqueue_session_event(&self, event: SessionEvt) -> RuntimeResult<()> {
        let result = match event.evt_type {
            SessionEvtType::Connect | SessionEvtType::Close => {
                self.session_evt_q.enqueue_ctrl(event)
            }
            SessionEvtType::RxEnq
            | SessionEvtType::RxDeq
            | SessionEvtType::TxEnq
            | SessionEvtType::TxDeq
            | SessionEvtType::ProtocolOutput => self.session_evt_q.enqueue_io(event),
        };
        result.map_err(|error| AppWorkerError::SessionEventQueue { error }.into())
    }

    pub fn take_scheduled_work(&mut self) -> Vec<SessionId> {
        let mut work = core::mem::take(&mut self.session_work_scratch);
        core::mem::swap(&mut self.session_work, &mut work);
        for session_id in work.as_slice() {
            if let Some(entry) = self.entries.get_mut(session_id.pool_index()) {
                entry.schedule_pending = false;
            }
        }
        work
    }

    pub fn keep_work_scratch(&mut self, mut work: Vec<SessionId>) {
        work.clear();
        self.session_work_scratch = work;
    }

    pub fn ack_tx_up_to(&mut self, session_id: SessionId, bytes: usize) -> RuntimeResult<()> {
        let Some(entry) = self.entries.get(session_id.pool_index()) else {
            return Ok(());
        };
        let dropped = entry.tx_fifo.dequeue_drop(bytes);
        self.publish_tx_dequeue(session_id, dropped)?;
        if dropped != 0 {
            self.mark_ready(session_id);
        }
        Ok(())
    }

    pub fn enqueue_rx(
        &mut self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        index: BufferIndex,
        offset: u32,
        urgent: bool,
    ) -> RuntimeResult<RxDelivery> {
        if offset == 0 {
            let (accepted, promoted) =
                self.copy_rx_from_buffer(session_id, buffers, index, urgent)?;
            let rx_available = self.rx_available_u32(session_id);
            let delivery = match NonZeroU32::new(accepted) {
                Some(accepted) => RxDelivery::InOrder {
                    accepted,
                    promoted,
                    rx_available,
                },
                None => RxDelivery::NotAccepted { rx_available },
            };
            if rx_available == 0 {
                self.request_rx_dequeue_notification(session_id);
            }
            return Ok(delivery);
        }
        let (accepted, newest) =
            self.copy_rx_from_buffer_ooo(session_id, buffers, index, offset)?;
        let rx_available = self.rx_available_u32(session_id);
        let delivery = match NonZeroU32::new(accepted) {
            Some(accepted) => {
                let (start, len) = newest.ok_or(SessionError::OooSpanMissing { session_id })?;
                let len =
                    NonZeroU32::new(len).ok_or(SessionError::OooSpanInvalid { session_id })?;
                RxDelivery::OutOfOrder {
                    accepted,
                    newest: OooSpan::new(start, len),
                    rx_available,
                }
            }
            None => RxDelivery::NotAccepted { rx_available },
        };
        if rx_available == 0 {
            self.request_rx_dequeue_notification(session_id);
        }
        Ok(delivery)
    }

    #[inline]
    pub fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        self.entries
            .get(session_id.pool_index())
            .map(|entry| entry.rx_fifo.max_enqueue())
    }

    #[inline]
    pub fn pending_send_len(&self, session_id: SessionId) -> RuntimeResult<Option<usize>> {
        Ok(self
            .entries
            .get(session_id.pool_index())
            .map(|entry| entry.tx_fifo.max_dequeue())
            .filter(|len| *len != 0))
    }

    #[inline]
    pub fn has_pending_send(&self, session_id: SessionId) -> bool {
        self.pending_send_len(session_id).ok().flatten().is_some()
    }

    pub fn connected(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        let next_state = {
            let entry = self
                .entries
                .get(session_id.pool_index())
                .ok_or(SessionError::SessionMissing { session_id })?;
            let Some(SessionType::Transport { state, .. }) = entry.session_type else {
                return Err(SessionError::SessionMissing { session_id }.into());
            };
            state
                .on_connected()
                .ok_or(SessionError::NotPublished { session_id })?
        };
        self.dispatch_application(session_id, SessionEvtType::Connect)?;
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .expect("validated transport Session remains installed during App publication");
        let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
            panic!("validated transport Session retains its type during App publication");
        };
        *state = next_state;
        Ok(())
    }

    pub fn copy_tx_to_buffer(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        offset: usize,
        len: usize,
        index: BufferIndex,
    ) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get(session_id.pool_index())
            .ok_or(SessionError::SessionMissing { session_id })?;
        let written = entry
            .tx_fifo
            .peek_segments(offset, len, |first, second| {
                if !first.is_empty() {
                    buffers.append(index, first)?;
                }
                if !second.is_empty() {
                    buffers.append(index, second)?;
                }
                Ok::<usize, RuntimeError>(first.len() + second.len())
            })
            .ok_or(SessionError::TxFifoRangeInvalid {
                session_id,
                tx_offset: offset,
                payload_len: len,
            })??;
        if written != len {
            return Err(SessionError::TxFifoRangeInvalid {
                session_id,
                tx_offset: offset,
                payload_len: len,
            }
            .into());
        }
        Ok(())
    }

    #[inline]
    fn rx_available_u32(&self, session_id: SessionId) -> u32 {
        self.rx_available_len(session_id)
            .map(|value| value.min(u32::MAX as usize) as u32)
            .unwrap_or(0)
    }

    #[inline]
    fn request_rx_dequeue_notification(&self, session_id: SessionId) {
        if let Some(entry) = self.entries.get(session_id.pool_index()) {
            entry.rx_fifo.want_deq_notification();
        }
    }

    fn copy_rx_from_buffer(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
        urgent: bool,
    ) -> RuntimeResult<(u32, u32)> {
        let Some(entry) = self.entries.get(session_id.pool_index()) else {
            return Ok((0, 0));
        };
        let mut total = 0u32;
        let mut accepted = 0u32;
        let mut promoted = 0u32;
        for buffer in buffers.chain(index) {
            let buffer = buffer?;
            let chunk = buffer.current();
            let chunk_len = u32::try_from(chunk.len())
                .map_err(|_| SessionError::RxLengthOverflow { session_id })?;
            if accepted == total {
                let rx_available_before = entry.rx_fifo.max_enqueue();
                if chunk.len() >= rx_available_before {
                    entry.rx_fifo.want_deq_notification();
                }
                let wrote = entry.rx_fifo.enqueue(chunk);
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
        self.publish_rx_enqueue_with_flags(
            session_id,
            accepted as usize + promoted as usize,
            if urgent {
                SessionEvtFlags::URGENT
            } else {
                SessionEvtFlags::empty()
            },
        )?;
        Ok((accepted, promoted))
    }

    fn copy_rx_from_buffer_ooo(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
        offset: u32,
    ) -> RuntimeResult<(u32, Option<(u32, u32)>)> {
        let Some(entry) = self.entries.get(session_id.pool_index()) else {
            return Ok((0, None));
        };
        let mut total_len = 0u32;
        let mut accepted = 0u32;
        let mut delivered = 0u32;
        let mut newest_start = None;
        let mut newest_end = None;
        for buffer in buffers.chain(index) {
            let buffer = buffer?;
            let current = buffer.current();
            let chunk_offset =
                offset
                    .checked_add(total_len)
                    .ok_or(SessionError::RxOutOfOrderOffsetOverflow {
                        session_id,
                        offset,
                        buffered_len: total_len,
                    })?;
            let result = entry
                .rx_fifo
                .enqueue_ooo(chunk_offset, current)
                .map_err(|source| SessionError::RxOutOfOrderEnqueue {
                    session_id,
                    offset: chunk_offset,
                    source,
                })?;
            accepted = accepted
                .checked_add(result.accepted)
                .ok_or(SessionError::RxLengthOverflow { session_id })?;
            delivered = delivered
                .checked_add(result.delivered)
                .ok_or(SessionError::RxLengthOverflow { session_id })?;
            if let Some(start) = result.start {
                let end = start
                    .checked_add(result.len)
                    .ok_or(SessionError::OooSpanInvalid { session_id })?;
                newest_start = Some(newest_start.map_or(start, |value: u32| value.min(start)));
                newest_end = Some(newest_end.map_or(end, |value: u32| value.max(end)));
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
        self.publish_rx_enqueue(session_id, delivered as usize)?;
        Ok((accepted, newest))
    }
}

impl<Index: Copy + Eq> SessionWorker<Index> {
    pub fn new(worker: DataWorkerId) -> RuntimeResult<Self> {
        Self::with_app_session_config(worker, AppSessionConfig::default())
    }

    /// Associates this Data Worker with the Session Main that owns listener
    /// routes before a transport may deliver accepted streams to it.
    pub fn set_listener_main(&mut self, main: Arc<SessionMain>) {
        self.listener_main = Some(main);
    }

    pub fn with_app_session_config(
        worker: DataWorkerId,
        app_session_config: AppSessionConfig,
    ) -> RuntimeResult<Self> {
        Self::with_worker_count_and_app_session_config(
            worker,
            worker.slot() + 1,
            app_session_config,
        )
    }

    pub(crate) fn with_worker_count_and_app_session_config(
        worker: DataWorkerId,
        worker_count: usize,
        app_session_config: AppSessionConfig,
    ) -> RuntimeResult<Self> {
        let cap = DEFAULT_SESSION_TX_EVENT_CAPACITY.next_power_of_two().max(2) as u32;
        let layout = SessionMsgQueue::layout_bytes(cap, cap.max(2))
            .map_err(|error| AppWorkerError::SessionEventQueue { error })?;
        let segment = Segment::local(
            layout
                .checked_add(128)
                .ok_or(AppWorkerError::TxEventQueueLayoutOverflow)?,
        );
        let offset = segment
            .alloc(layout, 64)
            .ok_or(AppWorkerError::TxEventQueueSegmentExhausted)?;
        let tx_evt_q = Arc::new(
            unsafe { SessionMsgQueue::init_at_with_signal(segment, offset, cap, cap.max(2)) }
                .map_err(|error| AppWorkerError::SessionEventQueue { error })?,
        );
        let session_evt_q = Arc::new(
            SessionMsgQueue::with_cfg(cap, cap.max(2))
                .map_err(|error| AppWorkerError::SessionEventQueue { error })?,
        );
        let app = AppWorker::new(DEFAULT_SESSION_POOL_CAPACITY, tx_evt_q, worker.slot());
        Ok(Self::from_app(
            worker,
            worker_count,
            app_session_config,
            None,
            app,
            session_evt_q,
        ))
    }

    pub(crate) fn with_app_session_attach(
        worker: DataWorkerId,
        worker_count: usize,
        app_session_config: AppSessionConfig,
        applications: Arc<ApplicationMain>,
        publisher: AppSessionPublisher,
    ) -> RuntimeResult<Self> {
        let cap = DEFAULT_SESSION_TX_EVENT_CAPACITY.next_power_of_two().max(2) as u32;
        let layout = SessionMsgQueue::layout_bytes(cap, cap.max(2))
            .map_err(|error| AppWorkerError::SessionEventQueue { error })?;
        let segment = Segment::shared(
            &format!("hammer-tx-event-w{}", worker.slot()),
            layout
                .checked_add(128)
                .ok_or(AppWorkerError::TxEventQueueLayoutOverflow)?,
        )
        .map_err(|source| RuntimeError::subsystem("session", source))?;
        let offset = segment
            .alloc(layout, 64)
            .ok_or(AppWorkerError::TxEventQueueSegmentExhausted)?;
        let tx_evt_q = Arc::new(
            unsafe {
                SessionMsgQueue::init_at_with_signal(segment.clone(), offset, cap, cap.max(2))
            }
            .map_err(|error| AppWorkerError::SessionEventQueue { error })?,
        );
        let app = AppWorker::with_attach(
            DEFAULT_SESSION_POOL_CAPACITY,
            tx_evt_q,
            worker.slot(),
            AppWorkerAttach::new(publisher, segment, offset),
        );
        let session_evt_q = Arc::new(
            SessionMsgQueue::with_cfg(cap, cap.max(2))
                .map_err(|error| AppWorkerError::SessionEventQueue { error })?,
        );
        Ok(Self::from_app(
            worker,
            worker_count,
            app_session_config,
            Some(applications),
            app,
            session_evt_q,
        ))
    }

    pub(crate) fn with_application_main(
        worker: DataWorkerId,
        worker_count: usize,
        app_session_config: AppSessionConfig,
        applications: Arc<ApplicationMain>,
    ) -> RuntimeResult<Self> {
        let mut worker = Self::with_worker_count_and_app_session_config(
            worker,
            worker_count,
            app_session_config,
        )?;
        worker.applications = Some(applications);
        Ok(worker)
    }

    fn from_app(
        worker: DataWorkerId,
        worker_count: usize,
        app_session_config: AppSessionConfig,
        applications: Option<Arc<ApplicationMain>>,
        app: AppWorker,
        session_evt_q: Arc<SessionMsgQueue>,
    ) -> Self {
        Self {
            worker,
            worker_count,
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            listener_main: None,
            applications,
            app,
            session_evt_q,
            app_session_config,
            transport_dispatches: Vec::new(),
            session_work: Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            session_work_scratch: Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            app_rx_events: FifoQueue::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            control_events: FifoQueue::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            readiness_file: None,
        }
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
    pub struct TransportSendFlags: u8 {
        const DESCHED = 1 << 0;
        const POSTPONE = 1 << 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportSendParams {
    pub snd_space: usize,
    pub tx_offset: usize,
    pub send_goal_size: usize,
    pub flags: TransportSendFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxBatchBuffer {
    pub index: BufferIndex,
    pub tx_offset: usize,
    pub payload_len: usize,
}

pub trait SessionTransport<Index>: Sized {
    type Tx: SessionTxStrategy<Self, Index>;

    const ID: SessionTransportId;

    /// Handles an application dequeue from the session-owned RX FIFO.
    ///
    /// Returning `true` asks Session Runtime to arm the next RX dequeue
    /// notification. The transport receives FIFO capacity facts but no FIFO
    /// or app scheduling authority.
    fn app_rx_evt(
        &mut self,
        _: Index,
        _: usize,
        _: usize,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut crate::session::node::SessionQueueOutput,
    ) -> RuntimeResult<bool> {
        Ok(false)
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;
}

pub trait SessionPacketizedTransport<Index>: SessionTransport<Index> {
    fn control_tx(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;

    fn send_params(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        pending_len: usize,
        now: Instant,
    ) -> RuntimeResult<TransportSendParams>;

    fn tx_action(
        &mut self,
        index: Index,
        batch: &[TxBatchBuffer],
        buffers: &DataPlaneBuffers,
        now: Instant,
    ) -> RuntimeResult<()>;
}

pub trait TransportInternalTransport<Index>: SessionTransport<Index> {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        session_id: SessionId,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;
}

pub trait SessionTxStrategy<T, Index>
where
    T: SessionTransport<Index>,
{
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        session_id: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;
}

pub struct SessionPacketizedTx;
pub struct TransportInternalTx;

impl<T, Index> SessionTxStrategy<T, Index> for SessionPacketizedTx
where
    T: SessionPacketizedTransport<Index>,
    Index: Copy + Eq,
{
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        session_id: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        transport.control_tx(sessions, index, runtime, output_next, frame, output, now)?;
        let Some(total_len) = sessions.pending_send_len(session_id)? else {
            return Ok(());
        };
        let params = transport.send_params(sessions, index, total_len, now)?;
        if params.tx_offset > total_len {
            return Err(SessionError::TxOffsetOutOfRange {
                session_id,
                tx_offset: params.tx_offset,
                available: total_len,
            }
            .into());
        }
        let mut batch_offset = params.tx_offset;
        let mut remaining_space = params.snd_space;
        let io_budget = output
            .remaining_io_budget()
            .min(DEFAULT_TX_DISPATCH_BUDGET)
            .min(frame.remaining_capacity());
        if batch_offset < total_len
            && remaining_space != 0
            && params.send_goal_size != 0
            && io_budget != 0
        {
            let mut batch = Vec::with_capacity(io_budget);
            while batch.len() < io_budget && remaining_space != 0 {
                let pending_len = total_len.saturating_sub(batch_offset);
                if pending_len == 0 {
                    break;
                }
                let payload_len = pending_len.min(remaining_space).min(params.send_goal_size);
                if payload_len == 0 {
                    break;
                }
                let buffer = runtime.buffers().alloc_index()?;
                sessions.copy_tx_to_buffer(
                    runtime.buffers(),
                    session_id,
                    batch_offset,
                    payload_len,
                    buffer,
                )?;
                batch.push(TxBatchBuffer {
                    index: buffer,
                    tx_offset: batch_offset,
                    payload_len,
                });
                batch_offset += payload_len;
                remaining_space -= payload_len;
            }
            transport.tx_action(index, batch.as_slice(), runtime.buffers(), now)?;
            for item in batch.as_slice() {
                if !output.try_enqueue_io(frame, output_next, item.index)? {
                    sessions.mark_ready(session_id);
                    break;
                }
            }
        }
        let pending_len = total_len.saturating_sub(batch_offset);
        let descheduled = params.flags.contains(TransportSendFlags::DESCHED)
            && !params.flags.contains(TransportSendFlags::POSTPONE);
        if pending_len != 0 && !(params.snd_space == 0 && descheduled) {
            sessions.mark_ready(session_id);
        }
        Ok(())
    }
}

impl<T, Index> SessionTxStrategy<T, Index> for TransportInternalTx
where
    T: TransportInternalTransport<Index>,
    Index: Copy + Eq,
{
    #[inline]
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        session_id: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        transport.internal_tx(
            sessions,
            session_id,
            index,
            runtime,
            output_next,
            frame,
            output,
            now,
        )
    }
}

pub fn dispatch_session_queue_once<T, Index>(
    runtime: &DataPlaneRuntime,
    owner: hammer_core::data_plane::NodeId,
    sessions: &mut SessionWorker<Index>,
    transport: &mut T,
    output_next: SessionQueueNext,
) -> RuntimeResult<SessionQueueStep>
where
    T: SessionTransport<Index>,
    Index: Copy + Eq,
{
    let now = Instant::now();
    sessions.poll_app()?;
    sessions.poll_session_events()?;
    let mut staging =
        BufferFrame::with_capacity(hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY);
    let mut output = crate::session::node::SessionQueueOutput::default();
    let step = dispatch_session_queue_pending(
        runtime,
        sessions,
        transport,
        output_next,
        &mut staging,
        &mut output,
        now,
    )?;
    runtime.with_current_node(owner, || output.flush(runtime, &mut staging));
    Ok(step)
}

pub fn dispatch_session_queue_pending<T, Index>(
    runtime: &DataPlaneRuntime,
    sessions: &mut SessionWorker<Index>,
    transport: &mut T,
    output_next: SessionQueueNext,
    frame: &mut BufferFrame,
    output: &mut crate::session::node::SessionQueueOutput,
    now: Instant,
) -> RuntimeResult<SessionQueueStep>
where
    T: SessionTransport<Index>,
    Index: Copy + Eq,
{
    transport.update_time(sessions, runtime, output_next, frame, output, now)?;
    let work = sessions.take_scheduled_work();
    let scheduled_sessions = work.len();

    let app_rx_event_count = sessions.app_rx_events.len();
    for _ in 0..app_rx_event_count {
        if output.remaining_io_budget() == 0 {
            break;
        }
        let Some(&session_id) = sessions.app_rx_events.front() else {
            break;
        };
        let Some((transport_id, index)) = sessions.session_transport(session_id) else {
            let _ = sessions.app_rx_events.pop_front();
            continue;
        };
        if transport_id != T::ID {
            let _ = sessions.app_rx_events.pop_front();
            continue;
        }
        let rx_available = sessions.rx_available_len(session_id).unwrap_or(0);
        let rx_capacity = sessions.app_session_config.fifo_capacity;
        let request_notification = transport.app_rx_evt(
            index,
            rx_available,
            rx_capacity,
            runtime,
            output_next,
            frame,
            output,
        )?;
        let _ = sessions.app_rx_events.pop_front();
        if request_notification {
            sessions.request_rx_dequeue_notification(session_id);
        }
    }

    let control_count = sessions.control_events.len();
    for _ in 0..control_count {
        let Some(event) = sessions.control_events.pop_front() else {
            break;
        };
        let session_id = match event {
            SessionControlEvent::Close(session_id) => {
                let session_type = sessions
                    .entries
                    .get(session_id.pool_index())
                    .and_then(|entry| entry.session_type);
                if let Some(SessionType::AppSessionProtocol {
                    protocol,
                    connection,
                }) = session_type
                {
                    let Some((session, _)) = sessions.protocol_sessions(protocol, connection)?
                    else {
                        continue;
                    };
                    sessions
                        .control_events
                        .push_back(SessionControlEvent::Close(session));
                    continue;
                }
                session_id
            }
            SessionControlEvent::TransportClosed(session_id) => {
                sessions.notify_application_closed(session_id)?;
                continue;
            }
        };
        let session_transport = sessions.session_transport(session_id);
        if !sessions.close_transport_session(session_id)? {
            continue;
        }
        if let Some((transport_id, index)) = session_transport
            && transport_id == T::ID
        {
            transport.disconnect(sessions, index, runtime, output_next, frame, output, now)?;
        }
    }

    for session_id in work.as_slice() {
        let Some((transport_id, index)) = sessions.session_transport(*session_id) else {
            continue;
        };
        if transport_id == T::ID {
            <T::Tx as SessionTxStrategy<T, Index>>::dispatch(
                transport,
                sessions,
                index,
                *session_id,
                runtime,
                output_next,
                frame,
                output,
                now,
            )?;
        }
    }
    sessions.keep_work_scratch(work);
    Ok(SessionQueueStep { scheduled_sessions })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Instant;

    use hammer_core::data_plane::BufferFrame;
    use hammer_infra::pool::Index;
    use hammer_runtime::app::AppSessionProtocolRole;
    use hammer_runtime::attach::AppServer;
    use hammer_runtime::{
        AttachError, DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, NodeRuntimeData,
        RuntimeError, RuntimeResult,
    };

    use super::{SessionMain, SessionQueueNext, SessionTransportId, SessionWorker};
    use crate::session::ApplicationMain;
    use crate::session::node::{SessionQueueNode, SessionQueueOutput};

    fn test_dispatch(
        _: &DataPlaneRuntime,
        _: NodeRuntimeData,
        _: SessionQueueNext,
        _: Instant,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    #[test]
    fn app_publication_queue_full_keeps_session_rollback_eligible() {
        let socket_path =
            std::path::PathBuf::from(format!("/tmp/hammer-sp-f-{}.sock", std::process::id()));
        let socket_path = socket_path.to_str().expect("socket path");
        let server = AppServer::bind(socket_path, 1).expect("bind App server");
        let applications = ApplicationMain::new(2);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::with_app_session_attach(
            DataWorkerId::new(1),
            2,
            hammer_runtime::app::AppSessionConfig::default(),
            applications,
            server.publisher(),
        )
        .expect("Session worker with external App publication");

        let first = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(1, 1),
                1,
                application,
                AppSessionProtocolRole::Server,
                &[],
                None,
            )
            .expect("first accepted Session");
        sessions
            .connection_published(first)
            .expect("publish first connection");
        sessions.connected(first).expect("fill publication queue");

        let second_index = Index::new(2, 1);
        let second = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                second_index,
                2,
                application,
                AppSessionProtocolRole::Server,
                &[],
                None,
            )
            .expect("second accepted Session");
        sessions
            .connection_published(second)
            .expect("publish second connection");
        let error = sessions
            .connected(second)
            .expect_err("full publication queue rejects connection establishment");
        assert!(matches!(
            error,
            RuntimeError::Attach(AttachError::PublicationQueueFull)
        ));

        assert_eq!(
            sessions
                .rollback_session_creation(second)
                .expect("publication failure remains rollback eligible"),
            Some(second_index)
        );
        assert!(!sessions.has_session(second));
    }

    #[test]
    fn closed_app_publication_queue_keeps_session_rollback_eligible() {
        let socket_path =
            std::path::PathBuf::from(format!("/tmp/hammer-sp-c-{}.sock", std::process::id()));
        let socket_path = socket_path.to_str().expect("socket path");
        let server = AppServer::bind(socket_path, 1).expect("bind App server");
        let publisher = server.publisher();
        drop(server);
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::with_app_session_attach(
            DataWorkerId::new(2),
            3,
            hammer_runtime::app::AppSessionConfig::default(),
            applications,
            publisher,
        )
        .expect("Session worker with closed external App publication");
        let connection_index = Index::new(3, 1);
        let session_id = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                connection_index,
                3,
                application,
                AppSessionProtocolRole::Server,
                &[],
                None,
            )
            .expect("accepted Session");
        sessions
            .connection_published(session_id)
            .expect("publish connection");

        let error = sessions
            .connected(session_id)
            .expect_err("closed publication queue rejects connection establishment");
        assert!(matches!(
            error,
            RuntimeError::Attach(AttachError::PublicationQueueClosed)
        ));
        assert_eq!(
            sessions
                .rollback_session_creation(session_id)
                .expect("publication failure remains rollback eligible"),
            Some(connection_index)
        );
        assert!(!sessions.has_session(session_id));
    }

    #[test]
    fn session_queue_dispatches_are_isolated_per_runtime_on_same_thread() {
        let worker = DataWorkerId::new(0);
        let runtime_a = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime A");
        let runtime_b = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime B");
        let main_a = Arc::new(SessionMain::new(1, ApplicationMain::new(1)));
        let main_b = Arc::new(SessionMain::new(1, ApplicationMain::new(1)));
        assert!(
            main_a
                .worker(worker)
                .expect("Session worker A slot")
                .install(SessionWorker::<Index>::new(worker).expect("Session worker A"))
                .is_ok(),
            "install Session worker A"
        );
        assert!(
            main_b
                .worker(worker)
                .expect("Session worker B slot")
                .install(SessionWorker::<Index>::new(worker).expect("Session worker B"))
                .is_ok(),
            "install Session worker B"
        );

        let data_a = NodeRuntimeData::from_usize(Arc::as_ptr(&main_a) as usize)
            .expect("Session Main A data");
        let data_b = NodeRuntimeData::from_usize(Arc::as_ptr(&main_b) as usize)
            .expect("Session Main B data");
        SessionQueueNode::install_worker_attachment(
            &runtime_a,
            data_a,
            SessionQueueNext::from_slot(0),
            test_dispatch,
        )
        .expect("install dispatch A");
        SessionQueueNode::install_worker_attachment(
            &runtime_a,
            data_a,
            SessionQueueNext::from_slot(1),
            test_dispatch,
        )
        .expect("install second dispatch A");
        SessionQueueNode::install_worker_attachment(
            &runtime_b,
            data_b,
            SessionQueueNext::from_slot(2),
            test_dispatch,
        )
        .expect("install dispatch B");

        let dispatches_a = main_a
            .with_worker_mut(&runtime_a, |sessions| {
                Ok(sessions.transport_dispatches.len())
            })
            .expect("read dispatches A");
        let dispatches_b = main_b
            .with_worker_mut(&runtime_b, |sessions| {
                Ok(sessions.transport_dispatches.len())
            })
            .expect("read dispatches B");

        assert_eq!(dispatches_a, 2);
        assert_eq!(dispatches_b, 1);
    }

    #[test]
    fn duplicate_worker_installation_preserves_existing_session_queue_dispatches() {
        let worker = DataWorkerId::new(0);
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime");
        let main = Arc::new(SessionMain::new(1, ApplicationMain::new(1)));
        assert!(
            main.worker(worker)
                .expect("Session worker slot")
                .install(SessionWorker::<Index>::new(worker).expect("Session worker"))
                .is_ok(),
            "install Session worker"
        );
        let data =
            NodeRuntimeData::from_usize(Arc::as_ptr(&main) as usize).expect("Session Main data");
        SessionQueueNode::install_worker_attachment(
            &runtime,
            data,
            SessionQueueNext::from_slot(0),
            test_dispatch,
        )
        .expect("install dispatch");

        let duplicate = SessionWorker::<Index>::new(worker).expect("duplicate Session worker");
        assert!(
            main.worker(worker)
                .expect("duplicate install slot")
                .install(duplicate)
                .is_err()
        );

        let dispatches = main
            .with_worker_mut(&runtime, |sessions| Ok(sessions.transport_dispatches.len()))
            .expect("read dispatches");
        assert_eq!(dispatches, 1);
    }
}

fn schedule_app_session_input(
    graph: &hammer_runtime::NodeRuntime,
    file: &mut File,
) -> RuntimeResult<()> {
    let node = hammer_core::data_plane::NodeId::new(file.private_data() as u32);
    let _ = graph.mark_interrupt_pending(node)?;
    Ok(())
}

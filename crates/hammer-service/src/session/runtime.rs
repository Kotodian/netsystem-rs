use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::os::fd::BorrowedFd;
use std::sync::Arc;
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use crossbeam_queue::ArrayQueue;
use hammer_core::data_plane::{
    BufferFrame, DataPlaneBuffers, Index as BufferIndex, NodeId, NodeState,
};
use hammer_infra::align::{CacheLine, align_up};
use hammer_infra::fifo::Fifo;
use hammer_infra::linked_list::LinkedList;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::segment::Segment;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{
    AppSession, AppSessionConfig, AppSessionError, ApplicationConnectionId, ApplicationId,
    ApplicationListenerId, SessionAppContext, SessionAppId, SessionDgramHeader, SessionEventQueue,
    SessionEvt, SessionEvtFlags, SessionEvtType, SessionHandle, SessionMqRing, SessionMsgQueue,
};
use hammer_runtime::attach::AppSessionPublisher;
use hammer_runtime::{
    AttachError, RuntimeError, RuntimeResult, SessionConnectEndpoint, SessionConnectionId,
    SessionListenEndpoint, SessionListenerId, SessionTransportRegistration,
};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Deadline, Engine, File, FileFunctions, NodeRuntime,
    NodeRuntimeData,
};

use crate::session::app::AppWorkerError;
use crate::session::application::{ApplicationMain, ApplicationMqResources};
use crate::session::error::{SessionError, SessionQueueError};
use crate::session::lookup::{SessionEndpointLookup, SessionEndpointState};
use crate::session::node::{AppSessionInputNode, SessionQueueTransportDispatch};
use crate::session::protocol::SessionAppCallbacks;
use crate::session::state::SessionState;
use crate::session::{AppWorker, SessionId, SessionQueueNext};

const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;
const DEFAULT_SESSION_EVENT_CAPACITY: usize = 2048;
const DEFAULT_TX_DISPATCH_BUDGET: usize = 64;
const SESSION_WORKER_INTERRUPT_DEADLINE: Duration = Duration::from_millis(1);
const SESSION_WORKER_IDLE_DEADLINE: Duration = Duration::from_millis(100);

/// VPP's adaptive Session Worker execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionWorkerState {
    Polling,
    Interrupt,
    Idle,
}

impl SessionWorkerState {
    #[inline]
    const fn deadline(self) -> Option<Duration> {
        match self {
            Self::Polling => None,
            Self::Interrupt => Some(SESSION_WORKER_INTERRUPT_DEADLINE),
            Self::Idle => Some(SESSION_WORKER_IDLE_DEADLINE),
        }
    }

    #[inline]
    const fn node_state(self) -> NodeState {
        match self {
            Self::Polling => NodeState::Polling,
            Self::Interrupt | Self::Idle => NodeState::Interrupt,
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionTransportId(u8);

impl SessionTransportId {
    #[inline]
    pub const fn new(value: u8) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn raw(self) -> u8 {
        self.0
    }
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

const DEFAULT_SESSION_MIGRATE_QUEUE_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionDgramArgs {
    pub index: BufferIndex,
    pub payload_offset: usize,
    pub payload_len: usize,
    pub urgent: bool,
    pub return_node: NodeId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMigrateResult {
    Queued,
    Handoff,
    Busy,
    QueueFull,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionSwitchPoolArgs {
    pub old_sh: SessionHandle,
    pub new_sh: Option<SessionHandle>,
    pub old_thread: DataWorkerId,
    pub new_thread: DataWorkerId,
    pub transport: SessionTransportId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub dgram: SessionDgramArgs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSwitchPoolStatus {
    Rejected,
    Prepared,
}

pub struct SessionMigrationState {
    transport: SessionTransportId,
    rx_fifo: Arc<Fifo>,
    tx_fifo: Arc<Fifo>,
}

pub struct SessionSwitchPoolReply {
    pub old_sh: SessionHandle,
    pub new_sh: Option<SessionHandle>,
    pub old_thread: DataWorkerId,
    pub new_thread: DataWorkerId,
    pub transport: SessionTransportId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub status: SessionSwitchPoolStatus,
    pub state: Option<SessionMigrationState>,
    pub dgram: SessionDgramArgs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSwitchPoolCompletionStatus {
    Accepted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionSwitchPoolCompletion {
    pub old_sh: SessionHandle,
    pub new_sh: SessionHandle,
    pub old_thread: DataWorkerId,
    pub new_thread: DataWorkerId,
    pub transport: SessionTransportId,
    pub local: SocketAddr,
    pub remote: SocketAddr,
    pub status: SessionSwitchPoolCompletionStatus,
}

struct SessionMigrateQueues {
    session_migrate_requests: Box<[ArrayQueue<SessionSwitchPoolArgs>]>,
    session_switch_pool_replies: Box<[ArrayQueue<SessionSwitchPoolReply>]>,
    session_switch_pool_completions: Box<[ArrayQueue<SessionSwitchPoolCompletion>]>,
}

impl SessionMigrateQueues {
    fn new(worker_count: usize) -> Self {
        Self {
            session_migrate_requests: (0..worker_count)
                .map(|_| ArrayQueue::new(DEFAULT_SESSION_MIGRATE_QUEUE_CAPACITY))
                .collect(),
            session_switch_pool_replies: (0..worker_count)
                .map(|_| ArrayQueue::new(DEFAULT_SESSION_MIGRATE_QUEUE_CAPACITY))
                .collect(),
            session_switch_pool_completions: (0..worker_count)
                .map(|_| ArrayQueue::new(DEFAULT_SESSION_MIGRATE_QUEUE_CAPACITY))
                .collect(),
        }
    }

    #[inline]
    fn push_session_migrate_request(
        &self,
        args: SessionSwitchPoolArgs,
    ) -> Result<(), SessionSwitchPoolArgs> {
        let Some(queue) = self.session_migrate_requests.get(args.old_thread.slot()) else {
            return Err(args);
        };
        queue.push(args)
    }

    #[inline]
    fn pop_session_migrate_request(&self, worker: DataWorkerId) -> Option<SessionSwitchPoolArgs> {
        self.session_migrate_requests
            .get(worker.slot())
            .and_then(ArrayQueue::pop)
    }

    #[inline]
    fn push_session_switch_pool_reply(
        &self,
        reply: SessionSwitchPoolReply,
    ) -> Result<(), SessionSwitchPoolReply> {
        let Some(queue) = self
            .session_switch_pool_replies
            .get(reply.new_thread.slot())
        else {
            return Err(reply);
        };
        queue.push(reply)
    }

    #[inline]
    fn pop_session_switch_pool_reply(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolReply> {
        self.session_switch_pool_replies
            .get(worker.slot())
            .and_then(ArrayQueue::pop)
    }

    #[inline]
    fn push_session_switch_pool_completion(
        &self,
        completion: SessionSwitchPoolCompletion,
    ) -> Result<(), SessionSwitchPoolCompletion> {
        let Some(queue) = self
            .session_switch_pool_completions
            .get(completion.old_thread.slot())
        else {
            return Err(completion);
        };
        queue.push(completion)
    }

    #[inline]
    fn pop_session_switch_pool_completion(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolCompletion> {
        self.session_switch_pool_completions
            .get(worker.slot())
            .and_then(ArrayQueue::pop)
    }
}

#[derive(Clone, Copy)]
enum SessionType<Index> {
    Transport {
        transport: SessionTransportId,
        state: SessionState<Index>,
    },
}

#[derive(Clone, Copy)]
enum SessionApplication {
    External(ApplicationId),
}

struct SessionEntry<Index> {
    session_type: Option<SessionType<Index>>,
    application: Option<SessionApplication>,
    owner_application: Option<ApplicationId>,
    app: Option<SessionAppId>,
    app_session: SessionAppContext,
    app_opaque: Option<u64>,
    server_name: Option<String>,
    accepted: bool,
    lower_session: Option<SessionId>,
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
            owner_application: None,
            app: None,
            app_session: 0,
            app_opaque: None,
            server_name: None,
            accepted: false,
            lower_session: None,
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
            owner_application: None,
            app: None,
            app_session: 0,
            app_opaque: None,
            server_name: None,
            accepted: false,
            lower_session: None,
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
    applications: Arc<ApplicationMain>,
    app: AppWorker,
    session_evt_q: Arc<SessionMsgQueue>,
    app_session_config: AppSessionConfig,
    session_app_callbacks: Vec<Option<SessionAppCallbacks<Index>>>,
    pub(crate) transport_dispatches: Vec<SessionQueueTransportDispatch>,
    pub(crate) control_events: LinkedList<SessionEvt>,
    pub(crate) new_io_events: LinkedList<SessionEvt>,
    pub(crate) old_io_events: LinkedList<SessionEvt>,
    app_rx_mqs: Vec<Option<Box<AppRxMqEntry>>>,
    app_rx_mq_pending: VecDeque<ApplicationId>,
    state: SessionWorkerState,
    state_deadline_file: Option<PoolIndex>,
    session_queue: Option<NodeId>,
}

struct AppRxMqEntry {
    application: ApplicationId,
    queue: Arc<SessionMsgQueue>,
    file: Option<PoolIndex>,
    appsl_input_node: hammer_core::data_plane::NodeId,
    pending_queue: usize,
    pending: bool,
    postponed: bool,
}

impl AppRxMqEntry {
    fn drain_snapshot_to(
        &mut self,
        dispatch_event: &mut impl FnMut(SessionMqRing, SessionEvt),
    ) -> usize {
        let was_postponed = self.postponed;
        if !was_postponed {
            self.queue.drain();
        }
        self.postponed = false;

        let snapshot = self.queue.len();
        let dispatched = (0..snapshot)
            .map_while(|_| self.queue.dequeue_with_ring())
            .map(|(ring, event)| {
                dispatch_event(ring, event);
            })
            .count();

        let mut has_work = !self.queue.is_empty();
        if was_postponed && !has_work {
            self.queue.drain();
            has_work = !self.queue.is_empty();
        }
        self.pending = has_work;
        self.postponed = has_work;
        dispatched
    }
}

pub struct SessionMain {
    workers: Box<[CacheLine<ThreadOwned<SessionWorker<PoolIndex>>>]>,
    owner: ThreadId,
    listeners: UnsafeCell<Pool<SessionListener>>,
    endpoint_lookup: SessionEndpointLookup,
    session_switch_pool_queues: Arc<SessionMigrateQueues>,
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

#[inline]
fn session_listener_id(index: PoolIndex) -> SessionListenerId {
    SessionListenerId::new(index.slot(), index.generation())
}

#[inline]
fn session_listener_index(listener: SessionListenerId) -> PoolIndex {
    PoolIndex::new(listener.slot(), listener.generation())
}

impl SessionMain {
    pub fn applications(&self) -> &ApplicationMain {
        &self.applications
    }

    pub fn new(worker_count: usize, applications: Arc<ApplicationMain>) -> Self {
        Self::with_pool_capacity(worker_count, applications, DEFAULT_SESSION_POOL_CAPACITY)
    }

    pub fn with_pool_capacity(
        worker_count: usize,
        applications: Arc<ApplicationMain>,
        pool_capacity: usize,
    ) -> Self {
        let workers = (0..worker_count)
            .map(|_| CacheLine::new(ThreadOwned::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            workers,
            owner: thread::current().id(),
            listeners: UnsafeCell::new(Pool::with_capacity(pool_capacity)),
            endpoint_lookup: SessionEndpointLookup::new(),
            session_switch_pool_queues: Arc::new(SessionMigrateQueues::new(worker_count)),
            applications,
        }
    }

    pub(crate) fn insert_endpoint(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: SessionTransportId,
        handle: SessionHandle,
    ) -> bool {
        self.endpoint_lookup
            .insert(local, remote, transport, handle)
    }

    pub(crate) fn mark_endpoint_ready(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: SessionTransportId,
        handle: SessionHandle,
    ) -> bool {
        self.endpoint_lookup
            .mark_ready(local, remote, transport, handle)
    }

    pub(crate) fn remove_endpoint_if_current(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: SessionTransportId,
        handle: SessionHandle,
    ) -> bool {
        self.endpoint_lookup
            .remove_if_current(local, remote, transport, handle)
    }

    pub(crate) fn replace_endpoint_if_current(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: SessionTransportId,
        old_handle: SessionHandle,
        new_handle: SessionHandle,
    ) -> bool {
        self.endpoint_lookup
            .replace_if_current(local, remote, transport, old_handle, new_handle)
    }

    pub(crate) fn publish_migration_endpoint(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: SessionTransportId,
        old_handle: SessionHandle,
        new_handle: SessionHandle,
    ) -> bool {
        self.endpoint_lookup
            .publish_migration(local, remote, transport, old_handle, new_handle)
    }

    pub fn lookup_endpoint(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: SessionTransportId,
    ) -> Option<SessionHandle> {
        self.endpoint_lookup.lookup(local, remote, transport)
    }

    pub fn endpoint_is_migrating(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: SessionTransportId,
    ) -> bool {
        self.endpoint_lookup.state(local, remote, transport)
            == Some(SessionEndpointState::Migrating)
    }

    pub fn program_thread_migration(
        &self,
        runtime: &DataPlaneRuntime,
        target_worker: DataWorkerId,
        old_handle: SessionHandle,
        transport: SessionTransportId,
        local: SocketAddr,
        remote: SocketAddr,
        dgram: SessionDgramArgs,
    ) -> SessionMigrateResult {
        let source_worker = DataWorkerId::new(old_handle.worker_index());
        if source_worker == target_worker
            || source_worker.slot() >= self.workers.len()
            || target_worker.slot() >= self.workers.len()
        {
            return SessionMigrateResult::Unavailable;
        }

        if !self
            .endpoint_lookup
            .claim_migration(local, remote, transport, old_handle)
        {
            return match self.endpoint_lookup.state(local, remote, transport) {
                Some(crate::session::lookup::SessionEndpointState::Ready)
                    if self.endpoint_lookup.lookup(local, remote, transport)
                        == Some(old_handle) =>
                {
                    SessionMigrateResult::Handoff
                }
                Some(crate::session::lookup::SessionEndpointState::Migrating) => {
                    SessionMigrateResult::Busy
                }
                _ => SessionMigrateResult::Unavailable,
            };
        }

        let args = SessionSwitchPoolArgs {
            old_sh: old_handle,
            new_sh: None,
            old_thread: source_worker,
            new_thread: target_worker,
            transport,
            local,
            remote,
            dgram,
        };
        if self
            .session_switch_pool_queues
            .push_session_migrate_request(args)
            .is_err()
        {
            let _ = self
                .endpoint_lookup
                .cancel_migration(local, remote, transport, old_handle);
            return SessionMigrateResult::QueueFull;
        }
        self.wake_worker(runtime, source_worker);
        SessionMigrateResult::Queued
    }

    pub(crate) fn cancel_migration(
        &self,
        local: SocketAddr,
        remote: SocketAddr,
        transport: SessionTransportId,
        old_handle: SessionHandle,
    ) -> bool {
        self.endpoint_lookup
            .cancel_migration(local, remote, transport, old_handle)
    }

    fn wake_worker(&self, runtime: &DataPlaneRuntime, worker: DataWorkerId) {
        if let Some(session_queue) = runtime.node_by_name("session-queue") {
            runtime.set_worker_node_interrupt_pending(worker, session_queue);
        }
    }

    pub fn push_session_switch_pool_reply(
        &self,
        runtime: &DataPlaneRuntime,
        reply: SessionSwitchPoolReply,
    ) -> Result<(), SessionSwitchPoolReply> {
        let target_worker = reply.new_thread;
        let result = self
            .session_switch_pool_queues
            .push_session_switch_pool_reply(reply);
        if result.is_ok() {
            self.wake_worker(runtime, target_worker);
        }
        result
    }

    pub fn push_session_switch_pool_completion(
        &self,
        runtime: &DataPlaneRuntime,
        completion: SessionSwitchPoolCompletion,
    ) -> Result<(), SessionSwitchPoolCompletion> {
        let source_worker = completion.old_thread;
        let result = self
            .session_switch_pool_queues
            .push_session_switch_pool_completion(completion);
        if result.is_ok() {
            self.wake_worker(runtime, source_worker);
        }
        result
    }

    pub fn pop_session_migrate_request(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolArgs> {
        self.session_switch_pool_queues
            .pop_session_migrate_request(worker)
    }

    pub fn pop_session_switch_pool_reply(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolReply> {
        self.session_switch_pool_queues
            .pop_session_switch_pool_reply(worker)
    }

    pub fn pop_session_switch_pool_completion(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolCompletion> {
        self.session_switch_pool_queues
            .pop_session_switch_pool_completion(worker)
    }

    pub fn listen(
        &self,
        application_listener: ApplicationListenerId,
        transport: SessionTransportRegistration,
        endpoint: SessionListenEndpoint,
    ) -> RuntimeResult<SessionListenerId> {
        self.with_control_barrier(|| {
            let (application, opaque) = self
                .applications
                .with_listener(application_listener, |listener| {
                    (listener.application(), listener.opaque())
                })
                .map_err(RuntimeError::from)?;
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
            if let Err(error) = start_listen(listener, application, opaque, endpoint) {
                self.with_listeners_mut(|listeners| {
                    listeners
                        .remove(session_listener_index(listener))
                        .expect("new Session listener remains present until rollback");
                })?;
                return Err(error);
            }
            Ok(listener)
        })?
    }

    pub fn unlisten(&self, listener: SessionListenerId) -> RuntimeResult<()> {
        self.with_control_barrier(|| {
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
        })?
    }

    pub(super) fn with_control_barrier<R>(
        &self,
        operation: impl FnOnce() -> R,
    ) -> RuntimeResult<R> {
        if thread::current().id() != self.owner {
            return Err(SessionError::ListenerControlWrongThread.into());
        }
        match Engine::with_current(|engine| engine.worker_barrier()) {
            Some(barrier) if barrier.is_pending() => Ok(operation()),
            Some(barrier) => Ok(barrier.sync(operation)),
            None => Ok(operation()),
        }
    }

    pub fn connect(
        &self,
        transport: SessionTransportRegistration,
        endpoint: SessionConnectEndpoint,
    ) -> RuntimeResult<SessionConnectionId> {
        let Some(connect) = transport.connect() else {
            return Err(SessionError::TransportConnectUnsupported {
                transport: transport.name(),
            }
            .into());
        };
        let connection = endpoint.connection;
        connect(endpoint)?;
        Ok(connection)
    }

    /// Installs one worker-local Session App callback table by registered name.
    pub fn install_session_app(
        &self,
        runtime: &DataPlaneRuntime,
        name: &str,
        callbacks: &SessionAppCallbacks,
    ) -> RuntimeResult<()> {
        let app = self
            .applications
            .session_app_id(name)
            .map_err(RuntimeError::from)?;
        self.with_worker_mut(runtime, |sessions| {
            sessions.install_session_app(app, *callbacks)
        })
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
            Some(barrier) if barrier.is_pending() => operation(listeners),
            Some(barrier) => barrier.sync(|| operation(listeners)),
            None => operation(listeners),
        })
    }

    fn worker(
        &self,
        worker: DataWorkerId,
    ) -> RuntimeResult<&ThreadOwned<SessionWorker<PoolIndex>>> {
        Ok(self.workers.get(worker.slot()).map(|slot| &**slot).ok_or(
            SessionQueueError::WorkerOutOfRange {
                worker: worker.slot(),
            },
        )?)
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
            .ok_or(SessionQueueError::WorkerUnavailable { thread_index })?;
        self.worker(worker)?.with_mut(operation).map_err(|source| {
            SessionQueueError::WorkerAccess {
                worker: worker.slot(),
                source,
            }
        })?
    }

    pub(crate) fn session_queue_is_interrupt(
        &self,
        runtime: &DataPlaneRuntime,
    ) -> RuntimeResult<bool> {
        self.with_worker_mut(runtime, |sessions| {
            Ok(sessions.state == SessionWorkerState::Interrupt)
        })
    }

    pub(crate) fn application_detached(
        self: &Arc<Self>,
        engine: &Engine,
        application: ApplicationId,
    ) -> RuntimeResult<()> {
        self.remove_application_mqs(engine, application)?;
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
        listeners
            .into_iter()
            .try_for_each(|listener| self.unlisten(listener))?;
        (0..self.workers.len()).try_for_each(|worker_slot| -> RuntimeResult<()> {
            let worker = DataWorkerId::new(worker_slot as u32);
            loop {
                let main = Arc::clone(self);
                match engine.schedule_on_worker(worker, move || {
                    main.worker(worker)
                        .expect("scheduled Application detach targets an existing Session worker")
                        .with_mut(|sessions| {
                            sessions.application_detached(application);
                            Engine::with_current(|engine| {
                                sessions.wake_session_queue(&engine.runtime)
                            })
                            .ok_or(RuntimeError::WorkerControlRequiresMainEngine)??;
                            Ok::<(), RuntimeError>(())
                        })
                        .expect("scheduled Application detach runs on its Session worker")
                        .expect("Application detach operation failed on its Session worker");
                }) {
                    Ok(()) => break,
                    Err(RuntimeError::WorkerControlQueueFull { .. }) => {
                        std::thread::yield_now();
                    }
                    Err(
                        RuntimeError::WorkerControlUnavailable { .. }
                        | RuntimeError::WorkerControlClosed { .. },
                    ) => return Err(RuntimeError::WorkerControlClosed { worker }),
                    Err(error) => return Err(error),
                }
            }
            Ok(())
        })?;
        Ok(())
    }

    pub fn install_application_mqs(
        self: &Arc<Self>,
        engine: &Engine,
        application: ApplicationId,
        resources: &ApplicationMqResources,
    ) -> RuntimeResult<()> {
        if engine.thread_index != 0 {
            return Err(RuntimeError::WorkerControlRequiresMainEngine);
        }
        let app_session_input = engine
            .runtime
            .nodes()
            .node_by_name("appsl-rx-mqs-input")
            .ok_or(SessionQueueError::NodeMissing)?;
        (0..resources.worker_count()).try_for_each(|worker_slot| {
            let worker = DataWorkerId::new(worker_slot as u32);
            let queue = resources
                .queue(worker)
                .ok_or(SessionQueueError::ApplicationMqMissing { application })?
                .clone();
            let main = Arc::clone(self);
            schedule_worker_task(engine, worker, move || {
                Engine::with_current(|engine| {
                    let runtime = &mut engine.runtime;
                    main.worker(worker)?
                        .with_mut(|sessions| {
                            sessions.install_app_mq(application, queue, app_session_input, runtime)
                        })
                        .map_err(|source| SessionQueueError::WorkerAccess {
                            worker: worker.slot(),
                            source,
                        })?
                })
                .ok_or(RuntimeError::WorkerControlRequiresMainEngine)?
            })?;
            Ok::<(), RuntimeError>(())
        })?;
        Ok(())
    }

    pub(crate) fn remove_application_mqs(
        self: &Arc<Self>,
        engine: &Engine,
        application: ApplicationId,
    ) -> RuntimeResult<()> {
        (0..self.workers.len()).try_for_each(|worker_slot| {
            let worker = DataWorkerId::new(worker_slot as u32);
            let main = Arc::clone(self);
            schedule_worker_task(engine, worker, move || {
                Engine::with_current(|_| {
                    main.worker(worker)?
                        .with_mut(|sessions| {
                            sessions.drain_app_mq(application)?;
                            Ok(())
                        })
                        .map_err(|source| SessionQueueError::WorkerAccess {
                            worker: worker.slot(),
                            source,
                        })?
                })
                .ok_or(RuntimeError::WorkerControlRequiresMainEngine)?
            })?;
            Ok::<(), RuntimeError>(())
        })?;

        let mut first_error = None;
        (0..self.workers.len()).for_each(|worker_slot| {
            let worker = DataWorkerId::new(worker_slot as u32);
            let main = Arc::clone(self);
            if let Err(error) = schedule_worker_task(engine, worker, move || {
                Engine::with_current(|engine| {
                    let runtime = &mut engine.runtime;
                    main.worker(worker)?
                        .with_mut(|sessions| sessions.remove_app_mq(application, runtime))
                        .map_err(|source| SessionQueueError::WorkerAccess {
                            worker: worker.slot(),
                            source,
                        })?
                })
                .ok_or(RuntimeError::WorkerControlRequiresMainEngine)?
            }) {
                first_error.get_or_insert(error);
            }
        });
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(())
    }
}

fn schedule_worker_task<R: Send + 'static>(
    engine: &Engine,
    worker: DataWorkerId,
    task: impl FnOnce() -> RuntimeResult<R> + Send + 'static,
) -> RuntimeResult<R> {
    let (tx, rx) = std::sync::mpsc::channel();
    engine.schedule_on_worker(worker, move || {
        let _ = tx.send(task());
    })?;
    rx.recv()
        .map_err(|_| RuntimeError::WorkerControlClosed { worker })?
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
    let worker_id = worker.worker();
    let slot = main.worker(worker_id)?;
    let previous_app_session_input_data = engine
        .runtime
        .nodes()
        .node_runtime_data(app_session_input)?;
    let previous_session_queue_data = engine.runtime.nodes().node_runtime_data(session_queue)?;
    let previous_app_session_input_state = engine.runtime.nodes().node_state(app_session_input)?;
    let previous_session_queue_state = engine.runtime.nodes().node_state(session_queue)?;

    let setup = (|| -> RuntimeResult<()> {
        engine.set_worker_node_runtime_data(app_session_input, input_data)?;
        engine.set_worker_node_runtime_data(session_queue, session_queue_data)?;
        engine
            .runtime
            .nodes()
            .set_node_state(session_queue, NodeState::Polling)?;
        engine
            .runtime
            .nodes()
            .set_node_state(app_session_input, NodeState::Interrupt)?;
        worker.install_state_deadline(&engine.runtime, session_queue)
    })();
    if let Err(error) = setup {
        cleanup_session_worker_install(&mut worker, engine);
        rollback_session_worker_graph(
            engine,
            app_session_input,
            previous_app_session_input_data,
            previous_app_session_input_state,
            session_queue,
            previous_session_queue_data,
            previous_session_queue_state,
        );
        return Err(error);
    }

    if let Err(mut worker) = slot.install(worker) {
        cleanup_session_worker_install(&mut worker, engine);
        rollback_session_worker_graph(
            engine,
            app_session_input,
            previous_app_session_input_data,
            previous_app_session_input_state,
            session_queue,
            previous_session_queue_data,
            previous_session_queue_state,
        );
        return Err(SessionQueueError::WorkerAlreadyInstalled {
            worker: worker_id.slot(),
        }
        .into());
    }
    Ok(())
}

fn cleanup_session_worker_install(worker: &mut SessionWorker<PoolIndex>, engine: &mut Engine) {
    if let Err(error) = worker.remove_state_deadline(&engine.runtime) {
        tracing::error!(%error, "failed to remove Session Worker deadline during install rollback");
    }
}

fn rollback_session_worker_graph(
    engine: &mut Engine,
    app_session_input: hammer_core::data_plane::NodeId,
    previous_app_session_input_data: NodeRuntimeData,
    previous_app_session_input_state: NodeState,
    session_queue: NodeId,
    previous_session_queue_data: NodeRuntimeData,
    previous_session_queue_state: NodeState,
) {
    if let Err(error) = engine
        .runtime
        .nodes()
        .set_node_state(session_queue, previous_session_queue_state)
    {
        tracing::error!(
            %error,
            ?session_queue,
            "failed to restore Session Queue state during install rollback"
        );
    }
    if let Err(error) = engine
        .runtime
        .nodes()
        .set_node_state(app_session_input, previous_app_session_input_state)
    {
        tracing::error!(
            %error,
            ?app_session_input,
            "failed to restore appsl input state during install rollback"
        );
    }
    if let Err(error) =
        engine.set_worker_node_runtime_data(session_queue, previous_session_queue_data)
    {
        tracing::error!(
            %error,
            ?session_queue,
            "failed to restore Session Queue runtime data during install rollback"
        );
    }
    if let Err(error) =
        engine.set_worker_node_runtime_data(app_session_input, previous_app_session_input_data)
    {
        tracing::error!(
            %error,
            ?app_session_input,
            "failed to restore appsl input runtime data during install rollback"
        );
    }
}

impl<Index: Copy + Eq> SessionWorker<Index> {
    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub const fn worker_count(&self) -> usize {
        self.worker_count
    }

    /// Updates the opaque Session App context selected by a callback.
    pub fn set_app_session(
        &mut self,
        session_id: SessionId,
        context: SessionAppContext,
    ) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .ok_or(SessionError::SessionMissing { session_id })?;
        entry.app_session = context;
        Ok(())
    }

    #[inline]
    pub const fn state(&self) -> SessionWorkerState {
        self.state
    }

    #[inline]
    pub const fn state_deadline(&self) -> Option<Duration> {
        self.state.deadline()
    }

    /// Installs the worker's adaptive deadline after the Session Queue node is
    /// known. The deadline callback only wakes that node; Session Worker state
    /// remains owned by this worker and is updated after queue dispatch.
    pub(crate) fn install_state_deadline(
        &mut self,
        runtime: &DataPlaneRuntime,
        session_queue: NodeId,
    ) -> RuntimeResult<()> {
        if self.state_deadline_file.is_some() {
            return Ok(());
        }
        let index = runtime.file_main_mut().add_deadline(Deadline::new(
            "session queue adaptive deadline",
            u64::from(session_queue.slot()),
            schedule_session_queue_deadline,
        ))?;
        self.state_deadline_file = Some(index);
        self.session_queue = Some(session_queue);
        Ok(())
    }

    pub(crate) fn remove_state_deadline(
        &mut self,
        runtime: &DataPlaneRuntime,
    ) -> RuntimeResult<()> {
        let Some(index) = self.state_deadline_file else {
            return Ok(());
        };
        if !runtime.file_main_mut().delete_deadline(index)? {
            return Err(RuntimeError::DeadlineIndexInvalid { index });
        }
        self.state_deadline_file = None;
        self.session_queue = None;
        Ok(())
    }

    /// Applies the VPP adaptive state transition after one Session Queue
    /// dispatch. `last_vectors_per_loop` is the worker's graph-output proxy;
    /// the state machine itself does not own transport timers.
    pub fn update_state(
        &mut self,
        runtime: &DataPlaneRuntime,
        last_vectors_per_loop: usize,
    ) -> RuntimeResult<()> {
        let pending_events = self.pending_event_count() != 0;
        let next = match self.state {
            SessionWorkerState::Polling if !pending_events && last_vectors_per_loop < 1 => {
                SessionWorkerState::Interrupt
            }
            SessionWorkerState::Interrupt if pending_events || last_vectors_per_loop > 1 => {
                SessionWorkerState::Polling
            }
            SessionWorkerState::Interrupt if self.entries.is_empty() => SessionWorkerState::Idle,
            SessionWorkerState::Idle if pending_events => SessionWorkerState::Interrupt,
            state => state,
        };
        self.apply_state(runtime, next)
    }

    #[inline]
    fn pending_event_count(&self) -> usize {
        self.session_evt_q
            .len()
            .saturating_add(self.control_events.len())
            .saturating_add(self.new_io_events.len())
            .saturating_add(self.old_io_events.len())
            .saturating_add(self.app_rx_mq_pending.len())
    }

    fn apply_state(
        &mut self,
        runtime: &DataPlaneRuntime,
        next: SessionWorkerState,
    ) -> RuntimeResult<()> {
        if self.state == next {
            return Ok(());
        }
        let previous_node_state = self.state.node_state();
        if let Some(session_queue) = self.session_queue {
            runtime
                .nodes()
                .set_node_state(session_queue, next.node_state())?;
        }
        if let Some(index) = self.state_deadline_file
            && let Err(error) = runtime.file_main_mut().set_deadline(index, next.deadline())
        {
            if let Some(session_queue) = self.session_queue
                && let Err(cleanup_error) = runtime
                    .nodes()
                    .set_node_state(session_queue, previous_node_state)
            {
                tracing::error!(
                    %cleanup_error,
                    "failed to restore Session Queue state after deadline update failed"
                );
            }
            return Err(error);
        }
        self.state = next;
        Ok(())
    }

    pub(crate) fn wake_session_queue(&self, runtime: &DataPlaneRuntime) -> RuntimeResult<()> {
        if let Some(session_queue) = self.session_queue {
            let _ = runtime.set_node_interrupt_pending(session_queue)?;
        }
        Ok(())
    }

    #[inline]
    pub fn app_session_config(&self) -> AppSessionConfig {
        self.app_session_config
    }

    /// Installs one concrete Session App callback table for this worker.
    pub fn install_session_app(
        &mut self,
        app: SessionAppId,
        callbacks: SessionAppCallbacks<Index>,
    ) -> RuntimeResult<()> {
        let index = app.raw() as usize;
        if self.session_app_callbacks.len() <= index {
            self.session_app_callbacks.resize_with(index + 1, || None);
        }
        if self.session_app_callbacks[index].is_some() {
            return Err(SessionQueueError::SessionAppAlreadyInstalled { app }.into());
        }
        self.session_app_callbacks[index] = Some(callbacks);
        Ok(())
    }

    #[inline]
    pub fn session_app_callbacks(&self, app: SessionAppId) -> Option<SessionAppCallbacks<Index>> {
        self.session_app_callbacks
            .get(app.raw() as usize)
            .copied()
            .flatten()
    }

    #[inline]
    pub fn fifo_pair(&self, session_id: SessionId) -> Option<(&Fifo, &Fifo)> {
        let entry = self.entries.get(session_id.pool_index())?;
        Some((entry.rx_fifo.as_ref(), entry.tx_fifo.as_ref()))
    }

    /// Enqueue one complete VPP-shaped datagram into a Session RX FIFO.
    ///
    /// The header and every source buffer segment are copied into one FIFO
    /// reservation. The reservation is committed only after all segments have
    /// been copied, so a short FIFO or a segment allocation failure cannot
    /// publish a partial datagram.
    pub fn enqueue_datagram_rx_from_buffer(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        index: BufferIndex,
        header: SessionDgramHeader,
        urgent: bool,
    ) -> RuntimeResult<usize> {
        self.enqueue_datagram_rx_from_buffer_at(buffers, session_id, index, 0, header, urgent)
    }

    /// Variant of [`Self::enqueue_datagram_rx_from_buffer`] for a packet
    /// buffer whose current window still contains network and UDP headers.
    /// Only `payload_offset..payload_offset + header.data_length` is copied.
    pub fn enqueue_datagram_rx_from_buffer_at(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        index: BufferIndex,
        payload_offset: usize,
        header: SessionDgramHeader,
        urgent: bool,
    ) -> RuntimeResult<usize> {
        let Some(entry) = self.entries.get(session_id.pool_index()) else {
            return Ok(0);
        };
        let payload_len = header.data_length() as usize;
        let payload_end = payload_offset.checked_add(payload_len).ok_or(
            SessionError::DatagramLengthMismatch {
                session_id,
                payload_len,
                header_len: header.data_length(),
            },
        )?;
        let mut source_len = 0usize;
        for buffer in buffers.chain(index) {
            source_len = source_len
                .checked_add(buffer?.current_len())
                .ok_or(SessionError::RxLengthOverflow { session_id })?;
        }
        if payload_end > source_len {
            return Err(SessionError::DatagramLengthMismatch {
                session_id,
                payload_len: source_len.saturating_sub(payload_offset),
                header_len: header.data_length(),
            }
            .into());
        }
        let total = SessionDgramHeader::SIZE.checked_add(payload_len).ok_or(
            SessionError::DatagramLengthMismatch {
                session_id,
                payload_len,
                header_len: header.data_length(),
            },
        )?;
        if entry.rx_fifo.max_enqueue() < total {
            entry.rx_fifo.want_deq_notification();
            return Ok(0);
        }

        let mut reservation = match entry.rx_fifo.reserve_write(total) {
            Ok(reservation) => reservation,
            Err(hammer_infra::fifo::FifoError::InsufficientCapacity { .. }) => {
                entry.rx_fifo.want_deq_notification();
                return Ok(0);
            }
            Err(source) => {
                return Err(SessionError::DatagramFifo { session_id, source }.into());
            }
        };
        let header_bytes = header.to_bytes();
        let copied_header = reservation
            .copy_from_segments([header_bytes.as_slice()])
            .map_err(|source| SessionError::DatagramFifo { session_id, source })?;
        if copied_header != SessionDgramHeader::SIZE {
            reservation.cancel();
            return Err(SessionError::DatagramLengthMismatch {
                session_id,
                payload_len: copied_header,
                header_len: SessionDgramHeader::SIZE as u32,
            }
            .into());
        }
        let mut skip = payload_offset;
        let mut remaining = payload_len;
        for buffer in buffers.chain(index) {
            let buffer = buffer?;
            if skip >= buffer.current_len() {
                skip -= buffer.current_len();
                continue;
            }
            let start = skip;
            let take = (buffer.current_len() - start).min(remaining);
            reservation
                .copy_from_segments([&buffer.current()[start..start + take]])
                .map_err(|source| SessionError::DatagramFifo { session_id, source })?;
            remaining -= take;
            skip = 0;
            if remaining == 0 {
                break;
            }
        }
        if remaining != 0 {
            reservation.cancel();
            return Err(SessionError::DatagramLengthMismatch {
                session_id,
                payload_len: payload_len - remaining,
                header_len: header.data_length(),
            }
            .into());
        }
        reservation
            .commit(total)
            .map_err(|source| SessionError::DatagramFifo { session_id, source })?;
        self.publish_rx_enqueue_with_flags(
            session_id,
            total,
            if urgent {
                SessionEvtFlags::URGENT
            } else {
                SessionEvtFlags::empty()
            },
        )?;
        Ok(payload_len)
    }

    /// Peek the next complete TX datagram header without consuming it.
    pub fn peek_tx_datagram(
        &self,
        session_id: SessionId,
    ) -> RuntimeResult<Option<SessionDgramHeader>> {
        let Some(entry) = self.entries.get(session_id.pool_index()) else {
            return Ok(None);
        };
        if entry.tx_fifo.max_dequeue() < SessionDgramHeader::SIZE {
            return Ok(None);
        }
        let mut bytes = [0u8; SessionDgramHeader::SIZE];
        if entry.tx_fifo.peek(0, bytes.len(), &mut bytes) != bytes.len() {
            return Ok(None);
        }
        let Some(header) = SessionDgramHeader::from_bytes(&bytes) else {
            return Ok(None);
        };
        let Some(total) = header.total_len() else {
            return Ok(None);
        };
        if header.data_offset() > header.data_length() || entry.tx_fifo.max_dequeue() < total {
            return Ok(None);
        }
        Ok(Some(header))
    }

    /// Copy a complete TX datagram payload into an already allocated buffer.
    /// The FIFO header remains at the head until [`Self::dequeue_tx_datagram`]
    /// is called after output header construction succeeds.
    pub fn copy_tx_datagram_to_buffer(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        header: SessionDgramHeader,
        index: BufferIndex,
    ) -> RuntimeResult<usize> {
        let data_offset = header.data_offset() as usize;
        let data_length = header.data_length() as usize;
        if data_offset > data_length {
            return Err(SessionError::DatagramLengthMismatch {
                session_id,
                payload_len: data_offset,
                header_len: header.data_length(),
            }
            .into());
        }
        let payload_len = data_length - data_offset;
        let fifo_offset = SessionDgramHeader::SIZE.checked_add(data_offset).ok_or(
            SessionError::DatagramLengthMismatch {
                session_id,
                payload_len,
                header_len: header.data_length(),
            },
        )?;
        self.copy_tx_to_buffer(buffers, session_id, fifo_offset, payload_len, index)?;
        Ok(payload_len)
    }

    /// Consume one complete TX datagram after its data-plane buffer is ready.
    pub fn dequeue_tx_datagram(
        &self,
        session_id: SessionId,
        header: SessionDgramHeader,
    ) -> RuntimeResult<usize> {
        let total = header
            .total_len()
            .ok_or(SessionError::DatagramLengthMismatch {
                session_id,
                payload_len: header.data_offset() as usize,
                header_len: header.data_length(),
            })?;
        let Some(entry) = self.entries.get(session_id.pool_index()) else {
            return Ok(0);
        };
        if entry.tx_fifo.max_dequeue() < total {
            return Ok(0);
        }
        let dropped = entry.tx_fifo.dequeue_drop(total);
        self.publish_tx_dequeue(session_id, dropped)?;
        Ok(dropped)
    }

    #[inline]
    pub fn session_app_endpoint(
        &self,
        session_id: SessionId,
    ) -> Option<(
        ApplicationId,
        Option<SessionAppId>,
        Option<u64>,
        Option<&str>,
    )> {
        let entry = self.entries.get(session_id.pool_index())?;
        Some((
            entry.owner_application?,
            entry.app,
            entry.app_opaque,
            entry.server_name.as_deref(),
        ))
    }

    /// Publishes a Session-owned upper App Session from a Session App callback.
    pub fn create_upper_session(
        &mut self,
        lower: SessionId,
        context: SessionAppContext,
    ) -> RuntimeResult<SessionId> {
        let (application, app, _opaque, _server_name) = self
            .session_app_endpoint(lower)
            .ok_or(SessionError::SessionMissing { session_id: lower })?;
        let Some(app) = app else {
            return Err(SessionError::SessionMissing { session_id: lower }.into());
        };
        let app_rx_mq = self
            .app_mq_worker(application)
            .ok_or(SessionQueueError::ApplicationMqMissing { application })?;
        let (rx_fifo, tx_fifo) = self.create_local_fifos()?;
        let upper = self.insert_session_entry(SessionEntry::unbound(rx_fifo, tx_fifo))?;
        let app_session = self
            .app
            .create_app_session(
                lower.get(),
                Some(application),
                self.session_handle(upper),
                self.app_session_config,
                app_rx_mq,
            )
            .map_err(|error| {
                let _ = self.entries.remove(upper.pool_index());
                error
            })?;
        let entry = self
            .entries
            .get_mut(upper.pool_index())
            .expect("new upper Session remains installed during publication");
        entry.rx_fifo = Arc::clone(app_session.rx_fifo());
        entry.tx_fifo = Arc::clone(app_session.tx_fifo());
        entry.application = Some(SessionApplication::External(application));
        entry.owner_application = Some(application);
        entry.app = Some(app);
        entry.app_session = context;
        entry.lower_session = Some(lower);
        self.app.attach_session(upper, app_session);
        if let Err(error) = self.app.connected(upper) {
            let _ = self.entries.remove(upper.pool_index());
            self.app.detach_session(upper);
            return Err(error);
        }
        Ok(upper)
    }

    /// Creates an App-facing upper Session bound to a lower Session and to
    /// the transport index that owns its Connection or Stream state.
    pub fn create_upper_transport_session(
        &mut self,
        lower: SessionId,
        transport: SessionTransportId,
        index: Index,
        context: SessionAppContext,
    ) -> RuntimeResult<SessionId> {
        let (application, _app, opaque, server_name) = self
            .session_app_endpoint(lower)
            .ok_or(SessionError::SessionMissing { session_id: lower })?;
        let server_name = server_name.map(str::to_owned);
        let upper = self.construct_transport_session(
            transport,
            index,
            lower.get(),
            application,
            None,
            opaque,
            server_name.as_deref(),
            false,
        )?;
        let entry = self
            .entries
            .get_mut(upper.pool_index())
            .expect("new upper transport Session remains installed during publication");
        entry.app_session = context;
        entry.lower_session = Some(lower);
        Ok(upper)
    }

    /// Publishes a handshake-established transport Session and notifies the
    /// external App without re-entering the lower Session App callback.
    pub fn publish_connected_transport_session(
        &mut self,
        session_id: SessionId,
    ) -> RuntimeResult<()> {
        self.connection_published(session_id)?;
        if let Err(error) = self.connected(session_id) {
            assert!(
                self.remove_session(session_id).is_ok(),
                "upper transport Session rollback failed"
            );
            return Err(error);
        }
        Ok(())
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

    /// Registers one per-Application MQ with this Data Worker's FileMain.
    pub(crate) fn install_app_mq(
        &mut self,
        application: ApplicationId,
        queue: Arc<SessionMsgQueue>,
        app_session_input: hammer_core::data_plane::NodeId,
        runtime: &mut DataPlaneRuntime,
    ) -> RuntimeResult<()> {
        let slot = application.slot() as usize;
        if slot >= self.app_rx_mqs.len() {
            self.app_rx_mqs.resize_with(slot + 1, || None);
        }
        if self.app_rx_mqs[slot]
            .as_ref()
            .is_some_and(|entry| entry.application == application)
        {
            return Err(SessionQueueError::ApplicationMqAlreadyRegistered { application }.into());
        }
        let Some(signal_read_fd) = queue.read_fd() else {
            return Err(AttachError::SessionSignalMissing.into());
        };
        // SAFETY: the queue retains its original read endpoint while FileMain
        // owns this independent duplicated descriptor.
        let signal_read = unsafe { BorrowedFd::borrow_raw(signal_read_fd) }
            .try_clone_to_owned()
            .map_err(|source| AttachError::SessionSignalDuplicate { source })?;
        let entry = Box::new(AppRxMqEntry {
            application,
            queue,
            file: None,
            appsl_input_node: app_session_input,
            pending_queue: &mut self.app_rx_mq_pending as *mut VecDeque<ApplicationId> as usize,
            pending: false,
            postponed: false,
        });
        let entry_ptr = Box::into_raw(entry);
        let file = match runtime.file_main_mut().add(File::new(
            signal_read,
            format!("app rx mq {:?}", application.raw()),
            entry_ptr as usize as u64,
            FileFunctions {
                read: Some(schedule_app_mq_pending),
                ..FileFunctions::default()
            },
        )) {
            Ok(file) => file,
            Err(error) => {
                // SAFETY: `entry_ptr` still owns the entry until FileMain add
                // succeeds; reconstruct it here so the queue is dropped.
                unsafe {
                    drop(Box::from_raw(entry_ptr));
                }
                return Err(error);
            }
        };
        // SAFETY: `entry_ptr` was produced by `Box::into_raw` and is still
        // unaliased; reconstruct it for worker-local storage.
        let mut entry = unsafe { Box::from_raw(entry_ptr) };
        entry.file = Some(file);
        self.app_rx_mqs[slot] = Some(entry);
        Ok(())
    }

    /// Removes one per-Application MQ registration before the queue is
    /// released by `ApplicationMain`.
    pub(crate) fn remove_app_mq(
        &mut self,
        application: ApplicationId,
        runtime: &mut DataPlaneRuntime,
    ) -> RuntimeResult<()> {
        let slot = application.slot() as usize;
        let Some(entry) = self.app_rx_mqs.get_mut(slot).and_then(|entry| {
            if entry
                .as_ref()
                .is_some_and(|entry| entry.application == application)
            {
                entry.take()
            } else {
                None
            }
        }) else {
            return Ok(());
        };
        self.app_rx_mq_pending
            .retain(|candidate| *candidate != application);
        if let Some(file) = entry.file {
            match runtime.file_main_mut().delete(file) {
                Ok(true) => {}
                Ok(false) => {
                    if entry.pending {
                        self.app_rx_mq_pending.push_back(application);
                    }
                    self.app_rx_mqs[slot] = Some(entry);
                    return Err(RuntimeError::FileIndexInvalid { index: file });
                }
                Err(error) => {
                    if entry.pending {
                        self.app_rx_mq_pending.push_back(application);
                    }
                    self.app_rx_mqs[slot] = Some(entry);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn mark_app_mq_pending(&mut self, application: ApplicationId) -> bool {
        let slot = application.slot() as usize;
        let Some(Some(entry)) = self.app_rx_mqs.get_mut(slot) else {
            return false;
        };
        if entry.application != application || entry.pending || entry.queue.is_empty() {
            return false;
        }
        entry.pending = true;
        self.app_rx_mq_pending.push_back(application);
        true
    }

    #[inline]
    pub(crate) fn app_mq_worker(&self, application: ApplicationId) -> Option<Arc<SessionMsgQueue>> {
        self.app_rx_mqs
            .get(application.slot() as usize)?
            .as_ref()
            .filter(|entry| entry.application == application)
            .map(|entry| Arc::clone(&entry.queue))
    }

    pub(crate) fn has_pending_app_mqs(&self) -> bool {
        !self.app_rx_mq_pending.is_empty()
    }

    fn drain_app_mq_events_to(
        &mut self,
        mut dispatch_event: impl FnMut(SessionMqRing, SessionEvt),
    ) -> usize {
        let pending_snapshot = core::mem::take(&mut self.app_rx_mq_pending);
        pending_snapshot
            .into_iter()
            .fold(0usize, |dispatched, application| {
                let slot = application.slot() as usize;
                let Some(Some(entry)) = self.app_rx_mqs.get_mut(slot) else {
                    return dispatched;
                };
                if entry.application != application || !entry.pending {
                    return dispatched;
                }
                let entry_dispatched = entry.drain_snapshot_to(&mut dispatch_event);
                if entry.pending {
                    self.app_rx_mq_pending.push_back(application);
                }
                dispatched.saturating_add(entry_dispatched)
            })
    }

    pub(crate) fn drain_app_mq(&mut self, application: ApplicationId) -> RuntimeResult<usize> {
        let mut control_events = core::mem::take(&mut self.control_events);
        let mut new_io_events = core::mem::take(&mut self.new_io_events);
        let handled = self.drain_one_app_mq_to(application, |ring, event| {
            enqueue_app_event(&mut control_events, &mut new_io_events, ring, event);
        });
        self.control_events = control_events;
        self.new_io_events = new_io_events;
        Ok(handled)
    }

    fn drain_one_app_mq_to(
        &mut self,
        application: ApplicationId,
        mut dispatch_event: impl FnMut(SessionMqRing, SessionEvt),
    ) -> usize {
        self.app_rx_mq_pending
            .retain(|candidate| *candidate != application);
        let slot = application.slot() as usize;
        let Some(Some(entry)) = self.app_rx_mqs.get_mut(slot) else {
            return 0;
        };
        if entry.application != application {
            return 0;
        }

        let dispatched = entry.drain_snapshot_to(&mut dispatch_event);
        if entry.pending {
            self.app_rx_mq_pending.push_back(application);
        }
        dispatched
    }

    #[inline]
    pub fn session_transport(&self, session_id: SessionId) -> Option<(SessionTransportId, Index)> {
        let entry = self.entries.get(session_id.pool_index())?;
        let Some(SessionType::Transport { transport, state }) = entry.session_type else {
            return None;
        };
        Some((transport, state.transport_index()?))
    }

    #[inline]
    pub fn has_session(&self, session_id: SessionId) -> bool {
        self.entries.contains_key(session_id.pool_index())
    }

    #[inline]
    pub fn app_session(&self, session_id: SessionId) -> Option<&Arc<AppSession>> {
        self.app.app_session(session_id)
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
            .with_listener(application_listener, |listener| {
                self.construct_stream_sessions(
                    transport,
                    index,
                    application_listener.raw(),
                    listener.application(),
                    listener.app(),
                    listener.opaque(),
                    None,
                    true,
                )
            })
            .map_err(RuntimeError::from)?
    }

    /// Constructs one transport Session with explicit Session-owned
    /// Application facts.
    ///
    /// This is the generic creation seam used by protocols that publish a
    /// child Session under a connection or listener without exposing their
    /// private parent relation to Session Runtime. The transport remains the
    /// sole owner of the relationship encoded in `index`.
    pub fn construct_transport_session(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        allocation_owner: u64,
        application: ApplicationId,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
        server_name: Option<&str>,
        accepted: bool,
    ) -> RuntimeResult<SessionId> {
        self.construct_stream_sessions(
            transport,
            index,
            allocation_owner,
            application,
            app,
            opaque,
            server_name,
            accepted,
        )
    }

    pub fn stream_connect(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        connection: SessionConnectionId,
    ) -> RuntimeResult<SessionId> {
        let application_connection = ApplicationConnectionId::from_raw(connection.raw());
        let applications = Arc::clone(&self.applications);
        let session_id = applications
            .with_connection(application_connection, |connection| {
                self.construct_stream_sessions(
                    transport,
                    index,
                    application_connection.raw(),
                    connection.application(),
                    connection.app(),
                    connection.opaque(),
                    connection.server_name(),
                    false,
                )
            })
            .map_err(RuntimeError::from)??;
        if let Err(source) = applications.complete_connection(application_connection) {
            self.remove_session(session_id)?;
            return Err(RuntimeError::from(source));
        }
        Ok(session_id)
    }

    fn construct_stream_sessions(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        allocation_owner: u64,
        application: ApplicationId,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
        server_name: Option<&str>,
        accepted: bool,
    ) -> RuntimeResult<SessionId> {
        let Some(app) = app else {
            return self.construct_external_transport_session(
                transport,
                index,
                allocation_owner,
                application,
            );
        };
        self.construct_app_transport_session(
            transport,
            index,
            application,
            app,
            opaque,
            server_name,
            accepted,
        )
    }

    fn construct_app_transport_session(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        application: ApplicationId,
        app: SessionAppId,
        opaque: Option<u64>,
        server_name: Option<&str>,
        accepted: bool,
    ) -> RuntimeResult<SessionId> {
        let (rx_fifo, tx_fifo) = self.create_local_fifos()?;
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            transport, rx_fifo, tx_fifo,
        ))?;
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .expect("new Session App transport Session remains installed during construction");
        entry.owner_application = Some(application);
        entry.app = Some(app);
        entry.app_opaque = opaque;
        entry.server_name = server_name.map(str::to_owned);
        entry.accepted = accepted;
        self.finish_transport_creation(session_id, index);
        Ok(session_id)
    }

    fn construct_external_transport_session(
        &mut self,
        transport: SessionTransportId,
        index: Index,
        allocation_owner: u64,
        application: ApplicationId,
    ) -> RuntimeResult<SessionId> {
        let (rx_fifo, tx_fifo) = self.create_local_fifos()?;
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            transport, rx_fifo, tx_fifo,
        ))?;
        let app_rx_mq = match self.app_mq_worker(application) {
            Some(queue) => queue,
            None => {
                self.rollback_stream_sessions(&[session_id], None);
                return Err(SessionQueueError::ApplicationMqMissing { application }.into());
            }
        };
        let application_session = match self.app.create_app_session(
            allocation_owner,
            Some(application),
            self.session_handle(session_id),
            self.app_session_config,
            app_rx_mq,
        ) {
            Ok(session) => session,
            Err(error) => {
                self.rollback_stream_sessions(&[session_id], None);
                return Err(error);
            }
        };
        {
            let entry = self
                .entries
                .get_mut(session_id.pool_index())
                .expect("new external Transport Session remains installed during construction");
            entry.rx_fifo = Arc::clone(application_session.rx_fifo());
            entry.tx_fifo = Arc::clone(application_session.tx_fifo());
            entry.application = Some(SessionApplication::External(application));
            entry.owner_application = Some(application);
        }
        self.finish_transport_creation(session_id, index);
        self.app.attach_session(session_id, application_session);
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
        session_ids.iter().rev().copied().for_each(|session_id| {
            let _ = self.entries.remove(session_id.pool_index());
        });
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
    pub fn session_id_from_handle(&self, handle: SessionHandle) -> Option<SessionId> {
        if handle.worker_index() != self.worker.slot() as u32 {
            return None;
        }
        self.entries
            .index_at_slot(handle.session_index())
            .map(SessionId::from)
    }

    pub fn program_thread_migration(
        &self,
        runtime: &DataPlaneRuntime,
        target_worker: DataWorkerId,
        old_handle: SessionHandle,
        transport: SessionTransportId,
        local: SocketAddr,
        remote: SocketAddr,
        dgram: SessionDgramArgs,
    ) -> SessionMigrateResult {
        self.listener_main
            .as_ref()
            .map_or(SessionMigrateResult::Unavailable, |main| {
                main.program_thread_migration(
                    runtime,
                    target_worker,
                    old_handle,
                    transport,
                    local,
                    remote,
                    dgram,
                )
            })
    }

    pub fn cancel_thread_migration(
        &self,
        old_handle: SessionHandle,
        transport: SessionTransportId,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> bool {
        self.listener_main
            .as_ref()
            .is_some_and(|main| main.cancel_migration(local, remote, transport, old_handle))
    }

    pub fn push_session_switch_pool_reply(
        &self,
        runtime: &DataPlaneRuntime,
        reply: SessionSwitchPoolReply,
    ) -> Result<(), SessionSwitchPoolReply> {
        let Some(main) = self.listener_main.as_ref() else {
            return Err(reply);
        };
        main.push_session_switch_pool_reply(runtime, reply)
    }

    pub fn pop_session_migrate_request(&self) -> Option<SessionSwitchPoolArgs> {
        self.listener_main
            .as_ref()?
            .pop_session_migrate_request(self.worker)
    }

    pub fn pop_session_switch_pool_reply(&self) -> Option<SessionSwitchPoolReply> {
        self.listener_main
            .as_ref()?
            .pop_session_switch_pool_reply(self.worker)
    }

    pub fn push_session_switch_pool_completion(
        &self,
        runtime: &DataPlaneRuntime,
        completion: SessionSwitchPoolCompletion,
    ) -> Result<(), SessionSwitchPoolCompletion> {
        self.listener_main.as_ref().map_or(Err(completion), |main| {
            main.push_session_switch_pool_completion(runtime, completion)
        })
    }

    pub fn pop_session_switch_pool_completion(&self) -> Option<SessionSwitchPoolCompletion> {
        self.listener_main
            .as_ref()?
            .pop_session_switch_pool_completion(self.worker)
    }

    pub fn insert_session_endpoint(
        &self,
        session_id: SessionId,
        transport: SessionTransportId,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.insert_endpoint(local, remote, transport, self.session_handle(session_id)))
    }

    pub fn mark_session_endpoint_ready(
        &self,
        session_id: SessionId,
        transport: SessionTransportId,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.mark_endpoint_ready(local, remote, transport, self.session_handle(session_id)))
    }

    pub fn remove_session_endpoint(
        &self,
        session_id: SessionId,
        transport: SessionTransportId,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.remove_endpoint_if_current(
            local,
            remote,
            transport,
            self.session_handle(session_id),
        ))
    }

    pub fn replace_session_endpoint(
        &self,
        old_session: SessionHandle,
        new_session: SessionHandle,
        transport: SessionTransportId,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.replace_endpoint_if_current(local, remote, transport, old_session, new_session))
    }

    pub fn publish_session_migration(
        &self,
        old_session: SessionHandle,
        new_session: SessionHandle,
        transport: SessionTransportId,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.publish_migration_endpoint(local, remote, transport, old_session, new_session))
    }

    pub fn lookup_session_endpoint(
        &self,
        transport: SessionTransportId,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<Option<SessionHandle>> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.lookup_endpoint(local, remote, transport))
    }

    pub fn session_endpoint_is_migrating(
        &self,
        transport: SessionTransportId,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.endpoint_is_migrating(local, remote, transport))
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

    pub fn migration_snapshot(&self, session_id: SessionId) -> Option<SessionMigrationState> {
        let entry = self.entries.get(session_id.pool_index())?;
        if entry.application.is_some()
            || entry.owner_application.is_some()
            || entry.app.is_some()
            || entry.app_session != 0
            || entry.lower_session.is_some()
        {
            return None;
        }
        let Some(SessionType::Transport { transport, state }) = entry.session_type else {
            return None;
        };
        state.transport_index()?;
        Some(SessionMigrationState {
            transport,
            rx_fifo: Arc::clone(&entry.rx_fifo),
            tx_fifo: Arc::clone(&entry.tx_fifo),
        })
    }

    pub fn install_migrated_session(
        &mut self,
        state: SessionMigrationState,
        index: Index,
    ) -> RuntimeResult<(SessionId, SessionHandle)> {
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            state.transport,
            state.rx_fifo,
            state.tx_fifo,
        ))?;
        self.finish_transport_creation(session_id, index);
        if let Err(error) = self.connection_published(session_id) {
            let _ = self.remove_session(session_id);
            return Err(error);
        }
        Ok((session_id, self.session_handle(session_id)))
    }

    pub fn accept_migrated_session(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        self.connected(session_id)?;
        let (tx_pending, rx_event) = {
            let entry = self
                .entries
                .get(session_id.pool_index())
                .ok_or(SessionError::SessionMissing { session_id })?;
            (entry.tx_fifo.max_dequeue() != 0, entry.rx_fifo.has_event())
        };
        if tx_pending {
            self.mark_ready(session_id);
        }
        if rx_event {
            self.new_io_events.push_back(SessionEvt::io(
                session_id.pool_index().slot(),
                SessionEvtType::RxEnq,
            ));
        }
        Ok(())
    }

    pub fn notify_migrated_session(
        &mut self,
        old_session: SessionId,
        new_handle: SessionHandle,
    ) -> RuntimeResult<()> {
        let Some((app, context)) = self.entries.get(old_session.pool_index()).and_then(|entry| {
            entry
                .app
                .zip((entry.app_session != 0).then_some(entry.app_session))
        }) else {
            return Ok(());
        };
        let callback = self
            .session_app_callbacks(app)
            .and_then(|callbacks| callbacks.migrate);
        if let Some(callback) = callback {
            callback(self, old_session, new_handle, context)?;
        }
        Ok(())
    }

    pub fn remove_migrated_session(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get(session_id.pool_index())
            .ok_or(SessionError::SessionMissing { session_id })?;
        if entry.application.is_some()
            || entry.owner_application.is_some()
            || entry.app.is_some()
            || entry.app_session != 0
            || entry.lower_session.is_some()
        {
            return Err(SessionError::SessionMissing { session_id }.into());
        }
        self.remove_session(session_id)
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

    #[doc(hidden)]
    pub fn insert_unbound_transport_session_for_test(
        &mut self,
        transport: SessionTransportId,
        index: Index,
    ) -> RuntimeResult<SessionId> {
        let (rx_fifo, tx_fifo) = self.create_local_fifos()?;
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            transport, rx_fifo, tx_fifo,
        ))?;
        self.finish_transport_creation(session_id, index);
        self.connection_published(session_id)?;
        self.connected(session_id)?;
        Ok(session_id)
    }

    pub fn insert_session_for_test(
        &mut self,
        transport: SessionTransportId,
        index: Index,
    ) -> SessionId {
        let application = self.applications.attach().expect("attach test Application");
        self.install_application_mq_for_test(application)
            .expect("install Application Rx MQ");
        let app_rx_mq = self
            .app_mq_worker(application)
            .expect("test Application Rx MQ remains installed");

        let (rx_fifo, tx_fifo) = self.create_local_fifos().expect("test Session FIFOs");
        let session_id = self
            .insert_session_entry(SessionEntry::creating_transport(
                transport, rx_fifo, tx_fifo,
            ))
            .expect("insert test Session");
        let session = self
            .app
            .create_app_session(
                0,
                None,
                self.session_handle(session_id),
                self.app_session_config,
                app_rx_mq,
            )
            .expect("create test App Session");
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .expect("test Session remains installed");
        entry.rx_fifo = Arc::clone(session.rx_fifo());
        entry.tx_fifo = Arc::clone(session.tx_fifo());
        entry.application = Some(SessionApplication::External(application));
        self.finish_transport_creation(session_id, index);
        self.app.attach_session(session_id, session);
        self.connection_published(session_id)
            .expect("publish session connection");
        self.connected(session_id).expect("connect session");
        session_id
    }

    /// Installs a per-Application MQ for direct Session Worker tests.
    ///
    /// These workers have no FileMain, so the queue is staged by `poll_app`.
    /// Runtime-attached applications use `install_app_mq`, which registers
    /// the queue's signal descriptor with FileMain instead.
    #[doc(hidden)]
    pub fn install_application_mq_for_test(
        &mut self,
        application: ApplicationId,
    ) -> RuntimeResult<()> {
        let application_slot = application.slot() as usize;
        if application_slot >= self.app_rx_mqs.len() {
            self.app_rx_mqs.resize_with(application_slot + 1, || None);
        }
        if self.app_rx_mqs[application_slot]
            .as_ref()
            .is_some_and(|entry| entry.application == application)
        {
            return Err(SessionQueueError::ApplicationMqAlreadyRegistered { application }.into());
        }
        let app_rx_mq = Arc::new(
            SessionMsgQueue::with_cfg(
                DEFAULT_SESSION_EVENT_CAPACITY as u32,
                DEFAULT_SESSION_EVENT_CAPACITY as u32,
            )
            .map_err(|error| AppWorkerError::SessionEventQueue { error })?,
        );
        let pending_queue = &mut self.app_rx_mq_pending as *mut VecDeque<ApplicationId> as usize;
        self.app_rx_mqs[application_slot] = Some(Box::new(AppRxMqEntry {
            application,
            queue: app_rx_mq,
            file: None,
            appsl_input_node: NodeId::new(0),
            pending_queue,
            pending: false,
            postponed: false,
        }));
        Ok(())
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
        sessions
            .into_iter()
            .for_each(|session_id| self.schedule_disconnect(session_id));
    }

    pub fn notify_transport_closed(
        &mut self,
        session_id: SessionId,
        index: Index,
    ) -> RuntimeResult<()> {
        self.notify_transport_event(session_id, index, SessionEvtType::TransportClosed)
    }

    pub fn notify_transport_closing(
        &mut self,
        runtime: &DataPlaneRuntime,
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
            self.notify_application_event(session_id, SessionEvtType::Disconnected)?;
            self.wake_session_queue(runtime)?;
        }
        Ok(())
    }

    pub fn notify_transport_reset(
        &mut self,
        session_id: SessionId,
        index: Index,
    ) -> RuntimeResult<()> {
        self.notify_transport_event(session_id, index, SessionEvtType::Reset)
    }

    fn notify_transport_event(
        &mut self,
        session_id: SessionId,
        index: Index,
        event: SessionEvtType,
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
            self.notify_application_event(session_id, event)?;
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
        let (app, context, lower_session) = self
            .entries
            .get(session_id.pool_index())
            .map(|entry| (entry.app, entry.app_session, entry.lower_session))
            .unwrap_or((None, 0, None));
        if let Some(app) = app
            && context != 0
            && let Some(callbacks) = self.session_app_callbacks(app)
            && let Some(cleanup) = callbacks.cleanup
        {
            cleanup(self, session_id, context)?;
        }
        if let Some(app) = app
            && context != 0
            && lower_session.is_none()
            && let Some(registration) = self.applications.session_app_registration(app)
        {
            registration.destroy(self.worker, context);
        }
        let Some(entry) = self.entries.remove(session_id.pool_index()) else {
            return Ok(());
        };
        if matches!(entry.application, Some(SessionApplication::External(_))) {
            drop(self.app.detach_session(session_id));
        }
        if app.is_some() && lower_session.is_none() {
            let upper_sessions = self
                .entries
                .iter()
                .filter_map(|(index, entry)| {
                    (entry.lower_session == Some(session_id)).then_some(SessionId::from(index))
                })
                .collect::<Vec<_>>();
            for upper in upper_sessions {
                self.remove_session(upper)?;
            }
        }
        Ok(())
    }

    fn notify_application_event(
        &mut self,
        session_id: SessionId,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let app = self
            .entries
            .get(session_id.pool_index())
            .and_then(|entry| entry.app);
        if let Some(app) = app {
            return self.dispatch_app_lifecycle(session_id, app, event);
        }
        let application = self
            .entries
            .get(session_id.pool_index())
            .and_then(|entry| entry.application);
        match application {
            Some(SessionApplication::External(_)) => match event {
                SessionEvtType::Disconnected => self.app.disconnected(session_id),
                SessionEvtType::Reset => self.app.reset(session_id),
                SessionEvtType::TransportClosed => self.app.transport_closed(session_id),
                _ => Ok(()),
            },
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
        self.new_io_events.push_back(SessionEvt::io(
            session_id.pool_index().slot(),
            SessionEvtType::TxEnq,
        ));
    }

    #[inline]
    fn reschedule_old(&mut self, session_id: SessionId) {
        let Some(entry) = self.entries.get_mut(session_id.pool_index()) else {
            return;
        };
        if entry.schedule_pending {
            return;
        }
        entry.schedule_pending = true;
        self.old_io_events.push_back(SessionEvt::io(
            session_id.pool_index().slot(),
            SessionEvtType::TxEnq,
        ));
    }

    #[inline]
    pub fn schedule_disconnect(&mut self, session_id: SessionId) {
        self.control_events.push_back(SessionEvt::ctrl(
            session_id.pool_index().slot(),
            self.worker.slot() as u32,
            SessionEvtType::Close,
        ));
    }

    #[inline]
    pub fn schedule_half_close(&mut self, session_id: SessionId) {
        self.control_events.push_back(SessionEvt::ctrl(
            session_id.pool_index().slot(),
            self.worker.slot() as u32,
            SessionEvtType::HalfClose,
        ));
    }

    pub fn poll_app(&mut self) -> RuntimeResult<usize> {
        // Direct test registrations have no FileMain descriptor; production
        // registrations always acquire one in `install_app_mq`.
        let pending_applications = self
            .app_rx_mqs
            .iter_mut()
            .filter_map(|entry| {
                let entry = entry.as_mut()?;
                (entry.file.is_none() && !entry.pending && !entry.queue.is_empty()).then(|| {
                    entry.pending = true;
                    entry.application
                })
            })
            .collect::<Vec<_>>();
        self.app_rx_mq_pending.extend(pending_applications);
        let mut control_events = core::mem::take(&mut self.control_events);
        let mut new_io_events = core::mem::take(&mut self.new_io_events);
        let app_mq_handled = self.drain_app_mq_events_to(|ring, event| {
            enqueue_app_event(&mut control_events, &mut new_io_events, ring, event);
        });
        self.control_events = control_events;
        self.new_io_events = new_io_events;
        Ok(app_mq_handled)
    }

    pub(crate) fn poll_session_events(&mut self) -> RuntimeResult<usize> {
        let snapshot = self.session_evt_q.len();
        let handled = (0..snapshot)
            .map_while(|_| self.session_evt_q.dequeue_with_ring())
            .map(|(ring, event)| {
                enqueue_app_event(
                    &mut self.control_events,
                    &mut self.new_io_events,
                    ring,
                    event,
                );
            })
            .count();
        Ok(handled)
    }

    fn dispatch_application(
        &mut self,
        session_id: SessionId,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let app = self
            .entries
            .get(session_id.pool_index())
            .and_then(|entry| entry.app);
        if let Some(app) = app {
            return self.dispatch_app_event(session_id, app, event);
        }
        let application = self
            .entries
            .get(session_id.pool_index())
            .and_then(|entry| entry.application);
        match application {
            Some(SessionApplication::External(_)) => match event {
                SessionEvtType::RxEnq | SessionEvtType::TxDeq => {
                    let Some(session) = self.app.app_session(session_id) else {
                        return Ok(());
                    };
                    session.push_io_event(event).map_err(RuntimeError::from)
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
        let app = self
            .entries
            .get(session_id.pool_index())
            .and_then(|entry| entry.app);
        let Some(app) = app else {
            return Ok(());
        };
        self.dispatch_app_event(session_id, app, event)
    }

    fn dispatch_app_event(
        &mut self,
        session_id: SessionId,
        app: SessionAppId,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let context = self
            .entries
            .get(session_id.pool_index())
            .map(|entry| entry.app_session)
            .unwrap_or(0);
        let callbacks = self
            .session_app_callbacks(app)
            .ok_or(SessionQueueError::SessionAppNotInstalled { app })?;
        let callback = match event {
            SessionEvtType::RxEnq | SessionEvtType::RxDeq => callbacks.builtin_rx,
            SessionEvtType::TxEnq | SessionEvtType::TxDeq | SessionEvtType::ProtocolOutput => {
                callbacks.builtin_tx
            }
            SessionEvtType::Connect => {
                let accepted = self
                    .entries
                    .get(session_id.pool_index())
                    .map(|entry| entry.accepted)
                    .unwrap_or(false);
                if accepted {
                    callbacks.accept
                } else {
                    callbacks.connected
                }
            }
            _ => return Ok(()),
        };
        if let Some(callback) = callback {
            callback(self, session_id, context)?;
        }
        Ok(())
    }

    fn dispatch_app_lifecycle(
        &mut self,
        session_id: SessionId,
        app: SessionAppId,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let context = self
            .entries
            .get(session_id.pool_index())
            .map(|entry| entry.app_session)
            .unwrap_or(0);
        let callbacks = self
            .session_app_callbacks(app)
            .ok_or(SessionQueueError::SessionAppNotInstalled { app })?;
        let callback = match event {
            SessionEvtType::Disconnected => callbacks.disconnect,
            SessionEvtType::Reset => callbacks.reset,
            SessionEvtType::TransportClosed => callbacks.transport_closed,
            _ => None,
        };
        if let Some(callback) = callback {
            callback(self, session_id, context)?;
        }
        Ok(())
    }

    pub fn publish_rx_enqueue(&self, session_id: SessionId, produced: usize) -> RuntimeResult<()> {
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
                    .push_io_event_with_flags(SessionEvtType::RxEnq, flags)
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

    pub fn publish_rx_dequeue(&self, session_id: SessionId, consumed: usize) -> RuntimeResult<()> {
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

    pub fn publish_tx_enqueue(&self, session_id: SessionId, produced: usize) -> RuntimeResult<()> {
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

    pub fn publish_tx_dequeue(&self, session_id: SessionId, consumed: usize) -> RuntimeResult<()> {
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
        self.session_evt_q
            .enqueue_io(event)
            .map_err(|error| AppWorkerError::SessionEventQueue { error }.into())
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
    pub fn new(
        worker: DataWorkerId,
        worker_count: usize,
        app_session_config: AppSessionConfig,
        pool_capacity: usize,
        applications: Arc<ApplicationMain>,
        publisher: Option<AppSessionPublisher>,
    ) -> RuntimeResult<Self> {
        let cap = DEFAULT_SESSION_EVENT_CAPACITY.next_power_of_two().max(2) as u32;
        let session_evt_q = Arc::new(
            SessionMsgQueue::with_cfg(cap, cap.max(2))
                .map_err(|error| AppWorkerError::SessionEventQueue { error })?,
        );
        let app = AppWorker::new(pool_capacity, worker.slot(), publisher);
        Ok(Self {
            worker,
            worker_count,
            entries: Pool::with_capacity(pool_capacity),
            listener_main: None,
            applications,
            app,
            session_evt_q,
            app_session_config,
            session_app_callbacks: Vec::new(),
            transport_dispatches: Vec::new(),
            control_events: LinkedList::new(),
            new_io_events: LinkedList::new(),
            old_io_events: LinkedList::new(),
            app_rx_mqs: Vec::new(),
            app_rx_mq_pending: VecDeque::new(),
            state: SessionWorkerState::Polling,
            state_deadline_file: None,
            session_queue: None,
        })
    }

    /// Associates this Data Worker with the Session Main that owns listener
    /// routes before a transport may deliver accepted streams to it.
    pub fn set_listener_main(&mut self, main: Arc<SessionMain>) {
        self.listener_main = Some(main);
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

    /// Returns the transport connection that owns the given Session transport
    /// object. One transport connection may own many Session transport objects.
    fn connection_index(&self, index: Index) -> RuntimeResult<Index> {
        Ok(index)
    }

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
                    sessions.reschedule_old(session_id);
                    break;
                }
            }
        }
        let pending_len = total_len.saturating_sub(batch_offset);
        let descheduled = params.flags.contains(TransportSendFlags::DESCHED)
            && !params.flags.contains(TransportSendFlags::POSTPONE);
        if pending_len != 0 && !(params.snd_space == 0 && descheduled) {
            sessions.reschedule_old(session_id);
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
    sessions.poll_session_events()?;
    let step = dispatch_session_queue_events(
        runtime,
        sessions,
        transport,
        output_next,
        frame,
        output,
        now,
    )?;
    sessions.update_state(runtime, output.io_count())?;
    Ok(step)
}

pub fn dispatch_session_queue_events<T, Index>(
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
    let mut control_events = core::mem::take(&mut sessions.control_events);
    std::iter::from_fn(|| control_events.pop_front()).try_for_each(
        |event| -> RuntimeResult<()> {
            if matches!(
                event.evt_type,
                SessionEvtType::Close | SessionEvtType::HalfClose
            ) && event.worker_index() != sessions.worker.slot() as u32
            {
                return Ok(());
            }
            let Some(index) = sessions.entries.index_at_slot(event.session_index()) else {
                return Ok(());
            };
            let session_id = SessionId::from(index);
            match event.evt_type {
                SessionEvtType::Close => {
                    let lower_session = sessions
                        .entries
                        .get(session_id.pool_index())
                        .and_then(|entry| entry.lower_session);
                    if let Some(lower) = lower_session {
                        sessions.schedule_disconnect(lower);
                        return Ok(());
                    }
                    let session_transport = sessions.session_transport(session_id);
                    if session_transport.is_some_and(|(transport_id, _)| transport_id != T::ID) {
                        sessions.control_events.push_back(event);
                        return Ok(());
                    }
                    if !sessions.close_transport_session(session_id)? {
                        return Ok(());
                    }
                    if let Some((transport_id, index)) = session_transport
                        && transport_id == T::ID
                    {
                        transport.disconnect(
                            sessions,
                            index,
                            runtime,
                            output_next,
                            frame,
                            output,
                            now,
                        )?;
                    }
                }
                SessionEvtType::HalfClose => {
                    let lower_session = sessions
                        .entries
                        .get(session_id.pool_index())
                        .and_then(|entry| entry.lower_session);
                    if let Some(lower) = lower_session {
                        sessions.schedule_half_close(lower);
                        return Ok(());
                    }
                    let session_transport = sessions.session_transport(session_id);
                    if session_transport.is_some_and(|(transport_id, _)| transport_id != T::ID) {
                        sessions.control_events.push_back(event);
                        return Ok(());
                    }
                    if let Some((transport_id, index)) = session_transport
                        && transport_id == T::ID
                    {
                        transport.disconnect(
                            sessions,
                            index,
                            runtime,
                            output_next,
                            frame,
                            output,
                            now,
                        )?;
                    }
                }
                SessionEvtType::Connect => {
                    sessions.dispatch_application(session_id, event.evt_type)?;
                }
                _ => {
                    sessions.notify_application_event(session_id, event.evt_type)?;
                }
            }
            Ok(())
        },
    )?;

    let mut scheduled_sessions = 0usize;
    let mut new_io_events = core::mem::take(&mut sessions.new_io_events);
    std::iter::from_fn(|| {
        if output.remaining_io_budget() == 0 {
            return None;
        }
        let event = new_io_events.pop_front()?;
        Some(
            dispatch_io_event(
                sessions,
                transport,
                runtime,
                output_next,
                frame,
                output,
                now,
                event,
                &mut scheduled_sessions,
            )
            .map(|accepted| {
                if !accepted {
                    sessions.new_io_events.push_back(event);
                }
            }),
        )
    })
    .try_for_each(|result| result)?;
    std::iter::from_fn(|| new_io_events.pop_back())
        .for_each(|event| sessions.new_io_events.push_front(event));

    let mut old_io_events = core::mem::take(&mut sessions.old_io_events);
    std::iter::from_fn(|| {
        if output.remaining_io_budget() == 0 {
            return None;
        }
        let event = old_io_events.pop_front()?;
        Some(
            dispatch_io_event(
                sessions,
                transport,
                runtime,
                output_next,
                frame,
                output,
                now,
                event,
                &mut scheduled_sessions,
            )
            .map(|accepted| {
                if !accepted {
                    sessions.old_io_events.push_back(event);
                }
            }),
        )
    })
    .try_for_each(|result| result)?;
    std::iter::from_fn(|| old_io_events.pop_back())
        .for_each(|event| sessions.old_io_events.push_front(event));
    Ok(SessionQueueStep { scheduled_sessions })
}

fn dispatch_io_event<T, Index>(
    sessions: &mut SessionWorker<Index>,
    transport: &mut T,
    runtime: &DataPlaneRuntime,
    output_next: SessionQueueNext,
    frame: &mut BufferFrame,
    output: &mut crate::session::node::SessionQueueOutput,
    now: Instant,
    event: SessionEvt,
    scheduled_sessions: &mut usize,
) -> RuntimeResult<bool>
where
    T: SessionTransport<Index>,
    Index: Copy + Eq,
{
    let Some(index) = sessions.entries.index_at_slot(event.session_index()) else {
        return Ok(true);
    };
    let session_id = SessionId::from(index);
    match event.evt_type {
        SessionEvtType::RxEnq | SessionEvtType::TxDeq => {
            sessions.dispatch_application(session_id, event.evt_type)?;
        }
        SessionEvtType::RxDeq => {
            let session_type = sessions
                .entries
                .get(session_id.pool_index())
                .and_then(|entry| entry.session_type);
            match session_type {
                Some(SessionType::Transport {
                    transport: transport_id,
                    state,
                }) => {
                    if transport_id != T::ID {
                        return Ok(false);
                    }
                    let Some(index) = state.transport_index() else {
                        return Ok(true);
                    };
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
                    if request_notification {
                        sessions.request_rx_dequeue_notification(session_id);
                    }
                }
                None => {
                    sessions.dispatch_session_type(session_id, event.evt_type)?;
                }
            }
        }
        SessionEvtType::TxEnq | SessionEvtType::ProtocolOutput => {
            let Some((transport_id, index)) = sessions.session_transport(session_id) else {
                sessions.dispatch_session_type(session_id, event.evt_type)?;
                return Ok(true);
            };
            if transport_id != T::ID {
                return Ok(false);
            }
            if let Some(entry) = sessions.entries.get_mut(session_id.pool_index()) {
                entry.schedule_pending = false;
                entry.tx_fifo.unset_event();
            }
            <T::Tx as SessionTxStrategy<T, Index>>::dispatch(
                transport,
                sessions,
                index,
                session_id,
                runtime,
                output_next,
                frame,
                output,
                now,
            )?;
            *scheduled_sessions += 1;
        }
        _ => {
            sessions.control_events.push_back(event);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use hammer_core::data_plane::{BufferFrame, NodeId, NodeState};
    use hammer_infra::pool::Index;
    use hammer_runtime::app::{AppSessionConfig, SessionEvt, SessionEvtType, SessionMsgQueueError};
    use hammer_runtime::attach::AppServer;
    use hammer_runtime::{
        AttachError, DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine,
        NodeRuntimeData, RuntimeError, RuntimeRegistry, RuntimeResult,
    };

    use super::{
        DEFAULT_SESSION_POOL_CAPACITY, SessionMain, SessionQueueNext, SessionTransportId,
        SessionEntry, SessionState, SessionType, SessionWorker, SessionWorkerState,
        queue_for_worker,
    };
    use crate::session::ApplicationMain;
    use crate::session::SessionId;
    use crate::session::application::{ApplicationError, ApplicationMqResources};
    use crate::session::node::{
        SessionQueueNode, SessionQueueOutput, register_app_session_input_node,
        register_session_queue_node,
    };
    use crate::session::protocol::SessionAppCallbacks;

    static SESSION_APP_RECORDED: AtomicU64 = AtomicU64::new(0);
    static SESSION_LISTEN_BARRIER_SEEN: AtomicBool = AtomicBool::new(false);
    static SESSION_UNLISTEN_BARRIER_SEEN: AtomicBool = AtomicBool::new(false);
    static SESSION_LISTEN_APPLICATION: AtomicU64 = AtomicU64::new(0);
    static SESSION_LISTEN_OPAQUE: AtomicU64 = AtomicU64::new(0);
    static SESSION_LISTEN_ATTEMPTED: AtomicU64 = AtomicU64::new(0);

    fn record_listen_barrier(
        _: hammer_runtime::SessionListenerId,
        application: hammer_runtime::app::ApplicationId,
        opaque: Option<u64>,
        _: hammer_runtime::SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        SESSION_LISTEN_BARRIER_SEEN.store(
            Engine::with_current(|engine| engine.worker_barrier().is_pending()).unwrap_or(false),
            Ordering::SeqCst,
        );
        SESSION_LISTEN_APPLICATION.store(application.raw(), Ordering::SeqCst);
        SESSION_LISTEN_OPAQUE.store(opaque.unwrap_or_default(), Ordering::SeqCst);
        Ok(())
    }

    fn record_unlisten_barrier(_: hammer_runtime::SessionListenerId) -> RuntimeResult<()> {
        SESSION_UNLISTEN_BARRIER_SEEN.store(
            Engine::with_current(|engine| engine.worker_barrier().is_pending()).unwrap_or(false),
            Ordering::SeqCst,
        );
        Ok(())
    }

    fn fail_listen(
        listener: hammer_runtime::SessionListenerId,
        _: hammer_runtime::app::ApplicationId,
        _: Option<u64>,
        _: hammer_runtime::SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        SESSION_LISTEN_ATTEMPTED.store(listener.raw(), Ordering::SeqCst);
        Err(RuntimeError::config_validation(
            "test transport listen failure",
        ))
    }

    fn fail_unlisten(_: hammer_runtime::SessionListenerId) -> RuntimeResult<()> {
        Err(RuntimeError::config_validation(
            "test transport unlisten failure",
        ))
    }

    fn record_session_app_rx(
        _: &mut SessionWorker<Index>,
        session: SessionId,
        context: u64,
    ) -> RuntimeResult<()> {
        SESSION_APP_RECORDED.store(
            (u64::from(session.pool_index().slot()) << 32) | (context & 0xffff_ffff),
            Ordering::SeqCst,
        );
        Ok(())
    }

    fn record_session_app_connected(
        _: &mut SessionWorker<Index>,
        session: SessionId,
        context: u64,
    ) -> RuntimeResult<()> {
        SESSION_APP_RECORDED.store(
            (u64::from(session.pool_index().slot()) << 32) | (context & 0xffff_ffff),
            Ordering::SeqCst,
        );
        Ok(())
    }

    #[derive(Debug, thiserror::Error)]
    enum SessionTestFailure {
        #[error(transparent)]
        Application(#[from] ApplicationError),
        #[error(transparent)]
        Runtime(#[from] RuntimeError),
        #[error(transparent)]
        EventQueue(#[from] SessionMsgQueueError),
        #[error(transparent)]
        SessionQueue(#[from] crate::session::SessionQueueError),
        #[error(transparent)]
        Conversion(#[from] std::num::TryFromIntError),
    }

    fn test_dispatch(
        _: &DataPlaneRuntime,
        _: &mut SessionWorker<Index>,
        _: NodeRuntimeData,
        _: SessionQueueNext,
        _: Instant,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    #[test]
    fn session_listen_and_unlisten_hold_worker_barrier_around_transport_calls()
    -> Result<(), SessionTestFailure> {
        let mut engine = Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        );
        engine.install_current();
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let application_listener = applications.register_listener(application, None, Some(0x55))?;
        applications.update_listener_opaque(application, application_listener, Some(0x66))?;
        let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));

        SESSION_LISTEN_BARRIER_SEEN.store(false, Ordering::SeqCst);
        SESSION_LISTEN_APPLICATION.store(0, Ordering::SeqCst);
        SESSION_LISTEN_OPAQUE.store(0, Ordering::SeqCst);
        let listener = main.listen(
            application_listener,
            hammer_runtime::SessionTransportRegistration::new(
                "barrier-udp",
                Some(record_listen_barrier),
                Some(record_unlisten_barrier),
                None,
            ),
            hammer_runtime::SessionListenEndpoint::new(
                "127.0.0.1:0".parse().expect("test listen endpoint"),
                DataWorkerId::new(0),
            ),
        )?;
        assert!(SESSION_LISTEN_BARRIER_SEEN.load(Ordering::SeqCst));
        assert_eq!(
            SESSION_LISTEN_APPLICATION.load(Ordering::SeqCst),
            application.raw()
        );
        assert_eq!(SESSION_LISTEN_OPAQUE.load(Ordering::SeqCst), 0x66);

        SESSION_UNLISTEN_BARRIER_SEEN.store(false, Ordering::SeqCst);
        main.unlisten(listener)?;
        assert!(SESSION_UNLISTEN_BARRIER_SEEN.load(Ordering::SeqCst));
        assert!(main.with_listener(listener, |_| ()).is_err());
        assert!(
            applications
                .with_listener(application_listener, |entry| entry.opaque())
                .is_ok()
        );

        applications.remove_listener(application, application_listener)?;
        assert!(
            applications
                .with_listener(application_listener, |_| ())
                .is_err()
        );

        Engine::uninstall_current();
        Ok(())
    }

    #[test]
    fn session_listen_failure_removes_only_session_listener() -> Result<(), SessionTestFailure> {
        let mut engine = Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        );
        engine.install_current();
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let application_listener = applications.register_listener(application, None, Some(0x77))?;
        let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));

        SESSION_LISTEN_ATTEMPTED.store(0, Ordering::SeqCst);
        assert!(
            main.listen(
                application_listener,
                hammer_runtime::SessionTransportRegistration::new(
                    "listen-failure",
                    Some(fail_listen),
                    None,
                    None,
                ),
                hammer_runtime::SessionListenEndpoint::new(
                    "127.0.0.1:0".parse().expect("test listen endpoint"),
                    DataWorkerId::new(0),
                ),
            )
            .is_err()
        );

        let attempted = hammer_runtime::SessionListenerId::from_raw(
            SESSION_LISTEN_ATTEMPTED.load(Ordering::SeqCst),
        );
        assert_ne!(attempted.raw(), 0);
        assert!(main.with_listener(attempted, |_| ()).is_err());
        assert_eq!(
            applications.with_listener(application_listener, |entry| entry.opaque())?,
            Some(0x77)
        );

        applications.remove_listener(application, application_listener)?;
        Engine::uninstall_current();
        Ok(())
    }

    #[test]
    fn session_unlisten_failure_keeps_session_and_application_listeners()
    -> Result<(), SessionTestFailure> {
        let mut engine = Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        );
        engine.install_current();
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let application_listener = applications.register_listener(application, None, Some(0x88))?;
        let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let listener = main.listen(
            application_listener,
            hammer_runtime::SessionTransportRegistration::new(
                "unlisten-failure",
                Some(record_listen_barrier),
                Some(fail_unlisten),
                None,
            ),
            hammer_runtime::SessionListenEndpoint::new(
                "127.0.0.1:0".parse().expect("test listen endpoint"),
                DataWorkerId::new(0),
            ),
        )?;

        assert!(main.unlisten(listener).is_err());
        assert!(main.with_listener(listener, |_| ()).is_ok());
        assert_eq!(
            applications.with_listener(application_listener, |entry| entry.opaque())?,
            Some(0x88)
        );

        Engine::uninstall_current();
        Ok(())
    }

    #[test]
    fn session_worker_state_transitions_match_vpp_deadlines() {
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            ApplicationMain::new(DEFAULT_SESSION_POOL_CAPACITY),
            None,
        )
        .expect("session worker");

        assert_eq!(sessions.state(), SessionWorkerState::Polling);
        assert_eq!(sessions.state_deadline(), None);

        sessions
            .update_state(&runtime, 0)
            .expect("polling to interrupt");
        assert_eq!(sessions.state(), SessionWorkerState::Interrupt);
        assert_eq!(sessions.state_deadline(), Some(Duration::from_millis(1)));

        sessions
            .update_state(&runtime, 0)
            .expect("interrupt to idle");
        assert_eq!(sessions.state(), SessionWorkerState::Idle);
        assert_eq!(sessions.state_deadline(), Some(Duration::from_millis(100)));

        sessions
            .new_io_events
            .push_back(SessionEvt::io(0, SessionEvtType::TxEnq));
        sessions
            .update_state(&runtime, 0)
            .expect("idle to interrupt");
        assert_eq!(sessions.state(), SessionWorkerState::Interrupt);
    }

    #[test]
    fn transport_closing_wakes_session_queue_for_external_app() -> Result<(), SessionTestFailure> {
        let mut runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
        let session_queue = register_session_queue_node(&runtime)?;
        runtime
            .nodes()
            .set_node_state(session_queue, NodeState::Interrupt)?;

        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let queue = queue_for_worker(
            &ApplicationMqResources::create_local(application, 1, 128)?,
            application,
        )?;
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_app_mq(application, queue, NodeId::new(0), &mut runtime)?;
        sessions.install_state_deadline(&runtime, session_queue)?;

        let session_id = sessions.construct_stream_sessions(
            SessionTransportId::new(1),
            Index::new(7, 1),
            7,
            application,
            None,
            None,
            None,
            true,
        )?;
        sessions.connection_published(session_id)?;
        sessions.connected(session_id)?;
        sessions.notify_transport_closing(&runtime, session_id, Index::new(7, 1))?;

        assert!(!runtime.set_node_interrupt_pending(session_queue)?);
        let app_session = sessions
            .app_session(session_id)
            .expect("external App Session remains published");
        let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
        let event_count = app_session.poll_events(&mut events);
        assert!(
            events[..event_count]
                .iter()
                .any(|event| event.evt_type == SessionEvtType::Disconnected)
        );
        Ok(())
    }

    #[test]
    fn external_plain_transport_routes_bytes_through_one_app_session()
    -> Result<(), SessionTestFailure> {
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let queue = queue_for_worker(
            &ApplicationMqResources::create_local(application, 1, 128)?,
            application,
        )?;
        let mut runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(1, 0)?;
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::new(64, 16),
            DEFAULT_SESSION_POOL_CAPACITY,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_app_mq(application, queue, NodeId::new(0), &mut runtime)?;
        let session_id = sessions.construct_stream_sessions(
            SessionTransportId::new(1),
            Index::new(7, 1),
            7,
            application,
            None,
            None,
            None,
            true,
        )?;
        let app_session = sessions
            .app_session(session_id)
            .cloned()
            .expect("plain transport owns the external App Session");
        let (transport_rx, transport_tx) = {
            let (rx_fifo, tx_fifo) = sessions
                .session_fifos(session_id)
                .expect("plain transport Session FIFOs");
            (Arc::clone(rx_fifo), Arc::clone(tx_fifo))
        };
        assert!(Arc::ptr_eq(&transport_rx, app_session.rx_fifo()));
        assert!(Arc::ptr_eq(&transport_tx, app_session.tx_fifo()));

        let ingress = runtime
            .alloc_index_with_bytes(b"request")
            .expect("transport RX buffer");
        sessions.enqueue_rx(&runtime.buffers(), session_id, ingress, 0, false)?;
        let mut received = [0_u8; 7];
        assert_eq!(app_session.recv_bytes(&mut received), received.len());
        assert_eq!(&received, b"request");
        assert_eq!(app_session.consume_rx(received.len()), received.len());

        app_session.send_bytes(b"reply").expect("application TX");
        let mut transmitted = [0_u8; 5];
        assert_eq!(transport_tx.peek(0, transmitted.len(), &mut transmitted), 5);
        assert_eq!(&transmitted, b"reply");
        Ok(())
    }

    #[test]
    fn session_worker_teardown_removes_deadline_before_runtime_drop() {
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            ApplicationMain::new(DEFAULT_SESSION_POOL_CAPACITY),
            None,
        )
        .expect("session worker");
        sessions
            .install_state_deadline(&runtime, NodeId::new(0))
            .expect("install adaptive deadline");
        let deadline = sessions
            .state_deadline_file
            .expect("Session Worker owns its deadline index");
        assert_eq!(
            runtime
                .file_main()
                .deadline(deadline)
                .expect("registered deadline"),
            None
        );

        sessions
            .remove_state_deadline(&runtime)
            .expect("remove adaptive deadline before runtime teardown");
        assert!(sessions.state_deadline_file.is_none());
        assert!(runtime.file_main().deadline(deadline).is_err());
    }

    #[test]
    fn appsl_rx_mq_wakes_session_queue_only_in_interrupt_state() -> Result<(), SessionTestFailure> {
        fn run_case(interrupt: bool) -> Result<usize, SessionTestFailure> {
            let main_engine = Engine::new(
                DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
                RuntimeRegistry::new(),
            );
            let session_queue = register_session_queue_node(&main_engine.runtime)?;
            let app_session_input = register_app_session_input_node(&main_engine.runtime)?;
            let mut engine = main_engine.spawn(1)?;
            let applications = ApplicationMain::new(1);
            let application = applications.attach()?;
            let resources = ApplicationMqResources::create_local(application, 1, 4096)?;
            let queue = queue_for_worker(&resources, application)?;
            let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
            let sessions = SessionWorker::<Index>::new(
                DataWorkerId::new(0),
                1,
                AppSessionConfig::default(),
                DEFAULT_SESSION_POOL_CAPACITY,
                applications,
                None,
            )?;

            super::install_session_worker(
                &main,
                &mut engine,
                app_session_input,
                session_queue,
                sessions,
            )?;
            main.worker(DataWorkerId::new(0))?
                .with_mut(|sessions| {
                    sessions.install_app_mq(
                        application,
                        queue.clone(),
                        app_session_input,
                        &mut engine.runtime,
                    )
                })
                .map_err(|source| super::SessionQueueError::WorkerAccess { worker: 0, source })??;

            if interrupt {
                main.worker(DataWorkerId::new(0))?
                    .with_mut(|sessions| sessions.update_state(&engine.runtime, 0))
                    .map_err(|source| super::SessionQueueError::WorkerAccess {
                        worker: 0,
                        source,
                    })??;
            }

            queue.enqueue_io(SessionEvt::io(1, SessionEvtType::TxEnq))?;
            let graph = engine.runtime.nodes().clone();
            assert_eq!(engine.file_main_mut().poll(&graph)?, 1);
            engine.runtime.schedule_empty_frame(app_session_input)?;
            let processed = engine.runtime.run_ready_nodes()?;

            main.worker(DataWorkerId::new(0))?
                .with_mut(|sessions| {
                    sessions.remove_app_mq(application, &mut engine.runtime)?;
                    sessions.remove_state_deadline(&engine.runtime)
                })
                .map_err(|source| super::SessionQueueError::WorkerAccess { worker: 0, source })??;
            Ok(processed)
        }

        assert_eq!(run_case(false)?, 1);
        assert_eq!(run_case(true)?, 2);
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
        let queue = ApplicationMqResources::create_local(application, 2, 128)
            .expect("Application MQ resources")
            .queue(DataWorkerId::new(1))
            .expect("worker Application MQ")
            .clone();
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(1),
            2,
            hammer_runtime::app::AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            Some(server.publisher()),
        )
        .expect("Session worker with external App publication");
        let mut runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(2, 1)
            .expect("worker runtime");
        sessions
            .install_app_mq(
                application,
                queue,
                hammer_core::data_plane::NodeId::new(0),
                &mut runtime,
            )
            .expect("install worker Application MQ");

        let first = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(1, 1),
                1,
                application,
                None,
                None,
                None,
                true,
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
                None,
                None,
                None,
                true,
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
    fn missing_worker_application_mq_rolls_back_session_creation() {
        let socket_path = std::path::PathBuf::from(format!(
            "/tmp/hammer-missing-app-mq-{}.sock",
            std::process::id()
        ));
        let socket_path = socket_path.to_str().expect("socket path");
        let server = AppServer::bind(socket_path, 1).expect("bind App server");
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            1,
            applications,
            Some(server.publisher()),
        )
        .expect("Session worker with external App publication");

        let error = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(1, 1),
                1,
                application,
                None,
                None,
                None,
                true,
            )
            .expect_err("missing worker Application MQ rejects Session creation");
        assert!(matches!(
            &error,
            RuntimeError::Subsystem { source, .. }
                if matches!(
                    source.downcast_ref::<crate::session::SessionQueueError>(),
                    Some(crate::session::SessionQueueError::ApplicationMqMissing {
                        application: missing,
                    }) if *missing == application
                )
        ));

        let queue = ApplicationMqResources::create_local(application, 1, 128)
            .expect("Application MQ resources")
            .queue(DataWorkerId::new(0))
            .expect("worker Application MQ")
            .clone();
        let mut runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime");
        sessions
            .install_app_mq(
                application,
                queue,
                hammer_core::data_plane::NodeId::new(0),
                &mut runtime,
            )
            .expect("install worker Application MQ");

        sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(2, 1),
                2,
                application,
                None,
                None,
                None,
                true,
            )
            .expect("failed creation released Session capacity");
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
        let queue = ApplicationMqResources::create_local(application, 3, 128)
            .expect("Application MQ resources")
            .queue(DataWorkerId::new(2))
            .expect("worker Application MQ")
            .clone();
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(2),
            3,
            hammer_runtime::app::AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            Some(publisher),
        )
        .expect("Session worker with closed external App publication");
        let mut runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(3, 2)
            .expect("worker runtime");
        sessions
            .install_app_mq(
                application,
                queue,
                hammer_core::data_plane::NodeId::new(0),
                &mut runtime,
            )
            .expect("install worker Application MQ");
        let connection_index = Index::new(3, 1);
        let session_id = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                connection_index,
                3,
                application,
                None,
                None,
                None,
                true,
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
        let applications_a = ApplicationMain::new(1);
        let applications_b = ApplicationMain::new(1);
        let main_a = Arc::new(SessionMain::new(1, Arc::clone(&applications_a)));
        let main_b = Arc::new(SessionMain::new(1, Arc::clone(&applications_b)));
        assert!(
            main_a
                .worker(worker)
                .expect("Session worker A slot")
                .install(
                    SessionWorker::<Index>::new(
                        worker,
                        1,
                        AppSessionConfig::default(),
                        DEFAULT_SESSION_POOL_CAPACITY,
                        applications_a,
                        None,
                    )
                    .expect("Session worker A"),
                )
                .is_ok(),
            "install Session worker A"
        );
        assert!(
            main_b
                .worker(worker)
                .expect("Session worker B slot")
                .install(
                    SessionWorker::<Index>::new(
                        worker,
                        1,
                        AppSessionConfig::default(),
                        DEFAULT_SESSION_POOL_CAPACITY,
                        applications_b,
                        None,
                    )
                    .expect("Session worker B"),
                )
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
            test_dispatch,
        )
        .expect("install dispatch A");
        SessionQueueNode::install_worker_attachment(
            &runtime_a,
            data_a,
            SessionQueueNext::from_slot(1),
            test_dispatch,
            test_dispatch,
        )
        .expect("install second dispatch A");
        SessionQueueNode::install_worker_attachment(
            &runtime_b,
            data_b,
            SessionQueueNext::from_slot(2),
            test_dispatch,
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
        let applications = ApplicationMain::new(1);
        let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        assert!(
            main.worker(worker)
                .expect("Session worker slot")
                .install(
                    SessionWorker::<Index>::new(
                        worker,
                        1,
                        AppSessionConfig::default(),
                        DEFAULT_SESSION_POOL_CAPACITY,
                        Arc::clone(&applications),
                        None,
                    )
                    .expect("Session worker"),
                )
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
            test_dispatch,
        )
        .expect("install dispatch");

        let duplicate = SessionWorker::<Index>::new(
            worker,
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )
        .expect("duplicate Session worker");
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

    #[test]
    fn session_worker_uses_configured_pool_capacity() {
        let worker = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            7,
            ApplicationMain::new(7),
            None,
        )
        .expect("Session worker with configured capacity");

        assert_eq!(worker.entries.capacity(), 7);
    }

    #[test]
    fn local_per_app_mq_event_is_drained_by_owning_worker() {
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach local Application");
        let queue = ApplicationMqResources::create_local(application, 1, 128)
            .expect("Application MQ resources")
            .queue(DataWorkerId::new(0))
            .expect("per-Application MQ")
            .clone();
        let mut runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications.clone(),
            None,
        )
        .expect("Session worker");
        sessions
            .install_app_mq(
                application,
                queue.clone(),
                hammer_core::data_plane::NodeId::new(0),
                &mut runtime,
            )
            .expect("install per-Application MQ");

        queue
            .enqueue_ctrl(SessionEvt::ctrl(1, 0, SessionEvtType::Close))
            .expect("enqueue app-to-session event");
        assert!(sessions.mark_app_mq_pending(application));

        let handled = sessions.poll_app().expect("drain per-Application MQ");
        assert_eq!(handled, 1);
    }

    #[test]
    fn app_mq_pending_is_idempotent_and_requires_non_empty_queue() {
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach local Application");
        let queue = ApplicationMqResources::create_local(application, 1, 128)
            .expect("Application MQ resources")
            .queue(DataWorkerId::new(0))
            .expect("per-Application MQ")
            .clone();
        let mut runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default())
            .for_worker(1, 0)
            .expect("worker runtime");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications.clone(),
            None,
        )
        .expect("Session worker");
        sessions
            .install_app_mq(
                application,
                queue.clone(),
                hammer_core::data_plane::NodeId::new(0),
                &mut runtime,
            )
            .expect("install per-Application MQ");

        assert!(!sessions.mark_app_mq_pending(application));
        queue
            .enqueue_io(SessionEvt::io(1, SessionEvtType::TxEnq))
            .expect("enqueue app-to-session event");
        assert!(sessions.mark_app_mq_pending(application));
        assert!(!sessions.mark_app_mq_pending(application));

        assert_eq!(
            sessions
                .drain_app_mq(application)
                .expect("drain pending Application MQ"),
            1
        );
        assert!(!sessions.mark_app_mq_pending(application));
    }

    #[test]
    fn migrated_session_accept_reschedules_existing_fifo_events()
    -> Result<(), SessionTestFailure> {
        let applications = ApplicationMain::new(1);
        let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )?;
        sessions.set_listener_main(main);
        let (rx_fifo, tx_fifo) = sessions.create_local_fifos()?;
        let session_id = sessions.insert_session_entry(SessionEntry::creating_transport(
            SessionTransportId::new(2),
            rx_fifo.clone(),
            tx_fifo.clone(),
        ))?;
        let transport_index = Index::new(7, 1);
        sessions.finish_transport_creation(session_id, transport_index);
        assert!(sessions.connection_published(session_id)?);

        assert_eq!(tx_fifo.enqueue(b"tx"), 2);
        assert_eq!(rx_fifo.enqueue(b"rx"), 2);
        assert!(rx_fifo.set_event());

        sessions.accept_migrated_session(session_id)?;

        assert!(matches!(
            sessions
                .entries
                .get(session_id.pool_index())
                .and_then(|entry| entry.session_type),
            Some(SessionType::Transport {
                state: SessionState::Active(_),
                ..
            })
        ));
        assert!(sessions.new_io_events.iter().any(|event| {
            event.session_index() == session_id.pool_index().slot()
                && event.evt_type == SessionEvtType::TxEnq
        }));
        assert!(sessions.new_io_events.iter().any(|event| {
            event.session_index() == session_id.pool_index().slot()
                && event.evt_type == SessionEvtType::RxEnq
        }));
        Ok(())
    }

    #[test]
    fn file_readiness_dispatches_postponed_application_events_through_session_queue()
    -> Result<(), SessionTestFailure> {
        let main_engine = Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        );
        let session_queue = register_session_queue_node(&main_engine.runtime)?;
        let app_session_input = register_app_session_input_node(&main_engine.runtime)?;
        let mut engine = main_engine.spawn(1)?;
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let queue = queue_for_worker(
            &ApplicationMqResources::create_local(application, 1, 4096)?,
            application,
        )?;
        let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )?;
        super::install_session_worker(
            &main,
            &mut engine,
            app_session_input,
            session_queue,
            sessions,
        )?;
        main.worker(DataWorkerId::new(0))?
            .with_mut(|sessions| {
                sessions.install_app_mq(
                    application,
                    queue.clone(),
                    app_session_input,
                    &mut engine.runtime,
                )
            })
            .map_err(|source| super::SessionQueueError::WorkerAccess { worker: 0, source })??;
        let target = main.with_worker_mut(&engine.runtime, |sessions| {
            let target = sessions.construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(7, 1),
                7,
                application,
                None,
                None,
                None,
                true,
            )?;
            sessions.connection_published(target)?;
            sessions.connected(target)?;
            Ok(target)
        })?;
        engine
            .runtime
            .nodes()
            .set_node_state(session_queue, hammer_core::data_plane::NodeState::Interrupt)?;

        queue.enqueue_io(SessionEvt::io(
            target.pool_index().slot(),
            SessionEvtType::TxEnq,
        ))?;
        (0..super::DEFAULT_SESSION_EVENT_CAPACITY)
            .try_for_each(|_| queue.enqueue_io(SessionEvt::io(u32::MAX, SessionEvtType::TxEnq)))?;

        let graph = engine.runtime.nodes().clone();
        assert_eq!(engine.file_main_mut().poll(&graph)?, 1);
        let _ = engine.file_main_mut().poll(&graph)?;
        assert_eq!(
            main.with_worker_mut(&engine.runtime, |sessions| {
                Ok(sessions
                    .app_rx_mq_pending
                    .iter()
                    .filter(|candidate| **candidate == application)
                    .count())
            })?,
            1
        );

        engine.runtime.schedule_empty_frame(app_session_input)?;
        engine.runtime.run_ready_nodes()?;
        main.with_worker_mut(&engine.runtime, |sessions| {
            assert!(!sessions.has_pending_app_mqs());
            assert!(
                sessions
                    .new_io_events
                    .iter()
                    .any(|event| event.session_index() == target.pool_index().slot())
            );
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn app_mq_drain_uses_snapshot_and_readds_postponed_work() -> Result<(), SessionTestFailure> {
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let queue = queue_for_worker(
            &ApplicationMqResources::create_local(application, 1, 128)?,
            application,
        )?;
        let mut runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(1, 0)?;
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications.clone(),
            None,
        )?;
        sessions.install_app_mq(
            application,
            queue.clone(),
            hammer_core::data_plane::NodeId::new(0),
            &mut runtime,
        )?;
        queue.enqueue_io(SessionEvt::io(1, SessionEvtType::TxEnq))?;
        assert!(sessions.mark_app_mq_pending(application));

        let mut dispatched = 0usize;
        let handled = sessions.drain_app_mq_events_to(|_, _| {
            dispatched += 1;
            if dispatched == 1 {
                queue
                    .enqueue_io(SessionEvt::io(1, SessionEvtType::TxDeq))
                    .expect("append event after the MQ snapshot");
            }
        });

        assert_eq!(handled, 1);
        assert_eq!(dispatched, 1);
        assert!(sessions.app_rx_mq_pending.contains(&application));
        assert!(sessions.has_pending_app_mqs());
        Ok(())
    }

    #[test]
    fn application_mq_event_bypasses_full_session_event_queue() -> Result<(), SessionTestFailure> {
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let queue = queue_for_worker(
            &ApplicationMqResources::create_local(application, 1, 128)?,
            application,
        )?;
        let mut runtime =
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()).for_worker(1, 0)?;
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_app_mq(
            application,
            queue.clone(),
            hammer_core::data_plane::NodeId::new(0),
            &mut runtime,
        )?;
        let target = sessions.construct_stream_sessions(
            SessionTransportId::new(1),
            Index::new(7, 1),
            7,
            application,
            None,
            None,
            None,
            true,
        )?;
        sessions.connection_published(target)?;
        sessions.connected(target)?;

        (0..super::DEFAULT_SESSION_EVENT_CAPACITY).try_for_each(|session_index| {
            sessions.session_evt_q.enqueue_io(SessionEvt::io(
                session_index as u32 + 1,
                SessionEvtType::TxEnq,
            ))
        })?;
        let event = SessionEvt::io(target.pool_index().slot(), SessionEvtType::TxEnq);
        queue.enqueue_io(event)?;
        assert!(sessions.mark_app_mq_pending(application));

        assert_eq!(sessions.poll_app()?, 1);
        assert_eq!(
            sessions.session_evt_q.len(),
            super::DEFAULT_SESSION_EVENT_CAPACITY
        );
        assert!(!sessions.has_pending_app_mqs());
        assert!(
            sessions
                .new_io_events
                .iter()
                .any(|candidate| *candidate == event)
        );
        Ok(())
    }

    #[test]
    fn session_app_rx_dispatches_exact_session_and_opaque_context() {
        SESSION_APP_RECORDED.store(0, Ordering::SeqCst);
        let applications = ApplicationMain::new(4);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            Arc::clone(&applications),
            None,
        )
        .expect("Session worker");
        sessions
            .install_application_mq_for_test(application)
            .expect("install test Application MQ");
        let session_id = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(9, 1),
                1,
                application,
                Some(hammer_runtime::app::SessionAppId::new(0)),
                Some(99),
                None,
                false,
            )
            .expect("construct Session App session");
        sessions
            .set_app_session(session_id, 0xABCD)
            .expect("set opaque context");
        sessions
            .install_session_app(
                hammer_runtime::app::SessionAppId::new(0),
                SessionAppCallbacks {
                    builtin_rx: Some(record_session_app_rx),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");

        sessions
            .dispatch_application(session_id, SessionEvtType::RxEnq)
            .expect("dispatch exact Session App RX event");
        assert_eq!(
            SESSION_APP_RECORDED.load(Ordering::SeqCst),
            (u64::from(session_id.pool_index().slot()) << 32) | 0xABCD
        );
    }

    #[test]
    fn session_app_connected_dispatches_exact_opaque_context() {
        SESSION_APP_RECORDED.store(0, Ordering::SeqCst);
        let applications = ApplicationMain::new(4);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )
        .expect("Session worker");
        sessions
            .install_application_mq_for_test(application)
            .expect("install test Application MQ");
        let session_id = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(11, 1),
                1,
                application,
                Some(hammer_runtime::app::SessionAppId::new(0)),
                None,
                None,
                false,
            )
            .expect("construct Session App session");
        sessions
            .set_app_session(session_id, 0x1234)
            .expect("set opaque context");
        sessions
            .install_session_app(
                hammer_runtime::app::SessionAppId::new(0),
                SessionAppCallbacks {
                    connected: Some(record_session_app_connected),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");

        sessions
            .dispatch_application(session_id, SessionEvtType::Connect)
            .expect("dispatch exact Session App connected event");
        assert_eq!(
            SESSION_APP_RECORDED.load(Ordering::SeqCst),
            (u64::from(session_id.pool_index().slot()) << 32) | 0x1234
        );
    }

    #[test]
    fn session_app_callback_publishes_upper_app_session_and_lower_teardown_removes_it() {
        let applications = ApplicationMain::new(4);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )
        .expect("Session worker");
        sessions
            .install_application_mq_for_test(application)
            .expect("install test Application MQ");
        let lower = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(1, 1),
                1,
                application,
                Some(hammer_runtime::app::SessionAppId::new(0)),
                None,
                None,
                false,
            )
            .expect("construct Session App session");

        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish upper Session from callback");
        assert!(sessions.app_session(upper).is_some());
        assert_eq!(
            sessions
                .entries
                .get(upper.pool_index())
                .and_then(|entry| entry.lower_session),
            Some(lower)
        );

        sessions
            .remove_session(lower)
            .expect("remove lower Session");
        assert!(!sessions.has_session(upper));
    }

    #[test]
    fn upper_transport_session_publishes_connect_without_session_app_callback() {
        static SESSION_APP_CONNECTED_CALLS: AtomicU64 = AtomicU64::new(0);

        fn record_connected(
            _: &mut SessionWorker<Index>,
            _: SessionId,
            _: u64,
        ) -> RuntimeResult<()> {
            SESSION_APP_CONNECTED_CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        SESSION_APP_CONNECTED_CALLS.store(0, Ordering::SeqCst);
        let applications = ApplicationMain::new(4);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )
        .expect("Session worker");
        sessions
            .install_application_mq_for_test(application)
            .expect("install test Application MQ");
        sessions
            .install_session_app(
                hammer_runtime::app::SessionAppId::new(0),
                SessionAppCallbacks {
                    connected: Some(record_connected),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");
        let lower = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(1, 1),
                1,
                application,
                Some(hammer_runtime::app::SessionAppId::new(0)),
                None,
                None,
                false,
            )
            .expect("construct lower Session App session");
        let transport = SessionTransportId::new(3);
        let transport_index = Index::new(7, 1);
        let upper = sessions
            .create_upper_transport_session(lower, transport, transport_index, 0x55)
            .expect("create upper transport Session");

        assert_eq!(
            sessions.session_transport(upper),
            Some((transport, transport_index))
        );
        assert_eq!(
            sessions
                .entries
                .get(upper.pool_index())
                .and_then(|entry| entry.lower_session),
            Some(lower)
        );

        sessions
            .publish_connected_transport_session(upper)
            .expect("publish connected upper transport Session");
        assert_eq!(SESSION_APP_CONNECTED_CALLS.load(Ordering::SeqCst), 0);
        let state =
            sessions
                .entries
                .get(upper.pool_index())
                .and_then(|entry| match entry.session_type {
                    Some(SessionType::Transport { state, .. }) => Some(state),
                    None => None,
                });
        assert!(matches!(
            state,
            Some(crate::session::state::SessionState::Active(_))
        ));
    }

    #[test]
    fn upper_transport_session_teardown_keeps_app_session_until_app_close() {
        let applications = ApplicationMain::new(4);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )
        .expect("Session worker");
        sessions
            .install_application_mq_for_test(application)
            .expect("install test Application MQ");
        let lower = sessions
            .construct_stream_sessions(
                SessionTransportId::new(1),
                Index::new(1, 1),
                1,
                application,
                Some(hammer_runtime::app::SessionAppId::new(0)),
                None,
                None,
                false,
            )
            .expect("construct lower Session App session");
        let transport = SessionTransportId::new(3);
        let transport_index = Index::new(7, 1);
        let upper = sessions
            .create_upper_transport_session(lower, transport, transport_index, 0x55)
            .expect("create upper transport Session");
        sessions
            .publish_connected_transport_session(upper)
            .expect("publish connected upper transport Session");

        sessions
            .notify_transport_closed(upper, transport_index)
            .expect("notify transport close");
        sessions
            .notify_transport_deleted(upper, transport_index)
            .expect("notify transport delete");
        assert!(sessions.has_session(upper));
        sessions
            .close_transport_session(upper)
            .expect("app close completes transport deletion");
        assert!(!sessions.has_session(upper));
    }
}

#[cfg(test)]
fn queue_for_worker(
    resources: &ApplicationMqResources,
    application: ApplicationId,
) -> RuntimeResult<Arc<SessionMsgQueue>> {
    resources
        .queue(DataWorkerId::new(0))
        .cloned()
        .ok_or_else(|| SessionQueueError::ApplicationMqMissing { application }.into())
}

fn enqueue_app_event(
    control_events: &mut LinkedList<SessionEvt>,
    new_io_events: &mut LinkedList<SessionEvt>,
    ring: SessionMqRing,
    event: SessionEvt,
) {
    match ring {
        SessionMqRing::Ctrl => control_events.push_back(event),
        SessionMqRing::Io => new_io_events.push_back(event),
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

fn schedule_session_queue_deadline(
    graph: &NodeRuntime,
    deadline: &mut Deadline,
) -> RuntimeResult<()> {
    let node = NodeId::new(
        u32::try_from(deadline.private_data())
            .expect("Session Queue node identity is stored as a u32"),
    );
    let _ = graph.mark_interrupt_pending(node)?;
    Ok(())
}

fn schedule_app_mq_pending(
    graph: &hammer_runtime::NodeRuntime,
    file: &mut File,
) -> RuntimeResult<()> {
    // SAFETY: the entry is boxed and owned by this worker's SessionWorker for
    // the File lifetime; FileMain deletes this File before the box is dropped.
    let entry = unsafe { &mut *(file.private_data() as usize as *mut AppRxMqEntry) };
    if entry.pending || entry.queue.is_empty() {
        return Ok(());
    }
    entry.pending = true;
    // SAFETY: the pending queue belongs to the same worker that owns this
    // File/entry, and FileMain deletes the File before the queue is dropped.
    let pending_queue = unsafe { &mut *(entry.pending_queue as *mut VecDeque<ApplicationId>) };
    pending_queue.push_back(entry.application);
    let _ = graph.mark_interrupt_pending(entry.appsl_input_node)?;
    Ok(())
}

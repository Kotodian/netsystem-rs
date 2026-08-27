use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::hint::spin_loop;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::os::fd::BorrowedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use crossbeam_queue::ArrayQueue;
use hammer_core::data_plane::{
    BufferFrame, DataPlaneBuffers, Index as BufferIndex, NodeId, NodeState,
};
use hammer_infra::align::{CacheLine, align_up};
use hammer_infra::fifo::Fifo;
use hammer_infra::linked_list::LinkedList;
use hammer_infra::pool::Pool;
use hammer_infra::segment::Segment;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{
    AppSession, AppSessionConfig, AppSessionError, SessionAcceptedMsg, SessionAppContext,
    SessionConnectError, SessionConnectedMsg, SessionControlError, SessionDgramHeader,
    SessionEventQueue, SessionEvt, SessionEvtType, SessionFlags, SessionHandle, SessionMqRing,
    SessionMsgQueue, SessionMsgQueueError,
};
use hammer_runtime::attach::AppSessionPublisher;
use hammer_runtime::session::SessionStreamDirection;
use hammer_runtime::{
    AttachError, RuntimeError, RuntimeResult, SessionConnectEndpoint, SessionListenEndpoint,
    SessionTransportRegistration,
};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Deadline, Engine, File, FileFunctions, NodeRuntime,
    NodeRuntimeData,
};

use crate::session::app::AppWorkerError;
use crate::session::application::{ApplicationMain, ApplicationMqResources};
use crate::session::error::{SessionError, SessionQueueError, SessionTransportActionError};
use crate::session::lookup::SessionEndpointLookup;
use crate::session::node::{AppSessionInputNode, SessionQueueTransportDispatch};
use crate::session::protocol::SessionAppCallbacks;
use crate::session::state::SessionState;
use crate::session::{AppWorker, SessionQueueNext};

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

pub type SessionTuple = (u8, SocketAddr, SocketAddr);

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
    pub tuple: SessionTuple,
    pub dgram: SessionDgramArgs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionSwitchPoolStatus {
    Rejected,
    Prepared,
}

pub struct SessionMigrationState {
    transport: u8,
    rx_fifo: Arc<Fifo>,
    tx_fifo: Arc<Fifo>,
}

pub struct SessionSwitchPoolReply {
    pub old_sh: SessionHandle,
    pub new_sh: Option<SessionHandle>,
    pub old_thread: DataWorkerId,
    pub new_thread: DataWorkerId,
    pub tuple: SessionTuple,
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
    pub tuple: SessionTuple,
    pub status: SessionSwitchPoolCompletionStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionSwitchPoolClosed {
    pub new_sh: SessionHandle,
}

struct SessionMigrateQueues {
    session_migrate_requests: Box<[ArrayQueue<SessionSwitchPoolArgs>]>,
    session_switch_pool_replies: Box<[ArrayQueue<SessionSwitchPoolReply>]>,
    session_switch_pool_completions: Box<[ArrayQueue<SessionSwitchPoolCompletion>]>,
    session_switch_pool_closed: Box<[ArrayQueue<SessionSwitchPoolClosed>]>,
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
            session_switch_pool_closed: (0..worker_count)
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

    #[inline]
    fn push_session_switch_pool_closed(
        &self,
        closed: SessionSwitchPoolClosed,
    ) -> Result<(), SessionSwitchPoolClosed> {
        let Some(queue) = self
            .session_switch_pool_closed
            .get(closed.new_sh.thread_index as usize)
        else {
            return Err(closed);
        };
        queue.push(closed)
    }

    #[inline]
    fn pop_session_switch_pool_closed(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolClosed> {
        self.session_switch_pool_closed
            .get(worker.slot())
            .and_then(ArrayQueue::pop)
    }
}

#[derive(Clone, Copy)]
enum SessionType<Index> {
    Transport {
        transport: u8,
        state: SessionState<Index>,
    },
}

#[derive(Clone, Copy)]
enum SessionApplication {
    External(u32),
}

struct SessionEntry<Index> {
    session_type: Option<SessionType<Index>>,
    application: Option<SessionApplication>,
    owner_application: Option<u32>,
    app: Option<u32>,
    app_session: SessionAppContext,
    app_opaque: Option<u64>,
    server_name: Option<String>,
    application_connection: Option<u32>,
    accepted: bool,
    listener: Option<SessionHandle>,
    flags: SessionFlags,
    lower_session: Option<u32>,
    upper_session: Option<u32>,
    parent_session: Option<u32>,
    rx_fifo: Arc<Fifo>,
    tx_fifo: Arc<Fifo>,
    schedule_pending: bool,
}

impl<Index: Copy + Eq> SessionEntry<Index> {
    #[inline]
    fn creating_transport(transport: u8, rx_fifo: Arc<Fifo>, tx_fifo: Arc<Fifo>) -> Self {
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
            application_connection: None,
            accepted: false,
            listener: None,
            flags: SessionFlags::empty(),
            lower_session: None,
            upper_session: None,
            parent_session: None,
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
            application_connection: None,
            accepted: false,
            listener: None,
            flags: SessionFlags::empty(),
            lower_session: None,
            upper_session: None,
            parent_session: None,
            rx_fifo,
            tx_fifo,
            schedule_pending: false,
        }
    }

    /// The connection endpoint role fixed by the Session's construction
    /// lifecycle: `accepted` names Sessions that arrived through a listener,
    /// the outbound connect paths construct with it unset. Per-entry and
    /// stable; never inferred from stream direction or app identity.
    #[inline]
    fn endpoint_role(&self) -> SessionEndpointRole {
        if self.accepted {
            SessionEndpointRole::Server
        } else {
            SessionEndpointRole::Client
        }
    }
}

/// Endpoint role of a Session connection, fixed at creation by the
/// listener/connect lifecycle.
///
/// VPP carries no `SESSION_F_IS_SERVER` session flag: server-ness is the
/// HTTP connection flag `HTTP_CONN_F_IS_SERVER`, set on the connection
/// accepted from a listener (http.c:1438) and absent from client
/// connections created by connect. Hammer records the same fact as the
/// Session entry's `accepted` lifecycle field; this enum is the typed
/// projection of that field. Stream children read their parent
/// connection's projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEndpointRole {
    /// The Session arrived through a listener (accepted connection).
    Server,
    /// The Session is an outbound connect (client) connection.
    Client,
}

/// Immutable accept-time metadata snapshot for one Session, read by a
/// builtin Session App accept callback without touching the worker again.
///
/// Static `Copy` data: [`SessionFlags`], the connection endpoint role, and,
/// for stream children, the parent connection's [`SessionAppContext`]
/// resolved from the child's pinned listener handle (VPP
/// `http_ts_accept_stream`, http.c:675: `conn_session =
/// session_get_from_handle (stream_session->listener_handle)`, parent context
/// `conn_session->opaque`). Root Sessions carry the role fixed by their own
/// construction lifecycle and no parent context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionAcceptMetadata {
    /// The flags a transport derived for the accepted Session
    /// (`set_session_flags`; VPP `SESSION_F_*`).
    pub flags: SessionFlags,
    /// The Session's endpoint role: `Server` for connections accepted from a
    /// listener, `Client` for outbound connects; stream children inherit the
    /// parent connection's role, `None` when the parent is absent, foreign,
    /// or no longer live (like [`Self::parent_app_context`]).
    pub role: Option<SessionEndpointRole>,
    /// The parent Session's `SessionAppContext`, `None` for roots and for
    /// streams whose pinned listener handle is absent, foreign, or no longer
    /// live.
    pub parent_app_context: Option<SessionAppContext>,
}

/// Static worker-local action table one transport installs for stream
/// operations dispatched by the owning Session Worker.
///
/// Mirrors VPP's transport VFT (`transport_close`, `transport_reset`,
/// `transport_half_close`, transport.h:63-70) and the application protocol
/// error attribute (`app_proto_err_code`, transport_types.h:447): the
/// transport implements the callbacks and receives the worker itself, so the
/// callbacks need no global state. Dispatch copies the table (it is `Copy`)
/// before invoking, keeping the path static, allocation-free and borrow-safe.
#[derive(Debug, Clone, Copy)]
pub struct SessionTransportWorkerActions<Index> {
    open_stream: SessionTransportOpenStream<Index>,
    reset_stream: SessionTransportResetStream<Index>,
    stop_sending: SessionTransportStopSending<Index>,
    close_connection: SessionTransportCloseConnection<Index>,
}

impl<Index> SessionTransportWorkerActions<Index> {
    #[inline]
    pub const fn new(
        open_stream: SessionTransportOpenStream<Index>,
        reset_stream: SessionTransportResetStream<Index>,
        stop_sending: SessionTransportStopSending<Index>,
        close_connection: SessionTransportCloseConnection<Index>,
    ) -> Self {
        Self {
            open_stream,
            reset_stream,
            stop_sending,
            close_connection,
        }
    }
}

/// Opens one stream child of `parent` on the worker; mirrors VPP
/// `vnet_connect_stream (parent_handle)` (application.c:1447). Returns the new
/// stream's Session identity.
pub type SessionTransportOpenStream<Index> = fn(
    &mut SessionWorker<Index>,
    u32,
    SessionStreamDirection,
    SessionAppContext,
) -> RuntimeResult<u32>;
/// Resets one stream with an application error code; mirrors VPP
/// `transport_reset (tp, conn_index, thread)` (transport.h:138).
pub type SessionTransportResetStream<Index> =
    fn(&mut SessionWorker<Index>, u32, u64) -> RuntimeResult<()>;
/// Stops the peer's send direction on one stream; mirrors VPP
/// `transport_half_close (tp, conn_index, thread)` (transport.h:135).
pub type SessionTransportStopSending<Index> =
    fn(&mut SessionWorker<Index>, u32, u64) -> RuntimeResult<()>;
/// Closes one connection with an application error code and raw reason bytes;
/// mirrors VPP `transport_close` plus the `APP_PROTO_ERR_CODE` endpoint
/// attribute (transport_types.h:447, quic.c:701-718).
pub type SessionTransportCloseConnection<Index> =
    fn(&mut SessionWorker<Index>, u32, u64, &[u8]) -> RuntimeResult<()>;

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
    transport_actions: Vec<Option<SessionTransportWorkerActions<Index>>>,
    pub(crate) transport_dispatches: Vec<SessionQueueTransportDispatch>,
    pub(crate) control_events: LinkedList<SessionEvt>,
    pub(crate) new_io_events: LinkedList<SessionEvt>,
    pub(crate) old_io_events: LinkedList<SessionEvt>,
    app_rx_mqs: Vec<Option<Box<AppRxMqEntry>>>,
    app_rx_mq_pending: VecDeque<u32>,
    state: SessionWorkerState,
    state_deadline_file: Option<u32>,
    session_queue: Option<NodeId>,
}

struct AppRxMqEntry {
    application: u32,
    queue: Arc<SessionMsgQueue>,
    file: Option<u32>,
    appsl_input_node: hammer_core::data_plane::NodeId,
    pending_queue: usize,
    pending: bool,
    postponed: bool,
}

impl AppRxMqEntry {
    fn drain_snapshot_to(
        &mut self,
        dispatch_event: &mut impl FnMut(SessionMqRing, SessionEvt),
    ) -> Result<usize, SessionMsgQueueError> {
        let was_postponed = self.postponed;
        if !was_postponed {
            self.queue.drain();
        }
        self.postponed = false;

        let snapshot = self.queue.len();
        let mut dispatched = 0usize;
        for _ in 0..snapshot {
            let Some((ring, event)) = self.queue.dequeue_with_ring()? else {
                break;
            };
            dispatch_event(ring, event);
            dispatched += 1;
        }

        let mut has_work = !self.queue.is_empty();
        if was_postponed && !has_work {
            self.queue.drain();
            has_work = !self.queue.is_empty();
        }
        self.pending = has_work;
        self.postponed = has_work;
        Ok(dispatched)
    }
}

pub struct SessionMain {
    workers: Box<[CacheLine<ThreadOwned<SessionWorker<u32>>>]>,
    owner: ThreadId,
    listeners: UnsafeCell<Pool<SessionListener>>,
    endpoint_lookup: SessionEndpointLookup,
    session_switch_pool_queues: Arc<SessionMigrateQueues>,
    session_migration_shutdown: AtomicBool,
    session_migration_shutdown_workers: AtomicU32,
    session_migration_shutdown_phase: AtomicU32,
    applications: Arc<ApplicationMain>,
}

pub(super) struct SessionListener {
    application: u32,
    application_listener: u32,
    transport: SessionTransportRegistration,
}

impl SessionListener {
    #[inline]
    pub(super) const fn application(&self) -> u32 {
        self.application
    }

    pub(super) const fn application_listener(&self) -> u32 {
        self.application_listener
    }
}

// SAFETY: Main Thread publishes listener state under the worker barrier; Data
// Workers only read the immutable entry selected by their transport callback.
unsafe impl Send for SessionMain {}
// SAFETY: `listeners` mutation is confined to `owner` and synchronized by the
// worker barrier before a Data Worker may observe it.
unsafe impl Sync for SessionMain {}

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
            session_migration_shutdown: AtomicBool::new(false),
            session_migration_shutdown_workers: AtomicU32::new(0),
            session_migration_shutdown_phase: AtomicU32::new(0),
            applications,
        }
    }

    pub fn begin_session_migration_shutdown(&self) {
        self.session_migration_shutdown
            .store(true, Ordering::Release);
    }

    #[inline]
    pub fn session_migration_shutdown(&self) -> bool {
        self.session_migration_shutdown.load(Ordering::Acquire)
    }

    pub fn wait_session_migration_shutdown_phase(&self) {
        let worker_count = self.workers.len() as u32;
        if worker_count == 0 {
            return;
        }
        let phase = self
            .session_migration_shutdown_phase
            .load(Ordering::Acquire);
        let expected = phase.saturating_add(1).saturating_mul(worker_count);
        let arrived = self
            .session_migration_shutdown_workers
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if arrived == expected {
            self.session_migration_shutdown_phase
                .store(phase.saturating_add(1), Ordering::Release);
            return;
        }
        while self
            .session_migration_shutdown_phase
            .load(Ordering::Acquire)
            <= phase
        {
            spin_loop();
        }
    }

    pub(crate) fn add_connection(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: u8,
        handle: SessionHandle,
    ) -> bool {
        self.endpoint_lookup
            .add_connection(local, remote, transport, handle)
    }

    pub(crate) fn del_connection(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: u8,
    ) -> bool {
        self.endpoint_lookup
            .del_connection(local, remote, transport)
    }

    pub(crate) fn replace_connection(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: u8,
        new_handle: SessionHandle,
    ) -> bool {
        if self
            .endpoint_lookup
            .del_connection(local, remote, transport)
        {
            self.endpoint_lookup
                .add_connection(local, remote, transport, new_handle)
        } else {
            false
        }
    }

    pub fn lookup_connection(
        &self,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
        transport: u8,
    ) -> Option<SessionHandle> {
        self.endpoint_lookup
            .lookup_connection(local, remote, transport)
    }

    pub fn program_thread_migration(
        &self,
        runtime: &DataPlaneRuntime,
        target_worker: DataWorkerId,
        old_handle: SessionHandle,
        tuple: SessionTuple,
        dgram: SessionDgramArgs,
    ) -> SessionMigrateResult {
        let (transport, local, remote) = tuple;
        if self.session_migration_shutdown() {
            return SessionMigrateResult::Unavailable;
        }
        let source_worker = DataWorkerId::new(old_handle.thread_index);
        if source_worker == target_worker
            || source_worker.slot() >= self.workers.len()
            || target_worker.slot() >= self.workers.len()
        {
            return SessionMigrateResult::Unavailable;
        }

        if self
            .endpoint_lookup
            .lookup_connection(local, remote, transport)
            != Some(old_handle)
        {
            return SessionMigrateResult::Unavailable;
        }

        let args = SessionSwitchPoolArgs {
            old_sh: old_handle,
            new_sh: None,
            old_thread: source_worker,
            new_thread: target_worker,
            tuple,
            dgram,
        };
        if self
            .session_switch_pool_queues
            .push_session_migrate_request(args)
            .is_err()
        {
            return SessionMigrateResult::QueueFull;
        }
        self.wake_worker(runtime, source_worker);
        SessionMigrateResult::Queued
    }

    pub(crate) fn cancel_migration(&self, tuple: SessionTuple, old_handle: SessionHandle) -> bool {
        let (transport, local, remote) = tuple;
        self.endpoint_lookup
            .lookup_connection(local, remote, transport)
            == Some(old_handle)
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

    pub fn push_session_switch_pool_closed(
        &self,
        runtime: &DataPlaneRuntime,
        closed: SessionSwitchPoolClosed,
    ) -> Result<(), SessionSwitchPoolClosed> {
        let target_worker = DataWorkerId::new(closed.new_sh.thread_index);
        let result = self
            .session_switch_pool_queues
            .push_session_switch_pool_closed(closed);
        if result.is_ok() {
            self.wake_worker(runtime, target_worker);
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

    pub fn pop_session_switch_pool_closed(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolClosed> {
        self.session_switch_pool_queues
            .pop_session_switch_pool_closed(worker)
    }

    pub fn listen(
        &self,
        application_listener: u32,
        transport: SessionTransportRegistration,
        endpoint: SessionListenEndpoint,
    ) -> Result<SessionHandle, SessionError> {
        self.with_control_barrier(|| {
            let (application, opaque) = self
                .applications
                .with_listener(application_listener, |listener| {
                    (listener.application(), listener.opaque())
                })
                .map_err(|source| SessionError::TransportOpFailed {
                    source: source.into(),
                })?;
            let listener = self
                .with_listeners_mut(|listeners| {
                    Ok(SessionHandle::new(
                        listeners.insert(SessionListener {
                            application,
                            application_listener,
                            transport,
                        }),
                        0,
                    ))
                })
                .map_err(|_| SessionError::ListenerControlWrongThread)??;
            let Some(start_listen) = transport.start_listen() else {
                self.with_listeners_mut(|listeners| {
                    drop(listeners.remove(listener.session_index));
                    Ok(())
                })
                .map_err(|_| SessionError::ListenerControlWrongThread)??;
                return Err(SessionError::TransportListenUnsupported {
                    transport: transport.name(),
                });
            };
            if let Err(error) = start_listen(listener, application, opaque, endpoint) {
                self.with_listeners_mut(|listeners| {
                    drop(listeners.remove(listener.session_index));
                    Ok(())
                })
                .map_err(|_| SessionError::ListenerControlWrongThread)??;
                return Err(SessionError::TransportOpFailed { source: error });
            }
            Ok(listener)
        })
        .map_err(|_| SessionError::ListenerControlWrongThread)?
    }

    pub fn unlisten(&self, listener: SessionHandle) -> Result<(), SessionError> {
        self.with_control_barrier(|| {
            let transport = self
                .with_listener(listener, |entry| entry.transport)
                .map_err(|_| SessionError::ListenerMissing { listener })?;
            let Some(stop_listen) = transport.stop_listen() else {
                return Err(SessionError::TransportListenUnsupported {
                    transport: transport.name(),
                });
            };
            stop_listen(listener).map_err(|source| SessionError::TransportOpFailed { source })?;
            self.with_listeners_mut(|listeners| {
                let index = listener.session_index;
                if !listeners.contains_key(index) {
                    return Err(SessionError::ListenerMissing { listener });
                }
                drop(listeners.remove(index));
                Ok(())
            })
            .map_err(|_| SessionError::ListenerControlWrongThread)??;
            Ok(())
        })
        .map_err(|_| SessionError::ListenerControlWrongThread)?
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
    ) -> Result<u32, SessionError> {
        let Some(connect) = transport.connect() else {
            return Err(SessionError::TransportConnectUnsupported {
                transport: transport.name(),
            });
        };
        let connection = endpoint.connection;
        let worker_count = self.workers.len();
        if worker_count == 0 {
            return Err(SessionError::NoDataWorkers);
        }
        let application_connection = connection;
        let endpoint = SessionConnectEndpoint {
            worker: DataWorkerId::new((application_connection as usize % worker_count) as u32),
            ..endpoint
        };
        connect(endpoint).map_err(|source| SessionError::TransportOpFailed { source })?;
        Ok(connection)
    }

    /// Opens one child stream on the parent Session's owning worker.
    pub fn connect_stream(
        &self,
        transport: SessionTransportRegistration,
        endpoint: SessionConnectEndpoint,
    ) -> Result<u32, SessionError> {
        let parent = endpoint
            .parent_handle
            .ok_or(SessionError::ConnectStreamParentMissing)?;
        let expected_worker = DataWorkerId::new(parent.thread_index);
        if endpoint.worker != expected_worker {
            return Err(SessionError::ConnectStreamWrongWorker {
                parent,
                expected: expected_worker,
                actual: endpoint.worker,
            });
        }
        let Some(connect_stream) = transport.connect_stream() else {
            return Err(SessionError::TransportConnectStreamUnsupported {
                transport: transport.name(),
            });
        };
        let connection = endpoint.connection;
        connect_stream(endpoint).map_err(|source| SessionError::TransportOpFailed { source })?;
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
        listener: SessionHandle,
        operation: impl FnOnce(&SessionListener) -> R,
    ) -> RuntimeResult<R> {
        // SAFETY: Data Workers read a listener only after Main Thread has
        // published it through the worker barrier.
        let listeners = unsafe { &*self.listeners.get() };
        let index = listener.session_index;
        if !listeners.contains_key(index) {
            return Err(SessionError::ListenerMissing { listener }.into());
        }
        Ok(operation(
            listeners
                .get(index)
                .ok_or(SessionError::ListenerMissing { listener })?,
        ))
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

    fn worker(&self, worker: DataWorkerId) -> RuntimeResult<&ThreadOwned<SessionWorker<u32>>> {
        Ok(self.workers.get(worker.slot()).map(|slot| &**slot).ok_or(
            SessionQueueError::WorkerOutOfRange {
                worker: worker.slot(),
            },
        )?)
    }

    pub fn with_worker_mut<R>(
        &self,
        runtime: &DataPlaneRuntime,
        operation: impl FnOnce(&mut SessionWorker<u32>) -> RuntimeResult<R>,
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
        application: u32,
    ) -> RuntimeResult<()> {
        self.remove_application_mqs(engine, application)?;
        let listeners = {
            // SAFETY: this call runs on the Main Thread. Listener removal
            // below stops Data Workers through the same barrier as publish.
            let listeners = unsafe { &*self.listeners.get() };
            listeners
                .iter()
                .filter_map(|(index, listener)| {
                    (listener.application() == application).then_some(SessionHandle::new(index, 0))
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
        application: u32,
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
        application: u32,
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

pub(super) fn schedule_worker_task<R: Send + 'static>(
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
    mut worker: SessionWorker<u32>,
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

fn cleanup_session_worker_install(worker: &mut SessionWorker<u32>, engine: &mut Engine) {
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
        session_id: u32,
        context: SessionAppContext,
    ) -> RuntimeResult<()> {
        let index = session_id;
        if !self.entries.contains_key(index) {
            return Err(SessionError::SessionMissing { session_id }.into());
        }
        let entry = self
            .entries
            .get_mut(index)
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
        let mut deadline = Deadline::new(
            "session queue adaptive deadline",
            session_queue.slot().into(),
            schedule_session_queue_deadline,
        );
        deadline.set_polling_thread_index(runtime.thread_index());
        let index = runtime.file_main_mut().add_deadline(deadline)?;
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
            .saturating_add(usize::from(self.app.has_pending_connected_sessions()))
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
        app: u32,
        callbacks: SessionAppCallbacks<Index>,
    ) -> RuntimeResult<()> {
        let index = app as usize;
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
    pub fn session_app_callbacks(&self, app: u32) -> Option<SessionAppCallbacks<Index>> {
        self.session_app_callbacks
            .get(app as usize)
            .copied()
            .flatten()
    }

    /// Installs one transport's worker-local action table, keyed by transport
    /// id. O(1); rejects a duplicate registration for the same id.
    pub fn install_transport_actions(
        &mut self,
        transport: u8,
        actions: SessionTransportWorkerActions<Index>,
    ) -> Result<(), SessionTransportActionError> {
        let slot = transport as usize;
        if self
            .transport_actions
            .get(slot)
            .is_some_and(Option::is_some)
        {
            return Err(SessionTransportActionError::AlreadyRegistered { transport });
        }
        if self.transport_actions.len() <= slot {
            self.transport_actions.resize(slot + 1, None);
        }
        self.transport_actions[slot] = Some(actions);
        Ok(())
    }

    /// Copies one transport's worker action table out of the worker. O(1).
    /// The table is `Copy`, so the borrow ends before the callback runs.
    #[inline]
    fn actions_for(
        &self,
        transport: u8,
    ) -> Result<SessionTransportWorkerActions<Index>, SessionTransportActionError> {
        self.transport_actions
            .get(transport as usize)
            .copied()
            .flatten()
            .ok_or(SessionTransportActionError::MissingRegistration { transport })
    }

    /// Resolves one Session entry to the transport that owns it; mirrors VPP
    /// `session_get_transport_proto (s)` guarding transport ownership before
    /// VFT dispatch (session.c:1658-1712). The Session entry is the transport
    /// authority, so dispatch never takes a caller-supplied transport id.
    #[inline]
    fn entry_transport(&self, session_id: u32) -> Result<u8, SessionTransportActionError> {
        let index = session_id;
        if !self.entries.contains_key(index) {
            return Err(SessionTransportActionError::InvalidSession { session_id });
        }
        let entry = self
            .entries
            .get(index)
            .ok_or(SessionTransportActionError::InvalidSession { session_id })?;
        let Some(SessionType::Transport { transport, .. }) = entry.session_type else {
            return Err(SessionTransportActionError::InvalidSession { session_id });
        };
        Ok(transport)
    }

    /// Applies the VPP app-close state guard shared by `reset_stream` and
    /// `close_connection` (session.c:1657-1703): returns Ok(false) without
    /// notifying the transport for sessions at or beyond AppClosed and for
    /// sessions still in Creating; returns Ok(true) for Created/Published/
    /// Active sessions after recording AppClosed, so the transport action
    /// runs with the close already recorded. The entry borrow ends before
    /// any callback runs.
    #[inline]
    fn entry_app_close_guard(
        &mut self,
        session_id: u32,
    ) -> Result<bool, SessionTransportActionError> {
        let index = session_id;
        if !self.entries.contains_key(index) {
            return Err(SessionTransportActionError::InvalidSession { session_id });
        }
        let entry = self
            .entries
            .get_mut(index)
            .ok_or(SessionTransportActionError::InvalidSession { session_id })?;
        let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
            return Err(SessionTransportActionError::InvalidSession { session_id });
        };
        Ok(state.on_app_close_dispatch())
    }

    /// Dispatches one worker-local `open_stream` action to the transport that
    /// owns `parent`; mirrors VPP `vnet_connect_stream (parent_handle)`
    /// (application.c:1447). The transport allocates the child stream on this
    /// worker and returns its Session identity.
    pub fn open_stream(
        &mut self,
        parent: u32,
        direction: SessionStreamDirection,
        app_context: SessionAppContext,
    ) -> Result<u32, SessionTransportActionError> {
        let transport = self.entry_transport(parent)?;
        let actions = self.actions_for(transport)?;
        (actions.open_stream)(self, parent, direction, app_context).map_err(|source| {
            SessionTransportActionError::TransportActionFailed {
                action: "open_stream",
                source,
            }
        })
    }

    /// Dispatches one worker-local `reset_stream` action to the transport that
    /// owns the stream; mirrors VPP `session_transport_reset`
    /// (session.c:1687-1703): only pre-close sessions dispatch, with AppClosed
    /// recorded before the transport is notified.
    pub fn reset_stream(
        &mut self,
        session_id: u32,
        code: u64,
    ) -> Result<(), SessionTransportActionError> {
        let transport = self.entry_transport(session_id)?;
        let actions = self.actions_for(transport)?;
        if !self.entry_app_close_guard(session_id)? {
            return Ok(());
        }
        (actions.reset_stream)(self, session_id, code).map_err(|source| {
            SessionTransportActionError::TransportActionFailed {
                action: "reset_stream",
                source,
            }
        })
    }

    /// Dispatches one worker-local `stop_sending` action to the transport that
    /// owns the stream; mirrors VPP `session_transport_half_close`
    /// (session.c:1637-1648): only a READY session (Hammer's Active) can be
    /// half-closed, so every other state returns Ok without notifying the
    /// transport. Half-close never changes the session state.
    pub fn stop_sending(
        &mut self,
        session_id: u32,
        code: u64,
    ) -> Result<(), SessionTransportActionError> {
        let transport = self.entry_transport(session_id)?;
        let actions = self.actions_for(transport)?;
        let active = matches!(
            self.entries
                .get(session_id)
                .and_then(|entry| entry.session_type),
            Some(SessionType::Transport {
                state: SessionState::Active(_),
                ..
            })
        );
        if !active {
            return Ok(());
        }
        (actions.stop_sending)(self, session_id, code).map_err(|source| {
            SessionTransportActionError::TransportActionFailed {
                action: "stop_sending",
                source,
            }
        })
    }

    /// Dispatches one worker-local `close_connection` action to the transport
    /// that owns the connection; mirrors VPP `session_transport_close`
    /// (session.c:1657-1682) plus the `APP_PROTO_ERR_CODE` endpoint attribute
    /// (transport_types.h:447, quic.c:701-718): only pre-close sessions
    /// dispatch, with AppClosed recorded before the transport is notified.
    /// The reason stays raw `&[u8]`, never narrowed to `&str`.
    pub fn close_connection(
        &mut self,
        connection: u32,
        code: u64,
        reason: &[u8],
    ) -> Result<(), SessionTransportActionError> {
        let transport = self.entry_transport(connection)?;
        let actions = self.actions_for(transport)?;
        if !self.entry_app_close_guard(connection)? {
            return Ok(());
        }
        (actions.close_connection)(self, connection, code, reason).map_err(|source| {
            SessionTransportActionError::TransportActionFailed {
                action: "close_connection",
                source,
            }
        })
    }

    #[inline]
    pub fn fifo_pair(&self, session_id: u32) -> Option<(&Arc<Fifo>, &Arc<Fifo>)> {
        let index = session_id;
        let entry = self.entries.get(index)?;
        Some((&entry.rx_fifo, &entry.tx_fifo))
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
        session_id: u32,
        index: BufferIndex,
        header: SessionDgramHeader,
    ) -> RuntimeResult<usize> {
        self.enqueue_datagram_rx_from_buffer_at(buffers, session_id, index, 0, header)
    }

    /// Variant of [`Self::enqueue_datagram_rx_from_buffer`] for a packet
    /// buffer whose current window still contains network and UDP headers.
    /// Only `payload_offset..payload_offset + header.data_length` is copied.
    pub fn enqueue_datagram_rx_from_buffer_at(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: u32,
        index: BufferIndex,
        payload_offset: usize,
        header: SessionDgramHeader,
    ) -> RuntimeResult<usize> {
        let pool_index = session_id;
        let Some(entry) = self.entries.get(pool_index) else {
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
        self.publish_rx_enqueue(session_id, total)?;
        Ok(payload_len)
    }

    /// Peek the next complete TX datagram header without consuming it.
    pub fn peek_tx_datagram(&self, session_id: u32) -> RuntimeResult<Option<SessionDgramHeader>> {
        let index = session_id;
        let Some(entry) = self.entries.get(index) else {
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
        session_id: u32,
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
        session_id: u32,
        header: SessionDgramHeader,
    ) -> RuntimeResult<usize> {
        let total = header
            .total_len()
            .ok_or(SessionError::DatagramLengthMismatch {
                session_id,
                payload_len: header.data_offset() as usize,
                header_len: header.data_length(),
            })?;
        let index = session_id;
        let Some(entry) = self.entries.get(index) else {
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
        session_id: u32,
    ) -> Option<(u32, Option<u32>, Option<u64>, Option<&str>)> {
        let index = session_id;
        let entry = self.entries.get(index)?;
        Some((
            entry.owner_application?,
            entry.app,
            entry.app_opaque,
            entry.server_name.as_deref(),
        ))
    }

    /// Lower-to-upper owner link: the lower Session records its single upper
    /// Session, mirroring VPP's per-session single app-worker owner
    /// (`session_t.app_wrk_index`, `session_types.h:266`).
    /// overwrite an existing attachment; the owner-link boundary is shared by
    /// both upper creation APIs and by upper removal.
    fn attach_upper_session(&mut self, lower: u32, upper: u32) -> RuntimeResult<()> {
        let index = lower;
        if !self.entries.contains_key(index) {
            return Err(SessionError::SessionMissing { session_id: lower }.into());
        }
        let entry = self
            .entries
            .get_mut(index)
            .ok_or(SessionError::SessionMissing { session_id: lower })?;
        if entry.upper_session.is_some() {
            return Err(SessionError::UpperSessionAlreadyAttached { lower }.into());
        }
        entry.upper_session = Some(upper);
        Ok(())
    }

    /// Clears the lower->upper owner link iff it still names `upper`; a
    /// reused slot or a newer attachment is never cleared, and the lower is
    /// never removed.
    #[inline]
    fn detach_upper_session(&mut self, lower: u32, upper: u32) {
        let index = lower;
        if self.entries.contains_key(index) {
            if let Some(entry) = self.entries.get_mut(index) {
                if entry.upper_session == Some(upper) {
                    entry.upper_session = None;
                }
            }
        }
    }

    /// Publishes a Session-owned upper App Session from a Session App callback.
    pub fn create_upper_session(
        &mut self,
        lower: u32,
        context: SessionAppContext,
    ) -> RuntimeResult<u32> {
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
        if let Err(error) = self.attach_upper_session(lower, upper) {
            let _ = self.entries.remove(upper);
            return Err(error);
        }
        let app_session = self
            .app
            .create_app_session(
                lower.into(),
                Some(application),
                self.session_handle(upper),
                self.app_session_config,
                app_rx_mq,
            )
            .map_err(|error| {
                let _ = self.entries.remove(upper);
                self.detach_upper_session(lower, upper);
                error
            })?;
        let entry = self
            .entries
            .get_mut(upper)
            .ok_or(SessionError::SessionMissing { session_id: upper })?;
        entry.rx_fifo = Arc::clone(app_session.rx_fifo());
        entry.tx_fifo = Arc::clone(app_session.tx_fifo());
        entry.application = Some(SessionApplication::External(application));
        entry.owner_application = Some(application);
        entry.app = Some(app);
        entry.app_session = context;
        entry.lower_session = Some(lower);
        self.app.attach_session(upper, app_session);
        if let Err(error) = self.app.connected(upper).map(|_| ()) {
            let _ = self.entries.remove(upper);
            self.app.detach_session(upper);
            self.detach_upper_session(lower, upper);
            return Err(error);
        }
        Ok(upper)
    }

    /// Creates an App-facing upper Session bound to a lower Session and to
    /// the transport index that owns its Connection or Stream state.
    pub fn create_upper_transport_session(
        &mut self,
        lower: u32,
        transport: u8,
        index: Index,
        context: SessionAppContext,
    ) -> RuntimeResult<u32> {
        let (application, _app, opaque, server_name) = self
            .session_app_endpoint(lower)
            .ok_or(SessionError::SessionMissing { session_id: lower })?;
        let server_name = server_name.map(str::to_owned);
        let upper = self.construct_transport_session(
            transport,
            index,
            lower.into(),
            application,
            None,
            opaque,
            server_name.as_deref(),
            false,
        )?;
        let entry = self
            .entries
            .get_mut(upper)
            .ok_or(SessionError::SessionMissing { session_id: upper })?;
        entry.app_session = context;
        entry.lower_session = Some(lower);
        if let Err(error) = self.attach_upper_session(lower, upper) {
            let _ = self.remove_session(upper);
            return Err(error);
        }
        Ok(upper)
    }

    /// Publishes a handshake-established transport Session and notifies the
    /// external App without re-entering the lower Session App callback.
    pub fn publish_connected_transport_session(
        &mut self,
        session_id: u32,
        connected: Option<SessionConnectedMsg>,
    ) -> RuntimeResult<()> {
        if let Err(error) = self.connection_published(session_id) {
            return Err(match self.remove_session(session_id) {
                Ok(()) => error,
                Err(cleanup) => SessionError::ConnectPublicationCleanup {
                    session_id,
                    publication: error,
                    cleanup,
                }
                .into(),
            });
        }
        if let Some(connected) = connected {
            if let Err(error) = self.app.set_connected(session_id, connected) {
                return Err(match self.remove_session(session_id) {
                    Ok(()) => error,
                    Err(cleanup) => SessionError::ConnectPublicationCleanup {
                        session_id,
                        publication: error,
                        cleanup,
                    }
                    .into(),
                });
            }
        }
        if let Err(error) = self.connected(session_id) {
            return Err(match self.remove_session(session_id) {
                Ok(()) => error,
                Err(cleanup) => SessionError::ConnectPublicationCleanup {
                    session_id,
                    publication: error,
                    cleanup,
                }
                .into(),
            });
        }
        Ok(())
    }

    /// Publishes an accepted external child with an ACCEPTED message (VPP
    /// `session_api.c` `app_wrk_send_ctrl_evt(app_wrk,
    /// SESSION_CTRL_EVT_ACCEPTED, &m, sizeof(m))`, session_api.c:301).
    ///
    /// The Session transitions Creating -> Published and stays Published
    /// (VPP LISTENING) until the Application answers ACCEPTED_REPLY; the
    /// ACCEPTED event is pushed to the App Session event queue and the
    /// ACCEPTED message rides the App Session publication. Queue-full
    /// retries are drained by [`SessionWorker::poll_app`] from
    /// [`AppWorker::pending_accepted`] without scanning.
    pub fn publish_accepted_transport_session(&mut self, session_id: u32) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        let application = match entry.application {
            Some(SessionApplication::External(application)) => application,
            _ => return Err(SessionError::SessionMissing { session_id }.into()),
        };
        let listener = entry
            .listener
            .ok_or(SessionError::SessionMissing { session_id })?;
        let flags = entry.flags;
        let accepted = SessionAcceptedMsg::new(
            application.into(),
            listener,
            self.session_handle(session_id),
            flags,
        );
        if let Err(error) = self.connection_published(session_id) {
            return Err(match self.remove_session(session_id) {
                Ok(()) => error,
                Err(cleanup) => SessionError::ConnectPublicationCleanup {
                    session_id,
                    publication: error,
                    cleanup,
                }
                .into(),
            });
        }
        if let Err(error) = self.app.set_accepted(session_id, accepted) {
            return Err(match self.remove_session(session_id) {
                Ok(()) => error,
                Err(cleanup) => SessionError::ConnectPublicationCleanup {
                    session_id,
                    publication: error,
                    cleanup,
                }
                .into(),
            });
        }
        if let Err(error) = self.app.accepted(session_id) {
            return Err(match self.remove_session(session_id) {
                Ok(()) => error,
                Err(cleanup) => SessionError::ConnectPublicationCleanup {
                    session_id,
                    publication: error,
                    cleanup,
                }
                .into(),
            });
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn local_app(&self) -> &AppWorker {
        &self.app
    }

    #[cfg(test)]
    pub(crate) fn session_fifos(&self, session_id: u32) -> Option<(&Arc<Fifo>, &Arc<Fifo>)> {
        self.entries
            .get(session_id)
            .map(|entry| (&entry.rx_fifo, &entry.tx_fifo))
    }

    /// Registers one per-Application MQ with this Data Worker's FileMain.
    pub(crate) fn install_app_mq(
        &mut self,
        application: u32,
        queue: Arc<SessionMsgQueue>,
        app_session_input: hammer_core::data_plane::NodeId,
        runtime: &mut DataPlaneRuntime,
    ) -> RuntimeResult<()> {
        let slot = application as usize;
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
            pending_queue: &mut self.app_rx_mq_pending as *mut VecDeque<u32> as usize,
            pending: false,
            postponed: false,
        });
        let entry_ptr = Box::into_raw(entry);
        let mut file = File::new(
            signal_read,
            format!("app rx mq {:?}", application),
            entry_ptr as usize as u64,
            FileFunctions {
                read: Some(schedule_app_mq_pending),
                ..FileFunctions::default()
            },
        );
        file.set_polling_thread_index(runtime.thread_index());
        let file = match runtime.file_main_mut().add(file) {
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
        application: u32,
        runtime: &mut DataPlaneRuntime,
    ) -> RuntimeResult<()> {
        let slot = application as usize;
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
    pub(crate) fn mark_app_mq_pending(&mut self, application: u32) -> bool {
        let slot = application as usize;
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
    pub(crate) fn app_mq_worker(&self, application: u32) -> Option<Arc<SessionMsgQueue>> {
        self.app_rx_mqs
            .get(application as usize)?
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
    ) -> Result<usize, SessionMsgQueueError> {
        let pending_snapshot = core::mem::take(&mut self.app_rx_mq_pending);
        let mut dispatched = 0usize;
        for application in pending_snapshot {
            let slot = application as usize;
            let Some(Some(entry)) = self.app_rx_mqs.get_mut(slot) else {
                continue;
            };
            if entry.application != application || !entry.pending {
                continue;
            }
            let entry_dispatched = entry.drain_snapshot_to(&mut dispatch_event)?;
            if entry.pending {
                self.app_rx_mq_pending.push_back(application);
            }
            dispatched = dispatched.saturating_add(entry_dispatched);
        }
        Ok(dispatched)
    }

    pub(crate) fn drain_app_mq(&mut self, application: u32) -> RuntimeResult<usize> {
        let mut control_events = core::mem::take(&mut self.control_events);
        let mut new_io_events = core::mem::take(&mut self.new_io_events);
        let handled = self.drain_one_app_mq_to(application, |ring, event| {
            enqueue_app_event(&mut control_events, &mut new_io_events, ring, event);
        })?;
        self.control_events = control_events;
        self.new_io_events = new_io_events;
        Ok(handled)
    }

    fn drain_one_app_mq_to(
        &mut self,
        application: u32,
        mut dispatch_event: impl FnMut(SessionMqRing, SessionEvt),
    ) -> Result<usize, SessionMsgQueueError> {
        self.app_rx_mq_pending
            .retain(|candidate| *candidate != application);
        let slot = application as usize;
        let Some(Some(entry)) = self.app_rx_mqs.get_mut(slot) else {
            return Ok(0);
        };
        if entry.application != application {
            return Ok(0);
        }

        let dispatched = entry.drain_snapshot_to(&mut dispatch_event)?;
        if entry.pending {
            self.app_rx_mq_pending.push_back(application);
        }
        Ok(dispatched)
    }

    #[inline]
    pub fn session_transport(&self, session_id: u32) -> Option<(u8, Index)> {
        let entry = self.entries.get(session_id)?;
        let Some(SessionType::Transport { transport, state }) = entry.session_type else {
            return None;
        };
        Some((transport, state.transport_index()?))
    }

    #[inline]
    pub fn has_session(&self, session_id: u32) -> bool {
        self.entries.contains_key(session_id)
    }

    /// Transport parent of a stream Session (VPP `sep_ext.parent_handle`,
    /// session_types.h:109, wired by `http_connect_transport_stream`).
    /// Parent topology is metadata distinct from `lower_session` protocol
    /// attachment and never drives upper/lower cleanup.
    #[inline]
    pub(crate) fn parent_session(&self, session_id: u32) -> Option<u32> {
        self.entries
            .get(session_id)
            .and_then(|entry| entry.parent_session)
    }

    /// Returns the lower transport Session bound to an upper Session.
    #[inline]
    pub fn lower_session(&self, upper: u32) -> Option<u32> {
        self.entries
            .get(upper)
            .and_then(|entry| entry.lower_session)
    }

    /// Returns the upper Session attached to a lower transport Session.
    #[inline]
    pub fn upper_session(&self, lower: u32) -> Option<u32> {
        self.entries
            .get(lower)
            .and_then(|entry| entry.upper_session)
    }

    /// Records the transport parent of a stream Session. Mirrors VPP
    /// `session_alloc_for_stream` (session.c:507-523) rejecting a child
    /// whose parent handle is invalid. O(1).
    #[inline]
    pub(crate) fn set_parent_session(&mut self, session_id: u32, parent: u32) -> RuntimeResult<()> {
        if !self.entries.contains_key(parent) {
            return Err(SessionError::ConnectStreamParentMissing.into());
        }
        if let Some(entry) = self.entries.get_mut(session_id) {
            entry.parent_session = Some(parent);
        }
        Ok(())
    }

    #[inline]
    pub fn session_app_closed(&self, session_id: u32) -> bool {
        matches!(
            self.entries
                .get(session_id)
                .and_then(|entry| entry.session_type),
            Some(SessionType::Transport {
                state: SessionState::AppClosed(_) | SessionState::Closed(_),
                ..
            })
        )
    }

    #[inline]
    pub fn app_session(&self, session_id: u32) -> Option<&Arc<AppSession>> {
        self.app.app_session(session_id)
    }

    /// Allocation owner of the Session's AppSession AttachSlot, used to
    /// inherit the parent worker for peer-opened transport children.
    #[inline]
    pub fn session_allocation_owner(&self, session_id: u32) -> Option<u64> {
        self.app.session_allocation_owner(session_id)
    }

    #[inline]
    pub fn prefetch_session(&self, session_id: u32) {
        let _ = self.entries.get(session_id);
    }

    pub fn stream_accept(
        &mut self,
        transport: u8,
        index: Index,
        listener: SessionHandle,
    ) -> RuntimeResult<u32> {
        let main = self
            .listener_main
            .as_ref()
            .cloned()
            .ok_or(SessionError::ListenerMainMissing)?;
        let application_listener =
            main.with_listener(listener, SessionListener::application_listener)?;
        let session_id = main
            .applications
            .with_listener(application_listener, |listener| {
                self.construct_stream_sessions(
                    transport,
                    index,
                    application_listener.into(),
                    listener.application(),
                    listener.app(),
                    listener.opaque(),
                    None,
                    true,
                )
            })
            .map_err(RuntimeError::from)??;
        // VPP `session_accepted_msg_t.listener_handle` (application_interface.h)
        // names the accepting listener; it rides the ACCEPTED publication.
        let entry = self
            .entries
            .get_mut(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        entry.listener = Some(listener);
        Ok(session_id)
    }

    /// Derives the child Session flags a transport computed for the stream
    /// (VPP `session_alloc_for_stream`, `quic_quicly_on_stream_open`:
    /// SESSION_F_STREAM, SESSION_F_UNIDIRECTIONAL). They ride the ACCEPTED
    /// message (`mq_send_session_accepted_cb`, session_api.c:255).
    pub fn set_session_flags(&mut self, session_id: u32, flags: SessionFlags) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get_mut(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        entry.flags = flags;
        Ok(())
    }

    /// Names the accepting listener of an accepted Session before the
    /// ACCEPTED publication is emitted (VPP `session_accepted_msg_t
    /// .listener_handle`, application_interface.h: quic_quicly_on_stream_open
    /// sets `stream_session->listener_handle =
    /// listen_session_get_handle (quic_session)`).
    pub fn pin_accepted_listener(
        &mut self,
        session_id: u32,
        listener: SessionHandle,
    ) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get_mut(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        entry.listener = Some(listener);
        Ok(())
    }

    /// Test seam: the flags derived for a Session (`set_session_flags`).
    #[doc(hidden)]
    pub fn session_flags(&self, session_id: u32) -> Option<SessionFlags> {
        self.entries.get(session_id).map(|entry| entry.flags)
    }

    /// O(1) snapshot of [`SessionAcceptMetadata`] for one accepted Session.
    ///
    /// Mirrors VPP `http_ts_accept_stream` (http.c:675), which resolves the
    /// accepted stream's parent from its listener handle
    /// (`conn_session = session_get_from_handle (stream_session->
    /// listener_handle)`) and inherits the parent context (`conn_session->
    /// opaque`). The child entry's flags gate the parent resolution exactly
    /// as `SESSION_F_STREAM` gates VPP's stream accept path, so roots never
    /// resolve a parent. The parent follows [`SessionWorker::
    /// session_id_from_handle`] semantics precisely — the live Session at the
    /// handle's slot, never a weaker stale check. Roots report the role
    /// fixed by their construction lifecycle ([`SessionEntry::endpoint_role`]:
    /// accepted `Server`, outbound connect `Client`); stream children inherit
    /// the parent connection's role together with its context, so an absent,
    /// foreign, or freed parent yields `None` for both. Static `Copy` data
    /// from at most two pool lookups: no allocation, locking, atomics, or
    /// scanning.
    #[inline]
    pub fn accept_metadata(&self, session_id: u32) -> Option<SessionAcceptMetadata> {
        let entry = self.entries.get(session_id)?;
        let (role, parent_app_context) = if entry.flags.contains(SessionFlags::STREAM) {
            match entry
                .listener
                .and_then(|listener| self.session_id_from_handle(listener))
            {
                Some(parent) => {
                    let parent = self.entries.get(parent)?;
                    (Some(parent.endpoint_role()), Some(parent.app_session))
                }
                None => (None, None),
            }
        } else {
            (Some(entry.endpoint_role()), None)
        };
        Some(SessionAcceptMetadata {
            flags: entry.flags,
            role,
            parent_app_context,
        })
    }

    /// Test seam: the retained ACCEPTED message of a Session whose
    /// publication is still pending delivery to the Application.
    #[doc(hidden)]
    pub fn accepted_message(&self, session_id: u32) -> Option<SessionAcceptedMsg> {
        self.app.accepted_message(session_id)
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
        transport: u8,
        index: Index,
        allocation_owner: u64,
        application: u32,
        app: Option<u32>,
        opaque: Option<u64>,
        server_name: Option<&str>,
        accepted: bool,
    ) -> RuntimeResult<u32> {
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
        transport: u8,
        index: Index,
        connection: u32,
    ) -> RuntimeResult<u32> {
        let session_id = self.stream_connect_pending(transport, index, connection)?;
        self.complete_stream_connect(session_id)?;
        Ok(session_id)
    }

    /// Constructs the Session owned by a transport half-open connection.
    /// Publication and application notification remain deferred until the
    /// transport reports that the connection is established.
    pub fn stream_connect_pending(
        &mut self,
        transport: u8,
        index: Index,
        connection: u32,
    ) -> RuntimeResult<u32> {
        let application_connection = connection;
        let applications = Arc::clone(&self.applications);
        let session_id = applications
            .with_connection(application_connection, |connection| {
                self.construct_stream_sessions(
                    transport,
                    index,
                    connection.context(),
                    connection.application(),
                    connection.app(),
                    connection.opaque(),
                    connection.server_name(),
                    false,
                )
            })
            .map_err(RuntimeError::from)??;
        let entry = self
            .entries
            .get_mut(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        entry.application_connection = Some(application_connection);
        Ok(session_id)
    }

    /// Completes a previously constructed transport connect transaction.
    ///
    /// Accepted children of an external listener publish an ACCEPTED
    /// publication instead of CONNECTED and stay Published (VPP listening)
    /// until the Application answers ACCEPTED_REPLY.
    pub fn complete_stream_connect(&mut self, session_id: u32) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        let accepted_external =
            entry.accepted && matches!(entry.application, Some(SessionApplication::External(_)));
        if accepted_external {
            return self.publish_accepted_transport_session(session_id);
        }
        let connected = entry
            .application_connection
            .filter(|_| self.app.app_session(session_id).is_some())
            .map(|connection| {
                let context = self
                    .applications
                    .with_connection(connection, |entry| entry.context())
                    .map_err(RuntimeError::from)?;
                let connected =
                    SessionConnectedMsg::new(context, Ok(self.session_handle(session_id)));
                Ok::<_, RuntimeError>(connected)
            })
            .transpose()?;
        self.publish_connected_transport_session(session_id, connected)
    }

    pub fn stream_connect_failed(
        &self,
        connection: u32,
        error: SessionConnectError,
    ) -> RuntimeResult<bool> {
        let application_connection = connection;
        let (application, context) = self
            .applications
            .with_connection(application_connection, |entry| {
                (entry.application(), entry.context())
            })
            .map_err(RuntimeError::from)?;
        let message = SessionConnectedMsg::new(context, Err(error));
        let accepted = self.app.publish_connect_failed(application, message)?;
        if accepted {
            self.applications
                .mark_connected(application_connection)
                .map_err(RuntimeError::from)?;
        }
        Ok(accepted)
    }

    fn construct_stream_sessions(
        &mut self,
        transport: u8,
        index: Index,
        allocation_owner: u64,
        application: u32,
        app: Option<u32>,
        opaque: Option<u64>,
        server_name: Option<&str>,
        accepted: bool,
    ) -> RuntimeResult<u32> {
        let Some(app) = app else {
            return self.construct_external_transport_session(
                transport,
                index,
                allocation_owner,
                application,
                opaque,
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
        transport: u8,
        index: Index,
        application: u32,
        app: u32,
        opaque: Option<u64>,
        server_name: Option<&str>,
        accepted: bool,
    ) -> RuntimeResult<u32> {
        let (rx_fifo, tx_fifo) = self.create_local_fifos()?;
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            transport, rx_fifo, tx_fifo,
        ))?;
        let entry = self
            .entries
            .get_mut(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        entry.owner_application = Some(application);
        entry.app = Some(app);
        entry.app_opaque = opaque;
        entry.server_name = server_name.map(str::to_owned);
        entry.accepted = accepted;
        self.finish_transport_creation(session_id, index)?;
        Ok(session_id)
    }

    fn construct_external_transport_session(
        &mut self,
        transport: u8,
        index: Index,
        allocation_owner: u64,
        application: u32,
        opaque: Option<u64>,
    ) -> RuntimeResult<u32> {
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
                .get_mut(session_id)
                .ok_or(SessionError::SessionMissing { session_id })?;
            entry.rx_fifo = Arc::clone(application_session.rx_fifo());
            entry.tx_fifo = Arc::clone(application_session.tx_fifo());
            entry.application = Some(SessionApplication::External(application));
            entry.owner_application = Some(application);
            // VPP `session_open_stream` (session.c:1412) sets
            // `s->opaque = sep->opaque` for external stream children too.
            entry.app_opaque = opaque;
        }
        self.finish_transport_creation(session_id, index)?;
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
        session_ids: &[u32],
        application_session: Option<&AppSession>,
    ) {
        if let Some(session) = application_session {
            self.app.discard_app_session(session);
        }
        session_ids.iter().rev().copied().for_each(|session_id| {
            let _ = self.entries.remove(session_id);
        });
    }

    fn insert_session_entry(&mut self, entry: SessionEntry<Index>) -> RuntimeResult<u32> {
        Ok(u32::from(self.entries.insert(entry)))
    }

    #[inline]
    pub fn session_handle(&self, session_id: u32) -> SessionHandle {
        SessionHandle::new(session_id, self.worker.slot() as u32)
    }

    #[inline]
    pub fn session_id_from_handle(&self, handle: SessionHandle) -> Option<u32> {
        if handle.thread_index != self.worker.slot() as u32 {
            return None;
        }
        let index = handle.session_index;
        self.entries.get(index).map(|_| u32::from(index))
    }

    pub fn program_thread_migration(
        &self,
        runtime: &DataPlaneRuntime,
        target_worker: DataWorkerId,
        old_handle: SessionHandle,
        tuple: SessionTuple,
        dgram: SessionDgramArgs,
    ) -> SessionMigrateResult {
        self.listener_main
            .as_ref()
            .map_or(SessionMigrateResult::Unavailable, |main| {
                main.program_thread_migration(runtime, target_worker, old_handle, tuple, dgram)
            })
    }

    pub fn cancel_thread_migration(&self, old_handle: SessionHandle, tuple: SessionTuple) -> bool {
        self.listener_main
            .as_ref()
            .is_some_and(|main| main.cancel_migration(tuple, old_handle))
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

    pub fn push_session_switch_pool_closed(
        &self,
        runtime: &DataPlaneRuntime,
        closed: SessionSwitchPoolClosed,
    ) -> Result<(), SessionSwitchPoolClosed> {
        self.listener_main.as_ref().map_or(Err(closed), |main| {
            main.push_session_switch_pool_closed(runtime, closed)
        })
    }

    pub fn pop_session_switch_pool_closed(&self) -> Option<SessionSwitchPoolClosed> {
        self.listener_main
            .as_ref()?
            .pop_session_switch_pool_closed(self.worker)
    }

    pub fn wait_session_migration_shutdown_phase(&self) {
        if let Some(main) = self.listener_main.as_ref() {
            main.wait_session_migration_shutdown_phase();
        }
    }

    pub fn insert_session_endpoint(
        &self,
        session_id: u32,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.add_connection(local, remote, transport, self.session_handle(session_id)))
    }

    pub fn remove_session_endpoint(
        &self,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.del_connection(local, remote, transport))
    }

    pub fn replace_session_endpoint(
        &self,
        new_session: SessionHandle,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.replace_connection(local, remote, transport, new_session))
    }

    pub fn publish_session_migration(
        &self,
        new_session: SessionHandle,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.replace_connection(local, remote, transport, new_session))
    }

    pub fn lookup_session_endpoint(
        &self,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<Option<SessionHandle>> {
        let main = self
            .listener_main
            .as_ref()
            .ok_or(SessionError::ListenerMainMissing)?;
        Ok(main.lookup_connection(local, remote, transport))
    }

    fn finish_transport_creation(&mut self, session_id: u32, index: Index) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get_mut(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
            return Err(SessionError::TransportSessionCreateIncomplete { session_id }.into());
        };
        *state = state
            .finish_creation(index)
            .ok_or(SessionError::TransportSessionCreateIncomplete { session_id })?;
        Ok(())
    }

    pub fn connection_published(&mut self, session_id: u32) -> RuntimeResult<bool> {
        let entry = self
            .entries
            .get_mut(session_id)
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

    pub fn migration_snapshot(&self, session_id: u32) -> Option<SessionMigrationState> {
        let entry = self.entries.get(session_id)?;
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
        if !matches!(state, SessionState::Active(_)) {
            return None;
        }
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
    ) -> RuntimeResult<(u32, SessionHandle)> {
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            state.transport,
            state.rx_fifo,
            state.tx_fifo,
        ))?;
        self.finish_transport_creation(session_id, index)?;
        if let Err(error) = self.connection_published(session_id) {
            let _ = self.remove_session(session_id);
            return Err(error);
        }
        Ok((session_id, self.session_handle(session_id)))
    }

    pub fn accept_migrated_session(&mut self, session_id: u32) -> RuntimeResult<()> {
        self.connected(session_id)?;
        let (tx_pending, rx_event) = {
            let entry = self
                .entries
                .get(session_id)
                .ok_or(SessionError::SessionMissing { session_id })?;
            (entry.tx_fifo.max_dequeue() != 0, entry.rx_fifo.has_event())
        };
        if tx_pending {
            self.mark_ready(session_id);
        }
        if rx_event {
            self.new_io_events
                .push_back(SessionEvt::io(session_id, SessionEvtType::RxEnq));
        }
        Ok(())
    }

    pub fn notify_migrated_session(
        &mut self,
        old_session: u32,
        new_handle: SessionHandle,
    ) -> RuntimeResult<()> {
        let Some(entry) = self.entries.get(old_session) else {
            return Ok(());
        };
        let Some((app, context)) = entry
            .app
            .zip((entry.app_session != 0).then_some(entry.app_session))
        else {
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

    pub fn remove_migrated_session(&mut self, session_id: u32) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get(session_id)
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

    pub fn rollback_session_creation(&mut self, session_id: u32) -> RuntimeResult<Option<Index>> {
        let entry = self
            .entries
            .get(session_id)
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
        transport: u8,
        index: Index,
    ) -> RuntimeResult<u32> {
        let (rx_fifo, tx_fifo) = self.create_local_fifos()?;
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            transport, rx_fifo, tx_fifo,
        ))?;
        self.finish_transport_creation(session_id, index)?;
        self.connection_published(session_id)?;
        self.connected(session_id)?;
        Ok(session_id)
    }

    pub fn insert_session_for_test(&mut self, transport: u8, index: Index) -> u32 {
        // Unique per-call allocation owner: the shared segment name embeds
        // the owner, and parallel test workers using the same name would
        // race on macOS shm_open.
        static TEST_SESSION_OWNER: AtomicU32 = AtomicU32::new(0);
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
                TEST_SESSION_OWNER.fetch_add(1, Ordering::Relaxed).into(),
                None,
                self.session_handle(session_id),
                self.app_session_config,
                app_rx_mq,
            )
            .expect("create test App Session");
        if let Some(entry) = self.entries.get_mut(session_id) {
            entry.rx_fifo = Arc::clone(session.rx_fifo());
            entry.tx_fifo = Arc::clone(session.tx_fifo());
            entry.application = Some(SessionApplication::External(application));
        }
        self.finish_transport_creation(session_id, index)
            .expect("test Session transport creation completes");
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
    pub fn install_application_mq_for_test(&mut self, application: u32) -> RuntimeResult<()> {
        let application_slot = application as usize;
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
        let pending_queue = &mut self.app_rx_mq_pending as *mut VecDeque<u32> as usize;
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

    fn application_detached(&mut self, application: u32) {
        let sessions = self
            .entries
            .iter()
            .filter_map(|(index, entry)| {
                matches!(
                    entry.application,
                    Some(SessionApplication::External(owner)) if owner == application
                )
                .then_some(u32::from(index))
            })
            .collect::<Vec<_>>();
        sessions
            .into_iter()
            .for_each(|session_id| self.schedule_disconnect(session_id));
    }

    pub fn notify_transport_closed(&mut self, session_id: u32, index: Index) -> RuntimeResult<()> {
        self.notify_transport_event(session_id, index, SessionEvtType::TransportClosed)
    }

    pub fn notify_transport_closing(
        &mut self,
        runtime: Option<&DataPlaneRuntime>,
        session_id: u32,
        index: Index,
    ) -> RuntimeResult<()> {
        if self.notify_transport_closing_event(session_id, index)?
            && let Some(runtime) = runtime
        {
            self.wake_session_queue(runtime)?;
        }
        Ok(())
    }

    fn notify_transport_closing_event(
        &mut self,
        session_id: u32,
        index: Index,
    ) -> RuntimeResult<bool> {
        let Some(entry) = self.entries.get_mut(session_id) else {
            return Ok(false);
        };
        let notify_application = match entry.session_type.as_mut() {
            Some(SessionType::Transport { state, .. }) => state.on_transport_close(index),
            _ => false,
        };
        if notify_application {
            self.notify_application_event(session_id, SessionEvtType::Disconnected)?;
        }
        Ok(notify_application)
    }

    pub fn notify_transport_reset(&mut self, session_id: u32, index: Index) -> RuntimeResult<()> {
        self.notify_transport_event(session_id, index, SessionEvtType::Reset)
    }

    fn notify_transport_event(
        &mut self,
        session_id: u32,
        index: Index,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let Some(entry) = self.entries.get_mut(session_id) else {
            return Ok(());
        };
        let notify_application = match entry.session_type.as_mut() {
            Some(SessionType::Transport { state, .. }) => state.on_transport_close(index),
            _ => false,
        };
        if notify_application {
            self.notify_application_event(session_id, event)?;
        }
        Ok(())
    }

    pub fn notify_transport_deleted(&mut self, session_id: u32, index: Index) -> RuntimeResult<()> {
        let Some(entry) = self.entries.get_mut(session_id) else {
            return Ok(());
        };
        let remove = match entry.session_type.as_mut() {
            Some(SessionType::Transport { state, .. }) => state.on_transport_deleted(index),
            _ => false,
        };
        if remove {
            self.remove_session(session_id)?;
        }
        Ok(())
    }

    fn close_transport_session(&mut self, session_id: u32) -> RuntimeResult<bool> {
        let Some(entry) = self.entries.get_mut(session_id) else {
            return Ok(false);
        };
        let (remove, disconnect) = match entry.session_type.as_mut() {
            Some(SessionType::Transport { state, .. }) => {
                let disconnect = matches!(
                    state,
                    SessionState::Created(_)
                        | SessionState::Published(_)
                        | SessionState::Active(_)
                        | SessionState::TransportClosed(_)
                );
                (state.on_app_close(), disconnect)
            }
            _ => (false, false),
        };
        if remove {
            self.remove_session(session_id)?;
        }
        Ok(disconnect)
    }

    /// Removes a Session and, when it is the lower-most Session of a Session App,
    /// the chain of upper Sessions attached along the owner link. Mirrors VPP
    /// `session_cleanup_notify` (session.c:304-318): the app cleanup callback
    /// runs while the Session is still live, then the entry is freed.
    /// freed. The cascade walks `upper_session` from the removed entry, follows
    /// each upper only while its live entry still points back at the previous
    /// link, and stops on a stale, reused, or mismatched link without touching
    /// the occupant. The links are revalidated after the cleanup callback, which
    /// runs with `&mut self` and may unlink or replace the entry or its child;
    /// the walk then stops without removing anything further. O(chain) time,
    /// O(1) space; no scan, collect, or recursion.
    fn remove_session(&mut self, session_id: u32) -> RuntimeResult<()> {
        let Some(entry) = self.entries.get(session_id) else {
            return Ok(());
        };
        let (app, context, lower_session) = (entry.app, entry.app_session, entry.lower_session);
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
        let Some(entry) = self.entries.remove(session_id) else {
            return Ok(());
        };
        let mut previous = session_id;
        let mut next_upper = if app.is_some() && lower_session.is_none() {
            entry.upper_session
        } else {
            None
        };
        if let Some(lower) = entry.lower_session {
            self.detach_upper_session(lower, session_id);
        }
        if matches!(entry.application, Some(SessionApplication::External(_))) {
            drop(self.app.detach_session(session_id));
        }
        while let Some(upper) = next_upper {
            let Some(upper_entry) = self.entries.get(upper) else {
                break;
            };
            if upper_entry.lower_session != Some(previous) {
                break;
            }
            let upper_app = upper_entry.app;
            let upper_context = upper_entry.app_session;
            let upper_application = upper_entry.application;
            let upper_child = upper_entry.upper_session;
            if let Some(app) = upper_app
                && upper_context != 0
                && let Some(callbacks) = self.session_app_callbacks(app)
                && let Some(cleanup) = callbacks.cleanup
            {
                cleanup(self, upper, upper_context)?;
            }
            // The callback ran with `&mut self` and may have unlinked or
            // replaced this entry or its child; remove this node only while it
            // still points back at the previous link, and never follow a child
            // the callback detached or replaced.
            let Some(upper_entry) = self.entries.get(upper) else {
                break;
            };
            if upper_entry.lower_session != Some(previous)
                || upper_entry.upper_session != upper_child
            {
                break;
            }
            let _ = self.entries.remove(upper);
            if matches!(upper_application, Some(SessionApplication::External(_))) {
                drop(self.app.detach_session(upper));
            }
            previous = upper;
            next_upper = upper_child;
        }
        Ok(())
    }

    /// Worker-local rollback boundary for a newly-created upper Session: the
    /// exactly-one HTTP/3 upper publication handoff removes the upper through
    /// this method instead of sweeping through `remove_session`. Mirrors VPP's
    /// callback-before-free ordering (`session_cleanup_notify`,
    /// session.c:304-318): the Session App cleanup callback runs while the
    /// upper is still live, then the entry is freed, then the lower owner link
    /// is cleared and the external App is detached. A newly-created upper has
    /// no published children, so no owner-link cascade is followed.
    ///
    /// The cleanup callback runs only for the exact requested `upper`
    /// [`u32`]: the read is gated on the exact slot lookup, so a removed
    /// or reused slot cannot invoke cleanup for a different live occupant.
    ///
    /// Reentrant cleanup is safe: the callback runs with `&mut self` and may
    /// itself remove or replace the upper. Removal and the follow-on detaches
    /// are revalidated through the exact [`u32`] slot lookup, so a slot
    /// already removed or reused by the callback is left untouched.
    pub fn remove_upper_session(&mut self, upper: u32) -> RuntimeResult<()> {
        // The callback must see only the occupant of the requested upper
        // u32: the read is gated on the exact slot lookup, so a removed
        // or reused slot cannot invoke cleanup against a fresh occupant.
        let Some(entry) = self.entries.get(upper) else {
            return Ok(());
        };
        let (app, context) = (entry.app, entry.app_session);
        if let Some(app) = app
            && context != 0
            && let Some(callbacks) = self.session_app_callbacks(app)
            && let Some(cleanup) = callbacks.cleanup
        {
            cleanup(self, upper, context)?;
        }
        // The callback ran with `&mut self`; free only while the slot still
        // holds this generation of the upper.
        let Some(entry) = self.entries.remove(upper) else {
            return Ok(());
        };
        if let Some(lower) = entry.lower_session {
            self.detach_upper_session(lower, upper);
        }
        if matches!(entry.application, Some(SessionApplication::External(_))) {
            drop(self.app.detach_session(upper));
        }
        Ok(())
    }

    fn notify_application_event(
        &mut self,
        session_id: u32,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let app = self.entries.get(session_id).and_then(|entry| entry.app);
        if let Some(app) = app {
            return self.dispatch_app_lifecycle(session_id, app, event);
        }
        let application = self
            .entries
            .get(session_id)
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
    pub fn mark_ready(&mut self, session_id: u32) {
        let Some(entry) = self.entries.get_mut(session_id) else {
            return;
        };
        if entry.schedule_pending {
            return;
        }
        entry.schedule_pending = true;
        self.new_io_events
            .push_back(SessionEvt::io(session_id, SessionEvtType::TxEnq));
    }

    #[inline]
    fn reschedule_old(&mut self, session_id: u32) {
        let Some(entry) = self.entries.get_mut(session_id) else {
            return;
        };
        if entry.schedule_pending {
            return;
        }
        entry.schedule_pending = true;
        self.old_io_events
            .push_back(SessionEvt::io(session_id, SessionEvtType::TxEnq));
    }

    #[inline]
    pub fn schedule_disconnect(&mut self, session_id: u32) {
        self.control_events.push_back(SessionEvt::ctrl(
            self.session_handle(session_id),
            SessionEvtType::Close,
        ));
    }

    #[inline]
    pub fn schedule_half_close(&mut self, session_id: u32) {
        self.control_events.push_back(SessionEvt::ctrl(
            self.session_handle(session_id),
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
        })?;
        self.control_events = control_events;
        self.new_io_events = new_io_events;
        let pending_sessions = self.app.pending_connected_sessions();
        let pending_session_count = pending_sessions.len();
        for session_id in pending_sessions {
            let accepted = self.dispatch_application(session_id, SessionEvtType::Connect)?;
            if accepted {
                self.complete_application_connection(session_id)?;
            }
        }
        // Drain accepted-publication retries from the bounded deque: each
        // session is re-attempted once per round and re-queued when still
        // blocked, so a full publisher queue cannot be lost or spun.
        let mut accepted_count = 0usize;
        while self.app.has_pending_accepted_sessions() {
            let Some(session_id) = self.app.next_pending_accepted() else {
                break;
            };
            if self.app.retry_pending_accepted(session_id)? {
                accepted_count += 1;
            } else {
                break;
            }
        }
        Ok(app_mq_handled
            .saturating_add(pending_session_count)
            .saturating_add(accepted_count))
    }

    pub(crate) fn poll_session_events(&mut self) -> RuntimeResult<usize> {
        let snapshot = self.session_evt_q.len();
        let mut handled = 0usize;
        for _ in 0..snapshot {
            let Some((ring, event)) = self.session_evt_q.dequeue_with_ring()? else {
                break;
            };
            enqueue_app_event(
                &mut self.control_events,
                &mut self.new_io_events,
                ring,
                event,
            );
            handled += 1;
        }
        Ok(handled)
    }

    fn dispatch_application(
        &mut self,
        session_id: u32,
        event: SessionEvtType,
    ) -> RuntimeResult<bool> {
        let app = self.entries.get(session_id).and_then(|entry| entry.app);
        if let Some(app) = app {
            self.dispatch_app_event(session_id, app, event)?;
            return Ok(true);
        }
        let application = self
            .entries
            .get(session_id)
            .and_then(|entry| entry.application);
        match application {
            Some(SessionApplication::External(_)) => match event {
                SessionEvtType::RxEnq | SessionEvtType::TxDeq => {
                    let Some(session) = self.app.app_session(session_id) else {
                        return Ok(true);
                    };
                    session.push_io_event(event).map_err(RuntimeError::from)?;
                    Ok(true)
                }
                SessionEvtType::Connect => self.app.connected(session_id),
                _ => Ok(true),
            },
            None => Ok(true),
        }
    }

    fn dispatch_session_type(
        &mut self,
        session_id: u32,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let app = self.entries.get(session_id).and_then(|entry| entry.app);
        let Some(app) = app else {
            return Ok(());
        };
        self.dispatch_app_event(session_id, app, event)
    }

    fn dispatch_app_event(
        &mut self,
        session_id: u32,
        app: u32,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let context = self
            .entries
            .get(session_id)
            .map_or(0, |entry| entry.app_session);
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
                    .get(session_id)
                    .is_some_and(|entry| entry.accepted);
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
        session_id: u32,
        app: u32,
        event: SessionEvtType,
    ) -> RuntimeResult<()> {
        let context = self
            .entries
            .get(session_id)
            .map_or(0, |entry| entry.app_session);
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

    pub fn publish_rx_enqueue(&self, session_id: u32, produced: usize) -> RuntimeResult<()> {
        if produced == 0 {
            return Ok(());
        }
        let notify = self
            .entries
            .get(session_id)
            .is_some_and(|entry| entry.rx_fifo.set_event());
        if !notify {
            return Ok(());
        }
        let external = matches!(
            self.entries
                .get(session_id)
                .and_then(|entry| entry.application),
            Some(SessionApplication::External(_))
        );
        if external {
            if let Some(session) = self.app.app_session(session_id) {
                session
                    .push_io_event(SessionEvtType::RxEnq)
                    .map_err(RuntimeError::from)?;
            }
            return Ok(());
        }
        if let Err(error) =
            self.enqueue_session_event(SessionEvt::io(session_id, SessionEvtType::RxEnq))
        {
            if let Some(entry) = self.entries.get(session_id) {
                entry.rx_fifo.unset_event();
            }
            return Err(error);
        }
        Ok(())
    }

    pub fn publish_rx_dequeue(&self, session_id: u32, consumed: usize) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        let notify = entry.rx_fifo.needs_deq_notification(consumed);
        if notify {
            self.enqueue_session_event(SessionEvt::io(session_id, SessionEvtType::RxDeq))?;
        }
        Ok(())
    }

    pub fn publish_tx_enqueue(&self, session_id: u32, produced: usize) -> RuntimeResult<()> {
        if produced == 0 {
            return Ok(());
        }
        let notify = self
            .entries
            .get(session_id)
            .is_some_and(|entry| entry.tx_fifo.set_event());
        if notify {
            if let Err(error) =
                self.enqueue_session_event(SessionEvt::io(session_id, SessionEvtType::TxEnq))
            {
                if let Some(entry) = self.entries.get(session_id) {
                    entry.tx_fifo.unset_event();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn publish_tx_dequeue(&self, session_id: u32, consumed: usize) -> RuntimeResult<()> {
        let external = matches!(
            self.entries
                .get(session_id)
                .and_then(|entry| entry.application),
            Some(SessionApplication::External(_))
        );
        if external {
            if let Some(session) = self.app.app_session(session_id) {
                session
                    .publish_tx_dequeue(consumed)
                    .map_err(RuntimeError::from)?;
            }
            return Ok(());
        }
        let entry = self
            .entries
            .get(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        let notify = entry.tx_fifo.needs_deq_notification(consumed);
        if notify {
            self.enqueue_session_event(SessionEvt::io(session_id, SessionEvtType::TxDeq))?;
        }
        Ok(())
    }

    fn enqueue_session_event(&self, event: SessionEvt) -> RuntimeResult<()> {
        self.session_evt_q
            .enqueue_io(event)
            .map_err(|error| AppWorkerError::SessionEventQueue { error }.into())
    }

    pub fn ack_tx_up_to(&mut self, session_id: u32, bytes: usize) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
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
        session_id: u32,
        index: BufferIndex,
        offset: u32,
    ) -> RuntimeResult<RxDelivery> {
        if offset == 0 {
            let (accepted, promoted) = self.copy_rx_from_buffer(session_id, buffers, index)?;
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
    pub fn rx_available_len(&self, session_id: u32) -> Option<usize> {
        self.entries
            .get(session_id)
            .map(|entry| entry.rx_fifo.max_enqueue())
    }

    #[inline]
    pub fn pending_send_len(&self, session_id: u32) -> RuntimeResult<Option<usize>> {
        Ok(self
            .entries
            .get(session_id)
            .map(|entry| entry.tx_fifo.max_dequeue())
            .filter(|len| *len != 0))
    }

    #[inline]
    pub fn has_pending_send(&self, session_id: u32) -> bool {
        self.pending_send_len(session_id).ok().flatten().is_some()
    }

    pub fn connected(&mut self, session_id: u32) -> RuntimeResult<()> {
        let next_state = {
            let entry = self
                .entries
                .get(session_id)
                .ok_or(SessionError::SessionMissing { session_id })?;
            let Some(SessionType::Transport { state, .. }) = entry.session_type else {
                return Err(SessionError::SessionMissing { session_id }.into());
            };
            state
                .on_connected()
                .ok_or(SessionError::NotPublished { session_id })?
        };
        let publication_accepted =
            self.dispatch_application(session_id, SessionEvtType::Connect)?;
        let entry = self
            .entries
            .get_mut(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
            return Err(SessionError::PublicationRejected { session_id }.into());
        };
        *state = next_state;
        if publication_accepted {
            self.complete_application_connection(session_id)?;
        }
        Ok(())
    }

    fn complete_application_connection(&mut self, session_id: u32) -> RuntimeResult<()> {
        let connection = self
            .entries
            .get(session_id)
            .and_then(|entry| entry.application_connection);
        let Some(connection) = connection else {
            return Ok(());
        };
        self.applications
            .mark_connected(connection)
            .map_err(RuntimeError::from)?;
        let entry = self
            .entries
            .get_mut(session_id)
            .ok_or(SessionError::SessionMissing { session_id })?;
        if entry.application_connection == Some(connection) {
            entry.application_connection = None;
        }
        Ok(())
    }

    /// Applies an ACCEPTED_REPLY from the Application (VPP
    /// `session_mq_accepted_reply_handler`, session_node.c:506-581).
    ///
    /// VPP redirects main-thread arrivals back to a worker before this
    /// handler runs (session_node.c:511-515); Hammer executes this
    /// worker-local transition through the engine task queue
    /// ([`schedule_worker_task`]), the only runtime-model difference. The
    /// transition mirrors VPP:
    /// - unknown Session or application-ownership mismatch: drop the reply
    ///   (session_node.c:516-521);
    /// - error retval: disconnect only this Session
    ///   (`vnet_disconnect_session` with this handle, session_node.c:523-528);
    /// - success: Published -> Active (VPP LISTENING -> READY +
    ///   `SESSION_F_RX_READY`, session_node.c:531-533), rx notify when the
    ///   fifo is non-empty (session_node.c:550-553), and a close that arrived
    ///   while the Application was deciding is resent with the closing state
    ///   kept (session_node.c:556-563).
    pub(crate) fn accept_reply(
        &mut self,
        application: u32,
        handle: SessionHandle,
        result: Result<(), SessionControlError>,
    ) -> RuntimeResult<()> {
        let Some(session_id) = self.session_id_from_handle(handle) else {
            return Ok(());
        };
        let owner = self
            .entries
            .get(session_id)
            .and_then(|entry| match entry.application {
                Some(SessionApplication::External(owner)) => Some(owner),
                _ => None,
            });
        if owner != Some(application) {
            tracing::warn!(?handle, ?application, "app doesn't own session");
            return Ok(());
        }
        if result.is_err() {
            return self.remove_session(session_id);
        }
        let next_state = {
            let entry = self
                .entries
                .get(session_id)
                .ok_or(SessionError::SessionMissing { session_id })?;
            let Some(SessionType::Transport { state, .. }) = entry.session_type else {
                return Err(SessionError::SessionMissing { session_id }.into());
            };
            state.on_connected()
        };
        if let Some(next_state) = next_state {
            let entry = self
                .entries
                .get_mut(session_id)
                .ok_or(SessionError::SessionMissing { session_id })?;
            let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
                return Err(SessionError::SessionMissing { session_id }.into());
            };
            *state = next_state;
        } else {
            let Some(state) =
                self.entries
                    .get(session_id)
                    .and_then(|entry| match &entry.session_type {
                        Some(SessionType::Transport { state, .. }) => Some(*state),
                        None => None,
                    })
            else {
                return Err(SessionError::SessionMissing { session_id }.into());
            };
            match state {
                SessionState::AppClosed(_)
                | SessionState::TransportClosed(_)
                | SessionState::Closed(_) => {
                    // VPP: closed while waiting for the app to reply; resend
                    // the close and keep the closing state (session_node.c:
                    // 556-563).
                    self.notify_application_event(session_id, SessionEvtType::Disconnected)?;
                }
                SessionState::Creating | SessionState::Created(_) => {
                    tracing::warn!(
                        ?handle,
                        ?application,
                        "ACCEPTED_REPLY for a Session that was never published"
                    );
                }
                SessionState::Active(_)
                | SessionState::Published(_)
                | SessionState::TransportDeleted => {
                    // Duplicate reply for an already-ready Session.
                }
            }
        }
        // VPP: `if (!svm_fifo_is_empty_prod (s->rx_fifo)) app_worker_rx_notify
        // (app_wrk, s);` (session_node.c:550-553).
        let notify_rx = self
            .entries
            .get(session_id)
            .is_some_and(|entry| !entry.rx_fifo.is_empty() && entry.rx_fifo.set_event());
        if notify_rx && let Some(session) = self.app.app_session(session_id) {
            session
                .push_io_event(SessionEvtType::RxEnq)
                .map_err(RuntimeError::from)?;
        }
        Ok(())
    }

    pub fn copy_tx_to_buffer(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: u32,
        offset: usize,
        len: usize,
        index: BufferIndex,
    ) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get(session_id)
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
    fn rx_available_u32(&self, session_id: u32) -> u32 {
        self.rx_available_len(session_id)
            .map(|value| value.min(u32::MAX as usize) as u32)
            .unwrap_or(0)
    }

    #[inline]
    fn request_rx_dequeue_notification(&self, session_id: u32) {
        if let Some(entry) = self.entries.get(session_id) {
            entry.rx_fifo.want_deq_notification();
        }
    }

    fn copy_rx_from_buffer(
        &self,
        session_id: u32,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
    ) -> RuntimeResult<(u32, u32)> {
        let Some(entry) = self.entries.get(session_id) else {
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
        self.publish_rx_enqueue(session_id, accepted as usize + promoted as usize)?;
        Ok((accepted, promoted))
    }

    fn copy_rx_from_buffer_ooo(
        &self,
        session_id: u32,
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
        offset: u32,
    ) -> RuntimeResult<(u32, Option<(u32, u32)>)> {
        let Some(entry) = self.entries.get(session_id) else {
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
            transport_actions: Vec::new(),
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

    const ID: u8;

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

    /// App-initiated transport reset (VPP `SESSION_CTRL_EVT_RESET` ->
    /// `session_transport_reset` -> `transport_reset`).
    ///
    /// The Session worker has already recorded the app-close state before
    /// invoking this method. Transports without a real reset fall back to
    /// [`Self::disconnect`], matching VPP `transport_reset` closing the
    /// connection when the transport VFT has no reset entry.
    fn reset(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.disconnect(sessions, index, runtime, output_next, frame, output, now)
    }
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
        session_id: u32,
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
        session_id: u32,
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
        session_id: u32,
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
        session_id: u32,
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
                SessionEvtType::Close | SessionEvtType::HalfClose | SessionEvtType::Reset
            ) && event.thread_index != sessions.worker.slot() as u32
            {
                return Ok(());
            }
            let index = event.session_index;
            if sessions.entries.get(index).is_none() {
                return Ok(());
            }
            let session_id = u32::from(index);
            match event.evt_type {
                SessionEvtType::Close => {
                    let lower_session = sessions
                        .entries
                        .get(session_id)
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
                        .get(session_id)
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
                SessionEvtType::Reset => {
                    // VPP `session_transport_reset`: record the app-close
                    // state, then invoke the transport reset VFT. Stream
                    // children are not rerouted to a lower Session; the
                    // owning transport decides stream-local behavior and
                    // reports a typed error for unsupported contexts.
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
                        transport.reset(
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
    let index = event.session_index;
    if sessions.entries.get(index).is_none() {
        return Ok(true);
    }
    let session_id = u32::from(index);
    match event.evt_type {
        SessionEvtType::RxEnq | SessionEvtType::TxDeq => {
            sessions.dispatch_application(session_id, event.evt_type)?;
        }
        SessionEvtType::RxDeq => {
            let session_type = sessions
                .entries
                .get(session_id)
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
            if let Some(entry) = sessions.entries.get_mut(session_id) {
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
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    type Index = u32;

    use hammer_core::data_plane::{BufferFrame, NodeId, NodeState};
    use hammer_runtime::app::{
        AppSessionConfig, SessionAppContext, SessionEvt, SessionEvtType, SessionFlags,
        SessionHandle, SessionMsgQueueError,
    };
    use hammer_runtime::attach::AppServer;
    use hammer_runtime::session::SessionStreamDirection;
    use hammer_runtime::{
        AttachError, DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine,
        NodeRuntimeData, RuntimeError, RuntimeRegistry, RuntimeResult,
    };

    use super::{
        DEFAULT_SESSION_POOL_CAPACITY, SessionDgramArgs, SessionEndpointRole, SessionEntry,
        SessionMain, SessionMigrateResult, SessionQueueNext, SessionState,
        SessionTransportWorkerActions, SessionType, SessionWorker, SessionWorkerState,
        queue_for_worker,
    };
    use crate::session::ApplicationMain;
    use crate::session::application::{ApplicationError, ApplicationMqResources};
    use crate::session::error::{SessionError, SessionTransportActionError};
    use crate::session::node::{
        SessionQueueNode, SessionQueueOutput, register_app_session_input_node,
        register_session_queue_node,
    };
    use crate::session::protocol::SessionAppCallbacks;

    static SESSION_APP_CALLBACK_VALUE: AtomicU64 = AtomicU64::new(0);
    static SESSION_LISTEN_BARRIER_SEEN: AtomicBool = AtomicBool::new(false);
    static SESSION_UNLISTEN_BARRIER_SEEN: AtomicBool = AtomicBool::new(false);
    static SESSION_LISTEN_APPLICATION: AtomicU64 = AtomicU64::new(0);
    static SESSION_LISTEN_OPAQUE: AtomicU64 = AtomicU64::new(0);
    static SESSION_LISTEN_ATTEMPTED: AtomicU64 = AtomicU64::new(0);

    fn observe_listen_barrier(
        _: hammer_runtime::app::SessionHandle,
        application: u32,
        opaque: Option<u64>,
        _: hammer_runtime::SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        SESSION_LISTEN_BARRIER_SEEN.store(
            Engine::with_current(|engine| engine.worker_barrier().is_pending()).unwrap_or(false),
            Ordering::SeqCst,
        );
        SESSION_LISTEN_APPLICATION.store(application, Ordering::SeqCst);
        SESSION_LISTEN_OPAQUE.store(opaque.unwrap_or_default(), Ordering::SeqCst);
        Ok(())
    }

    fn observe_unlisten_barrier(_: hammer_runtime::app::SessionHandle) -> RuntimeResult<()> {
        SESSION_UNLISTEN_BARRIER_SEEN.store(
            Engine::with_current(|engine| engine.worker_barrier().is_pending()).unwrap_or(false),
            Ordering::SeqCst,
        );
        Ok(())
    }

    fn fail_listen(
        listener: hammer_runtime::app::SessionHandle,
        _: u32,
        _: Option<u64>,
        _: hammer_runtime::SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        SESSION_LISTEN_ATTEMPTED.store(listener.into(), Ordering::SeqCst);
        Err(RuntimeError::config_validation(
            "test transport listen failure",
        ))
    }

    fn fail_unlisten(_: hammer_runtime::app::SessionHandle) -> RuntimeResult<()> {
        Err(RuntimeError::config_validation(
            "test transport unlisten failure",
        ))
    }

    fn stream_connect_must_not_run(_: hammer_runtime::SessionConnectEndpoint) -> RuntimeResult<()> {
        Err(RuntimeError::config_validation(
            "wrong-worker CONNECT_STREAM callback was invoked",
        ))
    }

    fn connect_endpoint_has_worker_one(
        endpoint: hammer_runtime::SessionConnectEndpoint,
    ) -> RuntimeResult<()> {
        if endpoint.worker == DataWorkerId::new(1) {
            return Ok(());
        }
        Err(RuntimeError::config_validation(
            "ordinary CONNECT selected the wrong worker",
        ))
    }

    fn session_app_rx_callback(
        _: &mut SessionWorker<Index>,
        session: u32,
        context: u64,
    ) -> RuntimeResult<()> {
        SESSION_APP_CALLBACK_VALUE.store(
            (session.into() << 32) | (context & 0xffff_ffff),
            Ordering::SeqCst,
        );
        Ok(())
    }

    fn session_app_connected_callback(
        _: &mut SessionWorker<Index>,
        session: u32,
        context: u64,
    ) -> RuntimeResult<()> {
        SESSION_APP_CALLBACK_VALUE.store(
            (session.into() << 32) | (context & 0xffff_ffff),
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
        Session(#[from] SessionError),
        #[error(transparent)]
        TransportAction(#[from] SessionTransportActionError),
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
    fn migration_is_unavailable_after_session_shutdown_begins() {
        let applications = ApplicationMain::new(1);
        let main = SessionMain::new(2, applications);
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
        let index = runtime
            .alloc_index_with_bytes(b"")
            .expect("test datagram buffer");
        let local = "127.0.0.1:9000".parse().expect("local endpoint");
        let remote = "127.0.0.1:50000".parse().expect("remote endpoint");
        let tuple = (1, local, remote);
        let old_handle = SessionHandle::new(1, 0);

        main.begin_session_migration_shutdown();

        assert_eq!(
            main.program_thread_migration(
                &runtime,
                DataWorkerId::new(1),
                old_handle,
                tuple,
                SessionDgramArgs {
                    index,
                    payload_offset: 0,
                    payload_len: 0,
                    urgent: false,
                    return_node: NodeId::new(0),
                },
            ),
            SessionMigrateResult::Unavailable
        );
        assert!(!main.lookup_connection(local, remote, tuple.0));
        runtime.buffers().drop_index_owned_with_trace(index, |_| {});
    }

    #[test]
    fn migration_snapshot_rejects_transport_closing_session() -> Result<(), SessionTestFailure> {
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
        let applications = ApplicationMain::new(1);
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )?;
        let transport_index = 7u32;
        let session_id = sessions.insert_unbound_transport_session_for_test(1, transport_index)?;

        assert!(sessions.migration_snapshot(session_id).is_some());
        sessions.notify_transport_closing(Some(&runtime), session_id, transport_index)?;
        assert!(sessions.migration_snapshot(session_id).is_none());
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
                Some(observe_listen_barrier),
                Some(observe_unlisten_barrier),
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
            application
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

        let attempted = SESSION_LISTEN_ATTEMPTED.load(Ordering::SeqCst);
        assert_ne!(attempted, 0);
        assert!(
            main.with_listener(SessionHandle::new(attempted as u32, 0), |_| ())
                .is_err()
        );
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
                Some(observe_listen_barrier),
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
    fn ordinary_connect_uses_session_owned_worker_policy() {
        let main = SessionMain::new(3, ApplicationMain::new(1));
        let endpoint = hammer_runtime::SessionConnectEndpoint::new(
            "127.0.0.1:4433".parse().expect("remote endpoint"),
            None,
            DataWorkerId::new(2),
            4,
            1,
            None,
            None,
        );
        main.connect(
            hammer_runtime::SessionTransportRegistration::new(
                "connect-test",
                None,
                None,
                Some(connect_endpoint_has_worker_one),
            ),
            endpoint,
        )
        .expect("ordinary CONNECT must use a configured Session worker");
    }

    #[test]
    fn connect_stream_drops_endpoint_for_the_wrong_parent_worker() {
        let main = SessionMain::new(2, ApplicationMain::new(1));
        let parent = SessionHandle::new(7, 1);
        let endpoint = hammer_runtime::SessionConnectEndpoint::new_stream(
            "127.0.0.1:4433".parse().expect("remote endpoint"),
            None,
            DataWorkerId::new(0),
            11,
            1,
            parent,
            hammer_runtime::app::SessionFlags::empty(),
            None,
            None,
        );
        let error = main
            .connect_stream(
                hammer_runtime::SessionTransportRegistration::with_connect_stream(
                    "stream-test",
                    None,
                    None,
                    None,
                    Some(stream_connect_must_not_run),
                ),
                endpoint,
            )
            .expect_err("wrong-worker CONNECT_STREAM must be dropped");

        assert!(matches!(
            &error,
            super::SessionError::ConnectStreamWrongWorker { .. }
        ));
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

        let session_id =
            sessions.construct_stream_sessions(1, 7u32, 7, application, None, None, None, true)?;
        sessions.connection_published(session_id)?;
        sessions.connected(session_id)?;
        sessions.notify_transport_closing(Some(&runtime), session_id, 7u32)?;

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
    fn external_connect_keeps_session_tracked() -> Result<(), SessionTestFailure> {
        let socket_path = format!(
            "/tmp/hammer-external-connect-tracked-{}.sock",
            std::process::id()
        );
        let server = AppServer::bind(&socket_path, 4)?;
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_application_mq_for_test(application)?;
        let application_connection =
            applications.register_connection(application, 0, None, None, None)?;
        let connection_id = application_connection;

        let session_id = sessions.stream_connect_pending(1, 7u32, connection_id)?;
        assert!(sessions.has_session(session_id));
        sessions.complete_stream_connect(session_id)?;
        assert!(
            sessions.has_session(session_id),
            "external CONNECT keeps the Session tracked after publication"
        );
        assert!(
            sessions.app_session(session_id).is_some(),
            "external CONNECT attaches the App Session"
        );
        Ok(())
    }

    #[test]
    fn external_stream_connect_propagates_opaque_to_session_app_endpoint()
    -> Result<(), SessionTestFailure> {
        // VPP `session_open_stream` (session.c:1412) sets `s->opaque = sep->opaque`
        // on the external stream child; the Session public seam must expose it.
        let socket_path = format!(
            "/tmp/hammer-external-connect-opaque-{}.sock",
            std::process::id()
        );
        let server = AppServer::bind(&socket_path, 4)?;
        let applications = ApplicationMain::new(2);
        let application = applications.attach()?;
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_application_mq_for_test(application)?;

        let opaque_connection =
            applications.register_connection(application, 0, None, None, Some(0x77))?;
        let opaque_id = sessions.stream_connect_pending(1, 7u32, opaque_connection)?;
        assert_eq!(
            sessions.session_app_endpoint(opaque_id),
            Some((application, None, Some(0x77), None)),
            "external stream child exposes the pending ApplicationConnection opaque"
        );

        let plain_connection =
            applications.register_connection(application, 1, None, None, None)?;
        let plain_id = sessions.stream_connect_pending(1, 8u32, plain_connection)?;
        assert_eq!(
            sessions.session_app_endpoint(plain_id),
            Some((application, None, None, None)),
            "external stream child without opaque stays None"
        );
        Ok(())
    }

    #[test]
    fn transport_closing_without_runtime_notifies_external_app() -> Result<(), SessionTestFailure> {
        let applications = ApplicationMain::new(1);
        let application = applications.attach()?;
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_application_mq_for_test(application)?;
        let session_id =
            sessions.construct_stream_sessions(1, 7u32, 7, application, None, None, None, true)?;
        sessions.connection_published(session_id)?;
        sessions.connected(session_id)?;
        sessions.notify_transport_closing(None, session_id, 7u32)?;

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
        let session_id =
            sessions.construct_stream_sessions(1, 7u32, 7, application, None, None, None, true)?;
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
        sessions.enqueue_rx(&runtime.buffers(), session_id, ingress, 0)?;
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
    fn app_publication_queue_full_keeps_active_session_pending() {
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
            .construct_stream_sessions(1, 1u32, 1, application, None, None, None, true)
            .expect("first accepted Session");
        sessions
            .connection_published(first)
            .expect("publish first connection");
        sessions.connected(first).expect("fill publication queue");

        let second_index = 2u32;
        let second = sessions
            .construct_stream_sessions(1, second_index, 2, application, None, None, None, true)
            .expect("second accepted Session");
        sessions
            .connection_published(second)
            .expect("publish second connection");
        sessions
            .connected(second)
            .expect("full publication queue defers CONNECTED message");

        assert!(sessions.has_session(second));
        assert!(sessions.rollback_session_creation(second).is_err());
        assert!(sessions.app.has_pending_connected_sessions());
    }

    #[test]
    fn app_control_queue_full_keeps_connect_retry_pending() {
        let socket_path = std::path::PathBuf::from(format!(
            "/tmp/hammer-sp-ctrl-full-{}.sock",
            std::process::id()
        ));
        let socket_path = socket_path.to_str().expect("socket path");
        let server = AppServer::bind(socket_path, 2).expect("bind App server");
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach Application");
        let queue = ApplicationMqResources::create_local(application, 1, 128)
            .expect("Application MQ resources")
            .queue(DataWorkerId::new(0))
            .expect("worker Application MQ")
            .clone();
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            Some(server.publisher()),
        )
        .expect("Session worker with external App publication");
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

        let session_id = sessions
            .construct_stream_sessions(1, 1u32, 1, application, None, None, None, true)
            .expect("accepted Session");
        let session = sessions
            .app_session(session_id)
            .cloned()
            .expect("external App Session");
        while session.push_control_event(SessionEvtType::RxEnq).is_ok() {}

        sessions
            .connection_published(session_id)
            .expect("publish connection");
        sessions
            .connected(session_id)
            .expect("full CTRL queue defers connection notification");

        assert!(sessions.has_session(session_id));
        assert!(sessions.app.has_pending_connected_sessions());
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
            .construct_stream_sessions(1, 1u32, 1, application, None, None, None, true)
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
            .construct_stream_sessions(1, 2u32, 2, application, None, None, None, true)
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
        let connection_index = 3u32;
        let session_id = sessions
            .construct_stream_sessions(1, connection_index, 3, application, None, None, None, true)
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
        assert!(
            !SessionQueueNode::install_worker_attachment(
                &runtime,
                data,
                SessionQueueNext::from_slot(0),
                test_dispatch,
                test_dispatch,
            )
            .expect("skip duplicate dispatch")
        );
        assert!(
            SessionQueueNode::remove_worker_attachment(
                &runtime,
                data,
                SessionQueueNext::from_slot(0),
                test_dispatch,
                test_dispatch,
            )
            .expect("remove dispatch")
        );

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
        assert_eq!(dispatches, 0);
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
            .enqueue_ctrl(SessionEvt::ctrl(
                SessionHandle::new(1, 0),
                SessionEvtType::Close,
            ))
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
    fn migrated_session_accept_reschedules_existing_fifo_events() -> Result<(), SessionTestFailure>
    {
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
            2,
            rx_fifo.clone(),
            tx_fifo.clone(),
        ))?;
        let transport_index = 7u32;
        sessions.finish_transport_creation(session_id, transport_index)?;
        assert!(sessions.connection_published(session_id)?);

        assert_eq!(tx_fifo.enqueue(b"tx"), 2);
        assert_eq!(rx_fifo.enqueue(b"rx"), 2);
        assert!(rx_fifo.set_event());

        sessions.accept_migrated_session(session_id)?;

        assert!(matches!(
            sessions
                .entries
                .get(session_id)
                .and_then(|entry| entry.session_type),
            Some(SessionType::Transport {
                state: SessionState::Active(_),
                ..
            })
        ));
        assert!(sessions.new_io_events.iter().any(|event| {
            event.session_index == session_id && event.evt_type == SessionEvtType::TxEnq
        }));
        assert!(sessions.new_io_events.iter().any(|event| {
            event.session_index == session_id && event.evt_type == SessionEvtType::RxEnq
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
                1,
                7u32,
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

        queue.enqueue_io(SessionEvt::io(target, SessionEvtType::TxEnq))?;
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
                    .any(|event| event.session_index == target)
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
        })?;

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
        let target =
            sessions.construct_stream_sessions(1, 7u32, 7, application, None, None, None, true)?;
        sessions.connection_published(target)?;
        sessions.connected(target)?;

        (0..super::DEFAULT_SESSION_EVENT_CAPACITY).try_for_each(|session_index| {
            sessions.session_evt_q.enqueue_io(SessionEvt::io(
                session_index as u32 + 1,
                SessionEvtType::TxEnq,
            ))
        })?;
        let event = SessionEvt::io(target, SessionEvtType::TxEnq);
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
        SESSION_APP_CALLBACK_VALUE.store(0, Ordering::SeqCst);
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
            .construct_stream_sessions(1, 9u32, 1, application, Some(0), Some(99), None, false)
            .expect("construct Session App session");
        sessions
            .set_app_session(session_id, 0xABCD)
            .expect("set opaque context");
        sessions
            .install_session_app(
                0,
                SessionAppCallbacks {
                    builtin_rx: Some(session_app_rx_callback),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");

        sessions
            .dispatch_application(session_id, SessionEvtType::RxEnq)
            .expect("dispatch exact Session App RX event");
        assert_eq!(
            SESSION_APP_CALLBACK_VALUE.load(Ordering::SeqCst),
            (session_id.into() << 32) | 0xABCD
        );
    }

    #[test]
    fn session_app_connected_dispatches_exact_opaque_context() {
        SESSION_APP_CALLBACK_VALUE.store(0, Ordering::SeqCst);
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
            .construct_stream_sessions(1, 11u32, 1, application, Some(0), None, None, false)
            .expect("construct Session App session");
        sessions
            .set_app_session(session_id, 0x1234)
            .expect("set opaque context");
        sessions
            .install_session_app(
                0,
                SessionAppCallbacks {
                    connected: Some(session_app_connected_callback),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");

        sessions
            .dispatch_application(session_id, SessionEvtType::Connect)
            .expect("dispatch exact Session App connected event");
        assert_eq!(
            SESSION_APP_CALLBACK_VALUE.load(Ordering::SeqCst),
            (session_id.into() << 32) | 0x1234
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct Session App session");

        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish upper Session from callback");
        assert!(sessions.app_session(upper).is_some());
        assert_eq!(
            sessions
                .entries
                .get(upper)
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

        fn count_session_app_connected(
            _: &mut SessionWorker<Index>,
            _: u32,
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
                0,
                SessionAppCallbacks {
                    connected: Some(count_session_app_connected),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");
        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let transport = 3;
        let transport_index = 7u32;
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
                .get(upper)
                .and_then(|entry| entry.lower_session),
            Some(lower)
        );

        sessions
            .publish_connected_transport_session(upper, None)
            .expect("publish connected upper transport Session");
        assert_eq!(SESSION_APP_CONNECTED_CALLS.load(Ordering::SeqCst), 0);
        let state = sessions
            .entries
            .get(upper)
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let transport = 3;
        let transport_index = 7u32;
        let upper = sessions
            .create_upper_transport_session(lower, transport, transport_index, 0x55)
            .expect("create upper transport Session");
        sessions
            .publish_connected_transport_session(upper, None)
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

    #[test]
    fn removing_upper_app_session_clears_lower_link_and_preserves_lower() {
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish upper Session");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            Some(upper),
            "upper creation attaches the reverse link on the lower"
        );

        sessions
            .remove_session(upper)
            .expect("direct upper removal");
        assert!(!sessions.has_session(upper), "upper is removed");
        assert!(sessions.has_session(lower), "lower survives upper removal");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            None,
            "direct upper removal clears the lower reverse link"
        );

        let replacement = sessions
            .create_upper_session(lower, 0x66)
            .expect("lower accepts a fresh upper after the link was cleared");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            Some(replacement),
            "the fresh upper re-attaches the reverse link"
        );
    }

    #[test]
    fn duplicate_upper_attachment_rejected_without_overwrite_or_leak() {
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("first upper");

        let error = sessions
            .create_upper_session(lower, 0x66)
            .expect_err("duplicate upper attachment is rejected");
        assert!(matches!(
            &error,
            RuntimeError::Subsystem { source, .. }
                if matches!(
                    source.downcast_ref::<SessionError>(),
                    Some(SessionError::UpperSessionAlreadyAttached { lower: linked })
                        if *linked == lower
                )
        ));

        assert!(sessions.has_session(upper), "first upper survives");
        assert_eq!(
            sessions
                .entries
                .get(upper)
                .and_then(|entry| entry.lower_session),
            Some(lower),
            "first upper keeps its forward link"
        );
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            Some(upper),
            "the existing reverse link is not overwritten"
        );
        assert_eq!(sessions.entries.len(), 2, "no leaked upper entry");
    }

    #[test]
    fn rollback_upper_session_removes_upper_and_preserves_lower_link() {
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("create upper Session");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            Some(upper),
            "the lower owns the upper before rollback"
        );

        sessions
            .remove_upper_session(upper)
            .expect("roll back the upper Session");

        assert!(!sessions.has_session(upper), "the upper is removed");
        assert!(sessions.has_session(lower), "the lower survives rollback");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            None,
            "rollback clears the lower owner link"
        );
        assert!(
            sessions.app.detach_session(upper).is_none(),
            "rollback detached the external App attachment"
        );
        assert_eq!(sessions.entries.len(), 1, "only the lower remains");
    }

    #[test]
    fn rollback_upper_session_cleanup_error_preserves_state_and_primary_error() {
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
                0,
                SessionAppCallbacks {
                    cleanup: Some(fail_cleanup_for_session),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");
        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("create upper Session");

        SESSION_CLEANUP_ATTEMPTS.store(0, Ordering::SeqCst);
        SESSION_CLEANUP_SUCCESSES.store(0, Ordering::SeqCst);
        SESSION_CLEANUP_FAILING.store(upper.into(), Ordering::SeqCst);
        let error = sessions
            .remove_upper_session(upper)
            .expect_err("the cleanup error propagates");
        assert!(matches!(
            &error,
            RuntimeError::Subsystem { source, .. }
                if matches!(
                    source.downcast_ref::<SessionError>(),
                    Some(SessionError::PublicationRejected { session_id })
                        if *session_id == upper
                )
        ));
        assert!(
            sessions.has_session(upper),
            "the upper stays live after a failed cleanup"
        );
        assert!(sessions.has_session(lower), "the lower is untouched");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            Some(upper),
            "the owner link is preserved when cleanup fails"
        );
        assert_eq!(
            SESSION_CLEANUP_ATTEMPTS.load(Ordering::SeqCst),
            1,
            "the cleanup callback ran exactly once"
        );
        assert_eq!(
            SESSION_CLEANUP_SUCCESSES.load(Ordering::SeqCst),
            0,
            "the failing cleanup counts no success"
        );
    }

    static SESSION_CLEANUP_STALE_ATTEMPTS: AtomicU64 = AtomicU64::new(0);

    fn count_stale_cleanup_attempt(
        _: &mut SessionWorker<Index>,
        _: u32,
        _: u64,
    ) -> RuntimeResult<()> {
        SESSION_CLEANUP_STALE_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[test]
    fn rollback_upper_session_stale_generation_never_removes_reused_slot() {
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
                0,
                SessionAppCallbacks {
                    cleanup: Some(count_stale_cleanup_attempt),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");
        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("first upper");

        // Reentrant cleanup removes the upper and the direct-index Pool free
        // list immediately reuses its numeric index for a fresh upper on the
        // same lower.
        sessions.entries.remove(upper);
        sessions.detach_upper_session(lower, upper);
        let fresh = sessions
            .create_upper_session(lower, 0x66)
            .expect("fresh upper reuses the slot");
        assert_eq!(fresh, upper, "the fresh upper occupies the removed slot");
        SESSION_CLEANUP_STALE_ATTEMPTS.store(0, Ordering::SeqCst);
        sessions
            .remove_upper_session(upper)
            .expect("a stale rollback id is a safe no-op");
        assert_eq!(
            SESSION_CLEANUP_STALE_ATTEMPTS.load(Ordering::SeqCst),
            0,
            "a stale rollback id never runs the fresh occupant's cleanup callback"
        );

        assert!(
            sessions.has_session(fresh),
            "the reused occupant survives the stale rollback"
        );
        assert!(
            !sessions.has_session(upper),
            "the stale id is not the occupant"
        );
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            Some(fresh),
            "the lower owner link still names the fresh upper"
        );
        assert_eq!(sessions.entries.len(), 2, "no session is swept");
    }

    static SESSION_CLEANUP_REMOVING: AtomicU64 = AtomicU64::new(0);

    fn remove_upper_on_cleanup(
        sessions: &mut SessionWorker<Index>,
        session_id: u32,
        _: u64,
    ) -> RuntimeResult<()> {
        SESSION_CLEANUP_REMOVING.fetch_add(1, Ordering::SeqCst);
        let lower = sessions
            .entries
            .get(session_id)
            .and_then(|entry| entry.lower_session);
        sessions.entries.remove(session_id);
        if let Some(lower) = lower {
            sessions.detach_upper_session(lower, session_id);
        }
        Ok(())
    }

    #[test]
    fn rollback_upper_session_after_reentrant_removal_is_safe_noop() {
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
                0,
                SessionAppCallbacks {
                    cleanup: Some(remove_upper_on_cleanup),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");
        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("create upper Session");

        SESSION_CLEANUP_REMOVING.store(0, Ordering::SeqCst);
        sessions
            .remove_upper_session(upper)
            .expect("the callback already removed the upper, so rollback is a no-op");

        assert!(!sessions.has_session(upper), "the upper is gone");
        assert!(sessions.has_session(lower), "the lower is untouched");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            None,
            "the reentrant cleanup cleared the owner link"
        );
        assert_eq!(sessions.entries.len(), 1, "no second removal happened");
        assert_eq!(
            SESSION_CLEANUP_REMOVING.load(Ordering::SeqCst),
            1,
            "the cleanup callback ran exactly once"
        );
    }

    #[test]
    fn upper_creation_failure_rolls_back_lower_link() {
        // A real publisher is required to make AppWorker::connected fail
        // after the upper is attached (queue closed), the only upper-creation
        // failure that happens after the lower reverse link is attached.
        let socket_path = format!("/tmp/hammer-upper-rollback-{}.sock", std::process::id());
        let server = hammer_runtime::attach::AppServer::bind(&socket_path, 1)
            .expect("bind App server with a single publication slot");
        let publisher = server.publisher();
        let applications = ApplicationMain::new(4);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            Some(publisher),
        )
        .expect("Session worker");
        drop(server); // closes the publication queue

        sessions
            .install_application_mq_for_test(application)
            .expect("install test Application MQ");
        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");

        let error = sessions
            .create_upper_session(lower, 0x55)
            .expect_err("connected publication fails once the server is dropped");
        assert!(matches!(
            &error,
            RuntimeError::Attach(AttachError::PublicationQueueClosed)
        ));
        assert_eq!(sessions.entries.len(), 1, "upper entry rolls back");
        assert!(
            sessions.has_session(lower),
            "lower survives the failed creation"
        );
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            None,
            "failed creation leaves the lower link unset"
        );
    }

    #[test]
    fn stale_upper_link_never_removes_reused_slot() {
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let filler = sessions
            .construct_stream_sessions(3, 7u32, 1, application, Some(0), None, None, false)
            .expect("construct filler Session");

        // Forge a stale reverse link to a direct index that is not live.
        let stale = u32::from(filler.wrapping_add(1));
        sessions
            .entries
            .get_mut(lower)
            .expect("lower entry")
            .upper_session = Some(stale);

        sessions
            .remove_session(lower)
            .expect("lower removal tolerates a stale upper link");
        assert!(
            sessions.has_session(filler),
            "a missing direct index must not remove a live session"
        );
        assert!(!sessions.has_session(lower), "lower itself is removed");
    }

    #[test]
    fn transport_upper_teardown_and_direct_removal_use_the_reverse_link() {
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
        let transport = 3;
        let transport_index = 7u32;

        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_transport_session(lower, transport, transport_index, 0x55)
            .expect("create upper transport Session");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            Some(upper),
            "upper transport creation attaches the reverse link"
        );

        sessions
            .remove_session(lower)
            .expect("remove lower Session");
        assert!(
            !sessions.has_session(upper),
            "lower removal removes the linked upper transport Session"
        );
        assert!(!sessions.has_session(lower), "lower is removed");

        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct a second lower Session");
        let upper = sessions
            .create_upper_transport_session(lower, transport, transport_index, 0x55)
            .expect("create upper transport Session");
        sessions
            .remove_session(upper)
            .expect("direct upper transport removal");
        assert!(sessions.has_session(lower), "lower survives upper removal");
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            None,
            "direct upper transport removal clears the reverse link"
        );
    }

    #[test]
    fn lower_session_returns_generation_checked_forward_link() {
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish upper Session from callback");
        assert_eq!(sessions.lower_session(upper), Some(lower));
        assert_eq!(sessions.lower_session(lower), None);
        sessions
            .remove_session(upper)
            .expect("remove upper Session");
        assert_eq!(sessions.lower_session(upper), None);
    }

    #[test]
    fn upper_session_returns_generation_checked_owner_link() {
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish upper Session from callback");
        assert_eq!(sessions.upper_session(lower), Some(upper));
        assert_eq!(sessions.upper_session(upper), None);
        sessions
            .remove_upper_session(upper)
            .expect("remove upper Session");
        assert_eq!(sessions.upper_session(lower), None);
    }

    #[test]
    fn lower_removal_follows_owner_link_only_leaving_forward_link_orphan() {
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let upper = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish upper Session from callback");
        assert_eq!(
            sessions
                .entries
                .get(upper)
                .and_then(|entry| entry.lower_session),
            Some(lower),
            "upper carries the forward link"
        );

        // Break the reverse owner link so the upper is reachable only through
        // its own forward `lower_session` pointer.
        sessions
            .entries
            .get_mut(lower)
            .expect("lower entry")
            .upper_session = None;

        sessions.remove_session(lower).expect("lower removal");
        assert!(!sessions.has_session(lower), "lower is removed");
        assert!(
            sessions.has_session(upper),
            "a forward-link-only orphan is not swept: the cascade follows the owner link"
        );
        assert_eq!(
            sessions
                .entries
                .get(upper)
                .and_then(|entry| entry.lower_session),
            Some(lower),
            "the orphan keeps its forward link and is untouched"
        );
        assert_eq!(sessions.entries.len(), 1, "only the orphan remains");
    }

    #[test]
    fn lower_removal_walks_full_owner_chain_lower_mid_top() {
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
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let mid = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish mid upper Session");
        let top = sessions
            .create_upper_session(mid, 0x66)
            .expect("publish top upper Session");
        assert_eq!(
            sessions
                .entries
                .get(mid)
                .and_then(|entry| entry.lower_session),
            Some(lower),
            "mid points back at lower"
        );
        assert_eq!(
            sessions
                .entries
                .get(lower)
                .and_then(|entry| entry.upper_session),
            Some(mid),
            "lower owns mid"
        );
        assert_eq!(
            sessions
                .entries
                .get(top)
                .and_then(|entry| entry.lower_session),
            Some(mid),
            "top points back at mid"
        );
        assert_eq!(
            sessions
                .entries
                .get(mid)
                .and_then(|entry| entry.upper_session),
            Some(top),
            "mid owns top"
        );

        sessions
            .remove_session(lower)
            .expect("remove lower Session");
        assert!(!sessions.has_session(lower), "lower is removed");
        assert!(
            !sessions.has_session(mid),
            "mid is removed through the owner link"
        );
        assert!(
            !sessions.has_session(top),
            "top is removed through the owner link"
        );
        assert_eq!(
            sessions.entries.len(),
            0,
            "the whole chain is swept iteratively"
        );
    }

    static SESSION_CLEANUP_FAILING: AtomicU64 = AtomicU64::new(0);
    static SESSION_CLEANUP_ATTEMPTS: AtomicU64 = AtomicU64::new(0);
    static SESSION_CLEANUP_SUCCESSES: AtomicU64 = AtomicU64::new(0);

    fn fail_cleanup_for_session(
        _: &mut SessionWorker<Index>,
        session_id: u32,
        _: u64,
    ) -> RuntimeResult<()> {
        SESSION_CLEANUP_ATTEMPTS.fetch_add(1, Ordering::SeqCst);
        if session_id.into() == SESSION_CLEANUP_FAILING.load(Ordering::SeqCst) {
            return Err(SessionError::PublicationRejected { session_id }.into());
        }
        SESSION_CLEANUP_SUCCESSES.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    #[test]
    fn upper_cleanup_error_stops_walk_and_preserves_remaining_chain() {
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
                0,
                SessionAppCallbacks {
                    cleanup: Some(fail_cleanup_for_session),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");
        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let mid = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish mid upper Session");
        let top = sessions
            .create_upper_session(mid, 0x66)
            .expect("publish top upper Session");

        SESSION_CLEANUP_ATTEMPTS.store(0, Ordering::SeqCst);
        SESSION_CLEANUP_SUCCESSES.store(0, Ordering::SeqCst);
        SESSION_CLEANUP_FAILING.store(mid.into(), Ordering::SeqCst);
        let error = sessions
            .remove_session(lower)
            .expect_err("the mid cleanup error propagates");
        assert!(matches!(
            &error,
            RuntimeError::Subsystem { source, .. }
                if matches!(
                    source.downcast_ref::<SessionError>(),
                    Some(SessionError::PublicationRejected { session_id })
                        if *session_id == mid
                )
        ));
        assert!(
            !sessions.has_session(lower),
            "lower is removed before the walk"
        );
        assert!(sessions.has_session(mid), "the failing upper stays live");
        assert!(
            sessions.has_session(top),
            "the walk stops at the first cleanup error"
        );
        assert_eq!(
            sessions.entries.len(),
            2,
            "exactly mid and top remain after the first cleanup error"
        );
        assert_eq!(
            SESSION_CLEANUP_ATTEMPTS.load(Ordering::SeqCst),
            1,
            "only mid's cleanup ran: the lower has no app session context, and top was never reached"
        );
        assert_eq!(
            SESSION_CLEANUP_SUCCESSES.load(Ordering::SeqCst),
            0,
            "the mid cleanup error propagates before any walk node is freed"
        );
    }

    static SESSION_CLEANUP_DETACHING_CALLS: AtomicU64 = AtomicU64::new(0);

    fn detach_child_on_cleanup(
        sessions: &mut SessionWorker<Index>,
        session_id: u32,
        _: u64,
    ) -> RuntimeResult<()> {
        SESSION_CLEANUP_DETACHING_CALLS.fetch_add(1, Ordering::SeqCst);
        let child = sessions
            .entries
            .get(session_id)
            .and_then(|entry| entry.upper_session);
        if let Some(child) = child {
            sessions.detach_upper_session(session_id, child);
        }
        Ok(())
    }

    #[test]
    fn cleanup_callback_detaching_child_stops_walk_before_removal() {
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
                0,
                SessionAppCallbacks {
                    cleanup: Some(detach_child_on_cleanup),
                    ..Default::default()
                },
            )
            .expect("install Session App callbacks");
        let lower = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct lower Session App session");
        let mid = sessions
            .create_upper_session(lower, 0x55)
            .expect("publish mid upper Session");
        let top = sessions
            .create_upper_session(mid, 0x66)
            .expect("publish top upper Session");

        SESSION_CLEANUP_DETACHING_CALLS.store(0, Ordering::SeqCst);
        sessions.remove_session(lower).expect("lower removal");
        assert!(
            !sessions.has_session(lower),
            "lower is removed before the walk"
        );
        assert!(
            sessions.has_session(mid),
            "mid is not removed: its callback detached the child, so the walk stops at the restructured link"
        );
        assert!(
            sessions.has_session(top),
            "the child detached by the cleanup callback is never swept"
        );
        assert_eq!(
            sessions
                .entries
                .get(mid)
                .and_then(|entry| entry.upper_session),
            None,
            "the callback detached the child from mid"
        );
        assert_eq!(
            sessions
                .entries
                .get(top)
                .and_then(|entry| entry.lower_session),
            Some(mid),
            "the detached child's back-link is untouched"
        );
        assert_eq!(
            SESSION_CLEANUP_DETACHING_CALLS.load(Ordering::SeqCst),
            1,
            "only mid's cleanup ran: the walk stops before any further cleanup"
        );
        assert_eq!(
            sessions.entries.len(),
            2,
            "exactly mid and top remain after the restructured link"
        );
    }

    #[test]
    fn parent_session_is_distinct_metadata_never_cascading_on_cleanup() {
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
        let parent = sessions
            .construct_stream_sessions(1, 1u32, 1, application, Some(0), None, None, false)
            .expect("construct parent transport Session");
        let child = sessions
            .construct_stream_sessions(3, 7u32, 1, application, Some(0), None, None, false)
            .expect("construct child transport Session");

        assert_eq!(sessions.parent_session(child), None);
        sessions
            .set_parent_session(child, parent)
            .expect("set transport parent");
        assert_eq!(sessions.parent_session(child), Some(parent));
        assert_eq!(
            sessions
                .entries
                .get(child)
                .and_then(|entry| entry.lower_session),
            None,
            "parent topology must not create lower attachment"
        );

        // VPP `session_alloc_for_stream` (session.c:507-523) rejects a child
        // whose parent handle is invalid; the same constraint applies here.
        let missing = 99;
        let error = sessions
            .set_parent_session(child, missing)
            .expect_err("missing parent rejects stream Session parenting");
        assert!(matches!(
            &error,
            RuntimeError::Subsystem { source, .. }
                if matches!(
                    source.downcast_ref::<SessionError>(),
                    Some(SessionError::ConnectStreamParentMissing)
                )
        ));
        assert_eq!(
            sessions.parent_session(child),
            Some(parent),
            "failed parenting must not mutate child metadata"
        );

        // Removing the parent must not sweep a child that only carries
        // parent topology: the upper/lower cascade keys on `lower_session`
        // attachment, never on `parent_session`.
        sessions
            .remove_session(parent)
            .expect("remove parent Session");
        assert!(!sessions.has_session(parent));
        assert!(sessions.has_session(child));

        // The child removes as a top-level Session: parent topology neither
        // suppresses its own cleanup nor cascades anywhere.
        sessions
            .remove_session(child)
            .expect("remove child Session");
        assert!(!sessions.has_session(child));
        assert_eq!(sessions.entries.len(), 0);
    }

    fn accepted_reply_fixture(
        application: u32,
        publisher: hammer_runtime::attach::AppSessionPublisher,
        applications: Arc<ApplicationMain>,
        session_index: u32,
    ) -> (SessionWorker<Index>, u32) {
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            Some(publisher),
        )
        .expect("Session worker with external App publication");
        sessions
            .install_application_mq_for_test(application)
            .expect("install test Application MQ");
        let session_id = sessions
            .construct_stream_sessions(
                1,
                session_index,
                session_index as u64,
                application,
                None,
                None,
                None,
                true,
            )
            .expect("accepted external Session");
        // Production pins the accepting listener in `stream_accept`
        // (VPP `session_accepted_msg_t.listener_handle`); the direct
        // construction seam used here mirrors that step.
        sessions
            .entries
            .get_mut(session_id)
            .expect("accepted Session entry")
            .listener = Some(SessionHandle::new(7, 0));
        sessions
            .publish_accepted_transport_session(session_id)
            .expect("publish ACCEPTED message");
        (sessions, session_id)
    }

    #[test]
    fn accepted_publication_carries_stream_session_flags() {
        let socket_path = std::path::PathBuf::from(format!(
            "/tmp/hammer-sr-accept-flags-{}.sock",
            std::process::id()
        ));
        let server = AppServer::bind(socket_path.to_str().expect("socket path"), 1)
            .expect("bind App server");
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach Application");
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            Arc::clone(&applications),
            Some(server.publisher()),
        )
        .expect("Session worker with external App publication");
        sessions
            .install_application_mq_for_test(application)
            .expect("install test Application MQ");
        // The first accepted Session fills the single publication slot so
        // the second Session's ACCEPTED message stays retained in the
        // pending publication and its payload can be inspected.
        let first = sessions
            .construct_stream_sessions(1, 11u32, 11, application, None, None, None, true)
            .expect("first accepted external Session");
        sessions
            .entries
            .get_mut(first)
            .expect("first accepted Session entry")
            .listener = Some(SessionHandle::new(7, 0));
        sessions
            .publish_accepted_transport_session(first)
            .expect("publish first ACCEPTED publication");
        let second = sessions
            .construct_stream_sessions(1, 12u32, 12, application, None, None, None, true)
            .expect("second accepted external Session");
        sessions
            .entries
            .get_mut(second)
            .expect("second accepted Session entry")
            .listener = Some(SessionHandle::new(7, 0));
        sessions
            .set_session_flags(
                second,
                hammer_runtime::app::SessionFlags::STREAM
                    | hammer_runtime::app::SessionFlags::UNIDIRECTIONAL,
            )
            .expect("derive QUIC stream Session flags");
        sessions
            .publish_accepted_transport_session(second)
            .expect("publish second ACCEPTED publication");
        // VPP `mq_send_session_accepted_cb` (session_api.c:255) rides the
        // child Session flags on the ACCEPTED payload.
        let accepted = sessions
            .accepted_message(second)
            .expect("retained ACCEPTED message");
        assert_eq!(
            accepted.flags,
            hammer_runtime::app::SessionFlags::STREAM
                | hammer_runtime::app::SessionFlags::UNIDIRECTIONAL
        );
        assert_eq!(accepted.session, sessions.session_handle(second));
        assert_eq!(accepted.listener, SessionHandle::new(7, 0));
        let _ = std::fs::remove_file(socket_path);
    }

    /// Inserts a bare transport Session in the pool without App publication,
    /// enough to exercise the static `accept_metadata` pool reads.
    fn insert_metadata_test_session(
        sessions: &mut SessionWorker<Index>,
        transport: u8,
        index: Index,
    ) -> u32 {
        let (rx_fifo, tx_fifo) = sessions.create_local_fifos().expect("test Session FIFOs");
        let session_id = sessions
            .insert_session_entry(SessionEntry::creating_transport(
                transport, rx_fifo, tx_fifo,
            ))
            .expect("insert test Session");
        sessions
            .finish_transport_creation(session_id, index)
            .expect("finish test Session creation");
        session_id
    }

    fn metadata_test_worker() -> SessionWorker<Index> {
        SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            ApplicationMain::new(1),
            None,
        )
        .expect("Session worker")
    }

    #[test]
    fn accept_metadata_root_never_resolves_parent_context() {
        let mut sessions = metadata_test_worker();
        let parent = insert_metadata_test_session(&mut sessions, 1, 1u32);
        sessions
            .entries
            .get_mut(parent)
            .expect("parent entry")
            .app_session = 42;
        // A root may pin its accepting listener (VPP accepted connections
        // name the listener) but the `SESSION_F_STREAM` gate keeps it from
        // resolving a parent: VPP walks `listener_handle` only on the stream
        // accept path (`http_ts_accept_stream`).
        let root = insert_metadata_test_session(&mut sessions, 1, 2u32);
        sessions.entries.get_mut(root).expect("root entry").listener =
            Some(sessions.session_handle(parent));

        let metadata = sessions
            .accept_metadata(root)
            .expect("root accept metadata");
        assert_eq!(metadata.flags, SessionFlags::empty());
        // Roots report their own construction-lifecycle role, never the
        // pinned listener's, and carry no parent context.
        assert_eq!(metadata.role, Some(SessionEndpointRole::Client));
        assert_eq!(metadata.parent_app_context, None);
    }

    #[test]
    fn accept_metadata_stream_children_carry_parent_app_context() {
        let mut sessions = metadata_test_worker();
        let parent = insert_metadata_test_session(&mut sessions, 1, 1u32);
        sessions
            .entries
            .get_mut(parent)
            .expect("parent entry")
            .app_session = 42;
        // A server connection: accepted from a listener (VPP
        // `HTTP_CONN_F_IS_SERVER`, http.c:1438).
        sessions
            .entries
            .get_mut(parent)
            .expect("parent entry")
            .accepted = true;
        let parent_handle = sessions.session_handle(parent);

        // Bidi stream child: VPP `http_ts_accept_stream` (http.c:675) resolves
        // the parent connection from the child's pinned listener handle and
        // inherits its context and endpoint role.
        let bidi = insert_metadata_test_session(&mut sessions, 1, 2u32);
        sessions
            .set_session_flags(bidi, SessionFlags::STREAM)
            .expect("derive bidi stream flags");
        sessions
            .entries
            .get_mut(bidi)
            .expect("bidi child entry")
            .listener = Some(parent_handle);
        let metadata = sessions
            .accept_metadata(bidi)
            .expect("bidi accept metadata");
        assert_eq!(metadata.flags, SessionFlags::STREAM);
        assert_eq!(metadata.role, Some(SessionEndpointRole::Server));
        assert_eq!(metadata.parent_app_context, Some(42));

        // Uni stream child inherits the same parent context and role.
        let uni = insert_metadata_test_session(&mut sessions, 1, 3u32);
        sessions
            .set_session_flags(uni, SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL)
            .expect("derive uni stream flags");
        sessions
            .entries
            .get_mut(uni)
            .expect("uni child entry")
            .listener = Some(parent_handle);
        let metadata = sessions.accept_metadata(uni).expect("uni accept metadata");
        assert_eq!(
            metadata.flags,
            SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL
        );
        assert_eq!(metadata.role, Some(SessionEndpointRole::Server));
        assert_eq!(metadata.parent_app_context, Some(42));
    }

    #[test]
    fn accept_metadata_outbound_root_reports_client() {
        let mut sessions = metadata_test_worker();
        // Outbound connect root: `stream_connect_pending` constructs with
        // `accepted` unset, so the root reports `Client` (VPP connects never
        // set `HTTP_CONN_F_IS_SERVER`).
        let root = insert_metadata_test_session(&mut sessions, 1, 1u32);
        let metadata = sessions
            .accept_metadata(root)
            .expect("root accept metadata");
        assert_eq!(metadata.role, Some(SessionEndpointRole::Client));
        assert_eq!(metadata.parent_app_context, None);
    }

    #[test]
    fn accept_metadata_stream_child_inherits_client_parent_role() {
        let mut sessions = metadata_test_worker();
        // Streams on an outbound connect root inherit its `Client` role
        // exactly as they inherit its context, regardless of stream flags.
        let parent = insert_metadata_test_session(&mut sessions, 1, 1u32);
        let parent_handle = sessions.session_handle(parent);
        for (slot, flags) in [
            (2, SessionFlags::STREAM),
            (3, SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL),
        ] {
            let child = insert_metadata_test_session(&mut sessions, 1, slot);
            sessions
                .set_session_flags(child, flags)
                .expect("derive stream flags");
            sessions
                .entries
                .get_mut(child)
                .expect("child entry")
                .listener = Some(parent_handle);
            let metadata = sessions
                .accept_metadata(child)
                .expect("child accept metadata");
            assert_eq!(
                metadata.role,
                Some(SessionEndpointRole::Client),
                "child inherits the parent connection role"
            );
        }
    }

    #[test]
    fn accept_metadata_missing_or_removed_child_returns_none() {
        let mut sessions = metadata_test_worker();
        // A Session id that was never installed (in-bounds slot, wrong
        // generation) and one that was removed both fail the pool lookup.
        assert!(sessions.accept_metadata(u32::from(1023u32)).is_none());
        let removed = insert_metadata_test_session(&mut sessions, 1, 1u32);
        sessions.entries.remove(removed).expect("remove Session");
        assert!(sessions.accept_metadata(removed).is_none());
    }

    #[test]
    fn accept_metadata_stale_or_foreign_parent_handle_yields_no_parent() {
        let mut sessions = metadata_test_worker();
        // A freed parent's handle no longer resolves: the slot is empty, so
        // `session_id_from_handle` misses exactly as VPP `session_get_from_handle`
        // misses on a freed pool element.
        let freed = insert_metadata_test_session(&mut sessions, 1, 1u32);
        let freed_handle = sessions.session_handle(freed);
        let stale_child = insert_metadata_test_session(&mut sessions, 1, 2u32);
        sessions
            .set_session_flags(stale_child, SessionFlags::STREAM)
            .expect("derive stale child stream flags");
        sessions
            .entries
            .get_mut(stale_child)
            .expect("stale child entry")
            .listener = Some(freed_handle);
        sessions.entries.remove(freed).expect("free parent");
        let metadata = sessions
            .accept_metadata(stale_child)
            .expect("stale child accept metadata");
        assert_eq!(metadata.parent_app_context, None);
        assert_eq!(metadata.role, None);

        // A handle naming another worker's slot is foreign and never resolves.
        let parent = insert_metadata_test_session(&mut sessions, 1, 3u32);
        let orphan_child = insert_metadata_test_session(&mut sessions, 1, 4u32);
        sessions
            .set_session_flags(orphan_child, SessionFlags::STREAM)
            .expect("derive orphan child stream flags");
        sessions
            .entries
            .get_mut(orphan_child)
            .expect("orphan child entry")
            .listener = Some(SessionHandle::new(
            sessions.session_handle(parent).session_index(),
            1,
        ));
        let metadata = sessions
            .accept_metadata(orphan_child)
            .expect("orphan child accept metadata");
        assert_eq!(metadata.parent_app_context, None);
        assert_eq!(metadata.role, None);
    }

    #[test]
    fn accept_metadata_parent_handle_resolves_live_slot_occupant() {
        let mut sessions = metadata_test_worker();
        // A handle pins a slot, not a Session identity: when the parent is
        // freed and the slot is reused, the child resolves the live occupant
        // (VPP `session_get_from_handle` resolves by pool slot, so a stale
        // handle sees the current entry, never a generation-checked miss).
        let first = insert_metadata_test_session(&mut sessions, 1, 1u32);
        sessions
            .entries
            .get_mut(first)
            .expect("first parent entry")
            .app_session = 100;
        let child = insert_metadata_test_session(&mut sessions, 1, 2u32);
        sessions
            .set_session_flags(child, SessionFlags::STREAM)
            .expect("derive child stream flags");
        sessions
            .entries
            .get_mut(child)
            .expect("child entry")
            .listener = Some(sessions.session_handle(first));
        sessions.entries.remove(first).expect("free first parent");
        let second = insert_metadata_test_session(&mut sessions, 1, 3u32);
        assert_eq!(second, first, "the freed parent slot is reused");
        sessions
            .entries
            .get_mut(second)
            .expect("second parent entry")
            .app_session = 200;
        // The slot occupant is a server connection; the child inherits its
        // role together with its context.
        sessions
            .entries
            .get_mut(second)
            .expect("second parent entry")
            .accepted = true;

        let metadata = sessions
            .accept_metadata(child)
            .expect("child accept metadata");
        assert_eq!(
            metadata.parent_app_context,
            Some(200),
            "the stale parent handle resolves the live slot occupant"
        );
        assert_eq!(
            metadata.role,
            Some(SessionEndpointRole::Server),
            "the role follows the live slot occupant"
        );
    }

    #[test]
    fn accepted_reply_transitions_published_session_to_active_with_rx_notify() {
        let socket_path = std::path::PathBuf::from(format!(
            "/tmp/hammer-sr-accept-reply-{}.sock",
            std::process::id()
        ));
        let server = AppServer::bind(socket_path.to_str().expect("socket path"), 4)
            .expect("bind App server");
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach Application");
        let (mut sessions, session_id) = accepted_reply_fixture(
            application,
            server.publisher(),
            Arc::clone(&applications),
            2,
        );
        let handle = sessions.session_handle(session_id);
        let entry = sessions
            .entries
            .get(session_id)
            .expect("accepted Session entry");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::Published(_),
                ..
            })
        ));
        // Data that arrived while the Application was deciding: the success
        // reply must issue the rx notification VPP sends after READY
        // (session_node.c:550-553).
        entry.rx_fifo.enqueue(&[1_u8; 16]);
        sessions
            .accept_reply(application, handle, Ok(()))
            .expect("apply ACCEPTED_REPLY");
        let entry = sessions
            .entries
            .get(session_id)
            .expect("active Session entry");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::Active(_),
                ..
            })
        ));
        let app_session = sessions
            .app
            .app_session(session_id)
            .expect("Application Session");
        let event = app_session
            .evt_q()
            .dequeue()
            .expect("dequeue ACCEPTED control event")
            .expect("ACCEPTED control event");
        assert_eq!(event.evt_type, SessionEvtType::Accepted);
        let event = app_session
            .evt_q()
            .dequeue()
            .expect("dequeue rx notification")
            .expect("rx notification event");
        assert_eq!(event.evt_type, SessionEvtType::RxEnq);
    }

    #[test]
    fn accepted_reply_resends_pending_close_removes_on_error_and_drops_foreign_owner() {
        let socket_path = std::path::PathBuf::from(format!(
            "/tmp/hammer-sr-accept-close-{}.sock",
            std::process::id()
        ));
        let server = AppServer::bind(socket_path.to_str().expect("socket path"), 4)
            .expect("bind App server");
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach Application");
        let publisher = server.publisher();

        // A close that arrived while the Application was deciding is recorded
        // without notifying (state.rs) and resent by ACCEPTED_REPLY with the
        // closing state kept (session_node.c:556-563).
        let (mut sessions, session_id) =
            accepted_reply_fixture(application, publisher.clone(), Arc::clone(&applications), 2);
        let handle = sessions.session_handle(session_id);
        let index = 2u32;
        sessions
            .notify_transport_closing(None, session_id, index)
            .expect("record transport closing");
        let entry = sessions
            .entries
            .get(session_id)
            .expect("closing Session entry");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::TransportClosed(_),
                ..
            })
        ));
        {
            let app_session = sessions
                .app
                .app_session(session_id)
                .expect("Application Session");
            let event = app_session
                .evt_q()
                .dequeue()
                .expect("dequeue ACCEPTED control event")
                .expect("ACCEPTED control event");
            assert_eq!(event.evt_type, SessionEvtType::Accepted);
            assert!(
                app_session.evt_q().dequeue().expect("dequeue").is_none(),
                "pending close is recorded without notifying until ACCEPTED_REPLY"
            );
        }
        sessions
            .accept_reply(application, handle, Ok(()))
            .expect("apply ACCEPTED_REPLY to closing Session");
        let entry = sessions
            .entries
            .get(session_id)
            .expect("closing Session entry after reply");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::TransportClosed(_),
                ..
            })
        ));
        assert_eq!(
            sessions
                .app
                .app_session(session_id)
                .expect("Application Session")
                .evt_q()
                .dequeue()
                .expect("dequeue close resend")
                .expect("close resend event")
                .evt_type,
            SessionEvtType::Disconnected
        );

        // An error reply disconnects only this child Session
        // (vnet_disconnect_session with this handle, session_node.c:523-528).
        let (mut sessions, session_id) =
            accepted_reply_fixture(application, publisher.clone(), Arc::clone(&applications), 3);
        let handle = sessions.session_handle(session_id);
        sessions
            .accept_reply(
                application,
                handle,
                Err(hammer_runtime::app::SessionControlError::ApplicationMissing),
            )
            .expect("apply rejecting ACCEPTED_REPLY");
        assert!(sessions.session_id_from_handle(handle).is_none());

        // A reply from an Application that does not own the Session is
        // dropped and leaves the Session Published (session_node.c:516-521).
        let foreign_applications = ApplicationMain::new(2);
        let owner = foreign_applications.attach().expect("owner Application");
        let foreign = foreign_applications.attach().expect("foreign Application");
        let (mut sessions, session_id) =
            accepted_reply_fixture(owner, publisher, Arc::clone(&foreign_applications), 5);
        let handle = sessions.session_handle(session_id);
        sessions
            .accept_reply(foreign, handle, Ok(()))
            .expect("drop foreign ACCEPTED_REPLY");
        let entry = sessions.entries.get(session_id).expect("Session entry");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::Published(_),
                ..
            })
        ));
    }

    #[test]
    fn accept_reply_for_stale_or_missing_child_never_aborts_worker_and_sibling_remains_valid() {
        let socket_path = std::path::PathBuf::from(format!(
            "/tmp/hammer-sr-accept-stale-{}.sock",
            std::process::id()
        ));
        let server = AppServer::bind(socket_path.to_str().expect("socket path"), 4)
            .expect("bind App server");
        let applications = ApplicationMain::new(1);
        let application = applications.attach().expect("attach Application");
        let (mut sessions, stale_id) = accepted_reply_fixture(
            application,
            server.publisher(),
            Arc::clone(&applications),
            2,
        );
        let stale_handle = sessions.session_handle(stale_id);
        let sibling_id = sessions
            .construct_stream_sessions(1, 3u32, 3, application, None, None, None, true)
            .expect("sibling external Session");
        sessions
            .entries
            .get_mut(sibling_id)
            .expect("sibling Session entry")
            .listener = Some(SessionHandle::new(7, 0));
        sessions
            .publish_accepted_transport_session(sibling_id)
            .expect("publish sibling ACCEPTED message");
        let sibling_handle = sessions.session_handle(sibling_id);

        // The child Session is removed while the Application is deciding; the
        // late reply is a stale handle and must be dropped without aborting
        // the worker (VPP `session_get_from_handle_if_valid` miss,
        // session_node.c:526-528).
        sessions
            .remove_session(stale_id)
            .expect("remove child before its reply");
        assert!(sessions.session_id_from_handle(stale_handle).is_none());
        sessions
            .accept_reply(application, stale_handle, Ok(()))
            .expect("stale child reply is dropped");
        // A reply for a Session that was never created is dropped the same way.
        sessions
            .accept_reply(application, SessionHandle::new(0, 0), Ok(()))
            .expect("missing child reply is dropped");

        // The sibling is untouched and its own reply still applies.
        let entry = sessions
            .entries
            .get(sibling_id)
            .expect("sibling Session entry");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::Published(_),
                ..
            })
        ));
        sessions
            .accept_reply(application, sibling_handle, Ok(()))
            .expect("apply sibling ACCEPTED_REPLY");
        let entry = sessions
            .entries
            .get(sibling_id)
            .expect("sibling Session entry after reply");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::Active(_),
                ..
            })
        ));
    }

    // --- Task 15: worker-local transport action dispatch ---

    const ACTION_TRANSPORT: u8 = 0;

    /// Per-thread capture for the fake transport callbacks. The callbacks run
    /// synchronously on the test's own worker thread, so a thread-local
    /// capture is race-free under parallel cargo test without any
    /// synchronization; the owned `close_reason` replaces the previous
    /// raw-pointer reconstruction.
    #[derive(Default)]
    struct TransportActionCapture {
        callback_count: u64,
        state_at_callback: u8,
        open_parent: u64,
        open_direction: u8,
        open_context: u64,
        open_child: u64,
        reset_session: u64,
        reset_code: u64,
        stop_session: u64,
        stop_code: u64,
        close_session: u64,
        close_code: u64,
        close_reason: Vec<u8>,
    }

    thread_local! {
        static TRANSPORT_ACTION_CAPTURE: RefCell<TransportActionCapture> =
            RefCell::new(TransportActionCapture::default());
    }

    fn with_capture<T>(f: impl FnOnce(&mut TransportActionCapture) -> T) -> T {
        TRANSPORT_ACTION_CAPTURE.with(|cell| f(&mut cell.borrow_mut()))
    }

    fn callback_count() -> u64 {
        with_capture(|capture| capture.callback_count)
    }

    fn state_at_callback() -> u8 {
        with_capture(|capture| capture.state_at_callback)
    }

    fn reset_capture() {
        with_capture(|capture| *capture = TransportActionCapture::default());
    }

    /// Applies one state-machine step to a Session entry from the test thread.
    fn mutate_state(
        sessions: &mut SessionWorker<Index>,
        session_id: u32,
        f: impl FnOnce(&mut SessionState<Index>),
    ) {
        let entry = sessions
            .entries
            .get_mut(session_id)
            .expect("transport Session entry");
        let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
            panic!("transport Session entry");
        };
        f(state);
    }

    fn fake_open_stream(
        worker: &mut SessionWorker<Index>,
        parent: u32,
        direction: SessionStreamDirection,
        app_context: SessionAppContext,
    ) -> RuntimeResult<u32> {
        with_capture(|capture| {
            capture.callback_count += 1;
            capture.open_parent = parent.into();
            capture.open_direction = match direction {
                SessionStreamDirection::Bidi => 0,
                SessionStreamDirection::Uni => 1,
            };
            capture.open_context = app_context;
        });
        let (rx_fifo, tx_fifo) = worker.create_local_fifos()?;
        let child = worker.insert_session_entry(SessionEntry::creating_transport(
            ACTION_TRANSPORT,
            rx_fifo,
            tx_fifo,
        ))?;
        with_capture(|capture| capture.open_child = child.into());
        Ok(child)
    }

    /// Records whether the Session entry already shows AppClosed at callback
    /// time; proves the seam transitions state before invoking the transport.
    fn record_state_at_callback(worker: &SessionWorker<Index>, session_id: u32) {
        let app_closed = worker.entries.get(session_id).is_some_and(|entry| {
            matches!(
                entry.session_type,
                Some(SessionType::Transport {
                    state: SessionState::AppClosed(_),
                    ..
                })
            )
        });
        with_capture(|capture| capture.state_at_callback = app_closed as u8);
    }

    fn fake_reset_stream(
        worker: &mut SessionWorker<Index>,
        session_id: u32,
        code: u64,
    ) -> RuntimeResult<()> {
        with_capture(|capture| {
            capture.callback_count += 1;
            capture.reset_session = session_id.into();
            capture.reset_code = code;
        });
        record_state_at_callback(worker, session_id);
        Ok(())
    }

    fn fake_stop_sending(
        _worker: &mut SessionWorker<Index>,
        session_id: u32,
        code: u64,
    ) -> RuntimeResult<()> {
        with_capture(|capture| {
            capture.callback_count += 1;
            capture.stop_session = session_id.into();
            capture.stop_code = code;
        });
        Ok(())
    }

    fn fake_close_connection(
        worker: &mut SessionWorker<Index>,
        connection: u32,
        code: u64,
        reason: &[u8],
    ) -> RuntimeResult<()> {
        with_capture(|capture| {
            capture.callback_count += 1;
            capture.close_session = connection.into();
            capture.close_code = code;
            capture.close_reason = reason.to_vec();
        });
        record_state_at_callback(worker, connection);
        Ok(())
    }

    fn fake_worker_actions() -> SessionTransportWorkerActions<Index> {
        SessionTransportWorkerActions::new(
            fake_open_stream,
            fake_reset_stream,
            fake_stop_sending,
            fake_close_connection,
        )
    }

    fn worker_with_transport_session(transport: u8, index: Index) -> (SessionWorker<Index>, u32) {
        let applications = ApplicationMain::new(1);
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            AppSessionConfig::default(),
            DEFAULT_SESSION_POOL_CAPACITY,
            applications,
            None,
        )
        .expect("Session worker");
        let (rx_fifo, tx_fifo) = sessions
            .create_local_fifos()
            .expect("Session FIFOs for transport action test");
        let session_id = sessions
            .insert_session_entry(SessionEntry::creating_transport(
                transport, rx_fifo, tx_fifo,
            ))
            .expect("insert transport Session");
        sessions
            .finish_transport_creation(session_id, index)
            .expect("complete transport Session");
        (sessions, session_id)
    }

    /// Moves a Created transport Session to Active via the state machine
    /// (Published then connected), as the transport does before dispatchable
    /// streams exist.
    fn make_active(sessions: &mut SessionWorker<Index>, session_id: u32) {
        mutate_state(sessions, session_id, |state| {
            let (published, _) = state
                .on_connection_published()
                .expect("Created transitions to Published");
            *state = published;
            *state = state
                .on_connected()
                .expect("Published transitions to Active");
        });
    }

    #[test]
    fn transport_worker_actions_receive_exact_typed_args() -> Result<(), SessionTestFailure> {
        reset_capture();
        let (mut sessions, parent) = worker_with_transport_session(ACTION_TRANSPORT, 7u32);
        sessions.install_transport_actions(ACTION_TRANSPORT, fake_worker_actions())?;
        assert!(
            sessions
                .install_transport_actions(ACTION_TRANSPORT, fake_worker_actions())
                .is_err(),
            "duplicate install is rejected"
        );

        let code = 0x1234u64;
        let child = sessions.open_stream(parent, SessionStreamDirection::Uni, 0xDEAD_BEEF)?;
        with_capture(|capture| {
            assert_eq!(capture.open_parent, parent.into());
            assert_eq!(capture.open_direction, 1);
            assert_eq!(capture.open_context, 0xDEAD_BEEF);
            assert_eq!(
                child, capture.open_child,
                "open_stream returns the child Session the callback created"
            );
        });
        assert_ne!(child, parent);
        assert!(
            sessions.entries.get(child).is_some(),
            "the returned child exists on the worker"
        );

        // stop_sending dispatches only for an Active session (VPP READY-only
        // half-close), so bring parent to Active before the close-family
        // actions; each close-family dispatch records AppClosed, so each
        // action below needs a still-open session.
        make_active(&mut sessions, parent);
        sessions.stop_sending(parent, code)?;
        with_capture(|capture| {
            assert_eq!(capture.stop_session, parent.into());
            assert_eq!(capture.stop_code, 0x1234);
        });

        sessions.reset_stream(parent, code)?;
        with_capture(|capture| {
            assert_eq!(capture.reset_session, parent.into());
            assert_eq!(capture.reset_code, 0x1234);
        });

        // Parent is AppClosed after the reset; finish and close the still-open
        // child instead (a Creating Session would be a guarded no-op).
        sessions
            .finish_transport_creation(child, 7u32)
            .expect("complete child transport Session");
        let reason = [0xFF, 0x00, 0xFE];
        sessions.close_connection(child, code, &reason)?;
        with_capture(|capture| {
            assert_eq!(capture.close_session, child.into());
            assert_eq!(capture.close_code, 0x1234);
            assert_eq!(
                capture.close_reason, reason,
                "the raw non-UTF8 reason bytes are passed through"
            );
        });
        Ok(())
    }

    #[test]
    fn transport_worker_actions_missing_registration_is_typed() -> Result<(), SessionTestFailure> {
        let (mut sessions, session) = worker_with_transport_session(ACTION_TRANSPORT, 1u32);

        assert!(matches!(
            sessions.open_stream(session, SessionStreamDirection::Bidi, 0),
            Err(SessionTransportActionError::MissingRegistration { transport })
                if transport == ACTION_TRANSPORT
        ));
        assert!(matches!(
            sessions.reset_stream(session, 0u64),
            Err(SessionTransportActionError::MissingRegistration { .. })
        ));
        assert!(matches!(
            sessions.stop_sending(session, 0u64),
            Err(SessionTransportActionError::MissingRegistration { .. })
        ));
        assert!(matches!(
            sessions.close_connection(session, 0u64, &[1]),
            Err(SessionTransportActionError::MissingRegistration { .. })
        ));
        Ok(())
    }

    #[test]
    fn transport_worker_actions_derive_transport_and_validate_session()
    -> Result<(), SessionTestFailure> {
        reset_capture();
        // The transport is derived from the Session entry, never caller-supplied:
        // with no table installed, the typed error names the derived transport.
        let (mut sessions, own) = worker_with_transport_session(ACTION_TRANSPORT, 1u32);
        assert!(matches!(
            sessions.reset_stream(own, 0u64),
            Err(SessionTransportActionError::MissingRegistration { transport })
                if transport == ACTION_TRANSPORT
        ));

        let foreign = 1;
        let (mut sessions, foreign_session) = worker_with_transport_session(foreign, 1u32);
        sessions.install_transport_actions(foreign, fake_worker_actions())?;
        assert!(matches!(
            sessions.reset_stream(999, 0u64),
            Err(SessionTransportActionError::InvalidSession { session_id })
                if session_id == 999
        ));
        sessions.reset_stream(foreign_session, 0x77u64)?;
        with_capture(|capture| {
            assert_eq!(
                capture.reset_session,
                foreign_session.into(),
                "the derived transport 1 table dispatched the session"
            );
            assert_eq!(capture.reset_code, 0x77);
        });
        Ok(())
    }

    /// Inserts a Created transport Session (finished creation) on the worker.
    fn extra_transport_session(sessions: &mut SessionWorker<Index>, index: Index) -> u32 {
        let (rx_fifo, tx_fifo) = sessions
            .create_local_fifos()
            .expect("Session FIFOs for transport action test");
        let session_id = sessions
            .insert_session_entry(SessionEntry::creating_transport(
                ACTION_TRANSPORT,
                rx_fifo,
                tx_fifo,
            ))
            .expect("insert transport Session");
        sessions
            .finish_transport_creation(session_id, index)
            .expect("complete transport Session");
        session_id
    }

    #[test]
    fn transport_worker_actions_repeated_or_invalid_states_keep_callback_count()
    -> Result<(), SessionTestFailure> {
        reset_capture();
        let (mut sessions, session) = worker_with_transport_session(ACTION_TRANSPORT, 2u32);
        sessions.install_transport_actions(ACTION_TRANSPORT, fake_worker_actions())?;
        let code = 0x42u64;

        // A still-Creating Session (no transport index yet) never dispatches.
        let (rx_fifo, tx_fifo) = sessions.create_local_fifos().expect("Session FIFOs");
        let creating = sessions
            .insert_session_entry(SessionEntry::creating_transport(
                ACTION_TRANSPORT,
                rx_fifo,
                tx_fifo,
            ))
            .expect("insert Creating Session");
        sessions.stop_sending(creating, code)?;
        sessions.reset_stream(creating, code)?;
        sessions.close_connection(creating, code, &[])?;
        assert_eq!(
            callback_count(),
            0,
            "Creating Sessions never reach the transport"
        );

        // A Created (not yet Active) Session cannot half-close (VPP READY-only).
        sessions.stop_sending(session, code)?;
        assert_eq!(callback_count(), 0);

        // But it does close: VPP dispatches transport_close for every state
        // below APP_CLOSED, with AppClosed recorded first.
        sessions.close_connection(session, code, &[])?;
        assert_eq!(callback_count(), 1);
        assert_eq!(
            state_at_callback(),
            1,
            "AppClosed was recorded before the close callback ran"
        );

        // Repeated close, reset and stop on the AppClosed Session are no-ops.
        sessions.close_connection(session, code, &[])?;
        sessions.reset_stream(session, code)?;
        sessions.stop_sending(session, code)?;
        assert_eq!(
            callback_count(),
            1,
            "repeated close-family actions on AppClosed never re-dispatch"
        );

        // A TransportClosed Session never dispatches close/reset/stop.
        let transport_closed = extra_transport_session(&mut sessions, 2u32);
        make_active(&mut sessions, transport_closed);
        mutate_state(&mut sessions, transport_closed, |state| {
            let _ = state.on_transport_close(2u32);
        });
        sessions.close_connection(transport_closed, code, &[])?;
        sessions.reset_stream(transport_closed, code)?;
        sessions.stop_sending(transport_closed, code)?;
        assert_eq!(
            callback_count(),
            1,
            "TransportClosed Sessions never re-dispatch"
        );

        // A TransportDeleted Session never dispatches close/reset.
        let transport_deleted = extra_transport_session(&mut sessions, 2u32);
        make_active(&mut sessions, transport_deleted);
        mutate_state(&mut sessions, transport_deleted, |state| {
            let _ = state.on_transport_deleted(2u32);
        });
        sessions.close_connection(transport_deleted, code, &[])?;
        sessions.reset_stream(transport_deleted, code)?;
        assert_eq!(
            callback_count(),
            1,
            "TransportDeleted Sessions never re-dispatch"
        );
        Ok(())
    }

    #[test]
    fn transport_worker_actions_reset_and_close_record_app_closed_before_callback()
    -> Result<(), SessionTestFailure> {
        reset_capture();
        let (mut sessions, session) = worker_with_transport_session(ACTION_TRANSPORT, 3u32);
        make_active(&mut sessions, session);
        sessions.install_transport_actions(ACTION_TRANSPORT, fake_worker_actions())?;
        let code = 0x51u64;

        // reset_stream on an Active Session: AppClosed is recorded before the
        // transport callback runs and remains after return.
        sessions.reset_stream(session, code)?;
        assert_eq!(callback_count(), 1);
        assert_eq!(
            state_at_callback(),
            1,
            "reset_stream recorded AppClosed before the callback ran"
        );
        let entry = sessions.entries.get(session).expect("Session entry");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::AppClosed(_),
                ..
            })
        ));

        // stop_sending is half-close: it dispatches but never changes state.
        let stream = extra_transport_session(&mut sessions, 3u32);
        make_active(&mut sessions, stream);
        sessions.stop_sending(stream, code)?;
        assert_eq!(callback_count(), 2);
        let entry = sessions.entries.get(stream).expect("Session entry");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::Active(_),
                ..
            })
        ));

        // close_connection on the Active stream: AppClosed before callback.
        sessions.close_connection(stream, code, &[1, 2, 3])?;
        assert_eq!(callback_count(), 3);
        assert_eq!(
            state_at_callback(),
            1,
            "close_connection recorded AppClosed before the callback ran"
        );
        let entry = sessions.entries.get(stream).expect("Session entry");
        assert!(matches!(
            entry.session_type,
            Some(SessionType::Transport {
                state: SessionState::AppClosed(_),
                ..
            })
        ));

        // A second close on the now-AppClosed stream is a silent no-op.
        sessions.close_connection(stream, code, &[4, 5])?;
        assert_eq!(
            callback_count(),
            3,
            "a second close on AppClosed never re-dispatches"
        );
        Ok(())
    }
}

#[cfg(test)]
fn queue_for_worker(
    resources: &ApplicationMqResources,
    application: u32,
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
    let pending_queue = unsafe { &mut *(entry.pending_queue as *mut VecDeque<u32>) };
    pending_queue.push_back(entry.application);
    let _ = graph.mark_interrupt_pending(entry.appsl_input_node)?;
    Ok(())
}

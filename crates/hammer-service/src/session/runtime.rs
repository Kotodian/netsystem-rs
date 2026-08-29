use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::hint::spin_loop;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::ops::Deref;
use std::os::fd::BorrowedFd;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use crossbeam_queue::ArrayQueue;
use hammer_core::data_plane::{
    BufferFrame, DataPlaneBuffers, Index as BufferIndex, NodeId, NodeState,
};
use hammer_infra::align::{CacheLineAlignMark, align_up};
use hammer_infra::fifo::Fifo;
use hammer_infra::linked_list::LinkedList;
use hammer_infra::pool::Pool;
use hammer_infra::segment::Segment;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{
    AppSession, AppSessionConfig, AppSessionError, SessionAcceptedMsg, SessionConnectError,
    SessionConnectedMsg, SessionControlError, SessionDgramHeader, SessionEventQueue, SessionEvt,
    SessionEvtType, SessionFlags, SessionHandle, SessionMqRing, SessionMsgQueue,
    SessionMsgQueueError,
};
use hammer_runtime::attach::AppSessionPublisher;
use hammer_runtime::session::SessionStreamDirection;
use hammer_runtime::{
    AttachError, RuntimeError, RuntimeResult, SessionConnectEndpoint, SessionListenEndpoint,
};
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, Deadline, File, FileFunctions, GlobalMain, NodeRuntime,
    NodeRuntimeData,
};

use crate::session::app::AppWorkerError;
use crate::session::application::{ApplicationMain, ApplicationMqResources, application_main};
use crate::session::error::{SessionError, SessionQueueError};
use crate::session::lookup::SessionEndpointLookup;
use crate::session::node::{AppSessionInputNode, SessionQueueTransportDispatch};
use crate::session::protocol::SessionAppVft;
use crate::session::state::SessionState;
use crate::session::{AppWorker, SessionQueueNext};
use crate::transport::transport_vft;

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
    session_migration_shutdown: AtomicBool,
    session_migration_shutdown_workers: AtomicU32,
    session_migration_shutdown_phase: AtomicU32,
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
            session_migration_shutdown: AtomicBool::new(false),
            session_migration_shutdown_workers: AtomicU32::new(0),
            session_migration_shutdown_phase: AtomicU32::new(0),
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
        let Ok(target_worker) = DataWorkerId::try_from(closed.new_sh.thread_index) else {
            return Err(closed);
        };
        let Some(queue) = self.session_switch_pool_closed.get(target_worker.slot()) else {
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

/// Worker-owned migration queues are published once so the exact target
/// worker partition can be reached without retaining a SessionMain owner.
static SESSION_MIGRATE_QUEUES: OnceLock<Arc<SessionMigrateQueues>> = OnceLock::new();

#[inline]
fn publish_session_migrate_queues(worker_count: usize) -> Arc<SessionMigrateQueues> {
    SESSION_MIGRATE_QUEUES
        .get_or_init(|| Arc::new(SessionMigrateQueues::new(worker_count)))
        .clone()
}

#[inline]
fn session_migrate_queues(worker_count: usize) -> Arc<SessionMigrateQueues> {
    SESSION_MIGRATE_QUEUES
        .get()
        .cloned()
        .unwrap_or_else(|| Arc::new(SessionMigrateQueues::new(worker_count)))
}

#[derive(Clone, Copy)]
enum SessionType {
    Transport { transport: u8, state: SessionState },
}

#[derive(Clone, Copy)]
enum SessionApplication {
    External(u32),
}

struct SessionEntry {
    session_type: Option<SessionType>,
    application: Option<SessionApplication>,
    owner_application: Option<u32>,
    app: Option<u32>,
    app_session: u64,
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

impl SessionEntry {
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
/// for stream children, the parent connection's app-state identity
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
    /// The parent Session's app-state identity, `None` for roots and for
    /// streams whose pinned listener handle is absent, foreign, or no longer
    /// live.
    pub parent_app_context: Option<u64>,
}

pub struct SessionWorker {
    worker: DataWorkerId,
    worker_count: usize,
    migration_queues: Arc<SessionMigrateQueues>,
    entries: Pool<SessionEntry>,
    app: AppWorker,
    session_evt_q: Arc<SessionMsgQueue>,
    app_session_config: AppSessionConfig,
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

#[repr(C)]
struct SessionWorkerSlot {
    cacheline0: CacheLineAlignMark,
    owner: ThreadOwned<SessionWorker>,
}

impl SessionWorkerSlot {
    fn new() -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            owner: ThreadOwned::new(),
        }
    }
}

impl Deref for SessionWorkerSlot {
    type Target = ThreadOwned<SessionWorker>;

    fn deref(&self) -> &Self::Target {
        &self.owner
    }
}

pub struct SessionMain {
    workers: Box<[SessionWorkerSlot]>,
    listeners: UnsafeCell<Pool<SessionListener>>,
    endpoint_lookup: SessionEndpointLookup,
}

/// The process-global Session authority, published by `session_init`.
pub static SESSION_MAIN: OnceLock<SessionMain> = OnceLock::new();

impl SessionMain {
    /// Initializes and publishes the process-global Session authority.
    pub fn init(worker_count: usize) -> RuntimeResult<()> {
        let workers = (0..worker_count)
            .map(|_| SessionWorkerSlot::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let _ = publish_session_migrate_queues(worker_count);
        let main = Self {
            workers,
            listeners: UnsafeCell::new(Pool::new()),
            endpoint_lookup: SessionEndpointLookup::new(),
        };
        SESSION_MAIN
            .set(main)
            .map_err(|_| RuntimeError::PluginStateNotInitialized { plugin: "session" })
    }

    /// Returns the published process-global Session authority.
    pub fn global() -> RuntimeResult<&'static Self> {
        SESSION_MAIN
            .get()
            .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "session" })
    }
}

/// Returns the published process-global Session authority.
#[inline]
pub fn session_main() -> &'static SessionMain {
    SessionMain::global().expect("SessionMain is initialized before Session use")
}

pub(super) struct SessionListener {
    application: u32,
    application_listener: u32,
    app: Option<u32>,
    protocol: u8,
    connection_index: Option<u32>,
    accepting: bool,
}

impl SessionListener {
    #[inline]
    pub(super) const fn application(&self) -> u32 {
        self.application
    }

    pub(super) const fn application_listener(&self) -> u32 {
        self.application_listener
    }

    #[inline]
    pub(super) const fn app(&self) -> Option<u32> {
        self.app
    }
}

// SAFETY: Main Thread publishes listener state under the worker barrier; Data
// Workers only read the immutable entry selected by their transport callback.
unsafe impl Send for SessionMain {}
// SAFETY: `listeners` mutation is confined to the GlobalMain Main control path and
// synchronized by the worker barrier before a Data Worker may observe it.
unsafe impl Sync for SessionMain {}

impl SessionMain {
    pub fn begin_session_migration_shutdown(&self) {
        session_migrate_queues(self.workers.len())
            .session_migration_shutdown
            .store(true, Ordering::Release);
    }

    #[inline]
    pub fn session_migration_shutdown(&self) -> bool {
        session_migrate_queues(self.workers.len())
            .session_migration_shutdown
            .load(Ordering::Acquire)
    }

    pub fn wait_session_migration_shutdown_phase(&self) {
        let worker_count = self.workers.len() as u32;
        if worker_count == 0 {
            return;
        }
        let queues = session_migrate_queues(self.workers.len());
        let phase = queues
            .session_migration_shutdown_phase
            .load(Ordering::Acquire);
        let expected = phase.saturating_add(1).saturating_mul(worker_count);
        let arrived = queues
            .session_migration_shutdown_workers
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if arrived == expected {
            queues
                .session_migration_shutdown_phase
                .store(phase.saturating_add(1), Ordering::Release);
            return;
        }
        while queues
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
        runtime: &DataPlaneMain,
        target_worker: DataWorkerId,
        old_handle: SessionHandle,
        tuple: SessionTuple,
        dgram: SessionDgramArgs,
    ) -> SessionMigrateResult {
        let (transport, local, remote) = tuple;
        if self.session_migration_shutdown() {
            return SessionMigrateResult::Unavailable;
        }
        let Ok(source_worker) = DataWorkerId::try_from(old_handle.thread_index) else {
            return SessionMigrateResult::Unavailable;
        };
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
        if session_migrate_queues(self.workers.len())
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

    fn wake_worker(&self, runtime: &DataPlaneMain, worker: DataWorkerId) {
        if let Some(session_queue) = runtime.node_by_name("session-queue") {
            runtime.set_worker_node_interrupt_pending(worker, session_queue);
        }
    }

    pub fn push_session_switch_pool_reply(
        &self,
        runtime: &DataPlaneMain,
        reply: SessionSwitchPoolReply,
    ) -> Result<(), SessionSwitchPoolReply> {
        let target_worker = reply.new_thread;
        let result =
            session_migrate_queues(self.workers.len()).push_session_switch_pool_reply(reply);
        if result.is_ok() {
            self.wake_worker(runtime, target_worker);
        }
        result
    }

    pub fn push_session_switch_pool_completion(
        &self,
        runtime: &DataPlaneMain,
        completion: SessionSwitchPoolCompletion,
    ) -> Result<(), SessionSwitchPoolCompletion> {
        let source_worker = completion.old_thread;
        let result = session_migrate_queues(self.workers.len())
            .push_session_switch_pool_completion(completion);
        if result.is_ok() {
            self.wake_worker(runtime, source_worker);
        }
        result
    }

    pub fn push_session_switch_pool_closed(
        &self,
        runtime: &DataPlaneMain,
        closed: SessionSwitchPoolClosed,
    ) -> Result<(), SessionSwitchPoolClosed> {
        let Ok(target_worker) = DataWorkerId::try_from(closed.new_sh.thread_index) else {
            return Err(closed);
        };
        let result =
            session_migrate_queues(self.workers.len()).push_session_switch_pool_closed(closed);
        if result.is_ok() {
            self.wake_worker(runtime, target_worker);
        }
        result
    }

    pub fn pop_session_migrate_request(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolArgs> {
        session_migrate_queues(self.workers.len()).pop_session_migrate_request(worker)
    }

    pub fn pop_session_switch_pool_reply(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolReply> {
        session_migrate_queues(self.workers.len()).pop_session_switch_pool_reply(worker)
    }

    pub fn pop_session_switch_pool_completion(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolCompletion> {
        session_migrate_queues(self.workers.len()).pop_session_switch_pool_completion(worker)
    }

    pub fn pop_session_switch_pool_closed(
        &self,
        worker: DataWorkerId,
    ) -> Option<SessionSwitchPoolClosed> {
        session_migrate_queues(self.workers.len()).pop_session_switch_pool_closed(worker)
    }

    pub fn listen(
        &self,
        application_listener: u32,
        protocol: u8,
        endpoint: SessionListenEndpoint,
    ) -> Result<SessionHandle, SessionError> {
        self.with_control_barrier(|| {
            let transport =
                transport_vft(protocol).ok_or(SessionError::TransportListenUnsupported)?;
            let (application, app, opaque) = application_main()
                .with_listener(application_listener, |listener| {
                    (listener.application(), listener.app(), listener.opaque())
                })
                .map_err(|source| SessionError::TransportOpFailed {
                    source: source.into(),
                })?;
            if let Some(app) = app {
                if application_main()
                    .session_callbacks(application, app)
                    .is_none()
                {
                    return Err(SessionError::SessionAppNotRegistered { app });
                }
            }
            let listener = self
                .with_listeners_mut(|listeners| {
                    Ok(SessionHandle::new(
                        listeners.insert(SessionListener {
                            application,
                            application_listener,
                            app,
                            protocol,
                            connection_index: None,
                            accepting: true,
                        }),
                        0,
                    ))
                })
                .map_err(|_| SessionError::ListenerControlWrongThread)??;
            let Some(start_listen) = transport.start_listen else {
                self.with_listeners_mut(|listeners| {
                    drop(listeners.remove(listener.session_index));
                    Ok(())
                })
                .map_err(|_| SessionError::ListenerControlWrongThread)??;
                return Err(SessionError::TransportListenUnsupported);
            };
            let connection_index = match start_listen(listener, application, opaque, endpoint) {
                Ok(index) => index,
                Err(error) => {
                    self.with_listeners_mut(|listeners| {
                        drop(listeners.remove(listener.session_index));
                        Ok(())
                    })
                    .map_err(|_| SessionError::ListenerControlWrongThread)??;
                    return Err(SessionError::TransportOpFailed { source: error });
                }
            };
            self.with_listeners_mut(|listeners| {
                let entry = listeners
                    .get_mut(listener.session_index)
                    .ok_or(SessionError::ListenerMissing { listener })?;
                entry.connection_index = Some(connection_index);
                Ok(())
            })
            .map_err(|_| SessionError::ListenerControlWrongThread)??;
            Ok(listener)
        })
        .map_err(|_| SessionError::ListenerControlWrongThread)?
    }

    pub fn unlisten(&self, listener: SessionHandle) -> Result<(), SessionError> {
        self.with_control_barrier(|| {
            let (protocol, connection_index) = self
                .with_listener(listener, |entry| (entry.protocol, entry.connection_index))
                .map_err(|_| SessionError::ListenerMissing { listener })?;
            let transport =
                transport_vft(protocol).ok_or(SessionError::TransportListenUnsupported)?;
            let Some(stop_listen) = transport.stop_listen else {
                return Err(SessionError::TransportListenUnsupported);
            };
            self.with_listeners_mut(|listeners| {
                let entry = listeners
                    .get_mut(listener.session_index)
                    .ok_or(SessionError::ListenerMissing { listener })?;
                entry.accepting = false;
                Ok(())
            })
            .map_err(|_| SessionError::ListenerControlWrongThread)??;
            let connection_index =
                connection_index.ok_or(SessionError::ListenerMissing { listener })?;
            if let Err(source) = stop_listen(connection_index) {
                self.with_listeners_mut(|listeners| {
                    if let Some(entry) = listeners.get_mut(listener.session_index) {
                        entry.accepting = true;
                    }
                    Ok(())
                })
                .map_err(|_| SessionError::ListenerControlWrongThread)??;
                return Err(SessionError::TransportOpFailed { source });
            }
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
        let barrier = match GlobalMain::with_current(|engine| {
            engine
                .ensure_main_thread()
                .map(|()| engine.worker_barrier())
        }) {
            Some(Ok(barrier)) => barrier,
            Some(Err(error)) => return Err(error),
            None => return Err(RuntimeError::ControlRequiresMainThread),
        };
        if barrier.is_pending() {
            Ok(operation())
        } else {
            Ok(barrier.sync(operation))
        }
    }

    pub fn connect(
        &self,
        protocol: u8,
        endpoint: SessionConnectEndpoint,
    ) -> Result<u32, SessionError> {
        let transport = transport_vft(protocol).ok_or(SessionError::TransportConnectUnsupported)?;
        let Some(connect) = transport.connect else {
            return Err(SessionError::TransportConnectUnsupported);
        };
        let connection = endpoint.connection;
        let worker_count = self.workers.len();
        if worker_count == 0 {
            return Err(SessionError::NoDataWorkers);
        }
        connect(endpoint).map_err(|source| SessionError::TransportOpFailed { source })?;
        Ok(connection)
    }

    /// Opens one child stream on the parent Session's owning worker.
    pub fn connect_stream(
        &self,
        protocol: u8,
        endpoint: SessionConnectEndpoint,
    ) -> Result<u32, SessionError> {
        let transport =
            transport_vft(protocol).ok_or(SessionError::TransportConnectStreamUnsupported)?;
        let parent = endpoint
            .parent_handle
            .ok_or(SessionError::ConnectStreamParentMissing)?;
        let Ok(expected_worker) = DataWorkerId::try_from(parent.thread_index) else {
            return Err(SessionError::ConnectStreamWrongWorker {
                parent,
                expected: endpoint.worker,
                actual: endpoint.worker,
            });
        };
        if endpoint.worker != expected_worker {
            return Err(SessionError::ConnectStreamWrongWorker {
                parent,
                expected: expected_worker,
                actual: endpoint.worker,
            });
        }
        let Some(connect_stream) = transport.connect_stream else {
            return Err(SessionError::TransportConnectStreamUnsupported);
        };
        let connection = endpoint.connection;
        connect_stream(endpoint).map_err(|source| SessionError::TransportOpFailed { source })?;
        Ok(connection)
    }

    pub(super) fn with_listener<R>(
        &self,
        listener: SessionHandle,
        operation: impl FnOnce(&SessionListener) -> R,
    ) -> RuntimeResult<R> {
        // SAFETY: Data Workers read a listener only after Main Thread has
        // published it through the worker barrier.
        let listeners = unsafe { &*self.listeners.get() };
        if listener.thread_index != 0 {
            return Err(SessionError::ListenerMissing { listener }.into());
        }
        let index = listener.session_index;
        if !listeners.contains_key(index) {
            return Err(SessionError::ListenerMissing { listener }.into());
        }
        let entry = listeners
            .get(index)
            .ok_or(SessionError::ListenerMissing { listener })?;
        if !entry.accepting {
            return Err(SessionError::ListenerMissing { listener }.into());
        }
        Ok(operation(entry))
    }

    fn with_listeners_mut<R>(
        &self,
        operation: impl FnOnce(&mut Pool<SessionListener>) -> R,
    ) -> RuntimeResult<R> {
        // SAFETY: only the Main Thread mutates this pool while Data Workers
        // are stopped by the barrier below.
        let listeners = unsafe { &mut *self.listeners.get() };
        let barrier = GlobalMain::with_current(|engine| engine.worker_barrier());
        Ok(match barrier {
            Some(barrier) if barrier.is_pending() => operation(listeners),
            Some(barrier) => barrier.sync(|| operation(listeners)),
            None => operation(listeners),
        })
    }

    fn worker(&self, worker: DataWorkerId) -> RuntimeResult<&ThreadOwned<SessionWorker>> {
        Ok(self.workers.get(worker.slot()).map(|slot| &**slot).ok_or(
            SessionQueueError::WorkerOutOfRange {
                worker: worker.slot(),
            },
        )?)
    }

    pub fn with_worker_mut<R>(
        &self,
        runtime: &DataPlaneMain,
        operation: impl FnOnce(&mut SessionWorker) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let thread_index = runtime.thread_index();
        let worker = DataWorkerId::try_from(thread_index)
            .map_err(|_| SessionQueueError::WorkerUnavailable { thread_index })?;
        self.worker(worker)?.with_mut(operation).map_err(|source| {
            SessionQueueError::WorkerAccess {
                worker: worker.slot(),
                source,
            }
        })?
    }

    pub(crate) fn session_queue_is_interrupt(
        &self,
        runtime: &DataPlaneMain,
    ) -> RuntimeResult<bool> {
        self.with_worker_mut(runtime, |sessions| {
            Ok(sessions.state == SessionWorkerState::Interrupt)
        })
    }

    pub(crate) fn application_detached(
        self: &'static Self,
        engine: &GlobalMain,
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
                let main = self;
                match engine.schedule_on_worker(worker, move || {
                    main.worker(worker)
                        .expect("scheduled Application detach targets an existing Session worker")
                        .with_mut(|sessions| {
                            sessions.application_detached(application);
                            hammer_runtime::with_data_plane_main(|main| {
                                sessions.wake_session_queue(main)
                            })?;
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
        self: &'static Self,
        engine: &GlobalMain,
        application: u32,
        resources: &ApplicationMqResources,
    ) -> RuntimeResult<()> {
        if engine.thread_index() != 0 {
            return Err(RuntimeError::WorkerControlRequiresGlobalMain);
        }
        let app_session_input = engine
            .data_plane_main()
            .nodes()
            .node_by_name("appsl-rx-mqs-input")
            .ok_or(SessionQueueError::NodeMissing)?;
        (0..resources.worker_count()).try_for_each(|worker_slot| {
            let worker = DataWorkerId::new(worker_slot as u32);
            let queue = resources
                .queue(worker)
                .ok_or(SessionQueueError::ApplicationMqMissing { application })?
                .clone();
            let main = self;
            schedule_worker_task(engine, worker, move || {
                hammer_runtime::with_data_plane_main_mut(|runtime| {
                    main.worker(worker)?
                        .with_mut(|sessions| {
                            sessions.install_app_mq(application, queue, app_session_input, runtime)
                        })
                        .map_err(|source| SessionQueueError::WorkerAccess {
                            worker: worker.slot(),
                            source,
                        })?
                })
            })?;
            Ok::<(), RuntimeError>(())
        })?;
        Ok(())
    }

    pub(crate) fn remove_application_mqs(
        self: &'static Self,
        engine: &GlobalMain,
        application: u32,
    ) -> RuntimeResult<()> {
        (0..self.workers.len()).try_for_each(|worker_slot| {
            let worker = DataWorkerId::new(worker_slot as u32);
            let main = self;
            schedule_worker_task(engine, worker, move || {
                main.worker(worker)?
                    .with_mut(|sessions| {
                        sessions.drain_app_mq(application)?;
                        Ok(())
                    })
                    .map_err(|source| SessionQueueError::WorkerAccess {
                        worker: worker.slot(),
                        source,
                    })?
            })?;
            Ok::<(), RuntimeError>(())
        })?;

        let mut first_error = None;
        (0..self.workers.len()).for_each(|worker_slot| {
            let worker = DataWorkerId::new(worker_slot as u32);
            let main = self;
            if let Err(error) = schedule_worker_task(engine, worker, move || {
                hammer_runtime::with_data_plane_main_mut(|runtime| {
                    main.worker(worker)?
                        .with_mut(|sessions| sessions.remove_app_mq(application, runtime))
                        .map_err(|source| SessionQueueError::WorkerAccess {
                            worker: worker.slot(),
                            source,
                        })?
                })
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
    engine: &GlobalMain,
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
    engine: &mut DataPlaneMain,
    app_session_input: hammer_core::data_plane::NodeId,
    session_queue: hammer_core::data_plane::NodeId,
    mut worker: SessionWorker,
) -> RuntimeResult<()> {
    let main = SESSION_MAIN
        .get()
        .expect("SessionMain is initialized before worker installation");
    let session_queue_data =
        hammer_runtime::NodeRuntimeData::from_usize(main as *const SessionMain as usize)?;
    let input_data = AppSessionInputNode::worker_runtime_data(session_queue_data, session_queue);
    let worker_id = worker.worker();
    let slot = main.worker(worker_id)?;
    let previous_app_session_input_data = engine.nodes().node_runtime_data(app_session_input)?;
    let previous_session_queue_data = engine.nodes().node_runtime_data(session_queue)?;
    let previous_app_session_input_state = engine.nodes().node_state(app_session_input)?;
    let previous_session_queue_state = engine.nodes().node_state(session_queue)?;

    let setup = (|| -> RuntimeResult<()> {
        engine.set_worker_node_runtime_data(app_session_input, input_data)?;
        engine.set_worker_node_runtime_data(session_queue, session_queue_data)?;
        engine
            .nodes()
            .set_node_state(session_queue, NodeState::Polling)?;
        engine
            .nodes()
            .set_node_state(app_session_input, NodeState::Interrupt)?;
        worker.install_state_deadline(engine, session_queue)
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

fn cleanup_session_worker_install(worker: &mut SessionWorker, engine: &mut DataPlaneMain) {
    if let Err(error) = worker.remove_state_deadline(engine) {
        tracing::error!(%error, "failed to remove Session Worker deadline during install rollback");
    }
}

fn rollback_session_worker_graph(
    engine: &mut DataPlaneMain,
    app_session_input: hammer_core::data_plane::NodeId,
    previous_app_session_input_data: NodeRuntimeData,
    previous_app_session_input_state: NodeState,
    session_queue: NodeId,
    previous_session_queue_data: NodeRuntimeData,
    previous_session_queue_state: NodeState,
) {
    if let Err(error) = engine
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

impl SessionWorker {
    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub const fn worker_count(&self) -> usize {
        self.worker_count
    }

    /// Updates the opaque Session App context selected by a callback.
    pub fn set_app_session(&mut self, session_id: u32, context: u64) -> RuntimeResult<()> {
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
        runtime: &DataPlaneMain,
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
        let index = runtime.file_main().add_deadline(deadline)?;
        self.state_deadline_file = Some(index);
        self.session_queue = Some(session_queue);
        Ok(())
    }

    pub(crate) fn remove_state_deadline(&mut self, runtime: &DataPlaneMain) -> RuntimeResult<()> {
        let Some(index) = self.state_deadline_file else {
            return Ok(());
        };
        if !runtime.file_main().delete_deadline(index)? {
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
        runtime: &DataPlaneMain,
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
        runtime: &DataPlaneMain,
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
            && let Err(error) = runtime.file_main().set_deadline(index, next.deadline())
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

    pub(crate) fn wake_session_queue(&self, runtime: &DataPlaneMain) -> RuntimeResult<()> {
        if let Some(session_queue) = self.session_queue {
            let _ = runtime.set_node_interrupt_pending(session_queue)?;
        }
        Ok(())
    }

    #[inline]
    pub fn app_session_config(&self) -> AppSessionConfig {
        self.app_session_config
    }

    /// Applies the VPP app-close state guard shared by `reset_stream` and
    /// `close_connection` (session.c:1657-1703): returns Ok(false) without
    /// notifying the transport for sessions at or beyond AppClosed and for
    /// sessions still in Creating; returns Ok(true) for Created/Published/
    /// Active sessions after recording AppClosed, so the transport action
    /// runs with the close already recorded. The entry borrow ends before
    /// any callback runs.
    #[inline]
    fn entry_app_close_guard(&mut self, session_id: u32) -> Result<bool, SessionError> {
        let index = session_id;
        if !self.entries.contains_key(index) {
            return Err(SessionError::SessionMissing { session_id });
        }
        let entry = self
            .entries
            .get_mut(index)
            .ok_or(SessionError::SessionMissing { session_id })?;
        let Some(SessionType::Transport { state, .. }) = entry.session_type.as_mut() else {
            return Err(SessionError::SessionMissing { session_id });
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
        app_context: u64,
    ) -> Result<u32, SessionError> {
        let transport = match self
            .entries
            .get(parent)
            .and_then(|entry| entry.session_type)
        {
            Some(SessionType::Transport { transport, .. }) => transport,
            _ => return Err(SessionError::SessionMissing { session_id: parent }),
        };
        let open_stream = transport_vft(transport)
            .and_then(|vft| vft.open_stream)
            .ok_or(SessionError::TransportConnectStreamUnsupported)?;
        open_stream(self, parent, direction, app_context)
            .map_err(|source| SessionError::TransportOpFailed { source })
    }

    /// Dispatches one worker-local `reset_stream` action to the transport that
    /// owns the stream; mirrors VPP `session_transport_reset`
    /// (session.c:1687-1703): only pre-close sessions dispatch, with AppClosed
    /// recorded before the transport is notified.
    pub fn reset_stream(&mut self, session_id: u32, code: u64) -> Result<(), SessionError> {
        let transport = match self
            .entries
            .get(session_id)
            .and_then(|entry| entry.session_type)
        {
            Some(SessionType::Transport { transport, .. }) => transport,
            _ => return Err(SessionError::SessionMissing { session_id }),
        };
        let reset_stream = transport_vft(transport)
            .and_then(|vft| vft.reset_stream)
            .ok_or(SessionError::TransportOpFailed {
                source: crate::transport::TransportError::OperationUnsupported {
                    operation: "reset_stream",
                }
                .into(),
            })?;
        if !self.entry_app_close_guard(session_id)? {
            return Ok(());
        }
        reset_stream(self, session_id, code)
            .map_err(|source| SessionError::TransportOpFailed { source })
    }

    /// Dispatches one worker-local `stop_sending` action to the transport that
    /// owns the stream; mirrors VPP `session_transport_half_close`
    /// (session.c:1637-1648): only a READY session (Hammer's Active) can be
    /// half-closed, so every other state returns Ok without notifying the
    /// transport. Half-close never changes the session state.
    pub fn stop_sending(&mut self, session_id: u32, code: u64) -> Result<(), SessionError> {
        let transport = match self
            .entries
            .get(session_id)
            .and_then(|entry| entry.session_type)
        {
            Some(SessionType::Transport { transport, .. }) => transport,
            _ => return Err(SessionError::SessionMissing { session_id }),
        };
        let stop_sending = transport_vft(transport)
            .and_then(|vft| vft.stop_sending)
            .ok_or(SessionError::TransportOpFailed {
                source: crate::transport::TransportError::OperationUnsupported {
                    operation: "stop_sending",
                }
                .into(),
            })?;
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
        stop_sending(self, session_id, code)
            .map_err(|source| SessionError::TransportOpFailed { source })
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
    ) -> Result<(), SessionError> {
        let transport = match self
            .entries
            .get(connection)
            .and_then(|entry| entry.session_type)
        {
            Some(SessionType::Transport { transport, .. }) => transport,
            _ => {
                return Err(SessionError::SessionMissing {
                    session_id: connection,
                });
            }
        };
        let close_connection = transport_vft(transport)
            .and_then(|vft| vft.close_connection)
            .ok_or(SessionError::TransportOpFailed {
                source: crate::transport::TransportError::OperationUnsupported {
                    operation: "close_connection",
                }
                .into(),
            })?;
        if !self.entry_app_close_guard(connection)? {
            return Ok(());
        }
        close_connection(self, connection, code, reason)
            .map_err(|source| SessionError::TransportOpFailed { source })
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
    pub fn create_upper_session(&mut self, lower: u32, context: u64) -> RuntimeResult<u32> {
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
            drop(self.entries.remove(upper));
            return Err(error);
        }
        let app_session = match self.app.create_app_session(
            lower.into(),
            Some(application),
            self.session_handle(upper),
            self.app_session_config,
            app_rx_mq,
        ) {
            Ok(session) => session,
            Err(error) => {
                drop(self.entries.remove(upper));
                self.detach_upper_session(lower, upper);
                return Err(error.into());
            }
        };
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
            drop(self.entries.remove(upper));
            drop(self.app.detach_session(upper));
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
        index: u32,
        context: u64,
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

    /// Registers one per-Application MQ with this Data Worker's FileMain.
    pub(crate) fn install_app_mq(
        &mut self,
        application: u32,
        queue: Arc<SessionMsgQueue>,
        app_session_input: hammer_core::data_plane::NodeId,
        runtime: &mut DataPlaneMain,
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
        let file = match runtime.file_main().add(file) {
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
        runtime: &mut DataPlaneMain,
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
            match runtime.file_main().delete(file) {
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
    pub fn transport_connection_index(&self, session_id: u32) -> Option<u32> {
        let entry = self.entries.get(session_id)?;
        let Some(SessionType::Transport { state, .. }) = entry.session_type else {
            return None;
        };
        state.transport_index()
    }

    #[inline]
    fn transport_protocol(&self, session_id: u32) -> Option<u8> {
        self.entries
            .get(session_id)
            .and_then(|entry| match entry.session_type {
                Some(SessionType::Transport { transport, .. }) => Some(transport),
                None => None,
            })
    }

    #[inline]
    pub fn owns_transport_session(&self, session_id: u32, protocol: u8) -> bool {
        self.transport_protocol(session_id) == Some(protocol)
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
        index: u32,
        listener: SessionHandle,
    ) -> RuntimeResult<u32> {
        let main = SESSION_MAIN
            .get()
            .expect("SessionMain is initialized before worker acceptance");
        let (application_listener, application, app) = main.with_listener(listener, |entry| {
            (
                entry.application_listener(),
                entry.application(),
                entry.app(),
            )
        })?;
        let opaque =
            application_main().with_listener(application_listener, |entry| entry.opaque())?;
        let session_id = self.construct_stream_sessions(
            transport,
            index,
            application_listener.into(),
            application,
            app,
            opaque,
            None,
            true,
        )?;
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
        index: u32,
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
        index: u32,
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
        index: u32,
        connection: u32,
    ) -> RuntimeResult<u32> {
        let application_connection = connection;
        let session_id = application_main()
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
                let context = application_main()
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
        let (application, context) = application_main()
            .with_connection(application_connection, |entry| {
                (entry.application(), entry.context())
            })
            .map_err(RuntimeError::from)?;
        let message = SessionConnectedMsg::new(context, Err(error));
        let accepted = self.app.publish_connect_failed(application, message)?;
        if accepted {
            application_main()
                .mark_connected(application_connection)
                .map_err(RuntimeError::from)?;
        }
        Ok(accepted)
    }

    fn construct_stream_sessions(
        &mut self,
        transport: u8,
        index: u32,
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
        index: u32,
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
        if let Err(error) = self.finish_transport_creation(session_id, index) {
            drop(self.entries.remove(session_id));
            return Err(error);
        }
        Ok(session_id)
    }

    fn construct_external_transport_session(
        &mut self,
        transport: u8,
        index: u32,
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
        if let Err(error) = self.finish_transport_creation(session_id, index) {
            self.app.discard_app_session(&application_session);
            drop(self.entries.remove(session_id));
            return Err(error);
        }
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
            drop(self.entries.remove(session_id));
        });
    }

    fn insert_session_entry(&mut self, entry: SessionEntry) -> RuntimeResult<u32> {
        Ok(u32::from(self.entries.insert(entry)))
    }

    #[inline]
    pub fn session_handle(&self, session_id: u32) -> SessionHandle {
        SessionHandle::new(session_id, self.worker.thread_index())
    }

    #[inline]
    pub fn session_id_from_handle(&self, handle: SessionHandle) -> Option<u32> {
        if handle.thread_index != self.worker.thread_index() {
            return None;
        }
        let index = handle.session_index;
        self.entries.get(index).map(|_| u32::from(index))
    }

    pub fn program_thread_migration(
        &self,
        runtime: &DataPlaneMain,
        target_worker: DataWorkerId,
        old_handle: SessionHandle,
        tuple: SessionTuple,
        dgram: SessionDgramArgs,
    ) -> SessionMigrateResult {
        SESSION_MAIN
            .get()
            .map_or(SessionMigrateResult::Unavailable, |main| {
                main.program_thread_migration(runtime, target_worker, old_handle, tuple, dgram)
            })
    }

    pub fn cancel_thread_migration(&self, old_handle: SessionHandle, tuple: SessionTuple) -> bool {
        SESSION_MAIN
            .get()
            .is_some_and(|main| main.cancel_migration(tuple, old_handle))
    }

    pub fn push_session_switch_pool_reply(
        &self,
        runtime: &DataPlaneMain,
        reply: SessionSwitchPoolReply,
    ) -> Result<(), SessionSwitchPoolReply> {
        let target_worker = reply.new_thread;
        let result = self.migration_queues.push_session_switch_pool_reply(reply);
        if result.is_ok() {
            self.wake_worker(runtime, target_worker);
        }
        result
    }

    pub fn pop_session_migrate_request(&self) -> Option<SessionSwitchPoolArgs> {
        self.migration_queues
            .pop_session_migrate_request(self.worker)
    }

    pub fn pop_session_switch_pool_reply(&self) -> Option<SessionSwitchPoolReply> {
        self.migration_queues
            .pop_session_switch_pool_reply(self.worker)
    }

    pub fn push_session_switch_pool_completion(
        &self,
        runtime: &DataPlaneMain,
        completion: SessionSwitchPoolCompletion,
    ) -> Result<(), SessionSwitchPoolCompletion> {
        let source_worker = completion.old_thread;
        let result = self
            .migration_queues
            .push_session_switch_pool_completion(completion);
        if result.is_ok() {
            self.wake_worker(runtime, source_worker);
        }
        result
    }

    pub fn pop_session_switch_pool_completion(&self) -> Option<SessionSwitchPoolCompletion> {
        self.migration_queues
            .pop_session_switch_pool_completion(self.worker)
    }

    pub fn push_session_switch_pool_closed(
        &self,
        runtime: &DataPlaneMain,
        closed: SessionSwitchPoolClosed,
    ) -> Result<(), SessionSwitchPoolClosed> {
        let Ok(target_worker) = DataWorkerId::try_from(closed.new_sh.thread_index) else {
            return Err(closed);
        };
        let result = self
            .migration_queues
            .push_session_switch_pool_closed(closed);
        if result.is_ok() {
            self.wake_worker(runtime, target_worker);
        }
        result
    }

    pub fn pop_session_switch_pool_closed(&self) -> Option<SessionSwitchPoolClosed> {
        self.migration_queues
            .pop_session_switch_pool_closed(self.worker)
    }

    pub fn wait_session_migration_shutdown_phase(&self) {
        let worker_count = self.worker_count as u32;
        if worker_count == 0 {
            return;
        }
        let phase = self
            .migration_queues
            .session_migration_shutdown_phase
            .load(Ordering::Acquire);
        let expected = phase.saturating_add(1).saturating_mul(worker_count);
        let arrived = self
            .migration_queues
            .session_migration_shutdown_workers
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if arrived == expected {
            self.migration_queues
                .session_migration_shutdown_phase
                .store(phase.saturating_add(1), Ordering::Release);
            return;
        }
        while self
            .migration_queues
            .session_migration_shutdown_phase
            .load(Ordering::Acquire)
            <= phase
        {
            spin_loop();
        }
    }

    pub fn insert_session_endpoint(
        &self,
        session_id: u32,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = SESSION_MAIN
            .get()
            .expect("SessionMain is initialized before endpoint publication");
        Ok(main.add_connection(local, remote, transport, self.session_handle(session_id)))
    }

    pub fn remove_session_endpoint(
        &self,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = SESSION_MAIN
            .get()
            .expect("SessionMain is initialized before endpoint removal");
        Ok(main.del_connection(local, remote, transport))
    }

    pub fn replace_session_endpoint(
        &self,
        new_session: SessionHandle,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = SESSION_MAIN
            .get()
            .expect("SessionMain is initialized before endpoint replacement");
        Ok(main.replace_connection(local, remote, transport, new_session))
    }

    pub fn publish_session_migration(
        &self,
        new_session: SessionHandle,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<bool> {
        let main = SESSION_MAIN
            .get()
            .expect("SessionMain is initialized before endpoint publication");
        Ok(main.replace_connection(local, remote, transport, new_session))
    }

    pub fn lookup_session_endpoint(
        &self,
        transport: u8,
        local: std::net::SocketAddr,
        remote: std::net::SocketAddr,
    ) -> RuntimeResult<Option<SessionHandle>> {
        let main = SESSION_MAIN
            .get()
            .expect("SessionMain is initialized before endpoint lookup");
        Ok(main.lookup_connection(local, remote, transport))
    }

    fn finish_transport_creation(&mut self, session_id: u32, index: u32) -> RuntimeResult<()> {
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
        index: u32,
    ) -> RuntimeResult<(u32, SessionHandle)> {
        let session_id = self.insert_session_entry(SessionEntry::creating_transport(
            state.transport,
            state.rx_fifo,
            state.tx_fifo,
        ))?;
        if let Err(error) = self.finish_transport_creation(session_id, index) {
            drop(self.entries.remove(session_id));
            return Err(error);
        }
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
            .session_callbacks(old_session, app)
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

    pub fn rollback_session_creation(&mut self, session_id: u32) -> RuntimeResult<Option<u32>> {
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

    pub fn notify_transport_closed(&mut self, session_id: u32, index: u32) -> RuntimeResult<()> {
        self.notify_transport_event(session_id, index, SessionEvtType::TransportClosed)
    }

    pub fn notify_transport_closing(
        &mut self,
        runtime: Option<&DataPlaneMain>,
        session_id: u32,
        index: u32,
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
        index: u32,
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

    pub fn notify_transport_reset(&mut self, session_id: u32, index: u32) -> RuntimeResult<()> {
        self.notify_transport_event(session_id, index, SessionEvtType::Reset)
    }

    fn notify_transport_event(
        &mut self,
        session_id: u32,
        index: u32,
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

    pub fn notify_transport_deleted(&mut self, session_id: u32, index: u32) -> RuntimeResult<()> {
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
            && let Some(callbacks) = self.session_callbacks(session_id, app)
            && let Some(cleanup) = callbacks.cleanup
        {
            cleanup(self, session_id, context)?;
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
                && let Some(callbacks) = self.session_callbacks(upper, app)
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
            drop(self.entries.remove(upper));
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
            && let Some(callbacks) = self.session_callbacks(upper, app)
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
            .session_callbacks(session_id, app)
            .ok_or(SessionError::SessionAppNotRegistered { app })?;
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
            .session_callbacks(session_id, app)
            .ok_or(SessionError::SessionAppNotRegistered { app })?;
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
        application_main()
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
            // VPP `session_mq_accepted_reply_handler` disconnects the exact
            // rejected Session and lets transport cleanup complete the
            // lifecycle; it does not free the Session directly.
            self.schedule_disconnect(session_id);
            return Ok(());
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

impl SessionWorker {
    #[inline]
    fn wake_worker(&self, runtime: &DataPlaneMain, worker: DataWorkerId) {
        if let Some(session_queue) = runtime.node_by_name("session-queue") {
            runtime.set_worker_node_interrupt_pending(worker, session_queue);
        }
    }

    #[inline]
    fn session_callbacks(&self, session_id: u32, app: u32) -> Option<SessionAppVft> {
        let application = match self.entries.get(session_id)?.application? {
            SessionApplication::External(application) => application,
        };
        ApplicationMain::global()
            .ok()?
            .session_callbacks(application, app)
    }

    pub fn new(
        worker: DataWorkerId,
        worker_count: usize,
        app_session_config: AppSessionConfig,
        pool_capacity: usize,
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
            migration_queues: session_migrate_queues(worker_count),
            entries: Pool::with_capacity(pool_capacity),
            app,
            session_evt_q,
            app_session_config,
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

pub trait SessionTransport: Sized {
    type Tx: SessionTxStrategy<Self>;

    /// Numeric protocol slot published by the owning transport Main.
    fn protocol(&self) -> u8;

    /// Returns the transport connection that owns the given Session transport
    /// object. One transport connection may own many Session transport objects.
    fn connection_index(&self, index: u32) -> RuntimeResult<u32> {
        Ok(index)
    }

    /// Handles an application dequeue from the session-owned RX FIFO.
    ///
    /// Returning `true` asks Session Runtime to arm the next RX dequeue
    /// notification. The transport receives FIFO capacity facts but no FIFO
    /// or app scheduling authority.
    fn app_rx_evt(
        &mut self,
        _: u32,
        _: usize,
        _: usize,
        _: &DataPlaneMain,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut crate::session::node::SessionQueueOutput,
    ) -> RuntimeResult<bool> {
        Ok(false)
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker,
        runtime: &DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker,
        index: u32,
        runtime: &DataPlaneMain,
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
        sessions: &mut SessionWorker,
        index: u32,
        runtime: &DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.disconnect(sessions, index, runtime, output_next, frame, output, now)
    }
}

pub trait SessionPacketizedTransport: SessionTransport {
    fn control_tx(
        &mut self,
        sessions: &mut SessionWorker,
        index: u32,
        runtime: &DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;

    fn send_params(
        &mut self,
        sessions: &mut SessionWorker,
        index: u32,
        pending_len: usize,
        now: Instant,
    ) -> RuntimeResult<TransportSendParams>;

    fn tx_action(
        &mut self,
        index: u32,
        batch: &[TxBatchBuffer],
        buffers: &DataPlaneBuffers,
        now: Instant,
    ) -> RuntimeResult<()>;
}

pub trait TransportInternalTransport: SessionTransport {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker,
        session_id: u32,
        index: u32,
        runtime: &DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;
}

pub trait SessionTxStrategy<T>
where
    T: SessionTransport,
{
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker,
        index: u32,
        session_id: u32,
        runtime: &DataPlaneMain,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()>;
}

pub struct SessionPacketizedTx;
pub struct TransportInternalTx;

impl<T> SessionTxStrategy<T> for SessionPacketizedTx
where
    T: SessionPacketizedTransport,
{
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker,
        index: u32,
        session_id: u32,
        runtime: &DataPlaneMain,
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
            .min(frame.capacity().saturating_sub(frame.len()));
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

impl<T> SessionTxStrategy<T> for TransportInternalTx
where
    T: TransportInternalTransport,
{
    #[inline]
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker,
        index: u32,
        session_id: u32,
        runtime: &DataPlaneMain,
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

pub fn dispatch_session_queue_once<T>(
    runtime: &DataPlaneMain,
    owner: hammer_core::data_plane::NodeId,
    sessions: &mut SessionWorker,
    transport: &mut T,
    output_next: SessionQueueNext,
) -> RuntimeResult<SessionQueueStep>
where
    T: SessionTransport,
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

pub fn dispatch_session_queue_pending<T>(
    runtime: &DataPlaneMain,
    sessions: &mut SessionWorker,
    transport: &mut T,
    output_next: SessionQueueNext,
    frame: &mut BufferFrame,
    output: &mut crate::session::node::SessionQueueOutput,
    now: Instant,
) -> RuntimeResult<SessionQueueStep>
where
    T: SessionTransport,
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

pub fn dispatch_session_queue_events<T>(
    runtime: &DataPlaneMain,
    sessions: &mut SessionWorker,
    transport: &mut T,
    output_next: SessionQueueNext,
    frame: &mut BufferFrame,
    output: &mut crate::session::node::SessionQueueOutput,
    now: Instant,
) -> RuntimeResult<SessionQueueStep>
where
    T: SessionTransport,
{
    let mut control_events = core::mem::take(&mut sessions.control_events);
    std::iter::from_fn(|| control_events.pop_front()).try_for_each(
        |event| -> RuntimeResult<()> {
            if matches!(
                event.evt_type,
                SessionEvtType::Close | SessionEvtType::HalfClose | SessionEvtType::Reset
            ) && event.thread_index != sessions.worker.thread_index()
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
                    let connection_index = sessions.transport_connection_index(session_id);
                    if sessions
                        .transport_protocol(session_id)
                        .is_some_and(|owner| owner != transport.protocol())
                    {
                        sessions.control_events.push_back(event);
                        return Ok(());
                    }
                    if !sessions.close_transport_session(session_id)? {
                        return Ok(());
                    }
                    if let Some(index) = connection_index {
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
                    let connection_index = sessions.transport_connection_index(session_id);
                    if sessions
                        .transport_protocol(session_id)
                        .is_some_and(|owner| owner != transport.protocol())
                    {
                        sessions.control_events.push_back(event);
                        return Ok(());
                    }
                    if let Some(index) = connection_index {
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
                    let connection_index = sessions.transport_connection_index(session_id);
                    if sessions
                        .transport_protocol(session_id)
                        .is_some_and(|owner| owner != transport.protocol())
                    {
                        sessions.control_events.push_back(event);
                        return Ok(());
                    }
                    if !sessions.close_transport_session(session_id)? {
                        return Ok(());
                    }
                    if let Some(index) = connection_index {
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

fn dispatch_io_event<T>(
    sessions: &mut SessionWorker,
    transport: &mut T,
    runtime: &DataPlaneMain,
    output_next: SessionQueueNext,
    frame: &mut BufferFrame,
    output: &mut crate::session::node::SessionQueueOutput,
    now: Instant,
    event: SessionEvt,
    scheduled_sessions: &mut usize,
) -> RuntimeResult<bool>
where
    T: SessionTransport,
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
                    if transport_id != transport.protocol() {
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
            if sessions.transport_protocol(session_id) != Some(transport.protocol()) {
                return Ok(false);
            }
            let Some(index) = sessions.transport_connection_index(session_id) else {
                sessions.dispatch_session_type(session_id, event.evt_type)?;
                return Ok(true);
            };
            if let Some(entry) = sessions.entries.get_mut(session_id) {
                entry.schedule_pending = false;
                entry.tx_fifo.unset_event();
            }
            <T::Tx as SessionTxStrategy<T>>::dispatch(
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

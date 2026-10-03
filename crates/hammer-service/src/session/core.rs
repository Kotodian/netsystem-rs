use std::cell::UnsafeCell;
use std::mem::size_of;
use std::num::NonZeroU32;
use std::os::fd::OwnedFd;
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use hammer_core::data_plane::NodeId;
use hammer_infra::bitmap::Bitmap;
use hammer_infra::linked_list::LinkedList;
use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_infra::svm::msg_queue::{SvmMsgQ, SvmMsgQConfig, SvmMsgQError, SvmMsgQRingConfig};
use hammer_infra::svm::queue::SvmQueueConditionalWait;
use hammer_infra::sync::SpinLock;
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, NodeRuntime, RuntimeResult, interrupt_worker_node,
    is_current_worker,
};
#[cfg(target_os = "linux")]
use hammer_runtime::{FILE_MAIN, File, FileFunctions, FileReadinessMode};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::app::{ApplicationMain, SessionCleanup};
use super::error::{SessionError, SessionQueueError};
use super::segment_manager::{SegmentManager, SegmentManagerError, SegmentManagerMain};
use crate::transport::{Transport, TransportSendParams, TransportTxTarget};

/// VPP: session_node.c:1858-1915. A registered protocol supplies one
/// monomorphized entry; the service retains no transport object or VFT.
pub type SessionIoDispatch = fn(
    &mut DataPlaneMain,
    &mut SessionWorker,
    u32,
    SessionEventType,
) -> Result<(usize, bool), SessionError>;

/// VPP session.c:1827-1847; session_node.c:1858-1910. One TX entry per
/// Session type; the protocol's RX/control/time entries remain separate.
pub type SessionTxDispatch = fn(
    &mut SessionWorker,
    &mut DataPlaneMain,
    &mut NodeRuntime,
    u32,
    &mut usize,
) -> SessionTxOutcome;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionTxOutcome {
    Ok,
    NoData,
    NoBuffers,
}

/// VPP: session_node.c:2024-2031, session_update_time_subscribers.
pub type SessionTimeDispatch =
    fn(&mut DataPlaneMain, &mut SessionWorker, f64) -> Result<(), SessionError>;

/// VPP: session_node.c:1756-1790 and session.c:1641-1709. A transport
/// plugin monomorphizes its Transport trait calls at registration.
pub type SessionControlDispatch =
    fn(&mut SessionWorker, &mut DataPlaneMain, u32, SessionEventType) -> Result<(), SessionError>;

pub const SESSION_INDEX_INVALID: u32 = u32::MAX;

#[repr(C)]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, Hash, KnownLayout, FromBytes, Immutable, IntoBytes,
)]
pub struct SessionHandle {
    pub worker_index: u32,
    pub session_index: u32,
}

impl SessionHandle {
    #[inline(always)]
    pub const fn invalid() -> Self {
        Self {
            worker_index: SESSION_INDEX_INVALID,
            session_index: SESSION_INDEX_INVALID,
        }
    }
}

impl From<hammer_core::session::SessionHandle> for SessionHandle {
    #[inline(always)]
    fn from(handle: hammer_core::session::SessionHandle) -> Self {
        Self {
            worker_index: handle.thread_index,
            session_index: handle.session_index,
        }
    }
}

impl From<SessionHandle> for hammer_core::session::SessionHandle {
    #[inline(always)]
    fn from(handle: SessionHandle) -> Self {
        Self::new(handle.session_index, handle.worker_index)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionConfig {
    pub worker_count: u32,
    pub configured_worker_mq_length: u32,
    pub worker_mq_segment_size: usize,
    pub preallocated_sessions: u32,
    pub session_enable_asap: bool,
    pub poll_main: bool,
    pub use_private_rx_mqs: bool,
    pub no_adaptive: bool,
    pub dma_enabled: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            worker_count: 1,
            configured_worker_mq_length: 0,
            worker_mq_segment_size: 0,
            preallocated_sessions: 0,
            session_enable_asap: false,
            poll_main: false,
            use_private_rx_mqs: false,
            no_adaptive: false,
            dma_enabled: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionEventElement {
    pub event: SessionEvent,
    pub next: u32,
    pub previous: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionControlData {
    pub bytes: [u8; 86],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionRxSegment {
    pub buffer_index: u32,
    pub offset: u32,
    pub length: u32,
}

#[derive(Debug)]
pub struct SessionDmaTransfer {
    pub pending_tx_buffers: Vec<u32>,
    pub pending_tx_nexts: Vec<u16>,
}

#[repr(C)]
#[derive(Debug)]
pub struct SessionTxContext {
    cacheline0: hammer_infra::align::CacheLineAlignMark,
    pub session: Option<SessionHandle>,
    pub send_params: TransportSendParams,
    pub max_dequeue: u32,
    pub left_to_send: u32,
    pub max_length_to_send: u32,
    pub dequeue_per_first_buffer: u16,
    pub dequeue_per_buffer: u16,
    pub segments_per_event: u16,
    pub buffers_needed: u16,
    pub buffers_per_segment: u8,
    cacheline1: hammer_infra::align::CacheLineAlignMark,
    pub tx_buffers: Vec<u32>,
    pub transport_pending_buffers: Vec<u32>,
}

impl Default for SessionTxContext {
    fn default() -> Self {
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
            session: None,
            send_params: TransportSendParams::default(),
            max_dequeue: 0,
            left_to_send: 0,
            max_length_to_send: 0,
            dequeue_per_first_buffer: 0,
            dequeue_per_buffer: 0,
            segments_per_event: 0,
            buffers_needed: 0,
            buffers_per_segment: 0,
            cacheline1: hammer_infra::align::CacheLineAlignMark,
            tx_buffers: Vec::new(),
            transport_pending_buffers: Vec::new(),
        }
    }
}

/// VPP: session_node.c:1130-1237, `session_tx_fill_buffer` and chain tail.
#[inline(always)]
fn tx_fill_buffer(
    runtime: &mut DataPlaneMain,
    fifo: &SvmFifo,
    context: &mut SessionTxContext,
    buffer_position: &mut usize,
    fifo_offset: &mut usize,
    payload_len: usize,
) -> u32 {
    let head = context.tx_buffers[*buffer_position];
    let mut previous = head;
    let mut remaining = payload_len;
    let mut tail_len = 0usize;
    while remaining != 0 {
        let index = context.tx_buffers[*buffer_position];
        *buffer_position += 1;
        if index == head {
            runtime.buffer_mut(index).make_headroom(140);
        }
        let capacity = if index == head {
            context.dequeue_per_first_buffer
        } else {
            context.dequeue_per_buffer
        };
        let length = remaining
            .min(usize::from(capacity))
            .min(runtime.buffer(index).space_left_at_end());
        assert_ne!(length, 0, "allocated TX Buffer has payload capacity");
        let payload = runtime.buffer_mut(index).put_uninit(length as u16);
        assert_eq!(
            fifo.peek(*fifo_offset, length, payload),
            length,
            "Session TX FIFO retains packetized bytes"
        );
        if index != head {
            runtime.buffer_mut(previous).set_next_buffer(Some(index));
            tail_len += length;
        }
        previous = index;
        *fifo_offset += length;
        remaining -= length;
    }
    if tail_len != 0 {
        runtime
            .buffer_mut(head)
            .set_total_len_not_including_first(tail_len)
            .expect("TX Buffer chain length fits its header");
    }
    context.left_to_send -= payload_len as u32;
    head
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionWorkerState {
    Polling,
    Interrupt,
    Idle,
}

impl From<u8> for SessionWorkerState {
    #[inline(always)]
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Polling,
            1 => Self::Interrupt,
            2 => Self::Idle,
            _ => panic!("invalid Session worker state {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionWorkerFlags {
    pub adaptive: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionMigrationRequest {
    pub old: SessionHandle,
    pub new: SessionHandle,
}

pub struct SessionMigrationState {
    pub requests: Vec<SessionMigrationRequest>,
    pub handling: Vec<SessionMigrationRequest>,
}

pub struct PoolReallocationState {
    pub workers_at_barrier: u32,
    pub workers_doing_work: u32,
}

struct SessionRpc {
    callback: fn(u64),
    argument: u64,
    sequence: u64,
}

// VPP session.c:20-86. Selects the payload written to the target worker's
// IO ring; the queue itself still stores one SessionEvent representation.
#[derive(Clone, Copy)]
enum SessionQueueEvent {
    Io {
        event_type: SessionEventType,
        session_index: u32,
    },
    Session {
        event_type: SessionEventType,
        handle: SessionHandle,
    },
    Rpc {
        callback: fn(u64),
        argument: u64,
    },
}

#[repr(C)]
pub struct SessionWorker {
    cacheline0: hammer_infra::align::CacheLineAlignMark,
    sessions: Pool<Session>,
    event_queue_index: u32,
    last_time: f64,
    last_time_us: u64,
    worker_index: u32,
    transport_time_subscriptions: Vec<u8>,
    sessions_to_enqueue: Vec<SessionHandle>,
    app_workers_pending: Bitmap,
    input_node: Option<NodeId>,
    queue_node: Option<NodeId>,
    timer_fd: Option<OwnedFd>,
    timer_fd_file: Option<u32>,
    flags: SessionWorkerFlags,
    tx_context: SessionTxContext,
    event_elements: Pool<SessionEventElement>,
    control_event_data: Pool<SessionControlData>,
    control_events: LinkedList<u32>,
    new_events: LinkedList<u32>,
    old_events: LinkedList<u32>,
    pending_connects: LinkedList<u32>,
    events_pending_main: LinkedList<u32>,
    pending_tx_buffers: Vec<u32>,
    pending_tx_nexts: Vec<u16>,
    pending_connect_count: u32,
    pending_notifications: Vec<u64>,
    rx_segments: Vec<SessionRxSegment>,
    migration: SpinLock<SessionMigrationState>,
    config_index: u32,
    dma_enabled: bool,
    dma_transfers: Vec<SessionDmaTransfer>,
    dma_head: u16,
    dma_tail: u16,
    dma_size: u16,
    dma_batch_number: u16,
    dma_batch: u32,
    last_event_poll: f64,
}

/// The FIFO outcome consumed by a transport's ACK and receive-window logic.
/// VPP: `session_enqueue_stream_connection`, session.h:778-829.
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
        newest: Option<(u32, NonZeroU32)>,
        rx_available: u32,
    },
}

impl SessionWorker {
    #[inline(always)]
    fn state(&self) -> SessionWorkerState {
        SessionMain::global()
            .expect("Session Main owns the published worker state")
            .worker_state(self.worker_index)
    }

    fn new(worker_index: u32, config: SessionConfig) -> Self {
        let preallocated_per_worker = if config.worker_count == 1 {
            config.preallocated_sessions
        } else {
            ((u64::from(config.preallocated_sessions) * 11) / (u64::from(config.worker_count) * 10))
                as u32
        };
        let event_capacity = config.configured_worker_mq_length.max(2_048) as usize;
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
            sessions: if preallocated_per_worker == 0 {
                Pool::new()
            } else {
                Pool::with_fixed_capacity(preallocated_per_worker)
            },
            event_queue_index: worker_index,
            last_time: 0.0,
            last_time_us: 0,
            worker_index,
            transport_time_subscriptions: Vec::new(),
            sessions_to_enqueue: Vec::new(),
            app_workers_pending: Bitmap::new(),
            input_node: None,
            queue_node: None,
            timer_fd: None,
            timer_fd_file: None,
            flags: SessionWorkerFlags { adaptive: false },
            tx_context: SessionTxContext::default(),
            event_elements: Pool::with_capacity(event_capacity),
            control_event_data: Pool::with_capacity(event_capacity),
            control_events: LinkedList::new(),
            new_events: LinkedList::new(),
            old_events: LinkedList::new(),
            pending_connects: LinkedList::new(),
            events_pending_main: LinkedList::new(),
            pending_tx_buffers: Vec::new(),
            pending_tx_nexts: Vec::new(),
            pending_connect_count: 0,
            pending_notifications: Vec::new(),
            rx_segments: Vec::new(),
            migration: SpinLock::new(SessionMigrationState {
                requests: Vec::new(),
                handling: Vec::new(),
            }),
            config_index: 0,
            dma_enabled: config.dma_enabled,
            dma_transfers: Vec::new(),
            dma_head: 0,
            dma_tail: 0,
            dma_size: 0,
            dma_batch_number: 0,
            dma_batch: 0,
            last_event_poll: 0.0,
        }
    }

    /// VPP: session_node.c:2180-2217. The worker retains the timerfd for
    /// settime; FileMain owns a duplicate for readiness dispatch.
    #[cfg(target_os = "linux")]
    fn enable_adaptive_mode(&mut self, runtime: &DataPlaneMain) -> Result<(), SessionQueueError> {
        assert!(self.timer_fd.is_none() && self.timer_fd_file.is_none());
        // SAFETY: timerfd_create returns a fresh owned descriptor on success.
        let raw_fd = unsafe {
            libc::timerfd_create(
                libc::CLOCK_MONOTONIC,
                libc::TFD_NONBLOCK | libc::TFD_CLOEXEC,
            )
        };
        if raw_fd < 0 {
            return Err(SessionQueueError::TimerCreate {
                worker: self.worker_index,
                source: std::io::Error::last_os_error(),
            });
        }
        // SAFETY: the successful timerfd_create transferred this descriptor.
        let timer_fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        let registered_fd =
            timer_fd
                .try_clone()
                .map_err(|source| SessionQueueError::TimerDuplicate {
                    worker: self.worker_index,
                    source,
                })?;
        let mut file = File::new(
            registered_fd,
            format!("session-wrk-tfd-{}", runtime.thread_index()),
            u64::from(runtime.thread_index()),
            FileFunctions {
                read: Some(super::node::session_queue_timer_ready),
                write: None,
                error: None,
            },
        );
        file.set_polling_thread_index(runtime.thread_index());
        file.set_readiness_mode(FileReadinessMode::Drain);
        let file_index = FILE_MAIN
            .get()
            .expect("FileMain initializes before Data Workers")
            .add(file)
            .map_err(|source| SessionQueueError::TimerRegistration {
                worker: self.worker_index,
                source,
            })?;
        self.timer_fd = Some(timer_fd);
        self.timer_fd_file = Some(file_index);
        self.flags.adaptive = true;
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn enable_adaptive_mode(&mut self, _: &DataPlaneMain) -> Result<(), SessionQueueError> {
        Err(SessionQueueError::TimerCreate {
            worker: self.worker_index,
            source: std::io::Error::from(std::io::ErrorKind::Unsupported),
        })
    }

    /// VPP: session_node.c:43-80. Polling disarms, Interrupt wakes every
    /// millisecond, and Idle wakes every hundred milliseconds.
    #[inline]
    #[cfg(target_os = "linux")]
    fn set_state(&mut self, state: SessionWorkerState) -> Result<(), SessionQueueError> {
        let timer_fd = self
            .timer_fd
            .as_ref()
            .expect("adaptive Session worker retains its timerfd");
        let nanoseconds =
            Self::timeout(state).map_or(0, |timeout| libc::c_long::from(timeout.subsec_nanos()));
        let interval = libc::timespec {
            tv_sec: 0,
            tv_nsec: nanoseconds,
        };
        let spec = libc::itimerspec {
            it_interval: interval,
            it_value: interval,
        };
        // SAFETY: timer_fd remains owned by this worker for the syscall.
        if unsafe { libc::timerfd_settime(timer_fd.as_raw_fd(), 0, &spec, std::ptr::null_mut()) }
            < 0
        {
            return Err(SessionQueueError::TimerArm {
                worker: self.worker_index,
                source: std::io::Error::last_os_error(),
            });
        }
        SessionMain::global()
            .expect("Session Main owns the published worker state")
            .set_worker_state(self.worker_index, state);
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn set_state(&mut self, _: SessionWorkerState) -> Result<(), SessionQueueError> {
        Err(SessionQueueError::TimerArm {
            worker: self.worker_index,
            source: std::io::Error::from(std::io::ErrorKind::Unsupported),
        })
    }

    /// VPP: session_node.c:56-66; Hammer Data Workers have no thread-zero case.
    #[inline(always)]
    const fn timeout(state: SessionWorkerState) -> Option<Duration> {
        match state {
            SessionWorkerState::Polling => None,
            SessionWorkerState::Interrupt => Some(Duration::from_millis(1)),
            SessionWorkerState::Idle => Some(Duration::from_millis(100)),
        }
    }

    /// VPP: session.h:1112-1116. Transport subscribers run after this write.
    #[inline(always)]
    pub(crate) fn update_time(&mut self, now: f64) {
        self.last_time = now;
        self.last_time_us = (now * 1_000_000.0) as u64;
    }

    /// VPP: session_node.c:1989-2030,2160-2164. Called after event and TX
    /// dispatch; its only graph mutation is the queue's polling mode.
    pub(crate) fn update_state(&mut self, runtime: &DataPlaneMain) {
        if !self.flags.adaptive {
            return;
        }
        let has_events = !self.event_elements.is_empty();
        let vectors = runtime.max_internal_frame_vectors();
        let next = match self.state() {
            SessionWorkerState::Polling if !has_events && vectors < 1 => {
                Some(SessionWorkerState::Interrupt)
            }
            SessionWorkerState::Interrupt if has_events || vectors > 1 => {
                Some(SessionWorkerState::Polling)
            }
            SessionWorkerState::Interrupt if self.sessions.is_empty() => {
                Some(SessionWorkerState::Idle)
            }
            // VPP's five permanent list heads make its Idle test true on
            // every queue dispatch, including a timer-only wakeup.
            SessionWorkerState::Idle => Some(SessionWorkerState::Interrupt),
            _ => None,
        };
        let Some(next) = next else { return };
        if let Err(error) = self.set_state(next) {
            tracing::error!(worker = self.worker_index, %error, "Session timer arm failed");
            self.flags.adaptive = false;
            SessionMain::global()
                .expect("Session Main owns the published worker state")
                .set_worker_state(self.worker_index, SessionWorkerState::Polling);
            runtime
                .nodes()
                .set_node_state(
                    self.queue_node
                        .expect("session-queue installs before dispatch"),
                    hammer_core::data_plane::NodeState::Polling,
                )
                .expect("installed session-queue accepts Polling state");
            return;
        }
        if next == SessionWorkerState::Polling || next == SessionWorkerState::Interrupt {
            runtime
                .nodes()
                .set_node_state(
                    self.queue_node
                        .expect("session-queue installs before dispatch"),
                    if next == SessionWorkerState::Polling {
                        hammer_core::data_plane::NodeState::Polling
                    } else {
                        hammer_core::data_plane::NodeState::Interrupt
                    },
                )
                .expect("installed session-queue accepts its worker state");
        }
    }

    #[inline(always)]
    pub fn session(&self, session_index: u32) -> Option<&Session> {
        self.sessions.get(session_index)
    }

    #[inline(always)]
    pub fn session_mut(&mut self, session_index: u32) -> Option<&mut Session> {
        self.sessions.get_mut(session_index)
    }

    #[inline(always)]
    pub const fn worker_index(&self) -> u32 {
        self.worker_index
    }

    /// VPP: `session_wrk_program_app_wrk_evts`, session.c:565-576.
    pub fn program_app_worker(&mut self, runtime: &DataPlaneMain, app_worker: u32) {
        debug_assert_eq!(
            runtime
                .data_worker_id()
                .expect("Application events execute on a Data Worker")
                .slot(),
            self.worker_index as usize
        );
        let need_interrupt = self.app_workers_pending.first_set().is_none();
        self.app_workers_pending.set(app_worker as usize);
        if need_interrupt {
            runtime
                .set_node_interrupt_pending(
                    self.input_node
                        .expect("session-input NodeId installs before application events"),
                )
                .expect("registered session-input Node accepts an interrupt");
        }
    }

    /// A callback can append another AppWorker event without holding the
    /// graph runtime; the next Session Queue dispatch schedules its input.
    pub(crate) fn schedule_pending_app_events(&self, runtime: &DataPlaneMain) {
        if self.app_workers_pending.first_set().is_some() {
            runtime
                .set_node_interrupt_pending(
                    self.input_node
                        .expect("session-input installs before Application events"),
                )
                .expect("registered session-input Node accepts an interrupt");
        }
    }

    pub(crate) fn install_input_node(&mut self, node: NodeId) {
        assert!(
            self.input_node.replace(node).is_none(),
            "session-input installs once"
        );
    }

    pub(crate) fn install_queue_node(&mut self, node: NodeId) {
        assert!(
            self.queue_node.replace(node).is_none(),
            "session-queue installs once"
        );
    }

    /// VPP: session.h:1101-1108, `session_add_pending_tx_buffer`.
    #[inline(always)]
    pub fn add_pending_tx_buffer(
        &mut self,
        runtime: &DataPlaneMain,
        buffer_index: u32,
        next: crate::session::SessionQueueNext,
    ) {
        assert_eq!(
            runtime
                .data_worker_id()
                .expect("transport output runs on a Data Worker")
                .slot(),
            self.worker_index as usize
        );
        self.pending_tx_buffers.push(buffer_index);
        self.pending_tx_nexts.push(next.slot());
        if self.state() == SessionWorkerState::Interrupt {
            runtime
                .set_node_interrupt_pending(
                    self.queue_node
                        .expect("session-queue installs before transport output"),
                )
                .expect("registered session-queue accepts an interrupt");
        }
    }

    /// VPP: session_node.c:1965-1973, `session_flush_pending_tx_buffers`.
    pub(crate) fn flush_pending_tx_buffers(
        &mut self,
        runtime: &mut DataPlaneMain,
        node: &mut hammer_runtime::NodeRuntime,
        frame: &mut hammer_core::data_plane::Frame,
    ) -> usize {
        assert_eq!(self.pending_tx_buffers.len(), self.pending_tx_nexts.len());
        let count = self.pending_tx_buffers.len();
        for offset in (0..self.pending_tx_buffers.len())
            .step_by(hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY)
        {
            let end = (offset + hammer_core::data_plane::DEFAULT_BUFFER_FRAME_CAPACITY)
                .min(self.pending_tx_buffers.len());
            let buffers = &self.pending_tx_buffers[offset..end];
            frame.set_vector_count(buffers.len());
            frame.vector_args_mut().copy_from_slice(buffers);
            runtime.enqueue_to_next(node, frame, &self.pending_tx_nexts[offset..end]);
        }
        frame.set_vector_count(0);
        self.pending_tx_buffers.clear();
        self.pending_tx_nexts.clear();
        count
    }

    #[inline(always)]
    pub fn pending_app_workers(&self) -> &Bitmap {
        &self.app_workers_pending
    }

    #[inline]
    pub fn clear_pending_app_worker(&mut self, app_worker: u32) {
        self.app_workers_pending.clear(app_worker as usize);
    }

    pub fn allocate(
        &mut self,
        state: SessionState,
        session_type: u8,
        transport_protocol: u8,
        opaque: u32,
    ) -> SessionHandle {
        let handle = SessionHandle {
            worker_index: self.worker_index,
            session_index: SESSION_INDEX_INVALID,
        };
        let session = Session::new(handle, state, session_type, transport_protocol, opaque);
        let session_index = self.sessions.insert(session);
        let handle = SessionHandle {
            session_index,
            ..handle
        };
        self.sessions
            .get_mut(session_index)
            .expect("inserted Session remains in its worker pool")
            .handle = handle;
        handle
    }

    /// VPP: `session_stream_accept`, session.c:1215-1276. The connection
    /// exists before its Session, and the Application adds FIFOs afterward.
    pub fn allocate_accepted(
        &mut self,
        listener: SessionHandle,
        connection_index: u32,
        session_type: u8,
        transport_protocol: u8,
        opaque: u32,
    ) -> SessionHandle {
        let handle = self.allocate(
            SessionState::Created,
            session_type,
            transport_protocol,
            opaque,
        );
        let session = self
            .session_mut(handle.session_index)
            .expect("new accepted Session remains in its worker pool");
        session.listener_handle = listener;
        session.connection_index = connection_index;
        handle
    }

    pub fn attach_transport(
        &mut self,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        if session.worker_index != self.worker_index {
            return Err(SessionError::NoSession);
        }
        let entry = self
            .session_mut(session.session_index)
            .ok_or(SessionError::NoSession)?;
        entry.connection_index = connection_index;
        Ok(())
    }

    pub(crate) fn attach_application(
        &mut self,
        session: SessionHandle,
        application_worker: u32,
        segment: &SegmentManager<'_>,
    ) -> Result<(), SegmentManagerError> {
        assert_ne!(application_worker, SESSION_INDEX_INVALID);
        assert_eq!(session.worker_index, self.worker_index);
        let worker_index = self.worker_index;
        let entry = self
            .session_mut(session.session_index)
            .expect("validated accepted Session remains allocated");
        assert_eq!(entry.handle, session);
        assert_eq!(entry.application_worker, SESSION_INDEX_INVALID);
        assert!(entry.rx_fifo.is_none() && entry.tx_fifo.is_none());
        let (rx_fifo, tx_fifo) = segment.allocate_session_fifos(worker_index)?;
        entry.rx_fifo = Some(Rc::new(rx_fifo));
        entry.tx_fifo = Some(Rc::new(tx_fifo));
        entry.application_worker = application_worker;
        Ok(())
    }

    /// VPP: `session_cleanup`, session.c:300-304. Removing the owned Session
    /// runs its Drop, which returns its FIFO pair to the segment owner.
    pub fn cleanup(&mut self, session: SessionHandle) -> Option<()> {
        if session.worker_index != self.worker_index {
            return None;
        }
        let entry = self.sessions.get(session.session_index)?;
        if entry.handle != session {
            return None;
        }
        assert!(
            entry.application_worker().is_none(),
            "Application cleanup must release its AppSession borrow before Session FIFO cleanup"
        );
        self.sessions
            .remove(session.session_index)
            .expect("validated Session remains in its worker pool until removal");
        Some(())
    }

    /// VPP: `session_enqueue_stream_connection`, session.h:778-829.
    /// The Session owns the only packet-to-application FIFO copy boundary.
    pub fn enqueue_rx(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        buffer_index: u32,
        offset: u32,
    ) -> Result<RxDelivery, SessionError> {
        let session = self
            .session_from_handle(handle)
            .ok_or(SessionError::NoSession)?;
        let fifo = session.rx_fifo().ok_or(SessionError::Invalid)?;
        let session_id = handle.session_index;
        let mut accepted = 0u32;
        let mut promoted = 0u32;
        let mut buffered_len = 0u32;
        let mut skip_promoted = 0usize;
        let mut newest = None;
        for buffer in runtime.chain(buffer_index) {
            let bytes = buffer.current();
            let length = u32::try_from(bytes.len())
                .expect("a packet buffer's current length fits the Session FIFO index");
            if offset == 0 {
                let skip = skip_promoted.min(bytes.len());
                skip_promoted -= skip;
                let bytes = &bytes[skip..];
                if !bytes.is_empty() {
                    if bytes.len() >= fifo.max_enqueue() {
                        fifo.want_deq_notification();
                    }
                    let result = fifo.enqueue_ooo(0, bytes).map_err(|source| {
                        SessionError::RxOutOfOrderEnqueue {
                            session_id,
                            offset: 0,
                            source,
                        }
                    })?;
                    accepted = accepted
                        .checked_add(result.accepted)
                        .expect("RX packet chain length fits u32");
                    promoted = promoted
                        .checked_add(result.delivered)
                        .expect("promoted RX FIFO bytes fit u32");
                    skip_promoted += result.delivered as usize;
                    if result.accepted as usize != bytes.len() {
                        break;
                    }
                }
            } else {
                let chunk_offset = offset.checked_add(buffered_len).ok_or(
                    SessionError::RxOutOfOrderOffsetOverflow {
                        session_id,
                        offset,
                        buffered_len,
                    },
                )?;
                let result = match fifo.enqueue_ooo(chunk_offset, bytes) {
                    Ok(result) => result,
                    Err(
                        hammer_infra::svm::fifo::FifoError::OutOfOrderCapacityExceeded { .. }
                        | hammer_infra::svm::fifo::FifoError::SegmentExhausted,
                    ) => break,
                    Err(source) => {
                        return Err(SessionError::RxOutOfOrderEnqueue {
                            session_id,
                            offset: chunk_offset,
                            source,
                        });
                    }
                };
                accepted = accepted
                    .checked_add(result.accepted)
                    .expect("RX packet chain length fits u32");
                promoted = promoted
                    .checked_add(result.delivered)
                    .expect("promoted RX FIFO bytes fit u32");
                newest = result
                    .start
                    .and_then(|start| NonZeroU32::new(result.len).map(|length| (start, length)));
            }
            buffered_len = buffered_len
                .checked_add(length)
                .expect("RX packet chain length fits u32");
        }
        let rx_available = u32::try_from(fifo.max_enqueue()).unwrap_or(u32::MAX);
        if rx_available == 0 {
            fifo.want_deq_notification();
        }
        if offset == 0 {
            let session = self
                .session_mut(handle.session_index)
                .expect("RX Session remains allocated during FIFO enqueue");
            if !session.flags.rx_event {
                session.flags.rx_event = true;
                self.sessions_to_enqueue.push(handle);
            }
        }
        let Some(accepted) = NonZeroU32::new(accepted) else {
            return Ok(RxDelivery::NotAccepted { rx_available });
        };
        if offset == 0 {
            Ok(RxDelivery::InOrder {
                accepted,
                promoted,
                rx_available,
            })
        } else {
            Ok(RxDelivery::OutOfOrder {
                accepted,
                newest,
                rx_available,
            })
        }
    }

    /// VPP: `session_wrk_handle_mq`, session_node.c:1975-1992. Only the
    /// descriptors present at entry are imported in this dispatch.
    pub fn drain_event_queue(&mut self, queue: &SvmMsgQ) -> Result<usize, SessionError> {
        let count = queue.size() as usize;
        for _ in 0..count {
            let descriptor = queue
                .sub(SvmQueueConditionalWait::Nowait)
                .map_err(|source| SessionError::MessageQueueAllocation { source })?;
            // VPP session_node.c:1936-1960. The control ring stores a
            // variable-size message at the SessionEvent union offset; copy
            // its fixed maximum before returning this descriptor to the ring.
            let decoded = match unsafe { queue.message_bytes(descriptor) } {
                Ok(bytes) => {
                    let event = bytes
                        .get(..size_of::<SessionEvent>())
                        .ok_or_else(|| SessionError::MessageQueueAllocation {
                            source: SvmMsgQError::ElementSizeMismatch {
                                requested: size_of::<SessionEvent>(),
                                stored: bytes.len(),
                            },
                        })
                        .and_then(|header| {
                            SessionEvent::read_from_bytes(header).map_err(|_| {
                                SessionError::MessageQueueAllocation {
                                    source: SvmMsgQError::InvalidHeader,
                                }
                            })
                        });
                    event.and_then(|event| {
                        let event_type = SessionEventType::try_from(event.event_type)?;
                        let control_data =
                            if u8::from(event_type) >= u8::from(SessionEventType::Bound) {
                                let source = bytes
                                    .get(2..2 + size_of::<SessionControlData>())
                                    .ok_or_else(|| SessionError::MessageQueueAllocation {
                                        source: SvmMsgQError::ElementSizeMismatch {
                                            requested: 2 + size_of::<SessionControlData>(),
                                            stored: bytes.len(),
                                        },
                                    })?;
                                let mut data = SessionControlData { bytes: [0; 86] };
                                data.bytes.copy_from_slice(source);
                                Some(data)
                            } else {
                                None
                            };
                        Ok((event, event_type, control_data))
                    })
                }
                Err(source) => Err(SessionError::MessageQueueAllocation { source }),
            };
            match queue.free_msg(descriptor) {
                Ok(())
                | Err(
                    SvmMsgQError::SignalAfterCommit { .. }
                    | SvmMsgQError::EventSignalAfterCommit { .. },
                ) => {}
                Err(source) => {
                    return Err(SessionError::MessageQueueAllocation { source });
                }
            }
            let (mut event, event_type, control_data) = decoded?;
            if let Some(data) = control_data {
                let index = self.allocate_control_data(data);
                event.session_index = index;
            }
            if u8::from(event_type) >= u8::from(SessionEventType::Rpc) {
                self.allocate_control_event(event);
            } else {
                self.allocate_new_event(event);
            }
        }
        Ok(count)
    }

    /// VPP: session_node.c:1756-1855,2069-2092. Only the control-list prefix
    /// present after MQ import is consumed in this queue dispatch.
    pub(crate) fn dispatch_control_events(
        &mut self,
        runtime: &mut DataPlaneMain,
        main: &SessionMain,
    ) -> Result<(), SessionError> {
        let Some(last_index) = self.control_events.back().copied() else {
            return Ok(());
        };
        loop {
            let index = self
                .control_events
                .pop_front()
                .expect("control-list tail remains reachable during this dispatch");
            let event = self
                .event_elements
                .get(index)
                .expect("control list retains its event element")
                .event;
            let event_type = SessionEventType::try_from(event.event_type)
                .expect("MQ import validates Session control event types");
            let dispatch = match event_type {
                SessionEventType::Rpc => {
                    let index = event.session_index();
                    let sequence = event.rpc_sequence;
                    if let Some(request) = main.take_rpc(index, sequence) {
                        (request.callback)(request.argument);
                    }
                    Ok(())
                }
                SessionEventType::HalfClose | SessionEventType::Close | SessionEventType::Reset => {
                    let handle = event.session_handle();
                    if let Some(session) = self.session_from_handle(handle) {
                        let state = session.load_state();
                        let protocol = session.transport_protocol();
                        match event_type {
                            SessionEventType::HalfClose
                                if matches!(
                                    state,
                                    SessionState::Ready | SessionState::TransportClosing
                                ) =>
                            {
                                if let Some(dispatch) = main.transport_control(protocol) {
                                    dispatch(self, runtime, handle.session_index, event_type)
                                } else {
                                    Err(SessionError::TransportNotRegistered)
                                }
                            }
                            SessionEventType::Close | SessionEventType::Reset
                                if u8::from(state) >= u8::from(SessionState::AppClosed) =>
                            {
                                if state == SessionState::TransportClosed {
                                    session.store_state(SessionState::Closed);
                                } else if u8::from(state)
                                    >= u8::from(SessionState::TransportDeleted)
                                    && !session.flags.half_open
                                {
                                    self.queue_cleanup_event(
                                        runtime,
                                        handle,
                                        SessionCleanup::Session,
                                    );
                                }
                                Ok(())
                            }
                            SessionEventType::Close | SessionEventType::Reset => {
                                if let Some(dispatch) = main.transport_control(protocol) {
                                    session.store_state(SessionState::AppClosed);
                                    dispatch(self, runtime, handle.session_index, event_type)
                                } else {
                                    Err(SessionError::TransportNotRegistered)
                                }
                            }
                            SessionEventType::HalfClose => Ok(()),
                            _ => unreachable!("matched a transport control event"),
                        }
                    } else {
                        // VPP session_event_dispatch_ctrl skips stale handles.
                        Ok(())
                    }
                }
                _ => {
                    tracing::warn!(?event_type, "unhandled Session control event");
                    Ok(())
                }
            };
            if u8::from(event_type) >= u8::from(SessionEventType::Bound) {
                self.release_control_data(event.session_index())
                    .expect("imported control data remains owned by its event");
            }
            self.event_elements
                .remove(index)
                .expect("dispatched control element remains allocated");
            dispatch?;
            if index == last_index {
                break;
            }
        }
        Ok(())
    }

    /// VPP: `SESSION_IO_EVT_BUILTIN_RX`, session_node.c:1891-1898. The worker
    /// MQ arm contains an index; the AppWorker notification is a separate step.
    pub(crate) fn dispatch_builtin_rx(&mut self, runtime: &DataPlaneMain) {
        let pending = self.new_events.len();
        for _ in 0..pending {
            let index = self
                .new_events
                .pop_front()
                .expect("snapshot contains a new Session event");
            let event = self
                .event_elements
                .get(index)
                .expect("new event list retains its pool element")
                .event;
            if event.event_type != u8::from(SessionEventType::BuiltinRx) {
                self.new_events.push_back(index);
                continue;
            }
            self.event_elements
                .remove(index)
                .expect("dispatched Session event remains allocated");
            let handle = SessionHandle {
                worker_index: self.worker_index,
                session_index: event.session_index(),
            };
            let Some(session) = self.session_from_handle(handle) else {
                continue;
            };
            if u8::from(session.load_state()) >= u8::from(SessionState::TransportClosing) {
                continue;
            }
            session
                .rx_fifo()
                .expect("attached builtin Session retains its RX FIFO")
                .unset_event();
            self.enqueue_notify(runtime, handle);
        }
    }

    /// VPP: session_node.c:1858-1915,2033-2140. New events precede the
    /// previous dispatch's old events; each list is visited only once.
    pub(crate) fn dispatch_io_events(
        &mut self,
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        main: &SessionMain,
    ) -> Result<usize, SessionError> {
        // VPP session_node.c:2046,2096: buffers already pending at entry
        // consume the same frame budget as newly packetized IO events.
        let mut packets = self.pending_tx_buffers.len();
        let new_count = self.new_events.len();
        let old_count = self.old_events.len();
        for (old, count) in [(false, new_count), (true, old_count)] {
            for _ in 0..count {
                if packets >= crate::session::node::SESSION_QUEUE_IO_BUDGET {
                    break;
                }
                let index = if old {
                    self.old_events.pop_front()
                } else {
                    self.new_events.pop_front()
                }
                .expect("snapshot retains its Session event");
                let event = self
                    .event_elements
                    .get(index)
                    .expect("linked Session event retains its pool element")
                    .event;
                let event_type = SessionEventType::try_from(event.event_type)?;
                let session_index = event.session_index();
                let mut keep = false;
                match event_type {
                    SessionEventType::BuiltinRx => {
                        let handle = SessionHandle {
                            worker_index: self.worker_index,
                            session_index,
                        };
                        if let Some(session) = self.session_from_handle(handle)
                            && u8::from(session.load_state())
                                < u8::from(SessionState::TransportClosing)
                        {
                            session
                                .rx_fifo()
                                .expect("attached builtin Session retains its RX FIFO")
                                .unset_event();
                            self.enqueue_notify(runtime, handle);
                        }
                    }
                    SessionEventType::Tx | SessionEventType::TxFlush => {
                        if let Some(session) = self.session(session_index) {
                            let tx = main
                                .session_tx(session.session_type())
                                .expect("registered Session type retains its TX entry");
                            tx(self, runtime, node, index, &mut packets);
                        } else {
                            self.event_elements
                                .remove(index)
                                .expect("orphaned TX event remains allocated");
                        }
                        continue;
                    }
                    SessionEventType::Rx => {
                        if let Some(session) = self.session(session_index) {
                            let dispatch = main
                                .transport_io(session.transport_protocol())
                                .ok_or(SessionError::TransportNotRegistered)?;
                            let (sent, pending) =
                                dispatch(runtime, self, session_index, event_type)?;
                            packets += sent;
                            keep = pending;
                        }
                    }
                    _ => panic!("Session IO list contains control event {event_type:?}"),
                }
                if keep {
                    self.old_events.push_back(index);
                } else {
                    self.event_elements
                        .remove(index)
                        .expect("dispatched Session event remains allocated");
                }
            }
        }
        Ok(packets)
    }

    /// VPP: session_node.c:2039-2050, transport time precedes worker MQ IO.
    pub(crate) fn update_transport_time(
        &mut self,
        runtime: &mut DataPlaneMain,
        main: &SessionMain,
        now: f64,
    ) -> Result<(), SessionError> {
        for index in 0..self.transport_time_subscriptions.len() {
            let protocol = self.transport_time_subscriptions[index];
            if let Some(update) = main.transport_time(protocol) {
                update(runtime, self, now)?;
            }
        }
        Ok(())
    }

    /// VPP: session_node.c:1696-1747, internal custom TX.
    pub fn tx_fifo_dequeue_internal<T, E>(
        &mut self,
        runtime: &mut DataPlaneMain,
        _: &mut NodeRuntime,
        event_index: u32,
        packets: &mut usize,
        transport: &T,
    ) -> usize
    where
        T: Transport<E>,
    {
        let event = self
            .event_elements
            .get(event_index)
            .expect("internal TX event remains allocated")
            .event;
        let session_index = event.session_index();
        let Some(session) = self.sessions.get(session_index) else {
            self.event_elements.remove(event_index);
            return 0;
        };
        let state = session.load_state();
        if u8::from(state) >= u8::from(SessionState::TransportClosed)
            || (state == SessionState::Connecting && session.flags.half_open)
        {
            self.event_elements.remove(event_index);
            return 0;
        }
        let handle = session.handle();
        self.sessions
            .get_mut(session_index)
            .expect("internal TX retains its Session")
            .flags
            .custom_tx = false;
        let max_burst = crate::session::node::SESSION_QUEUE_IO_BUDGET
            .saturating_sub(*packets)
            .min(crate::transport::Pacer::MAX_BURST_PACKETS as usize);
        let mut params = TransportSendParams {
            max_burst_size: max_burst as u32,
            ..TransportSendParams::default()
        };
        let sent = transport.custom_tx(
            runtime,
            self,
            TransportTxTarget::Session(handle),
            &mut params,
        );
        assert!(
            sent <= max_burst,
            "internal transport stays within Session frame budget"
        );
        *packets += sent;
        let session = self
            .sessions
            .get(session_index)
            .expect("internal custom TX retains its Session");
        let custom_tx = session.flags.custom_tx;
        let notify_dequeue = params.bytes_dequeued != 0
            && session
                .tx_fifo()
                .expect("internal custom TX retains its FIFO")
                .needs_deq_notification(params.bytes_dequeued as usize);
        if custom_tx {
            self.old_events.push_back(event_index);
        } else if !params.flags.deschedule {
            let fifo = session
                .tx_fifo()
                .expect("internal custom TX retains its FIFO");
            fifo.unset_event();
            if fifo.max_dequeue() != 0 && fifo.set_event() {
                self.old_events.push_front(event_index);
            } else {
                self.event_elements.remove(event_index);
            }
        } else {
            self.event_elements.remove(event_index);
        }
        if notify_dequeue {
            self.notify_tx_dequeue(runtime, handle);
        }
        sent
    }

    /// VPP: session_node.c:1472-1684, session_tx_fifo_peek_and_snd. TCP
    /// retains FIFO bytes until ACK; the event remains owned by this worker.
    pub fn tx_fifo_peek_and_send<T, E>(
        &mut self,
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        event_index: u32,
        packets: &mut usize,
        transport: &T,
    ) -> SessionTxOutcome
    where
        T: Transport<E>,
    {
        self.tx_fifo_read_and_send_i::<T, E, (), false>(
            runtime,
            node,
            event_index,
            packets,
            true,
            transport,
        )
    }

    /// VPP: session_node.c:1688-1694, ordinary stream dequeue TX.
    pub fn tx_fifo_dequeue_and_send<T, E, M, const DGRAM: bool>(
        &mut self,
        runtime: &mut DataPlaneMain,
        node: &mut NodeRuntime,
        event_index: u32,
        packets: &mut usize,
        transport: &T,
    ) -> SessionTxOutcome
    where
        T: Transport<E>,
    {
        self.tx_fifo_read_and_send_i::<T, E, M, DGRAM>(
            runtime,
            node,
            event_index,
            packets,
            false,
            transport,
        )
    }

    /// VPP: session_node.c:1472-1677, common peek/dequeue burst algorithm.
    #[inline(always)]
    fn tx_fifo_read_and_send_i<T, E, M, const DGRAM: bool>(
        &mut self,
        runtime: &mut DataPlaneMain,
        _: &mut NodeRuntime,
        event_index: u32,
        packets: &mut usize,
        peek_data: bool,
        transport: &T,
    ) -> SessionTxOutcome
    where
        T: Transport<E>,
    {
        let event = self
            .event_elements
            .get(event_index)
            .expect("TX event remains in its owner worker pool")
            .event;
        let session_index = event.session_index();
        let Some(session) = self.sessions.get(session_index) else {
            self.event_elements.remove(event_index);
            return SessionTxOutcome::NoData;
        };
        let state = session.load_state();
        let custom_tx = session.flags.custom_tx;
        let not_ready = if peek_data {
            if u8::from(state) < u8::from(SessionState::Ready) {
                state != SessionState::Accepting || !custom_tx
            } else if u8::from(state) >= u8::from(SessionState::TransportClosed) {
                state == SessionState::TransportDeleted || !custom_tx
            } else {
                false
            }
        } else {
            state == SessionState::TransportDeleted || session.tx_fifo().is_none()
        };
        if not_ready {
            if peek_data && u8::from(state) < u8::from(SessionState::Ready) {
                self.old_events.push_back(event_index);
            } else {
                self.event_elements.remove(event_index);
            }
            return SessionTxOutcome::NoData;
        }
        self.tx_context.session = Some(session.handle());
        let next = SessionMain::global()
            .expect("Session Main owns the registered TX output edge")
            .session_output_next(session.session_type())
            .expect("registered Session type retains an output edge");
        let connection = session.connection_index();
        // VPP session_node.c:1278-1290 selects the listener only for
        // dequeue TX on a Listening Session; peek TX always uses a connection.
        let target = if !peek_data && state == SessionState::Listening {
            TransportTxTarget::Listener(connection)
        } else {
            TransportTxTarget::Connection(connection)
        };
        if SessionEventType::try_from(event.event_type).expect("TX event type remains valid")
            == SessionEventType::TxFlush
        {
            transport.flush_data(self, target);
            self.event_elements
                .get_mut(event_index)
                .expect("flush event remains allocated")
                .event
                .event_type = u8::from(SessionEventType::Tx);
        }
        let mut params = TransportSendParams::default();
        let max_burst = crate::session::node::SESSION_QUEUE_IO_BUDGET.saturating_sub(*packets);
        if max_burst == 0 {
            self.old_events.push_back(event_index);
            return SessionTxOutcome::NoData;
        }
        if self
            .sessions
            .get(session_index)
            .expect("TX event retains its Session")
            .flags
            .custom_tx
        {
            self.sessions
                .get_mut(session_index)
                .expect("TX event retains its Session")
                .flags
                .custom_tx = false;
            params.max_burst_size = max_burst as u32;
            let sent = transport.custom_tx(runtime, self, target, &mut params);
            assert!(
                sent <= max_burst,
                "transport custom TX stays within frame budget"
            );
            *packets += sent;
            let session = self
                .sessions
                .get(session_index)
                .expect("custom TX retains its Session");
            if u8::from(session.load_state()) >= u8::from(SessionState::TransportClosed) {
                session
                    .tx_fifo()
                    .expect("custom TX retains TX FIFO")
                    .unset_event();
                self.event_elements.remove(event_index);
                return SessionTxOutcome::Ok;
            }
            if sent == max_burst || session.flags.custom_tx {
                self.old_events.push_back(event_index);
                return SessionTxOutcome::Ok;
            }
        }
        if transport.is_descheduled(runtime, target) {
            transport.clear_descheduled(runtime, target);
        }
        transport.send_params(runtime, self, target, &mut params);
        if params.send_space == 0 {
            if params.flags.deschedule {
                transport.deschedule(runtime, target);
                self.event_elements.remove(event_index);
            } else if params.flags.postpone {
                self.old_events.push_back(event_index);
            } else {
                self.old_events.push_front(event_index);
            }
            return SessionTxOutcome::NoData;
        }
        if transport.is_tx_paced(runtime, target) {
            let burst = transport.tx_pacer_burst(runtime, target);
            if burst < 1460 {
                self.old_events.push_front(event_index);
                return SessionTxOutcome::NoData;
            }
            let paced = params.send_space.min(burst);
            let mss = u32::from(params.send_mss);
            params.send_space = if paced >= mss {
                paced - paced % mss
            } else {
                paced
            };
        }
        let fifo = self
            .sessions
            .get(session_index)
            .expect("send params retain Session")
            .tx_fifo()
            .expect("ready Session retains TX FIFO");
        let tx_offset = if peek_data {
            params.tx_offset as usize
        } else {
            0
        };
        let record_header_len =
            size_of::<SessionDatagramPrefix>() + size_of::<M>() + size_of::<u16>();
        let mut datagram_length = 0usize;
        let mut datagram_offset = 0usize;
        let mut record_starts = [0usize; crate::session::node::SESSION_QUEUE_IO_BUDGET];
        let mut record_lengths = [0usize; crate::session::node::SESSION_QUEUE_IO_BUDGET];
        let mut record_offsets = [0usize; crate::session::node::SESSION_QUEUE_IO_BUDGET];
        let mut record_count = 1usize;
        let mut fifo_offset_start = tx_offset;
        let available = if DGRAM {
            assert!(!peek_data, "datagram TX uses the dequeue entry");
            if fifo.max_dequeue() <= record_header_len {
                fifo.unset_event();
                if fifo.max_dequeue() != 0 && fifo.set_event() {
                    self.old_events.push_front(event_index);
                } else {
                    self.event_elements.remove(event_index);
                }
                return SessionTxOutcome::NoData;
            }
            let mut prefix_bytes = [0u8; size_of::<SessionDatagramPrefix>()];
            assert_eq!(
                fifo.peek(0, prefix_bytes.len(), &mut prefix_bytes),
                prefix_bytes.len()
            );
            let prefix = SessionDatagramPrefix::read_from_bytes(&prefix_bytes)
                .expect("datagram prefix has its declared layout");
            datagram_length = prefix.data_length as usize;
            datagram_offset = prefix.data_offset as usize;
            if datagram_length == 0 {
                assert_eq!(fifo.drop_dequeue(record_header_len), record_header_len);
                fifo.unset_event();
                if fifo.max_dequeue() != 0 && fifo.set_event() {
                    self.old_events.push_front(event_index);
                } else {
                    self.event_elements.remove(event_index);
                }
                return SessionTxOutcome::NoData;
            }
            assert!(
                datagram_offset < datagram_length,
                "datagram offset stays within its payload"
            );
            let record_len = record_header_len
                .checked_add(datagram_length)
                .expect("datagram record length fits usize");
            if fifo.max_dequeue() < record_len {
                self.old_events.push_front(event_index);
                return SessionTxOutcome::NoData;
            }
            let mut gso_bytes = [0u8; size_of::<u16>()];
            assert_eq!(
                fifo.peek(
                    record_header_len - gso_bytes.len(),
                    gso_bytes.len(),
                    &mut gso_bytes
                ),
                gso_bytes.len()
            );
            let gso_size = u16::from_ne_bytes(gso_bytes);
            if gso_size != 0 {
                params.send_mss = params.send_mss.min(gso_size);
            }
            let first_remaining = datagram_length - datagram_offset;
            params.send_mss = params
                .send_mss
                .min(first_remaining.min(u16::MAX as usize) as u16);
            record_lengths[0] = datagram_length;
            record_offsets[0] = datagram_offset;
            fifo_offset_start = record_header_len + datagram_offset;
            let mut total = first_remaining;
            let data_size = runtime.buffer_default_data_size();
            assert!(
                data_size > 140,
                "default Buffer Pool fits transport headers"
            );
            if datagram_length <= (data_size - 140).min(usize::from(params.send_mss)) {
                let max_offset = fifo.max_dequeue().min(32 << 10);
                let mut next_start = record_len;
                while record_count < record_starts.len()
                    && next_start + record_header_len <= max_offset
                {
                    let mut next_bytes = [0u8; size_of::<SessionDatagramPrefix>()];
                    assert_eq!(
                        fifo.peek(next_start, next_bytes.len(), &mut next_bytes),
                        next_bytes.len(),
                    );
                    let next = SessionDatagramPrefix::read_from_bytes(&next_bytes)
                        .expect("datagram prefix has its declared layout");
                    let length = next.data_length as usize;
                    let offset = next.data_offset as usize;
                    if length == 0
                        || offset >= length
                        || length - offset != first_remaining
                        || next_start + record_header_len + length > fifo.max_dequeue()
                    {
                        break;
                    }
                    record_starts[record_count] = next_start;
                    record_lengths[record_count] = length;
                    record_offsets[record_count] = offset;
                    record_count += 1;
                    total += first_remaining;
                    next_start += record_header_len + length;
                }
            }
            total
        } else {
            fifo.max_dequeue().saturating_sub(tx_offset)
        };
        let mss = usize::from(params.send_mss);
        assert_ne!(mss, 0, "sendable transport has a nonzero MSS");
        let max_len = if available < params.send_space as usize {
            if available > mss {
                available - available % mss
            } else {
                available
            }
        } else {
            params.send_space as usize
        };
        let burst = crate::session::node::SESSION_QUEUE_IO_BUDGET.saturating_sub(*packets);
        let segments = max_len.div_ceil(mss).min(burst);
        let send_len = max_len.min(segments * mss);
        if send_len == 0 {
            transport.tx_pacer_reset_bucket(runtime, target, 0);
            fifo.unset_event();
            if fifo.max_dequeue() > if DGRAM { 0 } else { tx_offset } && fifo.set_event() {
                self.old_events.push_front(event_index);
            } else {
                transport.deschedule(runtime, target);
                self.event_elements.remove(event_index);
            }
            return SessionTxOutcome::NoData;
        }
        let data_size = runtime.buffer_default_data_size();
        assert!(
            data_size > 140,
            "default Buffer Pool fits transport headers"
        );
        let full_segment_buffers = (140 + mss).div_ceil(data_size);
        let final_segment_len = send_len - (segments - 1) * mss;
        self.tx_context.send_params = params;
        self.tx_context.max_dequeue = available.min(u32::MAX as usize) as u32;
        self.tx_context.left_to_send =
            u32::try_from(send_len).expect("Session TX length fits its FIFO index");
        self.tx_context.max_length_to_send = self.tx_context.left_to_send;
        self.tx_context.dequeue_per_first_buffer = u16::try_from(mss.min(data_size - 140))
            .expect("first Buffer payload capacity fits u16");
        self.tx_context.dequeue_per_buffer =
            u16::try_from(mss.min(data_size)).expect("tail Buffer payload capacity fits u16");
        self.tx_context.segments_per_event =
            u16::try_from(segments).expect("Session frame segment count fits u16");
        self.tx_context.buffers_per_segment = u8::try_from(full_segment_buffers)
            .expect("one transport segment fits its Buffer chain count");
        let buffers_needed = (segments - 1) * usize::from(self.tx_context.buffers_per_segment)
            + (140 + final_segment_len).div_ceil(data_size);
        self.tx_context.buffers_needed =
            u16::try_from(buffers_needed).expect("Session frame Buffer count fits u16");
        let buffers_needed = usize::from(self.tx_context.buffers_needed);
        self.tx_context.tx_buffers.resize(buffers_needed, 0);
        let allocated = runtime.buffer_alloc(&mut self.tx_context.tx_buffers[..buffers_needed]);
        if allocated != buffers_needed {
            if allocated != 0 {
                runtime.buffer_free(&self.tx_context.tx_buffers[..allocated]);
            }
            runtime
                .record_current_node_error(crate::session::node::SessionQueueNodeError::NoBuffer)
                .expect("session-queue registers its Buffer exhaustion counter");
            self.old_events.push_front(event_index);
            return SessionTxOutcome::NoBuffers;
        }
        if transport.is_tx_paced(runtime, target) {
            transport.tx_pacer_update_bytes(runtime, target, self.tx_context.max_length_to_send);
        }
        let mut heads = core::mem::take(&mut self.tx_context.transport_pending_buffers);
        heads.clear();
        let mut allocated_index = 0;
        let mut fifo_offset = if peek_data {
            self.tx_context.send_params.tx_offset as usize
        } else {
            fifo_offset_start
        };
        let mut n_left = usize::from(self.tx_context.segments_per_event);
        while n_left >= 4 {
            // VPP session_node.c:1585-1594 prefetches the next two Buffer
            // headers for store before filling the current pair.
            runtime.prefetch_header_write(self.tx_context.tx_buffers[allocated_index + 2]);
            runtime.prefetch_header_write(self.tx_context.tx_buffers[allocated_index + 3]);
            if DGRAM && record_count > 1 {
                let record = heads.len();
                fifo_offset = record_starts[record] + record_header_len + record_offsets[record];
            }
            heads.push(tx_fill_buffer(
                runtime,
                fifo,
                &mut self.tx_context,
                &mut allocated_index,
                &mut fifo_offset,
                mss,
            ));
            if DGRAM && record_count > 1 {
                let record = heads.len();
                fifo_offset = record_starts[record] + record_header_len + record_offsets[record];
            }
            heads.push(tx_fill_buffer(
                runtime,
                fifo,
                &mut self.tx_context,
                &mut allocated_index,
                &mut fifo_offset,
                mss,
            ));
            n_left -= 2;
        }
        while n_left != 0 {
            if n_left > 1 {
                runtime.prefetch_header_write(self.tx_context.tx_buffers[allocated_index + 1]);
            }
            let payload_len = if n_left == 1 { final_segment_len } else { mss };
            if DGRAM && record_count > 1 {
                let record = heads.len();
                fifo_offset = record_starts[record] + record_header_len + record_offsets[record];
            }
            heads.push(tx_fill_buffer(
                runtime,
                fifo,
                &mut self.tx_context,
                &mut allocated_index,
                &mut fifo_offset,
                payload_len,
            ));
            n_left -= 1;
        }
        assert_eq!(
            allocated_index, buffers_needed,
            "TX budget matches Buffer chain lengths"
        );
        assert_eq!(
            self.tx_context.left_to_send, 0,
            "TX burst fills its planned FIFO length"
        );
        if DGRAM
            && self
                .sessions
                .get(session_index)
                .expect("datagram TX retains its Session")
                .flags
                .connectionless
        {
            let mut payload_offset = datagram_offset;
            for (record, &head) in heads.iter().enumerate() {
                let packet_len = runtime.buffer(head).current_len()
                    + runtime.buffer(head).total_len_not_including_first();
                let buffer = runtime.buffer_mut(head);
                let header = buffer.push_uninit(
                    u8::try_from(record_header_len)
                        .expect("datagram metadata fits reserved Buffer headroom"),
                );
                let record_start = if record_count > 1 {
                    record_starts[record]
                } else {
                    0
                };
                assert_eq!(
                    fifo.peek(record_start, record_header_len, header),
                    record_header_len,
                    "complete datagram metadata remains in the Session FIFO"
                );
                let prefix = SessionDatagramPrefix {
                    data_length: if record_count > 1 {
                        record_lengths[record] as u32
                    } else {
                        datagram_length as u32
                    },
                    data_offset: if record_count > 1 {
                        record_offsets[record] as u32
                    } else {
                        payload_offset as u32
                    },
                };
                header[..size_of::<SessionDatagramPrefix>()].copy_from_slice(prefix.as_bytes());
                buffer.advance(record_header_len as isize);
                payload_offset += packet_len;
            }
        }
        transport.push_header(runtime, self, target, &heads, self.tx_context.max_dequeue);
        let fifo = self
            .sessions
            .get(session_index)
            .expect("sent Session retains its TX FIFO")
            .tx_fifo()
            .expect("sent Session retains its TX FIFO");
        let notify_dequeue = if peek_data {
            false
        } else if DGRAM {
            if record_count > 1 {
                let complete = send_len / mss;
                let partial = send_len % mss;
                let completed_bytes = record_lengths[..complete]
                    .iter()
                    .fold(0usize, |total, length| total + record_header_len + length);
                if completed_bytes != 0 {
                    assert_eq!(
                        fifo.drop_dequeue(completed_bytes),
                        completed_bytes,
                        "complete datagram records leave the Session FIFO"
                    );
                }
                if partial != 0 {
                    let prefix = SessionDatagramPrefix {
                        data_length: record_lengths[complete] as u32,
                        data_offset: (record_offsets[complete] + partial) as u32,
                    };
                    assert_eq!(
                        fifo.overwrite_head(prefix.as_bytes()),
                        prefix.as_bytes().len(),
                        "partial datagram retains its updated offset"
                    );
                }
            } else {
                let new_offset = datagram_offset + send_len;
                if new_offset == datagram_length {
                    let record_len = record_header_len + datagram_length;
                    assert_eq!(
                        fifo.drop_dequeue(record_len),
                        record_len,
                        "complete datagram record leaves the Session FIFO"
                    );
                } else {
                    let prefix = SessionDatagramPrefix {
                        data_length: datagram_length as u32,
                        data_offset: new_offset as u32,
                    };
                    assert_eq!(
                        fifo.overwrite_head(prefix.as_bytes()),
                        prefix.as_bytes().len(),
                        "partial datagram retains its updated offset"
                    );
                }
            }
            fifo.needs_deq_notification(send_len + segments * record_header_len)
        } else {
            assert_eq!(
                fifo.drop_dequeue(send_len),
                send_len,
                "dequeue TX consumes the packetized FIFO bytes"
            );
            fifo.needs_deq_notification(send_len)
        };
        *packets += segments;
        if send_len == available {
            fifo.unset_event();
            let recheck_offset = if DGRAM { 0 } else { tx_offset };
            let rearmed = fifo.max_dequeue() > recheck_offset && fifo.set_event();
            if rearmed {
                self.old_events.push_front(event_index);
            } else {
                transport.deschedule(runtime, target);
                self.event_elements.remove(event_index);
            }
        } else {
            self.old_events.push_back(event_index);
        }
        if notify_dequeue {
            self.notify_tx_dequeue(
                runtime,
                self.tx_context
                    .session
                    .expect("TX context retains its Session"),
            );
        }
        for &head in &heads {
            self.add_pending_tx_buffer(runtime, head, next);
        }
        self.tx_context.transport_pending_buffers = heads;
        SessionTxOutcome::Ok
    }

    pub fn allocate_control_event(&mut self, event: SessionEvent) -> u32 {
        let index = self.event_elements.insert(SessionEventElement {
            event,
            next: SESSION_INDEX_INVALID,
            previous: SESSION_INDEX_INVALID,
        });
        self.control_events.push_back(index);
        index
    }

    /// VPP: `session_program_transport_ctrl_evt`, session.c:223-245.
    pub(crate) fn program_close(&mut self, runtime: &DataPlaneMain, handle: SessionHandle) {
        self.allocate_control_event(SessionEvent::from((SessionEventType::Close, handle)));
        if self.state() == SessionWorkerState::Interrupt {
            runtime
                .set_node_interrupt_pending(
                    self.queue_node
                        .expect("session-queue installs before Session close"),
                )
                .expect("registered session-queue accepts an interrupt");
        }
    }

    pub fn allocate_new_event(&mut self, event: SessionEvent) -> u32 {
        let index = self.event_elements.insert(SessionEventElement {
            event,
            next: SESSION_INDEX_INVALID,
            previous: SESSION_INDEX_INVALID,
        });
        self.new_events.push_back(index);
        index
    }

    pub fn allocate_old_event(&mut self, event: SessionEvent) -> u32 {
        let index = self.event_elements.insert(SessionEventElement {
            event,
            next: SESSION_INDEX_INVALID,
            previous: SESSION_INDEX_INVALID,
        });
        self.old_events.push_back(index);
        index
    }

    #[inline(always)]
    pub fn session_from_handle(&self, handle: SessionHandle) -> Option<&Session> {
        if handle.worker_index != self.worker_index {
            return None;
        }
        self.session(handle.session_index)
    }

    #[inline(always)]
    pub fn store_state(
        &self,
        handle: SessionHandle,
        state: SessionState,
    ) -> Result<(), SessionError> {
        let Some(session) = self.session_from_handle(handle) else {
            return Err(SessionError::NoSession);
        };
        session.store_state(state);
        Ok(())
    }

    pub fn enqueue_ready(
        &mut self,
        handle: SessionHandle,
        protocol: u8,
    ) -> Result<(), SessionError> {
        let Some(session) = self.session_from_handle(handle) else {
            return Err(SessionError::NoSession);
        };
        if session.transport_protocol != protocol {
            return Err(SessionError::Invalid);
        }
        self.allocate_new_event(SessionEvent::from((
            SessionEventType::Tx,
            handle.session_index,
        )));
        Ok(())
    }

    /// VPP: session.c:172-202, session_add_self_custom_tx_evt.
    pub fn add_self_custom_tx_event(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        priority: bool,
        descheduled: bool,
    ) {
        assert_eq!(handle.worker_index, self.worker_index);
        let should_enqueue = {
            let session = self
                .sessions
                .get_mut(handle.session_index)
                .expect("custom TX retains its owner-worker Session");
            assert_ne!(session.load_state(), SessionState::TransportDeleted);
            if session.flags.custom_tx {
                return;
            }
            session.flags.custom_tx = true;
            session
                .tx_fifo()
                .expect("custom TX retains a TX FIFO")
                .set_event()
                || descheduled
        };
        if !should_enqueue {
            return;
        }
        let event = SessionEvent::from((SessionEventType::Tx, handle.session_index));
        if priority {
            self.allocate_new_event(event);
        } else {
            self.allocate_old_event(event);
        }
        if self.state() == SessionWorkerState::Interrupt {
            runtime
                .set_node_interrupt_pending(
                    self.queue_node
                        .expect("session-queue installs before custom TX"),
                )
                .expect("registered session-queue accepts an interrupt");
        }
    }

    /// VPP: session.c:204-221, sesssion_reschedule_tx.
    pub fn reschedule_tx(&mut self, runtime: &DataPlaneMain, handle: SessionHandle) {
        assert_eq!(handle.worker_index, self.worker_index);
        assert!(self.sessions.get(handle.session_index).is_some());
        self.allocate_new_event(SessionEvent::from((
            SessionEventType::Tx,
            handle.session_index,
        )));
        if self.state() == SessionWorkerState::Interrupt {
            runtime
                .set_node_interrupt_pending(
                    self.queue_node
                        .expect("session-queue installs before reschedule"),
                )
                .expect("registered session-queue accepts an interrupt");
        }
    }

    /// VPP session.c:738-749. ACKed bytes leave Session ownership once per
    /// input burst; notification follows the FIFO's dequeue request bit.
    pub fn tx_fifo_dequeue_drop(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        max_bytes: u32,
    ) -> u32 {
        let session = self
            .session_from_handle(handle)
            .expect("ACK retains its owner-worker Session");
        let fifo = session
            .tx_fifo()
            .expect("TCP data Session retains its TX FIFO");
        let dropped = fifo.drop_dequeue(max_bytes as usize) as u32;
        if fifo.needs_deq_notification(max_bytes as usize) {
            self.notify_tx_dequeue(runtime, handle);
        }
        dropped
    }

    /// VPP: session.c:657-680, `session_dequeue_notify`.
    fn notify_tx_dequeue(&mut self, runtime: &DataPlaneMain, handle: SessionHandle) {
        assert_eq!(handle.worker_index, self.worker_index);
        let session = self
            .sessions
            .get(handle.session_index)
            .expect("dequeue notification retains its Session");
        let Some(index) = session.application_worker() else {
            return;
        };
        let fifo = session
            .tx_fifo()
            .expect("attached Session retains its TX FIFO");
        let applications =
            ApplicationMain::global().expect("attached Session retains Application Main");
        // SAFETY: this Session worker exclusively writes its AppWorker event
        // slot; detach synchronizes before removing the worker.
        let app_worker = unsafe { applications.worker(index) }
            .expect("attached Session retains Application Worker");
        self.program_io_event(
            app_worker,
            session,
            SessionEventType::Tx,
            matches!(
                session.load_state(),
                SessionState::Listening | SessionState::Opened
            ),
        );
        self.app_workers_pending.set(index as usize);
        let application = unsafe { applications.application(app_worker.application()) }
            .expect("attached AppWorker retains its Application");
        for &subscriber in fifo.subscribers() {
            let Some(worker_index) = application.worker(u32::from(subscriber)) else {
                continue;
            };
            let Some(worker) = (unsafe { applications.worker(worker_index) }) else {
                continue;
            };
            self.program_io_event(worker, session, SessionEventType::Tx, false);
            self.app_workers_pending.set(worker_index as usize);
        }
        self.schedule_pending_app_events(runtime);
    }

    /// VPP: `session_enqueue_notify`, session.c:626-648. RX coalescing is
    /// performed by the caller; this operation only queues the app event.
    pub fn enqueue_notify(&mut self, runtime: &DataPlaneMain, handle: SessionHandle) -> Option<()> {
        self.queue_rx_notification(handle, false)?;
        self.schedule_pending_app_events(runtime);
        Some(())
    }

    /// VPP: `session_program_rx_io_evt`, session.c:115-131. The same-worker
    /// branch is consumed by session-input after its current callback returns.
    pub fn program_rx_io_event(
        &mut self,
        handle: SessionHandle,
    ) -> Result<Option<SessionEventEnqueue>, SessionError> {
        if handle.worker_index != self.worker_index {
            return SessionMain::global()?
                .enqueue_event(
                    handle.worker_index,
                    SessionQueueEvent::Io {
                        event_type: SessionEventType::BuiltinRx,
                        session_index: handle.session_index,
                    },
                )
                .map(Some);
        }
        let session = self
            .session_from_handle(handle)
            .ok_or(SessionError::NoSession)?;
        if u8::from(session.load_state()) >= u8::from(SessionState::TransportClosing) {
            return Ok(None);
        }
        self.queue_rx_notification(handle, false);
        Ok(None)
    }

    /// VPP: `session_enqueue_notify_inline`, session.c:626-648.
    #[inline(always)]
    fn queue_rx_notification(
        &mut self,
        handle: SessionHandle,
        is_connectionless: bool,
    ) -> Option<u32> {
        if handle.worker_index != self.worker_index {
            return None;
        }
        let session = self.sessions.get(handle.session_index)?;
        let app_worker_index = session.application_worker()?;
        let application = ApplicationMain::global()
            .expect("Application Main initializes before Session RX notification");
        // SAFETY: this Session worker exclusively executes its AppWorker
        // event slot; detach waits for WorkerBarrier before removing it.
        let app_worker = unsafe { application.worker(app_worker_index) }
            .expect("an attached Session retains its AppWorker");
        self.program_io_event(app_worker, session, SessionEventType::Rx, is_connectionless);
        self.app_workers_pending.set(app_worker_index as usize);
        let application_record = unsafe { application.application(app_worker.application()) }
            .expect("attached AppWorker retains its Application");
        for &subscriber in session
            .rx_fifo()
            .expect("attached Session retains its RX FIFO")
            .subscribers()
        {
            let Some(worker_index) = application_record.worker(u32::from(subscriber)) else {
                continue;
            };
            let Some(worker) = (unsafe { application.worker(worker_index) }) else {
                continue;
            };
            self.program_io_event(worker, session, SessionEventType::Rx, false);
            self.app_workers_pending.set(worker_index as usize);
        }
        Some(app_worker_index)
    }

    /// VPP: `session_main_flush_enqueue_events`, session.c:689-714.
    pub fn flush_enqueue_events(&mut self, runtime: &DataPlaneMain, protocol: u8) {
        let pending = std::mem::take(&mut self.sessions_to_enqueue);
        for handle in pending {
            let Some(session) = self.session_from_handle(handle) else {
                continue;
            };
            if session.transport_protocol() == protocol {
                let is_connectionless = session.flags.connectionless;
                self.queue_rx_notification(handle, is_connectionless);
                self.schedule_pending_app_events(runtime);
            } else {
                self.sessions_to_enqueue.push(handle);
            }
        }
    }

    /// VPP: `session_program_io_event`, session.c:578-599.
    #[inline(always)]
    fn program_io_event(
        &self,
        app_worker: &super::app::AppWorker<'_>,
        session: &Session,
        event: SessionEventType,
        is_connectionless: bool,
    ) {
        if is_connectionless {
            let event = match event {
                SessionEventType::Rx => SessionEventType::BuiltinRx,
                SessionEventType::Tx => SessionEventType::TxMain,
                _ => panic!("connectionless notification must be RX or TX"),
            };
            let notification = SessionEvent::from((event, session.handle()));
            app_worker.add_event_custom(self.worker_index, &notification);
        } else {
            app_worker.add_event(session, event);
        }
    }

    /// VPP: `session_transport_closing_notify`, session.c:958-980.
    pub fn transport_closing(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        let session = self
            .session_from_handle(handle)
            .ok_or(SessionError::NoSession)?;
        assert_eq!(session.connection_index(), connection_index);
        let state = session.load_state();
        if matches!(
            state,
            SessionState::TransportClosing
                | SessionState::Closing
                | SessionState::AppClosed
                | SessionState::TransportClosed
                | SessionState::Closed
                | SessionState::TransportDeleted
        ) {
            return Ok(());
        }
        session.store_state(SessionState::TransportClosing);
        if state != SessionState::Accepting {
            self.queue_application_event(runtime, handle, SessionEventType::Disconnected);
        }
        Ok(())
    }

    /// VPP: `session_transport_reset_notify`, session.c:1167-1185.
    pub fn transport_reset(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        let session = self
            .session_from_handle(handle)
            .ok_or(SessionError::NoSession)?;
        assert_eq!(session.connection_index(), connection_index);
        let state = session.load_state();
        if matches!(
            state,
            SessionState::TransportClosing
                | SessionState::Closing
                | SessionState::AppClosed
                | SessionState::TransportClosed
                | SessionState::Closed
                | SessionState::TransportDeleted
        ) {
            return Ok(());
        }
        session.store_state(SessionState::TransportClosing);
        if state != SessionState::Accepting {
            self.queue_application_event(runtime, handle, SessionEventType::Reset);
        }
        Ok(())
    }

    /// VPP: `session_transport_closed_notify`, session.c:1126-1164.
    pub fn transport_closed(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        let session = self
            .session_from_handle(handle)
            .ok_or(SessionError::NoSession)?;
        assert_eq!(session.connection_index(), connection_index);
        let state = session.load_state();
        if matches!(
            state,
            SessionState::TransportClosed | SessionState::Closed | SessionState::TransportDeleted
        ) {
            return Ok(());
        }
        if state == SessionState::Ready {
            self.transport_closing(runtime, handle, connection_index)?;
        }
        let session = self
            .session_from_handle(handle)
            .expect("transport-closed Session remains allocated");
        if state == SessionState::AppClosed {
            session.store_state(SessionState::Closed);
        } else if matches!(
            state,
            SessionState::Created
                | SessionState::Listening
                | SessionState::Connecting
                | SessionState::Accepting
                | SessionState::Ready
                | SessionState::Opened
                | SessionState::TransportClosing
                | SessionState::Closing
        ) {
            session.store_state(SessionState::TransportClosed);
        }
        self.queue_application_event(runtime, handle, SessionEventType::TransportClosed);
        Ok(())
    }

    /// VPP: `session_transport_delete_request`, session.c:1064-1121. The
    /// plugin removes its concrete lookup entry before this generic step.
    pub fn transport_delete_request(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        connection_index: u32,
    ) -> Result<bool, SessionError> {
        let session = self
            .session_from_handle(handle)
            .ok_or(SessionError::NoSession)?;
        assert_eq!(session.connection_index(), connection_index);
        let state = session.load_state();
        if state == SessionState::TransportDeleted {
            return Ok(true);
        }
        session.store_state(SessionState::TransportDeleted);
        let attached = self.queue_cleanup_event(runtime, handle, SessionCleanup::Transport);
        if attached {
            if state == SessionState::AppClosed {
                self.program_close(runtime, handle);
            } else if state == SessionState::Closed {
                self.queue_cleanup_event(runtime, handle, SessionCleanup::Session);
            }
        } else {
            self.cleanup(handle)
                .expect("unattached Session remains allocated until transport deletion");
        }
        Ok(attached)
    }

    /// VPP: `session_input.c:277-296`. The application has observed transport
    /// cleanup before its protocol-specific cleanup runs on this worker.
    pub(crate) fn finish_transport_cleanup(
        &mut self,
        runtime: &mut DataPlaneMain,
        handle: SessionHandle,
    ) -> Result<(), SessionError> {
        let session = self
            .session_from_handle(handle)
            .ok_or(SessionError::NoSession)?;
        let protocol = session.transport_protocol();
        let dispatch = SessionMain::global()?
            .transport_control(protocol)
            .ok_or(SessionError::TransportNotRegistered)?;
        dispatch(
            self,
            runtime,
            handle.session_index,
            SessionEventType::Cleanup,
        )?;
        self.session_mut(handle.session_index)
            .expect("transport cleanup retains its Session")
            .detach_transport();
        Ok(())
    }

    fn queue_cleanup_event(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        cleanup: SessionCleanup,
    ) -> bool {
        let session = self
            .session_from_handle(handle)
            .expect("cleanup retains its Session");
        let Some(app_worker_index) = session.application_worker() else {
            return false;
        };
        let application = ApplicationMain::global()
            .expect("Application Main remains initialized while Session is attached");
        // SAFETY: this Session worker exclusively executes its AppWorker event slot.
        let app_worker = unsafe { application.worker(app_worker_index) }
            .expect("attached Session retains its AppWorker");
        app_worker.add_event_custom(
            handle.worker_index,
            &SessionEvent {
                event_type: SessionEventType::Cleanup.into(),
                postponed: 0,
                session_index: handle.session_index,
                worker_index: cleanup as u32,
                rpc_sequence: 0,
            },
        );
        self.program_app_worker(runtime, app_worker_index);
        true
    }

    fn queue_application_event(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        event_type: SessionEventType,
    ) {
        let session = self
            .session_from_handle(handle)
            .expect("Application notification retains its Session");
        let Some(app_worker_index) = session.application_worker() else {
            return;
        };
        let application = ApplicationMain::global()
            .expect("Application Main remains initialized while Session is attached");
        // SAFETY: this Session worker exclusively executes its AppWorker event
        // slot; detach waits for WorkerBarrier before removing the AppWorker.
        let app_worker = unsafe { application.worker(app_worker_index) }
            .expect("attached Session retains its AppWorker");
        app_worker.add_event(session, event_type);
        self.program_app_worker(runtime, app_worker_index);
    }

    pub fn allocate_control_data(&mut self, data: SessionControlData) -> u32 {
        self.control_event_data.insert(data)
    }

    pub fn release_control_data(&mut self, index: u32) -> Result<(), SessionError> {
        if self.control_event_data.remove(index).is_some() {
            Ok(())
        } else {
            Err(SessionError::Invalid)
        }
    }

    pub fn queue_migration(
        &mut self,
        request: SessionMigrationRequest,
    ) -> Result<(), SessionError> {
        self.migration.lock().requests.push(request);
        Ok(())
    }

    pub fn handle_migrations(&mut self) -> Result<usize, SessionError> {
        let mut migration = self.migration.lock();
        let requests = std::mem::take(&mut migration.requests);
        migration.handling.extend(requests);
        Ok(migration.handling.len())
    }
}

/// VPP: session.c:2059-2061,2209-2241. Runtime invokes this worker-init
/// hook once after cloning the graph and before its Data Worker loop starts.
#[hammer_component_macros::worker_init_function(name = "session_worker_init")]
fn init_session_worker(runtime: &mut DataPlaneMain) -> RuntimeResult<()> {
    let main =
        SessionMain::global().expect("Session Main initializes before Session worker graph setup");
    let worker = unsafe { main.worker_mut(runtime) }
        .expect("Session worker graph setup runs on a configured Data Worker");
    let input = runtime
        .node_by_name("session-input")
        .ok_or(SessionQueueError::NodeMissing)?;
    let queue = runtime
        .node_by_name("session-queue")
        .ok_or(SessionQueueError::NodeMissing)?;
    worker.install_input_node(input);
    worker.install_queue_node(queue);

    let published = main
        .queue_node
        .compare_exchange(u32::MAX, queue.slot(), Ordering::AcqRel, Ordering::Acquire)
        .unwrap_or_else(|published| published);
    assert!(
        published == u32::MAX || published == queue.slot(),
        "Session queue NodeId is identical in every worker graph"
    );
    if !main.is_enabled() {
        return Ok(());
    }
    if main.config.use_private_rx_mqs && !main.config.no_adaptive {
        worker.enable_adaptive_mode(runtime)?;
    }
    runtime
        .nodes()
        .set_node_state(input, hammer_core::data_plane::NodeState::Interrupt)
        .expect("installed session-input accepts Interrupt state");
    runtime
        .nodes()
        .set_node_state(queue, hammer_core::data_plane::NodeState::Polling)
        .expect("installed session-queue accepts Polling state");
    Ok(())
}

pub struct Session {
    handle: SessionHandle,
    state: AtomicU8,
    session_type: u8,
    transport_protocol: u8,
    flags: SessionFlags,
    rx_fifo: Option<Rc<SvmFifo>>,
    tx_fifo: Option<Rc<SvmFifo>>,
    application_worker: u32,
    connection_index: u32,
    application_listener: u32,
    listener_handle: SessionHandle,
    half_open_index: u32,
    opaque: u32,
}

impl Session {
    fn new(
        handle: SessionHandle,
        state: SessionState,
        session_type: u8,
        transport_protocol: u8,
        opaque: u32,
    ) -> Self {
        Self {
            handle,
            state: AtomicU8::new(u8::from(state)),
            session_type,
            transport_protocol,
            flags: SessionFlags::default(),
            rx_fifo: None,
            tx_fifo: None,
            application_worker: SESSION_INDEX_INVALID,
            connection_index: SESSION_INDEX_INVALID,
            application_listener: SESSION_INDEX_INVALID,
            listener_handle: SessionHandle::invalid(),
            half_open_index: SESSION_INDEX_INVALID,
            opaque,
        }
    }

    #[inline(always)]
    pub fn store_state(&self, state: SessionState) {
        self.state.store(u8::from(state), Ordering::Release);
    }

    #[inline(always)]
    pub fn load_state(&self) -> SessionState {
        SessionState::from(self.state.load(Ordering::Acquire))
    }

    /// VPP: `session_close`, session.c:1540-1574. App close intent is
    /// independent of transport-initiated Session state transitions.
    pub fn close(&mut self) {
        if self.flags.app_closed {
            return;
        }
        self.flags.app_closed = true;
        self.flags.close_pending = true;
        if u8::from(self.load_state()) < u8::from(SessionState::Closing) {
            if let Some(fifo) = self.tx_fifo() {
                fifo.clear_deq_notification();
            }
            self.store_state(SessionState::Closing);
        }
    }

    #[inline(always)]
    pub(crate) fn take_close_request(&mut self) -> bool {
        std::mem::take(&mut self.flags.close_pending)
    }

    #[inline(always)]
    pub(crate) const fn app_closed(&self) -> bool {
        self.flags.app_closed
    }

    #[inline(always)]
    pub fn opaque(&self) -> &u32 {
        &self.opaque
    }

    #[inline(always)]
    pub fn opaque_mut(&mut self) -> &mut u32 {
        &mut self.opaque
    }

    // VPP: session_t.app_wrk_index, session_types.h:264-271.
    #[inline(always)]
    pub const fn app_worker_index(&self) -> Option<u32> {
        self.application_worker()
    }

    // VPP: app_worker_init_accepted/app_worker_init_connected,
    // application_worker.c:493-631. Only the owning worker mutates a Session.
    pub fn attach_app_worker(&mut self, app_worker_index: u32) {
        self.application_worker = app_worker_index;
    }

    // VPP: segment_manager_del_sessions_filter, segment_manager.c:716-743.
    pub fn detach_app_worker(&mut self) {
        self.application_worker = SESSION_INDEX_INVALID;
    }

    #[inline(always)]
    pub fn rx_fifo(&self) -> Option<&SvmFifo> {
        self.rx_fifo.as_deref()
    }

    #[inline(always)]
    pub fn tx_fifo(&self) -> Option<&SvmFifo> {
        self.tx_fifo.as_deref()
    }

    #[inline(always)]
    pub const fn handle(&self) -> SessionHandle {
        self.handle
    }

    #[inline(always)]
    pub const fn transport_protocol(&self) -> u8 {
        self.transport_protocol
    }

    #[inline(always)]
    pub const fn session_type(&self) -> u8 {
        self.session_type
    }

    #[inline(always)]
    pub const fn connection_index(&self) -> u32 {
        self.connection_index
    }

    #[inline(always)]
    pub(crate) fn detach_transport(&mut self) {
        self.connection_index = SESSION_INDEX_INVALID;
        self.store_state(SessionState::TransportDeleted);
    }

    #[inline(always)]
    pub const fn application_worker(&self) -> Option<u32> {
        if self.application_worker == SESSION_INDEX_INVALID {
            None
        } else {
            Some(self.application_worker)
        }
    }

    #[inline(always)]
    pub const fn application_listener(&self) -> Option<u32> {
        if self.application_listener == SESSION_INDEX_INVALID {
            None
        } else {
            Some(self.application_listener)
        }
    }

    /// VPP: `session_t.listener_handle`, session_types.h:258-275.
    #[inline(always)]
    pub const fn listener_handle(&self) -> SessionHandle {
        self.listener_handle
    }

    #[inline(always)]
    pub(crate) const fn rx_ready(&self) -> bool {
        self.flags.rx_ready
    }

    #[inline(always)]
    pub(crate) fn clear_rx_event(&mut self) {
        self.flags.rx_event = false;
    }

    #[inline(always)]
    pub(crate) fn set_rx_ready(&mut self, ready: bool) {
        self.flags.rx_ready = ready;
    }

    #[inline(always)]
    pub(crate) fn detach_application(&mut self) {
        self.flags.rx_ready = false;
        self.application_worker = SESSION_INDEX_INVALID;
    }
}

/// Application-side fields shared by builtin protocols. The FIFO storage is
/// owned by the server Session; the event queue belongs to its SessionWorker.
/// VPP: application_interface.h:275-290, app_session_t.
pub struct AppSession<T> {
    rx_fifo: Rc<SvmFifo>,
    tx_fifo: Rc<SvmFifo>,
    session_handle: SessionHandle,
    session_type: u8,
    state: AtomicU8,
    pub transport: Option<T>,
    event_queue: Arc<SvmMsgQ>,
    is_dgram: bool,
}

impl<T> AppSession<T> {
    /// VPP: vperf_test.h:215-229, vperf_app_session_init_.
    /// The owner must drop this record before the Session releases its FIFOs.
    #[inline]
    pub fn new(session: &Session) -> Self {
        let handle = session.handle();
        let event_queue = SessionMain::global()
            .expect("Session Main exists while an app Session is accepted")
            .worker_mq_segment
            .mqs
            .get(handle.worker_index as usize)
            .expect("accepted Session has a worker event queue");
        Self {
            rx_fifo: Rc::clone(
                session
                    .rx_fifo
                    .as_ref()
                    .expect("accepted Session has an RX FIFO"),
            ),
            tx_fifo: Rc::clone(
                session
                    .tx_fifo
                    .as_ref()
                    .expect("accepted Session has a TX FIFO"),
            ),
            session_handle: handle,
            session_type: session.session_type(),
            state: AtomicU8::new(SessionState::Created.into()),
            transport: None,
            event_queue: Arc::clone(event_queue),
            is_dgram: session.flags.connectionless,
        }
    }
}

impl<T> AppSession<T> {
    /// VPP: application_interface.h:275-290, app-side session_state.
    #[inline(always)]
    pub fn load_state(&self) -> SessionState {
        SessionState::from(self.state.load(Ordering::Acquire))
    }

    #[inline(always)]
    pub fn store_state(&self, state: SessionState) {
        self.state.store(state.into(), Ordering::Release);
    }

    /// VPP: application_interface.h:595-628, app_send_io_evt_to_vpp.
    #[inline]
    pub fn send_io_event(
        &self,
        event: SessionEventType,
        noblock: bool,
    ) -> Result<SessionEventEnqueue, SessionError> {
        assert!(matches!(
            event,
            SessionEventType::Rx | SessionEventType::Tx | SessionEventType::TxFlush
        ));
        let wait = if noblock {
            SvmQueueConditionalWait::Nowait
        } else {
            SvmQueueConditionalWait::Wait
        };
        let mut producer = match self.event_queue.producer(wait) {
            Ok(producer) => producer,
            Err(SvmMsgQError::LockBusy) => return Ok(SessionEventEnqueue::Busy),
            Err(source) => return Err(SessionError::MessageQueueAllocation { source }),
        };
        let descriptor = match producer.alloc_msg_on_ring(0) {
            Ok(descriptor) => descriptor,
            Err(SvmMsgQError::QueueFull | SvmMsgQError::RingFull { .. }) => {
                return Ok(SessionEventEnqueue::Full);
            }
            Err(source) => return Err(SessionError::MessageQueueAllocation { source }),
        };
        {
            // SAFETY: this producer just allocated the descriptor, which is
            // not visible to the Session worker before producer.add.
            let bytes = unsafe { producer.message_bytes_mut(descriptor) }
                .expect("allocated app IO event retains its ring slot");
            let record = SessionEvent::mut_from_bytes(bytes)
                .expect("Session worker IO ring stores one SessionEvent");
            record.event_type = event.into();
            record.postponed = 0;
            record.session_index = self.session_handle.session_index;
            record.worker_index = 0;
            record.rpc_sequence = 0;
        }
        match producer.add(descriptor) {
            Ok(())
            | Err(
                SvmMsgQError::SignalAfterCommit { .. }
                | SvmMsgQError::EventSignalAfterCommit { .. },
            ) => Ok(SessionEventEnqueue::Enqueued),
            Err(source) => panic!("locked Session worker MQ rejected allocated event: {source}"),
        }
    }

    /// VPP: application_interface.h:724-743, app_send_stream.
    #[inline(always)]
    pub fn send_stream(&self, data: &[u8], noblock: bool) -> Result<usize, SessionError> {
        assert!(!self.is_dgram, "stream send requires a stream Session");
        let sent = self.tx_fifo.enqueue(data);
        if sent != 0 && self.tx_fifo.set_event() {
            let status = self.send_io_event(SessionEventType::Tx, noblock)?;
            assert!(
                status == SessionEventEnqueue::Enqueued || noblock,
                "blocking app stream send must publish its TX event"
            );
        }
        Ok(sent)
    }

    /// VPP: application_interface.h:795-810, app_recv_stream.
    #[inline(always)]
    pub fn recv_stream(&self, out: &mut [u8]) -> usize {
        assert!(!self.is_dgram, "stream receive requires a stream Session");
        self.rx_fifo.unset_event();
        self.rx_fifo.dequeue(out.len(), out)
    }
}

/// VPP: session_types.h:496-500, session_dgram_pre_hdr_t.
#[repr(C)]
#[derive(KnownLayout, FromBytes, Immutable, IntoBytes)]
struct SessionDatagramPrefix {
    data_length: u32,
    data_offset: u32,
}

const _: () = assert!(size_of::<SessionDatagramPrefix>() == 8);

impl<T: Default + FromBytes + IntoBytes + Immutable> AppSession<T> {
    /// VPP: application_interface.h:682-691, app_send_dgram_raw_gso.
    #[inline(always)]
    pub fn send_dgram_raw_gso(
        &self,
        data: &[u8],
        gso_size: u16,
        event: SessionEventType,
        do_event: bool,
        noblock: bool,
    ) -> Result<Option<usize>, SessionError> {
        let header_len = size_of::<SessionDatagramPrefix>() + size_of::<T>() + size_of::<u16>();
        let segment_len = header_len
            .checked_add(data.len())
            .ok_or(SessionError::Invalid)?;
        self.send_dgram_segments_gso(&[data], segment_len, gso_size, event, do_event, noblock)
    }

    /// VPP: application_interface.h:654-679, app_send_dgram_segs_raw.
    #[inline(always)]
    pub fn send_dgram_segments_raw(
        &self,
        segments: &[&[u8]],
        segment_len: usize,
        event: SessionEventType,
        do_event: bool,
        noblock: bool,
    ) -> Result<Option<usize>, SessionError> {
        self.send_dgram_segments_gso(segments, segment_len, 0, event, do_event, noblock)
    }

    #[inline(always)]
    fn send_dgram_segments_gso(
        &self,
        segments: &[&[u8]],
        segment_len: usize,
        gso_size: u16,
        event: SessionEventType,
        do_event: bool,
        noblock: bool,
    ) -> Result<Option<usize>, SessionError> {
        assert!(self.is_dgram, "datagram send requires a datagram Session");
        let payload_len = segments.iter().try_fold(0usize, |length, segment| {
            length
                .checked_add(segment.len())
                .ok_or(SessionError::Invalid)
        })?;
        let header_len = size_of::<SessionDatagramPrefix>() + size_of::<T>() + size_of::<u16>();
        if segment_len
            != header_len
                .checked_add(payload_len)
                .ok_or(SessionError::Invalid)?
        {
            return Err(SessionError::Invalid);
        }
        let prefix = SessionDatagramPrefix {
            data_length: u32::try_from(payload_len).map_err(|_| SessionError::Invalid)?,
            data_offset: 0,
        };
        if self.tx_fifo.max_enqueue() < segment_len {
            return Ok(None);
        }
        let gso_bytes = gso_size.to_ne_bytes();
        let bytes = std::iter::once(prefix.as_bytes())
            .chain(std::iter::once(
                self.transport
                    .as_ref()
                    .expect("datagram AppSession has transport")
                    .as_bytes(),
            ))
            .chain(std::iter::once(gso_bytes.as_slice()))
            .chain(segments.iter().copied());
        let sent = self
            .tx_fifo
            .enqueue_segments(segment_len, bytes)
            .map_err(|source| SessionError::DatagramFifo {
                session_id: self.session_handle.session_index,
                source,
            })?;
        assert_eq!(sent, segment_len, "datagram FIFO commits a complete record");
        if do_event && self.tx_fifo.set_event() {
            let status = self.send_io_event(event, noblock)?;
            assert!(
                status == SessionEventEnqueue::Enqueued || noblock,
                "blocking datagram send must publish its TX event"
            );
        }
        Ok(Some(payload_len))
    }

    /// VPP: application_interface.h:754-785, app_recv_dgram_raw.
    #[inline(always)]
    pub fn recv_dgram_raw(
        &mut self,
        out: &mut [u8],
        clear_event: bool,
        peek: bool,
    ) -> Result<Option<usize>, SessionError> {
        assert!(
            self.is_dgram,
            "datagram receive requires a datagram Session"
        );
        if clear_event {
            self.rx_fifo.unset_event();
        }
        let header_len = size_of::<SessionDatagramPrefix>() + size_of::<T>() + size_of::<u16>();
        if self.rx_fifo.max_dequeue() < header_len {
            return Ok(None);
        }
        let mut prefix_bytes = [0u8; size_of::<SessionDatagramPrefix>()];
        assert_eq!(
            self.rx_fifo.peek(0, prefix_bytes.len(), &mut prefix_bytes),
            prefix_bytes.len(),
            "complete datagram prefix remains readable"
        );
        let prefix = SessionDatagramPrefix::read_from_bytes(&prefix_bytes)
            .expect("datagram prefix has its declared layout");
        assert!(
            prefix.data_length >= prefix.data_offset,
            "datagram offset stays within the payload"
        );
        let record_len = header_len
            .checked_add(prefix.data_length as usize)
            .expect("datagram record length fits usize");
        if self.rx_fifo.max_dequeue() < record_len {
            return Ok(None);
        }
        let mut transport = T::default();
        let transport_bytes = transport.as_mut_bytes();
        assert_eq!(
            self.rx_fifo.peek(
                size_of::<SessionDatagramPrefix>(),
                transport_bytes.len(),
                transport_bytes
            ),
            transport_bytes.len(),
            "complete datagram transport remains readable"
        );
        self.transport = Some(transport);
        let copied = out
            .len()
            .min((prefix.data_length - prefix.data_offset) as usize);
        assert_eq!(
            self.rx_fifo
                .peek(header_len + prefix.data_offset as usize, copied, out),
            copied,
            "complete datagram payload remains readable"
        );
        if !peek {
            assert_eq!(
                self.rx_fifo.drop_dequeue(record_len),
                record_len,
                "complete datagram is consumed as one record"
            );
        }
        Ok(Some(copied))
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        match (self.rx_fifo.as_ref(), self.tx_fifo.as_ref()) {
            (Some(rx), Some(tx)) => {
                assert_eq!(
                    Rc::strong_count(rx),
                    1,
                    "RX FIFO AppSession borrow is released before Session cleanup"
                );
                assert_eq!(
                    Rc::strong_count(tx),
                    1,
                    "TX FIFO AppSession borrow is released before Session cleanup"
                );
                let main = SegmentManagerMain::global()
                    .expect("Segment Manager Main remains initialized while Sessions exist");
                // SAFETY: the Session owner drops this pair on its worker;
                // detach retains the manager until all FIFO pairs return.
                let manager = unsafe { main.get(rx.segment_manager) }
                    .expect("Session FIFO Segment Manager remains allocated");
                if manager.release_session_fifos(rx, tx) {
                    SegmentManagerMain::remove_detached(rx.segment_manager);
                }
            }
            (None, None) => {}
            _ => panic!(
                "Session {:?} owns only one half of its FIFO pair",
                self.handle
            ),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Created,
    Listening,
    Connecting,
    Accepting,
    Ready,
    Opened,
    TransportClosing,
    Closing,
    AppClosed,
    TransportClosed,
    Closed,
    TransportDeleted,
}

impl From<SessionState> for u8 {
    #[inline(always)]
    fn from(state: SessionState) -> Self {
        state as u8
    }
}

impl From<u8> for SessionState {
    #[inline(always)]
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Created,
            1 => Self::Listening,
            2 => Self::Connecting,
            3 => Self::Accepting,
            4 => Self::Ready,
            5 => Self::Opened,
            6 => Self::TransportClosing,
            7 => Self::Closing,
            8 => Self::AppClosed,
            9 => Self::TransportClosed,
            10 => Self::Closed,
            11 => Self::TransportDeleted,
            _ => panic!("invalid session state {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionFlags {
    pub connectionless: bool,
    pub half_open: bool,
    pub migrating: bool,
    pub app_closed: bool,
    close_pending: bool,
    pub rx_event: bool,
    pub rx_ready: bool,
    pub tx_ready: bool,
    pub custom_tx: bool,
}

// VPP: session_types.h:476-492, session_event_t. The last 16 bytes are the
// event union: IO uses session_index, Session also uses worker_index, and
// RPC uses session_index as its pool index plus rpc_sequence.
#[repr(C, packed)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, KnownLayout, FromBytes, Immutable, IntoBytes)]
pub struct SessionEvent {
    pub event_type: u8,
    pub postponed: u8,
    pub session_index: u32,
    pub worker_index: u32,
    pub rpc_sequence: u64,
}

impl SessionEvent {
    #[inline(always)]
    pub(crate) fn session_index(&self) -> u32 {
        self.session_index
    }

    #[inline(always)]
    pub(crate) fn session_handle(&self) -> SessionHandle {
        SessionHandle {
            session_index: self.session_index,
            worker_index: self.worker_index,
        }
    }
}

impl From<(SessionEventType, u32)> for SessionEvent {
    #[inline(always)]
    fn from((event_type, session_index): (SessionEventType, u32)) -> Self {
        Self {
            event_type: event_type.into(),
            postponed: 0,
            session_index,
            worker_index: 0,
            rpc_sequence: 0,
        }
    }
}

impl From<(SessionEventType, SessionHandle)> for SessionEvent {
    #[inline(always)]
    fn from((event_type, handle): (SessionEventType, SessionHandle)) -> Self {
        Self {
            event_type: event_type.into(),
            postponed: 0,
            session_index: handle.session_index,
            worker_index: handle.worker_index,
            rpc_sequence: 0,
        }
    }
}

#[cfg(test)]
mod event_tests {
    use super::{Session, SessionEvent, SessionEventType, SessionHandle, SessionState};

    #[test]
    fn application_close_requests_one_transport_control_event() {
        let mut session = Session::new(
            SessionHandle {
                session_index: 7,
                worker_index: 1,
            },
            SessionState::Ready,
            0,
            0,
            0,
        );
        session.close();
        assert_eq!(session.load_state(), SessionState::Closing);
        assert!(session.app_closed());
        assert!(session.take_close_request());
        session.close();
        assert!(!session.take_close_request());
    }

    #[test]
    fn application_close_preserves_transport_deleted_state() {
        let mut session = Session::new(
            SessionHandle {
                session_index: 7,
                worker_index: 1,
            },
            SessionState::TransportDeleted,
            0,
            0,
            0,
        );
        session.close();
        assert_eq!(session.load_state(), SessionState::TransportDeleted);
        assert!(session.take_close_request());
    }

    #[test]
    fn session_event_matches_vpp_record_size() {
        assert_eq!(std::mem::size_of::<SessionEvent>(), 18);
    }

    #[test]
    fn io_event_stores_session_index() {
        let session_index = 0x1234_5678;
        let event = SessionEvent::from((SessionEventType::Rx, session_index));
        assert_eq!(event.session_index(), session_index);
        let worker_index = event.worker_index;
        let rpc_sequence = event.rpc_sequence;
        assert_eq!(worker_index, 0);
        assert_eq!(rpc_sequence, 0);
    }

    #[test]
    fn control_event_stores_session_index_before_worker_index() {
        let handle = SessionHandle {
            worker_index: 0x1122_3344,
            session_index: 0x5566_7788,
        };
        let event = SessionEvent::from((SessionEventType::Reset, handle));
        assert_eq!(event.session_handle(), handle);
        let rpc_sequence = event.rpc_sequence;
        assert_eq!(rpc_sequence, 0);
    }
}

/// VPP: `session_evt_type_t`, session_types.h:385-425. Discriminants are
/// preserved only at the SVM event boundary; Rust code selects enum variants.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionEventType {
    Rx,
    Tx,
    TxFlush,
    BuiltinRx,
    TxMain,
    Rpc,
    HalfClose,
    Close,
    Reset,
    Bound,
    UnlistenReply,
    Accepted,
    AcceptedReply,
    Connected,
    Disconnected,
    DisconnectedReply,
    ResetReply,
    RequestWorkerUpdate,
    WorkerUpdate,
    WorkerUpdateReply,
    Shutdown,
    Disconnect,
    Connect,
    ConnectUri,
    Listen,
    ListenUri,
    Unlisten,
    AppDetach,
    AppAddSegment,
    AppDelSegment,
    Migrated,
    Cleanup,
    AppWorkerRpc,
    TransportAttribute,
    TransportAttributeReply,
    TransportClosed,
    HalfCleanup,
    ConnectStream,
    Terminate,
}

impl TryFrom<u8> for SessionEventType {
    type Error = SessionError;

    #[inline]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        const EVENTS: &[SessionEventType] = &[
            SessionEventType::Rx,
            SessionEventType::Tx,
            SessionEventType::TxFlush,
            SessionEventType::BuiltinRx,
            SessionEventType::TxMain,
            SessionEventType::Rpc,
            SessionEventType::HalfClose,
            SessionEventType::Close,
            SessionEventType::Reset,
            SessionEventType::Bound,
            SessionEventType::UnlistenReply,
            SessionEventType::Accepted,
            SessionEventType::AcceptedReply,
            SessionEventType::Connected,
            SessionEventType::Disconnected,
            SessionEventType::DisconnectedReply,
            SessionEventType::ResetReply,
            SessionEventType::RequestWorkerUpdate,
            SessionEventType::WorkerUpdate,
            SessionEventType::WorkerUpdateReply,
            SessionEventType::Shutdown,
            SessionEventType::Disconnect,
            SessionEventType::Connect,
            SessionEventType::ConnectUri,
            SessionEventType::Listen,
            SessionEventType::ListenUri,
            SessionEventType::Unlisten,
            SessionEventType::AppDetach,
            SessionEventType::AppAddSegment,
            SessionEventType::AppDelSegment,
            SessionEventType::Migrated,
            SessionEventType::Cleanup,
            SessionEventType::AppWorkerRpc,
            SessionEventType::TransportAttribute,
            SessionEventType::TransportAttributeReply,
            SessionEventType::TransportClosed,
            SessionEventType::HalfCleanup,
            SessionEventType::ConnectStream,
            SessionEventType::Terminate,
        ];
        EVENTS
            .get(value as usize)
            .copied()
            .ok_or(SessionError::Invalid)
    }
}

impl From<SessionEventType> for u8 {
    #[inline(always)]
    fn from(value: SessionEventType) -> Self {
        value as Self
    }
}

/// VPP `session_send_evt_to_thread` enqueue outcomes. Busy and full mean that
/// no event was committed; they are normal retry results, not Session errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionEventEnqueue {
    Enqueued,
    Busy,
    Full,
}

pub struct SessionMain {
    started_at: Instant,
    config: SessionConfig,
    workers: Vec<UnsafeCell<SessionWorker>>,
    worker_states: Vec<AtomicU8>,
    queue_node: AtomicU32,
    listening_sessions: UnsafeCell<Pool<Session>>,
    session_tx: UnsafeCell<Vec<Option<SessionTxDispatch>>>,
    session_type_to_next: UnsafeCell<Vec<Option<super::node::SessionQueueNext>>>,
    transport_io: UnsafeCell<Vec<Option<SessionIoDispatch>>>,
    transport_control: UnsafeCell<Vec<Option<SessionControlDispatch>>>,
    transport_time: UnsafeCell<Vec<Option<SessionTimeDispatch>>>,
    transport_cl_thread: u32,
    pool_reallocation: SpinLock<PoolReallocationState>,
    rpc_requests: SpinLock<Pool<SessionRpc>>,
    rpc_sequence: AtomicU64,
    worker_mq_segment: SvmFifoSegment,
    last_transport_protocol: AtomicU8,
    is_enabled: AtomicBool,
    is_initialized: bool,
}

static SESSION_MAIN: OnceLock<SessionMain> = OnceLock::new();

impl SessionMain {
    /// VPP: `session_init`, session.c:2274-2294. Build every worker and MQ
    /// before publishing the process Session Main.
    pub fn init(
        config: SessionConfig,
        worker_mq_segment: SvmFifoSegment,
    ) -> Result<(), SessionError> {
        let main = Self::new(config, worker_mq_segment)?;
        assert!(
            SESSION_MAIN.set(main).is_ok(),
            "Session Main initializes once"
        );
        Ok(())
    }

    #[inline(always)]
    pub fn global() -> Result<&'static Self, SessionError> {
        SESSION_MAIN.get().ok_or(SessionError::Unknown)
    }

    #[inline(always)]
    pub(crate) fn queue_node(&self) -> NodeId {
        let slot = self.queue_node.load(Ordering::Acquire);
        assert_ne!(
            slot,
            u32::MAX,
            "Session queue NodeId installs before timer File dispatch"
        );
        NodeId::new(slot)
    }
}

// SAFETY: workers are constructed before publication and each mutable worker
// entry is owned by its runtime worker index. The separate listening pool is
// mutated only by Main Thread under WorkerBarrier; Data Workers may read its
// stable entries between barrier publications. SvmMsgQ synchronizes its own
// producer/consumer operations.
unsafe impl Send for SessionMain {}
unsafe impl Sync for SessionMain {}

impl SessionMain {
    fn new(config: SessionConfig, worker_mq_segment: SvmFifoSegment) -> Result<Self, SessionError> {
        let mut workers = Vec::with_capacity(config.worker_count as usize);
        for worker_index in 0..config.worker_count {
            workers.push(UnsafeCell::new(SessionWorker::new(worker_index, config)));
        }
        let mut main = Self {
            started_at: Instant::now(),
            config,
            workers,
            worker_states: (0..config.worker_count)
                .map(|_| AtomicU8::new(SessionWorkerState::Polling as u8))
                .collect(),
            queue_node: AtomicU32::new(u32::MAX),
            listening_sessions: UnsafeCell::new(Pool::new()),
            session_tx: UnsafeCell::new(Vec::new()),
            session_type_to_next: UnsafeCell::new(Vec::new()),
            transport_io: UnsafeCell::new(Vec::new()),
            transport_control: UnsafeCell::new(Vec::new()),
            transport_time: UnsafeCell::new(Vec::new()),
            transport_cl_thread: 0,
            pool_reallocation: SpinLock::new(PoolReallocationState {
                workers_at_barrier: 0,
                workers_doing_work: 0,
            }),
            rpc_requests: SpinLock::new(Pool::with_fixed_capacity(
                config
                    .worker_count
                    .saturating_mul(config.configured_worker_mq_length.max(2_048)),
            )),
            rpc_sequence: AtomicU64::new(0),
            worker_mq_segment,
            last_transport_protocol: AtomicU8::new(0),
            is_enabled: AtomicBool::new(config.session_enable_asap),
            is_initialized: true,
        };
        main.allocate_event_queues()?;
        Ok(main)
    }

    pub fn allocate_event_queues(&mut self) -> Result<(), SessionError> {
        let queue_length = self.config.configured_worker_mq_length.max(2_048);
        let rings = [
            SvmMsgQRingConfig::new(queue_length, size_of::<SessionEvent>() as u32),
            SvmMsgQRingConfig::new(queue_length >> 1, 256),
        ];
        let config = SvmMsgQConfig {
            consumer_pid: std::process::id() as i32,
            q_nitems: queue_length,
            rings: &rings,
        };
        for worker_index in self.worker_mq_segment.mqs.len() as u32..self.config.worker_count {
            if let Err(source) = self
                .worker_mq_segment
                .allocate_message_queue(worker_index, &config)
            {
                return Err(SessionError::SegmentCreate { source });
            }
        }
        Ok(())
    }

    #[inline(always)]
    pub fn worker(&mut self, worker_index: u32) -> Option<&SessionWorker> {
        self.workers
            .get_mut(worker_index as usize)
            .map(|slot| &*slot.get_mut())
    }

    /// VPP: `app_worker_del_all_events`, session_input.c:35-76. Detach runs
    /// on Main Thread with WorkerBarrier before the AppWorker pool entry drops.
    pub fn clear_pending_app_worker(&self, worker_index: u32, app_worker: u32) {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "AppWorker pending cleanup requires Main Thread and stopped Data Workers"
        );
        let slot = self
            .workers
            .get(worker_index as usize)
            .expect("Application worker event slot was constructed at Session init");
        // SAFETY: WorkerBarrier excludes the worker's mutable access.
        unsafe { &mut *slot.get() }.clear_pending_app_worker(app_worker);
    }

    #[inline(always)]
    /// # Safety
    /// The runtime must be the sole executor of this worker slot until the
    /// returned borrow ends; callers must not create overlapping mutable
    /// borrows of the same slot. Main-thread access requires stopped workers.
    #[expect(
        clippy::mut_from_ref,
        reason = "the caller exclusively owns this worker slot"
    )]
    pub unsafe fn worker_mut<'worker>(
        &'worker self,
        runtime: &DataPlaneMain,
    ) -> Result<&'worker mut SessionWorker, SessionError> {
        let worker = runtime
            .data_worker_id()
            .map_err(|_| SessionError::Invalid)?;
        let slot = self
            .workers
            .get(worker.slot())
            .ok_or(SessionError::Invalid)?;
        // SAFETY: DataPlaneMain is exclusively owned by this Data Worker;
        // worker slots are fixed before worker launch and never migrate.
        Ok(unsafe { &mut *slot.get() })
    }

    #[inline(always)]
    pub fn event_queue(&self, worker_index: u32) -> Option<&SvmMsgQ> {
        self.workers.get(worker_index as usize)?;
        self.worker_mq_segment.message_queue(worker_index)
    }

    #[inline(always)]
    fn worker_state(&self, worker_index: u32) -> SessionWorkerState {
        SessionWorkerState::from(self.worker_states[worker_index as usize].load(Ordering::Acquire))
    }

    #[inline(always)]
    fn set_worker_state(&self, worker_index: u32, state: SessionWorkerState) {
        self.worker_states[worker_index as usize].store(state as u8, Ordering::Release);
    }

    #[inline(always)]
    pub fn now(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn register_transport_protocol(&self) -> Result<u8, SessionError> {
        if hammer_runtime::ensure_main_thread_with_barrier().is_err() {
            return Err(SessionError::Invalid);
        }
        let previous = self.last_transport_protocol.load(Ordering::Acquire);
        let protocol = previous.checked_add(1).ok_or(SessionError::Invalid)?;
        // SAFETY: registration is Main Thread work performed before worker
        // launch or while WorkerBarrier excludes readers.
        unsafe { &mut *self.transport_io.get() }.push(None);
        unsafe { &mut *self.transport_control.get() }.push(None);
        unsafe { &mut *self.transport_time.get() }.push(None);
        for slot in &self.workers {
            // SAFETY: transport registration runs on Main Thread with the
            // barrier; no worker can hold its slot during this mutation.
            unsafe { &mut *slot.get() }
                .transport_time_subscriptions
                .push(protocol);
        }
        self.last_transport_protocol
            .store(protocol, Ordering::Release);
        Ok(protocol)
    }

    /// VPP session.c:1827-1847. The output edge was already compiled by the
    /// concrete transport output node before this startup publication.
    pub fn register_transport(
        &self,
        session_type: u8,
        output_next: super::node::SessionQueueNext,
        tx: SessionTxDispatch,
    ) {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Session transport registration requires Main Thread and stopped workers"
        );
        let slot = usize::from(session_type);
        let tx_entries = unsafe { &mut *self.session_tx.get() };
        let next_entries = unsafe { &mut *self.session_type_to_next.get() };
        if tx_entries.len() <= slot {
            tx_entries.resize(slot + 1, None);
        }
        if next_entries.len() <= slot {
            next_entries.resize(slot + 1, None);
        }
        assert!(tx_entries[slot].is_none() && next_entries[slot].is_none());
        tx_entries[slot] = Some(tx);
        next_entries[slot] = Some(output_next);
    }

    #[inline(always)]
    fn session_tx(&self, session_type: u8) -> Option<SessionTxDispatch> {
        unsafe { &*self.session_tx.get() }
            .get(usize::from(session_type))
            .copied()
            .flatten()
    }

    #[inline(always)]
    pub(crate) fn session_output_next(
        &self,
        session_type: u8,
    ) -> Option<super::node::SessionQueueNext> {
        unsafe { &*self.session_type_to_next.get() }
            .get(usize::from(session_type))
            .copied()
            .flatten()
    }

    /// VPP: session_node.c:1870-1883, dispatch by registered session type.
    pub fn register_transport_io(
        &self,
        protocol: u8,
        dispatch: SessionIoDispatch,
        update_time: SessionTimeDispatch,
    ) -> Result<(), SessionError> {
        if hammer_runtime::ensure_main_thread_with_barrier().is_err() {
            return Err(SessionError::Invalid);
        }
        let slot = unsafe { &mut *self.transport_io.get() }
            .get_mut(usize::from(protocol.saturating_sub(1)))
            .ok_or(SessionError::TransportNotRegistered)?;
        assert!(
            slot.replace(dispatch).is_none(),
            "transport IO registers once"
        );
        let slot = unsafe { &mut *self.transport_time.get() }
            .get_mut(usize::from(protocol - 1))
            .expect("registered transport retains its time slot");
        assert!(
            slot.replace(update_time).is_none(),
            "transport time registers once"
        );
        Ok(())
    }

    /// VPP: session_node.c:1766-1790. The registration is a single
    /// monomorphized entry for one protocol, not a transport VFT.
    pub fn register_transport_control(
        &self,
        protocol: u8,
        dispatch: SessionControlDispatch,
    ) -> Result<(), SessionError> {
        if hammer_runtime::ensure_main_thread_with_barrier().is_err() {
            return Err(SessionError::Invalid);
        }
        let slot = unsafe { &mut *self.transport_control.get() }
            .get_mut(usize::from(
                protocol.checked_sub(1).ok_or(SessionError::Invalid)?,
            ))
            .ok_or(SessionError::TransportNotRegistered)?;
        assert!(
            slot.replace(dispatch).is_none(),
            "transport control registers once"
        );
        Ok(())
    }

    #[inline(always)]
    fn transport_io(&self, protocol: u8) -> Option<SessionIoDispatch> {
        unsafe { &*self.transport_io.get() }
            .get(usize::from(protocol.checked_sub(1)?))
            .copied()
            .flatten()
    }

    #[inline(always)]
    fn transport_control(&self, protocol: u8) -> Option<SessionControlDispatch> {
        unsafe { &*self.transport_control.get() }
            .get(usize::from(protocol.checked_sub(1)?))
            .copied()
            .flatten()
    }

    #[inline(always)]
    fn transport_time(&self, protocol: u8) -> Option<SessionTimeDispatch> {
        unsafe { &*self.transport_time.get() }
            .get(usize::from(protocol.checked_sub(1)?))
            .copied()
            .flatten()
    }

    pub fn begin_pool_reallocation(&self) -> Result<(), SessionError> {
        let mut state = self.pool_reallocation.lock();
        state.workers_at_barrier = state.workers_at_barrier.saturating_add(1);
        Ok(())
    }

    pub fn finish_pool_reallocation(&self) -> Result<(), SessionError> {
        let mut state = self.pool_reallocation.lock();
        if state.workers_at_barrier == 0 {
            return Err(SessionError::Invalid);
        }
        state.workers_at_barrier -= 1;
        Ok(())
    }

    #[inline(always)]
    pub fn is_enabled(&self) -> bool {
        self.is_enabled.load(Ordering::Acquire)
    }

    /// VPP `session_enable` lifecycle transition. Registration and graph
    /// materialization remain separate from this flag.
    pub fn enable(&self) -> Result<(), SessionError> {
        self.is_enabled.store(true, Ordering::Release);
        Ok(())
    }

    /// VPP `session_disable` lifecycle transition.
    pub fn disable(&self) -> Result<(), SessionError> {
        self.is_enabled.store(false, Ordering::Release);
        Ok(())
    }

    pub fn allocate(
        &mut self,
        worker_index: u32,
        state: SessionState,
        session_type: u8,
        transport_protocol: u8,
        opaque: u32,
    ) -> Result<SessionHandle, SessionError> {
        let worker = self
            .workers
            .get_mut(worker_index as usize)
            .ok_or(SessionError::Invalid)?
            .get_mut();
        Ok(worker.allocate(state, session_type, transport_protocol, opaque))
    }

    /// VPP: `listen_session_alloc`, session.h:1044-1052. Hammer's Main Thread
    /// does not run a Data Worker, so its listening pool is separate from the
    /// Data Worker slots and owns no FIFO pair.
    pub fn allocate_listening_session(
        &self,
        session_type: u8,
        transport_protocol: u8,
        application_worker: u32,
        opaque: u32,
    ) -> Result<SessionHandle, SessionError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "listening Session allocation requires Main Thread and stopped Data Workers"
        );
        if application_worker == SESSION_INDEX_INVALID {
            return Err(SessionError::InvalidApplicationWorker);
        }
        // SAFETY: Main Thread holds WorkerBarrier while changing the
        // listener-only pool; no Data Worker can read it until publication.
        let sessions = unsafe { &mut *self.listening_sessions.get() };
        let mut handle = SessionHandle {
            worker_index: self.config.worker_count,
            session_index: SESSION_INDEX_INVALID,
        };
        handle.session_index = sessions.insert(Session::new(
            handle,
            SessionState::Listening,
            session_type,
            transport_protocol,
            opaque,
        ));
        let session = sessions
            .get_mut(handle.session_index)
            .expect("new listening Session remains allocated");
        session.handle = handle;
        session.application_worker = application_worker;
        Ok(handle)
    }

    /// VPP: `session_listen`, session.c:1467-1491. The concrete transport
    /// starts listening before its connection index is published here.
    pub fn attach_connection(
        &self,
        session: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "Session connection publication requires Main Thread and stopped Data Workers"
        );
        if session.worker_index != self.config.worker_count {
            return Err(SessionError::NoSession);
        }
        // SAFETY: Main Thread holds WorkerBarrier through listener setup.
        let sessions = unsafe { &mut *self.listening_sessions.get() };
        let entry = sessions
            .get_mut(session.session_index)
            .filter(|entry| entry.handle == session)
            .ok_or(SessionError::NoSession)?;
        entry.connection_index = connection_index;
        Ok(())
    }

    /// VPP: `app_worker_listen_sep`, application_worker.c:266-309. The
    /// listening Sessions retain their AppWorker and app-listener backlink.
    pub(crate) fn attach_listener(
        &self,
        listener: u32,
        global: Option<SessionHandle>,
        local: Option<SessionHandle>,
    ) {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "listening Session publication requires Main Thread and stopped Data Workers"
        );
        assert!(
            global.is_some() || local.is_some(),
            "listener publication requires a listening Session"
        );
        assert_ne!(global, local, "global and local listening Sessions differ");
        // SAFETY: Main Thread holds WorkerBarrier through listener setup.
        let sessions = unsafe { &mut *self.listening_sessions.get() };
        for handle in [global, local].into_iter().flatten() {
            assert_eq!(
                handle.worker_index, self.config.worker_count,
                "listening Session belongs to the control pool"
            );
            let session = sessions
                .get(handle.session_index)
                .expect("listening Session remains allocated until publication");
            assert_eq!(session.handle, handle, "listener Session handle is current");
            assert_eq!(session.load_state(), SessionState::Listening);
            assert_eq!(session.application_listener, SESSION_INDEX_INVALID);
            assert_ne!(
                session.application_worker, SESSION_INDEX_INVALID,
                "listening Session retains its Application worker before transport bind"
            );
        }
        for handle in [global, local].into_iter().flatten() {
            let session = sessions
                .get_mut(handle.session_index)
                .expect("validated listening Session remains allocated");
            session.application_listener = listener;
        }
    }

    /// VPP: `app_listener_get_w_handle`, application.c:78-85, follows the
    /// listening Session's `al_index` without traversing listener records.
    #[inline]
    pub(crate) fn application_listener(&self, handle: SessionHandle) -> Option<u32> {
        if handle.worker_index != self.config.worker_count {
            return None;
        }
        // SAFETY: Main Thread mutates this pool only under WorkerBarrier;
        // executing Data Workers are stopped before any pool change.
        let sessions = unsafe { &*self.listening_sessions.get() };
        sessions
            .get(handle.session_index)
            .filter(|session| session.handle() == handle)
            .and_then(Session::application_listener)
    }

    /// VPP: `session_stop_listen`, session.c:1497-1515, obtains the bound
    /// transport connection from the listening Session before unlisten.
    #[inline]
    pub fn listening_connection_index(&self, handle: SessionHandle) -> Option<u32> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "listening Session lookup requires Main Thread and stopped Data Workers"
        );
        if handle.worker_index != self.config.worker_count {
            return None;
        }
        // SAFETY: Main Thread holds WorkerBarrier while it reads this pool.
        unsafe { &*self.listening_sessions.get() }
            .get(handle.session_index)
            .filter(|session| session.handle() == handle)
            .map(Session::connection_index)
    }

    /// VPP: `listen_session_free`, session.h:1060-1065. The concrete
    /// transport has already stopped listening before this Session is freed.
    pub fn cleanup_listening_session(&self, handle: SessionHandle) -> Option<()> {
        assert!(
            hammer_runtime::ensure_main_thread_with_barrier().is_ok(),
            "listening Session cleanup requires Main Thread and stopped Data Workers"
        );
        if handle.worker_index != self.config.worker_count {
            return None;
        }
        // SAFETY: Main Thread holds WorkerBarrier through listener cleanup.
        let sessions = unsafe { &mut *self.listening_sessions.get() };
        let session = sessions.get(handle.session_index)?;
        if session.handle() != handle {
            return None;
        }
        assert_eq!(session.load_state(), SessionState::Listening);
        assert!(
            session.rx_fifo().is_none() && session.tx_fifo().is_none(),
            "listening Session owns no FIFO pair"
        );
        sessions.remove(handle.session_index).map(|_| ())
    }

    /// # Safety
    /// The runtime must exclusively own the session's worker slot.
    pub unsafe fn detach_transport(
        &self,
        runtime: &mut DataPlaneMain,
        session: SessionHandle,
    ) -> Result<(), SessionError> {
        let Some(entry) = (unsafe { self.worker_mut(runtime)? }).session_mut(session.session_index)
        else {
            return Err(SessionError::NoSession);
        };
        if entry.handle != session {
            return Err(SessionError::NoSession);
        }
        entry.detach_transport();
        Ok(())
    }

    /// VPP: `session_send_evt_to_thread`, session.c:28-86 (static inline).
    #[inline]
    fn enqueue_event(
        &self,
        worker_index: u32,
        event: SessionQueueEvent,
    ) -> Result<SessionEventEnqueue, SessionError> {
        let Some(queue) = self.event_queue(worker_index) else {
            return Err(SessionError::Invalid);
        };
        let mut producer = match queue.producer(SvmQueueConditionalWait::Nowait) {
            Ok(producer) => producer,
            Err(SvmMsgQError::LockBusy) => return Ok(SessionEventEnqueue::Busy),
            Err(source) => return Err(SessionError::MessageQueueAllocation { source }),
        };
        if queue.is_full()
            || queue
                .ring_is_full(0)
                .map_err(|source| SessionError::MessageQueueAllocation { source })?
        {
            return Ok(SessionEventEnqueue::Full);
        }
        // Lock order: target MQ producer, then RPC pool. The consumer drops
        // its MQ descriptor before taking the RPC pool lock.
        let rpc = if let SessionQueueEvent::Rpc { callback, argument } = event {
            let mut requests = self.rpc_requests.lock();
            if requests.len() == requests.capacity() {
                return Ok(SessionEventEnqueue::Full);
            }
            let sequence = self
                .rpc_sequence
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1);
            let index = requests.insert(SessionRpc {
                callback,
                argument,
                sequence,
            });
            Some((index, sequence))
        } else {
            None
        };
        let descriptor = producer
            .alloc_msg_on_ring(0)
            .expect("locked Session worker MQ retains checked IO ring space");
        {
            // SAFETY: this guard just allocated the descriptor; the target
            // worker cannot consume it until producer.add publishes it.
            let bytes = unsafe { producer.message_bytes_mut(descriptor) }
                .expect("allocated Session event retains its IO ring slot");
            let record = SessionEvent::mut_from_bytes(bytes)
                .expect("Session worker IO ring stores one SessionEvent");
            record.postponed = 0;
            record.worker_index = 0;
            record.rpc_sequence = 0;
            match event {
                SessionQueueEvent::Io {
                    event_type,
                    session_index,
                } => {
                    assert!(matches!(
                        event_type,
                        SessionEventType::Rx
                            | SessionEventType::Tx
                            | SessionEventType::TxFlush
                            | SessionEventType::BuiltinRx
                    ));
                    record.event_type = event_type.into();
                    record.session_index = session_index;
                }
                SessionQueueEvent::Session { event_type, handle } => {
                    assert!(matches!(
                        event_type,
                        SessionEventType::HalfClose
                            | SessionEventType::Close
                            | SessionEventType::Reset
                    ));
                    record.event_type = event_type.into();
                    record.session_index = handle.session_index;
                    record.worker_index = handle.worker_index;
                }
                SessionQueueEvent::Rpc { .. } => {
                    let (index, sequence) =
                        rpc.expect("RPC request is reserved before its MQ slot");
                    record.event_type = SessionEventType::Rpc.into();
                    record.session_index = index;
                    record.rpc_sequence = sequence;
                }
            }
        }
        let status = match producer.add(descriptor) {
            Ok(()) => SessionEventEnqueue::Enqueued,
            Err(
                SvmMsgQError::SignalAfterCommit { .. }
                | SvmMsgQError::EventSignalAfterCommit { .. },
            ) => SessionEventEnqueue::Enqueued,
            Err(source) => panic!("locked Session worker MQ rejected allocated event: {source}"),
        };
        drop(producer);
        if self.worker_state(worker_index) == SessionWorkerState::Interrupt {
            let queue_node = self.queue_node.load(Ordering::Acquire);
            assert_ne!(
                queue_node,
                u32::MAX,
                "Session queue NodeId installs before worker MQ publication"
            );
            interrupt_worker_node(DataWorkerId::new(worker_index), NodeId::new(queue_node));
        }
        Ok(status)
    }

    fn take_rpc(&self, index: u32, sequence: u64) -> Option<SessionRpc> {
        let mut requests = self.rpc_requests.lock();
        if requests.get(index)?.sequence != sequence {
            return None;
        }
        requests.remove(index)
    }

    pub fn program_migration(
        &mut self,
        request: SessionMigrationRequest,
    ) -> Result<(), SessionError> {
        let Some(worker) = self
            .workers
            .get_mut(request.old.worker_index as usize)
            .map(UnsafeCell::get_mut)
        else {
            return Err(SessionError::Invalid);
        };
        worker.queue_migration(request)
    }
}

/// VPP: `session_program_tx_io_evt`, session.c:107-113. The target worker
/// queue receives the Session index, not the application's pool index.
pub fn program_tx_io_event(
    handle: SessionHandle,
    event: SessionEventType,
) -> Result<SessionEventEnqueue, SessionError> {
    assert!(matches!(
        event,
        SessionEventType::Tx | SessionEventType::TxFlush
    ));
    SessionMain::global()?.enqueue_event(
        handle.worker_index,
        SessionQueueEvent::Io {
            event_type: event,
            session_index: handle.session_index,
        },
    )
}

/// VPP: `session_program_transport_io_evt`, session.c:133-140.
pub fn program_transport_io_event(
    handle: SessionHandle,
    event: SessionEventType,
) -> Result<SessionEventEnqueue, SessionError> {
    assert!(matches!(
        event,
        SessionEventType::Rx
            | SessionEventType::Tx
            | SessionEventType::TxFlush
            | SessionEventType::BuiltinRx
    ));
    SessionMain::global()?.enqueue_event(
        handle.worker_index,
        SessionQueueEvent::Io {
            event_type: event,
            session_index: handle.session_index,
        },
    )
}

/// VPP: `session_send_ctrl_evt_to_thread`, session.c:142-148. Session
/// control events carry the full handle in the worker MQ's IO ring.
pub fn send_control_event(
    handle: SessionHandle,
    event: SessionEventType,
) -> Result<SessionEventEnqueue, SessionError> {
    assert!(matches!(
        event,
        SessionEventType::HalfClose | SessionEventType::Close | SessionEventType::Reset
    ));
    SessionMain::global()?.enqueue_event(
        handle.worker_index,
        SessionQueueEvent::Session {
            event_type: event,
            handle,
        },
    )
}

/// VPP: `session_send_rpc_evt_to_thread_force`, session.c:151-156.
pub fn send_rpc_event_force(
    worker_index: u32,
    callback: fn(u64),
    argument: u64,
) -> Result<SessionEventEnqueue, SessionError> {
    SessionMain::global()?
        .enqueue_event(worker_index, SessionQueueEvent::Rpc { callback, argument })
}

/// VPP: `session_send_rpc_evt_to_thread`, session.c:158-165.
pub fn send_rpc_event(
    worker_index: u32,
    callback: fn(u64),
    argument: u64,
) -> Result<SessionEventEnqueue, SessionError> {
    let main = SessionMain::global()?;
    if main.event_queue(worker_index).is_none() {
        return Err(SessionError::Invalid);
    }
    if is_current_worker(DataWorkerId::new(worker_index)) {
        callback(argument);
        Ok(SessionEventEnqueue::Enqueued)
    } else {
        main.enqueue_event(worker_index, SessionQueueEvent::Rpc { callback, argument })
    }
}

/// VPP: `session_enqueue_notify`, session.c:626-648. A builtin callback may
/// append a self-tap while session-input still owns this AppWorker's pending
/// bit; the node reschedules the newly appended event after the callback.
pub fn enqueue_notify(session: &Session) -> Option<()> {
    let application_worker = session.application_worker()?;
    let application = ApplicationMain::global()?;
    // SAFETY: the builtin callback runs on the Session's worker and exclusively
    // owns its AppWorker event column. Detach waits for WorkerBarrier.
    let worker = unsafe { application.worker(application_worker) }?;
    worker.add_event(session, SessionEventType::Rx);
    Some(())
}

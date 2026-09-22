use std::cell::UnsafeCell;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use hammer_infra::linked_list::LinkedList;
use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_infra::svm::msg_queue::{SvmMsgQ, SvmMsgQConfig, SvmMsgQError, SvmMsgQRingConfig};
use hammer_infra::svm::queue::SvmQueueConditionalWait;
use hammer_infra::sync::SpinLock;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::transport::{TransportSendParams, TransportTxMode};

pub const SESSION_INDEX_INVALID: u32 = u32::MAX;

pub const SESSION_E_NONE: i32 = 0;
pub const SESSION_E_UNKNOWN: i32 = -1;
pub const SESSION_E_ALLOC: i32 = -4;
pub const SESSION_E_NOROUTE: i32 = -6;
pub const SESSION_E_NOINTF: i32 = -7;
pub const SESSION_E_NOIP: i32 = -8;
pub const SESSION_E_NOPORT: i32 = -9;
pub const SESSION_E_NOSUPPORT: i32 = -10;
pub const SESSION_E_NOSESSION: i32 = -12;
pub const SESSION_E_PORTINUSE: i32 = -15;
pub const SESSION_E_INVALID: i32 = -19;
pub const SESSION_E_SEG_NO_SPACE: i32 = -23;
pub const SESSION_E_SEG_CREATE: i32 = -25;
pub const SESSION_E_MQ_MSG_ALLOC: i32 = -31;
pub const SESSION_E_TRANSPORT_NO_REG: i32 = -40;
pub const SESSION_EVENT_QUEUE_LOCK_FAILED: i32 = -1;
pub const SESSION_EVENT_QUEUE_FULL: i32 = -2;

#[repr(C)]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, KnownLayout, FromBytes, Immutable, IntoBytes,
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
    pub event_ring_capacity: u32,
    pub event_element_size: u32,
    pub session_capacity: u32,
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
            configured_worker_mq_length: 2_048,
            worker_mq_segment_size: 64 << 20,
            event_ring_capacity: 2_048,
            event_element_size: size_of::<SessionEventRecord>() as u32,
            session_capacity: 1_024,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionDmaTransfer {
    pub pending_tx_buffers: u32,
    pub pending_tx_nexts: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionTxContext {
    pub session: SessionHandle,
    pub connection_index: u32,
    pub transport_protocol: u8,
    pub tx_mode: TransportTxMode,
    pub send_params: TransportSendParams,
    pub max_dequeue: u32,
    pub left_to_send: u32,
    pub max_length_to_send: u32,
    pub dequeue_per_first_buffer: u16,
    pub dequeue_per_buffer: u16,
    pub segments_per_event: u16,
    pub buffers_needed: u16,
    pub buffers_per_segment: u8,
    pub datagram_header: [u8; 32],
}

impl Default for SessionTxContext {
    fn default() -> Self {
        Self {
            session: SessionHandle::invalid(),
            connection_index: SESSION_INDEX_INVALID,
            transport_protocol: 0,
            tx_mode: TransportTxMode::Internal,
            send_params: TransportSendParams::default(),
            max_dequeue: 0,
            left_to_send: 0,
            max_length_to_send: 0,
            dequeue_per_first_buffer: 0,
            dequeue_per_buffer: 0,
            segments_per_event: 0,
            buffers_needed: 0,
            buffers_per_segment: 0,
            datagram_header: [0; 32],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionWorkerState {
    Polling,
    Interrupt,
    Idle,
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

pub struct SessionWorker<O = u32> {
    sessions: Pool<Session<O>>,
    event_queue_index: u32,
    last_time: f64,
    last_time_us: u64,
    worker_index: u32,
    transport_time_subscriptions: Vec<u8>,
    pending_io_sessions: Vec<SessionHandle>,
    timer_fd: i32,
    timer_fd_file: u64,
    timer: TimerWheel1t2w2048sl<SessionHandle>,
    flags: SessionWorkerFlags,
    state: SessionWorkerState,
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

impl<O> SessionWorker<O> {
    fn new(worker_index: u32, config: SessionConfig) -> Self {
        Self {
            sessions: Pool::with_capacity(config.session_capacity as usize),
            event_queue_index: worker_index,
            last_time: 0.0,
            last_time_us: 0,
            worker_index,
            transport_time_subscriptions: Vec::new(),
            pending_io_sessions: Vec::new(),
            timer_fd: -1,
            timer_fd_file: u64::MAX,
            timer: TimerWheel1t2w2048sl::new(config.event_ring_capacity as usize),
            flags: SessionWorkerFlags {
                adaptive: !config.no_adaptive,
            },
            state: if config.poll_main {
                SessionWorkerState::Polling
            } else {
                SessionWorkerState::Interrupt
            },
            tx_context: SessionTxContext::default(),
            event_elements: Pool::with_capacity(config.event_ring_capacity as usize),
            control_event_data: Pool::with_capacity(config.event_ring_capacity as usize),
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

    #[inline(always)]
    pub fn session(&self, session_index: u32) -> Option<&Session<O>> {
        self.sessions.get(session_index)
    }

    #[inline(always)]
    pub fn session_mut(&mut self, session_index: u32) -> Option<&mut Session<O>> {
        self.sessions.get_mut(session_index)
    }

    pub fn handle_event(&mut self, queue: &SvmMsgQ) -> i32 {
        loop {
            let descriptor = match queue.sub(SvmQueueConditionalWait::Nowait) {
                Ok(descriptor) => descriptor,
                Err(SvmMsgQError::Empty) => return SESSION_E_NONE,
                Err(_) => return SESSION_E_UNKNOWN,
            };
            let event = match queue.read::<SessionEventRecord>(descriptor) {
                Ok(event) => SessionEvent::from(event),
                Err(_) => {
                    drop(queue.free_msg(descriptor));
                    return SESSION_E_INVALID;
                }
            };
            if queue.free_msg(descriptor).is_err() {
                return SESSION_E_UNKNOWN;
            }
            let retval = self.consume_event(event);
            if retval != SESSION_E_NONE {
                return retval;
            }
        }
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
    pub fn session_from_handle(&self, handle: SessionHandle) -> Option<&Session<O>> {
        if handle.worker_index != self.worker_index {
            return None;
        }
        self.session(handle.session_index)
    }

    #[inline(always)]
    pub fn store_state(&self, handle: SessionHandle, state: SessionState) -> i32 {
        let Some(session) = self.session_from_handle(handle) else {
            return SESSION_E_NOSESSION;
        };
        session.store_state(state);
        SESSION_E_NONE
    }

    #[inline(always)]
    pub fn enqueue_ready(&mut self, handle: SessionHandle, protocol: u8) -> i32 {
        let Some(session) = self.session_from_handle(handle) else {
            return SESSION_E_NOSESSION;
        };
        if session.session_type != protocol {
            return SESSION_E_INVALID;
        }
        self.pending_io_sessions.push(handle);
        SESSION_E_NONE
    }

    pub fn allocate_control_data(&mut self, data: SessionControlData) -> u32 {
        self.control_event_data.insert(data)
    }

    pub fn release_control_data(&mut self, index: u32) -> i32 {
        if self.control_event_data.remove(index).is_some() {
            SESSION_E_NONE
        } else {
            SESSION_E_INVALID
        }
    }

    pub fn consume_event(&mut self, event: SessionEvent) -> i32 {
        if event.session.worker_index != self.worker_index
            || self.session(event.session.session_index).is_none()
        {
            return SESSION_E_NOSESSION;
        }
        self.pending_io_sessions.push(event.session);
        SESSION_E_NONE
    }

    pub fn queue_migration(&mut self, request: SessionMigrationRequest) -> i32 {
        self.migration.lock().requests.push(request);
        SESSION_E_NONE
    }

    pub fn handle_migrations(&mut self) -> i32 {
        let mut migration = self.migration.lock();
        let requests = std::mem::take(&mut migration.requests);
        migration.handling.extend(requests);
        i32::try_from(migration.handling.len()).unwrap_or(i32::MAX)
    }

    pub fn update_time(&mut self, now: f64, now_us: u64) {
        self.last_time = now;
        self.last_time_us = now_us;
        self.last_event_poll = now;
    }
}

pub struct Session<O = u32> {
    handle: SessionHandle,
    state: AtomicU8,
    session_type: u8,
    flags: SessionFlags,
    rx_fifo: Option<SvmFifo>,
    tx_fifo: Option<SvmFifo>,
    connection_index: u32,
    listener_handle: SessionHandle,
    half_open_index: u32,
    opaque: O,
}

impl<O> Session<O> {
    fn new(handle: SessionHandle, state: SessionState, protocol: u8, opaque: O) -> Self {
        Self {
            handle,
            state: AtomicU8::new(u8::from(state)),
            session_type: protocol,
            flags: SessionFlags::default(),
            rx_fifo: None,
            tx_fifo: None,
            connection_index: SESSION_INDEX_INVALID,
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

    #[inline(always)]
    pub fn opaque(&self) -> &O {
        &self.opaque
    }

    #[inline(always)]
    pub fn opaque_mut(&mut self) -> &mut O {
        &mut self.opaque
    }

    #[inline(always)]
    pub fn rx_fifo(&self) -> Option<&SvmFifo> {
        self.rx_fifo.as_ref()
    }

    #[inline(always)]
    pub fn tx_fifo(&self) -> Option<&SvmFifo> {
        self.tx_fifo.as_ref()
    }

    #[inline(always)]
    pub const fn handle(&self) -> SessionHandle {
        self.handle
    }

    #[inline(always)]
    pub const fn transport_protocol(&self) -> u8 {
        self.session_type
    }

    #[inline(always)]
    pub const fn connection_index(&self) -> u32 {
        self.connection_index
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
    pub rx_ready: bool,
    pub tx_ready: bool,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionEvent {
    pub event_type: u8,
    pub postponed: bool,
    pub protocol: u8,
    pub operation: u8,
    pub session: SessionHandle,
    pub control_data_index: u32,
    pub payload: [u64; 2],
}

#[repr(C)]
#[derive(Clone, Copy, KnownLayout, FromBytes, Immutable, IntoBytes)]
struct SessionEventRecord {
    event_type: u8,
    postponed: u8,
    protocol: u8,
    operation: u8,
    session: SessionHandle,
    control_data_index: u32,
    payload: [u64; 2],
}

impl From<SessionEvent> for SessionEventRecord {
    fn from(event: SessionEvent) -> Self {
        Self {
            event_type: event.event_type,
            postponed: u8::from(event.postponed),
            protocol: event.protocol,
            operation: event.operation,
            session: event.session,
            control_data_index: event.control_data_index,
            payload: event.payload,
        }
    }
}

impl From<SessionEventRecord> for SessionEvent {
    fn from(event: SessionEventRecord) -> Self {
        Self {
            event_type: event.event_type,
            postponed: event.postponed != 0,
            protocol: event.protocol,
            operation: event.operation,
            session: event.session,
            control_data_index: event.control_data_index,
            payload: event.payload,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionRuntimeEngine {
    Disabled,
    RuleTable,
    None,
    Sdl,
}

pub struct SessionMain<O = u32> {
    config: SessionConfig,
    workers: Vec<SessionWorker<O>>,
    session_tx_modes: UnsafeCell<Vec<TransportTxMode>>,
    session_type_to_next: UnsafeCell<Vec<u32>>,
    transport_cl_thread: u32,
    pool_reallocation: SpinLock<PoolReallocationState>,
    worker_mq_segment: SvmFifoSegment,
    last_transport_protocol: AtomicU8,
    is_enabled: AtomicBool,
    is_initialized: bool,
    runtime_engine: SessionRuntimeEngine,
    dump_worker_segments: bool,
}

// SAFETY: workers are constructed before publication and each mutable worker
// entry is owned by its runtime worker index. Main-thread pool publication is
// performed only while Data Workers are stopped; SvmMsgQ provides its own
// producer/consumer synchronization.
unsafe impl<O: Send> Sync for SessionMain<O> {}

impl<O: Default> SessionMain<O> {
    pub fn init(config: SessionConfig, worker_mq_segment: SvmFifoSegment) -> Result<Self, i32> {
        let mut workers = Vec::with_capacity(config.worker_count as usize);
        for worker_index in 0..config.worker_count {
            workers.push(SessionWorker::new(worker_index, config));
        }
        let mut main = Self {
            config,
            workers,
            session_tx_modes: UnsafeCell::new(Vec::new()),
            session_type_to_next: UnsafeCell::new(Vec::new()),
            transport_cl_thread: 0,
            pool_reallocation: SpinLock::new(PoolReallocationState {
                workers_at_barrier: 0,
                workers_doing_work: 0,
            }),
            worker_mq_segment,
            last_transport_protocol: AtomicU8::new(0),
            is_enabled: AtomicBool::new(config.session_enable_asap),
            is_initialized: true,
            runtime_engine: SessionRuntimeEngine::None,
            dump_worker_segments: false,
        };
        main.allocate_event_queues()?;
        Ok(main)
    }

    pub fn allocate_event_queues(&mut self) -> Result<(), i32> {
        if self.config.event_ring_capacity == 0 || self.config.configured_worker_mq_length == 0 {
            return Err(SESSION_E_INVALID);
        }
        let ring = [SvmMsgQRingConfig::new(
            self.config.event_ring_capacity,
            size_of::<SessionEventRecord>() as u32,
        )];
        let config = SvmMsgQConfig {
            consumer_pid: std::process::id() as i32,
            q_nitems: self.config.configured_worker_mq_length,
            rings: &ring,
        };
        for worker_index in self.worker_mq_segment.mqs.len() as u32..self.config.worker_count {
            if self
                .worker_mq_segment
                .allocate_message_queue(worker_index, &config)
                .is_err()
            {
                return Err(SESSION_E_MQ_MSG_ALLOC);
            }
        }
        Ok(())
    }

    #[inline(always)]
    pub fn worker(&self, worker_index: u32) -> Option<&SessionWorker<O>> {
        self.workers.get(worker_index as usize)
    }

    pub fn worker_mut(&mut self, worker_index: u32) -> Option<&mut SessionWorker<O>> {
        self.workers.get_mut(worker_index as usize)
    }

    #[inline(always)]
    pub fn event_queue(&self, worker_index: u32) -> Option<&SvmMsgQ> {
        let worker = self.worker(worker_index)?;
        self.worker_mq_segment
            .message_queue(worker.event_queue_index)
    }

    pub fn register_transport_type(
        &self,
        protocol: u8,
        tx_mode: TransportTxMode,
        output_next: u32,
    ) -> Result<u8, i32> {
        if hammer_runtime::ensure_main_thread_with_barrier().is_err() {
            return Err(SESSION_E_INVALID);
        }
        let previous = self.last_transport_protocol.load(Ordering::Acquire);
        let expected = previous.checked_add(1).ok_or(SESSION_E_INVALID)?;
        if protocol != expected {
            return Err(SESSION_E_INVALID);
        }
        // SAFETY: registration is Main Thread work performed before worker
        // launch or while WorkerBarrier excludes readers.
        unsafe { &mut *self.session_tx_modes.get() }.push(tx_mode);
        unsafe { &mut *self.session_type_to_next.get() }.push(output_next);
        self.last_transport_protocol
            .store(protocol, Ordering::Release);
        Ok(protocol)
    }

    pub fn begin_pool_reallocation(&self) -> i32 {
        let mut state = self.pool_reallocation.lock();
        state.workers_at_barrier = state.workers_at_barrier.saturating_add(1);
        SESSION_E_NONE
    }

    pub fn finish_pool_reallocation(&self) -> i32 {
        let mut state = self.pool_reallocation.lock();
        if state.workers_at_barrier == 0 {
            return SESSION_E_INVALID;
        }
        state.workers_at_barrier -= 1;
        SESSION_E_NONE
    }

    #[inline(always)]
    pub fn is_enabled(&self) -> bool {
        self.is_enabled.load(Ordering::Acquire)
    }

    pub fn allocate(
        &mut self,
        worker_index: u32,
        state: SessionState,
        protocol: u8,
    ) -> Result<SessionHandle, i32> {
        let worker = self.worker_mut(worker_index).ok_or(SESSION_E_INVALID)?;
        let session_index = worker.sessions.insert(Session::new(
            SessionHandle {
                worker_index,
                session_index: SESSION_INDEX_INVALID,
            },
            state,
            protocol,
            O::default(),
        ));
        let handle = SessionHandle {
            worker_index,
            session_index,
        };
        worker
            .sessions
            .get_mut(session_index)
            .expect("inserted Session remains in its worker pool")
            .handle = handle;
        Ok(handle)
    }

    pub fn attach_transport(
        &mut self,
        session: SessionHandle,
        protocol: u8,
        connection_index: u32,
    ) -> i32 {
        let Some(entry) = self
            .worker_mut(session.worker_index)
            .and_then(|worker| worker.session_mut(session.session_index))
        else {
            return SESSION_E_NOSESSION;
        };
        entry.session_type = protocol;
        entry.connection_index = connection_index;
        SESSION_E_NONE
    }

    pub fn detach_transport(&mut self, session: SessionHandle) -> i32 {
        let Some(entry) = self
            .worker_mut(session.worker_index)
            .and_then(|worker| worker.session_mut(session.session_index))
        else {
            return SESSION_E_NOSESSION;
        };
        entry.connection_index = SESSION_INDEX_INVALID;
        entry.store_state(SessionState::TransportDeleted);
        SESSION_E_NONE
    }

    pub fn enqueue_event(&self, worker_index: u32, event: SessionEvent) -> i32 {
        let Some(queue) = self.event_queue(worker_index) else {
            return SESSION_E_INVALID;
        };
        let mut producer = match queue.producer(SvmQueueConditionalWait::Nowait) {
            Ok(producer) => producer,
            Err(SvmMsgQError::LockBusy) => return SESSION_EVENT_QUEUE_LOCK_FAILED,
            Err(_) => return SESSION_EVENT_QUEUE_LOCK_FAILED,
        };
        let descriptor = match producer.alloc_msg(size_of::<SessionEventRecord>()) {
            Ok(descriptor) => descriptor,
            Err(SvmMsgQError::QueueFull | SvmMsgQError::RingFull { .. }) => {
                return SESSION_EVENT_QUEUE_FULL;
            }
            Err(_) => return SESSION_E_MQ_MSG_ALLOC,
        };
        let record = SessionEventRecord::from(event);
        if producer.write(descriptor, &record).is_err() {
            drop(queue.free_msg(descriptor));
            return SESSION_E_MQ_MSG_ALLOC;
        }
        match producer.add(descriptor) {
            Ok(()) => SESSION_E_NONE,
            Err(
                SvmMsgQError::SignalAfterCommit { .. }
                | SvmMsgQError::EventSignalAfterCommit { .. },
            ) => SESSION_E_UNKNOWN,
            Err(SvmMsgQError::QueueFull) => {
                drop(queue.free_msg(descriptor));
                SESSION_EVENT_QUEUE_FULL
            }
            Err(_) => {
                drop(queue.free_msg(descriptor));
                SESSION_E_MQ_MSG_ALLOC
            }
        }
    }

    pub fn program_migration(&mut self, request: SessionMigrationRequest) -> i32 {
        let Some(worker) = self.worker_mut(request.old.worker_index) else {
            return SESSION_E_INVALID;
        };
        worker.queue_migration(request)
    }

    pub fn flush_enqueue_events(&mut self, protocol: u8, worker_index: u32) -> i32 {
        let Some(worker) = self.worker_mut(worker_index) else {
            return SESSION_E_INVALID;
        };
        let sessions = &worker.sessions;
        worker.pending_io_sessions.retain(|handle| {
            sessions
                .get(handle.session_index)
                .is_some_and(|session| session.session_type != protocol)
        });
        SESSION_E_NONE
    }
}

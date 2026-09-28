use std::cell::UnsafeCell;
use std::mem::size_of;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use hammer_core::data_plane::NodeId;
use hammer_infra::bitmap::Bitmap;
use hammer_infra::linked_list::LinkedList;
use hammer_infra::pool::Pool;
use hammer_infra::svm::fifo::Fifo as SvmFifo;
use hammer_infra::svm::fifo_segment::SvmFifoSegment;
use hammer_infra::svm::msg_queue::{SvmMsgQ, SvmMsgQConfig, SvmMsgQError, SvmMsgQRingConfig};
use hammer_infra::svm::queue::SvmQueueConditionalWait;
use hammer_infra::sync::SpinLock;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::DataPlaneMain;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use super::app::ApplicationMain;
use super::error::SessionError;
use super::segment_manager::{SegmentManager, SegmentManagerError, SegmentManagerMain};
use crate::transport::{Transport, TransportSendParams, TransportTxMode};

/// VPP: session_node.c:1858-1915. A registered protocol supplies one
/// monomorphized entry; the service retains no transport object or VFT.
pub type SessionIoDispatch = fn(
    &mut DataPlaneMain,
    &mut SessionWorker,
    u32,
    SessionEventType,
) -> Result<(usize, bool), SessionError>;

/// VPP: session_node.c:2024-2031, session_update_time_subscribers.
pub type SessionTimeDispatch =
    fn(&mut DataPlaneMain, &mut SessionWorker, f64) -> Result<(), SessionError>;

pub const SESSION_INDEX_INVALID: u32 = u32::MAX;

// Compatibility names for callers that still cross the old retval boundary.
// Internal Session/Transport operations use SessionError below; these are
// deprecated and are not part of the ADR-0038 Rust contract.
#[deprecated(note = "use Result<(), SessionError>; SESSION_E_NONE is an ABI retval only")]
pub const SESSION_E_NONE: i32 = 0;
#[deprecated(note = "use SessionError::Unknown")]
pub const SESSION_E_UNKNOWN: i32 = -1;
#[deprecated(note = "use SessionError::Allocation")]
pub const SESSION_E_ALLOC: i32 = -4;
#[deprecated(note = "use SessionError::NoRoute")]
pub const SESSION_E_NOROUTE: i32 = -6;
#[deprecated(note = "use SessionError::NoInterface")]
pub const SESSION_E_NOINTF: i32 = -7;
#[deprecated(note = "use SessionError::NoIp")]
pub const SESSION_E_NOIP: i32 = -8;
#[deprecated(note = "use SessionError::NoPort")]
pub const SESSION_E_NOPORT: i32 = -9;
#[deprecated(note = "use SessionError::NotSupported")]
pub const SESSION_E_NOSUPPORT: i32 = -10;
#[deprecated(note = "use SessionError::NoSession")]
pub const SESSION_E_NOSESSION: i32 = -12;
#[deprecated(note = "use SessionError::PortInUse")]
pub const SESSION_E_PORTINUSE: i32 = -15;
#[deprecated(note = "use SessionError::Invalid")]
pub const SESSION_E_INVALID: i32 = -19;
#[deprecated(note = "use SessionError::SegmentNoSpace")]
pub const SESSION_E_SEG_NO_SPACE: i32 = -23;
#[deprecated(note = "use SessionError::SegmentCreate")]
pub const SESSION_E_SEG_CREATE: i32 = -25;
#[deprecated(note = "use SessionError::MessageQueueAllocation")]
pub const SESSION_E_MQ_MSG_ALLOC: i32 = -31;
#[deprecated(note = "use SessionError::TransportNotRegistered")]
pub const SESSION_E_TRANSPORT_NO_REG: i32 = -40;
#[deprecated(note = "use SessionEventEnqueue::Busy")]
pub const SESSION_EVENT_QUEUE_LOCK_FAILED: i32 = -1;
#[deprecated(note = "use SessionEventEnqueue::Full")]
pub const SESSION_EVENT_QUEUE_FULL: i32 = -2;

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
            event_element_size: size_of::<SessionEvent>() as u32,
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

pub struct SessionWorker {
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
        newest_start: u32,
        newest_len: NonZeroU32,
        rx_available: u32,
    },
}

impl SessionWorker {
    fn new(worker_index: u32, config: SessionConfig) -> Self {
        Self {
            sessions: Pool::with_capacity(config.session_capacity as usize),
            event_queue_index: worker_index,
            last_time: 0.0,
            last_time_us: 0,
            worker_index,
            transport_time_subscriptions: Vec::new(),
            sessions_to_enqueue: Vec::new(),
            app_workers_pending: Bitmap::new(),
            input_node: None,
            queue_node: None,
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
        if self.state == SessionWorkerState::Interrupt {
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

    pub fn allocate(&mut self, state: SessionState, protocol: u8, opaque: u32) -> SessionHandle {
        let handle = SessionHandle {
            worker_index: self.worker_index,
            session_index: SESSION_INDEX_INVALID,
        };
        let mut session = Session::new(handle, state, protocol, opaque);
        session.flags.connectionless = SessionMain::global()
            .expect("Session Main initializes before Session allocation")
            .transport_tx_mode(protocol)
            == Some(TransportTxMode::Datagram);
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
        protocol: u8,
        opaque: u32,
    ) -> SessionHandle {
        let handle = self.allocate(SessionState::Created, protocol, opaque);
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
        let mut newest_start: Option<u32> = None;
        let mut newest_end: Option<u32> = None;
        for buffer in runtime.chain(buffer_index) {
            let bytes = buffer.current();
            let length = u32::try_from(bytes.len())
                .expect("a packet buffer's current length fits the Session FIFO index");
            if offset == 0 {
                if accepted == buffered_len {
                    let available = fifo.max_enqueue();
                    if bytes.len() >= available {
                        fifo.want_deq_notification();
                    }
                    let written = fifo.enqueue(bytes);
                    let chunk_accepted = written.min(bytes.len()).min(available);
                    accepted = accepted
                        .checked_add(chunk_accepted as u32)
                        .expect("RX packet chain length fits u32");
                    promoted = promoted
                        .checked_add((written - chunk_accepted) as u32)
                        .expect("promoted RX FIFO bytes fit u32");
                }
            } else {
                let chunk_offset = offset.checked_add(buffered_len).ok_or(
                    SessionError::RxOutOfOrderOffsetOverflow {
                        session_id,
                        offset,
                        buffered_len,
                    },
                )?;
                let result = fifo.enqueue_ooo(chunk_offset, bytes).map_err(|source| {
                    SessionError::RxOutOfOrderEnqueue {
                        session_id,
                        offset: chunk_offset,
                        source,
                    }
                })?;
                accepted = accepted
                    .checked_add(result.accepted)
                    .expect("RX packet chain length fits u32");
                promoted = promoted
                    .checked_add(result.delivered)
                    .expect("promoted RX FIFO bytes fit u32");
                if let Some(start) = result.start {
                    let end = start
                        .checked_add(result.len)
                        .ok_or(SessionError::OooSpanInvalid { session_id })?;
                    newest_start = Some(newest_start.map_or(start, |value| value.min(start)));
                    newest_end = Some(newest_end.map_or(end, |value| value.max(end)));
                }
            }
            buffered_len = buffered_len
                .checked_add(length)
                .expect("RX packet chain length fits u32");
        }
        let rx_available = u32::try_from(fifo.max_enqueue()).unwrap_or(u32::MAX);
        if rx_available == 0 {
            fifo.want_deq_notification();
        }
        if offset == 0 && (accepted != 0 || promoted != 0) {
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
            let start = newest_start.ok_or(SessionError::OooSpanMissing { session_id })?;
            let len = newest_end
                .and_then(|end| end.checked_sub(start))
                .and_then(NonZeroU32::new)
                .ok_or(SessionError::OooSpanInvalid { session_id })?;
            Ok(RxDelivery::OutOfOrder {
                accepted,
                newest_start: start,
                newest_len: len,
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
            let event = queue.read::<SessionEvent>(descriptor);
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
            let event = event.map_err(|source| SessionError::MessageQueueAllocation { source })?;
            let event_type = SessionEventType::try_from(event.event_type)?;
            if u8::from(event_type) >= u8::from(SessionEventType::Rpc) {
                self.allocate_control_event(event);
            } else {
                self.allocate_new_event(event);
            }
        }
        Ok(count)
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
        main: &SessionMain,
    ) -> Result<usize, SessionError> {
        let mut packets = 0;
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
                    SessionEventType::Tx | SessionEventType::TxFlush | SessionEventType::Rx => {
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

    /// VPP: session_node.c:1472-1684, session_tx_fifo_peek_and_snd. TCP
    /// retains FIFO bytes until ACK; one packet is produced per visit.
    #[inline(always)]
    pub fn tx_fifo_peek_and_send<T, E>(
        &mut self,
        runtime: &mut DataPlaneMain,
        session_index: u32,
        transport: &T,
        next: crate::session::node::SessionQueueNext,
    ) -> Result<(usize, bool), SessionError>
    where
        T: Transport<E>,
    {
        let Some(session) = self.session(session_index) else {
            return Ok((0, false));
        };
        if session.load_state() != SessionState::Ready {
            return Ok((
                0,
                u8::from(session.load_state()) < u8::from(SessionState::TransportClosed),
            ));
        }
        let connection = session.connection_index();
        let fifo = session
            .tx_fifo()
            .expect("ready Session retains its TX FIFO");
        let params = transport.send_params(connection, self.worker_index);
        if params.send_space == 0 {
            return Ok((0, !params.flags.deschedule));
        }
        let available = fifo.max_dequeue().saturating_sub(params.tx_offset as usize);
        if available == 0 {
            fifo.unset_event();
            return Ok((
                0,
                fifo.max_dequeue() > params.tx_offset as usize && fifo.set_event(),
            ));
        }
        let mut buffers = [0u32; 1];
        if runtime.buffer_alloc(&mut buffers) == 0 {
            return Ok((0, true));
        }
        let buffer = runtime.buffer_mut(buffers[0]);
        let capacity = buffer.make_headroom(60).len();
        let length = available
            .min(params.send_space as usize)
            .min(params.send_mss as usize)
            .min(capacity)
            .min(u16::MAX as usize);
        if length == 0 {
            runtime.buffer_free_one(buffers[0]);
            return Ok((0, false));
        }
        let data = buffer.put_uninit(length as u16);
        assert_eq!(
            fifo.peek(params.tx_offset as usize, length, data),
            length,
            "sendable FIFO range remains readable while TCP owns the consumer"
        );
        transport.push_header(
            connection,
            self.worker_index,
            &mut buffers,
            fifo.max_dequeue().min(u32::MAX as usize) as u32,
        );
        let next_params = transport.send_params(connection, self.worker_index);
        let pending = fifo.max_dequeue() > next_params.tx_offset as usize;
        if !pending {
            fifo.unset_event();
            let rearmed = fifo.max_dequeue() > next_params.tx_offset as usize && fifo.set_event();
            self.add_pending_tx_buffer(runtime, buffers[0], next);
            return Ok((1, rearmed));
        }
        self.add_pending_tx_buffer(runtime, buffers[0], next);
        Ok((1, true))
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
        if session.session_type != protocol {
            return Err(SessionError::Invalid);
        }
        self.allocate_new_event(SessionEvent::from((
            SessionEventType::Tx,
            handle.session_index,
        )));
        Ok(())
    }

    /// VPP: `session_enqueue_notify`, session.c:626-648. RX coalescing is
    /// performed by the caller; this operation only queues the app event.
    pub fn enqueue_notify(&mut self, runtime: &DataPlaneMain, handle: SessionHandle) -> Option<()> {
        let app_worker_index = self.queue_rx_notification(handle)?;
        self.program_app_worker(runtime, app_worker_index);
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
                    SessionEvent::from((SessionEventType::BuiltinRx, handle.session_index)),
                )
                .map(Some);
        }
        let session = self
            .session_from_handle(handle)
            .ok_or(SessionError::NoSession)?;
        if u8::from(session.load_state()) >= u8::from(SessionState::TransportClosing) {
            return Ok(None);
        }
        if let Some(app_worker_index) = self.queue_rx_notification(handle) {
            self.app_workers_pending.set(app_worker_index as usize);
        }
        Ok(None)
    }

    /// VPP: `session_enqueue_notify_inline`, session.c:626-648.
    #[inline(always)]
    fn queue_rx_notification(&mut self, handle: SessionHandle) -> Option<u32> {
        let session = self.session_from_handle(handle)?;
        let app_worker_index = session.application_worker()?;
        let application = ApplicationMain::global()
            .expect("Application Main initializes before Session RX notification");
        // SAFETY: this Session worker exclusively executes its AppWorker
        // event slot; detach waits for WorkerBarrier before removing it.
        let app_worker = unsafe { application.worker(app_worker_index) }
            .expect("an attached Session retains its AppWorker");
        let session = self
            .session_from_handle(handle)
            .expect("RX notification retains its Session");
        self.program_io_event(
            app_worker,
            session,
            SessionEventType::Rx,
            session.flags.connectionless,
        );
        Some(app_worker_index)
    }

    /// VPP: `session_main_flush_enqueue_events`, session.c:689-714.
    pub fn flush_enqueue_events(&mut self, runtime: &DataPlaneMain, protocol: u8) {
        let pending = std::mem::take(&mut self.sessions_to_enqueue);
        for handle in pending {
            let Some(session) = self.session_from_handle(handle) else {
                continue;
            };
            if session.session_type == protocol {
                self.enqueue_notify(runtime, handle);
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

    pub fn update_time(&mut self, now: f64, now_us: u64) {
        self.last_time = now;
        self.last_time_us = now_us;
        self.last_event_poll = now;
    }
}

pub struct Session {
    handle: SessionHandle,
    state: AtomicU8,
    session_type: u8,
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
    fn new(handle: SessionHandle, state: SessionState, protocol: u8, opaque: u32) -> Self {
        Self {
            handle,
            state: AtomicU8::new(u8::from(state)),
            session_type: protocol,
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

    #[inline(always)]
    pub fn opaque(&self) -> &u32 {
        &self.opaque
    }

    #[inline(always)]
    pub fn opaque_mut(&mut self) -> &mut u32 {
        &mut self.opaque
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
        self.session_type
    }

    #[inline(always)]
    pub const fn connection_index(&self) -> u32 {
        self.connection_index
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
            session_type: session.transport_protocol(),
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
        let message = SessionEvent::from((event, self.session_handle.session_index));
        producer
            .write(descriptor, &message)
            .expect("Session worker IO ring stores one SessionEvent");
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
    pub rx_event: bool,
    pub rx_ready: bool,
    pub tx_ready: bool,
}

// VPP: session_types.h:476-492, session_event_t. The payload represents one
// union arm selected by the event type and its destination queue.
#[repr(C, packed)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, KnownLayout, FromBytes, Immutable, IntoBytes)]
pub struct SessionEvent {
    pub event_type: u8,
    pub postponed: u8,
    pub payload: [u8; 16],
}

impl SessionEvent {
    #[inline(always)]
    pub(crate) fn session_index(&self) -> u32 {
        u32::from_ne_bytes(
            self.payload[..4]
                .try_into()
                .expect("Session event index arm"),
        )
    }

    #[inline(always)]
    pub(crate) fn session_handle(&self) -> SessionHandle {
        SessionHandle {
            session_index: self.session_index(),
            worker_index: u32::from_ne_bytes(
                self.payload[4..8]
                    .try_into()
                    .expect("Session event handle arm"),
            ),
        }
    }
}

impl From<(SessionEventType, u32)> for SessionEvent {
    #[inline(always)]
    fn from((event_type, session_index): (SessionEventType, u32)) -> Self {
        let mut payload = [0; 16];
        payload[..4].copy_from_slice(&session_index.to_ne_bytes());
        Self {
            event_type: event_type.into(),
            postponed: 0,
            payload,
        }
    }
}

impl From<(SessionEventType, SessionHandle)> for SessionEvent {
    #[inline(always)]
    fn from((event_type, handle): (SessionEventType, SessionHandle)) -> Self {
        let mut event = Self::from((event_type, handle.session_index));
        event.payload[4..8].copy_from_slice(&handle.worker_index.to_ne_bytes());
        event
    }
}

impl From<SessionEvent> for [u64; 2] {
    #[inline(always)]
    fn from(event: SessionEvent) -> Self {
        [
            u64::from_ne_bytes(event.payload[..8].try_into().expect("first union word")),
            u64::from_ne_bytes(event.payload[8..].try_into().expect("second union word")),
        ]
    }
}

#[cfg(test)]
mod event_tests {
    use super::{SessionEvent, SessionEventType, SessionHandle};

    #[test]
    fn session_event_matches_vpp_record_size() {
        assert_eq!(std::mem::size_of::<SessionEvent>(), 18);
    }

    #[test]
    fn io_event_stores_session_index() {
        let session_index = 0x1234_5678;
        let event = SessionEvent::from((SessionEventType::Rx, session_index));
        assert_eq!(event.session_index(), session_index);
        assert_eq!(event.payload[..4], session_index.to_ne_bytes());
        assert_eq!(event.payload[4..], [0; 12]);
    }

    #[test]
    fn control_event_stores_session_index_before_worker_index() {
        let handle = SessionHandle {
            worker_index: 0x1122_3344,
            session_index: 0x5566_7788,
        };
        let event = SessionEvent::from((SessionEventType::Reset, handle));
        assert_eq!(event.session_handle(), handle);
        assert_eq!(event.payload[..4], handle.session_index.to_ne_bytes());
        assert_eq!(event.payload[4..8], handle.worker_index.to_ne_bytes());
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
    listening_sessions: UnsafeCell<Pool<Session>>,
    session_tx_modes: UnsafeCell<Vec<TransportTxMode>>,
    session_type_to_next: UnsafeCell<Vec<u32>>,
    transport_io: UnsafeCell<Vec<Option<SessionIoDispatch>>>,
    transport_time: UnsafeCell<Vec<Option<SessionTimeDispatch>>>,
    transport_cl_thread: u32,
    pool_reallocation: SpinLock<PoolReallocationState>,
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
            listening_sessions: UnsafeCell::new(Pool::new()),
            session_tx_modes: UnsafeCell::new(Vec::new()),
            session_type_to_next: UnsafeCell::new(Vec::new()),
            transport_io: UnsafeCell::new(Vec::new()),
            transport_time: UnsafeCell::new(Vec::new()),
            transport_cl_thread: 0,
            pool_reallocation: SpinLock::new(PoolReallocationState {
                workers_at_barrier: 0,
                workers_doing_work: 0,
            }),
            worker_mq_segment,
            last_transport_protocol: AtomicU8::new(0),
            is_enabled: AtomicBool::new(config.session_enable_asap),
            is_initialized: true,
        };
        main.allocate_event_queues()?;
        Ok(main)
    }

    pub fn allocate_event_queues(&mut self) -> Result<(), SessionError> {
        if self.config.event_ring_capacity == 0 || self.config.configured_worker_mq_length == 0 {
            return Err(SessionError::Invalid);
        }
        let ring = [SvmMsgQRingConfig::new(
            self.config.event_ring_capacity,
            size_of::<SessionEvent>() as u32,
        )];
        let config = SvmMsgQConfig {
            consumer_pid: std::process::id() as i32,
            q_nitems: self.config.configured_worker_mq_length,
            rings: &ring,
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
    fn transport_tx_mode(&self, protocol: u8) -> Option<TransportTxMode> {
        unsafe { &*self.session_tx_modes.get() }
            .get(usize::from(protocol.checked_sub(1)?))
            .copied()
    }

    #[inline(always)]
    pub(crate) fn now(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn register_transport_type(
        &self,
        tx_mode: TransportTxMode,
        output_next: u32,
    ) -> Result<u8, SessionError> {
        if hammer_runtime::ensure_main_thread_with_barrier().is_err() {
            return Err(SessionError::Invalid);
        }
        let previous = self.last_transport_protocol.load(Ordering::Acquire);
        let protocol = previous.checked_add(1).ok_or(SessionError::Invalid)?;
        // SAFETY: registration is Main Thread work performed before worker
        // launch or while WorkerBarrier excludes readers.
        unsafe { &mut *self.session_tx_modes.get() }.push(tx_mode);
        unsafe { &mut *self.session_type_to_next.get() }.push(output_next);
        unsafe { &mut *self.transport_io.get() }.push(None);
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

    #[inline(always)]
    fn transport_io(&self, protocol: u8) -> Option<SessionIoDispatch> {
        unsafe { &*self.transport_io.get() }
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
        protocol: u8,
        opaque: u32,
    ) -> Result<SessionHandle, SessionError> {
        let worker = self
            .workers
            .get_mut(worker_index as usize)
            .ok_or(SessionError::Invalid)?
            .get_mut();
        Ok(worker.allocate(state, protocol, opaque))
    }

    /// VPP: `listen_session_alloc`, session.h:1044-1052. Hammer's Main Thread
    /// does not run a Data Worker, so its listening pool is separate from the
    /// Data Worker slots and owns no FIFO pair.
    pub fn allocate_listening_session(
        &self,
        session_type: u8,
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
        entry.connection_index = SESSION_INDEX_INVALID;
        entry.store_state(SessionState::TransportDeleted);
        Ok(())
    }

    /// VPP: `session_send_evt_to_thread`, session.c:28-86 (static inline).
    #[inline]
    pub fn enqueue_event(
        &self,
        worker_index: u32,
        event: SessionEvent,
    ) -> Result<SessionEventEnqueue, SessionError> {
        let Some(queue) = self.event_queue(worker_index) else {
            return Err(SessionError::Invalid);
        };
        let mut producer = match queue.producer(SvmQueueConditionalWait::Nowait) {
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
        producer
            .write(descriptor, &event)
            .expect("Session worker IO ring stores one SessionEvent");
        match producer.add(descriptor) {
            Ok(()) => Ok(SessionEventEnqueue::Enqueued),
            Err(
                SvmMsgQError::SignalAfterCommit { .. }
                | SvmMsgQError::EventSignalAfterCommit { .. },
            ) => Ok(SessionEventEnqueue::Enqueued),
            Err(source) => panic!("locked Session worker MQ rejected allocated event: {source}"),
        }
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
        SessionEvent::from((event, handle.session_index)),
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
        SessionEvent::from((event, handle.session_index)),
    )
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

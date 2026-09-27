use std::cell::UnsafeCell;
use std::mem::size_of;
use std::num::NonZeroU32;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

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

use super::error::SessionError;
use super::app::ApplicationMain;
use super::segment_manager::{SegmentManager, SegmentManagerError, SegmentManagerMain};
use crate::transport::{TransportSendParams, TransportTxMode};

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
    pub fn session(&self, session_index: u32) -> Option<&Session<O>> {
        self.sessions.get(session_index)
    }

    #[inline(always)]
    pub fn session_mut(&mut self, session_index: u32) -> Option<&mut Session<O>> {
        self.sessions.get_mut(session_index)
    }

    #[inline(always)]
    pub const fn worker_index(&self) -> u32 {
        self.worker_index
    }

    /// VPP: `session_wrk_program_app_wrk_evts`, session.c:565-576.
    #[inline]
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

    pub fn allocate(&mut self, state: SessionState, protocol: u8, opaque: O) -> SessionHandle {
        let handle = SessionHandle {
            worker_index: self.worker_index,
            session_index: SESSION_INDEX_INVALID,
        };
        let session_index = self
            .sessions
            .insert(Session::new(handle, state, protocol, opaque));
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
        opaque: O,
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
        entry.rx_fifo = Some(rx_fifo);
        entry.tx_fifo = Some(tx_fifo);
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

    pub fn handle_event(&mut self, queue: &SvmMsgQ) -> Result<(), SessionError> {
        loop {
            let descriptor = match queue.sub(SvmQueueConditionalWait::Nowait) {
                Ok(descriptor) => descriptor,
                Err(SvmMsgQError::Empty) => return Ok(()),
                Err(source) => return Err(SessionError::MessageQueueAllocation { source }),
            };
            let event = match queue.read::<SessionEventRecord>(descriptor) {
                Ok(event) => SessionEvent::from(event),
                Err(_) => {
                    drop(queue.free_msg(descriptor));
                    return Err(SessionError::Invalid);
                }
            };
            if let Err(source) = queue.free_msg(descriptor) {
                return Err(SessionError::MessageQueueAllocation { source });
            }
            self.consume_event(event)?;
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

    #[inline(always)]
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
        self.pending_io_sessions.push(handle);
        Ok(())
    }

    /// VPP: `session_enqueue_notify`, session.c:627-647. The Session owns
    /// RX event coalescing; transport code never dispatches app callbacks.
    #[inline]
    pub fn enqueue_notify(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
    ) -> Option<()> {
        let session = self.session_from_handle(handle)?;
        let app_worker_index = session.application_worker()?;
        let protocol = session.transport_protocol();
        let rx_fifo = session
            .rx_fifo()
            .expect("an attached Session retains its RX FIFO");
        if !rx_fifo.set_event() {
            return Some(());
        }
        let application = ApplicationMain::global()
            .expect("Application Main initializes before Session RX notification");
        // SAFETY: this Session worker exclusively executes its AppWorker
        // event slot; detach waits for WorkerBarrier before removing it.
        let app_worker = unsafe { application.worker(app_worker_index) }
            .expect("an attached Session retains its AppWorker");
        app_worker.add_event(
            runtime,
            self,
            SessionEvent {
                event_type: u8::from(SessionEventType::Rx),
                postponed: false,
                protocol,
                operation: 0,
                session: handle,
                control_data_index: SESSION_INDEX_INVALID,
                payload: [0; 2],
            },
        );
        Some(())
    }

    /// VPP: `session_transport_closing_notify`, session.c:958-980.
    pub fn transport_closing(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        let session = self.session_from_handle(handle).ok_or(SessionError::NoSession)?;
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
            self.queue_application_event(runtime, handle, SessionEventType::Disconnected, 0);
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
        let session = self.session_from_handle(handle).ok_or(SessionError::NoSession)?;
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
            self.queue_application_event(runtime, handle, SessionEventType::Reset, 0);
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
        let session = self.session_from_handle(handle).ok_or(SessionError::NoSession)?;
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
        self.queue_application_event(runtime, handle, SessionEventType::TransportClosed, 0);
        Ok(())
    }

    fn queue_application_event(
        &mut self,
        runtime: &DataPlaneMain,
        handle: SessionHandle,
        event_type: SessionEventType,
        operation: u8,
    ) {
        let session = self
            .session_from_handle(handle)
            .expect("Application notification retains its Session");
        let Some(app_worker_index) = session.application_worker() else {
            return;
        };
        let protocol = session.transport_protocol();
        let application = ApplicationMain::global()
            .expect("Application Main remains initialized while Session is attached");
        // SAFETY: this Session worker exclusively executes its AppWorker event
        // slot; detach waits for WorkerBarrier before removing the AppWorker.
        let app_worker = unsafe { application.worker(app_worker_index) }
            .expect("attached Session retains its AppWorker");
        app_worker.add_event(
            runtime,
            self,
            SessionEvent {
                event_type: u8::from(event_type),
                postponed: false,
                protocol,
                operation,
                session: handle,
                control_data_index: SESSION_INDEX_INVALID,
                payload: [0; 2],
            },
        );
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

    pub fn consume_event(&mut self, event: SessionEvent) -> Result<(), SessionError> {
        if event.session.worker_index != self.worker_index
            || self.session(event.session.session_index).is_none()
        {
            return Err(SessionError::NoSession);
        }
        self.pending_io_sessions.push(event.session);
        Ok(())
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

pub struct Session<O = u32> {
    handle: SessionHandle,
    state: AtomicU8,
    session_type: u8,
    flags: SessionFlags,
    rx_fifo: Option<SvmFifo>,
    tx_fifo: Option<SvmFifo>,
    application_worker: u32,
    connection_index: u32,
    application_listener: u32,
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
    pub(crate) fn set_rx_ready(&mut self, ready: bool) {
        self.flags.rx_ready = ready;
    }

    #[inline(always)]
    pub(crate) fn detach_application(&mut self) {
        self.flags.rx_ready = false;
        self.application_worker = SESSION_INDEX_INVALID;
    }
}

impl<O> Drop for Session<O> {
    fn drop(&mut self) {
        match (self.rx_fifo.as_ref(), self.tx_fifo.as_ref()) {
            (Some(rx), Some(tx)) => {
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
            _ => panic!("Session {:?} owns only one half of its FIFO pair", self.handle),
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

#[repr(C)]
#[derive(Clone, Copy, KnownLayout, FromBytes, Immutable, IntoBytes)]
pub(crate) struct SessionEventRecord {
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

pub struct SessionMain<O = u32> {
    config: SessionConfig,
    workers: Vec<UnsafeCell<SessionWorker<O>>>,
    listening_sessions: UnsafeCell<Pool<Session<O>>>,
    session_tx_modes: UnsafeCell<Vec<TransportTxMode>>,
    session_type_to_next: UnsafeCell<Vec<u32>>,
    transport_cl_thread: u32,
    pool_reallocation: SpinLock<PoolReallocationState>,
    worker_mq_segment: SvmFifoSegment,
    last_transport_protocol: AtomicU8,
    is_enabled: AtomicBool,
    is_initialized: bool,
}

static SESSION_MAIN: OnceLock<SessionMain<u32>> = OnceLock::new();

impl SessionMain<u32> {
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
unsafe impl<O: Send> Sync for SessionMain<O> {}

impl<O> SessionMain<O> {
    fn new(config: SessionConfig, worker_mq_segment: SvmFifoSegment) -> Result<Self, SessionError> {
        let mut workers = Vec::with_capacity(config.worker_count as usize);
        for worker_index in 0..config.worker_count {
            workers.push(UnsafeCell::new(SessionWorker::new(worker_index, config)));
        }
        let mut main = Self {
            config,
            workers,
            listening_sessions: UnsafeCell::new(Pool::new()),
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
            size_of::<SessionEventRecord>() as u32,
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
                return Err(SessionError::MessageQueueAllocation { source });
            }
        }
        Ok(())
    }

    #[inline(always)]
    pub fn worker(&mut self, worker_index: u32) -> Option<&SessionWorker<O>> {
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
    ) -> Result<&'worker mut SessionWorker<O>, SessionError> {
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
        self.last_transport_protocol
            .store(protocol, Ordering::Release);
        Ok(protocol)
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
        opaque: O,
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
        opaque: O,
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
        let descriptor = match producer.alloc_msg(size_of::<SessionEventRecord>()) {
            Ok(descriptor) => descriptor,
            Err(SvmMsgQError::QueueFull | SvmMsgQError::RingFull { .. }) => {
                return Ok(SessionEventEnqueue::Full);
            }
            Err(source) => return Err(SessionError::MessageQueueAllocation { source }),
        };
        let record = SessionEventRecord::from(event);
        if let Err(source) = producer.write(descriptor, &record) {
            debug_assert!(queue.free_msg(descriptor).is_ok());
            return Err(SessionError::MessageQueueAllocation { source });
        }
        match producer.add(descriptor) {
            Ok(()) => Ok(SessionEventEnqueue::Enqueued),
            Err(
                SvmMsgQError::SignalAfterCommit { .. }
                | SvmMsgQError::EventSignalAfterCommit { .. },
            ) => Err(SessionError::Unknown),
            Err(SvmMsgQError::QueueFull) => {
                debug_assert!(queue.free_msg(descriptor).is_ok());
                Ok(SessionEventEnqueue::Full)
            }
            Err(source) => {
                debug_assert!(queue.free_msg(descriptor).is_ok());
                Err(SessionError::MessageQueueAllocation { source })
            }
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

    pub fn flush_enqueue_events(
        &mut self,
        protocol: u8,
        worker_index: u32,
    ) -> Result<(), SessionError> {
        let Some(worker) = self
            .workers
            .get_mut(worker_index as usize)
            .map(UnsafeCell::get_mut)
        else {
            return Err(SessionError::Invalid);
        };
        let sessions = &worker.sessions;
        worker.pending_io_sessions.retain(|handle| {
            sessions
                .get(handle.session_index)
                .is_some_and(|session| session.session_type != protocol)
        });
        Ok(())
    }
}

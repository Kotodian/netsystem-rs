use std::num::NonZeroU32;
use std::os::fd::BorrowedFd;
use std::sync::Arc;
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, DataPlaneBuffers, Index as BufferIndex};
use hammer_infra::align::CacheLine;
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::segment::Segment;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{
    AppSessionConfig, SessionEventQueue, SessionEvtType, SessionHandle, SessionMsgQueue,
};
use hammer_runtime::attach::AppSessionPublisher;
use hammer_runtime::{AttachError, RuntimeError, RuntimeResult};
use hammer_runtime::{DataPlaneRuntime, DataWorkerId, Engine, File, FileFunctions};

use crate::session::app::{AppWorkerAttach, AppWorkerError};
use crate::session::error::{SessionError, SessionQueueError};
use crate::session::state::SessionState;
use crate::session::{AppWorker, SessionId, SessionQueueNext};

const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;
const DEFAULT_SESSION_TX_EVENT_CAPACITY: usize = 2048;
const DEFAULT_TX_DISPATCH_BUDGET: usize = 64;

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
    Disconnect(SessionId),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SessionEntry<Index> {
    transport: SessionTransportId,
    state: SessionState<Index>,
    schedule_pending: bool,
}

impl<Index> SessionEntry<Index> {
    #[inline]
    const fn creating(transport: SessionTransportId) -> Self {
        Self {
            transport,
            state: SessionState::creating(),
            schedule_pending: false,
        }
    }
}

pub struct SessionWorker<Index> {
    worker: DataWorkerId,
    entries: Pool<SessionEntry<Index>>,
    app: AppWorker,
    app_session_config: AppSessionConfig,
    session_work: Vec<SessionId>,
    session_work_scratch: Vec<SessionId>,
    control_events: FifoQueue<SessionControlEvent>,
    readiness_file: Option<PoolIndex>,
}

pub struct SessionMain {
    workers: Box<[CacheLine<ThreadOwned<SessionWorker<PoolIndex>>>]>,
}

fn session_worker_error(error: SessionQueueError) -> RuntimeError {
    RuntimeError::subsystem("session", error)
}

impl SessionMain {
    pub fn new(worker_count: usize) -> Self {
        let workers = (0..worker_count)
            .map(|_| CacheLine::new(ThreadOwned::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { workers }
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
}

pub fn install_session_worker(
    main: &SessionMain,
    engine: &mut Engine,
    session_queue: hammer_core::data_plane::NodeId,
    mut worker: SessionWorker<PoolIndex>,
) -> RuntimeResult<()> {
    worker.install_queue_readiness(engine, session_queue)?;
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
    pub(crate) fn local_app_mut(&mut self) -> &mut AppWorker {
        &mut self.app
    }

    /// Registers the session queue signal with the worker FileMain.
    ///
    /// The queue retains its original endpoint. FileMain owns a duplicated
    /// descriptor and must remove it before this worker is replaced.
    pub fn install_queue_readiness(
        &mut self,
        engine: &mut Engine,
        session_queue: hammer_core::data_plane::NodeId,
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
            u64::from(session_queue.slot()),
            FileFunctions {
                read: Some(schedule_session_queue),
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
        Some((entry.transport, entry.state.transport_index()?))
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
        listener: u32,
    ) -> RuntimeResult<SessionId> {
        let session_id = self.insert_creating_session(transport)?;
        let handle = SessionHandle::new(session_id.pool_index().slot(), self.worker.slot() as u32);
        let tx_evt_q = self.app.tx_evt_q().clone();
        let app_session = match self.app.create_app_session(
            listener,
            handle,
            self.app_session_config,
            tx_evt_q,
        ) {
            Ok(session) => session,
            Err(error) => {
                self.remove_session_entry(session_id);
                return Err(error);
            }
        };
        self.app.attach_session(session_id, app_session);
        if let Err(error) = self.finish_session_creation(session_id, index) {
            self.remove_session_entry(session_id);
            return Err(error);
        }
        Ok(session_id)
    }

    fn insert_creating_session(
        &mut self,
        transport: SessionTransportId,
    ) -> RuntimeResult<SessionId> {
        self.entries
            .insert_with(|index| {
                let _ = index;
                SessionEntry::creating(transport)
            })
            .map(SessionId::from)
            .ok_or_else(|| {
                SessionError::CapacityExhausted {
                    capacity: self.entries.capacity(),
                }
                .into()
            })
    }

    fn finish_session_creation(
        &mut self,
        session_id: SessionId,
        index: Index,
    ) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .ok_or(SessionError::SessionMissing { session_id })?;
        entry.state = entry
            .state
            .finish_creation(index)
            .ok_or(SessionError::NotCreating { session_id })?;
        Ok(())
    }

    pub fn connection_published(&mut self, session_id: SessionId) -> RuntimeResult<bool> {
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .ok_or(SessionError::SessionMissing { session_id })?;
        let (state, initial) = entry
            .state
            .on_connection_published()
            .ok_or(SessionError::PublicationRejected { session_id })?;
        entry.state = state;
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
        let index = entry
            .state
            .rollback_index()
            .map_err(|_| SessionError::RollbackRejected { session_id })?;
        self.remove_session_entry(session_id);
        Ok(index)
    }

    pub fn insert_session_for_test(
        &mut self,
        transport: SessionTransportId,
        index: Index,
    ) -> SessionId {
        let session_id = self
            .insert_creating_session(transport)
            .expect("session pool capacity exhausted");
        self.finish_session_creation(session_id, index)
            .expect("finish session creation");
        self.connection_published(session_id)
            .expect("publish session connection");
        self.connected(session_id).expect("connect session");
        session_id
    }

    fn remove_session_entry(&mut self, session_id: SessionId) -> bool {
        self.app.discard_all_tx_bytes_for_session(session_id);
        let _ = self.app.detach_session(session_id);
        self.entries.remove(session_id.pool_index()).is_some()
    }

    pub fn notify_transport_closed(
        &mut self,
        session_id: SessionId,
        index: Index,
    ) -> RuntimeResult<()> {
        let notify_app = self
            .entries
            .get_mut(session_id.pool_index())
            .is_some_and(|entry| entry.state.on_transport_close(index));
        if notify_app {
            self.app.closed(session_id)?;
        }
        Ok(())
    }

    pub fn notify_transport_deleted(&mut self, session_id: SessionId, index: Index) {
        let remove = self
            .entries
            .get_mut(session_id.pool_index())
            .is_some_and(|entry| entry.state.on_transport_deleted(index));
        if remove {
            self.remove_session_entry(session_id);
        }
    }

    fn notify_app_closed(&mut self, session_id: SessionId) -> bool {
        let remove = self
            .entries
            .get_mut(session_id.pool_index())
            .is_some_and(|entry| entry.state.on_app_close());
        if remove {
            self.remove_session_entry(session_id);
        }
        remove
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
            .push_back(SessionControlEvent::Disconnect(session_id));
    }

    pub fn poll_app(&mut self) -> RuntimeResult<()> {
        let entries = &mut self.entries;
        let session_work = &mut self.session_work;
        let control_events = &mut self.control_events;
        self.app
            .drain_tx_events_to(|session_id, evt_type| match evt_type {
                SessionEvtType::TxDeq => {
                    if let Some(entry) = entries.get_mut(session_id.pool_index())
                        && !entry.schedule_pending
                    {
                        entry.schedule_pending = true;
                        session_work.push(session_id);
                    }
                }
                SessionEvtType::Close => {
                    control_events.push_back(SessionControlEvent::Disconnect(session_id));
                }
                SessionEvtType::RxEnq | SessionEvtType::Connect => {}
            });
        Ok(())
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
        self.app
            .discard_acked_tx_bytes(session_id, bytes)
            .map(|_| ())
    }

    pub fn enqueue_rx(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        index: BufferIndex,
        offset: u32,
        urgent: bool,
    ) -> RuntimeResult<RxDelivery> {
        if offset == 0 {
            let (accepted, promoted) = self
                .app
                .copy_rx_from_buffer(session_id, buffers, index, urgent)?;
            let rx_available = self.rx_available_u32(session_id);
            return Ok(match NonZeroU32::new(accepted) {
                Some(accepted) => RxDelivery::InOrder {
                    accepted,
                    promoted,
                    rx_available,
                },
                None => RxDelivery::NotAccepted { rx_available },
            });
        }
        let (accepted, newest) = self
            .app
            .copy_rx_from_buffer_ooo(session_id, buffers, index, offset)?;
        let rx_available = self.rx_available_u32(session_id);
        Ok(match NonZeroU32::new(accepted) {
            Some(accepted) => {
                let (start, len) =
                    newest.ok_or(SessionError::OooSpanMissing { session_id })?;
                let len = NonZeroU32::new(len)
                    .ok_or(SessionError::OooSpanInvalid { session_id })?;
                RxDelivery::OutOfOrder {
                    accepted,
                    newest: OooSpan::new(start, len),
                    rx_available,
                }
            }
            None => RxDelivery::NotAccepted { rx_available },
        })
    }

    #[inline]
    pub fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        self.app.rx_available_len(session_id)
    }

    #[inline]
    pub fn pending_send_len(&self, session_id: SessionId) -> RuntimeResult<Option<usize>> {
        self.app.pending_send_len(session_id)
    }

    #[inline]
    pub fn has_pending_send(&self, session_id: SessionId) -> bool {
        self.pending_send_len(session_id).ok().flatten().is_some()
    }

    pub fn connected(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        let Self { entries, app, .. } = self;
        let entry = entries
            .get_mut(session_id.pool_index())
            .ok_or(SessionError::SessionMissing { session_id })?;
        let connected = entry
            .state
            .on_connected()
            .ok_or(SessionError::NotPublished { session_id })?;
        app.connected(session_id)?;
        entry.state = connected;
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
        self.app
            .copy_tx_to_buffer(buffers, session_id, offset, len, index)
    }

    #[inline]
    fn rx_available_u32(&self, session_id: SessionId) -> u32 {
        self.rx_available_len(session_id)
            .map(|value| value.min(u32::MAX as usize) as u32)
            .unwrap_or(0)
    }
}

impl<Index: Copy + Eq> SessionWorker<Index> {
    pub fn new(worker: DataWorkerId) -> RuntimeResult<Self> {
        Self::with_app_session_config(worker, AppSessionConfig::default())
    }

    pub fn with_app_session_config(
        worker: DataWorkerId,
        app_session_config: AppSessionConfig,
    ) -> RuntimeResult<Self> {
        let cap = DEFAULT_SESSION_TX_EVENT_CAPACITY.next_power_of_two().max(2) as u32;
        let layout = SessionMsgQueue::layout_bytes(cap, cap.max(2))
            .map_err(|error| AppWorkerError::TxEventQueue { error })?;
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
                .map_err(|error| AppWorkerError::TxEventQueue { error })?,
        );
        let app = AppWorker::new(DEFAULT_SESSION_POOL_CAPACITY, tx_evt_q, worker.slot());
        Ok(Self::from_app(worker, app_session_config, app))
    }

    pub(crate) fn with_app_session_attach(
        worker: DataWorkerId,
        app_session_config: AppSessionConfig,
        publisher: AppSessionPublisher,
    ) -> RuntimeResult<Self> {
        let cap = DEFAULT_SESSION_TX_EVENT_CAPACITY.next_power_of_two().max(2) as u32;
        let layout = SessionMsgQueue::layout_bytes(cap, cap.max(2))
            .map_err(|error| AppWorkerError::TxEventQueue { error })?;
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
            .map_err(|error| AppWorkerError::TxEventQueue { error })?,
        );
        let app = AppWorker::with_attach(
            DEFAULT_SESSION_POOL_CAPACITY,
            tx_evt_q,
            worker.slot(),
            AppWorkerAttach::new(publisher, segment, offset),
        );
        Ok(Self::from_app(worker, app_session_config, app))
    }

    fn from_app(
        worker: DataWorkerId,
        app_session_config: AppSessionConfig,
        app: AppWorker,
    ) -> Self {
        Self {
            worker,
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            app,
            app_session_config,
            session_work: Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            session_work_scratch: Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
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
        _: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        frame: &mut BufferFrame,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        transport.internal_tx(sessions, index, runtime, output_next, frame, output, now)
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
    sessions.poll_app()?;

    let control_count = sessions.control_events.len();
    for _ in 0..control_count {
        let Some(SessionControlEvent::Disconnect(session_id)) = sessions.control_events.pop_front()
        else {
            break;
        };
        let session_transport = sessions.session_transport(session_id);
        sessions.notify_app_closed(session_id);
        if let Some((transport_id, index)) = session_transport
            && transport_id == T::ID
        {
            transport.disconnect(sessions, index, runtime, output_next, frame, output, now)?;
        }
    }

    let work = sessions.take_scheduled_work();
    let scheduled_sessions = work.len();
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

fn schedule_session_queue(
    graph: &hammer_runtime::NodeRuntime,
    file: &mut File,
) -> RuntimeResult<()> {
    let node = hammer_core::data_plane::NodeId::new(file.private_data() as u32);
    let _ = graph.mark_interrupt_pending(node)?;
    Ok(())
}

// TCP-coupled driver tests live in hammer-plugin-tcp (session_driver_tests).
// #[cfg(test)]
// #[path = "runtime/tests.rs"]
// mod tests;

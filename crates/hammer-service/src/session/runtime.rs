use std::num::NonZeroU32;
use std::os::fd::BorrowedFd;
use std::sync::Arc;
use std::time::Instant;

use hammer_core::data_plane::{BufferFrame, DataPlaneBuffers, Index};
use hammer_infra::align::CacheLine;
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::segment::{Local, Segment, Svm};
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{
    AppSessionConfig, SessionEventQueue, SessionEvtType, SessionHandle, SessionMsgQueue,
    with_current_app_worker,
};
use hammer_runtime::{AttachError, RuntimeError, RuntimeResult};
use hammer_runtime::{DataPlaneRuntime, DataWorkerId, Engine, File, FileFunctions};

use crate::session::app::SessionAppRuntimeCreate;
use crate::session::config::SessionBackend;
use crate::session::error::SessionQueueError;
use crate::session::state::SessionState;
use crate::session::{SessionAppRuntime, SessionId, SessionQueueNext};

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
            state: SessionState::TransportDeleted,
            schedule_pending: false,
        }
    }
}

enum SessionApp {
    Local(SessionAppRuntime<Local>),
    Svm(SessionAppRuntime<Svm>),
}

impl SessionApp {
    fn signal_read_fd(&self) -> Option<std::os::fd::RawFd> {
        match self {
            Self::Local(app) => app.tx_evt_q().read_fd(),
            Self::Svm(app) => app.tx_evt_q().read_fd(),
        }
    }

    fn signal(&self) {
        match self {
            Self::Local(app) => app.tx_evt_q().fire(),
            Self::Svm(app) => app.tx_evt_q().fire(),
        }
    }

    fn create_session(
        &mut self,
        session_id: SessionId,
        handle: SessionHandle,
        config: AppSessionConfig,
    ) -> RuntimeResult<()> {
        match self {
            Self::Local(app) => {
                let session = app.create_app_session(handle, config, app.tx_evt_q().clone())?;
                app.attach_session(session_id, session);
            }
            Self::Svm(app) => {
                let session = app.create_app_session(handle, config, app.tx_evt_q().clone())?;
                app.attach_session(session_id, session);
            }
        }
        Ok(())
    }

    fn detach_session(&mut self, session_id: SessionId) {
        match self {
            Self::Local(app) => {
                app.discard_all_tx_bytes_for_session(session_id);
                let _ = app.detach_session(session_id);
            }
            Self::Svm(app) => {
                app.discard_all_tx_bytes_for_session(session_id);
                let _ = app.detach_session(session_id);
            }
        }
    }

    fn connected(&self, session_id: SessionId) -> RuntimeResult<()> {
        match self {
            Self::Local(app) => app.connected(session_id),
            Self::Svm(app) => app.connected(session_id),
        }
    }

    fn closed(&self, session_id: SessionId) -> RuntimeResult<()> {
        match self {
            Self::Local(app) => app.closed(session_id),
            Self::Svm(app) => app.closed(session_id),
        }
    }

    fn discard_acked_tx_bytes(&mut self, session_id: SessionId, len: usize) -> RuntimeResult<()> {
        match self {
            Self::Local(app) => {
                let _ = app.discard_acked_tx_bytes(session_id, len)?;
            }
            Self::Svm(app) => {
                let _ = app.discard_acked_tx_bytes(session_id, len)?;
            }
        }
        Ok(())
    }

    fn pending_send_len(&self, session_id: SessionId) -> RuntimeResult<Option<usize>> {
        match self {
            Self::Local(app) => app.pending_send_len(session_id),
            Self::Svm(app) => app.pending_send_len(session_id),
        }
    }

    fn copy_tx_to_buffer(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        offset: usize,
        len: usize,
        index: Index,
    ) -> RuntimeResult<()> {
        match self {
            Self::Local(app) => app.copy_tx_to_buffer(buffers, session_id, offset, len, index),
            Self::Svm(app) => app.copy_tx_to_buffer(buffers, session_id, offset, len, index),
        }
    }

    fn copy_rx_from_buffer(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: Index,
        urgent: bool,
    ) -> RuntimeResult<(u32, u32)> {
        match self {
            Self::Local(app) => app.copy_rx_from_buffer(session_id, buffers, index, urgent),
            Self::Svm(app) => app.copy_rx_from_buffer(session_id, buffers, index, urgent),
        }
    }

    fn copy_rx_from_buffer_ooo(
        &self,
        session_id: SessionId,
        buffers: &DataPlaneBuffers,
        index: Index,
        offset: u32,
    ) -> RuntimeResult<(u32, Option<(u32, u32)>)> {
        match self {
            Self::Local(app) => app.copy_rx_from_buffer_ooo(session_id, buffers, index, offset),
            Self::Svm(app) => app.copy_rx_from_buffer_ooo(session_id, buffers, index, offset),
        }
    }

    fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        match self {
            Self::Local(app) => app.rx_available_len(session_id),
            Self::Svm(app) => app.rx_available_len(session_id),
        }
    }

    fn drain_tx_events_to(&self, dispatch: impl FnMut(SessionId, SessionEvtType)) -> usize {
        match self {
            Self::Local(app) => app.drain_tx_events_to(dispatch),
            Self::Svm(app) => app.drain_tx_events_to(dispatch),
        }
    }
}

pub struct SessionWorker<Index> {
    worker: DataWorkerId,
    entries: Pool<SessionEntry<Index>>,
    app: SessionApp,
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
    fn with_app_session_config(
        worker: DataWorkerId,
        app_session_config: AppSessionConfig,
        app: SessionApp,
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
        self.app.signal_read_fd()
    }

    #[inline]
    pub fn signal_queue(&self) {
        self.app.signal();
    }

    #[cfg(test)]
    pub(crate) fn local_app(&self) -> &SessionAppRuntime<Local> {
        let SessionApp::Local(app) = &self.app else {
            panic!("local Session app required by test");
        };
        app
    }

    #[cfg(test)]
    pub(crate) fn local_app_mut(&mut self) -> &mut SessionAppRuntime<Local> {
        let SessionApp::Local(app) = &mut self.app else {
            panic!("local Session app required by test");
        };
        app
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
        let Some(signal_read_fd) = self.app.signal_read_fd() else {
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
            "SVM session queue app-to-dataplane signal".to_owned(),
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

    pub fn insert_creating_session(
        &mut self,
        transport: SessionTransportId,
    ) -> RuntimeResult<SessionId> {
        self.entries
            .insert_with(|index| {
                let _ = index;
                SessionEntry::creating(transport)
            })
            .map(SessionId::from)
            .ok_or_else(|| RuntimeError::invariant("session pool capacity exhausted"))
    }

    pub fn create_app_session(&mut self, session_id: SessionId) -> RuntimeResult<()> {
        let handle = SessionHandle::new(session_id.pool_index().slot(), self.worker.slot() as u32);
        self.app
            .create_session(session_id, handle, self.app_session_config)
    }

    pub fn finish_session_creation(
        &mut self,
        session_id: SessionId,
        index: Index,
    ) -> RuntimeResult<()> {
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .ok_or_else(|| RuntimeError::invariant("session is missing"))?;
        entry.state = SessionState::active(index);
        Ok(())
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
        session_id
    }

    pub fn remove_session_entry(&mut self, session_id: SessionId) -> bool {
        self.app.detach_session(session_id);
        let handle = SessionHandle::new(session_id.pool_index().slot(), self.worker.slot() as u32);
        with_current_app_worker(self.worker.slot(), |worker| {
            let _ = worker.detach_session(handle);
        });
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
        self.app.discard_acked_tx_bytes(session_id, bytes)
    }

    pub fn enqueue_rx(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        index: hammer_core::data_plane::Index,
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
                let (start, len) = newest.ok_or_else(|| {
                    RuntimeError::invariant("accepted OOO delivery must report a retained span")
                })?;
                let len = NonZeroU32::new(len).ok_or_else(|| {
                    RuntimeError::invariant(
                        "accepted OOO delivery must report non-zero span length",
                    )
                })?;
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

    #[inline]
    pub fn connected(&self, session_id: SessionId) -> RuntimeResult<()> {
        self.app.connected(session_id)
    }

    pub fn copy_tx_to_buffer(
        &self,
        buffers: &DataPlaneBuffers,
        session_id: SessionId,
        offset: usize,
        len: usize,
        index: Index,
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
    pub fn new(worker: DataWorkerId) -> Self {
        Self::with_session_config(worker, SessionBackend::Local, AppSessionConfig::default())
    }

    pub fn with_session_config(
        worker: DataWorkerId,
        backend: SessionBackend,
        app_session_config: AppSessionConfig,
    ) -> Self {
        match backend {
            SessionBackend::Local => Self::new_local(worker, app_session_config),
            SessionBackend::Svm => Self::new_svm(worker, app_session_config),
        }
    }

    fn new_local(worker: DataWorkerId, app_session_config: AppSessionConfig) -> Self {
        let cap = DEFAULT_SESSION_TX_EVENT_CAPACITY.next_power_of_two().max(2) as u32;
        let tx_evt_q =
            Arc::new(SessionMsgQueue::with_cfg(cap, cap.max(2)).expect("local tx_evt_q"));
        let app = SessionAppRuntime::new(
            DEFAULT_SESSION_POOL_CAPACITY,
            tx_evt_q,
            worker.slot(),
            Local::default(),
        );
        Self::with_app_session_config(worker, app_session_config, SessionApp::Local(app))
    }

    fn new_svm(worker: DataWorkerId, app_session_config: AppSessionConfig) -> Self {
        let seg = Svm::default();
        let cap = DEFAULT_SESSION_TX_EVENT_CAPACITY.next_power_of_two().max(2) as u32;
        let layout = SessionMsgQueue::<Svm>::layout_bytes(cap, cap.max(2)).expect("tx layout");
        let off = seg.alloc(layout, 64);
        let tx_evt_q = Arc::new(
            unsafe {
                SessionMsgQueue::<Svm>::init_at_with_signal(seg.clone(), off, cap, cap.max(2))
            }
            .expect("svm tx_evt_q"),
        );
        let app =
            SessionAppRuntime::new(DEFAULT_SESSION_POOL_CAPACITY, tx_evt_q, worker.slot(), seg);
        Self::with_app_session_config(worker, app_session_config, SessionApp::Svm(app))
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
    pub index: Index,
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
        sessions: &mut SessionWorker<Index>,
        index: Index,
        batch: &[TxBatchBuffer],
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
            return Err(RuntimeError::invariant(
                "session tx offset exceeds chain length",
            ));
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
            transport.tx_action(sessions, index, batch.as_slice(), now)?;
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

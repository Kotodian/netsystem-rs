use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;
use hammer_core::data_plane::{BufferIndex, DataPlaneBuffers, Frame, Next};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::msg_queue::{MsgQueue, SessionEvtType};
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::segment::{Local, Segment, Svm};
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::app::{AppContext, AppSessionConfig, SessionHandle, with_current_app_worker};
use hammer_runtime::{DataPlaneRuntime, DataWorkerId, NodeRuntimeData};

use crate::session::app::SessionAppRuntimeCreate;
use crate::session::state::SessionState;
use crate::session::{SessionAppRuntime, SessionId, SessionQueueHandle, SessionQueueNext};

const DEFAULT_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;
const DEFAULT_SESSION_TX_EVENT_CAPACITY: usize = 2048;
const DEFAULT_TX_DISPATCH_BUDGET: usize = 64;
const SESSION_TIMER_KIND_COUNT: usize = u32::BITS as usize;

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
struct ExpiredTimer {
    session_id: SessionId,
    timer_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionControlEvent {
    Disconnect(SessionId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OooSpan {
    start: u32,
    len: NonZeroU32,
}

impl OooSpan {
    #[inline]
    pub(crate) const fn new(start: u32, len: NonZeroU32) -> Self {
        Self { start, len }
    }

    #[inline]
    pub(crate) const fn start(self) -> u32 {
        self.start
    }

    #[inline]
    pub(crate) const fn len(self) -> NonZeroU32 {
        self.len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RxDelivery {
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
    pub(crate) expired_timers: usize,
    pub(crate) scheduled_sessions: usize,
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

pub struct SessionWorker<Index, Seg: Segment = Local> {
    worker: DataWorkerId,
    entries: Pool<SessionEntry<Index>>,
    app: SessionAppRuntime<Seg>,
    app_context: Option<AppContext<Local>>,
    app_session_config: AppSessionConfig,
    buffers: DataPlaneBuffers,
    session_work: hammer_infra::vec::Vec<SessionId>,
    session_work_scratch: hammer_infra::vec::Vec<SessionId>,
    control_events: FifoQueue<SessionControlEvent>,
    timers: TimerWheel1t2w2048sl<u32>,
    expired_timers: hammer_infra::vec::Vec<u32>,
    pending_timers: FifoQueue<ExpiredTimer>,
    timer_tick_duration: Duration,
    last_timer_tick: Instant,
}

impl<Index: Copy + Eq, Seg: Segment> SessionWorker<Index, Seg> {
    fn with_app_session_config(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        app_session_config: AppSessionConfig,
        seg: Seg,
        worker_index: usize,
    ) -> Self {
        let tx_evt_q = Arc::new(
            MsgQueue::<Seg>::new(seg.clone(), DEFAULT_SESSION_TX_EVENT_CAPACITY)
                .map_err(|error| CoreError::internal(format!("tx_evt_q: {error:?}")))
                .expect("tx_evt_q allocation"),
        );
        Self {
            worker,
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            app: SessionAppRuntime::new(
                DEFAULT_SESSION_POOL_CAPACITY,
                buffers.clone(),
                tx_evt_q,
                worker_index,
                seg,
            ),
            app_context: None,
            app_session_config,
            buffers,
            session_work: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            session_work_scratch: hammer_infra::vec::Vec::with_capacity(
                DEFAULT_SESSION_POOL_CAPACITY,
            ),
            control_events: FifoQueue::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            timers: TimerWheel1t2w2048sl::with_timer_ids(0, SESSION_TIMER_KIND_COUNT),
            expired_timers: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            pending_timers: FifoQueue::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            timer_tick_duration: DEFAULT_SESSION_TIMER_TICK,
            last_timer_tick: Instant::now(),
        }
    }

    #[inline]
    pub(crate) const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn app(&self) -> &SessionAppRuntime<Seg> {
        &self.app
    }

    #[inline]
    pub fn app_mut(&mut self) -> &mut SessionAppRuntime<Seg> {
        &mut self.app
    }

    #[inline]
    pub fn buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    #[inline]
    pub(crate) fn timers_mut(&mut self) -> &mut TimerWheel1t2w2048sl<u32> {
        &mut self.timers
    }

    #[inline]
    pub(crate) fn session_transport(
        &self,
        session_id: SessionId,
    ) -> Option<(SessionTransportId, Index)> {
        let entry = self.entries.get(session_id.pool_index())?;
        Some((entry.transport, entry.state.transport_index()?))
    }

    #[inline]
    pub(crate) fn has_session(&self, session_id: SessionId) -> bool {
        self.entries.contains_key(session_id.pool_index())
    }

    #[inline]
    pub(crate) fn prefetch_session(&self, session_id: SessionId) {
        self.entries.prefetch_slot(session_id.pool_index());
    }

    fn insert_creating_session(&mut self, transport: SessionTransportId) -> CoreResult<SessionId> {
        self.entries
            .insert_with(|index| {
                let _ = index;
                SessionEntry::creating(transport)
            })
            .map(SessionId::from)
            .ok_or_else(|| CoreError::internal("session pool capacity exhausted"))
    }

    fn finish_session_creation(&mut self, session_id: SessionId, index: Index) -> CoreResult<()> {
        let entry = self
            .entries
            .get_mut(session_id.pool_index())
            .ok_or_else(|| CoreError::internal("session is missing"))?;
        entry.state = SessionState::active(index);
        Ok(())
    }

    pub(crate) fn insert_session_for_test(
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

    pub(crate) fn remove_session_entry(&mut self, session_id: SessionId) -> bool {
        self.app.discard_all_tx_bytes_for_session(session_id);
        let _ = self.app.detach_session(session_id);
        let handle = SessionHandle::new(session_id.pool_index().slot(), self.worker.slot() as u32);
        with_current_app_worker(self.worker.slot(), |worker| {
            let _ = worker.detach_session(handle);
        });
        self.entries.remove(session_id.pool_index()).is_some()
    }

    pub(crate) fn notify_transport_closed(
        &mut self,
        session_id: SessionId,
        index: Index,
    ) -> CoreResult<()>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let notify_app = self
            .entries
            .get_mut(session_id.pool_index())
            .is_some_and(|entry| entry.state.on_transport_close(index));
        if notify_app {
            self.app.closed(session_id)?;
        }
        Ok(())
    }

    pub(crate) fn notify_transport_deleted(&mut self, session_id: SessionId, index: Index) {
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
    pub(crate) fn mark_ready(&mut self, session_id: SessionId) {
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
    fn schedule_disconnect(&mut self, session_id: SessionId) {
        self.control_events
            .push_back(SessionControlEvent::Disconnect(session_id));
    }

    fn poll_app(&mut self) -> CoreResult<()> {
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

    fn take_scheduled_work(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        let mut work = core::mem::take(&mut self.session_work_scratch);
        core::mem::swap(&mut self.session_work, &mut work);
        for session_id in work.as_slice() {
            if let Some(entry) = self.entries.get_mut(session_id.pool_index()) {
                entry.schedule_pending = false;
            }
        }
        work
    }

    fn keep_work_scratch(&mut self, mut work: hammer_infra::vec::Vec<SessionId>) {
        work.clear();
        self.session_work_scratch = work;
    }

    fn elapsed_timer_ticks(&mut self, now: Instant) -> u32 {
        if self.timer_tick_duration.is_zero() {
            self.last_timer_tick = now;
            return 0;
        }
        let elapsed = now.saturating_duration_since(self.last_timer_tick);
        let ticks =
            (elapsed.as_nanos() / self.timer_tick_duration.as_nanos()).min(u32::MAX as u128) as u32;
        if ticks != 0 {
            self.last_timer_tick += self.timer_tick_duration * ticks;
        }
        ticks
    }

    fn expire_legacy_timers(&mut self, ticks: u32) -> usize {
        self.expired_timers.clear();
        let count = self.timers.expire(ticks, &mut self.expired_timers);
        for payload in self.expired_timers.as_slice() {
            let Some((slot, generation, timer_id)) = self.timers.take_expired_timer(*payload)
            else {
                continue;
            };
            self.pending_timers.push_back(ExpiredTimer {
                session_id: SessionId::from(PoolIndex::new(slot, generation)),
                timer_id,
            });
        }
        count
    }

    pub(crate) fn ack_tx_up_to(&mut self, session_id: SessionId, bytes: usize) -> CoreResult<()>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let _ = self.app.discard_acked_tx_bytes(session_id, bytes)?;
        Ok(())
    }

    pub(crate) fn enqueue_rx(
        &self,
        session_id: SessionId,
        index: BufferIndex,
        offset: u32,
        _: bool,
    ) -> CoreResult<RxDelivery>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        if offset == 0 {
            let (accepted, promoted) =
                self.app
                    .copy_rx_from_buffer(session_id, &self.buffers, index)?;
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
        let (accepted, newest) =
            self.app
                .copy_rx_from_buffer_ooo(session_id, &self.buffers, index, offset)?;
        let rx_available = self.rx_available_u32(session_id);
        Ok(match NonZeroU32::new(accepted) {
            Some(accepted) => {
                let (start, len) = newest.ok_or_else(|| {
                    CoreError::internal("accepted OOO delivery must report a retained span")
                })?;
                let len = NonZeroU32::new(len).ok_or_else(|| {
                    CoreError::internal("accepted OOO delivery must report non-zero span length")
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
    pub(crate) fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        self.app.rx_available_len(session_id)
    }

    #[inline]
    fn rx_available_u32(&self, session_id: SessionId) -> u32 {
        self.rx_available_len(session_id)
            .map(|value| value.min(u32::MAX as usize) as u32)
            .unwrap_or(0)
    }
}

pub struct SessionDriverRuntime<T, Seg: Segment = Local, Index = PoolIndex> {
    sessions: CachePadded<SessionWorker<Index, Seg>>,
    transports: CachePadded<T>,
}

impl<T, Seg, Index> SessionDriverRuntime<T, Seg, Index>
where
    Seg: Segment,
    Index: Copy + Eq,
{
    fn with_app_session_config(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        transports: T,
        app_session_config: AppSessionConfig,
        seg: Seg,
        worker_index: usize,
    ) -> Self {
        Self {
            sessions: CachePadded::new(SessionWorker::with_app_session_config(
                worker,
                buffers,
                app_session_config,
                seg,
                worker_index,
            )),
            transports: CachePadded::new(transports),
        }
    }

    #[inline]
    pub(crate) fn sessions(&self) -> &SessionWorker<Index, Seg> {
        &self.sessions
    }

    #[inline]
    pub(crate) fn sessions_mut(&mut self) -> &mut SessionWorker<Index, Seg> {
        &mut self.sessions
    }

    #[inline]
    pub(crate) fn transports(&self) -> &T {
        &self.transports
    }

    #[inline]
    pub(crate) fn transports_mut(&mut self) -> &mut T {
        &mut self.transports
    }

    #[inline]
    pub fn insert_session(&mut self, transport: SessionTransportId, index: Index) -> SessionId {
        self.sessions.insert_session_for_test(transport, index)
    }

    pub(crate) fn insert_session_with_transport<F>(
        &mut self,
        transport: SessionTransportId,
        create_transport: F,
    ) -> CoreResult<SessionId>
    where
        F: FnOnce(SessionId, &mut T) -> CoreResult<Index>,
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let session_id = self.sessions.insert_creating_session(transport)?;
        let handle = SessionHandle::new(
            session_id.pool_index().slot(),
            self.sessions.worker.slot() as u32,
        );
        let app_session = match self.sessions.app.create_app_session(
            handle,
            self.sessions.app_session_config,
            self.sessions.app.tx_evt_q().clone(),
        ) {
            Ok(session) => session,
            Err(error) => {
                self.sessions.remove_session_entry(session_id);
                return Err(error);
            }
        };
        self.sessions.app.attach_session(session_id, app_session);
        let index = match create_transport(session_id, &mut self.transports) {
            Ok(index) => index,
            Err(error) => {
                self.sessions.remove_session_entry(session_id);
                return Err(error);
            }
        };
        self.sessions.finish_session_creation(session_id, index)?;
        Ok(session_id)
    }

    #[inline]
    pub fn app(&self) -> &SessionAppRuntime<Seg> {
        self.sessions.app()
    }

    #[inline]
    pub fn app_mut(&mut self) -> &mut SessionAppRuntime<Seg> {
        self.sessions.app_mut()
    }

    #[inline]
    pub(crate) fn worker(&self) -> DataWorkerId {
        self.sessions.worker()
    }

    #[inline]
    pub(crate) fn prefetch_session(&self, session_id: SessionId) {
        self.sessions.prefetch_session(session_id);
    }

    #[inline]
    pub(crate) fn ack_tx_up_to(&mut self, session_id: SessionId, bytes: usize) -> CoreResult<()>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        self.sessions.ack_tx_up_to(session_id, bytes)
    }

    #[inline]
    pub(crate) fn enqueue_rx(
        &self,
        session_id: SessionId,
        index: BufferIndex,
        offset: u32,
        queue_event: bool,
    ) -> CoreResult<RxDelivery>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        self.sessions
            .enqueue_rx(session_id, index, offset, queue_event)
    }

    #[inline]
    pub(crate) fn timers_mut(&mut self) -> &mut TimerWheel1t2w2048sl<u32> {
        self.sessions.timers_mut()
    }

    #[cfg(test)]
    pub(crate) fn schedule_disconnect_for_test(&mut self, session_id: SessionId) {
        self.sessions.schedule_disconnect(session_id);
    }

    #[cfg(test)]
    pub(crate) fn schedule_session_work_for_test(&mut self, session_id: SessionId) {
        self.sessions.mark_ready(session_id);
    }
}

impl<T, Index: Copy + Eq> SessionDriverRuntime<T, Local, Index> {
    pub fn new(worker: DataWorkerId, buffers: DataPlaneBuffers, transports: T) -> Self {
        Self::with_app_session_config(
            worker,
            buffers,
            transports,
            AppSessionConfig::default(),
            Local::default(),
            worker.slot(),
        )
    }

    pub(crate) fn with_app_context(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        transports: T,
        app_context: AppContext<Local>,
    ) -> Self {
        let mut driver = Self::with_app_session_config(
            worker,
            buffers,
            transports,
            app_context.app_session_config(),
            Local::default(),
            worker.slot(),
        );
        driver.sessions.app_context = Some(app_context);
        driver
    }
}

impl<T, Index: Copy + Eq> SessionDriverRuntime<T, Svm, Index> {
    pub(crate) fn new_svm(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        transports: T,
        app_session_config: AppSessionConfig,
    ) -> Self {
        Self::with_app_session_config(
            worker,
            buffers,
            transports,
            app_session_config,
            Svm::default(),
            worker.slot(),
        )
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

pub trait SessionTransport<Index, Seg: Segment>: Sized {
    type Tx: SessionTxStrategy<Self, Index, Seg>;

    const ID: SessionTransportId;

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn handle_legacy_timer(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        timer_id: u32,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;
}

pub trait SessionPacketizedTransport<Index, Seg: Segment>: SessionTransport<Index, Seg> {
    fn control_tx(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn send_params(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<TransportSendParams>;

    fn tx_action(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        batch: &[TxBatchBuffer],
        now: Instant,
    ) -> CoreResult<()>;
}

pub trait TransportInternalTransport<Index, Seg: Segment>: SessionTransport<Index, Seg> {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;
}

pub trait SessionTxStrategy<T, Index, Seg: Segment>
where
    T: SessionTransport<Index, Seg>,
{
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        session_id: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;
}

pub struct SessionPacketizedTx;
pub struct TransportInternalTx;

impl<T, Index, Seg> SessionTxStrategy<T, Index, Seg> for SessionPacketizedTx
where
    T: SessionPacketizedTransport<Index, Seg>,
    Index: Copy + Eq,
    Seg: Segment,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        session_id: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        transport.control_tx(sessions, index, runtime, output_next, output, now)?;
        let Some(total_len) = sessions.app.pending_send_len(session_id)? else {
            return Ok(());
        };
        let params = transport.send_params(sessions, index, total_len, now)?;
        if params.tx_offset > total_len {
            return Err(CoreError::internal(
                "session tx offset exceeds chain length",
            ));
        }
        let mut batch_offset = params.tx_offset;
        let mut remaining_space = params.snd_space;
        if batch_offset < total_len && remaining_space != 0 && params.send_goal_size != 0 {
            let mut frame: Frame<Next> = sessions.buffers.get_next_frame(output_next.node())?;
            let batch_capacity = frame.remaining_capacity().min(DEFAULT_TX_DISPATCH_BUDGET);
            let mut batch = hammer_infra::vec::Vec::with_capacity(batch_capacity);
            while batch.len() < DEFAULT_TX_DISPATCH_BUDGET
                && frame.remaining_capacity() != 0
                && remaining_space != 0
            {
                let pending_len = total_len.saturating_sub(batch_offset);
                if pending_len == 0 {
                    break;
                }
                let payload_len = pending_len.min(remaining_space).min(params.send_goal_size);
                if payload_len == 0 {
                    break;
                }
                let buffer = sessions.buffers.alloc_index()?;
                frame.push_index(buffer)?;
                sessions
                    .app
                    .copy_tx_to_buffer(session_id, batch_offset, payload_len, buffer)?;
                batch.push(TxBatchBuffer {
                    index: buffer,
                    tx_offset: batch_offset,
                    payload_len,
                });
                batch_offset += payload_len;
                remaining_space -= payload_len;
            }
            transport.tx_action(sessions, index, batch.as_slice(), now)?;
            output.enqueue_frame(runtime, frame)?;
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

impl<T, Index, Seg> SessionTxStrategy<T, Index, Seg> for TransportInternalTx
where
    T: TransportInternalTransport<Index, Seg>,
    Index: Copy + Eq,
    Seg: Segment,
{
    #[inline]
    fn dispatch(
        transport: &mut T,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        _: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        transport.internal_tx(sessions, index, runtime, output_next, output, now)
    }
}

pub trait SessionTransports<Index, Seg: Segment> {
    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn disconnect(
        &mut self,
        id: SessionTransportId,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn ready(
        &mut self,
        id: SessionTransportId,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        session_id: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;

    fn handle_legacy_timer(
        &mut self,
        id: SessionTransportId,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        timer_id: u32,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()>;
}

impl<Index, Seg: Segment> SessionTransports<Index, Seg> for () {
    fn update_time(
        &mut self,
        _: &mut SessionWorker<Index, Seg>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut crate::session::node::SessionQueueOutput,
        _: Instant,
    ) -> CoreResult<()> {
        Ok(())
    }

    fn disconnect(
        &mut self,
        _: SessionTransportId,
        _: &mut SessionWorker<Index, Seg>,
        _: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut crate::session::node::SessionQueueOutput,
        _: Instant,
    ) -> CoreResult<()> {
        Err(CoreError::internal("session transport is not registered"))
    }

    fn ready(
        &mut self,
        _: SessionTransportId,
        _: &mut SessionWorker<Index, Seg>,
        _: Index,
        _: SessionId,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut crate::session::node::SessionQueueOutput,
        _: Instant,
    ) -> CoreResult<()> {
        Err(CoreError::internal("session transport is not registered"))
    }

    fn handle_legacy_timer(
        &mut self,
        _: SessionTransportId,
        _: &mut SessionWorker<Index, Seg>,
        _: Index,
        _: u32,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut crate::session::node::SessionQueueOutput,
        _: Instant,
    ) -> CoreResult<()> {
        Err(CoreError::internal("session transport is not registered"))
    }
}

impl<Head, Tail, Index, Seg> SessionTransports<Index, Seg> for (Head, Tail)
where
    Head: SessionTransport<Index, Seg>,
    Tail: SessionTransports<Index, Seg>,
    Index: Copy + Eq,
    Seg: Segment,
{
    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index, Seg>,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        self.0
            .update_time(sessions, runtime, output_next, output, now)?;
        self.1
            .update_time(sessions, runtime, output_next, output, now)
    }

    fn disconnect(
        &mut self,
        id: SessionTransportId,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        if id == Head::ID {
            return self
                .0
                .disconnect(sessions, index, runtime, output_next, output, now);
        }
        self.1
            .disconnect(id, sessions, index, runtime, output_next, output, now)
    }

    fn ready(
        &mut self,
        id: SessionTransportId,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        session_id: SessionId,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        if id == Head::ID {
            return <Head::Tx as SessionTxStrategy<Head, Index, Seg>>::dispatch(
                &mut self.0,
                sessions,
                index,
                session_id,
                runtime,
                output_next,
                output,
                now,
            );
        }
        self.1.ready(
            id,
            sessions,
            index,
            session_id,
            runtime,
            output_next,
            output,
            now,
        )
    }

    fn handle_legacy_timer(
        &mut self,
        id: SessionTransportId,
        sessions: &mut SessionWorker<Index, Seg>,
        index: Index,
        timer_id: u32,
        runtime: &DataPlaneRuntime,
        output_next: SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        now: Instant,
    ) -> CoreResult<()> {
        if id == Head::ID {
            return self.0.handle_legacy_timer(
                sessions,
                index,
                timer_id,
                runtime,
                output_next,
                output,
                now,
            );
        }
        self.1.handle_legacy_timer(
            id,
            sessions,
            index,
            timer_id,
            runtime,
            output_next,
            output,
            now,
        )
    }
}

pub fn dispatch_session_queue_for_ticks<T, Seg, Index>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<T, Seg, Index>,
    timer_ticks: u32,
    output_next: SessionQueueNext,
) -> CoreResult<SessionQueueStep>
where
    T: SessionTransports<Index, Seg>,
    Seg: Segment,
    Index: Copy + Eq,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let now = Instant::now();
    let mut output = crate::session::node::SessionQueueOutput::default();
    let step = dispatch_session_queue_pending(
        runtime,
        driver,
        timer_ticks,
        output_next,
        &mut output,
        now,
    )?;
    output.schedule(runtime)?;
    Ok(step)
}

pub(crate) fn dispatch_registered_session_queue_once_at<T, Seg, Index>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: Instant,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<()>
where
    T: SessionTransports<Index, Seg> + 'static,
    Seg: Segment,
    Index: Copy + Eq + 'static,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let mut driver =
        SessionQueueHandle::<SessionDriverRuntime<T, Seg, Index>>::new(data).borrow_mut()?;
    let ticks = driver.sessions.elapsed_timer_ticks(now);
    dispatch_session_queue_pending(runtime, &mut driver, ticks, output_next, output, now)?;
    Ok(())
}

pub fn dispatch_session_queue_pending<T, Seg, Index>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<T, Seg, Index>,
    timer_ticks: u32,
    output_next: SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
    now: Instant,
) -> CoreResult<SessionQueueStep>
where
    T: SessionTransports<Index, Seg>,
    Seg: Segment,
    Index: Copy + Eq,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let SessionDriverRuntime {
        sessions,
        transports,
    } = driver;
    transports.update_time(sessions, runtime, output_next, output, now)?;
    sessions.poll_app()?;
    let expired_timers = sessions.expire_legacy_timers(timer_ticks);

    let control_count = sessions.control_events.len();
    for _ in 0..control_count {
        let Some(SessionControlEvent::Disconnect(session_id)) = sessions.control_events.pop_front()
        else {
            break;
        };
        let transport = sessions.session_transport(session_id);
        sessions.notify_app_closed(session_id);
        if let Some((transport, index)) = transport {
            transports.disconnect(
                transport,
                sessions,
                index,
                runtime,
                output_next,
                output,
                now,
            )?;
        }
    }

    let pending_timer_count = sessions.pending_timers.len();
    for _ in 0..pending_timer_count {
        let Some(timer) = sessions.pending_timers.pop_front() else {
            break;
        };
        let Some((transport, index)) = sessions.session_transport(timer.session_id) else {
            continue;
        };
        transports.handle_legacy_timer(
            transport,
            sessions,
            index,
            timer.timer_id,
            runtime,
            output_next,
            output,
            now,
        )?;
    }

    let work = sessions.take_scheduled_work();
    let scheduled_sessions = work.len();
    for session_id in work.as_slice() {
        let Some((transport, index)) = sessions.session_transport(*session_id) else {
            continue;
        };
        transports.ready(
            transport,
            sessions,
            index,
            *session_id,
            runtime,
            output_next,
            output,
            now,
        )?;
    }
    sessions.keep_work_scratch(work);
    Ok(SessionQueueStep {
        expired_timers,
        scheduled_sessions,
    })
}

#[cfg(test)]
#[path = "runtime/tests.rs"]
mod tests;

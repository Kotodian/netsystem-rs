use std::ptr::NonNull;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;
use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, NodeRuntimeData,
    buffer::{Frame, Next},
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::msg_queue::{MsgQueue, SessionEvtType};
use hammer_infra::pool::{Index as PoolIndex, Pool};

use hammer_infra::segment::{Local, Segment, Svm};
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::app::{AppContext, AppSessionConfig, SessionHandle, with_current_app_worker};

use crate::session::app::SessionAppRuntimeCreate;
use crate::session::protocol::{ScheduleSessionWorkFn, SessionQueueControlContext};
use crate::session::{SessionAppRuntime, SessionId, SessionQueueHandle, SessionQueueNext};

const DEFAULT_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;
const DEFAULT_SESSION_TX_EVENT_CAPACITY: usize = 2048;
const DEFAULT_TX_DISPATCH_BUDGET: usize = 64;
const SESSION_TIMER_KIND_COUNT: usize = u32::BITS as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpiredTimer {
    session_id: SessionId,
    timer_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionControlEvent {
    Disconnect(SessionId),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionRxEnqueue {
    pub(crate) accepted_len: u32,
    pub(crate) delivered_len: u32,
    pub(crate) newest_ooo_start: Option<u32>,
    pub(crate) newest_ooo_len: u32,
    pub(crate) fifo_full: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SessionQueueStep {
    pub(crate) expired_timers: usize,
    pub(crate) scheduled_sessions: usize,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    session_work: hammer_infra::vec::Vec<SessionId>,
    session_work_scratch: hammer_infra::vec::Vec<SessionId>,
    timers: TimerWheel1t2w2048sl<u32>,
    expired_timers: hammer_infra::vec::Vec<u32>,
    pending_timers: FifoQueue<ExpiredTimer>,
    control_events: FifoQueue<SessionControlEvent>,
    timer_tick_duration: Duration,
    last_timer_tick: Instant,
}

impl WorkerSessionRuntime {
    #[inline]
    pub fn new(worker: DataWorkerId) -> Self {
        Self::with_timer_clock(worker, DEFAULT_SESSION_TIMER_TICK, Instant::now())
    }

    pub(crate) fn with_timer_clock(
        worker: DataWorkerId,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
    ) -> Self {
        Self {
            worker,
            session_work: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            session_work_scratch: hammer_infra::vec::Vec::with_capacity(
                DEFAULT_SESSION_POOL_CAPACITY,
            ),
            timers: TimerWheel1t2w2048sl::with_timer_ids(0, SESSION_TIMER_KIND_COUNT),
            expired_timers: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            pending_timers: FifoQueue::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            control_events: FifoQueue::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            timer_tick_duration,
            last_timer_tick,
        }
    }

    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    fn schedule_session_work(&mut self, session_id: SessionId) {
        self.session_work.push(session_id);
    }

    #[inline]
    fn scheduled_session_work_len(&self) -> usize {
        self.session_work.len()
    }

    #[inline]
    fn take_scheduled_session_work(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        let mut session_work = core::mem::take(&mut self.session_work_scratch);
        core::mem::swap(&mut self.session_work, &mut session_work);
        session_work
    }

    #[inline]
    fn keep_scheduled_session_work_scratch(
        &mut self,
        mut session_work: hammer_infra::vec::Vec<SessionId>,
    ) {
        session_work.clear();
        self.session_work_scratch = session_work;
    }

    #[inline]
    fn schedule_control_event(&mut self, event: SessionControlEvent) {
        self.control_events.push_back(event);
    }

    pub(crate) fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        self.expired_timers.clear();
        let expired_timers = self.timers.expire(timer_ticks, &mut self.expired_timers);
        for index in 0..self.expired_timers.len() {
            let payload = self.expired_timers[index];
            let Some((slot, generation, timer_id)) = self.timers.take_expired_timer(payload) else {
                continue;
            };
            let session_id = SessionId::from(PoolIndex::new(slot, generation));
            self.pending_timers.push_back(ExpiredTimer {
                session_id,
                timer_id,
            });
        }
        Ok(SessionQueueStep {
            expired_timers,
            scheduled_sessions: self.scheduled_session_work_len(),
        })
    }

    fn elapsed_timer_ticks(&mut self, now: Instant) -> u32 {
        if self.timer_tick_duration.is_zero() {
            self.last_timer_tick = now;
            return 0;
        }
        let elapsed = now.saturating_duration_since(self.last_timer_tick);
        let tick_nanos = self.timer_tick_duration.as_nanos();
        let elapsed_ticks = elapsed.as_nanos() / tick_nanos;
        let ticks = elapsed_ticks.min(u32::MAX as u128) as u32;
        if ticks == 0 {
            return 0;
        }
        if let Some(advance) = self.timer_tick_duration.checked_mul(ticks) {
            self.last_timer_tick += advance;
        } else {
            self.last_timer_tick = now;
        }
        ticks
    }
}

struct SessionEntry<St> {
    state: St,
    schedule_pending: bool,
}

impl<St> SessionEntry<St> {
    #[inline]
    const fn new(state: St) -> Self {
        Self {
            state,
            schedule_pending: false,
        }
    }
}

#[inline]
fn mark_schedule_pending<St>(entries: &mut Pool<SessionEntry<St>>, session_id: SessionId) -> bool {
    let Some(entry) = entries.get_mut(session_id.pool_index()) else {
        return false;
    };
    if entry.schedule_pending {
        return false;
    }
    entry.schedule_pending = true;
    true
}

unsafe fn schedule_session_work_for_driver<St, Seg: Segment>(
    scheduler: *mut (),
    session_id: SessionId,
) {
    let driver = unsafe { &mut *(scheduler as *mut SessionDriverRuntime<St, Seg>) };
    driver.schedule_session_work(session_id);
}

struct SessionDriverRuntimeCore<St> {
    sessions: WorkerSessionRuntime,
    entries: Pool<SessionEntry<St>>,
    buffers: DataPlaneBuffers,
}

struct SessionDriverRuntimeAppState<Seg: Segment> {
    app: SessionAppRuntime<Seg>,
    #[allow(dead_code)]
    app_context: Option<AppContext<Local>>,
    app_session_config: AppSessionConfig,
}

pub struct SessionDriverRuntime<St, Seg: Segment = Local> {
    runtime: CachePadded<SessionDriverRuntimeCore<St>>,
    app_state: CachePadded<SessionDriverRuntimeAppState<Seg>>,
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

pub trait SessionQueueProtocol: Sized {
    fn handle_expired_timer(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        timer_id: u32,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<bool>;

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<bool>;

    fn handle_disconnect(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<bool>;

    fn send_params(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<TransportSendParams>;

    fn push_header(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        batch: &[TxBatchBuffer],
        now: Instant,
    ) -> CoreResult<()>;

    fn custom_tx(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
        max_burst: usize,
        now: Instant,
    ) -> CoreResult<usize>;

    fn on_close(&mut self, context: &mut crate::session::protocol::SessionQueueControlContext);
}

pub(crate) trait SessionStateFactory<T> {
    fn build(self, session_id: SessionId) -> T;
}

impl<T, F> SessionStateFactory<T> for F
where
    F: FnOnce(SessionId) -> T,
{
    #[inline]
    fn build(self, session_id: SessionId) -> T {
        self(session_id)
    }
}

impl<St, Seg: Segment> SessionDriverRuntime<St, Seg> {
    #[inline]
    fn schedule_session_work(&mut self, session_id: SessionId) {
        let core = &mut *self.runtime;
        let SessionDriverRuntimeCore {
            sessions, entries, ..
        } = core;
        if mark_schedule_pending(entries, session_id) {
            sessions.schedule_session_work(session_id);
        }
    }

    #[inline]
    pub(crate) fn with_app_session_config(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        app_session_config: AppSessionConfig,
        seg: Seg,
        worker_index: usize,
    ) -> Self {
        let tx_evt_q = Arc::new(
            MsgQueue::<Seg>::new(seg.clone(), DEFAULT_SESSION_TX_EVENT_CAPACITY)
                .map_err(|e| CoreError::internal(format!("tx_evt_q: {e:?}")))
                .expect("tx_evt_q allocation"),
        );
        let app = SessionAppRuntime::new(
            DEFAULT_SESSION_POOL_CAPACITY,
            buffers.clone(),
            tx_evt_q,
            worker_index,
            seg,
        );
        Self {
            runtime: CachePadded::new(SessionDriverRuntimeCore {
                sessions: WorkerSessionRuntime::new(worker),
                entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
                buffers,
            }),
            app_state: CachePadded::new(SessionDriverRuntimeAppState {
                app,
                app_context: None,
                app_session_config,
            }),
        }
    }

    #[inline]
    pub(crate) fn worker(&self) -> DataWorkerId {
        self.runtime.sessions.worker()
    }

    #[inline]
    pub fn insert_session(&mut self, state: St) -> SessionId {
        let index = self
            .runtime
            .entries
            .insert(SessionEntry::new(state))
            .expect("session pool capacity exhausted");
        SessionId::from(index)
    }

    #[inline]
    pub fn session(&self, id: SessionId) -> Option<&St> {
        self.runtime
            .entries
            .get(id.pool_index())
            .map(|entry| &entry.state)
    }

    #[inline]
    pub(crate) fn session_mut(&mut self, id: SessionId) -> Option<&mut St> {
        self.runtime
            .entries
            .get_mut(id.pool_index())
            .map(|entry| &mut entry.state)
    }

    /// Prefetch the session pool slot cacheline for `id`.
    ///
    /// Thin pass-through to `hammer_infra::pool::Pool::prefetch_slot`,
    /// mirroring how `session`/`session_mut` expose `Pool::get`/`get_mut`.
    /// Intended to be called as soon as a `SessionId` is resolved on the
    /// input path (after `read_session_id`, before the `session_mut`
    /// borrow), so the packet-parse work in between gives the prefetch lead
    /// time to warm the cache-cold session slot.
    #[inline]
    pub(crate) fn prefetch_session(&self, id: SessionId) {
        self.runtime.entries.prefetch_slot(id.pool_index());
    }

    pub(crate) fn remove_session(&mut self, id: SessionId) -> Option<St> {
        self.app_state.app.discard_all_tx_bytes_for_session(id);
        let _ = self.app_state.app.detach_session(id);
        let handle = SessionHandle::new(id.pool_index().slot(), self.worker().slot() as u32);
        with_current_app_worker(self.worker().slot(), |worker| {
            let _ = worker.detach_session(handle);
        });
        let removed = self.runtime.entries.remove(id.pool_index())?;
        Some(removed.state)
    }

    pub(crate) fn close_session(&mut self, id: SessionId) -> CoreResult<Option<St>>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        if self.session(id).is_none() {
            return Ok(None);
        }
        self.app_state.app.closed(id)?;
        Ok(self.remove_session(id))
    }

    #[inline]
    pub fn app(&self) -> &SessionAppRuntime<Seg> {
        &self.app_state.app
    }

    #[inline]
    pub fn app_mut(&mut self) -> &mut SessionAppRuntime<Seg> {
        &mut self.app_state.app
    }

    #[inline]
    pub(crate) fn timers_mut(&mut self) -> &mut TimerWheel1t2w2048sl<u32> {
        &mut self.runtime.sessions.timers
    }

    #[inline]
    pub(crate) fn session_control_context(
        &mut self,
        session_id: SessionId,
    ) -> SessionQueueControlContext {
        let has_pending_send = self.app_state.app.has_pending_send(session_id);
        SessionQueueControlContext::new(
            &mut self.runtime.sessions.timers as *mut _,
            NonNull::from(&mut *self).cast(),
            schedule_session_work_for_driver::<St, Seg> as ScheduleSessionWorkFn,
            &self.runtime.buffers as *const _,
            session_id,
            has_pending_send,
        )
    }

    pub fn has_session_tx(&self, session_id: SessionId) -> bool {
        self.app_state.app.has_pending_send(session_id)
    }

    pub(crate) fn rx_available_len(&self, session_id: SessionId) -> Option<usize> {
        self.app_state.app.rx_available_len(session_id)
    }

    pub(crate) fn ack_tx_up_to(&mut self, session_id: SessionId, bytes: usize) -> CoreResult<()>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let _ = self
            .app_state
            .app
            .discard_acked_tx_bytes(session_id, bytes)?;
        Ok(())
    }

    pub(crate) fn poll_app(&mut self) -> CoreResult<()> {
        let app = &self.app_state.app;
        let core = &mut *self.runtime;
        let SessionDriverRuntimeCore {
            sessions, entries, ..
        } = core;
        app.drain_tx_events_to(|session_id, evt_type| match evt_type {
            SessionEvtType::TxDeq => {
                if mark_schedule_pending(entries, session_id) {
                    sessions.schedule_session_work(session_id);
                }
            }
            SessionEvtType::Close => {
                sessions.schedule_control_event(SessionControlEvent::Disconnect(session_id));
            }
            SessionEvtType::RxEnq | SessionEvtType::Connect => {}
        });
        Ok(())
    }

    #[inline]
    pub(crate) fn enqueue_rx(
        &self,
        session_id: SessionId,
        index: BufferIndex,
        offset: u32,
        _: bool,
    ) -> CoreResult<SessionRxEnqueue>
    where
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let buffers = &self.runtime.buffers;
        if offset == 0 {
            let wrote = self
                .app_state
                .app
                .copy_rx_from_buffer(session_id, buffers, index)?;
            Ok(SessionRxEnqueue {
                accepted_len: wrote as u32,
                delivered_len: wrote as u32,
                newest_ooo_start: None,
                newest_ooo_len: 0,
                fifo_full: wrote == 0,
            })
        } else {
            let (delivered, ooo_start, ooo_len) = self
                .app_state
                .app
                .copy_rx_from_buffer_ooo(session_id, buffers, index, offset)?;
            Ok(SessionRxEnqueue {
                accepted_len: ooo_len,
                delivered_len: delivered,
                newest_ooo_start: ooo_start,
                newest_ooo_len: ooo_len,
                fifo_full: ooo_len == 0,
            })
        }
    }

    pub fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        self.runtime.sessions.poll_once_for_ticks(timer_ticks)
    }

    pub(crate) fn poll_once_at(&mut self, now: Instant) -> CoreResult<SessionQueueStep> {
        let timer_ticks = self.runtime.sessions.elapsed_timer_ticks(now);
        self.poll_once_for_ticks(timer_ticks)
    }

    fn take_scheduled_session_work(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        let session_work = self.runtime.sessions.take_scheduled_session_work();
        for index in 0..session_work.len() {
            if let Some(entry) = self
                .runtime
                .entries
                .get_mut(session_work[index].pool_index())
            {
                entry.schedule_pending = false;
            }
        }
        session_work
    }

    #[inline]
    fn keep_scheduled_session_work_scratch(
        &mut self,
        session_work: hammer_infra::vec::Vec<SessionId>,
    ) {
        self.runtime
            .sessions
            .keep_scheduled_session_work_scratch(session_work);
    }
}

impl<St, Seg: Segment> SessionDriverRuntime<St, Seg> {
    #[inline]
    pub(crate) fn insert_session_with_id<F>(&mut self, f: F) -> CoreResult<SessionId>
    where
        F: SessionStateFactory<St>,
        SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let index = self
            .runtime
            .entries
            .insert_with(|index| SessionEntry::new(f.build(SessionId::from(index))))
            .ok_or_else(|| CoreError::internal("session pool capacity exhausted"))?;
        let session_id = SessionId::from(index);
        let handle =
            SessionHandle::new(session_id.pool_index().slot(), self.worker().slot() as u32);
        let app_session = match self.app_state.app.create_app_session(
            handle,
            self.app_state.app_session_config,
            self.app_state.app.tx_evt_q().clone(),
        ) {
            Ok(session) => session,
            Err(error) => {
                let _ = self.runtime.entries.remove(index);
                return Err(error);
            }
        };
        self.app_state.app.attach_session(session_id, app_session);
        Ok(session_id)
    }
}

impl<St> SessionDriverRuntime<St, Local> {
    #[inline]
    pub fn new(worker: DataWorkerId, buffers: DataPlaneBuffers) -> Self {
        Self::with_app_session_config(
            worker,
            buffers,
            AppSessionConfig::default(),
            Local::default(),
            worker.slot(),
        )
    }

    #[inline]
    pub(crate) fn with_app_context(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        app_context: AppContext<Local>,
    ) -> Self {
        let mut driver = Self::with_app_session_config(
            worker,
            buffers,
            app_context.app_session_config(),
            Local::default(),
            worker.slot(),
        );
        driver.app_state.app_context = Some(app_context);
        driver
    }
}

impl<St> SessionDriverRuntime<St, Svm> {
    #[inline]
    pub(crate) fn new_svm(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        app_session_config: AppSessionConfig,
    ) -> Self {
        Self::with_app_session_config(
            worker,
            buffers,
            app_session_config,
            Svm::default(),
            worker.slot(),
        )
    }
}

pub fn dispatch_session_queue_for_ticks<St, Seg: Segment>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<St, Seg>,
    timer_ticks: u32,
    output_next: crate::session::SessionQueueNext,
) -> CoreResult<SessionQueueStep>
where
    St: SessionQueueProtocol,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let mut step = driver.poll_once_for_ticks(timer_ticks)?;
    let now = Instant::now();
    let mut output = crate::session::node::SessionQueueOutput::default();
    dispatch_session_queue_pending(runtime, driver, output_next, &mut output, &mut step, now)?;
    output.schedule(runtime)?;
    Ok(step)
}

pub(crate) fn dispatch_session_queue_once_at<St, Seg: Segment>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<St, Seg>,
    now: Instant,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<SessionQueueStep>
where
    St: SessionQueueProtocol,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let mut step = driver.poll_once_at(now)?;
    dispatch_session_queue_pending(runtime, driver, output_next, output, &mut step, now)?;
    Ok(step)
}

pub(crate) fn dispatch_registered_session_queue_once_at<St, Seg: Segment>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: Instant,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<()>
where
    St: SessionQueueProtocol + 'static,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let mut driver = SessionQueueHandle::<SessionDriverRuntime<St, Seg>>::new(data).borrow_mut()?;
    dispatch_session_queue_once_at(runtime, &mut driver, now, output_next, output)?;
    Ok(())
}

unsafe fn session_queue_context<St, Seg: Segment>(
    driver: *mut SessionDriverRuntime<St, Seg>,
    session_id: SessionId,
) -> crate::session::protocol::SessionQueueControlContext {
    let driver = unsafe { &mut *driver };
    driver.session_control_context(session_id)
}

/// SAFETY: `state` (from `entries`) and `timers`/`ready`/`buffers` (from
/// `sessions`/`buffers`) are disjoint `CachePadded` fields within
/// `SessionDriverRuntime`. This single function encapsulates the unsafe
/// pointer derivation so callers do not repeat it.
unsafe fn with_session_state<St, Seg, F, R>(
    driver: *mut SessionDriverRuntime<St, Seg>,
    session_id: SessionId,
    f: F,
) -> CoreResult<R>
where
    Seg: Segment,
    F: FnOnce(&mut St, &mut SessionQueueControlContext) -> CoreResult<R>,
{
    let pool_index = session_id.pool_index();
    let driver = unsafe { &mut *driver };
    let driver_ptr = core::ptr::from_mut(driver);
    let state = driver
        .runtime
        .entries
        .get_mut(pool_index)
        .map(|entry| &mut entry.state)
        .ok_or_else(|| CoreError::internal("session is missing"))?;
    let mut context = unsafe { session_queue_context(driver_ptr, session_id) };
    f(state, &mut context)
}

unsafe fn close_session_from_protocol<St, Seg>(
    driver: *mut SessionDriverRuntime<St, Seg>,
    session_id: SessionId,
) -> CoreResult<()>
where
    St: SessionQueueProtocol,
    Seg: Segment,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let driver_ref = unsafe { &mut *driver };
    if driver_ref.session(session_id).is_none() {
        return Ok(());
    }
    {
        let state = driver_ref
            .session_mut(session_id)
            .ok_or_else(|| CoreError::internal("session is missing"))?;
        let mut context = unsafe { session_queue_context(driver, session_id) };
        state.on_close(&mut context);
    }
    let _ = driver_ref.close_session(session_id)?;
    Ok(())
}

pub fn dispatch_session_queue_pending<St, Seg: Segment>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<St, Seg>,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
    step: &mut SessionQueueStep,
    now: Instant,
) -> CoreResult<()>
where
    St: SessionQueueProtocol,
    SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    driver.poll_app()?;
    let expired_timer_count = driver.runtime.sessions.pending_timers.len();
    for _ in 0..expired_timer_count {
        let Some(expired_timer) = driver.runtime.sessions.pending_timers.pop_front() else {
            break;
        };
        let driver = driver as *mut SessionDriverRuntime<St, Seg>;
        let close_current = unsafe {
            with_session_state(driver, expired_timer.session_id, |state, context| {
                state.handle_expired_timer(
                    runtime,
                    context,
                    expired_timer.timer_id,
                    output_next,
                    output,
                )
            })?
        };
        if close_current {
            let id = expired_timer.session_id;
            unsafe { close_session_from_protocol(driver, id)? };
        }
    }
    let control_event_count = driver.runtime.sessions.control_events.len();
    let mut controlled_sessions = hammer_infra::vec::Vec::with_capacity(control_event_count);
    for _ in 0..control_event_count {
        let Some(event) = driver.runtime.sessions.control_events.pop_front() else {
            break;
        };
        let SessionControlEvent::Disconnect(session_id) = event;
        if driver.session(session_id).is_none() {
            continue;
        }
        controlled_sessions.push(session_id);
        let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
        let close_current = unsafe {
            with_session_state(driver_ptr, session_id, |state, context| {
                state.handle_disconnect(runtime, context, output_next, output)
            })?
        };
        if close_current {
            unsafe { close_session_from_protocol(driver_ptr, session_id)? };
        }
    }
    let session_work = driver.take_scheduled_session_work();
    step.scheduled_sessions = session_work.len();
    for index in 0..session_work.len() {
        let session_id = session_work[index];
        if controlled_sessions
            .as_slice()
            .iter()
            .any(|controlled_session_id| *controlled_session_id == session_id)
        {
            continue;
        }
        if driver.session(session_id).is_none() {
            continue;
        }
        let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
        let close_current = unsafe {
            with_session_state(driver_ptr, session_id, |state, context| {
                state.handle_ready_session(runtime, context, output_next, output)
            })?
        };
        if close_current {
            unsafe { close_session_from_protocol(driver_ptr, session_id)? };
            continue;
        }
        let mut requeue = false;
        let Some(total_len) = driver.app_state.app.pending_send_len(session_id)? else {
            continue;
        };
        let params = {
            let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
            unsafe {
                with_session_state(driver_ptr, session_id, |state, context| {
                    state.send_params(context, total_len, now)
                })?
            }
        };
        let transport_desched = params.flags.contains(TransportSendFlags::DESCHED);
        let transport_postpone = params.flags.contains(TransportSendFlags::POSTPONE);
        if params.tx_offset > total_len {
            return Err(CoreError::internal(
                "session tx offset exceeds chain length",
            ));
        }

        let mut batch_offset = params.tx_offset;
        let mut remaining_space = params.snd_space;
        let pending_len = total_len.saturating_sub(batch_offset);
        if pending_len != 0 && remaining_space != 0 && params.send_goal_size != 0 {
            let mut owner: Frame<Next> =
                driver.runtime.buffers.get_next_frame(output_next.node())?;
            let batch_capacity = owner.remaining_capacity().min(DEFAULT_TX_DISPATCH_BUDGET);
            let mut batch = hammer_infra::vec::Vec::with_capacity(batch_capacity);
            while batch.len() < DEFAULT_TX_DISPATCH_BUDGET
                && owner.remaining_capacity() > 0
                && remaining_space > 0
            {
                let pending_len = total_len.saturating_sub(batch_offset);
                if pending_len == 0 {
                    break;
                }
                let payload_len = pending_len.min(remaining_space).min(params.send_goal_size);
                if payload_len == 0 {
                    break;
                }

                let index = driver.runtime.buffers.alloc_index()?;
                owner.push_index(index)?;
                driver.app_state.app.copy_tx_to_buffer(
                    session_id,
                    batch_offset,
                    payload_len,
                    index,
                )?;
                batch.push(TxBatchBuffer {
                    index,
                    tx_offset: batch_offset,
                    payload_len,
                });
                batch_offset += payload_len;
                remaining_space -= payload_len;
            }

            let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
            unsafe {
                with_session_state(driver_ptr, session_id, |state, context| {
                    state.push_header(context, batch.as_slice(), now)
                })?
            };
            output.enqueue_frame(runtime, owner)?;
        }

        let pending_len = total_len.saturating_sub(batch_offset);
        if pending_len > 0 {
            requeue = !(params.snd_space == 0 && transport_desched && !transport_postpone);
        }
        if requeue {
            driver.schedule_session_work(session_id);
        }
    }
    driver.keep_scheduled_session_work_scratch(session_work);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, OnceLock};

    use crate::session::protocol::SessionQueueControlContext;
    use hammer_adapter::{
        BufferFrame, InternalNode, Node, NodeId, NodeProcessFn, NodeRegistration, NodeResult,
        NodeRuntimeData,
    };
    use hammer_infra::msg_queue::{SessionEvt, SessionEvtType};
    use hammer_runtime::app::AppSession;

    fn infra_vec<T>(items: impl IntoIterator<Item = T>) -> hammer_infra::vec::Vec<T> {
        let mut values = hammer_infra::vec::Vec::new();
        for item in items {
            values.push(item);
        }
        values
    }

    fn assert_next_pending_timer(
        runtime: &mut WorkerSessionRuntime,
        session_id: SessionId,
        timer_id: u32,
    ) {
        assert_eq!(
            runtime.pending_timers.pop_front(),
            Some(ExpiredTimer {
                session_id,
                timer_id,
            })
        );
        assert!(runtime.pending_timers.is_empty());
    }

    #[derive(Default)]
    struct FakeTxProtocol {
        send_params_calls: usize,
        push_header_calls: usize,
        custom_tx_calls: usize,
        snd_space: Option<usize>,
        send_goal_size: usize,
        flags: TransportSendFlags,
    }

    impl SessionQueueProtocol for FakeTxProtocol {
        fn handle_expired_timer(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: u32,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn handle_disconnect(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn send_params(
            &mut self,
            _: &mut SessionQueueControlContext,
            pending_len: usize,
            _: Instant,
        ) -> CoreResult<TransportSendParams> {
            self.send_params_calls += 1;
            Ok(TransportSendParams {
                snd_space: self.snd_space.unwrap_or(pending_len),
                tx_offset: 0,
                send_goal_size: self.send_goal_size.max(1),
                flags: self.flags,
            })
        }

        fn push_header(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: &[TxBatchBuffer],
            _: Instant,
        ) -> CoreResult<()> {
            self.push_header_calls += 1;
            Ok(())
        }

        fn custom_tx(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            self.custom_tx_calls += 1;
            Ok(0)
        }

        fn on_close(&mut self, _: &mut SessionQueueControlContext) {}
    }

    struct NoTxPayloadProtocol;

    impl SessionQueueProtocol for NoTxPayloadProtocol {
        fn handle_expired_timer(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: u32,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn handle_disconnect(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn send_params(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: usize,
            _: Instant,
        ) -> CoreResult<TransportSendParams> {
            Ok(TransportSendParams {
                snd_space: 0,
                tx_offset: 0,
                send_goal_size: 4,
                flags: TransportSendFlags::default(),
            })
        }

        fn push_header(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: &[TxBatchBuffer],
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport tx push_header must not run"))
        }

        fn custom_tx(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Err(CoreError::internal("transport custom tx must not run"))
        }

        fn on_close(&mut self, _: &mut SessionQueueControlContext) {}
    }

    #[derive(Default)]
    struct MarkReadyTwiceProtocol {
        ready_calls: usize,
    }

    impl SessionQueueProtocol for MarkReadyTwiceProtocol {
        fn handle_expired_timer(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: u32,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            context: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            self.ready_calls += 1;
            context.mark_ready();
            context.mark_ready();
            Ok(false)
        }

        fn handle_disconnect(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn send_params(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: usize,
            _: Instant,
        ) -> CoreResult<TransportSendParams> {
            Err(CoreError::internal("transport send_params must not run"))
        }

        fn push_header(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: &[TxBatchBuffer],
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport push_header must not run"))
        }

        fn custom_tx(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Err(CoreError::internal("transport custom_tx must not run"))
        }

        fn on_close(&mut self, _: &mut SessionQueueControlContext) {}
    }

    #[derive(Default)]
    struct TimerMarkReadyProtocol {
        timer_calls: usize,
        ready_calls: usize,
        received_timer: Option<u32>,
    }

    impl SessionQueueProtocol for TimerMarkReadyProtocol {
        fn handle_expired_timer(
            &mut self,
            _: &DataPlaneRuntime,
            context: &mut SessionQueueControlContext,
            timer_id: u32,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            self.timer_calls += 1;
            self.received_timer = Some(timer_id);
            context.mark_ready();
            Ok(false)
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            self.ready_calls += 1;
            Ok(false)
        }

        fn handle_disconnect(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn send_params(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: usize,
            _: Instant,
        ) -> CoreResult<TransportSendParams> {
            Err(CoreError::internal("transport send_params must not run"))
        }

        fn push_header(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: &[TxBatchBuffer],
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport push_header must not run"))
        }

        fn custom_tx(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Err(CoreError::internal("transport custom_tx must not run"))
        }

        fn on_close(&mut self, _: &mut SessionQueueControlContext) {}
    }

    #[derive(Default)]
    struct EventClassificationProtocol {
        ready_calls: usize,
        disconnect_calls: usize,
        close_calls: usize,
        close_removes: bool,
    }

    impl SessionQueueProtocol for EventClassificationProtocol {
        fn handle_expired_timer(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: u32,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            self.ready_calls += 1;
            Ok(false)
        }

        fn handle_disconnect(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            self.disconnect_calls += 1;
            Ok(self.close_removes)
        }

        fn send_params(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: usize,
            _: Instant,
        ) -> CoreResult<TransportSendParams> {
            Err(CoreError::internal("transport send_params must not run"))
        }

        fn push_header(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: &[TxBatchBuffer],
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport push_header must not run"))
        }

        fn custom_tx(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Ok(0)
        }

        fn on_close(&mut self, _: &mut SessionQueueControlContext) {
            self.close_calls += 1;
        }
    }

    #[derive(Default)]
    struct NonRemovingDisconnectProtocol {
        ready_calls: usize,
        disconnect_calls: usize,
        send_params_calls: usize,
    }

    impl SessionQueueProtocol for NonRemovingDisconnectProtocol {
        fn handle_expired_timer(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: u32,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            self.ready_calls += 1;
            Ok(false)
        }

        fn handle_disconnect(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            self.disconnect_calls += 1;
            Ok(false)
        }

        fn send_params(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: usize,
            _: Instant,
        ) -> CoreResult<TransportSendParams> {
            self.send_params_calls += 1;
            Ok(TransportSendParams {
                snd_space: 0,
                tx_offset: 0,
                send_goal_size: 1,
                flags: TransportSendFlags::default(),
            })
        }

        fn push_header(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: &[TxBatchBuffer],
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport push_header must not run"))
        }

        fn custom_tx(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Err(CoreError::internal("transport custom_tx must not run"))
        }

        fn on_close(&mut self, _: &mut SessionQueueControlContext) {}
    }

    #[derive(Default)]
    struct CaptureState {
        packets: std::vec::Vec<std::vec::Vec<u8>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states().lock().expect("capture registry");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
            }
        }
    }

    impl Node for CaptureNode {
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }

        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl InternalNode for CaptureNode {
        fn node_registration(&self) -> NodeRegistration
        where
            Self: Sized,
        {
            NodeRegistration::Plain
        }
    }

    fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
    }

    fn chain_bytes(
        buffers: &DataPlaneBuffers,
        index: BufferIndex,
    ) -> CoreResult<hammer_infra::vec::Vec<u8>> {
        let mut bytes = hammer_infra::vec::Vec::new();
        for buffer in buffers.chain(index) {
            bytes.extend_from_slice(buffer?.current());
        }
        Ok(bytes)
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> NodeResult {
        let slot = match data.usize_word(0) {
            Ok(s) => s,
            Err(_) => return NodeResult::drop(),
        };
        let state = {
            let states = capture_states().lock().expect("capture registry");
            match states.get(slot) {
                Some(s) => Arc::clone(s),
                None => return NodeResult::drop(),
            }
        };
        let mut state = state.lock().expect("capture state");
        for &index in frame.pending_indices() {
            let packet = match chain_bytes(runtime.buffers(), index) {
                Ok(bytes) => bytes,
                Err(_) => return NodeResult::drop(),
            };
            state.packets.push(packet.to_vec());
        }
        NodeResult::drop()
    }

    #[test]
    fn worker_session_runtime_expires_timer_into_expiry_without_session_work() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(9);
        let timer_id = 16;
        let mut runtime = WorkerSessionRuntime::new(worker);
        let session = session_id.pool_index();

        runtime
            .timers
            .arm_timer(session.slot(), session.generation(), timer_id, 3)
            .expect("arm session timer");

        assert_eq!(
            runtime
                .poll_once_for_ticks(2)
                .expect("expire before deadline")
                .expired_timers,
            0
        );
        assert!(runtime.pending_timers.is_empty());
        assert!(runtime.take_scheduled_session_work().is_empty());

        assert_eq!(
            runtime
                .poll_once_for_ticks(1)
                .expect("expire at deadline")
                .expired_timers,
            1
        );
        assert_next_pending_timer(&mut runtime, session_id, timer_id);
        assert!(runtime.take_scheduled_session_work().is_empty());
    }

    #[test]
    fn worker_session_runtime_rearming_same_timer_suppresses_stale_expiry() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(10);
        let timer_id = 2;
        let mut runtime = WorkerSessionRuntime::new(worker);
        let session = session_id.pool_index();

        runtime
            .timers
            .arm_timer(session.slot(), session.generation(), timer_id, 2)
            .expect("arm first timer");
        runtime
            .timers
            .arm_timer(session.slot(), session.generation(), timer_id, 5)
            .expect("rearm timer");

        assert_eq!(
            runtime
                .poll_once_for_ticks(2)
                .expect("expire stale timer")
                .expired_timers,
            0
        );
        assert!(runtime.pending_timers.is_empty());
        assert!(runtime.take_scheduled_session_work().is_empty());

        assert_eq!(
            runtime
                .poll_once_for_ticks(3)
                .expect("expire rearmed timer")
                .expired_timers,
            1
        );
        assert!(runtime.take_scheduled_session_work().is_empty());
        assert_next_pending_timer(&mut runtime, session_id, timer_id);
    }

    #[test]
    fn worker_session_runtime_cancel_timer_suppresses_expiry() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(11);
        let timer_id = 3;
        let mut runtime = WorkerSessionRuntime::new(worker);
        let session = session_id.pool_index();

        runtime
            .timers
            .arm_timer(session.slot(), session.generation(), timer_id, 2)
            .expect("arm timer");
        assert!(
            runtime
                .timers
                .cancel_timer(session.slot(), session.generation(), timer_id)
        );

        assert_eq!(
            runtime
                .poll_once_for_ticks(2)
                .expect("expire canceled timer")
                .expired_timers,
            0
        );
        assert!(runtime.pending_timers.is_empty());
        assert!(runtime.take_scheduled_session_work().is_empty());
    }

    #[test]
    fn worker_session_runtime_rearm_after_pending_expiry_drops_stale_delivery() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(12);
        let timer_id = 7;
        let mut runtime = WorkerSessionRuntime::new(worker);
        let session = session_id.pool_index();

        runtime
            .timers
            .arm_timer(session.slot(), session.generation(), timer_id, 1)
            .expect("arm first timer");
        assert_eq!(
            runtime
                .poll_once_for_ticks(1)
                .expect("expire first timer")
                .expired_timers,
            1
        );
        assert!(runtime.take_scheduled_session_work().is_empty());
        assert_next_pending_timer(&mut runtime, session_id, timer_id);

        runtime
            .timers
            .arm_timer(session.slot(), session.generation(), timer_id, 2)
            .expect("rearm timer after pending expiry");
        runtime.pending_timers.clear();
        assert!(runtime.pending_timers.is_empty());

        assert_eq!(
            runtime
                .poll_once_for_ticks(1)
                .expect("before second deadline")
                .expired_timers,
            0
        );
        assert!(runtime.pending_timers.is_empty());

        assert_eq!(
            runtime
                .poll_once_for_ticks(1)
                .expect("expire second timer")
                .expired_timers,
            1
        );
        assert_next_pending_timer(&mut runtime, session_id, timer_id);
    }

    #[test]
    fn worker_session_runtime_advances_timer_wheel_from_elapsed_clock_ticks() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(32);
        let timer_id = 4;
        let start = Instant::now();
        let mut runtime =
            WorkerSessionRuntime::with_timer_clock(worker, Duration::from_millis(10), start);
        let session = session_id.pool_index();

        runtime
            .timers
            .arm_timer(session.slot(), session.generation(), timer_id, 2)
            .expect("arm timer");

        let first_ticks = runtime.elapsed_timer_ticks(start + Duration::from_millis(10));
        let first = runtime
            .poll_once_for_ticks(first_ticks)
            .expect("first poll");
        assert_eq!(first.expired_timers, 0);
        assert!(runtime.pending_timers.is_empty());

        let second_ticks = runtime.elapsed_timer_ticks(start + Duration::from_millis(20));
        let second = runtime
            .poll_once_for_ticks(second_ticks)
            .expect("second poll");
        assert_eq!(second.expired_timers, 1);
        assert_next_pending_timer(&mut runtime, session_id, timer_id);
    }

    #[test]
    fn session_driver_runtime_suppresses_duplicate_session_work_and_reschedules_after_drain() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let mut driver = SessionDriverRuntime::<NoTxPayloadProtocol, Local>::new(
            DataWorkerId::new(0),
            runtime.buffers().clone(),
        );
        let session_id = driver.insert_session(NoTxPayloadProtocol);

        driver.schedule_session_work(session_id);
        driver.schedule_session_work(session_id);

        assert_eq!(
            driver.take_scheduled_session_work(),
            infra_vec([session_id])
        );
        assert!(driver.take_scheduled_session_work().is_empty());

        driver.schedule_session_work(session_id);

        assert_eq!(
            driver.take_scheduled_session_work(),
            infra_vec([session_id])
        );
    }

    #[test]
    fn session_driver_runtime_context_schedules_session_work() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let mut driver = SessionDriverRuntime::<NoTxPayloadProtocol, Local>::new(
            DataWorkerId::new(0),
            runtime.buffers().clone(),
        );
        let session_id = driver.insert_session(NoTxPayloadProtocol);
        let mut context = driver.session_control_context(session_id);

        context.mark_ready();
        context.mark_ready();

        assert_eq!(
            driver.take_scheduled_session_work(),
            infra_vec([session_id])
        );
        assert!(driver.take_scheduled_session_work().is_empty());

        context.mark_ready();

        assert_eq!(
            driver.take_scheduled_session_work(),
            infra_vec([session_id])
        );
    }

    #[test]
    fn session_context_mark_ready_coalesces_duplicate_requests_during_dispatch() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let mut driver = SessionDriverRuntime::<MarkReadyTwiceProtocol, Local>::new(
            DataWorkerId::new(0),
            runtime.buffers().clone(),
        );
        let session_id = driver.insert_session(MarkReadyTwiceProtocol::default());
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();

        let mut context = driver.session_control_context(session_id);
        context.mark_ready();
        let mut step = driver.poll_once_for_ticks(0).expect("poll first turn");
        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch first turn");

        assert_eq!(step.scheduled_sessions, 1);
        assert_eq!(driver.session(session_id).expect("state").ready_calls, 1);

        let mut step = driver.poll_once_for_ticks(0).expect("poll second turn");
        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch second turn");

        assert_eq!(step.scheduled_sessions, 1);
        assert_eq!(driver.session(session_id).expect("state").ready_calls, 2);
    }

    #[test]
    fn timer_handler_schedules_session_work_through_context() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let mut driver = SessionDriverRuntime::<TimerMarkReadyProtocol, Local>::new(
            DataWorkerId::new(0),
            runtime.buffers().clone(),
        );
        let session_id = driver.insert_session(TimerMarkReadyProtocol::default());
        let timer_id = 19;
        let session = session_id.pool_index();
        driver
            .timers_mut()
            .arm_timer(session.slot(), session.generation(), timer_id, 1)
            .expect("arm timer");

        let mut step = driver.poll_once_for_ticks(1).expect("poll timer");
        assert_eq!(step.expired_timers, 1);
        assert_eq!(step.scheduled_sessions, 0);
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();
        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch timer");

        let protocol = driver.session(session_id).expect("protocol state");
        assert_eq!(step.scheduled_sessions, 1);
        assert_eq!(protocol.timer_calls, 1);
        assert_eq!(protocol.ready_calls, 1);
        assert_eq!(protocol.received_timer, Some(timer_id));
    }

    fn attach_app_session<St>(
        driver: &mut SessionDriverRuntime<St, Local>,
        session_id: SessionId,
    ) -> Arc<AppSession<Local>> {
        let app_session = Arc::new(
            AppSession::<Local>::new_in_segment(
                Local::default(),
                AppSessionConfig::new(256, 64),
                SessionHandle::new(session_id.pool_index().slot() as u32, 0),
                driver.app().tx_evt_q().clone(),
            )
            .expect("create app session"),
        );
        driver
            .app_mut()
            .attach_session(session_id, Arc::clone(&app_session));
        app_session
    }

    fn enqueue_runtime_event<St>(
        driver: &SessionDriverRuntime<St, Local>,
        session_id: SessionId,
        evt_type: SessionEvtType,
    ) {
        driver
            .app()
            .tx_evt_q()
            .enqueue(SessionEvt {
                session_index: session_id.pool_index().slot(),
                evt_type,
            })
            .expect("enqueue runtime event");
    }

    #[test]
    fn app_tx_deq_event_schedules_session_work() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let mut driver = SessionDriverRuntime::<EventClassificationProtocol, Local>::new(
            DataWorkerId::new(0),
            runtime.buffers().clone(),
        );
        let session_id = driver.insert_session(EventClassificationProtocol::default());
        let _app_session = attach_app_session(&mut driver, session_id);
        enqueue_runtime_event(&driver, session_id, SessionEvtType::TxDeq);
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch tx event");

        let protocol = driver.session(session_id).expect("protocol state");
        assert_eq!(step.scheduled_sessions, 1);
        assert_eq!(protocol.ready_calls, 1);
        assert_eq!(protocol.disconnect_calls, 0);
        assert_eq!(protocol.close_calls, 0);
    }

    #[test]
    fn app_close_event_dispatches_disconnect_control_event() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let mut driver = SessionDriverRuntime::<EventClassificationProtocol, Local>::new(
            DataWorkerId::new(0),
            runtime.buffers().clone(),
        );
        let session_id = driver.insert_session(EventClassificationProtocol::default());
        let _app_session = attach_app_session(&mut driver, session_id);
        enqueue_runtime_event(&driver, session_id, SessionEvtType::Close);
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch close event");

        let protocol = driver.session(session_id).expect("protocol state");
        assert_eq!(step.scheduled_sessions, 0);
        assert_eq!(protocol.ready_calls, 0);
        assert_eq!(protocol.disconnect_calls, 1);
        assert_eq!(protocol.close_calls, 0);
    }

    #[test]
    fn same_turn_close_dispatches_before_tx_work_and_skips_removed_session() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let mut driver = SessionDriverRuntime::<EventClassificationProtocol, Local>::new(
            DataWorkerId::new(0),
            runtime.buffers().clone(),
        );
        let session_id = driver.insert_session(EventClassificationProtocol {
            close_removes: true,
            ..EventClassificationProtocol::default()
        });
        let app_session = attach_app_session(&mut driver, session_id);
        app_session.send_bytes(b"tx").expect("send pending tx");
        enqueue_runtime_event(&driver, session_id, SessionEvtType::Close);
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch close plus tx");

        assert_eq!(step.scheduled_sessions, 1);
        assert!(driver.session(session_id).is_none());
        assert!(driver.take_scheduled_session_work().is_empty());
    }

    #[test]
    fn same_turn_close_skips_tx_work_when_disconnect_keeps_session_allocated() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let mut driver = SessionDriverRuntime::<NonRemovingDisconnectProtocol, Local>::new(
            DataWorkerId::new(0),
            runtime.buffers().clone(),
        );
        let session_id = driver.insert_session(NonRemovingDisconnectProtocol::default());
        let app_session = attach_app_session(&mut driver, session_id);
        app_session.send_bytes(b"tx").expect("send pending tx");
        enqueue_runtime_event(&driver, session_id, SessionEvtType::Close);
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch close plus tx");

        let protocol = driver
            .session(session_id)
            .expect("session remains allocated");
        assert_eq!(step.scheduled_sessions, 1);
        assert_eq!(protocol.disconnect_calls, 1);
        assert_eq!(protocol.ready_calls, 0);
        assert_eq!(protocol.send_params_calls, 0);
        assert!(driver.take_scheduled_session_work().is_empty());

        let mut context = driver.session_control_context(session_id);
        context.mark_ready();
        let mut step = driver.poll_once_for_ticks(0).expect("poll later turn");
        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch later tx");

        let protocol = driver
            .session(session_id)
            .expect("session remains allocated");
        assert_eq!(step.scheduled_sessions, 1);
        assert_eq!(protocol.disconnect_calls, 1);
        assert_eq!(protocol.ready_calls, 1);
        assert_eq!(protocol.send_params_calls, 1);
    }

    #[test]
    fn session_tx_does_not_call_transport_when_app_has_no_pending_send() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let buffers = runtime.buffers();
        let mut driver = SessionDriverRuntime::<FakeTxProtocol, Local>::new(
            DataWorkerId::new(0),
            buffers.clone(),
        );
        let session_id = driver.insert_session(FakeTxProtocol::default());
        let mut context = driver.session_control_context(session_id);
        context.mark_ready();
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch without app tx");

        let protocol = driver.session(session_id).expect("protocol state");
        assert_eq!(protocol.send_params_calls, 0);
        assert_eq!(protocol.push_header_calls, 0);
        assert_eq!(protocol.custom_tx_calls, 0);
        assert!(!driver.has_session_tx(session_id));
    }

    #[test]
    fn session_tx_desched_flag_leaves_session_unqueued_when_send_space_is_zero() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let buffers = runtime.buffers();
        let mut driver = SessionDriverRuntime::<FakeTxProtocol, Local>::new(
            DataWorkerId::new(0),
            buffers.clone(),
        );
        let session_id = driver.insert_session(FakeTxProtocol {
            snd_space: Some(0),
            send_goal_size: 4,
            flags: TransportSendFlags::DESCHED,
            ..FakeTxProtocol::default()
        });

        let app_session = Arc::new(
            AppSession::<Local>::new_in_segment(
                Local::default(),
                AppSessionConfig::new(256, 64),
                SessionHandle::new(session_id.pool_index().slot() as u32, 0),
                driver.app().tx_evt_q().clone(),
            )
            .expect("create app session"),
        );
        app_session
            .send_bytes(&[0xABu8; 8])
            .expect("send tx payload");

        driver.app_mut().attach_session(session_id, app_session);
        let mut context = driver.session_control_context(session_id);
        context.mark_ready();
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch partial tx");

        let protocol = driver.session(session_id).expect("protocol state");
        assert_eq!(protocol.send_params_calls, 1);
        assert_eq!(protocol.push_header_calls, 0);
        assert!(driver.has_session_tx(session_id));
        assert!(driver.take_scheduled_session_work().is_empty());
    }

    #[test]
    fn session_tx_postpone_flag_requeues_remaining_data_when_send_space_is_zero() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let buffers = runtime.buffers();
        let mut driver = SessionDriverRuntime::<FakeTxProtocol, Local>::new(
            DataWorkerId::new(0),
            buffers.clone(),
        );
        let session_id = driver.insert_session(FakeTxProtocol {
            snd_space: Some(0),
            send_goal_size: 4,
            flags: TransportSendFlags::POSTPONE,
            ..FakeTxProtocol::default()
        });

        let app_session = Arc::new(
            AppSession::<Local>::new_in_segment(
                Local::default(),
                AppSessionConfig::new(256, 64),
                SessionHandle::new(session_id.pool_index().slot() as u32, 0),
                driver.app().tx_evt_q().clone(),
            )
            .expect("create app session"),
        );
        app_session
            .send_bytes(&[0xABu8; 8])
            .expect("send tx payload");

        driver.app_mut().attach_session(session_id, app_session);
        let mut context = driver.session_control_context(session_id);
        context.mark_ready();
        let next: crate::session::SessionQueueNext = NodeId::new(9).into();
        let mut output = crate::session::node::SessionQueueOutput::default();
        let mut step = driver.poll_once_for_ticks(0).expect("poll");

        dispatch_session_queue_pending(
            &runtime,
            &mut driver,
            next,
            &mut output,
            &mut step,
            Instant::now(),
        )
        .expect("dispatch postponed tx");

        let protocol = driver.session(session_id).expect("protocol state");
        assert_eq!(protocol.send_params_calls, 1);
        assert_eq!(protocol.push_header_calls, 0);
        assert!(driver.has_session_tx(session_id));
        assert_eq!(
            driver.take_scheduled_session_work(),
            infra_vec([session_id])
        );
    }
}

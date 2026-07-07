use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;
use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::msg_queue::MsgQueue;
use hammer_infra::pool::{Index as PoolIndex, Pool};

use hammer_infra::segment::{Local, Segment, Svm};
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::app::{AppContext, AppSessionConfig, SessionHandle, with_current_app_worker};

use crate::session::app::SessionAppRuntimeCreate;
use crate::session::protocol::SessionQueueControlContext;
use crate::session::{
    SessionAppRuntime, SessionId, SessionQueueHandle, SessionQueueNext, SessionReadyQueue,
};

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
    pub(crate) ready_sessions: usize,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    ready: SessionReadyQueue,
    timers: TimerWheel1t2w2048sl<u32>,
    expired_timers: hammer_infra::vec::Vec<u32>,
    pending_timers: FifoQueue<ExpiredTimer>,
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
            ready: SessionReadyQueue::new(),
            timers: TimerWheel1t2w2048sl::with_timer_ids(0, SESSION_TIMER_KIND_COUNT),
            expired_timers: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            pending_timers: FifoQueue::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            timer_tick_duration,
            last_timer_tick,
        }
    }

    #[inline]
    pub const fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub fn mark_ready(&mut self, session_id: SessionId) {
        self.ready.mark_ready(session_id);
    }

    #[inline]
    pub(crate) fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        self.ready.take_ready_sessions()
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
            self.ready.mark_ready(session_id);
        }
        Ok(SessionQueueStep {
            expired_timers,
            ready_sessions: self.ready.len(),
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

struct SessionDriverRuntimeCore<St> {
    sessions: WorkerSessionRuntime,
    entries: Pool<St>,
    buffers: DataPlaneBuffers,
    pending_closes: SessionReadyQueue,
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
        close_requested: bool,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<bool>;

    fn tx_offset(
        &self,
        context: &crate::session::protocol::SessionQueueControlContext,
    ) -> CoreResult<usize>;

    fn tx_payload_len(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        tx_offset: usize,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<usize>;

    fn prepare_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        index: BufferIndex,
        tx_offset: usize,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()>;

    fn cancel_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        index: BufferIndex,
    );

    fn commit_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        index: BufferIndex,
        tx_offset: usize,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()>;

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

                pending_closes: SessionReadyQueue::new(),
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
    pub fn mark_ready(&mut self, session_id: SessionId) {
        self.runtime.sessions.mark_ready(session_id);
    }

    #[inline]
    pub fn insert_session(&mut self, state: St) -> SessionId {
        let index = self
            .runtime
            .entries
            .insert(state)
            .expect("session pool capacity exhausted");
        SessionId::from(index)
    }

    #[inline]
    pub fn session(&self, id: SessionId) -> Option<&St> {
        self.runtime.entries.get(id.pool_index())
    }

    #[inline]
    pub(crate) fn session_mut(&mut self, id: SessionId) -> Option<&mut St> {
        self.runtime.entries.get_mut(id.pool_index())
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
        self.runtime.pending_closes.take(id);
        self.app_state.app.discard_all_tx_bytes_for_session(id);
        let _ = self.app_state.app.detach_session(id);
        let handle = SessionHandle::new(id.pool_index().slot(), self.worker().slot() as u32);
        with_current_app_worker(self.worker().slot(), |worker| {
            let _ = worker.detach_session(handle);
        });
        let removed = self.runtime.entries.remove(id.pool_index())?;
        Some(removed)
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
    pub(crate) fn buffers(&self) -> &DataPlaneBuffers {
        &self.runtime.buffers
    }

    #[inline]
    pub(crate) fn timers_mut(&mut self) -> &mut TimerWheel1t2w2048sl<u32> {
        &mut self.runtime.sessions.timers
    }

    #[inline]
    pub(crate) fn ready_mut_ptr(&mut self) -> *mut SessionReadyQueue {
        &mut self.runtime.sessions.ready as *mut _
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
        self.app_state
            .app
            .drain_tx_events_to(&mut self.runtime.sessions.ready);
        Ok(())
    }

    #[inline]
    pub(crate) fn take_close_request(&mut self, session_id: SessionId) -> bool {
        self.runtime.pending_closes.take(session_id)
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

    pub(crate) fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        self.runtime.sessions.take_ready_sessions()
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
            .insert_with(|index| f.build(SessionId::from(index)))
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
    let has_pending_send = driver.app_state.app.has_pending_send(session_id);
    crate::session::protocol::SessionQueueControlContext::new(
        &mut driver.runtime.sessions.timers as *mut _,
        &mut driver.runtime.sessions.ready as *mut _,
        &driver.runtime.buffers as *const _,
        session_id,
        has_pending_send,
    )
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
        .ok_or_else(|| CoreError::internal("session is missing"))?;
    let mut context = unsafe { session_queue_context(driver_ptr, session_id) };
    f(state, &mut context)
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
            unsafe {
                let driver_ref = &mut *driver;
                let state = driver_ref
                    .session_mut(id)
                    .ok_or_else(|| CoreError::internal("session is missing"))?;
                let mut context = unsafe { session_queue_context(driver, id) };
                state.on_close(&mut context);
                let _ = state;
                let _ = driver_ref.close_session(id)?;
            }
        }
    }
    let ready_count = driver.runtime.sessions.ready.len();
    step.ready_sessions = ready_count;
    for _ in 0..ready_count {
        let Some(session_id) = driver.runtime.sessions.ready.pop_front() else {
            break;
        };
        if driver.session(session_id).is_none() {
            continue;
        }
        let close_requested = driver.take_close_request(session_id);
        let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
        let close_current = unsafe {
            with_session_state(driver_ptr, session_id, |state, context| {
                state.handle_ready_session(runtime, context, close_requested, output_next, output)
            })?
        };
        if close_current {
            unsafe {
                let driver_ref = &mut *driver_ptr;
                let state = driver_ref
                    .session_mut(session_id)
                    .ok_or_else(|| CoreError::internal("session is missing"))?;
                let mut context = session_queue_context(driver_ptr, session_id);
                state.on_close(&mut context);
                let _ = driver_ref.close_session(session_id)?;
            }
            continue;
        }
        for _ in 0..DEFAULT_TX_DISPATCH_BUDGET {
            let Some(total_len) = driver.app_state.app.pending_send_len(session_id)? else {
                break;
            };
            let tx_offset = {
                let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
                unsafe {
                    with_session_state(driver_ptr, session_id, |state, context| {
                        state.tx_offset(context)
                    })?
                }
            };
            if tx_offset > total_len {
                return Err(CoreError::internal(
                    "session tx offset exceeds chain length",
                ));
            }
            let pending_len = total_len.saturating_sub(tx_offset);
            let payload_len = {
                let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
                unsafe {
                    with_session_state(driver_ptr, session_id, |state, context| {
                        state
                            .tx_payload_len(context, tx_offset, pending_len, now)
                            .map(|len| len.min(pending_len))
                    })?
                }
            };
            if payload_len == 0 {
                break;
            }

            let mut owner = driver.runtime.buffers.get_next_frame(output_next.node())?;
            let index = driver.runtime.buffers.alloc_index()?;
            owner.push_index(index)?;
            if let Err(error) =
                driver
                    .app_state
                    .app
                    .copy_tx_to_buffer(session_id, tx_offset, payload_len, index)
            {
                return Err(error);
            }

            {
                let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
                unsafe {
                    with_session_state(driver_ptr, session_id, |state, context| {
                        state.prepare_tx(context, index, tx_offset, payload_len, now)
                    })?
                };
            }

            output.enqueue_frame(runtime, owner)?;

            let commit_result = {
                let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
                unsafe {
                    with_session_state(driver_ptr, session_id, |state, context| {
                        state.commit_tx(context, index, tx_offset, payload_len, now)
                    })
                }
            };
            if let Err(err) = commit_result {
                let driver_ptr = driver as *mut SessionDriverRuntime<St, Seg>;
                unsafe {
                    let _ = with_session_state(driver_ptr, session_id, |state, context| {
                        state.cancel_tx(context, index);
                        Ok(())
                    });
                }
                return Err(err);
            }
        }
        let remaining = driver
            .app_state
            .app
            .pending_send_len(session_id)?
            .unwrap_or(0);
        if remaining > 0 {
            driver.mark_ready(session_id);
        }
    }
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
        prepared: usize,
        committed: usize,
        canceled: usize,
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
            _: bool,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn tx_offset(&self, _: &SessionQueueControlContext) -> CoreResult<usize> {
            Ok(0)
        }

        fn tx_payload_len(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: usize,
            pending_len: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Ok(pending_len.min(4))
        }

        fn prepare_tx(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: BufferIndex,
            _: usize,
            payload_len: usize,
            _: Instant,
        ) -> CoreResult<()> {
            self.prepared += payload_len;
            Ok(())
        }

        fn cancel_tx(&mut self, _: &mut SessionQueueControlContext, _: BufferIndex) {
            self.canceled += 1;
        }

        fn commit_tx(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: BufferIndex,
            _: usize,
            payload_len: usize,
            _: Instant,
        ) -> CoreResult<()> {
            self.committed += payload_len;
            Ok(())
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
            _: bool,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<bool> {
            Ok(false)
        }

        fn tx_offset(&self, _: &SessionQueueControlContext) -> CoreResult<usize> {
            Ok(0)
        }

        fn tx_payload_len(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: usize,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Ok(0)
        }

        fn prepare_tx(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: BufferIndex,
            _: usize,
            _: usize,
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport tx prepare must not run"))
        }

        fn cancel_tx(&mut self, _: &mut SessionQueueControlContext, _: BufferIndex) {}

        fn commit_tx(
            &mut self,
            _: &mut SessionQueueControlContext,
            _: BufferIndex,
            _: usize,
            _: usize,
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport tx commit must not run"))
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
    fn worker_session_runtime_expires_timer_into_expiry_and_ready_session() {
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
        assert!(runtime.take_ready_sessions().is_empty());

        assert_eq!(
            runtime
                .poll_once_for_ticks(1)
                .expect("expire at deadline")
                .expired_timers,
            1
        );
        assert_next_pending_timer(&mut runtime, session_id, timer_id);
        assert_eq!(runtime.take_ready_sessions(), infra_vec([session_id]));
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
        assert!(runtime.take_ready_sessions().is_empty());

        assert_eq!(
            runtime
                .poll_once_for_ticks(3)
                .expect("expire rearmed timer")
                .expired_timers,
            1
        );
        assert_eq!(runtime.take_ready_sessions(), infra_vec([session_id]));
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
        assert!(runtime.take_ready_sessions().is_empty());
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
        assert_eq!(runtime.take_ready_sessions(), infra_vec([session_id]));

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
        driver.mark_ready(session_id);
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
        assert_eq!(protocol.prepared, 0);
        assert_eq!(protocol.committed, 0);
        assert_eq!(protocol.canceled, 0);
        assert!(!driver.has_session_tx(session_id));
    }
}

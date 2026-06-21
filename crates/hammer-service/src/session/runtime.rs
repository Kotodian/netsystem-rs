use std::time::{Duration, Instant};

use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, RouteMetadata,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo::FifoQueue;
use hammer_infra::map::FlatHashTable;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_runtime::app::AppOpId;
#[cfg(test)]
use hammer_runtime::app::AppRingHandle;

use crate::session::{
    SessionAppRuntime, SessionId, SessionReadyQueue, SessionTimerExpiry, SessionTimerToken,
    SessionTimerWheel,
};

const DEFAULT_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionRxBuffer {
    pub(crate) index: BufferIndex,
    pub(crate) offset: u32,
    pub(crate) len: u32,
    pub(crate) fin: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionRxEnqueue {
    pub(crate) delivered_len: u32,
    pub(crate) newest_ooo_start: Option<u32>,
    pub(crate) newest_ooo_len: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionQueueStep {
    pub(crate) expired_timers: usize,
    pub(crate) ready_sessions: usize,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    ready: SessionReadyQueue,
    timers: SessionTimerWheel,
    pending_timer_expiries: hammer_infra::vec::Vec<SessionTimerExpiry>,
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
            timers: SessionTimerWheel::new(),
            pending_timer_expiries: hammer_infra::vec::Vec::new(),
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

    #[inline]
    pub fn arm_timer_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.clear_pending_timer_expiry(session_id, token);
        self.timers.arm_ticks(session_id, token, ticks)
    }

    #[inline]
    pub fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        self.clear_pending_timer_expiry(session_id, token);
        self.timers.cancel(session_id, token)
    }

    pub(crate) fn expire_timers(&mut self, ticks: u32) -> CoreResult<usize> {
        let expired = self.timers.expire(ticks, &mut self.ready)?;
        self.pending_timer_expiries
            .extend(self.timers.take_expiries());
        Ok(expired)
    }

    pub(crate) fn take_timer_expiries(&mut self) -> hammer_infra::vec::Vec<SessionTimerExpiry> {
        self.pending_timer_expiries.drain(..).collect()
    }

    fn clear_pending_timer_expiry(&mut self, session_id: SessionId, token: SessionTimerToken) {
        let pending = self
            .pending_timer_expiries
            .drain(..)
            .collect::<hammer_infra::vec::Vec<_>>();
        for expiry in pending {
            if expiry.session_id() != session_id || expiry.token() != token {
                self.pending_timer_expiries.push(expiry);
            }
        }
    }

    pub(crate) fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        let expired_timers = self.expire_timers(timer_ticks)?;
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

pub(crate) struct SessionDriverRuntime<S, A> {
    sessions: WorkerSessionRuntime,
    entries: Pool<S>,
    aux: A,
    app_ops: FlatHashTable<u64, AppOpId>,
    app: SessionAppRuntime,
    buffers: DataPlaneBuffers,
    pending_closes: SessionReadyQueue,
    tx: Pool<FifoQueue<BufferIndex>>,
    tx_index: FlatHashTable<u64, PoolIndex>,
    rx: Pool<FifoQueue<SessionRxBuffer>>,
    rx_index: FlatHashTable<u64, PoolIndex>,
}

pub(crate) trait SessionQueueProtocol<A>: Sized {
    fn handle_timer_expiry(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, A>,
        expiry: SessionTimerExpiry,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<()>;

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, A>,
        close_requested: bool,
        output_next: crate::session::SessionQueueNext,
        output: &mut crate::session::node::SessionQueueOutput,
    ) -> CoreResult<()>;

    fn tx_payload_len(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, A>,
        pending_len: usize,
        now: Instant,
    ) -> CoreResult<usize>;

    fn prepare_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, A>,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()>;

    fn cancel_tx(&mut self, aux: &mut A, index: BufferIndex);

    fn commit_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext<'_, A>,
        index: BufferIndex,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()>;
}

pub(crate) trait SessionStateFactory<S> {
    fn build(self, session_id: SessionId) -> S;
}

impl<S, F> SessionStateFactory<S> for F
where
    F: FnOnce(SessionId) -> S,
{
    #[inline]
    fn build(self, session_id: SessionId) -> S {
        self(session_id)
    }
}

impl<S, A> SessionDriverRuntime<S, A> {
    #[inline]
    pub(crate) fn new(worker: DataWorkerId, buffers: DataPlaneBuffers, aux: A) -> Self {
        Self {
            sessions: WorkerSessionRuntime::new(worker),
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            aux,
            app_ops: FlatHashTable::new(),
            app: SessionAppRuntime::new(),
            buffers,
            pending_closes: SessionReadyQueue::new(),
            tx: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            tx_index: FlatHashTable::new(),
            rx: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            rx_index: FlatHashTable::new(),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn with_timer_clock(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        timer_tick_duration: Duration,
        last_timer_tick: Instant,
        aux: A,
    ) -> Self {
        Self {
            sessions: WorkerSessionRuntime::with_timer_clock(
                worker,
                timer_tick_duration,
                last_timer_tick,
            ),
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            aux,
            app_ops: FlatHashTable::new(),
            app: SessionAppRuntime::new(),
            buffers,
            pending_closes: SessionReadyQueue::new(),
            tx: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            tx_index: FlatHashTable::new(),
            rx: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            rx_index: FlatHashTable::new(),
        }
    }

    #[inline]
    pub(crate) fn worker(&self) -> DataWorkerId {
        self.sessions.worker()
    }

    #[inline]
    pub(crate) fn mark_ready(&mut self, session_id: SessionId) {
        self.sessions.mark_ready(session_id);
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn insert_session(&mut self, state: S) -> SessionId {
        let index = self
            .entries
            .insert(state)
            .expect("session pool capacity exhausted");
        SessionId::from(index)
    }

    #[inline]
    pub(crate) fn insert_session_with_id<F>(&mut self, f: F) -> SessionId
    where
        F: SessionStateFactory<S>,
    {
        let index = self
            .entries
            .insert_with(|index| f.build(SessionId::from(index)))
            .expect("session pool capacity exhausted");
        SessionId::from(index)
    }

    #[inline]
    pub(crate) fn session(&self, id: SessionId) -> Option<&S> {
        self.entries.get(id.pool_index())
    }

    #[inline]
    pub(crate) fn session_mut(&mut self, id: SessionId) -> Option<&mut S> {
        self.entries.get_mut(id.pool_index())
    }

    #[inline]
    pub(crate) fn aux(&self) -> &A {
        &self.aux
    }

    #[inline]
    pub(crate) fn aux_mut(&mut self) -> &mut A {
        &mut self.aux
    }

    pub(crate) fn remove_session(&mut self, id: SessionId) -> Option<S> {
        self.pending_closes.take(id);
        self.release_session_tx(id);
        self.release_session_rx(id);
        let removed = self.entries.remove(id.pool_index())?;
        if let Some(op) = self.app_ops.remove(&id.get()) {
            self.app.unbind_ring(op);
        }
        Some(removed)
    }

    pub(crate) fn close_session(&mut self, id: SessionId) -> CoreResult<Option<S>> {
        if self.session(id).is_none() {
            return Ok(None);
        }
        if let Some(op) = self.app_ops.lookup(&id.get()) {
            self.app.complete_closed(op)?;
        }
        Ok(self.remove_session(id))
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn bind_session_app_ring(
        &mut self,
        id: SessionId,
        op: AppOpId,
        ring: AppRingHandle,
    ) -> bool {
        if self.session(id).is_none() {
            return false;
        }
        self.app_ops.insert(id.get(), op);
        self.app.bind_ring(id, op, ring);
        true
    }

    #[inline]
    pub(crate) fn session_app_op(&self, id: SessionId) -> Option<AppOpId> {
        self.app_ops.lookup(&id.get())
    }

    #[inline]
    pub(crate) fn app(&self) -> &SessionAppRuntime {
        &self.app
    }

    #[inline]
    pub(crate) fn app_mut(&mut self) -> &mut SessionAppRuntime {
        &mut self.app
    }

    #[inline]
    pub(crate) fn buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    fn tx_queue_mut_or_alloc(&mut self, session_id: SessionId) -> &mut FifoQueue<BufferIndex> {
        let key = session_id.get();
        let index = match self.tx_index.lookup(&key) {
            Some(index) => index,
            None => {
                let index = self
                    .tx
                    .insert(FifoQueue::new())
                    .expect("session tx queue pool exhausted");
                self.tx_index.insert(key, index);
                index
            }
        };
        self.tx.get_mut(index).expect("session tx queue index is valid")
    }

    fn rx_queue_mut_or_alloc(
        &mut self,
        session_id: SessionId,
    ) -> &mut FifoQueue<SessionRxBuffer> {
        let key = session_id.get();
        let index = match self.rx_index.lookup(&key) {
            Some(index) => index,
            None => {
                let index = self
                    .rx
                    .insert(FifoQueue::new())
                    .expect("session rx queue pool exhausted");
                self.rx_index.insert(key, index);
                index
            }
        };
        self.rx.get_mut(index).expect("session rx queue index is valid")
    }

    pub(crate) fn retain_tx_buffer(&mut self, session_id: SessionId, index: BufferIndex) {
        self.tx_queue_mut_or_alloc(session_id).push_back(index);
    }

    #[cfg(test)]
    pub(crate) fn has_retained_tx(&self, session_id: SessionId) -> bool {
        let Some(index) = self.tx_index.lookup(&session_id.get()) else {
            return false;
        };
        self.tx.get(index).is_some_and(|queue| !queue.is_empty())
    }

    pub(crate) fn release_tx_up_to(
        &mut self,
        session_id: SessionId,
        mut bytes: usize,
    ) -> CoreResult<()> {
        let key = session_id.get();
        let Some(index) = self.tx_index.lookup(&key) else {
            return Ok(());
        };
        let mut remove_queue = false;
        while bytes != 0 {
            let current = {
                let queue = self
                    .tx
                    .get_mut(index)
                    .ok_or_else(|| CoreError::internal("session tx queue index is invalid"))?;
                queue.front().copied()
            };
            let Some(current) = current else {
                break;
            };
            let current_len = self
                .buffers
                .current_len(current)?
                .checked_add(self.buffers.total_len_not_including_first(current)?)
                .ok_or_else(|| CoreError::internal("session tx chain length overflow"))?;
            if bytes < current_len {
                self.buffers.advance(current, bytes)?;
                bytes = 0;
            } else {
                {
                    let queue = self
                        .tx
                        .get_mut(index)
                        .ok_or_else(|| CoreError::internal("session tx queue index is invalid"))?;
                    let removed = queue
                        .pop_front()
                        .ok_or_else(|| CoreError::internal("session tx buffer is missing"))?;
                    self.buffers.free_index(removed);
                    remove_queue = queue.is_empty();
                }
                bytes -= current_len;
            }
        }
        if remove_queue {
            self.tx_index.remove(&key);
            let _ = self.tx.remove(index);
        }
        Ok(())
    }

    fn release_session_tx(&mut self, session_id: SessionId) {
        let Some(index) = self.tx_index.remove(&session_id.get()) else {
            return;
        };
        let Some(mut queue) = self.tx.remove(index) else {
            return;
        };
        while let Some(buffer) = queue.pop_front() {
            self.buffers.free_index(buffer);
        }
    }

    fn release_session_rx(&mut self, session_id: SessionId) {
        let Some(index) = self.rx_index.remove(&session_id.get()) else {
            return;
        };
        let Some(mut queue) = self.rx.remove(index) else {
            return;
        };
        while let Some(buffer) = queue.pop_front() {
            self.buffers.free_index(buffer.index);
        }
    }

    pub(crate) fn poll_app(&mut self) -> CoreResult<()> {
        self.app.drain_submissions()?;
        let mut ready = hammer_infra::vec::Vec::new();
        self.app.take_ready_sessions(&mut ready);
        let recv_ready = ready.clone();
        for close in self.app.take_drained_closes() {
            self.pending_closes.mark_ready(close.session_id());
            if !ready.iter().any(|session_id| *session_id == close.session_id()) {
                ready.push(close.session_id());
            }
        }
        for session_id in recv_ready {
            self.flush_session_rx(session_id)?;
        }
        for session_id in ready {
            self.mark_ready(session_id);
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn take_close_request(&mut self, session_id: SessionId) -> bool {
        self.pending_closes.take(session_id)
    }

    #[inline]
    pub(crate) fn enqueue_rx(
        &mut self,
        session_id: SessionId,
        index: BufferIndex,
        offset: u32,
        fin: bool,
    ) -> CoreResult<SessionRxEnqueue> {
        let len = self
            .buffers
            .current_len(index)?
            .checked_add(self.buffers.total_len_not_including_first(index)?)
            .ok_or_else(|| CoreError::internal("session rx chain length overflow"))?;
        let len = u32::try_from(len)
            .map_err(|_| CoreError::internal("session rx buffer length exceeds u32"))?;
        let entry = SessionRxBuffer {
            index,
            offset,
            len,
            fin,
        };
        let (first_future, insert_at) = {
            let queue = self.rx_queue_mut_or_alloc(session_id);
            let mut first_future = queue.len();
            for (position, current) in queue.iter().enumerate() {
                if current.offset != 0 {
                    first_future = position;
                    break;
                }
            }
            let mut insert_at = if offset == 0 { first_future } else { queue.len() };
            if offset != 0 {
                for position in first_future..queue.len() {
                    let current = queue
                        .get(position)
                        .copied()
                        .ok_or_else(|| CoreError::internal("session rx queue slot is invalid"))?;
                    if offset < current.offset {
                        insert_at = position;
                        break;
                    }
                }
            }
            queue.insert(insert_at, entry);
            (first_future, insert_at)
        };
        if offset == 0 {
            let mut delivered_len = len;
            let mut position = insert_at + 1;
            loop {
                let current = {
                    let queue = self.rx_queue_mut_or_alloc(session_id);
                    queue.get(position).copied()
                };
                let Some(current) = current else {
                    break;
                };
                if current.offset > delivered_len {
                    break;
                }
                let current_end = current
                    .offset
                    .checked_add(current.len)
                    .ok_or_else(|| CoreError::internal("session rx end offset overflow"))?;
                if current_end <= delivered_len {
                    let removed = self.rx_queue_mut_or_alloc(session_id).remove(position);
                    self.buffers.free_index(removed.index);
                    continue;
                }
                if current.offset < delivered_len {
                    let trim = delivered_len - current.offset;
                    let trim = usize::try_from(trim)
                        .map_err(|_| CoreError::internal("session rx trim length exceeds usize"))?;
                    self.buffers.advance(current.index, trim)?;
                    let queue = self.rx_queue_mut_or_alloc(session_id);
                    let current = queue
                        .get_mut(position)
                        .ok_or_else(|| CoreError::internal("session rx queue slot is invalid"))?;
                    current.offset = 0;
                    current.len = current.len.saturating_sub(trim as u32);
                } else {
                    let current = self
                        .rx_queue_mut_or_alloc(session_id)
                        .get_mut(position)
                        .ok_or_else(|| CoreError::internal("session rx queue slot is invalid"))?;
                    current.offset = 0;
                }
                delivered_len = current_end;
                position += 1;
            }
            let mut newest_ooo_start = None;
            let mut newest_ooo_len = 0u32;
            let mut position = insert_at + 1;
            loop {
                let current = {
                    let queue = self.rx_queue_mut_or_alloc(session_id);
                    queue.get(position).copied()
                };
                let Some(current) = current else {
                    break;
                };
                if current.offset != 0 {
                    let rebased = current
                        .offset
                        .checked_sub(delivered_len)
                        .ok_or_else(|| CoreError::internal("session rx rebased offset underflow"))?;
                    let current = self
                        .rx_queue_mut_or_alloc(session_id)
                        .get_mut(position)
                        .ok_or_else(|| CoreError::internal("session rx queue slot is invalid"))?;
                    current.offset = rebased;
                    if newest_ooo_start.is_none() {
                        newest_ooo_start = Some(rebased);
                        newest_ooo_len = current.len;
                    }
                }
                position += 1;
            }
            return Ok(SessionRxEnqueue {
                delivered_len,
                newest_ooo_start,
                newest_ooo_len,
            });
        }
        let mut newest_ooo_start = None;
        let mut newest_ooo_len = 0u32;
        let queue_len = self.rx_queue_mut_or_alloc(session_id).len();
        for position in first_future..queue_len {
            let current = self
                .rx_queue_mut_or_alloc(session_id)
                .get(position)
                .copied()
                .ok_or_else(|| CoreError::internal("session rx queue slot is invalid"))?;
            if current.index == index {
                newest_ooo_start = Some(current.offset);
                newest_ooo_len = current.len;
                break;
            }
        }
        Ok(SessionRxEnqueue {
            delivered_len: 0,
            newest_ooo_start,
            newest_ooo_len,
        })
    }

    pub(crate) fn flush_session_rx(&mut self, session_id: SessionId) -> CoreResult<()> {
        let Some(index) = self.rx_index.lookup(&session_id.get()) else {
            return Ok(());
        };
        let Some(op) = self.session_app_op(session_id) else {
            return Ok(());
        };
        loop {
            let current = {
                let queue = self
                    .rx
                    .get_mut(index)
                    .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
                queue.front().copied()
            };
            let Some(current) = current else {
                break;
            };
            if current.offset != 0 {
                break;
            }
            let consumed = self
                .app
                .complete_recv(op, self.buffers.clone(), current.index, current.fin)?;
            if !consumed {
                break;
            }
            let queue = self
                .rx
                .get_mut(index)
                .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
            let _ = queue
                .pop_front()
                .ok_or_else(|| CoreError::internal("session rx buffer is missing"))?;
            if queue.is_empty() {
                self.rx_index.remove(&session_id.get());
                let _ = self.rx.remove(index);
                break;
            }
        }
        Ok(())
    }


    pub(crate) fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
        self.sessions.poll_once_for_ticks(timer_ticks)
    }

    pub(crate) fn poll_once_at(&mut self, now: Instant) -> CoreResult<SessionQueueStep> {
        let timer_ticks = self.sessions.elapsed_timer_ticks(now);
        self.poll_once_for_ticks(timer_ticks)
    }

    pub(crate) fn take_timer_expiries(&mut self) -> hammer_infra::vec::Vec<SessionTimerExpiry> {
        self.sessions.take_timer_expiries()
    }

    pub(crate) fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        self.sessions.take_ready_sessions()
    }

    pub(crate) fn arm_timer_ticks(
        &mut self,
        session_id: SessionId,
        token: SessionTimerToken,
        ticks: u64,
    ) -> CoreResult<()> {
        self.sessions.arm_timer_ticks(session_id, token, ticks)
    }

    pub(crate) fn cancel_timer(&mut self, session_id: SessionId, token: SessionTimerToken) -> bool {
        self.sessions.cancel_timer(session_id, token)
    }
}

#[cfg(test)]
pub(crate) fn dispatch_session_queue_for_ticks<S, A>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S, A>,
    timer_ticks: u32,
    output_next: crate::session::SessionQueueNext,
) -> CoreResult<SessionQueueStep>
where
    S: SessionQueueProtocol<A>,
{
    let mut step = driver.poll_once_for_ticks(timer_ticks)?;
    let now = Instant::now();
    let mut output = crate::session::node::SessionQueueOutput::default();
    dispatch_session_queue_pending(
        runtime,
        driver,
        output_next,
        &mut output,
        &mut step,
        now,
    )?;
    output.schedule(runtime)?;
    Ok(step)
}

pub(crate) fn dispatch_session_queue_once_at<S, A>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S, A>,
    now: Instant,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<SessionQueueStep>
where
    S: SessionQueueProtocol<A>,
{
    let mut step = driver.poll_once_at(now)?;
    dispatch_session_queue_pending(
        runtime,
        driver,
        output_next,
        output,
        &mut step,
        now,
    )?;
    Ok(step)
}

pub(crate) fn dispatch_session_queue_pending<S, A>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<S, A>,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
    step: &mut SessionQueueStep,
    now: Instant,
) -> CoreResult<()>
where
    S: SessionQueueProtocol<A>,
{
    driver.poll_app()?;
    let expiries = driver.take_timer_expiries();
    for expiry in expiries {
        let driver = driver as *mut SessionDriverRuntime<S, A>;
        // SAFETY: same disjoint-access argument as above.
        unsafe {
            let app_op = (*driver).session_app_op(expiry.session_id());
            let state = (*driver)
                .session_mut(expiry.session_id())
                .ok_or_else(|| CoreError::internal("session is missing"))?;
            let mut context = crate::session::protocol::SessionQueueControlContext::new(
                &mut (*driver).sessions as *mut _,
                &mut (*driver).app as *mut _,
                &(*driver).buffers as *const _,
                &mut (*driver).rx as *mut _,
                &mut (*driver).rx_index as *mut _,
                &mut (*driver).aux as *mut _,
                expiry.session_id(),
                app_op,
            );
            state.handle_timer_expiry(runtime, &mut context, expiry, output_next, output)?;
        }
    }
    let ready_sessions = driver.take_ready_sessions();
    step.ready_sessions = ready_sessions.len();
    for session_id in ready_sessions {
        loop {
            let Some(pending_len) = driver.app().pending_send_len(session_id)? else {
                break;
            };
            let payload_len = {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                // SAFETY: `state` and `context` access disjoint parts of the same
                // session queue: the state lives in `entries`, while the context
                // exposes runtime sidecar resources.
                unsafe {
                    let app_op = (*driver).session_app_op(session_id);
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = crate::session::protocol::SessionQueueControlContext::new(
                        &mut (*driver).sessions as *mut _,
                        &mut (*driver).app as *mut _,
                        &(*driver).buffers as *const _,
                        &mut (*driver).rx as *mut _,
                        &mut (*driver).rx_index as *mut _,
                        &mut (*driver).aux as *mut _,
                        session_id,
                        app_op,
                    );
                    state.tx_payload_len(&mut context, pending_len, now)?.min(pending_len)
                }
            };
            if payload_len == 0 {
                break;
            }

            let payload_index = driver.buffers().alloc_index(RouteMetadata::default())?;
            let Some(payload_len) = driver.app().copy_pending_send_to_buffer(
                session_id,
                payload_len,
                driver.buffers(),
                payload_index,
            )?
            else {
                driver.buffers().free_index(payload_index);
                return Err(CoreError::internal("session app tx progress is missing"));
            };
            if payload_len == 0 {
                driver.buffers().free_index(payload_index);
                if !driver.app_mut().commit_pending_send_bytes(session_id, 0)? {
                    break;
                }
                continue;
            }

            let index = driver.buffers().alloc_index(RouteMetadata::default())?;
            if let Err(err) = driver.buffers().attach_clone(index, payload_index) {
                driver.buffers().free_index(index);
                driver.buffers().free_index(payload_index);
                return Err(err);
            }

            {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                // SAFETY: same reasoning as above; the current session state and
                // runtime sidecars are accessed together to execute one transport
                // callback.
                unsafe {
                    let app_op = (*driver).session_app_op(session_id);
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = crate::session::protocol::SessionQueueControlContext::new(
                        &mut (*driver).sessions as *mut _,
                        &mut (*driver).app as *mut _,
                        &(*driver).buffers as *const _,
                        &mut (*driver).rx as *mut _,
                        &mut (*driver).rx_index as *mut _,
                        &mut (*driver).aux as *mut _,
                        session_id,
                        app_op,
                    );
                    if let Err(err) = state.prepare_tx(&mut context, index, payload_len, now) {
                        context.buffers().free_index(index);
                        context.buffers().free_index(payload_index);
                        return Err(err);
                    }
                }
            }

            if let Err(err) = output.enqueue(runtime, output_next.node(), index) {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                // SAFETY: same disjoint-access argument as above.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    state.cancel_tx(&mut (*driver).aux, index);
                    (*driver).buffers().free_index(index);
                    (*driver).buffers().free_index(payload_index);
                }
                return Err(err);
            }

            let commit_result = {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                // SAFETY: same disjoint-access argument as above.
                unsafe {
                    let app_op = (*driver).session_app_op(session_id);
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = crate::session::protocol::SessionQueueControlContext::new(
                        &mut (*driver).sessions as *mut _,
                        &mut (*driver).app as *mut _,
                        &(*driver).buffers as *const _,
                        &mut (*driver).rx as *mut _,
                        &mut (*driver).rx_index as *mut _,
                        &mut (*driver).aux as *mut _,
                        session_id,
                        app_op,
                    );
                    state.commit_tx(&mut context, index, payload_len, now)
                }
            };
            if let Err(err) = commit_result {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                // SAFETY: same disjoint-access argument as above.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    state.cancel_tx(&mut (*driver).aux, index);
                    (*driver).buffers().free_index(payload_index);
                }
                return Err(err);
            }

            let completed = driver
                .app_mut()
                .commit_pending_send_bytes(session_id, payload_len)?;
            driver.retain_tx_buffer(session_id, payload_index);
            if !completed {
                driver.mark_ready(session_id);
            }
        }
        let close_requested = driver.take_close_request(session_id);
        let driver = driver as *mut SessionDriverRuntime<S, A>;
        // SAFETY: same disjoint-access argument as above.
        unsafe {
            let app_op = (*driver).session_app_op(session_id);
            let state = (*driver)
                .session_mut(session_id)
                .ok_or_else(|| CoreError::internal("session is missing"))?;
            let mut context = crate::session::protocol::SessionQueueControlContext::new(
                &mut (*driver).sessions as *mut _,
                &mut (*driver).app as *mut _,
                &(*driver).buffers as *const _,
                &mut (*driver).rx as *mut _,
                &mut (*driver).rx_index as *mut _,
                &mut (*driver).aux as *mut _,
                session_id,
                app_op,
            );
            state.handle_ready_session(runtime, &mut context, close_requested, output_next, output)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, OnceLock};

    use hammer_adapter::{
        BufferFrame, InternalNode, Node, NodeId, NodeProcessFn, NodeRegistration, NodeResult,
        NodeRuntimeData,
    };
    use hammer_runtime::app::{AppRingHandle, AppSendData};

    use crate::session::protocol::SessionQueueControlContext;

    fn infra_vec<T>(items: impl IntoIterator<Item = T>) -> hammer_infra::vec::Vec<T> {
        let mut values = hammer_infra::vec::Vec::new();
        for item in items {
            values.push(item);
        }
        values
    }

    #[derive(Debug, Clone, Default)]
    struct FakeTxState {
        prepared: usize,
        committed: usize,
        canceled: usize,
    }

    struct FakeTxProtocol;

    impl SessionQueueProtocol<FakeTxState> for FakeTxProtocol {
        fn handle_timer_expiry(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionTimerExpiry,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<()> {
            Ok(())
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: bool,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<()> {
            Ok(())
        }

        fn tx_payload_len(
            &mut self,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            pending_len: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Ok(pending_len.min(4))
        }

        fn prepare_tx(
            &mut self,
            context: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: BufferIndex,
            payload_len: usize,
            _: Instant,
        ) -> CoreResult<()> {
            let state = context.aux_mut();
            state.prepared += payload_len;
            Ok(())
        }

        fn cancel_tx(&mut self, _: &mut FakeTxState, _: BufferIndex) {}

        fn commit_tx(
            &mut self,
            context: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: BufferIndex,
            payload_len: usize,
            _: Instant,
        ) -> CoreResult<()> {
            let state = context.aux_mut();
            state.committed += payload_len;
            Ok(())
        }
    }

    struct NoTxPayloadProtocol;

    impl SessionQueueProtocol<FakeTxState> for NoTxPayloadProtocol {
        fn handle_timer_expiry(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: SessionTimerExpiry,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<()> {
            Ok(())
        }

        fn handle_ready_session(
            &mut self,
            _: &DataPlaneRuntime,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: bool,
            _: crate::session::SessionQueueNext,
            _: &mut crate::session::node::SessionQueueOutput,
        ) -> CoreResult<()> {
            Ok(())
        }

        fn tx_payload_len(
            &mut self,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: usize,
            _: Instant,
        ) -> CoreResult<usize> {
            Ok(0)
        }

        fn prepare_tx(
            &mut self,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: BufferIndex,
            _: usize,
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport tx prepare must not run"))
        }

        fn cancel_tx(&mut self, _: &mut FakeTxState, _: BufferIndex) {}

        fn commit_tx(
            &mut self,
            _: &mut SessionQueueControlContext<'_, FakeTxState>,
            _: BufferIndex,
            _: usize,
            _: Instant,
        ) -> CoreResult<()> {
            Err(CoreError::internal("transport tx commit must not run"))
        }
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
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> CoreResult<NodeResult> {
            Err(CoreError::internal(
                "capture node must use descriptor process",
            ))
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

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let slot = data.usize_word(0)?;
        let state = {
            let states = capture_states().lock().expect("capture registry");
            Arc::clone(
                states
                    .get(slot)
                    .ok_or_else(|| CoreError::internal("capture slot is invalid"))?,
            )
        };
        let mut state = state.lock().expect("capture state");
        for index in frame.drain_pending() {
            let packet = runtime.copy_current_chain(index)?;
            state.packets.push(packet.to_vec());
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }

    #[test]
    fn worker_session_runtime_expires_timer_into_expiry_and_ready_session() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(9);
        let token = SessionTimerToken::new(17);
        let mut runtime = WorkerSessionRuntime::new(worker);

        runtime
            .arm_timer_ticks(session_id, token, 3)
            .expect("arm session timer");

        assert_eq!(runtime.expire_timers(2).expect("expire before deadline"), 0);
        assert!(runtime.take_timer_expiries().is_empty());
        assert!(runtime.take_ready_sessions().is_empty());

        assert_eq!(runtime.expire_timers(1).expect("expire at deadline"), 1);
        assert_eq!(
            runtime.take_timer_expiries(),
            infra_vec([SessionTimerExpiry::new(session_id, token)])
        );
        assert_eq!(runtime.take_ready_sessions(), infra_vec([session_id]));
    }

    #[test]
    fn worker_session_runtime_rearming_same_timer_suppresses_stale_expiry() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(10);
        let token = SessionTimerToken::new(3);
        let mut runtime = WorkerSessionRuntime::new(worker);

        runtime
            .arm_timer_ticks(session_id, token, 2)
            .expect("arm first timer");
        runtime
            .arm_timer_ticks(session_id, token, 5)
            .expect("rearm timer");

        assert_eq!(runtime.expire_timers(2).expect("expire stale timer"), 0);
        assert!(runtime.take_timer_expiries().is_empty());
        assert!(runtime.take_ready_sessions().is_empty());

        assert_eq!(runtime.expire_timers(3).expect("expire rearmed timer"), 1);
        assert_eq!(runtime.take_ready_sessions(), infra_vec([session_id]));
    }

    #[test]
    fn worker_session_runtime_cancel_timer_suppresses_expiry() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(11);
        let token = SessionTimerToken::new(4);
        let mut runtime = WorkerSessionRuntime::new(worker);

        runtime
            .arm_timer_ticks(session_id, token, 2)
            .expect("arm timer");
        assert!(runtime.cancel_timer(session_id, token));

        assert_eq!(runtime.expire_timers(2).expect("expire canceled timer"), 0);
        assert!(runtime.take_timer_expiries().is_empty());
        assert!(runtime.take_ready_sessions().is_empty());
    }

    #[test]
    fn worker_session_runtime_advances_timer_wheel_from_elapsed_clock_ticks() {
        let worker = DataWorkerId::new(0);
        let session_id = SessionId::new(32);
        let token = SessionTimerToken::new(5);
        let start = Instant::now();
        let mut runtime =
            WorkerSessionRuntime::with_timer_clock(worker, Duration::from_millis(10), start);

        runtime
            .arm_timer_ticks(session_id, token, 2)
            .expect("arm timer");

        let first_ticks = runtime.elapsed_timer_ticks(start + Duration::from_millis(10));
        let first = runtime
            .poll_once_for_ticks(first_ticks)
            .expect("first poll");
        assert_eq!(first.expired_timers, 0);
        assert!(runtime.take_timer_expiries().is_empty());

        let second_ticks = runtime.elapsed_timer_ticks(start + Duration::from_millis(20));
        let second = runtime
            .poll_once_for_ticks(second_ticks)
            .expect("second poll");
        assert_eq!(second.expired_timers, 1);
        assert_eq!(
            runtime.take_timer_expiries(),
            infra_vec([SessionTimerExpiry::new(session_id, token)])
        );
    }

    #[test]
    fn session_tx_copies_app_send_and_commits_progress() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol);
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"abcdef").expect("data"))
            .try_into()
            .expect("transfer");
        driver.app_mut().push_pending_send(session_id, send);
        driver.mark_ready(session_id);

        let capture = Arc::new(Mutex::new(CaptureState::default()));
        let output_node = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&capture)));
        let next = crate::session::SessionQueueNext::from_node(output_node);
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
        .expect("dispatch");

        assert_eq!(driver.aux().prepared, 6);
        assert_eq!(driver.aux().committed, 6);
        assert_eq!(driver.aux().canceled, 0);
        assert!(!driver.app().has_pending_send(session_id));
        assert!(driver.has_retained_tx(session_id));

        output.schedule(&runtime).expect("schedule output");
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 1);
        let packets = &capture.lock().expect("capture").packets;
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].as_slice(), b"abcd");
        assert_eq!(packets[1].as_slice(), b"ef");
    }

    #[test]
    fn session_tx_does_not_call_transport_when_app_has_no_pending_send() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol);
        driver.mark_ready(session_id);
        let next = crate::session::SessionQueueNext::from_node(NodeId::new(9));
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

        assert_eq!(driver.aux().prepared, 0);
        assert_eq!(driver.aux().committed, 0);
        assert_eq!(driver.aux().canceled, 0);
        assert!(!driver.has_retained_tx(session_id));
    }

    #[test]
    fn session_tx_keeps_pending_send_when_transport_has_no_capacity() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(NoTxPayloadProtocol);
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"pending").expect("data"))
            .try_into()
            .expect("transfer");
        driver.app_mut().push_pending_send(session_id, send);
        driver.mark_ready(session_id);

        let next = crate::session::SessionQueueNext::from_node(NodeId::new(9));
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
        .expect("dispatch without capacity");

        assert_eq!(driver.aux().prepared, 0);
        assert_eq!(driver.aux().committed, 0);
        assert_eq!(driver.aux().canceled, 0);
        assert!(driver.app().has_pending_send(session_id));
        assert!(!driver.has_retained_tx(session_id));
    }

}

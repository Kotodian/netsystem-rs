use std::time::{Duration, Instant};

use hammer_adapter::{BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, NodeRuntimeData};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo::FifoQueue;
use hammer_infra::map::FlatHashTable;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::rbtree::RbTree;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::app::AppOpId;
#[cfg(test)]
use hammer_runtime::app::AppRingHandle;

use crate::session::{
    SessionAppRuntime, SessionId, SessionQueueHandle, SessionQueueNext, SessionReadyQueue,
};

const DEFAULT_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;
const SESSION_TIMER_KIND_COUNT: usize = u32::BITS as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExpiredTimer {
    session_id: SessionId,
    timer_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SessionRxBuffer {
    pub(crate) index: BufferIndex,
    pub(crate) offset: u32,
    pub(crate) len: u32,
    pub(crate) fin: bool,
}

#[derive(Debug)]
pub(crate) struct SessionRxQueue {
    delivered: FifoQueue<SessionRxBuffer>,
    ooo_base: u32,
    ooo_entries: Pool<SessionRxBuffer>,
    ooo_index: RbTree<u32, PoolIndex>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionRxEnqueue {
    pub(crate) delivered_len: u32,
    pub(crate) newest_ooo_start: Option<u32>,
    pub(crate) newest_ooo_len: u32,
}

impl SessionRxQueue {
    const DEFAULT_OOO_CAPACITY: usize = 8;

    #[inline]
    fn new() -> Self {
        Self {
            delivered: FifoQueue::new(),
            ooo_base: 0,
            ooo_entries: Pool::with_capacity(Self::DEFAULT_OOO_CAPACITY),
            ooo_index: RbTree::with_capacity(Self::DEFAULT_OOO_CAPACITY),
        }
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.delivered.len() + self.ooo_entries.len()
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.delivered.is_empty() && self.ooo_entries.is_empty()
    }

    #[inline]
    pub(crate) fn front(&self) -> Option<&SessionRxBuffer> {
        self.delivered.front()
    }

    #[inline]
    pub(crate) fn pop_front(&mut self) -> Option<SessionRxBuffer> {
        self.delivered.pop_front()
    }

    fn first_ooo_relative(&self) -> Option<(u32, u32)> {
        let (offset, entry_index) = self
            .ooo_index
            .first()
            .map(|(offset, entry_index)| (*offset, *entry_index))?;
        let entry = self
            .ooo_entries
            .get(entry_index)
            .expect("session rx OOO entry index is valid");
        Some((
            offset
                .checked_sub(self.ooo_base)
                .expect("session rx OOO offset should not precede base"),
            entry.len,
        ))
    }

    fn first_ooo_at_or_after(&self, offset: u32) -> Option<(u32, PoolIndex)> {
        if let Some(entry_index) = self.ooo_index.get(&offset).copied() {
            return Some((offset, entry_index));
        }
        if offset == 0 {
            return self
                .ooo_index
                .first()
                .map(|(key, entry_index)| (*key, *entry_index));
        }
        self.ooo_index
            .successor(&(offset - 1))
            .map(|(key, entry_index)| (*key, *entry_index))
    }

    fn remove_ooo_entry(&mut self, offset: u32, entry_index: PoolIndex) -> SessionRxBuffer {
        let removed = self
            .ooo_index
            .remove(&offset)
            .expect("session rx OOO tree entry should exist");
        debug_assert_eq!(removed, entry_index);
        self.ooo_entries
            .remove(entry_index)
            .expect("session rx OOO pool entry should exist")
    }

    fn insert_ooo(
        &mut self,
        buffers: &DataPlaneBuffers,
        mut entry: SessionRxBuffer,
    ) -> CoreResult<SessionRxEnqueue> {
        let mut start = self
            .ooo_base
            .checked_add(entry.offset)
            .ok_or_else(|| CoreError::internal("session rx OOO start offset overflow"))?;
        let mut end = start
            .checked_add(entry.len)
            .ok_or_else(|| CoreError::internal("session rx OOO end offset overflow"))?;

        if let Some((_, predecessor_index)) = self
            .ooo_index
            .predecessor(&start)
            .map(|(key, entry_index)| (*key, *entry_index))
        {
            let predecessor = self
                .ooo_entries
                .get(predecessor_index)
                .expect("session rx predecessor entry is valid");
            let predecessor_end = predecessor
                .offset
                .checked_add(predecessor.len)
                .ok_or_else(|| CoreError::internal("session rx predecessor end offset overflow"))?;
            if predecessor_end >= end {
                buffers.free_index(entry.index);
                return Ok(SessionRxEnqueue::default());
            }
            if predecessor_end > start {
                let trim = predecessor_end - start;
                let mut buffer = buffers.get_buffer_mut(entry.index)?;
                buffer.advance(
                    isize::try_from(usize::try_from(trim).map_err(|_| {
                        CoreError::internal("session rx predecessor trim exceeds usize")
                    })?)
                    .map_err(|_| {
                        CoreError::internal("session rx predecessor trim exceeds isize")
                    })?,
                )?;
                start = predecessor_end;
                entry.len = entry.len.saturating_sub(trim);
                entry.offset = start;
            }
        }

        end = start
            .checked_add(entry.len)
            .ok_or_else(|| CoreError::internal("session rx OOO end offset overflow"))?;
        if start >= end {
            buffers.free_index(entry.index);
            return Ok(SessionRxEnqueue::default());
        }

        let mut candidate = self.first_ooo_at_or_after(start);
        while let Some((candidate_offset, candidate_index)) = candidate {
            if candidate_offset > end {
                break;
            }
            let candidate_entry = self
                .ooo_entries
                .get(candidate_index)
                .expect("session rx OOO candidate entry is valid");
            let candidate_end = candidate_entry
                .offset
                .checked_add(candidate_entry.len)
                .ok_or_else(|| {
                    CoreError::internal("session rx OOO candidate end offset overflow")
                })?;
            if candidate_offset == start && candidate_end >= end {
                buffers.free_index(entry.index);
                return Ok(SessionRxEnqueue::default());
            }
            if candidate_end <= end {
                let removed = self.remove_ooo_entry(candidate_offset, candidate_index);
                buffers.free_index(removed.index);
                candidate = self
                    .ooo_index
                    .successor(&candidate_offset)
                    .map(|(key, entry_index)| (*key, *entry_index));
                continue;
            }
            if candidate_offset < end {
                let trim = end - candidate_offset;
                let current = self
                    .ooo_entries
                    .get_mut(candidate_index)
                    .expect("session rx OOO candidate entry is valid");
                let mut buffer = buffers.get_buffer_mut(current.index)?;
                buffer.advance(
                    isize::try_from(usize::try_from(trim).map_err(|_| {
                        CoreError::internal("session rx OOO successor trim exceeds usize")
                    })?)
                    .map_err(|_| {
                        CoreError::internal("session rx OOO successor trim exceeds isize")
                    })?,
                )?;
                current.offset = end;
                current.len = current.len.saturating_sub(trim);
                let _ = self
                    .ooo_index
                    .remove(&candidate_offset)
                    .expect("session rx OOO candidate key should exist");
                self.ooo_index.insert(end, candidate_index);
            }
            break;
        }

        entry.offset = start;
        let entry_index = self
            .ooo_entries
            .insert(entry)
            .expect("session rx OOO entry pool exhausted");
        self.ooo_index.insert(start, entry_index);
        let newest_ooo_start = start
            .checked_sub(self.ooo_base)
            .ok_or_else(|| CoreError::internal("session rx OOO start underflow"))?;
        Ok(SessionRxEnqueue {
            delivered_len: 0,
            newest_ooo_start: Some(newest_ooo_start),
            newest_ooo_len: end - start,
        })
    }

    fn insert_head(
        &mut self,
        buffers: &DataPlaneBuffers,
        mut entry: SessionRxBuffer,
    ) -> CoreResult<SessionRxEnqueue> {
        let base = self.ooo_base;
        let mut delivered_len = entry.len;
        entry.offset = 0;
        self.delivered.push_back(entry);

        while let Some((candidate_offset, candidate_index)) = self
            .ooo_index
            .first()
            .map(|(key, entry_index)| (*key, *entry_index))
        {
            let boundary = base
                .checked_add(delivered_len)
                .ok_or_else(|| CoreError::internal("session rx contiguous boundary overflow"))?;
            if candidate_offset > boundary {
                break;
            }
            let mut candidate = self.remove_ooo_entry(candidate_offset, candidate_index);
            let candidate_end = candidate.offset.checked_add(candidate.len).ok_or_else(|| {
                CoreError::internal("session rx OOO candidate end offset overflow")
            })?;
            if candidate_end <= boundary {
                buffers.free_index(candidate.index);
                continue;
            }
            if candidate.offset < boundary {
                let trim = boundary - candidate.offset;
                let mut buffer = buffers.get_buffer_mut(candidate.index)?;
                buffer.advance(
                    isize::try_from(usize::try_from(trim).map_err(|_| {
                        CoreError::internal("session rx contiguous trim exceeds usize")
                    })?)
                    .map_err(|_| CoreError::internal("session rx contiguous trim exceeds isize"))?,
                )?;
                candidate.offset = boundary;
                candidate.len = candidate.len.saturating_sub(trim);
            }
            delivered_len = candidate
                .offset
                .checked_add(candidate.len)
                .and_then(|end| end.checked_sub(base))
                .ok_or_else(|| CoreError::internal("session rx delivered length overflow"))?;
            candidate.offset = 0;
            self.delivered.push_back(candidate);
        }

        self.ooo_base = base
            .checked_add(delivered_len)
            .ok_or_else(|| CoreError::internal("session rx OOO base overflow"))?;
        let (newest_ooo_start, newest_ooo_len) = self.first_ooo_relative().unwrap_or((0, 0));
        Ok(SessionRxEnqueue {
            delivered_len,
            newest_ooo_start: (newest_ooo_len != 0).then_some(newest_ooo_start),
            newest_ooo_len,
        })
    }

    fn free_all(mut self, buffers: &DataPlaneBuffers) {
        while let Some(buffer) = self.delivered.pop_front() {
            buffers.free_index(buffer.index);
        }
        while let Some((offset, entry_index)) = self
            .ooo_index
            .first()
            .map(|(key, entry_index)| (*key, *entry_index))
        {
            let removed = self.remove_ooo_entry(offset, entry_index);
            buffers.free_index(removed.index);
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SessionQueueStep {
    pub(crate) expired_timers: usize,
    pub(crate) ready_sessions: usize,
}

pub struct WorkerSessionRuntime {
    worker: DataWorkerId,
    ready: SessionReadyQueue,
    timers: TimerWheel1t2w2048sl<u32>,
    expired_timers: hammer_infra::vec::Vec<u32>,
    pending_timers: hammer_infra::vec::Vec<ExpiredTimer>,
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
            pending_timers: hammer_infra::vec::Vec::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
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
            self.pending_timers.push(ExpiredTimer {
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

pub(crate) struct SessionDriverRuntime<S, A> {
    sessions: WorkerSessionRuntime,
    entries: Pool<S>,
    buffers: DataPlaneBuffers,
    rx: Pool<SessionRxQueue>,
    rx_index: FlatHashTable<u64, PoolIndex>,
    pending_closes: SessionReadyQueue,
    app: SessionAppRuntime,
    app_ops: FlatHashTable<u64, AppOpId>,
    aux: A,
}

pub(crate) trait SessionQueueProtocol<A>: Sized {
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

    fn cancel_tx(&mut self, aux: &mut A, index: BufferIndex);

    fn commit_tx(
        &mut self,
        context: &mut crate::session::protocol::SessionQueueControlContext,
        index: BufferIndex,
        tx_offset: usize,
        payload_len: usize,
        now: Instant,
    ) -> CoreResult<()>;

    fn on_close(&mut self, aux: &mut A, session_id: SessionId);
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
        let app = SessionAppRuntime::new(buffers.clone());
        Self {
            sessions: WorkerSessionRuntime::new(worker),
            entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            buffers,
            rx: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            rx_index: FlatHashTable::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            pending_closes: SessionReadyQueue::new(),
            app,
            app_ops: FlatHashTable::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
            aux,
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
    pub(crate) fn insert_session_with_id<F>(&mut self, f: F) -> CoreResult<SessionId>
    where
        F: SessionStateFactory<S>,
    {
        let index = self
            .entries
            .insert_with(|index| f.build(SessionId::from(index)))
            .ok_or_else(|| CoreError::internal("session pool capacity exhausted"))?;
        Ok(SessionId::from(index))
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
        self.app.free_pending_send(id);
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
    #[cfg(test)]
    pub(crate) fn app_mut(&mut self) -> &mut SessionAppRuntime {
        &mut self.app
    }

    #[inline]
    pub(crate) fn buffers(&self) -> &DataPlaneBuffers {
        &self.buffers
    }

    #[inline]
    pub(crate) fn timers_mut(&mut self) -> &mut TimerWheel1t2w2048sl<u32> {
        &mut self.sessions.timers
    }

    #[inline]
    pub(crate) fn ready_mut_ptr(&mut self) -> *mut SessionReadyQueue {
        &mut self.sessions.ready as *mut _
    }

    #[cfg(test)]
    pub(crate) fn has_session_tx(&self, session_id: SessionId) -> bool {
        self.app.has_pending_send(session_id)
    }

    pub(crate) fn release_tx_up_to(
        &mut self,
        session_id: SessionId,
        bytes: usize,
    ) -> CoreResult<()> {
        let _ = self.app.release_pending_send_bytes(session_id, bytes)?;
        Ok(())
    }

    fn release_session_rx(&mut self, session_id: SessionId) {
        let Some(index) = self.rx_index.remove(&session_id.get()) else {
            return;
        };
        let Some(queue) = self.rx.remove(index) else {
            return;
        };
        queue.free_all(&self.buffers);
    }

    pub(crate) fn poll_app(&mut self) -> CoreResult<()> {
        self.app.drain_submissions()?;
        let mut ready_tx = hammer_infra::vec::Vec::new();
        self.app.take_ready_tx_sessions(&mut ready_tx);
        for session_id in ready_tx {
            self.mark_ready(session_id);
        }
        let mut ready = hammer_infra::vec::Vec::new();
        self.app.take_ready_sessions(&mut ready);
        for session_id in ready {
            self.flush_session_rx(session_id)?;
            self.mark_ready(session_id);
        }
        for close in self.app.take_drained_closes() {
            self.pending_closes.mark_ready(close.session_id());
            self.mark_ready(close.session_id());
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
        let key = session_id.get();
        let rx_index_table = &mut self.rx_index;
        let rx = &mut self.rx;
        let buffers = self.buffers.clone();
        let rx_index = match rx_index_table.lookup(&key) {
            Some(index) => index,
            None => {
                let index = rx
                    .insert(SessionRxQueue::new())
                    .expect("session rx queue pool exhausted");
                rx_index_table.insert(key, index);
                index
            }
        };
        let len = buffers
            .current_len(index)?
            .checked_add(buffers.total_len_not_including_first(index)?)
            .ok_or_else(|| CoreError::internal("session rx chain length overflow"))?;
        let len = u32::try_from(len)
            .map_err(|_| CoreError::internal("session rx buffer length exceeds u32"))?;
        let entry = SessionRxBuffer {
            index,
            offset,
            len,
            fin,
        };
        let queue = rx
            .get_mut(rx_index)
            .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
        if offset == 0 {
            return queue.insert_head(&buffers, entry);
        }
        queue.insert_ooo(&buffers, entry)
    }

    pub(crate) fn flush_session_rx(&mut self, session_id: SessionId) -> CoreResult<()> {
        let key = session_id.get();
        let rx_index = &mut self.rx_index;
        let Some(index) = rx_index.lookup(&key) else {
            return Ok(());
        };
        let app_ops = &self.app_ops;
        let Some(op) = app_ops.lookup(&key) else {
            return Ok(());
        };
        let rx = &mut self.rx;
        let app = &self.app;
        let buffers = self.buffers.clone();
        loop {
            let current = {
                let queue = rx
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
            let consumed = app.complete_recv(op, buffers.clone(), current.index, current.fin)?;
            if !consumed {
                break;
            }
            let queue = rx
                .get_mut(index)
                .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
            let _ = queue
                .pop_front()
                .ok_or_else(|| CoreError::internal("session rx buffer is missing"))?;
            if queue.is_empty() {
                rx_index.remove(&key);
                let _ = rx.remove(index);
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

    pub(crate) fn take_ready_sessions(&mut self) -> hammer_infra::vec::Vec<SessionId> {
        self.sessions.take_ready_sessions()
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
    dispatch_session_queue_pending(runtime, driver, output_next, &mut output, &mut step, now)?;
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
    dispatch_session_queue_pending(runtime, driver, output_next, output, &mut step, now)?;
    Ok(step)
}

pub(crate) fn dispatch_registered_session_queue_once_at<S, A>(
    runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: Instant,
    output: &mut crate::session::node::SessionQueueOutput,
) -> CoreResult<()>
where
    S: SessionQueueProtocol<A> + 'static,
    A: 'static,
{
    let mut driver = SessionQueueHandle::<SessionDriverRuntime<S, A>>::new(data).borrow_mut()?;
    dispatch_session_queue_once_at(runtime, &mut driver, now, output_next, output)?;
    Ok(())
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
    let expired_timers: hammer_infra::vec::Vec<_> =
        driver.sessions.pending_timers.drain(..).collect();
    for expired_timer in expired_timers {
        let driver = driver as *mut SessionDriverRuntime<S, A>;
        // SAFETY: same disjoint-access argument as above.
        unsafe {
            let state = (*driver)
                .session_mut(expired_timer.session_id)
                .ok_or_else(|| CoreError::internal("session is missing"))?;
            let mut context = crate::session::protocol::SessionQueueControlContext::new(
                &mut (*driver).sessions.timers as *mut _,
                &mut (*driver).sessions.ready as *mut _,
                &(*driver).buffers as *const _,
                expired_timer.session_id,
                (*driver)
                    .app
                    .pending_send_head(expired_timer.session_id)
                    .is_some(),
            );
            let close_current = state.handle_expired_timer(
                runtime,
                &mut context,
                expired_timer.timer_id,
                output_next,
                output,
            )?;
            if close_current {
                state.on_close(&mut (*driver).aux, expired_timer.session_id);
                let _ = (*driver).close_session(expired_timer.session_id)?;
            }
        }
    }
    let ready_sessions = driver.take_ready_sessions();
    step.ready_sessions = ready_sessions.len();
    for session_id in ready_sessions {
        if driver.session(session_id).is_none() {
            continue;
        }
        let close_requested = driver.take_close_request(session_id);
        let driver_ptr = driver as *mut SessionDriverRuntime<S, A>;
        // SAFETY: same disjoint-access argument as above.
        unsafe {
            let state = (*driver_ptr)
                .session_mut(session_id)
                .ok_or_else(|| CoreError::internal("session is missing"))?;
            let mut context = crate::session::protocol::SessionQueueControlContext::new(
                &mut (*driver_ptr).sessions.timers as *mut _,
                &mut (*driver_ptr).sessions.ready as *mut _,
                &(*driver_ptr).buffers as *const _,
                session_id,
                (*driver_ptr).app.pending_send_head(session_id).is_some(),
            );
            let close_current = state.handle_ready_session(
                runtime,
                &mut context,
                close_requested,
                output_next,
                output,
            )?;
            if close_current {
                state.on_close(&mut (*driver_ptr).aux, session_id);
                let _ = (*driver_ptr).close_session(session_id)?;
                continue;
            }
        }
        loop {
            let Some(tx_head) = driver.app.pending_send_head(session_id) else {
                break;
            };
            let total_len = driver
                .buffers()
                .current_len(tx_head)?
                .checked_add(driver.buffers().total_len_not_including_first(tx_head)?)
                .ok_or_else(|| CoreError::internal("session tx chain length overflow"))?;
            let tx_offset = {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                unsafe {
                    let state = (*driver)
                        .session(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let context = crate::session::protocol::SessionQueueControlContext::new(
                        &mut (*driver).sessions.timers as *mut _,
                        &mut (*driver).sessions.ready as *mut _,
                        &(*driver).buffers as *const _,
                        session_id,
                        (*driver).app.pending_send_head(session_id).is_some(),
                    );
                    state.tx_offset(&context)?
                }
            };
            if tx_offset > total_len {
                return Err(CoreError::internal(
                    "session tx offset exceeds chain length",
                ));
            }
            let pending_len = total_len.saturating_sub(tx_offset);
            let payload_len = {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                // SAFETY: `state` and `context` access disjoint parts of the same
                // session queue: the state lives in `entries`, while the context
                // exposes runtime sidecar resources.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = crate::session::protocol::SessionQueueControlContext::new(
                        &mut (*driver).sessions.timers as *mut _,
                        &mut (*driver).sessions.ready as *mut _,
                        &(*driver).buffers as *const _,
                        session_id,
                        (*driver).app.pending_send_head(session_id).is_some(),
                    );
                    state
                        .tx_payload_len(&mut context, tx_offset, pending_len, now)?
                        .min(pending_len)
                }
            };
            if payload_len == 0 {
                break;
            }

            let index = driver.buffers().alloc_index()?;
            let append_result = (|| {
                let mut skip = tx_offset;
                let mut remaining = payload_len;
                let mut current = Some(tx_head);
                while remaining != 0 {
                    let current_index = current
                        .ok_or_else(|| CoreError::internal("session tx chain ended early"))?;
                    let (segment_len, copy_ptr, copy_len, next) = {
                        let buffer = driver.buffers().get_buffer(current_index)?;
                        let segment_len = buffer.current_len();
                        if skip >= segment_len {
                            (segment_len, std::ptr::null(), 0, buffer.next_buffer())
                        } else {
                            let take = remaining.min(segment_len - skip);
                            (
                                segment_len,
                                unsafe { buffer.current_ptr().add(skip) },
                                take,
                                buffer.next_buffer(),
                            )
                        }
                    };
                    if copy_len != 0 {
                        // SAFETY: source bytes remain valid for the duration of this
                        // copy because session TX ownership stays in the source chain.
                        let bytes = unsafe { std::slice::from_raw_parts(copy_ptr, copy_len) };
                        driver.buffers().append(index, bytes)?;
                        remaining -= copy_len;
                        skip = 0;
                    } else {
                        skip -= segment_len;
                    }
                    current = next;
                }
                Ok(())
            })();
            if let Err(error) = append_result {
                driver.buffers().free_index(index);
                return Err(error);
            }

            {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                // SAFETY: same reasoning as above; the current session state and
                // runtime sidecars are accessed together to execute one transport
                // callback.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = crate::session::protocol::SessionQueueControlContext::new(
                        &mut (*driver).sessions.timers as *mut _,
                        &mut (*driver).sessions.ready as *mut _,
                        &(*driver).buffers as *const _,
                        session_id,
                        (*driver).app.pending_send_head(session_id).is_some(),
                    );
                    if let Err(err) =
                        state.prepare_tx(&mut context, index, tx_offset, payload_len, now)
                    {
                        context.buffers().free_index(index);
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
                }
                return Err(err);
            }

            let commit_result = {
                let driver = driver as *mut SessionDriverRuntime<S, A>;
                // SAFETY: same disjoint-access argument as above.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = crate::session::protocol::SessionQueueControlContext::new(
                        &mut (*driver).sessions.timers as *mut _,
                        &mut (*driver).sessions.ready as *mut _,
                        &(*driver).buffers as *const _,
                        session_id,
                        (*driver).app.pending_send_head(session_id).is_some(),
                    );
                    state.commit_tx(&mut context, index, tx_offset, payload_len, now)
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
                }
                return Err(err);
            }
            let remaining = driver.app.pending_send_len(session_id)?.unwrap_or(0);
            if remaining > 0 {
                driver.mark_ready(session_id);
            }
            break;
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
    use hammer_runtime::app::{AppRingHandle, AppSendData, AppSqe};

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

    #[derive(Default)]
    struct FakeTxProtocol {
        prepared: usize,
        committed: usize,
        canceled: usize,
    }

    impl SessionQueueProtocol<FakeTxState> for FakeTxProtocol {
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

        fn cancel_tx(&mut self, _: &mut FakeTxState, _: BufferIndex) {
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

        fn on_close(&mut self, _: &mut FakeTxState, _: SessionId) {}
    }

    struct NoTxPayloadProtocol;

    impl SessionQueueProtocol<FakeTxState> for NoTxPayloadProtocol {
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

        fn cancel_tx(&mut self, _: &mut FakeTxState, _: BufferIndex) {}

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

        fn on_close(&mut self, _: &mut FakeTxState, _: SessionId) {}
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
        assert_eq!(
            runtime.pending_timers,
            infra_vec([ExpiredTimer {
                session_id,
                timer_id,
            }])
        );
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
        assert_eq!(
            runtime.pending_timers,
            infra_vec([ExpiredTimer {
                session_id,
                timer_id,
            }])
        );
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
        assert_eq!(
            runtime.pending_timers,
            infra_vec([ExpiredTimer {
                session_id,
                timer_id,
            }])
        );
    }

    #[test]
    fn enqueue_rx_keeps_ordered_delivery_and_preserves_ooo_offsets() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol::default());
        let op = AppOpId::new(7);
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        assert!(driver.bind_session_app_ring(session_id, op, ring.clone()));

        let future = buffers.alloc_index().expect("future buffer");
        buffers.append(future, b"cdef").expect("future payload");
        let future_enqueue = driver
            .enqueue_rx(session_id, future, 2, false)
            .expect("enqueue future");
        assert_eq!(future_enqueue.delivered_len, 0);
        assert_eq!(future_enqueue.newest_ooo_start, Some(2));
        assert_eq!(future_enqueue.newest_ooo_len, 4);

        let head = buffers.alloc_index().expect("head buffer");
        buffers.append(head, b"ab").expect("head payload");
        let head_enqueue = driver
            .enqueue_rx(session_id, head, 0, false)
            .expect("enqueue head");
        assert_eq!(head_enqueue.delivered_len, 6);
        assert_eq!(head_enqueue.newest_ooo_start, None);
        assert_eq!(head_enqueue.newest_ooo_len, 0);

        let rx_index = driver
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.rx.get(rx_index).expect("rx queue");
        assert_eq!(queue.len(), 2);
        assert_eq!(queue.delivered.len(), 2);
        assert_eq!(queue.delivered.get(0).expect("head slot").offset, 0);
        assert_eq!(queue.delivered.get(0).expect("head slot").len, 2);
        assert_eq!(queue.delivered.get(1).expect("future slot").offset, 0);
        assert_eq!(queue.delivered.get(1).expect("future slot").len, 4);

        ring.push_test_submission(AppSqe::recv(None, op, 64))
            .expect("queue recv submission");
        driver.flush_session_rx(session_id).expect("flush rx");
        let completions = ring.take_test_completions(4);
        assert_eq!(completions.len(), 1);

        let rx_index = driver
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue still present after one recv completion");
        let queue = driver.rx.get(rx_index).expect("rx queue");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.front().expect("remaining slot").offset, 0);
        assert_eq!(queue.front().expect("remaining slot").len, 4);

        ring.push_test_submission(AppSqe::recv(None, op, 64))
            .expect("queue second recv submission");
        driver
            .flush_session_rx(session_id)
            .expect("flush remaining rx");
        let completions = ring.take_test_completions(4);
        assert_eq!(completions.len(), 1);
        assert!(driver.rx_index.lookup(&session_id.get()).is_none());
    }

    #[test]
    fn release_tx_up_to_advances_session_tx_chain() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol::default());
        let ring = AppRingHandle::with_data_area(8, 8, 256, 8).expect("ring");
        let send: AppSendData = ring
            .send_from_data(ring.alloc_data_for_bytes(b"abcdefgh").expect("data"))
            .try_into()
            .expect("transfer");
        driver.app_mut().push_pending_send(session_id, send);

        let first = driver
            .app()
            .pending_send_head(session_id)
            .expect("session tx head");

        driver
            .release_tx_up_to(session_id, 2)
            .expect("release partial");

        assert_eq!(driver.app.pending_send_head(session_id), Some(first));
        assert_eq!(
            buffers.copy_current_chain(first).expect("remaining first"),
            b"cdefgh"
        );

        driver
            .release_tx_up_to(session_id, 2)
            .expect("release rest of first");

        assert_eq!(driver.app.pending_send_head(session_id), Some(first));
        assert_eq!(
            buffers.copy_current_chain(first).expect("second intact"),
            b"efgh"
        );
    }

    #[test]
    fn session_rx_queue_inserts_future_segments_by_offset_order() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol::default());

        let later = buffers.alloc_index().expect("later buffer");
        buffers.append(later, b"ef").expect("later payload");
        let later_enqueue = driver
            .enqueue_rx(session_id, later, 4, false)
            .expect("enqueue later future");
        assert_eq!(later_enqueue.newest_ooo_start, Some(4));
        assert_eq!(later_enqueue.newest_ooo_len, 2);

        let earlier = buffers.alloc_index().expect("earlier buffer");
        buffers.append(earlier, b"cd").expect("earlier payload");
        let earlier_enqueue = driver
            .enqueue_rx(session_id, earlier, 2, false)
            .expect("enqueue earlier future");
        assert_eq!(earlier_enqueue.newest_ooo_start, Some(2));
        assert_eq!(earlier_enqueue.newest_ooo_len, 2);

        let rx_index = driver
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.rx.get(rx_index).expect("rx queue");
        let ooo_offsets: std::vec::Vec<_> =
            queue.ooo_index.iter().map(|(offset, _)| *offset).collect();
        assert_eq!(ooo_offsets, vec![2, 4]);
    }

    #[test]
    fn session_rx_queue_duplicate_covered_segment_is_discarded() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol::default());

        let original = buffers.alloc_index().expect("original buffer");
        buffers.append(original, b"cdef").expect("original payload");
        let original_enqueue = driver
            .enqueue_rx(session_id, original, 2, false)
            .expect("enqueue original");
        assert_eq!(original_enqueue.newest_ooo_start, Some(2));
        assert_eq!(original_enqueue.newest_ooo_len, 4);

        let duplicate = buffers.alloc_index().expect("duplicate buffer");
        buffers.append(duplicate, b"de").expect("duplicate payload");
        let duplicate_enqueue = driver
            .enqueue_rx(session_id, duplicate, 3, false)
            .expect("enqueue covered duplicate");
        assert_eq!(duplicate_enqueue, SessionRxEnqueue::default());

        let rx_index = driver
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.rx.get(rx_index).expect("rx queue");
        assert_eq!(queue.delivered.len(), 0);
        assert_eq!(queue.ooo_entries.len(), 1);
        assert_eq!(queue.first_ooo_relative(), Some((2, 4)));
    }

    #[test]
    fn session_rx_queue_overlap_trims_new_segment_against_predecessor() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol::default());

        let first = buffers.alloc_index().expect("first buffer");
        buffers.append(first, b"cdef").expect("first payload");
        let first_enqueue = driver
            .enqueue_rx(session_id, first, 2, false)
            .expect("enqueue first future");
        assert_eq!(first_enqueue.newest_ooo_start, Some(2));
        assert_eq!(first_enqueue.newest_ooo_len, 4);

        let overlap = buffers.alloc_index().expect("overlap buffer");
        buffers.append(overlap, b"efghij").expect("overlap payload");
        let overlap_enqueue = driver
            .enqueue_rx(session_id, overlap, 4, false)
            .expect("enqueue overlap future");
        assert_eq!(overlap_enqueue.newest_ooo_start, Some(6));
        assert_eq!(overlap_enqueue.newest_ooo_len, 4);

        let rx_index = driver
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.rx.get(rx_index).expect("rx queue");
        let ooo_offsets: std::vec::Vec<_> =
            queue.ooo_index.iter().map(|(offset, _)| *offset).collect();
        assert_eq!(ooo_offsets, vec![2, 6]);

        let overlap_index = *queue
            .ooo_index
            .get(&6)
            .expect("trimmed overlap entry should be reinserted at new offset");
        let overlap_entry = queue
            .ooo_entries
            .get(overlap_index)
            .expect("trimmed overlap entry");
        assert_eq!(overlap_entry.offset, 6);
        assert_eq!(overlap_entry.len, 4);
        assert_eq!(
            buffers
                .copy_current_chain(overlap_entry.index)
                .expect("trimmed overlap payload"),
            b"ghij"
        );
    }

    #[test]
    fn session_rx_queue_overlap_trims_existing_successor_and_rekeys_tree() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol::default());

        let successor = buffers.alloc_index().expect("successor buffer");
        buffers
            .append(successor, b"ghij")
            .expect("successor payload");
        let successor_enqueue = driver
            .enqueue_rx(session_id, successor, 6, false)
            .expect("enqueue successor future");
        assert_eq!(successor_enqueue.newest_ooo_start, Some(6));
        assert_eq!(successor_enqueue.newest_ooo_len, 4);

        let overlap = buffers.alloc_index().expect("overlap buffer");
        buffers.append(overlap, b"cdefgh").expect("overlap payload");
        let overlap_enqueue = driver
            .enqueue_rx(session_id, overlap, 2, false)
            .expect("enqueue overlap future");
        assert_eq!(overlap_enqueue.newest_ooo_start, Some(2));
        assert_eq!(overlap_enqueue.newest_ooo_len, 6);

        let rx_index = driver
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.rx.get(rx_index).expect("rx queue");
        let ooo_offsets: std::vec::Vec<_> =
            queue.ooo_index.iter().map(|(offset, _)| *offset).collect();
        assert_eq!(ooo_offsets, vec![2, 8]);

        let successor_index = *queue
            .ooo_index
            .get(&8)
            .expect("trimmed successor should be rekeyed");
        let successor_entry = queue
            .ooo_entries
            .get(successor_index)
            .expect("trimmed successor entry");
        assert_eq!(successor_entry.offset, 8);
        assert_eq!(successor_entry.len, 2);
        assert_eq!(
            buffers
                .copy_current_chain(successor_entry.index)
                .expect("trimmed successor payload"),
            b"ij"
        );
    }

    #[test]
    fn session_rx_queue_gap_close_promotes_contiguous_ooo_segments() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.packet_buffers();
        let mut driver = SessionDriverRuntime::new(
            DataWorkerId::new(0),
            buffers.clone(),
            FakeTxState::default(),
        );
        let session_id = driver.insert_session(FakeTxProtocol::default());

        let future = buffers.alloc_index().expect("future buffer");
        buffers.append(future, b"late").expect("future payload");
        let future_enqueue = driver
            .enqueue_rx(session_id, future, 4, false)
            .expect("enqueue future");
        assert_eq!(future_enqueue.newest_ooo_start, Some(4));
        assert_eq!(future_enqueue.newest_ooo_len, 4);

        let gap_closer = buffers.alloc_index().expect("gap closer buffer");
        buffers
            .append(gap_closer, b"gap-")
            .expect("gap closer payload");
        let gap_close_enqueue = driver
            .enqueue_rx(session_id, gap_closer, 0, false)
            .expect("enqueue gap closer");
        assert_eq!(gap_close_enqueue.delivered_len, 8);
        assert_eq!(gap_close_enqueue.newest_ooo_start, None);
        assert_eq!(gap_close_enqueue.newest_ooo_len, 0);

        let rx_index = driver
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.rx.get(rx_index).expect("rx queue");
        assert_eq!(queue.delivered.len(), 2);
        assert_eq!(queue.ooo_entries.len(), 0);
        assert_eq!(queue.ooo_index.len(), 0);
        assert_eq!(queue.delivered.get(0).expect("head slot").len, 4);
        assert_eq!(queue.delivered.get(1).expect("promoted slot").len, 4);
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
        let session_id = driver.insert_session(FakeTxProtocol::default());
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
        let next: crate::session::SessionQueueNext = output_node.into();
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

        let protocol = driver.session(session_id).expect("protocol state");
        assert_eq!(protocol.prepared, 4);
        assert_eq!(protocol.committed, 4);
        assert_eq!(protocol.canceled, 0);
        assert!(driver.has_session_tx(session_id));
        assert_eq!(
            driver
                .app()
                .pending_send_len(session_id)
                .expect("pending len"),
            Some(6)
        );

        output.schedule(&runtime).expect("schedule output");
        assert_eq!(runtime.run_ready_nodes().expect("run output"), 1);
        let packets = &capture.lock().expect("capture").packets;
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].as_slice(), b"abcd");
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
        .expect("dispatch without capacity");

        assert_eq!(driver.aux().prepared, 0);
        assert_eq!(driver.aux().committed, 0);
        assert_eq!(driver.aux().canceled, 0);
        assert!(driver.has_session_tx(session_id));
        assert_eq!(
            driver
                .app()
                .pending_send_len(session_id)
                .expect("pending len"),
            Some(7)
        );
    }
}

use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;
use hammer_adapter::{
    BufferIndex, DataPlaneBuffers, DataPlaneRuntime, DataWorkerId, NodeRuntimeData,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::fifo_queue::FifoQueue;
use hammer_infra::map::FlatHashTable;
use hammer_infra::msg_queue::MsgQueue;
use hammer_infra::pool::{Index as PoolIndex, Pool};
use hammer_infra::rbtree::RbTree;
use hammer_infra::segment::{Local, Segment};
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::app::{AppContext, AppSessionConfig, SessionHandle, with_current_app_worker};

use crate::session::{
    SessionAppRuntime, SessionId, SessionQueueHandle, SessionQueueNext, SessionReadyQueue,
};

const DEFAULT_SESSION_TIMER_TICK: Duration = Duration::from_millis(10);
const DEFAULT_SESSION_POOL_CAPACITY: usize = 1024;
const DEFAULT_SESSION_TX_EVENT_CAPACITY: usize = 2048;
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
    rx: Pool<SessionRxQueue>,
    rx_index: FlatHashTable<u64, PoolIndex>,
    pending_closes: SessionReadyQueue,
}

struct SessionDriverRuntimeAppState<Seg: Segment> {
    app: SessionAppRuntime<Seg>,
    app_context: Option<AppContext<Seg>>,
    app_session_config: AppSessionConfig,
}

pub(crate) struct SessionDriverRuntime<St, Seg: Segment = Local> {
    runtime: CachePadded<SessionDriverRuntimeCore<St>>,
    app_state: CachePadded<SessionDriverRuntimeAppState<Seg>>,
}

pub(crate) trait SessionQueueProtocol: Sized {
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
    pub(crate) fn new(worker: DataWorkerId, buffers: DataPlaneBuffers) -> Self
    where
        Seg: Default,
    {
        Self::with_app_session_config(worker, buffers, AppSessionConfig::default(), Seg::default())
    }

    #[inline]
    pub(crate) fn with_app_session_config(
        worker: DataWorkerId,
        buffers: DataPlaneBuffers,
        app_session_config: AppSessionConfig,
        seg: Seg,
    ) -> Self {
        let tx_evt_q = Arc::new(
            MsgQueue::<Seg>::new(seg, DEFAULT_SESSION_TX_EVENT_CAPACITY, false)
                .expect("session tx event queue capacity is valid"),
        );
        let app = SessionAppRuntime::new(DEFAULT_SESSION_POOL_CAPACITY, buffers.clone(), tx_evt_q);
        Self {
            runtime: CachePadded::new(SessionDriverRuntimeCore {
                sessions: WorkerSessionRuntime::new(worker),
                entries: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
                buffers,
                rx: Pool::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
                rx_index: FlatHashTable::with_capacity(DEFAULT_SESSION_POOL_CAPACITY),
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
    pub(crate) fn mark_ready(&mut self, session_id: SessionId) {
        self.runtime.sessions.mark_ready(session_id);
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn insert_session(&mut self, state: St) -> SessionId {
        let index = self
            .runtime
            .entries
            .insert(state)
            .expect("session pool capacity exhausted");
        SessionId::from(index)
    }

    #[inline]
    pub(crate) fn session(&self, id: SessionId) -> Option<&St> {
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
        self.app_state.app.free_pending_send(id);
        let _ = self.app_state.app.detach_session(id);
        let handle = SessionHandle::new(id.pool_index().slot(), self.worker().slot() as u32);
        with_current_app_worker(self.worker().slot() as usize, |worker| {
            let _ = worker.detach_session(handle);
        });
        self.release_session_rx(id);
        let removed = self.runtime.entries.remove(id.pool_index())?;
        Some(removed)
    }

    pub(crate) fn close_session(&mut self, id: SessionId) -> CoreResult<Option<St>> {
        if self.session(id).is_none() {
            return Ok(None);
        }
        self.app_state.app.closed(id)?;
        Ok(self.remove_session(id))
    }

    #[inline]
    pub(crate) fn app(&self) -> &SessionAppRuntime<Seg> {
        &self.app_state.app
    }

    #[inline]
    #[cfg(test)]
    pub(crate) fn app_mut(&mut self) -> &mut SessionAppRuntime<Seg> {
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

    #[cfg(test)]
    pub(crate) fn has_session_tx(&self, session_id: SessionId) -> bool {
        self.app_state.app.has_pending_send(session_id)
    }

    pub(crate) fn release_tx_up_to(
        &mut self,
        session_id: SessionId,
        bytes: usize,
    ) -> CoreResult<()> {
        let _ = self
            .app_state
            .app
            .release_pending_send_bytes(session_id, bytes)?;
        Ok(())
    }

    fn release_session_rx(&mut self, session_id: SessionId) {
        let Some(index) = self.runtime.rx_index.remove(&session_id.get()) else {
            return;
        };
        let Some(queue) = self.runtime.rx.remove(index) else {
            return;
        };
        queue.free_all(&self.runtime.buffers);
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
        &mut self,
        session_id: SessionId,
        index: BufferIndex,
        offset: u32,
        fin: bool,
    ) -> CoreResult<SessionRxEnqueue> {
        let key = session_id.get();
        let runtime = &mut *self.runtime;
        let buffers = runtime.buffers.clone();
        let rx_index = match runtime.rx_index.lookup(&key) {
            Some(index) => index,
            None => {
                let index = runtime
                    .rx
                    .insert(SessionRxQueue::new())
                    .expect("session rx queue pool exhausted");
                runtime.rx_index.insert(key, index);
                index
            }
        };
        let buffer = buffers.get_buffer(index)?;
        let len = buffer
            .current_len()
            .checked_add(buffer.total_len_not_including_first())
            .ok_or_else(|| CoreError::internal("session rx chain length overflow"))?;
        drop(buffer);
        let len = u32::try_from(len)
            .map_err(|_| CoreError::internal("session rx buffer length exceeds u32"))?;
        let entry = SessionRxBuffer {
            index,
            offset,
            len,
            fin,
        };
        let queue = runtime
            .rx
            .get_mut(rx_index)
            .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
        if offset == 0 {
            return queue.insert_head(&buffers, entry);
        }
        queue.insert_ooo(&buffers, entry)
    }

    pub(crate) fn flush_session_rx(&mut self, session_id: SessionId) -> CoreResult<()> {
        let key = session_id.get();
        let Some(index) = self.runtime.rx_index.lookup(&key) else {
            return Ok(());
        };
        let buffers = self.runtime.buffers.clone();
        loop {
            let current = {
                let queue = self
                    .runtime
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
            let consumed =
                self.app_state
                    .app
                    .enqueue_rx(session_id, buffers.clone(), current.index)?;
            if !consumed {
                break;
            }
            let queue = self
                .runtime
                .rx
                .get_mut(index)
                .ok_or_else(|| CoreError::internal("session rx queue index is invalid"))?;
            let _ = queue
                .pop_front()
                .ok_or_else(|| CoreError::internal("session rx buffer is missing"))?;
            if queue.is_empty() {
                self.runtime.rx_index.remove(&key);
                let _ = self.runtime.rx.remove(index);
                break;
            }
        }
        Ok(())
    }

    pub(crate) fn poll_once_for_ticks(&mut self, timer_ticks: u32) -> CoreResult<SessionQueueStep> {
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

impl<St> SessionDriverRuntime<St, Local> {
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
        );
        driver.app_state.app_context = Some(app_context);
        driver
    }

    #[inline]
    pub(crate) fn insert_session_with_id<F>(&mut self, f: F) -> CoreResult<SessionId>
    where
        F: SessionStateFactory<St>,
    {
        let index = self
            .runtime
            .entries
            .insert_with(|index| f.build(SessionId::from(index)))
            .ok_or_else(|| CoreError::internal("session pool capacity exhausted"))?;
        let session_id = SessionId::from(index);
        let handle =
            SessionHandle::new(session_id.pool_index().slot(), self.worker().slot() as u32);
        let app_session = if let Some(app_context) = &self.app_state.app_context {
            match app_context.session(handle) {
                Ok(Some(session)) => session,
                Ok(None) => {
                    let _ = self.runtime.entries.remove(index);
                    return Err(CoreError::internal("app session is missing"));
                }
                Err(error) => {
                    let _ = self.runtime.entries.remove(index);
                    return Err(CoreError::from(error));
                }
            }
        } else {
            match with_current_app_worker(self.worker().slot() as usize, |worker| {
                worker.attach_session_local_with_runtime_tx(
                    handle,
                    self.app_state.app_session_config,
                    self.app_state.app.tx_evt_q().clone(),
                )
            }) {
                Ok(session) => session,
                Err(error) => {
                    let _ = self.runtime.entries.remove(index);
                    return Err(CoreError::from(error));
                }
            }
        };
        self.app_state.app.attach_session(session_id, app_session);
        Ok(session_id)
    }
}

#[cfg(test)]
pub(crate) fn dispatch_session_queue_for_ticks<St, Seg: Segment>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<St, Seg>,
    timer_ticks: u32,
    output_next: crate::session::SessionQueueNext,
) -> CoreResult<SessionQueueStep>
where
    St: SessionQueueProtocol,
{
    let mut step = driver.poll_once_for_ticks(timer_ticks)?;
    let now = Instant::now();
    let mut output = crate::session::node::SessionQueueOutput::default();
    dispatch_session_queue_pending(runtime, driver, output_next, &mut output, &mut step, now)?;
    output.schedule(runtime);
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

pub(crate) fn dispatch_session_queue_pending<St, Seg: Segment>(
    runtime: &DataPlaneRuntime,
    driver: &mut SessionDriverRuntime<St, Seg>,
    output_next: crate::session::SessionQueueNext,
    output: &mut crate::session::node::SessionQueueOutput,
    step: &mut SessionQueueStep,
    now: Instant,
) -> CoreResult<()>
where
    St: SessionQueueProtocol,
{
    driver.poll_app()?;
    let expired_timer_count = driver.runtime.sessions.pending_timers.len();
    for _ in 0..expired_timer_count {
        let Some(expired_timer) = driver.runtime.sessions.pending_timers.pop_front() else {
            break;
        };
        let driver = driver as *mut SessionDriverRuntime<St, Seg>;
        // SAFETY: same disjoint-access argument as above.
        unsafe {
            let state = (*driver)
                .session_mut(expired_timer.session_id)
                .ok_or_else(|| CoreError::internal("session is missing"))?;
            let mut context = session_queue_context(driver, expired_timer.session_id);
            let close_current = state.handle_expired_timer(
                runtime,
                &mut context,
                expired_timer.timer_id,
                output_next,
                output,
            )?;
            if close_current {
                state.on_close(&mut context);
                let _ = (*driver).close_session(expired_timer.session_id)?;
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
        // SAFETY: same disjoint-access argument as above.
        unsafe {
            let state = (*driver_ptr)
                .session_mut(session_id)
                .ok_or_else(|| CoreError::internal("session is missing"))?;
            let mut context = session_queue_context(driver_ptr, session_id);
            let close_current = state.handle_ready_session(
                runtime,
                &mut context,
                close_requested,
                output_next,
                output,
            )?;
            if close_current {
                state.on_close(&mut context);
                let _ = (*driver_ptr).close_session(session_id)?;
                continue;
            }
        }
        #[allow(clippy::never_loop)]
        loop {
            let Some(total_len) = driver.app_state.app.pending_send_len(session_id)? else {
                break;
            };
            let tx_offset = {
                let driver = driver as *mut SessionDriverRuntime<St, Seg>;
                unsafe {
                    let state = (*driver)
                        .session(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let context = session_queue_context(driver, session_id);
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
                let driver = driver as *mut SessionDriverRuntime<St, Seg>;
                // SAFETY: `state` and `context` access disjoint parts of the same
                // session queue: the state lives in `entries`, while the context
                // exposes runtime sidecar resources.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = session_queue_context(driver, session_id);
                    state
                        .tx_payload_len(&mut context, tx_offset, pending_len, now)?
                        .min(pending_len)
                }
            };
            if payload_len == 0 {
                break;
            }

            let index = driver.runtime.buffers.alloc_index()?;
            if let Err(error) =
                driver
                    .app_state
                    .app
                    .copy_tx_to_buffer(session_id, tx_offset, payload_len, index)
            {
                driver.runtime.buffers.free_index(index);
                return Err(error);
            }

            {
                let driver = driver as *mut SessionDriverRuntime<St, Seg>;
                // SAFETY: same reasoning as above; the current session state and
                // runtime sidecars are accessed together to execute one transport
                // callback.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = session_queue_context(driver, session_id);
                    if let Err(err) =
                        state.prepare_tx(&mut context, index, tx_offset, payload_len, now)
                    {
                        context.buffers().free_index(index);
                        return Err(err);
                    }
                }
            }

            output.enqueue(runtime, output_next.node(), index);

            let commit_result = {
                let driver = driver as *mut SessionDriverRuntime<St, Seg>;
                // SAFETY: same disjoint-access argument as above.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = session_queue_context(driver, session_id);
                    state.commit_tx(&mut context, index, tx_offset, payload_len, now)
                }
            };
            if let Err(err) = commit_result {
                let driver = driver as *mut SessionDriverRuntime<St, Seg>;
                // SAFETY: same disjoint-access argument as above.
                unsafe {
                    let state = (*driver)
                        .session_mut(session_id)
                        .ok_or_else(|| CoreError::internal("session is missing"))?;
                    let mut context = session_queue_context(driver, session_id);
                    state.cancel_tx(&mut context, index);
                }
                return Err(err);
            }
            let remaining = driver
                .app_state
                .app
                .pending_send_len(session_id)?
                .unwrap_or(0);
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
        for index in frame.drain_pending() {
            let packet = match chain_bytes(runtime.buffers(), index) {
                Ok(bytes) => bytes,
                Err(_) => return NodeResult::drop(),
            };
            state.packets.push(packet.to_vec());
            runtime.free_index(index);
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
    fn session_rx_queue_inserts_future_segments_by_offset_order() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.buffers();
        let mut driver = SessionDriverRuntime::<FakeTxProtocol, Local>::new(
            DataWorkerId::new(0),
            buffers.clone(),
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
            .runtime
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.runtime.rx.get(rx_index).expect("rx queue");
        let ooo_offsets: std::vec::Vec<_> =
            queue.ooo_index.iter().map(|(offset, _)| *offset).collect();
        assert_eq!(ooo_offsets, vec![2, 4]);
    }

    #[test]
    fn session_rx_queue_duplicate_covered_segment_is_discarded() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.buffers();
        let mut driver = SessionDriverRuntime::<FakeTxProtocol, Local>::new(
            DataWorkerId::new(0),
            buffers.clone(),
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
            .runtime
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.runtime.rx.get(rx_index).expect("rx queue");
        assert_eq!(queue.delivered.len(), 0);
        assert_eq!(queue.ooo_entries.len(), 1);
        assert_eq!(queue.first_ooo_relative(), Some((2, 4)));
    }

    #[test]
    fn session_rx_queue_overlap_trims_new_segment_against_predecessor() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.buffers();
        let mut driver = SessionDriverRuntime::<FakeTxProtocol, Local>::new(
            DataWorkerId::new(0),
            buffers.clone(),
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
            .runtime
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.runtime.rx.get(rx_index).expect("rx queue");
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
            chain_bytes(buffers, overlap_entry.index).expect("trimmed overlap payload"),
            b"ghij"
        );
    }

    #[test]
    fn session_rx_queue_overlap_trims_existing_successor_and_rekeys_tree() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.buffers();
        let mut driver = SessionDriverRuntime::<FakeTxProtocol, Local>::new(
            DataWorkerId::new(0),
            buffers.clone(),
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
            .runtime
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.runtime.rx.get(rx_index).expect("rx queue");
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
            chain_bytes(buffers, successor_entry.index).expect("trimmed successor payload"),
            b"ij"
        );
    }

    #[test]
    fn session_rx_queue_gap_close_promotes_contiguous_ooo_segments() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
        let buffers = runtime.buffers();
        let mut driver = SessionDriverRuntime::<FakeTxProtocol, Local>::new(
            DataWorkerId::new(0),
            buffers.clone(),
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
            .runtime
            .rx_index
            .lookup(&session_id.get())
            .expect("rx queue present");
        let queue = driver.runtime.rx.get(rx_index).expect("rx queue");
        assert_eq!(queue.delivered.len(), 2);
        assert_eq!(queue.ooo_entries.len(), 0);
        assert_eq!(queue.ooo_index.len(), 0);
        assert_eq!(queue.delivered.get(0).expect("head slot").len, 4);
        assert_eq!(queue.delivered.get(1).expect("promoted slot").len, 4);
    }

    #[test]
    fn session_tx_does_not_call_transport_when_app_has_no_pending_send() {
        let runtime = DataPlaneRuntime::with_capacities(2048, 16, 8, 8);
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

use std::cell::UnsafeCell;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::error::RuntimeResult;
use crossbeam_queue::ArrayQueue;
use hammer_core::data_plane::NodeId;
use hammer_core::error::DataPlaneError;

pub(crate) const HANDOFF_SLOT_CAPACITY: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct DataWorkerId(u32);

impl DataWorkerId {
    #[inline(always)]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub const fn slot(self) -> usize {
        self.0 as usize
    }

    /// Returns the VPP-style runtime thread index for this Data Worker.
    /// Thread zero is reserved for the Main Thread.
    #[inline(always)]
    pub const fn thread_index(self) -> u32 {
        self.0.saturating_add(1)
    }
}

impl From<DataWorkerId> for usize {
    #[inline]
    fn from(worker: DataWorkerId) -> Self {
        worker.slot()
    }
}

impl TryFrom<u32> for DataWorkerId {
    type Error = crate::error::RuntimeError;

    #[inline(always)]
    fn try_from(thread_index: u32) -> Result<Self, Self::Error> {
        thread_index
            .checked_sub(1)
            .map(Self)
            .ok_or(crate::error::RuntimeError::DataWorkerIdUnavailable { thread_index })
    }
}

#[derive(Debug, Clone)]
pub struct DataPlaneHandoff {
    inner: Arc<DataPlaneHandoffInner>,
}

struct DataPlaneHandoffInner {
    queues: Box<[ArrayQueue<HandoffFrame>]>,
    worker_interrupt_pending: Box<[Vec<AtomicBool>]>,
    worker_interrupt_threads: Box<[UnsafeCell<Option<thread::Thread>>]>,
    worker_interrupt_ready: Box<[AtomicBool]>,
}

// SAFETY: each worker writes only its own thread handle once. A release store
// to its ready flag publishes the handle to acquire-loading producers.
unsafe impl Sync for DataPlaneHandoffInner {}

impl fmt::Debug for DataPlaneHandoffInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DataPlaneHandoffInner")
            .field("workers", &self.queues.len())
            .field(
                "nodes",
                &self
                    .worker_interrupt_pending
                    .first()
                    .map(|pending| pending.len()),
            )
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct DataPlaneHandoffWorker {
    worker: DataWorkerId,
    inner: Arc<DataPlaneHandoffInner>,
}

#[derive(Debug, Clone)]
pub(crate) struct HandoffFrame {
    pub(crate) target: NodeId,
    pub(crate) slot: HandoffSlot,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HandoffSlot {
    indices: [u32; HANDOFF_SLOT_CAPACITY],
    len: usize,
}

impl HandoffSlot {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            indices: [0; HANDOFF_SLOT_CAPACITY],
            len: 0,
        }
    }

    #[inline]
    pub(crate) fn single(index: u32) -> Self {
        let mut slot = Self::new();
        let pushed = slot.push(index);
        debug_assert!(pushed);
        slot
    }

    #[inline]
    pub(crate) fn from_prefix(indices: &[u32]) -> Self {
        let mut slot = Self::new();
        for index in indices.iter().copied().take(HANDOFF_SLOT_CAPACITY) {
            let pushed = slot.push(index);
            debug_assert!(pushed);
        }
        slot
    }

    #[inline]
    pub(crate) fn push(&mut self, index: u32) -> bool {
        if self.len == HANDOFF_SLOT_CAPACITY {
            return false;
        }
        self.indices[self.len] = index;
        self.len += 1;
        true
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.indices[..self.len].iter().copied()
    }
}

#[derive(Debug)]
pub(crate) struct HandoffEnqueueError {
    error: DataPlaneError,
    slot: HandoffSlot,
}

impl HandoffEnqueueError {
    #[inline]
    fn new(error: DataPlaneError, slot: HandoffSlot) -> Self {
        Self { error, slot }
    }

    #[inline]
    pub(crate) fn into_parts(self) -> (DataPlaneError, HandoffSlot) {
        (self.error, self.slot)
    }
}

impl DataPlaneHandoff {
    #[inline]
    pub fn new(workers: usize, queue_capacity: usize) -> Self {
        Self::with_node_capacity(workers, queue_capacity, 0)
    }

    #[inline]
    pub fn with_node_capacity(workers: usize, queue_capacity: usize, node_capacity: usize) -> Self {
        Self {
            inner: Arc::new(DataPlaneHandoffInner {
                queues: (0..workers)
                    .map(|_| ArrayQueue::new(queue_capacity))
                    .collect::<Box<[_]>>(),
                worker_interrupt_pending: (0..workers)
                    .map(|_| (0..node_capacity).map(|_| AtomicBool::new(false)).collect())
                    .collect(),
                worker_interrupt_threads: (0..workers).map(|_| UnsafeCell::new(None)).collect(),
                worker_interrupt_ready: (0..workers).map(|_| AtomicBool::new(false)).collect(),
            }),
        }
    }

    #[inline]
    pub fn worker(&self, worker: DataWorkerId) -> DataPlaneHandoffWorker {
        DataPlaneHandoffWorker {
            worker,
            inner: Arc::clone(&self.inner),
        }
    }
}

impl DataPlaneHandoffWorker {
    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub(crate) fn enqueue_slot(
        &self,
        worker: DataWorkerId,
        target: NodeId,
        slot: HandoffSlot,
    ) -> Result<(), HandoffEnqueueError> {
        self.enqueue_indices(worker, target, slot)
    }

    #[inline]
    pub(crate) fn enqueue_index(
        &self,
        worker: DataWorkerId,
        target: NodeId,
        index: u32,
    ) -> Result<(), HandoffEnqueueError> {
        self.enqueue_indices(worker, target, HandoffSlot::single(index))
    }

    #[inline]
    pub(crate) fn ensure_enqueue_slots(
        &self,
        worker: DataWorkerId,
        slots: usize,
    ) -> RuntimeResult<()> {
        let queue = self
            .inner
            .queues
            .get(worker.slot())
            .ok_or(DataPlaneError::HandoffTargetWorkerOutOfBounds)?;
        if queue.capacity().saturating_sub(queue.len()) < slots {
            return Err(DataPlaneError::HandoffQueueExhausted.into());
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn enqueue_indices(
        &self,
        worker: DataWorkerId,
        target: NodeId,
        slot: HandoffSlot,
    ) -> Result<(), HandoffEnqueueError> {
        let Some(queue) = self.inner.queues.get(worker.slot()) else {
            return Err(HandoffEnqueueError::new(
                DataPlaneError::HandoffTargetWorkerOutOfBounds,
                slot,
            ));
        };
        queue.push(HandoffFrame { target, slot }).map_err(|frame| {
            HandoffEnqueueError::new(DataPlaneError::HandoffQueueExhausted, frame.slot)
        })
    }

    #[inline]
    pub(crate) fn pop(&self) -> Option<HandoffFrame> {
        self.inner
            .queues
            .get(self.worker.slot())
            .and_then(|queue| queue.pop())
    }

    #[inline]
    pub(crate) fn attach_current_thread(&self) {
        let slot = self.inner.worker_interrupt_threads[self.worker.slot()].get();
        // SAFETY: only this worker installs its own handle before its loop can
        // park; acquire readers cannot observe it until ready is published.
        let handle = unsafe { &mut *slot };
        assert!(
            handle.replace(thread::current()).is_none(),
            "handoff worker attaches once"
        );
        self.inner.worker_interrupt_ready[self.worker.slot()].store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn set_worker_node_interrupt_pending(&self, worker: DataWorkerId, node: NodeId) {
        let Some(pending) = self.inner.worker_interrupt_pending.get(worker.slot()) else {
            return;
        };
        let Some(bit) = pending.get(node.slot() as usize) else {
            return;
        };
        if bit.swap(true, Ordering::Release) {
            return;
        }
        if self.inner.worker_interrupt_ready[worker.slot()].load(Ordering::Acquire) {
            // SAFETY: the acquire load observes the worker's handle install,
            // and that handle remains immutable for the handoff lifetime.
            let thread = unsafe { &*self.inner.worker_interrupt_threads[worker.slot()].get() };
            thread
                .as_ref()
                .expect("ready handoff worker has a thread handle")
                .unpark();
        }
        if let Some(threads) = crate::thread_main::THREAD_MAIN.get()
            && let Some(descriptor) = threads.thread_by_index(worker.thread_index())
        {
            descriptor.wake_for_barrier();
        }
    }

    #[inline]
    pub(crate) fn drain_worker_interrupts(&self, mut schedule: impl FnMut(NodeId)) {
        let Some(pending) = self.inner.worker_interrupt_pending.get(self.worker.slot()) else {
            return;
        };
        for (node_slot, bit) in pending.iter().enumerate() {
            if bit.swap(false, Ordering::Acquire) {
                schedule(NodeId::new(node_slot as u32));
            }
        }
    }
}

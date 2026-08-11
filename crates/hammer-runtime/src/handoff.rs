use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;

use crate::error::RuntimeResult;
use crossbeam_queue::ArrayQueue;
use hammer_core::data_plane::{BufferPoolArena, Index, NodeHandle, NodeId};
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
}

impl From<DataWorkerId> for usize {
    #[inline]
    fn from(worker: DataWorkerId) -> Self {
        worker.slot()
    }
}

#[derive(Debug, Clone)]
pub struct DataPlaneHandoff {
    inner: Arc<DataPlaneHandoffInner>,
}

struct DataPlaneHandoffInner {
    queues: Box<[ArrayQueue<HandoffFrame>]>,
    buffer_arena: Option<BufferPoolArena>,
    worker_interrupt_pending: Box<[Vec<AtomicBool>]>,
    worker_interrupt_threads: Box<[OnceLock<thread::Thread>]>,
}

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
    pub(crate) target: NodeHandle,
    pub(crate) slot: HandoffSlot,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct HandoffSlot {
    indices: [Option<Index>; HANDOFF_SLOT_CAPACITY],
    len: usize,
}

impl HandoffSlot {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            indices: [None; HANDOFF_SLOT_CAPACITY],
            len: 0,
        }
    }

    #[inline]
    pub(crate) fn single(index: Index) -> Self {
        let mut slot = Self::new();
        let pushed = slot.push(index);
        debug_assert!(pushed);
        slot
    }

    #[inline]
    pub(crate) fn from_prefix(indices: &[Index]) -> Self {
        let mut slot = Self::new();
        for index in indices.iter().copied().take(HANDOFF_SLOT_CAPACITY) {
            let pushed = slot.push(index);
            debug_assert!(pushed);
        }
        slot
    }

    #[inline]
    pub(crate) fn push(&mut self, index: Index) -> bool {
        if self.len == HANDOFF_SLOT_CAPACITY {
            return false;
        }
        self.indices[self.len] = Some(index);
        self.len += 1;
        true
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = Index> + '_ {
        self.indices[..self.len].iter().filter_map(|index| *index)
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
                buffer_arena: None,
                worker_interrupt_pending: (0..workers)
                    .map(|_| (0..node_capacity).map(|_| AtomicBool::new(false)).collect())
                    .collect(),
                worker_interrupt_threads: (0..workers).map(|_| OnceLock::new()).collect(),
            }),
        }
    }

    #[inline]
    pub fn new_shared_buffer_arena(
        workers: usize,
        queue_capacity: usize,
        buffer_arena: BufferPoolArena,
    ) -> Self {
        Self::new_shared_buffer_arena_with_node_capacity(workers, queue_capacity, 0, buffer_arena)
    }

    #[inline]
    pub fn new_shared_buffer_arena_with_node_capacity(
        workers: usize,
        queue_capacity: usize,
        node_capacity: usize,
        buffer_arena: BufferPoolArena,
    ) -> Self {
        Self {
            inner: Arc::new(DataPlaneHandoffInner {
                queues: (0..workers)
                    .map(|_| ArrayQueue::new(queue_capacity))
                    .collect::<Box<[_]>>(),
                buffer_arena: Some(buffer_arena),
                worker_interrupt_pending: (0..workers)
                    .map(|_| (0..node_capacity).map(|_| AtomicBool::new(false)).collect())
                    .collect(),
                worker_interrupt_threads: (0..workers).map(|_| OnceLock::new()).collect(),
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
    pub(crate) fn configured_buffer_arena(&self) -> Option<BufferPoolArena> {
        self.inner.buffer_arena.clone()
    }

    #[inline]
    pub(crate) fn enqueue_slot(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        slot: HandoffSlot,
    ) -> Result<(), HandoffEnqueueError> {
        self.enqueue_indices(worker, target, slot)
    }

    #[inline]
    pub(crate) fn enqueue_index(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        index: Index,
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
        target: NodeHandle,
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
        if let Some(slot) = self.inner.worker_interrupt_threads.get(self.worker.slot()) {
            let _ = slot.set(thread::current());
        }
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
        if let Some(thread) = self
            .inner
            .worker_interrupt_threads
            .get(worker.slot())
            .and_then(OnceLock::get)
        {
            thread.unpark();
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

#[cfg(test)]
mod tests {
    use crate::{DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig};
    use hammer_core::data_plane::NodeId;

    use super::*;

    #[test]
    fn enqueue_index_uses_inline_handoff_payload() {
        let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
            buffers: DataPlaneBufferConfig {
                buffer_slot_capacity: 64,
                buffer_slots: 1,
                ..DataPlaneBufferConfig::default()
            },
        });
        let index = runtime.alloc_index().expect("alloc index");
        let handoff = DataPlaneHandoff::new(2, 4);
        let source = handoff.worker(DataWorkerId::new(0));
        let target = handoff.worker(DataWorkerId::new(1));
        let node = NodeHandle::new(7);

        source
            .enqueue_index(DataWorkerId::new(1), node, index)
            .expect("enqueue index");

        let frame = target.pop().expect("handoff frame");
        assert_eq!(frame.target, node);
        assert_eq!(frame.slot.len(), 1);
        assert!(frame.slot.iter().any(|value| value == index));
        let mut cleanup = runtime
            .buffers()
            .get_next_frame(NodeId::new(0))
            .expect("cleanup frame");
        cleanup.push_index(index).expect("cleanup push");
    }

    #[test]
    fn handoff_index_resolves_local_continuation_before_enqueue() {
        use crate::node::{NodeDescriptor, NodeResult, NodeRuntimeData};
        use hammer_core::data_plane::{NodeKind, NodeRegistration};

        let handoff = DataPlaneHandoff::new(2, 4);
        let runtime = DataPlaneRuntime::attach_handoff_worker(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig {
                buffers: DataPlaneBufferConfig {
                    buffer_slot_capacity: 64,
                    buffer_slots: 4,
                    frame_slots: 4,
                    ..DataPlaneBufferConfig::default()
                },
            }),
            handoff.worker(DataWorkerId::new(0)),
        );
        let continuation = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    |_, _, _| NodeResult::drop(),
                    NodeRuntimeData::empty(),
                    NodeRegistration::next("continuation", 0),
                    &[],
                    None,
                ),
            )
            .expect("continuation");
        let owner = runtime
            .nodes()
            .try_register_descriptor(
                NodeKind::Internal,
                NodeDescriptor::new(
                    |_, _, _| NodeResult::drop(),
                    NodeRuntimeData::empty(),
                    NodeRegistration::next("owner", 1),
                    &[continuation],
                    None,
                ),
            )
            .expect("owner");
        let index = runtime.alloc_index().expect("alloc");
        let target = NodeHandle::new(9);

        runtime
            .with_current_node(owner, || {
                runtime.handoff_index(DataWorkerId::new(1), target, index, Some(0u16))
            })
            .expect("handoff with continuation");

        assert_eq!(
            runtime.buffers().current_config(index).expect("config"),
            continuation
        );
        let frame = handoff.worker(DataWorkerId::new(1)).pop().expect("queued");
        assert_eq!(frame.target, target);
        assert!(frame.slot.iter().any(|value| value == index));
    }
}

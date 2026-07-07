use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use hammer_core::error::{CoreResult, DataPlaneError};

use crate::{BufferIndex, BufferPoolArena, NodeHandle};

pub(crate) const HANDOFF_SLOT_CAPACITY: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone)]
pub struct DataPlaneHandoff {
    inner: Arc<DataPlaneHandoffInner>,
}

#[derive(Debug)]
struct DataPlaneHandoffInner {
    queues: Box<[ArrayQueue<HandoffFrame>]>,
    buffer_arena: Option<BufferPoolArena>,
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
    indices: [Option<BufferIndex>; HANDOFF_SLOT_CAPACITY],
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
    pub(crate) fn single(index: BufferIndex) -> Self {
        let mut slot = Self::new();
        let pushed = slot.push(index);
        debug_assert!(pushed);
        slot
    }

    #[inline]
    pub(crate) fn from_prefix(indices: &[BufferIndex]) -> Self {
        let mut slot = Self::new();
        for index in indices.iter().copied().take(HANDOFF_SLOT_CAPACITY) {
            let pushed = slot.push(index);
            debug_assert!(pushed);
        }
        slot
    }

    #[inline]
    pub(crate) fn push(&mut self, index: BufferIndex) -> bool {
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
    pub(crate) fn iter(&self) -> impl Iterator<Item = BufferIndex> + '_ {
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
        Self {
            inner: Arc::new(DataPlaneHandoffInner {
                queues: (0..workers)
                    .map(|_| ArrayQueue::new(queue_capacity))
                    .collect::<Box<[_]>>(),
                buffer_arena: None,
            }),
        }
    }

    #[inline]
    pub fn new_shared_buffer_arena(
        workers: usize,
        queue_capacity: usize,
        buffer_arena: BufferPoolArena,
    ) -> Self {
        Self {
            inner: Arc::new(DataPlaneHandoffInner {
                queues: (0..workers)
                    .map(|_| ArrayQueue::new(queue_capacity))
                    .collect::<Box<[_]>>(),
                buffer_arena: Some(buffer_arena),
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
        index: BufferIndex,
    ) -> Result<(), HandoffEnqueueError> {
        self.enqueue_indices(worker, target, HandoffSlot::single(index))
    }

    #[inline]
    pub(crate) fn ensure_enqueue_slots(
        &self,
        worker: DataWorkerId,
        slots: usize,
    ) -> CoreResult<()> {
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
}

#[cfg(test)]
mod tests {
    use crate::{DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig};

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
            .get_next_frame(crate::NodeId::new(0))
            .expect("cleanup frame");
        cleanup.push_index(index).expect("cleanup push");
    }
}

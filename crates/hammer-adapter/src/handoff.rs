use std::cell::RefCell;
use std::rc::Rc;

use crossbeam_queue::ArrayQueue;
use hammer_core::error::{CoreError, CoreResult};

use crate::{BufferIndex, BufferPoolArena, NodeHandle};

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
    inner: Rc<DataPlaneHandoffInner>,
}

#[derive(Debug)]
struct DataPlaneHandoffInner {
    queues: Vec<ArrayQueue<HandoffFrame>>,
    buffer_arena: RefCell<Option<BufferPoolArena>>,
}

#[derive(Debug, Clone)]
pub struct DataPlaneHandoffWorker {
    worker: DataWorkerId,
    inner: Rc<DataPlaneHandoffInner>,
}

#[derive(Debug, Clone)]
pub(crate) struct HandoffFrame {
    pub(crate) target: NodeHandle,
    pub(crate) indices: HandoffIndices,
}

#[derive(Debug, Clone)]
pub(crate) enum HandoffIndices {
    Single(BufferIndex),
    Batch(Vec<BufferIndex>),
}

impl DataPlaneHandoff {
    #[inline]
    pub fn new(workers: usize, queue_capacity: usize) -> Self {
        Self {
            inner: Rc::new(DataPlaneHandoffInner {
                queues: (0..workers)
                    .map(|_| ArrayQueue::new(queue_capacity))
                    .collect(),
                buffer_arena: RefCell::new(None),
            }),
        }
    }

    #[inline]
    pub fn with_buffer_arena(
        workers: usize,
        queue_capacity: usize,
        buffer_arena: BufferPoolArena,
    ) -> Self {
        Self {
            inner: Rc::new(DataPlaneHandoffInner {
                queues: (0..workers)
                    .map(|_| ArrayQueue::new(queue_capacity))
                    .collect(),
                buffer_arena: RefCell::new(Some(buffer_arena)),
            }),
        }
    }

    #[inline]
    pub fn worker(&self, worker: DataWorkerId) -> DataPlaneHandoffWorker {
        DataPlaneHandoffWorker {
            worker,
            inner: Rc::clone(&self.inner),
        }
    }

    #[inline]
    pub fn buffer_arena(&self) -> BufferPoolArena {
        self.inner
            .buffer_arena
            .borrow()
            .as_ref()
            .expect("data plane handoff buffer arena is not configured")
            .clone()
    }
}

impl DataPlaneHandoffWorker {
    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub(crate) fn buffer_arena(&self) -> BufferPoolArena {
        self.inner
            .buffer_arena
            .borrow()
            .as_ref()
            .expect("data plane handoff buffer arena is not configured")
            .clone()
    }

    #[inline]
    pub(crate) fn set_or_get_buffer_arena(&self, arena: BufferPoolArena) -> BufferPoolArena {
        let mut configured = self.inner.buffer_arena.borrow_mut();
        if let Some(configured) = configured.as_ref() {
            return configured.clone();
        }
        *configured = Some(arena.clone());
        arena
    }

    #[inline]
    pub(crate) fn enqueue(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        indices: Vec<BufferIndex>,
    ) -> CoreResult<()> {
        self.enqueue_indices(worker, target, HandoffIndices::Batch(indices))
    }

    #[inline]
    pub(crate) fn enqueue_index(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        index: BufferIndex,
    ) -> CoreResult<()> {
        self.enqueue_indices(worker, target, HandoffIndices::Single(index))
    }

    #[inline]
    pub(crate) fn ensure_enqueue_capacity(&self, worker: DataWorkerId) -> CoreResult<()> {
        let queue = self
            .inner
            .queues
            .get(worker.slot())
            .ok_or_else(|| CoreError::internal("handoff target worker out of bounds"))?;
        if queue.is_full() {
            return Err(CoreError::internal("handoff queue exhausted"));
        }
        Ok(())
    }

    #[inline]
    fn enqueue_indices(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        indices: HandoffIndices,
    ) -> CoreResult<()> {
        let queue = self
            .inner
            .queues
            .get(worker.slot())
            .ok_or_else(|| CoreError::internal("handoff target worker out of bounds"))?;
        queue
            .push(HandoffFrame { target, indices })
            .map_err(|_| CoreError::internal("handoff queue exhausted"))
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
    use crate::{DataPlaneRuntime, NoopNode, RouteMetadata};

    use super::*;

    #[test]
    fn enqueue_index_uses_inline_handoff_payload() {
        let runtime = DataPlaneRuntime::<NoopNode>::with_buffer_capacity(64, 1);
        let index = runtime
            .alloc_index(RouteMetadata::default())
            .expect("alloc index");
        let handoff = DataPlaneHandoff::new(2, 4);
        let source = handoff.worker(DataWorkerId::new(0));
        let target = handoff.worker(DataWorkerId::new(1));
        let node = NodeHandle::new(7);

        source
            .enqueue_index(DataWorkerId::new(1), node, index)
            .expect("enqueue index");

        let frame = target.pop().expect("handoff frame");
        assert_eq!(frame.target, node);
        assert!(matches!(frame.indices, HandoffIndices::Single(value) if value == index));

        runtime.free_index(index);
    }
}

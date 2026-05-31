use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crossbeam_queue::ArrayQueue;
use hammer_core::error::{CoreError, CoreResult};

use crate::{BufferIndex, BufferPool, NodeHandle};

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
    buffer_pools: RefCell<HashMap<u64, BufferPool>>,
}

#[derive(Debug, Clone)]
pub struct DataPlaneHandoffWorker {
    worker: DataWorkerId,
    inner: Rc<DataPlaneHandoffInner>,
}

#[derive(Debug, Clone)]
pub(crate) struct HandoffFrame {
    pub(crate) target: NodeHandle,
    pub(crate) indices: Vec<BufferIndex>,
}

impl DataPlaneHandoff {
    #[inline]
    pub fn new(workers: usize, queue_capacity: usize) -> Self {
        Self {
            inner: Rc::new(DataPlaneHandoffInner {
                queues: (0..workers)
                    .map(|_| ArrayQueue::new(queue_capacity))
                    .collect(),
                buffer_pools: RefCell::new(HashMap::new()),
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
}

impl DataPlaneHandoffWorker {
    #[inline]
    pub fn worker(&self) -> DataWorkerId {
        self.worker
    }

    #[inline]
    pub(crate) fn register_buffer_pool(&self, pool: BufferPool) {
        self.inner
            .buffer_pools
            .borrow_mut()
            .insert(pool.pool_id(), pool);
    }

    #[inline]
    pub(crate) fn buffer_pool(&self, pool_id: u64) -> Option<BufferPool> {
        self.inner.buffer_pools.borrow().get(&pool_id).cloned()
    }

    #[inline]
    pub(crate) fn enqueue(
        &self,
        worker: DataWorkerId,
        target: NodeHandle,
        indices: Vec<BufferIndex>,
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

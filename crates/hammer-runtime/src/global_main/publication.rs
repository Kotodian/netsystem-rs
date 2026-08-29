use core::cell::UnsafeCell;
use core::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::{RuntimeError, RuntimeResult};
use crate::global_main::GlobalMain;
use crate::init::WorkerInitFunction;
use crate::node::NodeRuntimeInner;

#[derive(Clone)]
pub(crate) struct WorkerGraphUpdate {
    pub(crate) graph: NodeRuntimeInner,
    pub(crate) worker_init_functions: Vec<WorkerInitFunction>,
}

/// Runtime-owned slots exchanged across the worker barrier.
///
/// The main GlobalMain publishes the graph while workers are stopped. Workers own
/// their error slots: they write them before acknowledging a barrier, and the
/// main GlobalMain reads them only after the matching completion count has
/// finished. This is deliberately an owner-specific publication record rather
/// than a generic synchronization wrapper.
pub(crate) struct WorkerPublication {
    graph: UnsafeCell<Option<WorkerGraphUpdate>>,
    graph_errors: Box<[UnsafeCell<Option<RuntimeError>>]>,
}

impl WorkerPublication {
    pub(crate) fn new(worker_count: usize) -> Self {
        Self {
            graph: UnsafeCell::new(None),
            graph_errors: (0..worker_count)
                .map(|_| UnsafeCell::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[inline]
    pub(crate) fn worker_count(&self) -> usize {
        self.graph_errors.len()
    }

    /// # Safety
    /// The caller must hold the main-thread worker barrier before publishing
    /// or replacing the graph.
    pub(crate) unsafe fn set_graph(&self, graph: Option<WorkerGraphUpdate>) {
        // SAFETY: guaranteed by the caller's worker-barrier phase.
        unsafe { *self.graph.get() = graph };
    }

    /// # Safety
    /// The caller must prove that the main thread has published the graph and
    /// that no main-thread writer can run until the returned reference ends.
    pub(crate) unsafe fn graph(&self) -> &Option<WorkerGraphUpdate> {
        // SAFETY: guaranteed by the caller's publication lifetime proof.
        unsafe { &*self.graph.get() }
    }

    /// # Safety
    /// The caller must hold the main-thread worker barrier while clearing all
    /// worker error slots.
    pub(crate) unsafe fn clear_graph_errors(&self) {
        for slot in &self.graph_errors {
            // SAFETY: the enclosing worker-barrier phase excludes workers.
            unsafe { *slot.get() = None };
        }
    }

    /// # Safety
    /// The caller must be the Data Worker that owns `worker`'s slot, during a
    /// graph refork. The main thread must not read the slot until the refork
    /// completion count reaches zero.
    pub(crate) unsafe fn set_graph_error(&self, worker: usize, error: RuntimeError) {
        let slot = self
            .graph_errors
            .get(worker)
            .expect("worker graph error slot must exist");
        // SAFETY: the owning worker has exclusive access to this slot during
        // the refork completion phase.
        unsafe { *slot.get() = Some(error) };
    }

    /// # Safety
    /// The caller must have observed the graph refork completion count at zero.
    pub(crate) unsafe fn take_graph_error(&self, worker: usize) -> Option<RuntimeError> {
        let slot = self
            .graph_errors
            .get(worker)
            .expect("worker graph error slot must exist");
        // SAFETY: no worker can access the slot after the completion count is
        // zero.
        unsafe { (*slot.get()).take() }
    }
}

// SAFETY: all shared access to these UnsafeCell values follows the ownership
// and completion contracts documented on WorkerPublication's methods.
unsafe impl Sync for WorkerPublication {}

impl GlobalMain {
    pub(crate) fn prepare_worker_publication(&mut self, worker_count: usize) {
        self.publication = Arc::new(WorkerPublication::new(worker_count));
    }

    pub(crate) fn publish_worker_graph(&self, worker_count: u32) -> RuntimeResult<()> {
        assert_ne!(worker_count, 0, "worker graph publication requires workers");
        if self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            return Err(RuntimeError::WorkerGraphUpdateAlreadyPending);
        }
        let update = WorkerGraphUpdate {
            graph: self.main.nodes().snapshot(),
            worker_init_functions: self.plugin_main.worker_init_functions(),
        };
        // SAFETY: the main GlobalMain calls this only while every worker is held at
        // `self.barrier`, before the refork completion count is published.
        unsafe {
            self.publication.set_graph(Some(update));
            self.publication.clear_graph_errors();
        }
        self.workers_updating_graph
            .store(worker_count, Ordering::Release);
        Ok(())
    }

    pub(crate) fn finish_worker_graph_update(&self) -> RuntimeResult<()> {
        if self.barrier.recursion_level() != 0 {
            self.deferred_finish_pending.store(true, Ordering::Release);
            return Ok(());
        }
        self.drain_worker_graph_update()
    }

    pub fn finish_deferred_worker_graph_update(&self) -> RuntimeResult<()> {
        if !self.deferred_finish_pending.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.drain_worker_graph_update()
    }

    fn drain_worker_graph_update(&self) -> RuntimeResult<()> {
        while self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            core::hint::spin_loop();
        }

        // SAFETY: every worker completed its refork before decrementing the
        // counter to zero, so none can still access the graph or error slots.
        if unsafe { self.publication.graph() }.is_none() {
            return Err(RuntimeError::WorkerGraphUpdateMissing);
        }

        let mut failures = Vec::new();
        for worker in 0..self.publication.worker_count() {
            // SAFETY: the refork completion count is zero, so the worker that
            // owns this slot can no longer read or write it.
            if let Some(error) = unsafe { self.publication.take_graph_error(worker) } {
                failures.push((worker, error));
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(RuntimeError::WorkerGraphUpdate { failures })
        }
    }
}

use core::cell::UnsafeCell;
use core::sync::atomic::Ordering;
use std::sync::Arc;

use crate::global_main::GlobalMain;
use crate::node::NodeRuntimeInner;

/// Runtime-owned slots exchanged across the worker barrier.
///
/// The main GlobalMain publishes the graph while workers are stopped. The graph
/// remains alive until the refork completion count reaches zero.
pub(crate) struct WorkerPublication {
    graph: UnsafeCell<Option<NodeRuntimeInner>>,
}

impl WorkerPublication {
    pub(crate) fn new() -> Self {
        Self {
            graph: UnsafeCell::new(None),
        }
    }

    /// # Safety
    /// The caller must hold the main-thread worker barrier before publishing
    /// or replacing the graph.
    pub(crate) unsafe fn set_graph(&self, graph: NodeRuntimeInner) {
        // SAFETY: guaranteed by the caller's worker-barrier phase.
        unsafe { *self.graph.get() = Some(graph) };
    }

    /// # Safety
    /// The caller must prove that the main thread has published the graph and
    /// that no main-thread writer can run until the returned reference ends.
    pub(crate) unsafe fn graph(&self) -> &Option<NodeRuntimeInner> {
        // SAFETY: guaranteed by the caller's publication lifetime proof.
        unsafe { &*self.graph.get() }
    }
}

// SAFETY: all shared access to these UnsafeCell values follows the ownership
// and completion contracts documented on WorkerPublication's methods.
unsafe impl Sync for WorkerPublication {}

impl GlobalMain {
    pub(crate) fn prepare_worker_publication(&mut self) {
        self.publication = Arc::new(WorkerPublication::new());
    }

    pub(crate) fn request_worker_graph_refork(&self) {
        self.worker_graph_refork_pending
            .store(true, Ordering::Release);
    }

    pub(crate) fn publish_worker_graph_refork(&self, worker_count: u32) -> bool {
        assert_ne!(worker_count, 0, "worker graph publication requires workers");
        if !self
            .worker_graph_refork_pending
            .swap(false, Ordering::AcqRel)
        {
            return false;
        }
        // SAFETY: the main GlobalMain calls this only while every worker is held at
        // `self.barrier`, before the refork completion count is published.
        unsafe { self.publication.set_graph(self.main.nodes().snapshot()) };
        if self.workers_updating_graph.load(Ordering::Acquire) == 0 {
            self.workers_updating_graph
                .store(worker_count, Ordering::Release);
        }
        true
    }

    pub(crate) fn wait_for_worker_graph_refork(&self) {
        if self.barrier.recursion_level() != 0 {
            return;
        }
        let deadline = std::time::Instant::now() + crate::barrier::BARRIER_SYNC_TIMEOUT;
        loop {
            let observed = self.workers_updating_graph.load(Ordering::Acquire);
            if observed == 0 {
                return;
            }
            if std::time::Instant::now() > deadline {
                crate::barrier::barrier_deadlock("worker graph refork", 0, observed);
            }
            core::hint::spin_loop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataPlaneBufferConfig, DataPlaneMain, RuntimeRegistry};

    #[test]
    fn graph_updates_coalesce_until_barrier_release() {
        let runtime = DataPlaneMain::new(DataPlaneBufferConfig::default());
        let mut engine = GlobalMain::new(runtime, RuntimeRegistry::new());
        engine.prepare_worker_publication();

        engine.request_worker_graph_refork();
        engine.request_worker_graph_refork();
        assert!(engine.publish_worker_graph_refork(1));
        assert_eq!(engine.workers_updating_graph.load(Ordering::Acquire), 1);
        assert!(!engine.publish_worker_graph_refork(1));

        let previous = engine.workers_updating_graph.fetch_sub(1, Ordering::AcqRel);
        assert_eq!(previous, 1);
        engine.wait_for_worker_graph_refork();
        assert_eq!(engine.workers_updating_graph.load(Ordering::Acquire), 0);
        assert!(unsafe { engine.publication.graph() }.is_some());
    }
}

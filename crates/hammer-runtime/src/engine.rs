use core::hint::spin_loop;
use std::cell::{Ref, RefCell, RefMut, UnsafeCell};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use crate::error::{RuntimeError, RuntimeResult};
use hammer_core::data_plane::NodeId;
use hammer_runtime::DataPlaneRuntime;
use hammer_runtime::RuntimeRegistry;

use crate::config::Worker;
use crate::data_plane::{DataPlaneRuntimeWorkerConfig, DataPlaneRuntimeWorkerSeed};
use crate::file::FileRuntimeStatsRow;
use crate::init::InitFunction;
use crate::node::{NodeRuntimeData, NodeRuntimeInner, NodeRuntimeStatsRow};
use crate::process::ProcessMain;
use crate::spawn::{DataRemoteLocalQueue, DataRemoteLocalQueueError};
use crate::{DataPlaneHandoffWorker, DataWorkerId, FileMain, PluginMain, ProcessHandle};

thread_local! {
    static CURRENT_ENGINE: RefCell<Option<*mut Engine>> = const { RefCell::new(None) };
}

pub(crate) struct EngineWorkerSeed {
    runtime_seed: DataPlaneRuntimeWorkerSeed,
    registry: Arc<RuntimeRegistry>,
    barrier: crate::barrier::WorkerBarrier,
    main_loop_exit_now: Arc<AtomicBool>,
    publication: Arc<WorkerPublication>,
    workers_updating_graph: Arc<AtomicU32>,
    worker_init_functions: Vec<InitFunction>,
    memory_initialized: bool,
    worker_config: Worker,
}

#[derive(Clone)]
pub(crate) struct WorkerGraphUpdate {
    pub(crate) graph: NodeRuntimeInner,
    pub(crate) worker_init_functions: Vec<InitFunction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRuntimeStats {
    pub thread_index: u32,
    pub numa_node: u32,
    pub main_loop_count: u32,
    pub nodes: Vec<NodeRuntimeStatsRow>,
    pub files: Vec<FileRuntimeStatsRow>,
}

/// Runtime-owned slots exchanged across the worker barrier.
///
/// The main Engine publishes the graph while workers are stopped. Workers own
/// their error and statistics slots: they write them before acknowledging a
/// barrier, and the main Engine reads them only after the matching completion
/// count or barrier acknowledgement has finished. This is deliberately an
/// owner-specific publication record rather than a generic synchronization
/// wrapper.
struct WorkerPublication {
    graph: UnsafeCell<Option<WorkerGraphUpdate>>,
    graph_errors: Box<[UnsafeCell<Option<RuntimeError>>]>,
    runtime_stats: Box<[UnsafeCell<Option<WorkerRuntimeStats>>]>,
}

impl WorkerPublication {
    fn new(worker_count: usize) -> Self {
        Self {
            graph: UnsafeCell::new(None),
            graph_errors: (0..worker_count)
                .map(|_| UnsafeCell::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            runtime_stats: (0..worker_count)
                .map(|_| UnsafeCell::new(None))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[inline]
    fn worker_count(&self) -> usize {
        self.runtime_stats.len()
    }

    /// # Safety
    /// The caller must hold the main-thread worker barrier before publishing
    /// or replacing the graph.
    unsafe fn set_graph(&self, graph: Option<WorkerGraphUpdate>) {
        // SAFETY: guaranteed by the caller's worker-barrier phase.
        unsafe { *self.graph.get() = graph };
    }

    /// # Safety
    /// The caller must prove that the main thread has published the graph and
    /// that no main-thread writer can run until the returned reference ends.
    unsafe fn graph(&self) -> &Option<WorkerGraphUpdate> {
        // SAFETY: guaranteed by the caller's publication lifetime proof.
        unsafe { &*self.graph.get() }
    }

    /// # Safety
    /// The caller must hold the main-thread worker barrier while clearing all
    /// worker error slots.
    unsafe fn clear_graph_errors(&self) {
        for slot in &self.graph_errors {
            // SAFETY: the enclosing worker-barrier phase excludes workers.
            unsafe { *slot.get() = None };
        }
    }

    /// # Safety
    /// The caller must be the Data Worker that owns `worker`'s slot, during a
    /// graph refork. The main thread must not read the slot until the refork
    /// completion count reaches zero.
    unsafe fn set_graph_error(&self, worker: usize, error: RuntimeError) {
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
    unsafe fn take_graph_error(&self, worker: usize) -> Option<RuntimeError> {
        let slot = self
            .graph_errors
            .get(worker)
            .expect("worker graph error slot must exist");
        // SAFETY: no worker can access the slot after the completion count is
        // zero.
        unsafe { (*slot.get()).take() }
    }

    /// # Safety
    /// The caller must be the Data Worker that owns `worker`'s slot and must
    /// write before acknowledging the worker barrier.
    unsafe fn set_runtime_stats(&self, worker: usize, stats: WorkerRuntimeStats) {
        let slot = self
            .runtime_stats
            .get(worker)
            .expect("worker runtime statistics slot must exist");
        // SAFETY: the worker owns its slot until it acknowledges the barrier.
        unsafe { *slot.get() = Some(stats) };
    }

    /// # Safety
    /// The caller must hold the worker barrier after every worker has published
    /// its statistics slot.
    unsafe fn runtime_stats(&self, worker: usize) -> Option<&WorkerRuntimeStats> {
        let slot = self
            .runtime_stats
            .get(worker)
            .expect("worker runtime statistics slot must exist");
        // SAFETY: the enclosing barrier phase excludes all slot writers.
        unsafe { (*slot.get()).as_ref() }
    }
}

// SAFETY: all shared access to these UnsafeCell values follows the ownership
// and completion contracts documented on WorkerPublication's methods.
unsafe impl Sync for WorkerPublication {}

#[repr(align(64))]
pub struct Engine {
    pub thread_index: u32,
    pub numa_node: u32,
    pub main_loop_count: AtomicU32,
    pub runtime: DataPlaneRuntime,
    pub registry: Arc<RuntimeRegistry>,
    pub(crate) barrier: crate::barrier::WorkerBarrier,
    pub main_loop_exit_now: Arc<AtomicBool>,
    pub main_loop_exit_status: Mutex<i32>,
    pub(crate) memory_initialized: bool,
    processes: ProcessMain,
    pub(crate) worker_init_functions: Vec<InitFunction>,
    worker_exit_functions: Vec<fn(&mut Engine) -> RuntimeResult<()>>,
    worker_config: Worker,
    pub(crate) called_init_functions: HashSet<&'static str>,
    pub(crate) called_worker_init_functions: HashSet<&'static str>,
    pub(crate) called_early_config_functions: HashSet<&'static str>,
    pub(crate) called_config_functions: HashSet<&'static str>,
    pub(crate) called_main_loop_enter_functions: HashSet<&'static str>,
    pub(crate) called_main_loop_exit_functions: HashSet<&'static str>,
    pub(crate) main_loop_entered: bool,
    materialized_registration_generation: u64,
    publication: Arc<WorkerPublication>,
    pub(crate) workers_updating_graph: Arc<AtomicU32>,
    // VPP `need_vlib_worker_thread_node_runtime_update`: set when a graph
    // publication finishes inside an outer barrier and the refork drain is
    // deferred to the outermost barrier owner (Binary API dispatch).
    deferred_finish_pending: AtomicBool,
    main_loop_exit_functions_called: bool,
    worker_threads: Vec<JoinHandle<RuntimeResult<()>>>,
    worker_control_queues: Arc<[DataRemoteLocalQueue]>,
    // Drop after every owner that may retain DSO code or Drop glue. Plugin
    // images themselves remain mapped for the full process lifetime.
    plugin_main: PluginMain,
}

impl Engine {
    #[inline]
    fn worker_with_runtime(
        runtime: DataPlaneRuntime,
        registry: Arc<RuntimeRegistry>,
        barrier: crate::barrier::WorkerBarrier,
        main_loop_exit_now: Arc<AtomicBool>,
        publication: Arc<WorkerPublication>,
        workers_updating_graph: Arc<AtomicU32>,
        worker_init_functions: Vec<InitFunction>,
        memory_initialized: bool,
        worker_config: Worker,
        index: u32,
        numa_node: u32,
    ) -> RuntimeResult<Self> {
        let engine = Self {
            thread_index: index,
            numa_node,
            main_loop_count: AtomicU32::new(0),
            runtime,
            registry,
            barrier,
            main_loop_exit_now,
            main_loop_exit_status: Mutex::new(0),
            memory_initialized,
            plugin_main: PluginMain::default(),
            worker_init_functions,
            worker_exit_functions: Vec::new(),
            worker_config,
            called_init_functions: HashSet::new(),
            called_worker_init_functions: HashSet::new(),
            called_early_config_functions: HashSet::new(),
            called_config_functions: HashSet::new(),
            called_main_loop_enter_functions: HashSet::new(),
            called_main_loop_exit_functions: HashSet::new(),
            main_loop_entered: false,
            materialized_registration_generation: 0,
            publication,
            workers_updating_graph,
            deferred_finish_pending: AtomicBool::new(false),
            main_loop_exit_functions_called: false,
            worker_threads: Vec::new(),
            worker_control_queues: Arc::from([]),
            processes: ProcessMain::new(),
        };
        Ok(engine)
    }

    pub fn new(runtime: DataPlaneRuntime, registry: Arc<RuntimeRegistry>) -> Self {
        Self {
            thread_index: 0,
            numa_node: 0,
            main_loop_count: AtomicU32::new(0),
            runtime,
            registry,
            barrier: crate::barrier::WorkerBarrier::new(0),
            main_loop_exit_now: Arc::new(AtomicBool::new(false)),
            main_loop_exit_status: Mutex::new(0),
            memory_initialized: false,
            plugin_main: PluginMain::default(),
            worker_init_functions: Vec::new(),
            worker_exit_functions: Vec::new(),
            worker_config: Worker::default(),
            called_init_functions: HashSet::new(),
            called_worker_init_functions: HashSet::new(),
            called_early_config_functions: HashSet::new(),
            called_config_functions: HashSet::new(),
            called_main_loop_enter_functions: HashSet::new(),
            called_main_loop_exit_functions: HashSet::new(),
            main_loop_entered: false,
            materialized_registration_generation: 0,
            publication: Arc::new(WorkerPublication::new(0)),
            workers_updating_graph: Arc::new(AtomicU32::new(0)),
            deferred_finish_pending: AtomicBool::new(false),
            main_loop_exit_functions_called: false,
            worker_threads: Vec::new(),
            worker_control_queues: Arc::from([]),
            processes: ProcessMain::new(),
        }
    }

    pub fn new_configured(registry: Arc<RuntimeRegistry>, worker: Worker) -> RuntimeResult<Self> {
        worker.validate()?;
        let runtime = worker.create_runtime()?;
        let mut engine = Self::new(runtime, registry);
        engine.worker_config = worker;
        engine.memory_initialized = true;
        Ok(engine)
    }

    pub fn loaded_plugins(&self) -> Vec<String> {
        self.plugin_main.loaded_plugins()
    }

    pub fn plugin_main(&self) -> &PluginMain {
        &self.plugin_main
    }

    pub fn plugin_main_mut(&mut self) -> &mut PluginMain {
        &mut self.plugin_main
    }

    /// Returns the synchronization authority for publishing worker-visible state.
    #[inline]
    pub fn worker_barrier(&self) -> crate::barrier::WorkerBarrier {
        self.barrier.clone()
    }

    /// Verifies that the current Engine is the main/control thread and that
    /// the worker barrier is held whenever Data Workers are running.
    pub fn ensure_main_thread_with_barrier(&self) -> RuntimeResult<()> {
        if self.thread_index != 0 {
            return Err(RuntimeError::ControlRequiresMainThread);
        }
        if self.barrier.worker_count() != 0 && !self.barrier.is_pending() {
            return Err(RuntimeError::ControlRequiresWorkerBarrier);
        }
        Ok(())
    }

    /// Add plugin roots and materialize their newly published runtime state.
    ///
    /// This is the sole plugin-loading interface. The main thread owns DSO
    /// activation, lifecycle/config dispatch, and append-only Graph Runtime
    /// mutation. Data Workers never load images or mutate graph topology.
    /// Apply the runtime-owned early configuration before any plugin image is
    /// loaded.
    pub fn configure_early(&mut self, config: &str) -> RuntimeResult<()> {
        crate::init::run_config_functions(self, true, config)
    }

    /// Add plugin roots and materialize their newly published runtime state.
    ///
    /// Registered owners deserialize their own declared section from `config`.
    pub fn load_plugins(&mut self, roots: &[String], config: &str) -> RuntimeResult<()> {
        if !self.memory_initialized {
            return Err(RuntimeError::MemoryNotInitialized);
        }

        let resume_main_loop = self.main_loop_entered;
        let resume_processes = self.processes.is_started();
        self.plugin_main.load(env!("CARGO_PKG_VERSION"), roots)?;
        let registration_generation = self.plugin_main.registration_generation();
        if registration_generation == self.materialized_registration_generation {
            return Ok(());
        }

        let worker_count = if resume_main_loop {
            self.barrier.worker_count()
        } else {
            0
        };
        let barrier = self.barrier.clone();
        barrier.sync(|| -> RuntimeResult<()> {
            crate::init::run_config_functions(self, true, config)?;
            crate::init::run_init_functions(self)?;
            let entries = self.plugin_main.graph_nodes();
            let functions = self.plugin_main.node_functions();
            self.runtime
                .extend_graph_with_node_functions(&entries, &functions)?;
            crate::init::run_config_functions(self, false, config)?;
            if worker_count != 0 {
                self.publish_worker_graph(worker_count)?;
            }
            Ok(())
        })?;
        if worker_count != 0 {
            self.finish_worker_graph_update()?;
        }
        self.materialized_registration_generation = registration_generation;
        if resume_main_loop {
            crate::init::run_main_loop_enter(self)?;
        }
        if resume_processes {
            self.start_process_nodes()?;
        }
        Ok(())
    }

    fn publish_worker_graph(&self, worker_count: u32) -> RuntimeResult<()> {
        assert_ne!(worker_count, 0, "worker graph publication requires workers");
        if self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            return Err(RuntimeError::WorkerGraphUpdateAlreadyPending);
        }
        let update = WorkerGraphUpdate {
            graph: self.runtime.nodes().snapshot(),
            worker_init_functions: self.plugin_main.worker_init_functions(),
        };
        // SAFETY: the main Engine calls this only while every worker is held at
        // `self.barrier`, before the refork completion count is published.
        unsafe {
            self.publication.set_graph(Some(update));
            self.publication.clear_graph_errors();
        }
        self.workers_updating_graph
            .store(worker_count, Ordering::Release);
        Ok(())
    }

    fn finish_worker_graph_update(&self) -> RuntimeResult<()> {
        // VPP recursive barrier semantics: when this load nests inside an
        // outer barrier (Binary API dispatch), the workers are parked and can
        // only refork at the outer release; they apply the pending update
        // then. Record the finish as deferred and let the outermost barrier
        // owner drain it after release
        // (`finish_deferred_worker_graph_update`).
        if self.barrier.recursion_level() != 0 {
            self.deferred_finish_pending.store(true, Ordering::Release);
            return Ok(());
        }
        self.drain_worker_graph_update()
    }

    /// Drains a graph publication whose finish was deferred by a nested
    /// barrier (VPP `need_vlib_worker_thread_node_runtime_update` is applied
    /// at the outermost `vlib_worker_thread_barrier_release`). The Binary API
    /// dispatch owner calls this immediately after its outer
    /// `WorkerBarrier::sync` releases, before the reply completes: it waits
    /// for every worker refork, drains each worker's refork error exactly
    /// once, and clears the deferred-finish record.
    pub fn finish_deferred_worker_graph_update(&self) -> RuntimeResult<()> {
        if !self.deferred_finish_pending.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.drain_worker_graph_update()
    }

    fn drain_worker_graph_update(&self) -> RuntimeResult<()> {
        while self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            spin_loop();
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
            // VPP waits for the refork cohort at the outermost release
            // (threads.c:1497); every worker failure is delivered to the owner
            // as one typed aggregate, exactly once, and never logged in
            // synchronization code.
            Err(RuntimeError::WorkerGraphUpdate { failures })
        }
    }

    pub(crate) fn refork_worker_graph(&mut self) -> bool {
        if self.workers_updating_graph.load(Ordering::Acquire) == 0 {
            return true;
        }

        // SAFETY: the main Engine published this value before releasing the
        // barrier and retains it until every worker completes the refork.
        let update = unsafe { self.publication.graph() }
            .as_ref()
            .expect("published worker graph must be present")
            .clone();
        let result = self
            .runtime
            .nodes()
            .replace_graph_preserving_worker_state(update.graph)
            .and_then(|()| {
                self.worker_init_functions = update.worker_init_functions;
                crate::init::run_worker_init_functions(self)
            });
        let succeeded = result.is_ok();
        if let Err(error) = result {
            self.main_loop_exit_now.store(true, Ordering::Release);
            let worker = self
                .data_worker_id()
                .expect("worker graph update runs only on a Data Worker");
            // SAFETY: each Data Worker owns exactly one error slot throughout
            // the refork; the main Engine reads it only after completion.
            unsafe { self.publication.set_graph_error(worker.slot(), error) };
        }

        let updating = self.workers_updating_graph.fetch_sub(1, Ordering::AcqRel);
        assert_ne!(updating, 0, "worker graph completion count underflow");
        while self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
        succeeded && !self.main_loop_exit_now.load(Ordering::Acquire)
    }

    pub fn spawn(&self, index: u32) -> RuntimeResult<Self> {
        self.spawn_on_numa(index, self.numa_node)
    }

    pub fn spawn_on_numa(&self, index: u32, numa_node: u32) -> RuntimeResult<Self> {
        Self::worker_with_runtime(
            self.runtime.for_worker(index, numa_node)?,
            Arc::clone(&self.registry),
            self.barrier.clone(),
            Arc::clone(&self.main_loop_exit_now),
            Arc::clone(&self.publication),
            Arc::clone(&self.workers_updating_graph),
            self.plugin_main.worker_init_functions(),
            self.memory_initialized,
            self.worker_config.clone(),
            index,
            numa_node,
        )
    }

    #[inline]
    pub fn data_worker_id(&self) -> RuntimeResult<DataWorkerId> {
        self.thread_index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or(RuntimeError::DataWorkerIdUnavailable {
                thread_index: self.thread_index,
            })
    }

    /// The configured number of data workers. This is runtime state, not a
    /// retained startup document.
    #[inline]
    pub fn configured_worker_count(&self) -> usize {
        self.worker_config.count
    }

    /// Schedules bounded control work on one running Data Worker.
    ///
    /// Runtime owns the queue and worker lifecycle only. The task runs on the
    /// selected worker and may reach that worker's concrete plugin state through
    /// the plugin's own ownership path; Runtime neither stores nor erases it.
    pub fn schedule_on_worker(
        &self,
        worker: DataWorkerId,
        task: impl FnOnce() + Send + 'static,
    ) -> RuntimeResult<()> {
        if self.thread_index != 0 {
            return Err(RuntimeError::WorkerControlRequiresMainEngine);
        }
        if worker.slot() >= self.worker_config.count {
            return Err(RuntimeError::DataWorkerIndexOutOfRange {
                worker: worker.slot(),
                worker_count: self.worker_config.count,
            });
        }
        let queue = self
            .worker_control_queues
            .get(worker.slot())
            .ok_or(RuntimeError::WorkerControlUnavailable { worker })?;
        queue.push(task).map_err(|error| match error {
            DataRemoteLocalQueueError::Closed => RuntimeError::WorkerControlClosed { worker },
            DataRemoteLocalQueueError::Full { capacity } => {
                RuntimeError::WorkerControlQueueFull { worker, capacity }
            }
        })
    }

    #[inline]
    pub(crate) fn worker_config(&self) -> &Worker {
        &self.worker_config
    }

    pub(crate) fn install_worker_control_queues(&mut self, queues: Arc<[DataRemoteLocalQueue]>) {
        self.worker_control_queues = queues;
    }

    pub(crate) fn worker_init_functions(&self) -> Vec<InitFunction> {
        if self.thread_index == 0 {
            self.plugin_main.worker_init_functions()
        } else {
            self.worker_init_functions.clone()
        }
    }

    pub fn register_worker_exit_function(
        &mut self,
        function: fn(&mut Engine) -> RuntimeResult<()>,
    ) {
        self.worker_exit_functions.push(function);
    }

    pub(crate) fn take_worker_exit_functions(
        &mut self,
    ) -> Vec<fn(&mut Engine) -> RuntimeResult<()>> {
        std::mem::take(&mut self.worker_exit_functions)
    }

    pub(crate) fn apply_worker_config(&mut self, worker: Worker) -> RuntimeResult<()> {
        if self.memory_initialized {
            return if self.worker_config == worker {
                Ok(())
            } else {
                Err(RuntimeError::WorkerConfigurationAlreadyInitialized)
            };
        }
        worker.validate()?;
        self.runtime = worker.create_runtime()?;
        self.worker_config = worker;
        self.memory_initialized = true;
        Ok(())
    }

    /// Replace only one cloned Data Worker's node-local runtime data.
    ///
    /// Worker initialization may use this after the main thread has built the
    /// graph. It cannot change node identity, process functions, or next arcs;
    /// those remain under main-thread Graph authority.
    pub fn set_worker_node_runtime_data(
        &mut self,
        node: NodeId,
        data: NodeRuntimeData,
    ) -> RuntimeResult<()> {
        self.data_worker_id()?;
        self.runtime.nodes().set_node_runtime_data(node, data)
    }

    pub fn file_main(&self) -> Ref<'_, FileMain> {
        self.runtime.file_main()
    }

    pub fn file_main_mut(&self) -> RefMut<'_, FileMain> {
        self.runtime.file_main_mut()
    }

    pub(crate) fn poll_file_readiness(&mut self) -> RuntimeResult<usize> {
        let graph = self.runtime.nodes();
        self.file_main_mut().poll(graph)
    }

    pub(crate) fn worker_seed(&self) -> EngineWorkerSeed {
        EngineWorkerSeed {
            runtime_seed: DataPlaneRuntimeWorkerSeed::from(&self.runtime),
            registry: Arc::clone(&self.registry),
            barrier: self.barrier.clone(),
            main_loop_exit_now: Arc::clone(&self.main_loop_exit_now),
            publication: Arc::clone(&self.publication),
            workers_updating_graph: Arc::clone(&self.workers_updating_graph),
            worker_init_functions: self.plugin_main.worker_init_functions(),
            memory_initialized: self.memory_initialized,
            worker_config: self.worker_config.clone(),
        }
    }

    pub(crate) fn prepare_worker_runtime_stats(&mut self, worker_count: usize) {
        self.publication = Arc::new(WorkerPublication::new(worker_count));
    }

    pub(crate) fn publish_worker_runtime_stats(&self) {
        let Some(worker) = self.thread_index.checked_sub(1).map(|index| index as usize) else {
            return;
        };
        let snapshot = WorkerRuntimeStats {
            thread_index: self.thread_index,
            numa_node: self.numa_node,
            main_loop_count: self.main_loop_count.load(Ordering::Relaxed),
            nodes: self.runtime.nodes().node_runtime_stats_snapshot(),
            files: self.file_main().runtime_stats_snapshot(),
        };
        // SAFETY: this worker owns its slot and publishes it before
        // acknowledging the worker barrier.
        unsafe { self.publication.set_runtime_stats(worker, snapshot) };
    }

    pub fn worker_runtime_stats_snapshot(&self) -> RuntimeResult<Vec<WorkerRuntimeStats>> {
        if self.thread_index != 0 {
            return Err(RuntimeError::lifecycle(
                "snapshot worker runtime statistics",
                "only the main Runtime Engine can synchronize data workers",
            ));
        }
        let worker_count = self.publication.worker_count();
        if worker_count == 0 {
            return Ok(Vec::new());
        }
        debug_assert_eq!(self.barrier.worker_count() as usize, worker_count);
        self.barrier.sync(|| {
            (0..worker_count)
                .map(|slot| {
                    // SAFETY: the enclosing worker barrier is held, so no
                    // worker can replace its statistics slot.
                    unsafe { self.publication.runtime_stats(slot) }
                        .cloned()
                        .ok_or_else(|| {
                            RuntimeError::lifecycle(
                                "snapshot worker runtime statistics",
                                format!("data worker {} did not publish its state", slot + 1),
                            )
                        })
                })
                .collect()
        })
    }

    pub(crate) fn retain_worker_threads(
        &mut self,
        threads: &mut Vec<JoinHandle<RuntimeResult<()>>>,
    ) -> RuntimeResult<()> {
        if !self.worker_threads.is_empty() {
            return Err(RuntimeError::DataWorkersAlreadyStarted);
        }
        self.worker_threads.extend(threads.drain(..));
        Ok(())
    }

    pub fn start_process_nodes(&mut self) -> RuntimeResult<()> {
        if self.thread_index != 0 {
            return Err(RuntimeError::ProcessNodesRequireMainEngine);
        }
        self.processes.start(
            Arc::clone(&self.registry),
            self.runtime.clone(),
            self.plugin_main.process_nodes(),
        )
    }

    pub fn process_handle(&self, name: &str) -> Option<ProcessHandle> {
        self.processes.handle(name)
    }

    pub fn run_processes_until<F>(&self, runtime: &tokio::runtime::Runtime, future: F) -> F::Output
    where
        F: std::future::Future,
    {
        self.processes.run_until(runtime, future)
    }

    pub fn shutdown_process_nodes(
        &mut self,
        runtime: &tokio::runtime::Runtime,
    ) -> RuntimeResult<()> {
        self.processes.shutdown(runtime)
    }

    fn join_worker_threads(&mut self) -> RuntimeResult<()> {
        let threads = std::mem::take(&mut self.worker_threads);
        let mut worker_error = None;
        let mut unwind_payload = None;
        for (worker, thread) in threads.into_iter().enumerate() {
            match thread.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) if worker_error.is_none() => worker_error = Some(error),
                Ok(Err(error)) => {
                    tracing::error!(worker, %error, "data worker failed during shutdown");
                }
                Err(payload) if unwind_payload.is_none() => unwind_payload = Some(payload),
                Err(payload) => {
                    tracing::error!(
                        worker,
                        panic = %thread_panic_message(payload),
                        "data worker panicked during shutdown"
                    );
                }
            }
        }
        if let Some(payload) = unwind_payload {
            std::panic::resume_unwind(payload);
        }
        match worker_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn install_current(&mut self) {
        CURRENT_ENGINE.with(|cell| {
            *cell.borrow_mut() = Some(self as *mut Engine);
        });
    }

    pub fn with_current<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&mut Engine) -> R,
    {
        CURRENT_ENGINE.with(|cell| {
            let ptr = *cell.borrow();
            ptr.map(|p| {
                let engine = unsafe { &mut *p };
                f(engine)
            })
        })
    }

    pub fn uninstall_current() {
        CURRENT_ENGINE.with(|cell| {
            *cell.borrow_mut() = None;
        });
    }
}

/// Verifies the current thread is the main/control engine and is inside the
/// worker-barrier phase when Data Workers are running.
pub fn ensure_main_thread_with_barrier() -> RuntimeResult<()> {
    match Engine::with_current(|engine| engine.ensure_main_thread_with_barrier()) {
        Some(result) => result,
        None => Err(RuntimeError::ControlRequiresMainThread),
    }
}

impl EngineWorkerSeed {
    #[inline]
    pub(crate) fn spawn_on_numa(
        self,
        index: u32,
        numa_node: u32,
        handoff: DataPlaneHandoffWorker,
    ) -> RuntimeResult<Engine> {
        let handoff_owner = handoff.worker();
        let Self {
            runtime_seed,
            registry,
            barrier,
            main_loop_exit_now,
            publication,
            workers_updating_graph,
            worker_init_functions,
            memory_initialized,
            worker_config,
        } = self;
        let runtime = DataPlaneRuntime::attach_handoff_worker(
            DataPlaneRuntime::try_from(DataPlaneRuntimeWorkerConfig {
                seed: runtime_seed,
                thread_index: index,
                numa_node,
            })?,
            handoff,
        );
        let engine = Engine::worker_with_runtime(
            runtime,
            registry,
            barrier,
            main_loop_exit_now,
            publication,
            workers_updating_graph,
            worker_init_functions,
            memory_initialized,
            worker_config,
            index,
            numa_node,
        )?;
        let worker = engine.data_worker_id()?;
        if worker != handoff_owner {
            return Err(RuntimeError::HandoffWorkerMismatch {
                worker,
                handoff_owner,
            });
        }
        Ok(engine)
    }
}

pub struct EnginePool {
    pub engines: Vec<Engine>,
    pub name: String,
    pub exec_path: String,
    pub argv: Vec<String>,
    pub startup_config: String,
    ipc_listener: Option<tokio::net::TcpListener>,
}

impl EnginePool {
    pub fn new(main: Engine) -> Self {
        let mut engines = Vec::new();
        engines.push(main);
        Self {
            engines,
            name: String::new(),
            exec_path: String::new(),
            argv: Vec::new(),
            startup_config: String::new(),
            ipc_listener: None,
        }
    }

    pub fn main_engine(&self) -> &Engine {
        &self.engines[0]
    }

    pub fn main_engine_mut(&mut self) -> &mut Engine {
        &mut self.engines[0]
    }

    pub fn worker_count(&self) -> usize {
        self.engines.len().saturating_sub(1)
    }

    pub fn engine(&self, index: usize) -> Option<&Engine> {
        self.engines.get(index)
    }

    pub fn engine_mut(&mut self, index: usize) -> Option<&mut Engine> {
        self.engines.get_mut(index)
    }

    pub fn set_ipc_listener(&mut self, listener: tokio::net::TcpListener) {
        self.ipc_listener = Some(listener);
    }

    pub fn take_ipc_listener(&mut self) -> Option<tokio::net::TcpListener> {
        self.ipc_listener.take()
    }

    pub fn main_loop_enter(
        engine: &mut Engine,
        roots: &[String],
        config: &str,
    ) -> RuntimeResult<()> {
        engine.install_current();
        engine.configure_early(config)?;
        engine.load_plugins(roots, config)?;
        crate::init::run_main_loop_enter(engine)?;
        engine.start_process_nodes()?;
        Ok(())
    }

    pub fn main_loop_exit(engine: &Engine) {
        engine
            .main_loop_exit_now
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn close(&mut self) -> RuntimeResult<()> {
        let barrier = self.main_engine().barrier.clone();
        let exit_result = barrier.sync(|| {
            let result = {
                let main = self.main_engine_mut();
                if main.main_loop_exit_functions_called {
                    Ok(())
                } else {
                    main.main_loop_exit_functions_called = true;
                    crate::init::run_main_loop_exit(main)
                }
            };
            Self::main_loop_exit(self.main_engine());
            result
        });
        let worker_result = self.main_engine_mut().join_worker_threads();
        if let Some(listener) = self.ipc_listener.take() {
            drop(listener);
        }
        worker_result.and(exit_result)
    }
}

pub(crate) fn thread_panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_owned(),
            Err(_) => "non-string panic payload".to_owned(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataPlaneBufferConfig;
    use crate::start_workers::start_workers;
    use hammer_runtime::RuntimeRegistry;
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};
    use std::sync::Arc;

    fn test_runtime() -> DataPlaneRuntime {
        let buffers = DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots: 16,
            frame_slots: 16,
            ..DataPlaneBufferConfig::default()
        };
        DataPlaneRuntime::new(DataPlaneRuntimeConfig { buffers })
    }

    fn test_engine() -> Engine {
        Engine::new(test_runtime(), RuntimeRegistry::new())
    }

    #[test]
    fn configured_engine_keeps_arenas_for_the_same_worker_config() {
        let mut worker = Worker::default();
        worker.buffer.slot_bytes = 64;
        worker.buffer.slots_per_numa = 16;
        worker.buffer.frame_pool_size = 16;
        worker.buffer.page_size = Some(hammer_infra::PageSize::Default);
        let mut engine = Engine::new_configured(RuntimeRegistry::new(), worker.clone())
            .expect("configured engine");
        let pool_id = engine
            .runtime
            .buffers()
            .buffer_arenas()
            .next()
            .expect("buffer arena")
            .pool_id();

        engine
            .apply_worker_config(worker)
            .expect("same worker config");

        assert_eq!(
            engine
                .runtime
                .buffers()
                .buffer_arenas()
                .next()
                .expect("buffer arena")
                .pool_id(),
            pool_id
        );
    }

    #[test]
    fn configured_engine_rejects_a_different_worker_config() {
        let mut worker = Worker::default();
        worker.buffer.slot_bytes = 64;
        worker.buffer.slots_per_numa = 16;
        worker.buffer.frame_pool_size = 16;
        worker.buffer.page_size = Some(hammer_infra::PageSize::Default);
        let mut engine = Engine::new_configured(RuntimeRegistry::new(), worker.clone())
            .expect("configured engine");
        let mut changed = worker;
        changed.buffer.frame_pool_size += 1;

        assert!(engine.apply_worker_config(changed).is_err());
    }

    #[test]
    fn spawn_shares_registry_and_resets_thread_index() {
        let main = test_engine();
        let worker = main.spawn(3).expect("spawn worker");
        assert_eq!(worker.thread_index, 3);
        assert_eq!(main.thread_index, 0);
        assert!(Arc::ptr_eq(&main.registry, &worker.registry));
    }

    #[test]
    fn data_worker_id_maps_one_based_engine_workers_to_zero_based_ids() {
        let main = test_engine();
        let worker_1 = main.spawn(1).expect("spawn worker 1");
        let worker_2 = main.spawn(2).expect("spawn worker 2");

        assert!(main.data_worker_id().is_err());
        assert_eq!(worker_1.data_worker_id().unwrap(), DataWorkerId::new(0));
        assert_eq!(worker_2.data_worker_id().unwrap(), DataWorkerId::new(1));
    }

    #[test]
    fn main_thread_cannot_replace_worker_node_runtime_data() {
        let mut main = test_engine();
        let error = main
            .set_worker_node_runtime_data(NodeId::new(0), NodeRuntimeData::empty())
            .expect_err("main thread must not bind worker runtime data");
        assert!(matches!(
            error,
            RuntimeError::DataWorkerIdUnavailable { thread_index: 0 }
        ));
    }

    #[test]
    fn spawn_resets_loop_count_and_shares_exit_flag() {
        let main = test_engine();
        main.main_loop_count
            .store(42, std::sync::atomic::Ordering::Relaxed);
        main.main_loop_exit_now
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let worker = main.spawn(1).expect("spawn worker");
        assert_eq!(
            worker
                .main_loop_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert!(Arc::ptr_eq(
            &main.main_loop_exit_now,
            &worker.main_loop_exit_now
        ));
        assert!(
            worker
                .main_loop_exit_now
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }

    #[test]
    fn engine_pool_main_engine_at_index_zero() {
        let main = test_engine();
        let pool = EnginePool::new(main);
        assert_eq!(pool.worker_count(), 0);
        assert!(pool.engine(0).is_some());
        assert!(pool.engine(1).is_none());
    }

    #[test]
    fn ensure_main_thread_with_barrier_requires_held_barrier_when_workers_exist() {
        let mut engine = test_engine();
        engine.barrier = crate::barrier::WorkerBarrier::new(1);
        engine.install_current();

        assert!(matches!(
            super::ensure_main_thread_with_barrier(),
            Err(RuntimeError::ControlRequiresWorkerBarrier)
        ));

        let barrier = engine.worker_barrier();
        barrier.arm();
        super::ensure_main_thread_with_barrier()
            .expect("control operation is inside the worker barrier");
        barrier.release();

        Engine::uninstall_current();
    }

    #[test]
    fn with_current_engine() {
        let mut engine = test_engine();
        engine.install_current();

        let result = Engine::with_current(|e| {
            e.thread_index = 42;
            e.thread_index
        });
        assert_eq!(result, Some(42));

        Engine::uninstall_current();
        let result = Engine::with_current(|_| true);
        assert_eq!(result, None);
    }

    // VPP `need_vlib_worker_thread_node_runtime_update`: a graph publication
    // finished inside an outer barrier defers its refork drain to the
    // outermost release. Both tests use real Data Workers that can refork only
    // after the outer barrier releases, so the shared statics serialize.
    static DEFERRED_FINISH_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    static FAIL_WORKER_REFORK: AtomicBool = AtomicBool::new(false);

    #[hammer_component_macros::worker_init_function(name = "injected_refork_failure")]
    fn injected_refork_failure(_: &mut Engine) -> RuntimeResult<()> {
        if FAIL_WORKER_REFORK.load(Ordering::Acquire) {
            return Err(RuntimeError::lifecycle(
                "injected worker refork failure".to_string(),
                "test injection".to_string(),
            ));
        }
        Ok(())
    }

    crate::__declare_registration_image!(
        init_functions = [];
        config_functions = [];
        early_config_functions = [];
        main_loop_enter_functions = [];
        main_loop_exit_functions = [];
        worker_init_functions = [__INIT_FN_INJECTED_REFORK_FAILURE];
        graph_nodes = [];
        node_functions = [];
        process_nodes = [];
        session_transports = [];
        session_apps = [];
        binary_api_methods = [];
    );

    fn deferred_finish_pool() -> EnginePool {
        let mut worker = Worker::default();
        worker.count = 2;
        worker.buffer.slot_bytes = 128;
        worker.buffer.slots_per_numa = 64;
        worker.buffer.frame_pool_size = 5;
        worker.buffer.page_size = Some(hammer_infra::PageSize::Default);
        EnginePool::new(
            Engine::new_configured(RuntimeRegistry::new(), worker).expect("configured main engine"),
        )
    }

    #[test]
    fn nested_graph_finish_defers_to_outer_barrier_release_and_drains_once() {
        let _serial = DEFERRED_FINISH_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut pool = deferred_finish_pool();
        start_workers(pool.main_engine_mut()).expect("worker startup");

        let engine = pool.main_engine();
        let outer = engine.worker_barrier();
        let nested = outer.sync(|| -> RuntimeResult<()> {
            engine.publish_worker_graph(outer.worker_count())?;
            engine.finish_worker_graph_update()?;
            // Workers are parked by the outer barrier, so the nested finish
            // must not wait for the refork completion count: it returns with
            // the publication still pending.
            assert_eq!(engine.workers_updating_graph.load(Ordering::Acquire), 2);
            Ok(())
        });
        nested.expect("nested finish returns without waiting for parked workers");

        // The outer release lets the workers refork; the deferred finish then
        // waits for the cohort, drains once, and clears the pending record.
        engine
            .finish_deferred_worker_graph_update()
            .expect("deferred finish waits for refork completion");
        assert_eq!(engine.workers_updating_graph.load(Ordering::Acquire), 0);
        engine
            .finish_deferred_worker_graph_update()
            .expect("repeat deferred finish is a no-op");

        // `EnginePool::close` parks the workers, sets the exit flag inside the
        // barrier, and joins: setting `main_loop_exit` before the sync would
        // let the workers observe it at step 8 and exit before parking.
        pool.close().expect("close worker pool");
    }

    #[test]
    fn deferred_finish_drains_worker_refork_error_once_and_clears_state() {
        let _serial = DEFERRED_FINISH_SERIAL
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        FAIL_WORKER_REFORK.store(false, Ordering::Release);
        let mut pool = deferred_finish_pool();
        start_workers(pool.main_engine_mut()).expect("worker startup");
        // Register the failing init only after startup, so each worker's
        // already-called set does not skip it during the deferred refork.
        pool.main_engine_mut()
            .plugin_main_mut()
            .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
        FAIL_WORKER_REFORK.store(true, Ordering::Release);

        let engine = pool.main_engine();
        let outer = engine.worker_barrier();
        outer
            .sync(|| -> RuntimeResult<()> {
                engine.publish_worker_graph(outer.worker_count())?;
                engine.finish_worker_graph_update()?;
                Ok(())
            })
            .expect("nested graph publication finishes without waiting");

        // The deferred finish surfaces the refork error instead of reporting
        // success, and clears the pending record: a repeat call is a no-op.
        engine
            .finish_deferred_worker_graph_update()
            .expect_err("deferred finish surfaces the worker refork error");
        engine
            .finish_deferred_worker_graph_update()
            .expect("pending state clears after the deferred drain");

        // The failed refork made both workers exit; join directly instead of
        // syncing the barrier with no workers parked.
        pool.main_engine_mut().join_worker_threads().ok();
        FAIL_WORKER_REFORK.store(false, Ordering::Release);
    }
}

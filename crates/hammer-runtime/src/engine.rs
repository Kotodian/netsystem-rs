use core::hint::spin_loop;
use std::any::{TypeId, type_name};
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashSet;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
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
use crate::{DataPlaneHandoffWorker, DataWorkerId, FileMain, PluginMain, ProcessHandle};

thread_local! {
    static CURRENT_ENGINE: RefCell<Option<*mut Engine>> = const { RefCell::new(None) };
}

pub(crate) struct EngineWorkerSeed {
    runtime_seed: DataPlaneRuntimeWorkerSeed,
    registry: Arc<RuntimeRegistry>,
    barrier: crate::barrier::WorkerBarrier,
    main_loop_exit_now: Arc<AtomicBool>,
    worker_graph: Arc<crate::barrier::Barrier<Option<WorkerGraphUpdate>>>,
    workers_updating_graph: Arc<AtomicU32>,
    worker_graph_errors: Arc<[crate::barrier::Barrier<Option<RuntimeError>>]>,
    worker_runtime_stats: Arc<[crate::barrier::Barrier<Option<WorkerRuntimeStats>>]>,
    worker_init_functions: Vec<InitFunction>,
    memory_initialized: bool,
    worker_config: Worker,
}

#[derive(Clone)]
pub(crate) struct WorkerGraphUpdate {
    pub(crate) graph: NodeRuntimeInner,
    pub(crate) worker_init_functions: Vec<InitFunction>,
}

type WorkerMainLoopCallback = fn(&mut Engine) -> RuntimeResult<()>;

struct ThreadState {
    value: NonNull<()>,
    value_type: TypeId,
    release: unsafe fn(NonNull<()>),
}

impl ThreadState {
    fn new<T: 'static>(value: T) -> Self {
        Self {
            value: NonNull::from(Box::leak(Box::new(value))).cast(),
            value_type: TypeId::of::<T>(),
            release: Self::release_value::<T>,
        }
    }

    #[inline]
    fn value<T: 'static>(&self) -> Option<&T> {
        (self.value_type == TypeId::of::<T>()).then(|| {
            // SAFETY: `new` records the allocation's concrete type, and the
            // shared borrow prevents mutable access for the returned lifetime.
            unsafe { self.value.cast::<T>().as_ref() }
        })
    }

    #[inline]
    fn value_mut<T: 'static>(&mut self) -> Option<&mut T> {
        (self.value_type == TypeId::of::<T>()).then(|| {
            // SAFETY: `new` records the allocation's concrete type, and the
            // exclusive borrow prevents any other access for this lifetime.
            unsafe { self.value.cast::<T>().as_mut() }
        })
    }

    fn into_value<T: 'static>(self) -> Option<T> {
        if self.value_type != TypeId::of::<T>() {
            return None;
        }
        let state = ManuallyDrop::new(self);
        // SAFETY: the matching TypeId proves that `new` allocated a `Box<T>`.
        // `ManuallyDrop` prevents the erased release function from running.
        Some(unsafe { *Box::from_raw(state.value.cast::<T>().as_ptr()) })
    }

    unsafe fn release_value<T>(value: NonNull<()>) {
        // SAFETY: `new` pairs this monomorphized function with one `Box<T>`.
        unsafe { drop(Box::from_raw(value.cast::<T>().as_ptr())) };
    }
}

impl Drop for ThreadState {
    fn drop(&mut self) {
        // SAFETY: this record uniquely owns the allocation created by `new`.
        unsafe { (self.release)(self.value) };
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerRuntimeStats {
    pub thread_index: u32,
    pub numa_node: u32,
    pub main_loop_count: u32,
    pub nodes: Vec<NodeRuntimeStatsRow>,
    pub files: Vec<FileRuntimeStatsRow>,
}

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
    worker_config: Worker,
    pub(crate) called_init_functions: HashSet<&'static str>,
    pub(crate) called_worker_init_functions: HashSet<&'static str>,
    pub(crate) called_early_config_functions: HashSet<&'static str>,
    pub(crate) called_config_functions: HashSet<&'static str>,
    pub(crate) called_main_loop_enter_functions: HashSet<&'static str>,
    pub(crate) called_main_loop_exit_functions: HashSet<&'static str>,
    pub(crate) main_loop_entered: bool,
    materialized_registration_generation: u64,
    pub(crate) worker_graph: Arc<crate::barrier::Barrier<Option<WorkerGraphUpdate>>>,
    pub(crate) workers_updating_graph: Arc<AtomicU32>,
    pub(crate) worker_graph_errors: Arc<[crate::barrier::Barrier<Option<RuntimeError>>]>,
    worker_runtime_stats: Arc<[crate::barrier::Barrier<Option<WorkerRuntimeStats>>]>,
    main_loop_exit_functions_called: bool,
    worker_threads: Vec<JoinHandle<RuntimeResult<()>>>,
    worker_main_loop_callbacks: Vec<WorkerMainLoopCallback>,
    thread_states: Vec<ThreadState>,
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
        worker_graph: Arc<crate::barrier::Barrier<Option<WorkerGraphUpdate>>>,
        workers_updating_graph: Arc<AtomicU32>,
        worker_graph_errors: Arc<[crate::barrier::Barrier<Option<RuntimeError>>]>,
        worker_runtime_stats: Arc<[crate::barrier::Barrier<Option<WorkerRuntimeStats>>]>,
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
            worker_config,
            called_init_functions: HashSet::new(),
            called_worker_init_functions: HashSet::new(),
            called_early_config_functions: HashSet::new(),
            called_config_functions: HashSet::new(),
            called_main_loop_enter_functions: HashSet::new(),
            called_main_loop_exit_functions: HashSet::new(),
            main_loop_entered: false,
            materialized_registration_generation: 0,
            worker_graph,
            workers_updating_graph,
            worker_graph_errors,
            worker_runtime_stats,
            main_loop_exit_functions_called: false,
            worker_threads: Vec::new(),
            worker_main_loop_callbacks: Vec::new(),
            thread_states: Vec::new(),
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
            worker_config: Worker::default(),
            called_init_functions: HashSet::new(),
            called_worker_init_functions: HashSet::new(),
            called_early_config_functions: HashSet::new(),
            called_config_functions: HashSet::new(),
            called_main_loop_enter_functions: HashSet::new(),
            called_main_loop_exit_functions: HashSet::new(),
            main_loop_entered: false,
            materialized_registration_generation: 0,
            worker_graph: Arc::new(crate::barrier::Barrier::new(None)),
            workers_updating_graph: Arc::new(AtomicU32::new(0)),
            worker_graph_errors: Arc::from([]),
            worker_runtime_stats: Arc::from([]),
            main_loop_exit_functions_called: false,
            worker_threads: Vec::new(),
            worker_main_loop_callbacks: Vec::new(),
            thread_states: Vec::new(),
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
        barrier.sync(self, |engine| -> RuntimeResult<()> {
            crate::init::run_config_functions(engine, true, config)?;
            crate::init::run_init_functions(engine)?;
            let entries = engine.plugin_main.graph_nodes();
            let functions = engine.plugin_main.node_functions();
            engine
                .runtime
                .extend_graph_with_node_functions(&entries, &functions)?;
            crate::init::run_config_functions(engine, false, config)?;
            if worker_count != 0 {
                engine.publish_worker_graph(worker_count)?;
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
            self.worker_graph
                .with_mut_unchecked(|graph| *graph = Some(update));
            for error in self.worker_graph_errors.iter() {
                error.with_mut_unchecked(|error| *error = None);
            }
        }
        self.workers_updating_graph
            .store(worker_count, Ordering::Release);
        Ok(())
    }

    fn finish_worker_graph_update(&self) -> RuntimeResult<()> {
        while self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            spin_loop();
        }

        // SAFETY: every worker completed its refork before decrementing the
        // counter to zero, so none can still access the graph or error slots.
        if unsafe { self.worker_graph.get_unchecked() }.is_none() {
            return Err(RuntimeError::WorkerGraphUpdateMissing);
        }

        let mut graph_update_error = None;
        for (worker, slot) in self.worker_graph_errors.iter().enumerate() {
            // SAFETY: the refork completion count is zero, so the worker that
            // owns this slot can no longer read or write it.
            if let Some(error) = unsafe { slot.with_mut_unchecked(Option::take) } {
                if graph_update_error.is_none() {
                    graph_update_error = Some(error);
                } else {
                    tracing::error!(worker, %error, "additional worker graph update failed");
                }
            }
        }
        graph_update_error.map_or(Ok(()), Err)
    }

    pub(crate) fn refork_worker_graph(&mut self) -> bool {
        if self.workers_updating_graph.load(Ordering::Acquire) == 0 {
            return true;
        }

        // SAFETY: the main Engine published this value before releasing the
        // barrier and retains it until every worker completes the refork.
        let update = unsafe { self.worker_graph.get_unchecked() }
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
            let slot = self
                .worker_graph_errors
                .get(worker.slot())
                .expect("worker graph error slot must exist");
            // SAFETY: each Data Worker owns exactly one error slot throughout
            // the refork; the main Engine reads it only after completion.
            unsafe {
                slot.with_mut_unchecked(|slot| *slot = Some(error));
            }
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
            Arc::clone(&self.worker_graph),
            Arc::clone(&self.workers_updating_graph),
            Arc::clone(&self.worker_graph_errors),
            Arc::clone(&self.worker_runtime_stats),
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

    /// Runs one Main Thread mutation while every Data Worker is paused.
    ///
    /// The worker barrier is the publication boundary. Worker main-loop
    /// callbacks run immediately after release and before File or Packet Graph
    /// dispatch, matching VPP's `worker_thread_main_loop_callbacks` ordering.
    pub fn synchronize_workers<R>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> R,
    ) -> RuntimeResult<R> {
        if self.thread_index != 0 {
            return Err(RuntimeError::WorkerBarrierMainThreadRequired {
                thread_index: self.thread_index,
            });
        }
        let expected = self.configured_worker_count();
        let active = self.barrier.worker_count() as usize;
        if active != expected {
            return Err(RuntimeError::WorkerBarrierUnavailable { expected, active });
        }
        let barrier = self.barrier.clone();
        Ok(barrier.sync(self, operation))
    }

    /// Installs state whose lifetime and thread affinity are owned by this
    /// Runtime Engine.
    pub fn install_thread_state<T: 'static>(&mut self, value: T) -> RuntimeResult<()> {
        let value_type = TypeId::of::<T>();
        if self
            .thread_states
            .iter()
            .any(|state| state.value_type == value_type)
        {
            return Err(RuntimeError::ThreadStateAlreadyInstalled {
                type_name: type_name::<T>(),
                thread_index: self.thread_index,
            });
        }
        self.thread_states.push(ThreadState::new(value));
        Ok(())
    }

    /// Borrows state owned by this Runtime Engine's thread lifecycle.
    #[inline]
    pub fn thread_state<T: 'static>(&self) -> RuntimeResult<&T> {
        self.thread_states
            .iter()
            .find_map(ThreadState::value::<T>)
            .ok_or(RuntimeError::ThreadStateMissing {
                type_name: type_name::<T>(),
                thread_index: self.thread_index,
            })
    }

    /// Mutably borrows state owned by this Runtime Engine's thread lifecycle.
    #[inline]
    pub fn thread_state_mut<T: 'static>(&mut self) -> RuntimeResult<&mut T> {
        self.thread_states
            .iter_mut()
            .find_map(ThreadState::value_mut::<T>)
            .ok_or(RuntimeError::ThreadStateMissing {
                type_name: type_name::<T>(),
                thread_index: self.thread_index,
            })
    }

    /// Removes and returns thread-bound state during orderly teardown.
    pub fn remove_thread_state<T: 'static>(&mut self) -> Option<T> {
        let position = self
            .thread_states
            .iter()
            .position(|state| state.value_type == TypeId::of::<T>())?;
        self.thread_states.remove(position).into_value()
    }

    /// Registers work invoked at the top of this Data Worker's main loop.
    ///
    /// The callback remains owned by this worker and therefore need not be
    /// `Send` or `Sync`. It runs after barrier/refork handling and before File
    /// readiness or Packet Graph dispatch.
    pub fn register_worker_main_loop_callback(
        &mut self,
        callback: WorkerMainLoopCallback,
    ) -> RuntimeResult<()> {
        self.data_worker_id()?;
        self.worker_main_loop_callbacks.push(callback);
        Ok(())
    }

    pub(crate) fn run_worker_main_loop_callbacks(&mut self) -> RuntimeResult<()> {
        let callbacks = self.worker_main_loop_callbacks.clone();
        callbacks
            .into_iter()
            .try_for_each(|callback| callback(self))
    }

    #[inline]
    pub(crate) fn worker_config(&self) -> &Worker {
        &self.worker_config
    }

    pub(crate) fn worker_init_functions(&self) -> Vec<InitFunction> {
        if self.thread_index == 0 {
            self.plugin_main.worker_init_functions()
        } else {
            self.worker_init_functions.clone()
        }
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
            worker_graph: Arc::clone(&self.worker_graph),
            workers_updating_graph: Arc::clone(&self.workers_updating_graph),
            worker_graph_errors: Arc::clone(&self.worker_graph_errors),
            worker_runtime_stats: Arc::clone(&self.worker_runtime_stats),
            worker_init_functions: self.plugin_main.worker_init_functions(),
            memory_initialized: self.memory_initialized,
            worker_config: self.worker_config.clone(),
        }
    }

    pub(crate) fn prepare_worker_runtime_stats(&mut self, worker_count: usize) {
        self.worker_runtime_stats = (0..worker_count)
            .map(|_| crate::barrier::Barrier::new(None))
            .collect::<Vec<_>>()
            .into();
        self.worker_graph_errors = (0..worker_count)
            .map(|_| crate::barrier::Barrier::new(None))
            .collect::<Vec<_>>()
            .into();
    }

    pub(crate) fn publish_worker_runtime_stats(&self) {
        let Some(slot) = self
            .thread_index
            .checked_sub(1)
            .and_then(|index| self.worker_runtime_stats.get(index as usize))
        else {
            return;
        };
        let snapshot = WorkerRuntimeStats {
            thread_index: self.thread_index,
            numa_node: self.numa_node,
            main_loop_count: self.main_loop_count.load(Ordering::Relaxed),
            nodes: self.runtime.nodes().node_runtime_stats_snapshot(),
            files: self.file_main().runtime_stats_snapshot(),
        };
        // SAFETY: each worker owns exactly one slot and replaces it before
        // acknowledging the barrier. The main Engine reads only after every
        // worker has acknowledged.
        unsafe {
            slot.with_mut_unchecked(|slot| *slot = Some(snapshot));
        }
    }

    pub fn worker_runtime_stats_snapshot(&self) -> RuntimeResult<Vec<WorkerRuntimeStats>> {
        if self.thread_index != 0 {
            return Err(RuntimeError::lifecycle(
                "snapshot worker runtime statistics",
                "only the main Runtime Engine can synchronize data workers",
            ));
        }
        let worker_count = self.worker_runtime_stats.len();
        if worker_count == 0 {
            return Ok(Vec::new());
        }
        debug_assert_eq!(self.barrier.worker_count() as usize, worker_count);
        let mut stats = Arc::clone(&self.worker_runtime_stats);
        self.barrier.sync(&mut stats, |stats| {
            stats
                .iter()
                .enumerate()
                .map(|(slot, snapshot)| {
                    // SAFETY: the enclosing worker barrier is held, so no
                    // worker can replace its statistics slot.
                    unsafe { snapshot.get_unchecked() }.clone().ok_or_else(|| {
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
            worker_graph,
            workers_updating_graph,
            worker_graph_errors,
            worker_runtime_stats,
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
            worker_graph,
            workers_updating_graph,
            worker_graph_errors,
            worker_runtime_stats,
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
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn close(&mut self) -> RuntimeResult<()> {
        Self::main_loop_exit(self.main_engine());
        let worker_result = self.main_engine_mut().join_worker_threads();
        let exit_result = {
            let main = self.main_engine_mut();
            if main.main_loop_exit_functions_called {
                Ok(())
            } else {
                main.main_loop_exit_functions_called = true;
                crate::init::run_main_loop_exit(main)
            }
        };
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
    use hammer_runtime::RuntimeRegistry;
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};
    use std::cell::Cell;
    use std::rc::Rc;
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
    fn runtime_engine_owns_non_send_thread_state() {
        let mut engine = test_engine();
        let value = Rc::new(Cell::new(41usize));

        engine
            .install_thread_state(Rc::clone(&value))
            .expect("install thread-bound value");
        engine
            .thread_state::<Rc<Cell<usize>>>()
            .expect("borrow thread-bound value")
            .set(42);
        assert_eq!(value.get(), 42);
        assert!(matches!(
            engine.install_thread_state(Rc::new(Cell::new(0usize))),
            Err(RuntimeError::ThreadStateAlreadyInstalled { .. })
        ));

        let removed = engine
            .remove_thread_state::<Rc<Cell<usize>>>()
            .expect("remove thread-bound value");
        assert_eq!(removed.get(), 42);
        assert!(matches!(
            engine.thread_state::<Rc<Cell<usize>>>(),
            Err(RuntimeError::ThreadStateMissing { .. })
        ));
    }

    #[test]
    fn worker_main_loop_callbacks_borrow_worker_owned_values() {
        let main = test_engine();
        let mut worker = main.spawn(1).expect("spawn worker Runtime Engine");
        worker
            .install_thread_state(Rc::new(Cell::new(0usize)))
            .expect("install worker value");
        worker
            .register_worker_main_loop_callback(|runtime| {
                let value = runtime.thread_state::<Rc<Cell<usize>>>()?;
                value.set(value.get() + 1);
                Ok(())
            })
            .expect("register worker callback");

        worker
            .run_worker_main_loop_callbacks()
            .expect("first callback pass");
        worker
            .run_worker_main_loop_callbacks()
            .expect("second callback pass");
        assert_eq!(
            worker
                .thread_state::<Rc<Cell<usize>>>()
                .expect("worker value remains installed")
                .get(),
            2
        );
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
}

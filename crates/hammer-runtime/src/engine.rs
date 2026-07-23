use core::hint::spin_loop;
use std::cell::RefCell;
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
use crate::init::InitFunction;
use crate::node::{NodeRuntimeData, NodeRuntimeInner};
use crate::process::ProcessMain;
use crate::{DataPlaneHandoffWorker, DataWorkerId, FileMain, PluginMain, ProcessHandle};

thread_local! {
    static CURRENT_ENGINE: RefCell<Option<*mut Engine>> = const { RefCell::new(None) };
}

pub(crate) struct EngineWorkerSeed {
    runtime_seed: DataPlaneRuntimeWorkerSeed,
    registry: Arc<RuntimeRegistry>,
    wait_at_barrier: Arc<AtomicU32>,
    workers_at_barrier: Arc<AtomicU32>,
    main_loop_exit_now: Arc<AtomicBool>,
    pending_worker_graph: Arc<Mutex<Option<WorkerGraphUpdate>>>,
    workers_updating_graph: Arc<AtomicU32>,
    worker_graph_update_error: Arc<Mutex<Option<RuntimeError>>>,
    worker_init_functions: Vec<InitFunction>,
    memory_initialized: bool,
    worker_config: Worker,
}

#[derive(Clone)]
struct WorkerGraphUpdate {
    graph: NodeRuntimeInner,
    worker_init_functions: Vec<InitFunction>,
}

#[repr(align(64))]
pub struct Engine {
    pub thread_index: u32,
    pub numa_node: u32,
    pub main_loop_count: AtomicU32,
    pub runtime: DataPlaneRuntime,
    pub registry: Arc<RuntimeRegistry>,
    pub wait_at_barrier: Arc<AtomicU32>,
    pub workers_at_barrier: Arc<AtomicU32>,
    pub main_loop_exit_now: Arc<AtomicBool>,
    pub main_loop_exit_status: Mutex<i32>,
    pub(crate) memory_initialized: bool,
    processes: ProcessMain,
    worker_init_functions: Vec<InitFunction>,
    worker_config: Worker,
    pub(crate) called_init_functions: HashSet<&'static str>,
    pub(crate) called_worker_init_functions: HashSet<&'static str>,
    pub(crate) called_early_config_functions: HashSet<&'static str>,
    pub(crate) called_config_functions: HashSet<&'static str>,
    pub(crate) called_main_loop_enter_functions: HashSet<&'static str>,
    pub(crate) called_main_loop_exit_functions: HashSet<&'static str>,
    pub(crate) main_loop_entered: bool,
    materialized_registration_generation: u64,
    pending_worker_graph: Arc<Mutex<Option<WorkerGraphUpdate>>>,
    workers_updating_graph: Arc<AtomicU32>,
    worker_graph_update_error: Arc<Mutex<Option<RuntimeError>>>,
    main_loop_exit_functions_called: bool,
    file_main: Option<FileMain>,
    worker_threads: Mutex<Vec<JoinHandle<()>>>,
    // Drop after every owner that may retain DSO code or Drop glue. Plugin
    // images themselves remain mapped for the full process lifetime.
    plugin_main: PluginMain,
}

impl Engine {
    #[inline]
    fn worker_with_runtime(
        runtime: DataPlaneRuntime,
        registry: Arc<RuntimeRegistry>,
        wait_at_barrier: Arc<AtomicU32>,
        workers_at_barrier: Arc<AtomicU32>,
        main_loop_exit_now: Arc<AtomicBool>,
        pending_worker_graph: Arc<Mutex<Option<WorkerGraphUpdate>>>,
        workers_updating_graph: Arc<AtomicU32>,
        worker_graph_update_error: Arc<Mutex<Option<RuntimeError>>>,
        worker_init_functions: Vec<InitFunction>,
        memory_initialized: bool,
        worker_config: Worker,
        index: u32,
        numa_node: u32,
    ) -> RuntimeResult<Self> {
        let worker = index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or_else(|| RuntimeError::invariant("data worker thread index must be non-zero"))?;
        Ok(Self {
            thread_index: index,
            numa_node,
            main_loop_count: AtomicU32::new(0),
            runtime,
            registry,
            wait_at_barrier,
            workers_at_barrier,
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
            pending_worker_graph,
            workers_updating_graph,
            worker_graph_update_error,
            main_loop_exit_functions_called: false,
            file_main: Some(FileMain::new(worker)?),
            worker_threads: Mutex::new(Vec::new()),
            processes: ProcessMain::new(),
        })
    }

    pub fn new(runtime: DataPlaneRuntime, registry: Arc<RuntimeRegistry>) -> Self {
        let pending_worker_graph = Arc::new(Mutex::new(None));
        let workers_updating_graph = Arc::new(AtomicU32::new(0));
        let worker_graph_update_error = Arc::new(Mutex::new(None));
        Self {
            thread_index: 0,
            numa_node: 0,
            main_loop_count: AtomicU32::new(0),
            runtime,
            registry,
            wait_at_barrier: Arc::new(AtomicU32::new(0)),
            workers_at_barrier: Arc::new(AtomicU32::new(0)),
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
            pending_worker_graph,
            workers_updating_graph,
            worker_graph_update_error,
            main_loop_exit_functions_called: false,
            file_main: None,
            worker_threads: Mutex::new(Vec::new()),
            processes: ProcessMain::new(),
        }
    }

    pub fn new_configured(registry: Arc<RuntimeRegistry>, worker: Worker) -> RuntimeResult<Self> {
        worker.validate()?;
        let runtime = crate::new_worker_runtime(&worker)?;
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
            let count = self
                .worker_threads
                .lock()
                .map_err(|_| RuntimeError::invariant("worker thread registry poisoned"))?
                .len();
            u32::try_from(count).map_err(|_| RuntimeError::WorkerCountOverflow { count })?
        } else {
            0
        };
        let barrier_guard = if worker_count == 0 {
            None
        } else {
            Some(crate::barrier::barrier_sync(
                &self.wait_at_barrier,
                &self.workers_at_barrier,
                worker_count,
            ))
        };

        crate::init::run_config_functions(self, true, config)?;
        crate::init::run_init_functions(self)?;
        let entries = self.plugin_main.graph_nodes();
        let functions = self.plugin_main.node_functions();
        self.runtime
            .extend_graph_with_node_functions(0, &entries, &functions)?;
        crate::init::run_config_functions(self, false, config)?;
        if worker_count != 0 {
            self.publish_worker_graph(worker_count)?;
        }
        drop(barrier_guard);
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
        if self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            return Err(RuntimeError::WorkerGraphUpdateAlreadyPending);
        }
        let update = WorkerGraphUpdate {
            graph: self.runtime.nodes().snapshot(),
            worker_init_functions: self.plugin_main.worker_init_functions(),
        };
        let mut published = self
            .pending_worker_graph
            .lock()
            .map_err(|_| RuntimeError::WorkerGraphUpdateStatePoisoned)?;
        if published.is_some() {
            return Err(RuntimeError::WorkerGraphUpdateAlreadyPending);
        }
        let mut error = self
            .worker_graph_update_error
            .lock()
            .map_err(|_| RuntimeError::WorkerGraphUpdateStatePoisoned)?;
        *error = None;
        *published = Some(update);
        self.workers_updating_graph
            .store(worker_count, Ordering::Release);
        Ok(())
    }

    fn finish_worker_graph_update(&self) -> RuntimeResult<()> {
        while self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
        let error = self
            .worker_graph_update_error
            .lock()
            .map_err(|_| RuntimeError::WorkerGraphUpdateStatePoisoned)?
            .take();
        self.pending_worker_graph
            .lock()
            .map_err(|_| RuntimeError::WorkerGraphUpdateStatePoisoned)?
            .take();
        match error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) fn apply_worker_graph_update_after_barrier(&mut self) -> bool {
        if self.workers_updating_graph.load(Ordering::Acquire) == 0 {
            return true;
        }

        let result = self
            .pending_worker_graph
            .lock()
            .map_err(|_| RuntimeError::WorkerGraphUpdateStatePoisoned)
            .and_then(|published| {
                published
                    .as_ref()
                    .cloned()
                    .ok_or(RuntimeError::WorkerGraphUpdateMissing)
            })
            .and_then(|update| {
                self.runtime
                    .nodes()
                    .replace_graph_preserving_worker_state(update.graph)?;
                self.worker_init_functions = update.worker_init_functions;
                crate::init::run_worker_init_functions(self)
            });
        let succeeded = result.is_ok();
        if let Err(error) = result {
            self.main_loop_exit_now.store(true, Ordering::Release);
            if let Ok(mut first_error) = self.worker_graph_update_error.lock()
                && first_error.is_none()
            {
                *first_error = Some(error);
            }
        }

        let previous = self.workers_updating_graph.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous, 0);
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
            self.runtime.for_worker(index, numa_node),
            Arc::clone(&self.registry),
            Arc::clone(&self.wait_at_barrier),
            Arc::clone(&self.workers_at_barrier),
            Arc::clone(&self.main_loop_exit_now),
            Arc::clone(&self.pending_worker_graph),
            Arc::clone(&self.workers_updating_graph),
            Arc::clone(&self.worker_graph_update_error),
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
            .ok_or_else(|| RuntimeError::invariant("main thread has no data worker id"))
    }

    /// The configured number of data workers. This is runtime state, not a
    /// retained startup document.
    #[inline]
    pub fn configured_worker_count(&self) -> usize {
        self.worker_config.count
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
                Err(RuntimeError::invariant(
                    "worker configuration cannot change after runtime initialization",
                ))
            };
        }
        worker.validate()?;
        self.runtime = crate::new_worker_runtime(&worker)?;
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

    pub fn file_main(&self) -> RuntimeResult<&FileMain> {
        self.file_main
            .as_ref()
            .ok_or_else(|| RuntimeError::invariant("main thread has no FileMain"))
    }

    pub fn file_main_mut(&mut self) -> RuntimeResult<&mut FileMain> {
        self.file_main
            .as_mut()
            .ok_or_else(|| RuntimeError::invariant("main thread has no FileMain"))
    }

    pub(crate) fn worker_seed(&self) -> EngineWorkerSeed {
        EngineWorkerSeed {
            runtime_seed: DataPlaneRuntimeWorkerSeed::from(&self.runtime),
            registry: Arc::clone(&self.registry),
            wait_at_barrier: Arc::clone(&self.wait_at_barrier),
            workers_at_barrier: Arc::clone(&self.workers_at_barrier),
            main_loop_exit_now: Arc::clone(&self.main_loop_exit_now),
            pending_worker_graph: Arc::clone(&self.pending_worker_graph),
            workers_updating_graph: Arc::clone(&self.workers_updating_graph),
            worker_graph_update_error: Arc::clone(&self.worker_graph_update_error),
            worker_init_functions: self.plugin_main.worker_init_functions(),
            memory_initialized: self.memory_initialized,
            worker_config: self.worker_config.clone(),
        }
    }

    pub(crate) fn retain_worker_threads(
        &self,
        threads: &mut Vec<JoinHandle<()>>,
    ) -> RuntimeResult<()> {
        let mut retained = self
            .worker_threads
            .lock()
            .map_err(|_| RuntimeError::invariant("worker thread registry poisoned"))?;
        if !retained.is_empty() {
            return Err(RuntimeError::invariant("data workers are already started"));
        }
        retained.extend(threads.drain(..));
        Ok(())
    }

    pub fn start_process_nodes(&mut self) -> RuntimeResult<()> {
        if self.thread_index != 0 {
            return Err(RuntimeError::invariant(
                "Process Nodes can only start on the main thread",
            ));
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

    fn join_worker_threads(&self) {
        let threads = self
            .worker_threads
            .lock()
            .map(|mut threads| std::mem::take(&mut *threads));
        let Ok(threads) = threads else {
            tracing::error!("worker thread registry poisoned during shutdown");
            return;
        };
        for thread in threads {
            if thread.join().is_err() {
                tracing::error!("data worker panicked during shutdown");
            }
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
        let worker = index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or_else(|| RuntimeError::invariant("data worker thread index must be non-zero"))?;
        let Self {
            runtime_seed,
            registry,
            wait_at_barrier,
            workers_at_barrier,
            main_loop_exit_now,
            pending_worker_graph,
            workers_updating_graph,
            worker_graph_update_error,
            worker_init_functions,
            memory_initialized,
            worker_config,
        } = self;
        let runtime = DataPlaneRuntime::attach_handoff_worker(
            DataPlaneRuntime::from(DataPlaneRuntimeWorkerConfig {
                seed: runtime_seed,
                thread_index: index,
                numa_node,
            }),
            worker,
            handoff,
        );
        Engine::worker_with_runtime(
            runtime,
            registry,
            wait_at_barrier,
            workers_at_barrier,
            main_loop_exit_now,
            pending_worker_graph,
            workers_updating_graph,
            worker_graph_update_error,
            worker_init_functions,
            memory_initialized,
            worker_config,
            index,
            numa_node,
        )
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
        self.main_engine().join_worker_threads();
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
        exit_result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataPlaneBufferConfig;
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
        assert!(
            error
                .to_string()
                .contains("main thread has no data worker id")
        );
    }

    #[test]
    fn spawn_shares_barrier_arcs() {
        let main = test_engine();
        let worker = main.spawn(1).expect("spawn worker");
        assert!(Arc::ptr_eq(&main.wait_at_barrier, &worker.wait_at_barrier));
        assert!(Arc::ptr_eq(
            &main.workers_at_barrier,
            &worker.workers_at_barrier
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

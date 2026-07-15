use core::hint::spin_loop;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use hammer_core::config::Config;
use hammer_core::data_plane::NodeId;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::DataPlaneRuntime;

use crate::data_plane::{DataPlaneRuntimeWorkerConfig, DataPlaneRuntimeWorkerSeed};
use crate::node::{NodeRuntimeData, NodeRuntimeInner};
use crate::process::ProcessMain;
use crate::{DataPlaneHandoffWorker, DataWorkerId, FileMain, PluginMain, ProcessHandle};
use hammer_infra::map::FlatHashTable;
use hammer_infra::vec::Vec;

thread_local! {
    static CURRENT_ENGINE: RefCell<Option<*mut Engine>> = const { RefCell::new(None) };
}

pub(crate) struct EngineWorkerSeed {
    runtime_seed: DataPlaneRuntimeWorkerSeed,
    registry: Arc<RuntimeRegistry>,
    wait_at_barrier: Arc<AtomicU32>,
    workers_at_barrier: Arc<AtomicU32>,
    main_loop_exit_now: Arc<AtomicBool>,
    pending_worker_graph: Arc<Mutex<Option<NodeRuntimeInner>>>,
    workers_updating_graph: Arc<AtomicU32>,
    worker_graph_update_error: Arc<Mutex<Option<HammerError>>>,
    memory_initialized: bool,
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
    pub(crate) called_init_functions: FlatHashTable<&'static str, ()>,
    pub(crate) called_worker_init_functions: FlatHashTable<&'static str, ()>,
    pub(crate) called_early_config_functions: FlatHashTable<&'static str, ()>,
    pub(crate) called_config_functions: FlatHashTable<&'static str, ()>,
    pub(crate) called_main_loop_enter_functions: FlatHashTable<&'static str, ()>,
    pub(crate) called_main_loop_exit_functions: FlatHashTable<&'static str, ()>,
    pub(crate) main_loop_entered: bool,
    materialized_registration_generation: u64,
    pending_worker_graph: Arc<Mutex<Option<NodeRuntimeInner>>>,
    workers_updating_graph: Arc<AtomicU32>,
    worker_graph_update_error: Arc<Mutex<Option<HammerError>>>,
    main_loop_exit_functions_called: bool,
    file_main: Option<FileMain>,
    worker_threads: Mutex<Vec<JoinHandle<()>>>,
    processes: ProcessMain,
}

impl Engine {
    #[inline]
    fn worker_with_runtime(
        runtime: DataPlaneRuntime,
        registry: Arc<RuntimeRegistry>,
        wait_at_barrier: Arc<AtomicU32>,
        workers_at_barrier: Arc<AtomicU32>,
        main_loop_exit_now: Arc<AtomicBool>,
        pending_worker_graph: Arc<Mutex<Option<NodeRuntimeInner>>>,
        workers_updating_graph: Arc<AtomicU32>,
        worker_graph_update_error: Arc<Mutex<Option<HammerError>>>,
        memory_initialized: bool,
        index: u32,
        numa_node: u32,
    ) -> HammerResult<Self> {
        let worker = index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or_else(|| HammerError::internal("data worker thread index must be non-zero"))?;
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
            called_init_functions: FlatHashTable::new(),
            called_worker_init_functions: FlatHashTable::new(),
            called_early_config_functions: FlatHashTable::new(),
            called_config_functions: FlatHashTable::new(),
            called_main_loop_enter_functions: FlatHashTable::new(),
            called_main_loop_exit_functions: FlatHashTable::new(),
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
            called_init_functions: FlatHashTable::new(),
            called_worker_init_functions: FlatHashTable::new(),
            called_early_config_functions: FlatHashTable::new(),
            called_config_functions: FlatHashTable::new(),
            called_main_loop_enter_functions: FlatHashTable::new(),
            called_main_loop_exit_functions: FlatHashTable::new(),
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

    pub fn loaded_plugins(&self) -> Vec<&'static str> {
        PluginMain::loaded_plugins()
    }

    /// Add plugin roots and materialize their newly published runtime state.
    ///
    /// This is the sole plugin-loading interface. The main thread owns DSO
    /// activation, lifecycle/config dispatch, and append-only Graph Runtime
    /// mutation. Data Workers never load images or mutate graph topology.
    pub fn load_plugins(
        &mut self,
        plugin_path: &std::path::Path,
        roots: &[String],
    ) -> HammerResult<()> {
        if !self.memory_initialized {
            return Err(HammerError::MemoryNotInitialized);
        }

        let resume_main_loop = self.main_loop_entered;
        let resume_processes = self.processes.is_started();
        PluginMain::load(env!("CARGO_PKG_VERSION"), plugin_path, roots)?;
        let registration_generation = crate::registration::generation();
        if registration_generation == self.materialized_registration_generation {
            return Ok(());
        }

        let worker_count = if resume_main_loop {
            let count = self.registry.require::<Config>()?.worker.count;
            u32::try_from(count).map_err(|_| HammerError::WorkerCountOverflow { count })?
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

        crate::init::run_config_functions(self, true)?;
        crate::init::run_init_functions(self)?;
        let entries = crate::registration::graph_nodes();
        let functions = crate::registration::node_functions();
        self.runtime
            .extend_graph_with_node_functions(0, &entries, &functions)?;
        crate::init::run_config_functions(self, false)?;
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

    fn publish_worker_graph(&self, worker_count: u32) -> HammerResult<()> {
        if self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            return Err(HammerError::WorkerGraphUpdateAlreadyPending);
        }
        let graph = self.runtime.nodes().snapshot();
        let mut published = self
            .pending_worker_graph
            .lock()
            .map_err(|_| HammerError::WorkerGraphUpdateStatePoisoned)?;
        if published.is_some() {
            return Err(HammerError::WorkerGraphUpdateAlreadyPending);
        }
        let mut error = self
            .worker_graph_update_error
            .lock()
            .map_err(|_| HammerError::WorkerGraphUpdateStatePoisoned)?;
        *error = None;
        *published = Some(graph);
        self.workers_updating_graph
            .store(worker_count, Ordering::Release);
        Ok(())
    }

    fn finish_worker_graph_update(&self) -> HammerResult<()> {
        while self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
        let error = self
            .worker_graph_update_error
            .lock()
            .map_err(|_| HammerError::WorkerGraphUpdateStatePoisoned)?
            .take();
        self.pending_worker_graph
            .lock()
            .map_err(|_| HammerError::WorkerGraphUpdateStatePoisoned)?
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
            .map_err(|_| HammerError::WorkerGraphUpdateStatePoisoned)
            .and_then(|published| {
                published
                    .as_ref()
                    .cloned()
                    .ok_or(HammerError::WorkerGraphUpdateMissing)
            })
            .and_then(|graph| {
                self.runtime.nodes().replace_graph(graph);
                self.called_worker_init_functions.clear();
                crate::init::run_worker_init_functions(self)
            });
        let succeeded = result.is_ok();
        if let Err(error) = result
            && let Ok(mut first_error) = self.worker_graph_update_error.lock()
            && first_error.is_none()
        {
            *first_error = Some(error);
        }

        let previous = self.workers_updating_graph.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous, 0);
        while self.workers_updating_graph.load(Ordering::Acquire) != 0 {
            spin_loop();
        }
        succeeded
    }

    pub fn spawn(&self, index: u32) -> HammerResult<Self> {
        self.spawn_on_numa(index, self.numa_node)
    }

    pub fn spawn_on_numa(&self, index: u32, numa_node: u32) -> HammerResult<Self> {
        Self::worker_with_runtime(
            self.runtime.for_worker(index, numa_node),
            Arc::clone(&self.registry),
            Arc::clone(&self.wait_at_barrier),
            Arc::clone(&self.workers_at_barrier),
            Arc::clone(&self.main_loop_exit_now),
            Arc::clone(&self.pending_worker_graph),
            Arc::clone(&self.workers_updating_graph),
            Arc::clone(&self.worker_graph_update_error),
            self.memory_initialized,
            index,
            numa_node,
        )
    }

    #[inline]
    pub fn data_worker_id(&self) -> HammerResult<DataWorkerId> {
        self.thread_index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or_else(|| HammerError::internal("main thread has no data worker id"))
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
    ) -> HammerResult<()> {
        self.data_worker_id()?;
        self.runtime.nodes().set_node_runtime_data(node, data)
    }

    pub fn file_main(&self) -> HammerResult<&FileMain> {
        self.file_main
            .as_ref()
            .ok_or_else(|| HammerError::internal("main thread has no FileMain"))
    }

    pub fn file_main_mut(&mut self) -> HammerResult<&mut FileMain> {
        self.file_main
            .as_mut()
            .ok_or_else(|| HammerError::internal("main thread has no FileMain"))
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
            memory_initialized: self.memory_initialized,
        }
    }

    pub(crate) fn retain_worker_threads(
        &self,
        threads: &mut Vec<JoinHandle<()>>,
    ) -> HammerResult<()> {
        let mut retained = self
            .worker_threads
            .lock()
            .map_err(|_| HammerError::internal("worker thread registry poisoned"))?;
        if !retained.is_empty() {
            return Err(HammerError::internal("data workers are already started"));
        }
        retained.extend(threads.drain(..));
        Ok(())
    }

    pub fn start_process_nodes(&mut self) -> HammerResult<()> {
        if self.thread_index != 0 {
            return Err(HammerError::internal(
                "Process Nodes can only start on the main thread",
            ));
        }
        self.processes
            .start(Arc::clone(&self.registry), self.runtime.clone())
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
    ) -> HammerResult<()> {
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
    ) -> HammerResult<Engine> {
        let worker = index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or_else(|| HammerError::internal("data worker thread index must be non-zero"))?;
        let Self {
            runtime_seed,
            registry,
            wait_at_barrier,
            workers_at_barrier,
            main_loop_exit_now,
            pending_worker_graph,
            workers_updating_graph,
            worker_graph_update_error,
            memory_initialized,
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
            memory_initialized,
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

    pub fn main_loop_enter(engine: &mut Engine) -> HammerResult<()> {
        engine.install_current();
        let config = engine.registry.require::<Config>()?;
        crate::memory::memory_init(engine, config)?;
        let plugin_path = crate::plugin_loader::configured_plugin_path()?;
        let roots = engine
            .registry
            .require::<Config>()?
            .requested_plugins()
            .to_vec();
        engine.load_plugins(&plugin_path, &roots)?;
        crate::init::run_main_loop_enter(engine)?;
        engine.start_process_nodes()?;
        Ok(())
    }

    pub fn main_loop_exit(engine: &Engine) {
        engine
            .main_loop_exit_now
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn close(&mut self) -> HammerResult<()> {
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
    use hammer_core::data_plane::DataPlaneBufferConfig;
    use hammer_core::registry::RuntimeRegistry;
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

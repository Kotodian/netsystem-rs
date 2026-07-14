use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use hammer_core::config::Config;
use hammer_core::error::{HammerError, HammerResult};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::DataPlaneRuntime;

use crate::{DataPlaneHandoffWorker, DataWorkerId, FileMain};
use hammer_infra::vec::Vec;

thread_local! {
    static CURRENT_ENGINE: RefCell<Option<*mut Engine>> = const { RefCell::new(None) };
}

pub(crate) struct EngineWorkerSeed<F> {
    runtime_seed: F,
    registry: Arc<RuntimeRegistry>,
    loaded_plugins: Arc<[&'static str]>,
    wait_at_barrier: Arc<AtomicU32>,
    workers_at_barrier: Arc<AtomicU32>,
    main_loop_exit_now: Arc<AtomicBool>,
}

#[repr(align(64))]
pub struct Engine {
    pub thread_index: u32,
    pub numa_node: u32,
    pub main_loop_count: AtomicU32,
    pub runtime: DataPlaneRuntime,
    pub registry: Arc<RuntimeRegistry>,
    /// Plugins selected from `Config.plugins` against the compiled catalog.
    loaded_plugins: Arc<[&'static str]>,
    pub wait_at_barrier: Arc<AtomicU32>,
    pub workers_at_barrier: Arc<AtomicU32>,
    pub main_loop_exit_now: Arc<AtomicBool>,
    pub main_loop_exit_status: Mutex<i32>,
    file_main: Option<FileMain>,
    worker_threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Engine {
    #[inline]
    fn worker_with_runtime(
        runtime: DataPlaneRuntime,
        registry: Arc<RuntimeRegistry>,
        loaded_plugins: Arc<[&'static str]>,
        wait_at_barrier: Arc<AtomicU32>,
        workers_at_barrier: Arc<AtomicU32>,
        main_loop_exit_now: Arc<AtomicBool>,
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
            loaded_plugins,
            wait_at_barrier,
            workers_at_barrier,
            main_loop_exit_now,
            main_loop_exit_status: Mutex::new(0),
            file_main: Some(FileMain::new(worker)?),
            worker_threads: Mutex::new(Vec::new()),
        })
    }

    pub fn new(runtime: DataPlaneRuntime, registry: Arc<RuntimeRegistry>) -> Self {
        Self {
            thread_index: 0,
            numa_node: 0,
            main_loop_count: AtomicU32::new(0),
            runtime,
            registry,
            loaded_plugins: Arc::from([]),
            wait_at_barrier: Arc::new(AtomicU32::new(0)),
            workers_at_barrier: Arc::new(AtomicU32::new(0)),
            main_loop_exit_now: Arc::new(AtomicBool::new(false)),
            main_loop_exit_status: Mutex::new(0),
            file_main: None,
            worker_threads: Mutex::new(Vec::new()),
        }
    }

    /// Plugins selected at `main_loop_enter` (empty until selection runs).
    #[inline]
    pub fn loaded_plugins(&self) -> &[&'static str] {
        &self.loaded_plugins
    }

    /// Validate `Config.plugins` and store the loaded set on this engine.
    pub fn select_plugins_from_config(&mut self) -> HammerResult<()> {
        let requested = self
            .registry
            .get::<Config>()
            .map(|config| config.requested_plugins().to_vec())
            .unwrap_or_default();
        let loaded = crate::plugin::select_loaded_plugins(&requested)?;
        self.loaded_plugins = Arc::from(loaded.as_slice());
        Ok(())
    }

    pub fn spawn(&self, index: u32) -> HammerResult<Self> {
        self.spawn_on_numa(index, self.numa_node)
    }

    pub fn spawn_on_numa(&self, index: u32, numa_node: u32) -> HammerResult<Self> {
        Self::worker_with_runtime(
            self.runtime.clone_for_worker(index, numa_node),
            Arc::clone(&self.registry),
            Arc::clone(&self.loaded_plugins),
            Arc::clone(&self.wait_at_barrier),
            Arc::clone(&self.workers_at_barrier),
            Arc::clone(&self.main_loop_exit_now),
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

    pub(crate) fn worker_seed(
        &self,
    ) -> HammerResult<EngineWorkerSeed<impl Fn(u32, u32) -> DataPlaneRuntime + Send + 'static>>
    {
        let runtime_seed = self.runtime.worker_seed()?;
        Ok(EngineWorkerSeed {
            runtime_seed: move |thread_index, numa_node| {
                runtime_seed.clone_for_worker(thread_index, numa_node)
            },
            registry: Arc::clone(&self.registry),
            loaded_plugins: Arc::clone(&self.loaded_plugins),
            wait_at_barrier: Arc::clone(&self.wait_at_barrier),
            workers_at_barrier: Arc::clone(&self.workers_at_barrier),
            main_loop_exit_now: Arc::clone(&self.main_loop_exit_now),
        })
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

impl<F> EngineWorkerSeed<F>
where
    F: Fn(u32, u32) -> DataPlaneRuntime + Send + 'static,
{
    #[inline]
    pub(crate) fn spawn_on_numa(
        &self,
        index: u32,
        numa_node: u32,
        handoff: DataPlaneHandoffWorker,
    ) -> HammerResult<Engine> {
        let worker = index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or_else(|| HammerError::internal("data worker thread index must be non-zero"))?;
        let runtime = DataPlaneRuntime::attach_handoff_worker(
            (self.runtime_seed)(index, numa_node),
            worker,
            handoff,
        );
        Engine::worker_with_runtime(
            runtime,
            Arc::clone(&self.registry),
            Arc::clone(&self.loaded_plugins),
            Arc::clone(&self.wait_at_barrier),
            Arc::clone(&self.workers_at_barrier),
            Arc::clone(&self.main_loop_exit_now),
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
        engine.select_plugins_from_config()?;
        crate::init::run_config_functions(engine, true)?;
        crate::init::run_init_functions(engine)?;
        crate::init::run_config_functions(engine, false)?;
        crate::init::run_main_loop_enter(engine)?;
        Ok(())
    }

    pub fn main_loop_exit(engine: &Engine) {
        engine
            .main_loop_exit_now
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn close(&mut self) {
        Self::main_loop_exit(self.main_engine());
        self.main_engine().join_worker_threads();
        if let Some(listener) = self.ipc_listener.take() {
            drop(listener);
        }
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
    fn spawn_resets_loop_count_and_exit_flag() {
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
        assert!(
            !worker
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

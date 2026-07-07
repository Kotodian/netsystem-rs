use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};

use hammer_adapter::DataPlaneRuntime;
use hammer_core::error::HammerResult;
use hammer_core::registry::RuntimeRegistry;

thread_local! {
    static CURRENT_ENGINE: RefCell<Option<*mut Engine>> = const { RefCell::new(None) };
}

pub(crate) struct EngineWorkerSeed<F> {
    runtime_seed: F,
    registry: Arc<RuntimeRegistry>,
    wait_at_barrier: Arc<AtomicU32>,
    workers_at_barrier: Arc<AtomicU32>,
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
    pub main_loop_exit_now: AtomicBool,
    pub main_loop_exit_status: Mutex<i32>,
}

impl Engine {
    #[inline]
    fn worker_with_runtime(
        runtime: DataPlaneRuntime,
        registry: Arc<RuntimeRegistry>,
        wait_at_barrier: Arc<AtomicU32>,
        workers_at_barrier: Arc<AtomicU32>,
        index: u32,
        numa_node: u32,
    ) -> Self {
        Self {
            thread_index: index,
            numa_node,
            main_loop_count: AtomicU32::new(0),
            runtime,
            registry,
            wait_at_barrier,
            workers_at_barrier,
            main_loop_exit_now: AtomicBool::new(false),
            main_loop_exit_status: Mutex::new(0),
        }
    }

    pub fn new(runtime: DataPlaneRuntime, registry: Arc<RuntimeRegistry>) -> Self {
        Self {
            thread_index: 0,
            numa_node: 0,
            main_loop_count: AtomicU32::new(0),
            runtime,
            registry,
            wait_at_barrier: Arc::new(AtomicU32::new(0)),
            workers_at_barrier: Arc::new(AtomicU32::new(0)),
            main_loop_exit_now: AtomicBool::new(false),
            main_loop_exit_status: Mutex::new(0),
        }
    }

    pub fn spawn(&self, index: u32) -> Self {
        self.spawn_on_numa(index, self.numa_node)
    }

    pub fn spawn_on_numa(&self, index: u32, numa_node: u32) -> Self {
        Self::worker_with_runtime(
            self.runtime.clone_for_worker(index, numa_node),
            Arc::clone(&self.registry),
            Arc::clone(&self.wait_at_barrier),
            Arc::clone(&self.workers_at_barrier),
            index,
            numa_node,
        )
    }

    pub(crate) fn worker_seed(
        &self,
    ) -> EngineWorkerSeed<impl Fn(u32, u32) -> DataPlaneRuntime + Send + 'static> {
        EngineWorkerSeed {
            runtime_seed: self.runtime.worker_seed(),
            registry: Arc::clone(&self.registry),
            wait_at_barrier: Arc::clone(&self.wait_at_barrier),
            workers_at_barrier: Arc::clone(&self.workers_at_barrier),
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
    pub(crate) fn spawn_on_numa(&self, index: u32, numa_node: u32) -> Engine {
        Engine::worker_with_runtime(
            (self.runtime_seed)(index, numa_node),
            Arc::clone(&self.registry),
            Arc::clone(&self.wait_at_barrier),
            Arc::clone(&self.workers_at_barrier),
            index,
            numa_node,
        )
    }
}

pub struct EnginePool {
    pub engines: hammer_infra::vec::Vec<Engine>,
    pub name: String,
    pub exec_path: String,
    pub argv: Vec<String>,
    pub startup_config: String,
    ipc_listener: Option<tokio::net::TcpListener>,
}

impl EnginePool {
    pub fn new(main: Engine) -> Self {
        let mut engines = hammer_infra::vec::Vec::new();
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
        crate::init::run_init_functions(engine)?;
        Ok(())
    }

    pub fn main_loop_exit(engine: &Engine) {
        engine
            .main_loop_exit_now
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn close(&mut self) {
        if let Some(listener) = self.ipc_listener.take() {
            drop(listener);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hammer_adapter::DataPlaneRuntime;
    use hammer_adapter::buffer::{DataPlaneBufferConfig, DataPlaneRuntimeConfig};
    use hammer_core::registry::RuntimeRegistry;
    use std::sync::Arc;

    fn test_runtime() -> DataPlaneRuntime {
        let buffers = DataPlaneBufferConfig {
            buffer_slot_capacity: 64,
            buffer_slots: 16,
            frame_capacity: 16,
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
        let worker = main.spawn(3);
        assert_eq!(worker.thread_index, 3);
        assert_eq!(main.thread_index, 0);
        assert!(Arc::ptr_eq(&main.registry, &worker.registry));
    }

    #[test]
    fn spawn_shares_barrier_arcs() {
        let main = test_engine();
        let worker = main.spawn(1);
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
        let worker = main.spawn(1);
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

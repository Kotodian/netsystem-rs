use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};

use crate::PluginMain;
use crate::config::Worker;
use crate::error::{RuntimeError, RuntimeResult};
use crate::global_main::{GlobalMain, WorkerPublication};
use crate::{DataPlaneMain, RuntimeRegistry};

impl GlobalMain {
    #[inline]
    pub fn control_thread(&self) -> &crate::ControlThread {
        &self.control_thread
    }

    #[inline]
    pub fn data_plane_main(&self) -> &DataPlaneMain {
        &self.main
    }

    #[inline]
    pub fn data_plane_main_mut(&mut self) -> &mut DataPlaneMain {
        &mut self.main
    }

    #[inline]
    pub fn registry(&self) -> &RuntimeRegistry {
        &self.registry
    }

    #[inline]
    pub fn thread_index(&self) -> u32 {
        self.main.thread_index()
    }

    pub fn new(runtime: DataPlaneMain, registry: Arc<RuntimeRegistry>) -> Self {
        let barrier = crate::barrier::WorkerBarrier::new(0);
        let main_loop_exit_now = Arc::new(AtomicBool::new(false));
        let main_loop_exit_status = Arc::new(Mutex::new(0));
        let publication = Arc::new(WorkerPublication::new(0));
        let workers_updating_graph = Arc::new(AtomicU32::new(0));
        let worker_config = Worker::default();
        let worker_control_queues = Arc::from([]);
        let mut main = runtime;
        main.install_global_control(
            Arc::clone(&registry),
            barrier.clone(),
            Arc::clone(&main_loop_exit_now),
            Arc::clone(&main_loop_exit_status),
            Arc::clone(&publication),
            Arc::clone(&workers_updating_graph),
            worker_config.clone(),
            Vec::new(),
            Arc::clone(&worker_control_queues),
        );
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
            main,
            control_thread: crate::ControlThread::new(
                std::time::Instant::now(),
                crate::log::Level::Info,
            ),
            registry,
            barrier,
            main_loop_exit_now,
            main_loop_exit_status,
            memory_initialized: false,
            plugin_main: PluginMain::default(),
            worker_config,
            called_init_functions: HashSet::new(),
            called_early_config_functions: HashSet::new(),
            called_config_functions: HashSet::new(),
            called_main_loop_enter_functions: HashSet::new(),
            called_main_loop_exit_functions: HashSet::new(),
            main_loop_entered: false,
            publication,
            workers_updating_graph,
            deferred_finish_pending: AtomicBool::new(false),
            main_loop_exit_functions_called: false,
            worker_threads: Vec::new(),
            worker_control_queues,
            control_file_main: None,
            ipc_listener: None,
            closed: false,
        }
    }

    pub fn new_configured(registry: Arc<RuntimeRegistry>, worker: Worker) -> RuntimeResult<Self> {
        worker.validate()?;
        let runtime = worker.create_runtime()?;
        let mut main = Self::new(runtime, registry);
        main.worker_config = worker;
        main.main.set_worker_config(main.worker_config.clone());
        main.memory_initialized = true;
        Ok(main)
    }

    #[inline]
    pub fn worker_barrier(&self) -> crate::barrier::WorkerBarrier {
        self.barrier.clone()
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
        self.main = worker.create_runtime()?;
        self.main.set_worker_config(worker.clone());
        self.main.install_global_control(
            Arc::clone(&self.registry),
            self.barrier.clone(),
            Arc::clone(&self.main_loop_exit_now),
            Arc::clone(&self.main_loop_exit_status),
            Arc::clone(&self.publication),
            Arc::clone(&self.workers_updating_graph),
            worker.clone(),
            Vec::new(),
            Arc::clone(&self.worker_control_queues),
        );
        self.worker_config = worker;
        self.memory_initialized = true;
        Ok(())
    }
}

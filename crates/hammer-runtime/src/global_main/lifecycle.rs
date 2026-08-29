use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::AsyncFileMain;
use crate::error::{RuntimeError, RuntimeResult};
use crate::global_main::GlobalMain;
use crate::node::NodeRuntime;
use crate::process::ProcessHandle;
use hammer_stats::StatsMain;

impl GlobalMain {
    pub fn start_process_nodes(&mut self) -> RuntimeResult<()> {
        if self.main.thread_index() != 0 {
            return Err(RuntimeError::ProcessNodesRequireGlobalMain);
        }
        self.processes.start(
            Arc::clone(&self.registry),
            self.main.clone(),
            self.plugin_main.process_nodes(),
        )
    }

    pub fn process_handle(&self, name: &str) -> Option<ProcessHandle> {
        self.processes.handle(name)
    }

    pub fn init_control(&mut self, runtime: &tokio::runtime::Runtime) -> RuntimeResult<()> {
        crate::file::init_file_main(self)?;
        let _entered = runtime.enter();
        self.control_file_main = Some(AsyncFileMain::new()?);
        Ok(())
    }

    pub fn run_processes_until<F>(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        future: F,
    ) -> RuntimeResult<F::Output>
    where
        F: std::future::Future,
    {
        let control_file_main = self.control_file_main.as_mut().ok_or_else(|| {
            RuntimeError::lifecycle(
                "run control processes",
                "control runtime is not initialized",
            )
        })?;
        let graph = NodeRuntime::default();
        runtime.block_on(async {
            let process_future = self.processes.run_until(future);
            tokio::pin!(process_future);
            loop {
                tokio::select! {
                    output = &mut process_future => return Ok(output),
                    result = control_file_main.next_ready(&graph) => {
                        result?;
                    },
                }
            }
        })
    }

    pub fn shutdown_process_nodes(
        &mut self,
        runtime: &tokio::runtime::Runtime,
    ) -> RuntimeResult<()> {
        self.processes.shutdown(runtime)
    }

    pub fn main_loop_enter(&mut self, roots: &[String], config: &str) -> RuntimeResult<()> {
        self.install_current();
        self.configure_early(config)?;
        self.load_plugins(roots, config)?;
        crate::init::run_config_functions(self, false, config)?;
        crate::init::run_init_functions(self)?;
        StatsMain::global()?;
        crate::init::run_stats_registrations(self)?;
        crate::init::run_main_loop_enter(self)?;
        self.start_process_nodes()
    }

    pub fn close(&mut self) -> RuntimeResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        let barrier = self.barrier.clone();
        let exit_result = barrier.sync(|| {
            let result = if self.main_loop_exit_functions_called {
                Ok(())
            } else {
                self.main_loop_exit_functions_called = true;
                crate::init::run_main_loop_exit(self)
            };
            self.main_loop_exit_now.store(true, Ordering::Release);
            result
        });
        let worker_result = self.join_worker_threads();
        drop(self.ipc_listener.take());
        let unlink_result = match StatsMain::global() {
            Ok(stats_main) => stats_main.unlink_socket_path().map_err(RuntimeError::from),
            Err(hammer_stats::StatsError::NotInitialized) => Ok(()),
            Err(error) => Err(RuntimeError::from(error)),
        };

        let mut first_error = None;
        for result in [exit_result, worker_result, unlink_result] {
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
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
                        panic = %super::thread_panic_message(payload),
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
}

use crate::error::{RuntimeError, RuntimeResult};
use crate::global_main::GlobalMain;
use crate::plugin::PluginMain;

impl GlobalMain {
    pub fn loaded_plugins(&self) -> Vec<String> {
        self.plugin_main.loaded_plugins()
    }

    pub fn plugin_main(&self) -> &PluginMain {
        &self.plugin_main
    }

    pub fn plugin_main_mut(&mut self) -> &mut PluginMain {
        &mut self.plugin_main
    }

    /// Load plugin images and publish their graph/init declarations.
    pub fn load_plugins(&mut self, roots: &[String], config: &str) -> RuntimeResult<()> {
        if !self.memory_initialized {
            return Err(RuntimeError::MemoryNotInitialized);
        }

        let resume_main_loop = self.main_loop_entered;
        let resume_processes = self.control_thread.is_started();
        let loaded_plugin_count = self.plugin_main.loaded_plugins().len();
        self.plugin_main.load(env!("CARGO_PKG_VERSION"), roots)?;
        if self.plugin_main.loaded_plugins().len() == loaded_plugin_count {
            return Ok(());
        }

        let worker_count = if resume_main_loop {
            crate::barrier::global()
                .expect("worker barrier is installed")
                .worker_count()
        } else {
            0
        };
        let mut published_refork = false;
        let mut load = || -> RuntimeResult<()> {
            crate::init::run_config_functions(self, true, config)?;
            crate::init::run_init_functions(self)?;
            let entries = self.plugin_main.graph_nodes();
            let functions = self.plugin_main.node_functions();
            self.main
                .extend_graph_with_node_functions(&entries, &functions)?;
            crate::init::run_config_functions(self, false, config)?;
            if worker_count != 0 {
                self.request_worker_graph_refork();
                published_refork = self.publish_worker_graph_refork(worker_count);
            }
            Ok(())
        };
        if resume_main_loop {
            crate::barrier::global()
                .expect("worker barrier is installed")
                .sync(load)?;
        } else {
            load()?;
        }
        if published_refork {
            self.wait_for_worker_graph_refork();
        }
        if resume_main_loop {
            crate::init::run_stats_registrations(self)?;
            crate::init::run_main_loop_enter(self)?;
        }
        if resume_processes {
            self.start_process_nodes()?;
        }
        Ok(())
    }
}

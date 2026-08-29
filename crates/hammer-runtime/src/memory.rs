use crate::config::{Memory, Worker};
use crate::error::RuntimeResult;
use crate::global_main::GlobalMain;
use hammer_component_macros::config_function;

impl Memory {
    pub fn ensure_main_heap(&self) -> RuntimeResult<usize> {
        self.validate()?;
        Ok(hammer_infra::main_heap::init_with(
            self.main_heap_size_bytes()?,
            self.main_heap_page_size,
            None,
        )?)
    }
}

#[config_function(name = "runtime_worker_config", section = "worker", early = true)]
fn configure_worker(worker: Worker, engine: &mut GlobalMain) -> RuntimeResult<()> {
    engine.apply_worker_config(worker)
}

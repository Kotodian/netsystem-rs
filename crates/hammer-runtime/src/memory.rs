use crate::config::{Memory, Worker};
use crate::engine::Engine;
use crate::error::RuntimeResult;
use hammer_component_macros::config_function;

pub fn ensure_main_heap(config: &Memory) -> RuntimeResult<()> {
    config.validate()?;
    hammer_infra::main_heap::init(config.main_heap_size)?;
    Ok(())
}

#[config_function(name = "runtime_worker_config", section = "worker", early = true)]
fn configure_worker(worker: Worker, engine: &mut Engine) -> RuntimeResult<()> {
    engine.apply_worker_config(worker)
}

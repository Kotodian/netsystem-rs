use std::sync::Arc;

use crate::engine::Engine;
use crate::new_worker_runtime;
use hammer_component_macros::init_function;
use hammer_core::config::Config;
use hammer_core::error::HammerResult;

pub(crate) fn ensure_main_heap(config: &Config) -> HammerResult<()> {
    hammer_infra::main_heap::init(config.memory.main_heap_size)?;
    Ok(())
}

#[init_function(name = "memory_init")]
pub fn memory_init(engine: &mut Engine, config: Arc<Config>) -> HammerResult<()> {
    if engine.memory_initialized {
        return Ok(());
    }
    engine.runtime = new_worker_runtime(&config)?;
    engine.memory_initialized = true;
    Ok(())
}

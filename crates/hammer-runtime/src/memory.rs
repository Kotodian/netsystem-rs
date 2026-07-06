use hammer_adapter::memory::{DEFAULT_MEMORY_CONFIG, MemoryMain};
use hammer_component_macros::init_function;
use hammer_core::error::HammerResult;

use crate::engine::Engine;

#[init_function(name = "memory_init", runs_before = ["start_workers"])]
pub fn memory_init(engine: &mut Engine) -> HammerResult<()> {
    let memory = MemoryMain::from_static_config(DEFAULT_MEMORY_CONFIG)?;
    engine.runtime = memory.runtime(engine.thread_index, engine.numa_node)?;
    Ok(())
}

use hammer_component_macros::init_function;
use hammer_core::config::Config;
use hammer_core::error::HammerResult;
use hammer_runtime::new_worker_runtime;

use crate::engine::Engine;

#[init_function(name = "memory_init")]
pub fn memory_init(engine: &mut Engine, config: Arc<Config>) -> HammerResult<()> {
    engine.runtime = new_worker_runtime(&config)?;
    Ok(())
}
use std::sync::Arc;

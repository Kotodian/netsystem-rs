use crate::config::Memory;
use crate::error::RuntimeResult;
use hammer_component_macros::config_function;

impl Memory {
    pub fn ensure_main_heap(&self) -> RuntimeResult<usize> {
        self.validate()?;
        let config = hammer_infra::mem::MainHeapConfig {
            size: self.main_heap_size,
            page_size: self.main_heap_page_size,
            default_hugepage_size: None,
        };
        Ok(config.initialize()?)
    }
}

#[config_function(name = "runtime_worker_config", section = "worker", early = true)]
fn configure_worker(section: toml::Table) -> RuntimeResult<()> {
    crate::config::worker::install(section)
}

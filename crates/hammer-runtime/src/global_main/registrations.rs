use crate::error::RuntimeResult;
use crate::global_main::GlobalMain;

impl GlobalMain {
    /// Apply runtime-owned configuration before plugin images are loaded.
    pub fn configure_early(&mut self, config: &str) -> RuntimeResult<()> {
        crate::init::run_config_functions(self, true, config)
    }
}

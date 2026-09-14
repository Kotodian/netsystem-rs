//! Early configuration for the process-wide physical-memory authority.

use std::num::NonZeroUsize;
use std::sync::OnceLock;

use crate::error::RuntimeResult;

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct PhysmemConfig {
    pub base_addr: Option<usize>,
    pub max_size: usize,
}

impl PhysmemConfig {
    pub fn validate(&self) -> RuntimeResult<()> {
        if self.base_addr == Some(0) {
            return Err(crate::RuntimeError::ConfigValidation {
                message: "physmem.base_addr must be non-zero when present".to_owned(),
            });
        }
        Ok(())
    }

    pub fn base_addr(&self) -> Option<NonZeroUsize> {
        self.base_addr.and_then(NonZeroUsize::new)
    }
}

static PHYSMEM_CONFIG: OnceLock<PhysmemConfig> = OnceLock::new();

pub(crate) fn physmem() -> &'static PhysmemConfig {
    PHYSMEM_CONFIG
        .get()
        .expect("physmem configuration is installed before BufferMain")
}

#[hammer_component_macros::config_function(
    name = "runtime_physmem_config",
    section = "physmem",
    early = true
)]
fn configure_physmem(config: PhysmemConfig) -> RuntimeResult<()> {
    config.validate()?;
    assert!(
        PHYSMEM_CONFIG.set(config).is_ok(),
        "physmem configuration callback executes once"
    );
    Ok(())
}

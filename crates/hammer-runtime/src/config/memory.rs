//! `[memory]` config for the VPP-shaped fixed-capacity main heap.

use byte_unit::Byte;
use hammer_infra::PageSize;

use crate::error::{RuntimeError, RuntimeResult};

pub const DEFAULT_MAIN_HEAP_SIZE: usize = hammer_infra::main_heap::DEFAULT_MAIN_HEAP_SIZE;
pub const DEFAULT_MAIN_HEAP_PAGE_SIZE: PageSize = PageSize::Default;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Memory {
    pub main_heap_size: Byte,
    pub main_heap_page_size: PageSize,
}

impl Default for Memory {
    fn default() -> Self {
        Self {
            main_heap_size: Byte::from_u64(DEFAULT_MAIN_HEAP_SIZE as u64),
            main_heap_page_size: DEFAULT_MAIN_HEAP_PAGE_SIZE,
        }
    }
}

impl Memory {
    pub fn validate(&self) -> RuntimeResult<()> {
        let requested = self.main_heap_size_bytes()?;
        let minimum = hammer_infra::main_heap::minimum_capacity();
        if requested < minimum {
            return Err(RuntimeError::config_validation(format!(
                "memory.main_heap_size must be at least {} bytes",
                minimum
            )));
        }
        if !self.main_heap_page_size.is_supported_on_current_platform() {
            return Err(RuntimeError::config_validation(format!(
                "memory.main_heap_page_size `{}` is unsupported on this platform",
                self.main_heap_page_size
            )));
        }
        Ok(())
    }

    pub(crate) fn main_heap_size_bytes(&self) -> RuntimeResult<usize> {
        usize::try_from(self.main_heap_size)
            .map_err(|_| RuntimeError::config_validation("memory.main_heap_size overflows usize"))
    }
}

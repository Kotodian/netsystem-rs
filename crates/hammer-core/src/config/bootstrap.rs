//! Allocation bootstrap configuration loaded before the Main Heap is ready.

use super::Memory;

/// The startup fields required to publish the fixed-capacity Main Heap.
///
/// Include paths and TOML parsing state are consumed inside
/// [`super::load_bootstrap_config`]. The returned value is allocation-free so
/// callers can copy out the capacity and end the bootstrap scope before
/// publishing the Main Heap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BootstrapConfig {
    pub memory: Memory,
}

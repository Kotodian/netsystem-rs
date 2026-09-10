use super::*;

/// Runtime Buffer Pool selection and local Frame policy.
///
/// Core owns packet storage and frame ownership. Runtime selects the worker and
/// NUMA layout used to publish the process Buffer Main before Workers.
#[derive(Debug, Clone)]
pub struct DataPlaneBufferConfig {
    pub buffer_slot_capacity: usize,
    pub buffer_slots: usize,
    pub frame_slots: usize,
    pub numa_nodes: &'static [u32],
    pub thread_index: u32,
    pub active_numa_node: u32,
    pub page_size: PageSize,
}

impl Default for DataPlaneBufferConfig {
    #[inline]
    fn default() -> Self {
        Self {
            buffer_slot_capacity: BUFFER_CACHE_LINE_SIZE,
            buffer_slots: 1024,
            frame_slots: DEFAULT_BUFFER_FRAME_POOL_SIZE,
            numa_nodes: &[0],
            thread_index: 0,
            active_numa_node: 0,
            page_size: PageSize::Default,
        }
    }
}

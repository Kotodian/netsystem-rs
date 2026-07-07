use hammer_adapter::buffer::{
    DEFAULT_BUFFER_FRAME_CAPACITY, DEFAULT_BUFFER_FRAME_POOL_SIZE, DataPlaneBufferConfig,
    DataPlaneRuntime, DataPlaneRuntimeConfig,
};
use hammer_component_macros::init_function;
use hammer_core::error::HammerResult;

use crate::engine::Engine;

const DEFAULT_BUFFER_SLOT_CAPACITY: usize = 2048;
const DEFAULT_BUFFER_SLOTS_PER_NUMA: usize = 4096;
const DEFAULT_NUMA_NODES: &[u32] = &[0];

#[init_function(name = "memory_init", runs_before = ["start_workers"])]
pub fn memory_init(engine: &mut Engine) -> HammerResult<()> {
    engine.runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: DEFAULT_BUFFER_SLOT_CAPACITY,
            buffer_slots: DEFAULT_BUFFER_SLOTS_PER_NUMA,
            frame_capacity: DEFAULT_BUFFER_FRAME_CAPACITY,
            frame_slots: DEFAULT_BUFFER_FRAME_POOL_SIZE,
            numa_nodes: DEFAULT_NUMA_NODES,
            thread_index: engine.thread_index,
            active_numa_node: engine.numa_node,
            ..DataPlaneBufferConfig::default()
        },
    });
    Ok(())
}

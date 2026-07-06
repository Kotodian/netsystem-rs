use std::sync::Arc;

use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::heap::Heap;

use crate::buffer::{
    BufferPoolArena, DEFAULT_BUFFER_FRAME_CAPACITY, DEFAULT_BUFFER_FRAME_POOL_SIZE,
    DataPlaneRuntime,
};

pub const HAMMER_MAX_NUMA_NODES: usize = 64;

#[derive(Clone, Copy)]
pub struct MemoryConfig {
    pub numa_nodes: &'static [u32],
    pub buffer_slot_capacity: usize,
    pub buffer_slots_per_numa: usize,
    pub frame_capacity: usize,
    pub frame_slots: usize,
}

pub const DEFAULT_MEMORY_CONFIG: MemoryConfig = MemoryConfig {
    numa_nodes: &[0],
    buffer_slot_capacity: 2048,
    buffer_slots_per_numa: 4096,
    frame_capacity: DEFAULT_BUFFER_FRAME_CAPACITY,
    frame_slots: DEFAULT_BUFFER_FRAME_POOL_SIZE,
};

pub struct StaticNumaTable<T, const N: usize> {
    entries: [Option<T>; N],
}

impl<T: Clone, const N: usize> Clone for StaticNumaTable<T, N> {
    fn clone(&self) -> Self {
        Self {
            entries: std::array::from_fn(|index| self.entries[index].clone()),
        }
    }
}

impl<T, const N: usize> StaticNumaTable<T, N> {
    pub fn new() -> Self {
        Self {
            entries: std::array::from_fn(|_| None),
        }
    }

    pub fn insert(&mut self, numa_node: u32, value: T) -> CoreResult<()> {
        let index = usize::try_from(numa_node)
            .map_err(|_| CoreError::config_validation("NUMA node does not fit usize"))?;
        let slot = self
            .entries
            .get_mut(index)
            .ok_or_else(|| CoreError::config_validation("NUMA node exceeds static memory table"))?;
        if slot.is_some() {
            return Err(CoreError::config_validation(
                "duplicate NUMA memory entry in static table",
            ));
        }
        *slot = Some(value);
        Ok(())
    }

    pub fn get(&self, numa_node: u32) -> Option<&T> {
        let index = usize::try_from(numa_node).ok()?;
        self.entries.get(index).and_then(Option::as_ref)
    }
}

impl<T, const N: usize> Default for StaticNumaTable<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MemoryMain {
    config: MemoryConfig,
    heaps: StaticNumaTable<Arc<Heap>, HAMMER_MAX_NUMA_NODES>,
    arenas: StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
}

impl MemoryMain {
    pub fn from_static_config(config: MemoryConfig) -> CoreResult<Self> {
        let mut heaps = StaticNumaTable::new();
        let mut arenas = StaticNumaTable::new();

        for &numa_node in config.numa_nodes {
            let heap = Arc::new(Heap::local(numa_node));
            arenas.insert(
                numa_node,
                BufferPoolArena::with_capacity_in(
                    config.buffer_slot_capacity,
                    config.buffer_slots_per_numa,
                    Arc::clone(&heap),
                ),
            )?;
            heaps.insert(numa_node, heap)?;
        }

        Ok(Self {
            config,
            heaps,
            arenas,
        })
    }

    pub fn runtime(&self, thread_index: u32, numa_node: u32) -> CoreResult<DataPlaneRuntime> {
        let arena = self.arenas.get(numa_node).cloned().ok_or_else(|| {
            CoreError::config_validation(format!(
                "no static buffer arena configured for thread {thread_index} on NUMA node {numa_node}"
            ))
        })?;
        debug_assert!(self.heaps.get(numa_node).is_some());
        Ok(DataPlaneRuntime::with_static_buffer_arena(
            arena,
            self.config.frame_capacity,
            self.config.frame_slots,
            numa_node,
        ))
    }
}

use super::*;

/// Runtime-owned buffer arena policy.
///
/// Core owns packet storage and frame ownership. Runtime selects the worker and
/// NUMA layout that constructs those storage arenas.
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

impl TryFrom<DataPlaneBufferConfig> for DataPlaneBuffers {
    type Error = DataPlaneError;

    fn try_from(config: DataPlaneBufferConfig) -> Result<Self, Self::Error> {
        config.create_buffers(config.numa_nodes.iter().copied())
    }
}

impl DataPlaneBufferConfig {
    fn create_buffers(
        &self,
        numa_nodes: impl IntoIterator<Item = u32>,
    ) -> DataPlaneResult<DataPlaneBuffers> {
        let arenas = numa_nodes
            .into_iter()
            .map(|numa_node| {
                BufferPoolArena::with_capacity_on_numa(
                    self.buffer_slot_capacity,
                    self.buffer_slots,
                    self.page_size,
                    numa_node,
                )
            })
            .collect::<DataPlaneResult<Vec<_>>>()?;
        Ok(DataPlaneBuffers::from_arenas(
            arenas,
            self.frame_slots,
            self.thread_index,
            self.active_numa_node,
        ))
    }
}

impl Worker {
    pub fn create_runtime(&self) -> RuntimeResult<DataPlaneMain> {
        let buffer = &self.buffer;
        let numa_nodes = self.buffer_numa_nodes()?;
        let create_buffers = |page_size| -> DataPlaneResult<DataPlaneBuffers> {
            DataPlaneBufferConfig {
                buffer_slot_capacity: buffer.slot_bytes,
                buffer_slots: buffer.slots_per_numa,
                frame_slots: buffer.frame_pool_size,
                active_numa_node: numa_nodes[0],
                page_size,
                ..DataPlaneBufferConfig::default()
            }
            .create_buffers(numa_nodes.iter().copied())
        };

        let buffers = match buffer.page_size {
            Some(page_size) => create_buffers(page_size)?,
            None => {
                #[cfg(target_os = "linux")]
                {
                    match create_buffers(PageSize::DefaultHuge) {
                        Ok(buffers) => buffers,
                        Err(source) => {
                            tracing::warn!(%source, "default HugeTLB Buffer Arena unavailable; using ordinary pages");
                            create_buffers(PageSize::Default)?
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    create_buffers(PageSize::Default)?
                }
            }
        };
        DataPlaneMain::from_buffers(buffers, native_simd_bytes())
    }

    fn buffer_numa_nodes(&self) -> RuntimeResult<Vec<u32>> {
        #[cfg(target_os = "linux")]
        {
            if self.numa.enabled {
                return self.buffer_numa_nodes_with(crate::numa::node_for_cpu);
            }
        }
        Ok(vec![0])
    }

    #[cfg(target_os = "linux")]
    fn buffer_numa_nodes_with(
        &self,
        mut node_for_cpu: impl FnMut(usize) -> RuntimeResult<u32>,
    ) -> RuntimeResult<Vec<u32>> {
        let mut nodes = Vec::with_capacity(self.count);
        for worker in 0..self.count {
            let core = crate::worker_thread::worker_core(worker, &self.cpu).ok_or_else(|| {
                RuntimeError::config_validation(format!(
                    "worker {worker} has no available CPU core"
                ))
            })?;
            let node = node_for_cpu(core)?;
            if node >= 32 {
                return Err(RuntimeError::config_validation(format!(
                    "worker CPU {core} resolves to unsupported NUMA node {node}"
                )));
            }
            nodes.push(node);
        }
        nodes.sort_unstable();
        nodes.dedup();
        Ok(nodes)
    }
}

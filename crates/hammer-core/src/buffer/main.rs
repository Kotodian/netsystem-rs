use std::cell::RefCell;
use std::sync::OnceLock;

use hammer_infra::{PageSize, physmem::PhysmemMap};
use spinning_top::Spinlock;

use super::{BUFFER_CACHE_LINE_SIZE, BUFFER_THREAD_CACHE_HIGH_WATER, Buffer};
use crate::error::{DataPlaneError, DataPlaneResult};

const MAX_BUFFER_POOLS: usize = 255;
const MAX_BUFFER_MEMORY: usize = 1 << 38;
const MAX_NUMA_NODES: usize = 32;

static BUFFER_MAIN: OnceLock<BufferMain> = OnceLock::new();

/// Process-wide authority for Physmem-backed packet Buffer Pools.
///
/// Mappings and address identity are fixed before publication. Pool free
/// indices are shared; each cache is accessed only by its bound worker.
pub struct BufferMain {
    pub(super) buffer_mem_start: usize,
    pub(super) buffer_mem_size: usize,
    pub(super) pools: Vec<BufferPool>,
    pub(super) default_pool_by_numa: [u8; MAX_NUMA_NODES],
}

// SAFETY: each Buffer Pool cache slot is permanently assigned to one runtime
// thread. Callers borrow only the slot selected by the executing thread index;
// shared Pool state uses its own synchronization.
unsafe impl Sync for BufferMain {}

pub(super) struct BufferPool {
    pub(super) mapping: PhysmemMap,
    pub(super) index: u8,
    pub(super) data_size: usize,
    pub(super) allocation_size: usize,
    pub(super) first_buffer: usize,
    pub(super) buffer_count: usize,
    pub(super) free: Spinlock<Vec<u32>>,
    // VPP buffer_known_hash equivalent: diagnostics only, never ownership.
    #[cfg(debug_assertions)]
    pub(super) known_allocated: Spinlock<std::collections::HashSet<u32>>,
    pub(super) workers: Box<[RefCell<BufferThreadCache>]>,
    pub(super) template: super::header::BufferTemplate,
}

#[repr(align(64))]
#[derive(Debug)]
/// The actual per-Pool Worker cache, borrowed exclusively for a runtime lifetime.
/// Its fields remain private; packet operations borrow the complete cache set.
pub struct BufferThreadCache {
    pub(super) pool_index: u8,
    pub(super) thread_index: u32,
    pub(super) indices: [u32; BUFFER_THREAD_CACHE_HIGH_WATER],
    pub(super) len: usize,
}

impl BufferMain {
    pub fn new(
        data_size: usize,
        buffers_per_numa: usize,
        numa_nodes: &[u32],
        worker_count: usize,
        page_size: PageSize,
    ) -> DataPlaneResult<&'static Self> {
        if numa_nodes.len() > MAX_BUFFER_POOLS {
            return Err(DataPlaneError::BufferPoolCountExceeded {
                requested: numa_nodes.len(),
                maximum: MAX_BUFFER_POOLS,
            });
        }
        if numa_nodes.is_empty() || buffers_per_numa == 0 || data_size == 0 {
            return Err(DataPlaneError::BufferPoolsUnavailable);
        }
        for &numa_node in numa_nodes {
            if numa_node as usize >= MAX_NUMA_NODES {
                return Err(DataPlaneError::NumaNodeExceedsStaticMemoryTable {
                    numa_node,
                    capacity: MAX_NUMA_NODES,
                });
            }
        }
        // vlib_buffer_alloc_size rounds to cachelines and selects an odd
        // cacheline count to distribute buffers across cache sets.
        let allocation_size = size_of::<Buffer>()
            .checked_add(data_size)
            .and_then(|size| size.checked_add(BUFFER_CACHE_LINE_SIZE - 1))
            .map(|size| (size & !(BUFFER_CACHE_LINE_SIZE - 1)) | BUFFER_CACHE_LINE_SIZE)
            .ok_or(DataPlaneError::BufferAllocationSizeOverflow { data_size })?;
        let page_bytes = page_size
            .bytes()
            .map_err(|source| DataPlaneError::BufferPoolMapping {
                numa_node: numa_nodes[0],
                source: hammer_infra::physmem::PhysmemError::HugePageDiscovery {
                    requested: page_size,
                    source,
                },
            })?;
        if allocation_size > page_bytes {
            return Err(DataPlaneError::BufferAllocationExceedsPage {
                allocation_size,
                page_size: page_bytes,
            });
        }
        let per_page = page_bytes / allocation_size;
        let mapping_size = buffers_per_numa
            .div_ceil(per_page)
            .checked_mul(page_bytes)
            .ok_or(DataPlaneError::BufferAllocationSizeOverflow { data_size })?;
        if mapping_size > MAX_BUFFER_MEMORY {
            return Err(DataPlaneError::BufferMemorySpanExceeded {
                bytes: mapping_size,
                maximum: MAX_BUFFER_MEMORY,
            });
        }
        let thread_count = worker_count
            .checked_add(1)
            .expect("worker count fits thread indexing");
        let mut mappings = Vec::with_capacity(numa_nodes.len());
        for &numa_node in numa_nodes {
            mappings.push(
                PhysmemMap::create("buffers", mapping_size, page_size, numa_node)
                    .map_err(|source| DataPlaneError::BufferPoolMapping { numa_node, source })?,
            );
        }
        // Establish the final base before producing any index: later mappings
        // may lie below an earlier one. No already-issued index is rebased.
        let start = mappings
            .iter()
            .map(|mapping| mapping.base() as usize)
            .min()
            .expect("nonempty Pool mapping registry");
        let end = mappings
            .iter()
            .map(|mapping| {
                (mapping.base() as usize)
                    .checked_add(mapping.size())
                    .expect("mapping address range fits usize")
            })
            .max()
            .expect("nonempty Pool mapping registry");
        let span = end - start;
        if span > MAX_BUFFER_MEMORY {
            return Err(DataPlaneError::BufferMemorySpanExceeded {
                bytes: span,
                maximum: MAX_BUFFER_MEMORY,
            });
        }
        let mut main = Self {
            buffer_mem_start: start,
            buffer_mem_size: span,
            pools: Vec::with_capacity(mappings.len()),
            default_pool_by_numa: [u8::MAX; MAX_NUMA_NODES],
        };
        for mapping in mappings {
            let index = main.pools.len() as u8;
            let mut template = super::header::BufferTemplate::default();
            template.buffer_pool_index = index;
            let base = mapping.base() as usize;
            let end = base + mapping.size();
            let first_buffer = base + allocation_size - base % allocation_size;
            let mut address = first_buffer;
            let mut indices = Vec::with_capacity(mapping.size() / allocation_size);
            while address < end - allocation_size {
                // Match vlib_buffer_pool_create, including its strict check
                // against the address immediately following the allocation.
                if address / mapping.page_size()
                    == (address + allocation_size) / mapping.page_size()
                {
                    let buffer_index = ((address - start) >> 6) as u32;
                    assert_ne!(buffer_index, 0, "Buffer Index zero is never allocated");
                    // SAFETY: the naturally aligned candidate lies wholly in
                    // this owned mapping/page; no Buffer users exist yet.
                    // Only the first-cacheline template is initialized here.
                    unsafe {
                        std::ptr::write(
                            address as *mut super::header::BufferTemplate,
                            template.clone(),
                        );
                    }
                    indices.push(buffer_index);
                }
                address += allocation_size;
            }
            if indices.is_empty() {
                return Err(DataPlaneError::BufferPoolsUnavailable);
            }
            let default = &mut main.default_pool_by_numa[mapping.numa_node() as usize];
            if *default == u8::MAX {
                *default = index;
            }
            let buffer_count = indices.len();
            main.pools.push(BufferPool {
                mapping,
                index,
                data_size,
                allocation_size,
                first_buffer,
                buffer_count,
                free: Spinlock::new(indices),
                #[cfg(debug_assertions)]
                known_allocated: Spinlock::new(std::collections::HashSet::with_capacity(
                    buffer_count,
                )),
                workers: (0..thread_count)
                    .map(|thread_index| {
                        RefCell::new(BufferThreadCache {
                            pool_index: index,
                            thread_index: thread_index as u32,
                            indices: [0; BUFFER_THREAD_CACHE_HIGH_WATER],
                            len: 0,
                        })
                    })
                    .collect(),
                template,
            });
        }
        assert!(
            BUFFER_MAIN.set(main).is_ok(),
            "Buffer Main is initialized exactly once"
        );
        Ok(Self::global())
    }

    pub fn global() -> &'static Self {
        BUFFER_MAIN
            .get()
            .expect("Buffer Main is published before worker initialization")
    }
}

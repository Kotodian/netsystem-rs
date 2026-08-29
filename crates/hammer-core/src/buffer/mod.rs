use core::ptr;
use core::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;
use std::fmt;
use std::ops::{Deref, DerefMut};

use crate::error::{BufferInvariant, DataPlaneError, DataPlaneResult};
use crate::graph::{NodeErrorIndex, NodeId};
use hammer_infra::{
    PageSize,
    align::align_up,
    physmem::PhysmemMap,
    prefetch::{prefetch_read_l1, prefetch_read_l2, prefetch_write_l1},
    simd::movemask_4,
};
use spinning_top::{
    RawRwSpinlock, RwSpinlock,
    lock_api::{MappedRwLockReadGuard, MappedRwLockWriteGuard},
    relax::Spin,
};
use std::rc::Rc;
use std::sync::Arc;

use self::memory::{HAMMER_MAX_NUMA_NODES, StaticNumaTable};

mod chain;
mod checked_out;
mod clone;
mod cursor;
mod flags;
mod frame;
mod frame_pool;
mod header;
mod index;
mod memory;
mod opaque;
mod pool;
mod prefetch;

pub use checked_out::{Frame, FrameBatchWidth, Next, Pending};
pub use cursor::BufferPacketCursor;
pub use flags::BufferFlags;
pub use frame::BufferFrame;
pub use index::Index;
pub use opaque::{PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES, PrimaryOpaque, SecondaryOpaque};

/// Production graph Frame logical maximum. Insertion enforces this limit even
/// though the underlying standard vector remains growable.
pub const DEFAULT_BUFFER_FRAME_CAPACITY: usize = 256;
pub const DEFAULT_BUFFER_FRAME_POOL_SIZE: usize = 64;
pub const BUFFER_CACHE_LINE_SIZE: usize = 64;
pub const DEFAULT_PACKET_HEADROOM: usize = 256;
const DEFAULT_PRE_DATA_SIZE: usize = 128;
const BUFFER_INVALID_INDEX: u32 = u32::MAX;

/// Number of free slots moved between the per-thread cache and the arena free
/// list in a single batch. Batching amortises the `Rc<RefCell>` borrow across
/// this many alloc/free operations.
const BUFFER_THREAD_CACHE_BATCH: usize = 32;
/// High-water mark at which the thread cache returns a batch back to the
/// arena free list, preventing unbounded cache growth and keeping arena free
/// list non-empty for other consumers.
const BUFFER_THREAD_CACHE_HIGH_WATER: usize = 512;
/// `in_use` is folded from the lazy `in_use_delta` counter once its absolute
/// value exceeds this threshold or when the count is read.
const BUFFER_IN_USE_FOLD_THRESHOLD: i32 = 64;

pub use header::Buffer;
pub(super) use header::buffer_data_offset;

#[derive(Clone)]
struct BufferSlot {
    generation: u32,
    allocated: bool,
}
struct BufferPoolInner {
    pool_id: u64,
    numa_node: u32,
    slot_capacity: usize,
    slot_stride: usize,
    region: PhysmemMap,
    region_size: usize,
    slot_states: Box<[BufferSlot]>,
    available_stack: Vec<u32>,
    total_slots: usize,
    in_use: usize,
    in_use_delta: i32,
}

impl fmt::Debug for BufferPoolInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferPoolInner")
            .field("pool_id", &self.pool_id)
            .field("numa_node", &self.numa_node)
            .field("slot_capacity", &self.slot_capacity)
            .field("slot_stride", &self.slot_stride)
            .field("region_base", &self.region.base())
            .field("region_size", &self.region_size)
            .field("available_len", &self.available_stack.len())
            .field("total_slots", &self.total_slots)
            .field("in_use", &self.in_use)
            .field("in_use_delta", &self.in_use_delta)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct BufferPoolArena {
    inner: Arc<RwSpinlock<BufferPoolInner>>,
}

type BufferMappedReadGuard<'a, T> = MappedRwLockReadGuard<'a, RawRwSpinlock<Spin>, T>;
type BufferMappedWriteGuard<'a, T> = MappedRwLockWriteGuard<'a, RawRwSpinlock<Spin>, T>;

#[derive(Debug)]
pub struct BufferRef<'a> {
    guard: BufferMappedReadGuard<'a, Buffer>,
}

impl Deref for BufferRef<'_> {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

#[derive(Debug)]
pub struct BufferRefMut<'a> {
    guard: BufferMappedWriteGuard<'a, Buffer>,
}

impl Deref for BufferRefMut<'_> {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for BufferRefMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

#[derive(Debug, Clone)]
struct BufferThreadCache {
    cached_slots: [u32; BUFFER_THREAD_CACHE_HIGH_WATER],
    len: usize,
}

impl BufferThreadCache {
    #[inline]
    fn new() -> Self {
        Self {
            cached_slots: [0; BUFFER_THREAD_CACHE_HIGH_WATER],
            len: 0,
        }
    }

    #[inline]
    fn cached_free_len(&self) -> usize {
        self.len
    }

    #[inline]
    fn push(&mut self, slot: u32) {
        debug_assert!(self.len < BUFFER_THREAD_CACHE_HIGH_WATER);
        self.cached_slots[self.len] = slot;
        self.len += 1;
    }

    #[inline]
    fn pop(&mut self) -> Option<u32> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(self.cached_slots[self.len])
    }

    #[inline]
    fn last(&self) -> Option<u32> {
        if self.len == 0 {
            return None;
        }
        Some(self.cached_slots[self.len - 1])
    }
}

#[derive(Debug)]
struct BufferPool {
    arena: BufferPoolArena,
    thread_cache: Rc<RefCell<BufferThreadCache>>,
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self {
            arena: self.arena.clone(),
            thread_cache: Rc::clone(&self.thread_cache),
        }
    }
}

#[derive(Debug)]
struct FrameSlot {
    generation: u32,
    allocated: bool,
    frame: Option<BufferFrame>,
}

#[derive(Debug)]
struct FramePoolInner {
    pool_id: u64,
    slots: Box<[FrameSlot]>,
    available: Box<[u32]>,
    available_len: usize,
    in_use: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct FramePool {
    inner: Rc<RefCell<FramePoolInner>>,
}

#[derive(Clone)]
pub struct DataPlaneBuffers {
    buffer_pools: StaticNumaTable<BufferPool, HAMMER_MAX_NUMA_NODES>,
    active_numa_node: u32,
    thread_index: u32,
    frames: FramePool,
    frame_slots: usize,
}

impl fmt::Debug for DataPlaneBuffers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataPlaneBuffers")
            .field("active_numa_node", &self.active_numa_node)
            .field("thread_index", &self.thread_index)
            .field("frame_capacity", &DEFAULT_BUFFER_FRAME_CAPACITY)
            .field("frame_slots", &self.frame_slots)
            .finish()
    }
}

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

#[inline]
fn next_pool_id() -> u64 {
    let id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
    if id == 0 || id == u64::MAX {
        // Nonzero namespace; never wrap to a previously used ID.
        abort_pool_id_namespace_exhausted();
    }
    id
}

#[inline(never)]
#[cold]
fn abort_pool_id_namespace_exhausted() -> ! {
    panic!("data-plane pool ID namespace exhausted");
}

/// Advance a slot generation. Retires the slot when the generation would wrap.
#[inline]
fn advance_generation(current: u32) -> Option<u32> {
    if current == u32::MAX {
        None
    } else {
        Some(current.wrapping_add(1).max(1))
    }
}

#[cold]
pub(super) fn abort_checked_out_frame() -> ! {
    std::process::abort()
}

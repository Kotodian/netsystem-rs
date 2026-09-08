use core::ptr;
use core::sync::atomic::{AtomicU64, Ordering};
use std::cell::RefCell;
use std::fmt;
use std::ops::{Deref, DerefMut};

use crate::error::{BufferInvariant, DataPlaneError, DataPlaneResult};
use crate::graph::{NodeErrorIndex, NodeId};
use hammer_infra::{
    PageSize,
    prefetch::{prefetch_read_l1, prefetch_write_l1},
    simd::movemask_4,
};
use spinning_top::{
    RawRwSpinlock,
    lock_api::{MappedRwLockReadGuard, MappedRwLockWriteGuard},
    relax::Spin,
};
use std::rc::Rc;

use self::memory::{HAMMER_MAX_NUMA_NODES, StaticNumaTable};

mod chain;
mod checked_out;
mod clone;
mod cursor;
mod flags;
mod frame;
mod frame_pool;
mod header;
mod main;
mod memory;
mod opaque;
mod operations;
mod pool;
mod prefetch;

pub use checked_out::{Frame, FrameBatchWidth, Next, Pending};
pub use cursor::BufferPacketCursor;
pub use flags::BufferFlags;
pub use frame::BufferFrame;
pub use main::BufferMain;
pub use opaque::{BufferOpaque, BufferOpaqueRegion, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES};
use opaque::{PrimaryOpaque, SecondaryOpaque};

/// Production graph Frame logical maximum. Insertion enforces this limit even
/// though the underlying standard vector remains growable.
pub const DEFAULT_BUFFER_FRAME_CAPACITY: usize = 256;
pub const DEFAULT_BUFFER_FRAME_POOL_SIZE: usize = 64;
pub const BUFFER_CACHE_LINE_SIZE: usize = 64;
pub const DEFAULT_PACKET_HEADROOM: usize = 256;
include!(concat!(env!("OUT_DIR"), "/buffer_config.rs"));
const BUFFER_INVALID_INDEX: u32 = 0;

/// Central free indices are transferred to a Worker cache in batches.
const BUFFER_THREAD_CACHE_BATCH: usize = 32;
/// High-water mark at which the thread cache returns a batch back to the
/// arena free list, preventing unbounded cache growth and keeping arena free
/// list non-empty for other consumers.
const BUFFER_THREAD_CACHE_HIGH_WATER: usize = 512;
pub use header::Buffer;
use main::{BufferPool, BufferThreadCache};

#[derive(Debug, Clone)]
pub struct BufferPoolArena {
    pool_index: u8,
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
    buffer_pools: StaticNumaTable<BufferPoolArena, HAMMER_MAX_NUMA_NODES>,
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

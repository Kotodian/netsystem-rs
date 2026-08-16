//! Shared segment header, placed at the base of the reserved first page.
//!
//! Mirrors `vlib_stats_shared_header_t` (version, base, epoch, in_progress,
//! directory) but replaces the raw directory pointer with an offset-based
//! directory allocation, so the directory can be relocated to a larger
//! block without invalidating reader pointers.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::directory::NULL_INDEX;
use crate::offset::Offset;

/// Fixed magic identifying a Hammer stats segment.
pub(crate) const STATS_MAGIC: u64 = u64::from_le_bytes(*b"HMSTATS!");
/// Layout version of the mapped structures.
pub(crate) const STATS_VERSION: u32 = 1;

/// Shared header occupying the first 128 bytes of the reserved first page.
///
/// The immutable identity fields (magic, version, capacity) are written once
/// at construction and never mutated afterwards; the structure fields are
/// atomics so that concurrent readers can observe publication boundaries.
/// The header itself is never exposed as `&mut` after construction.
#[repr(C)]
pub(crate) struct StatsHeader {
    magic: u64,
    version: u32,
    reserved0: u32,
    /// Total segment size in bytes (immutable after construction).
    capacity: u64,
    reserved1: [u64; 3],
    /// Monotonic structure epoch, bumped after every completed publication.
    epoch: AtomicU64,
    /// VPP-style sequence marker: set while a publication is in progress.
    in_progress: AtomicU64,
    /// Offset of the current directory block (relocated on growth).
    directory_offset: AtomicU64,
    /// Slot count of the current directory block.
    directory_capacity: AtomicU64,
    /// High-water mark of initialized slots in the current block.
    initialized_len: AtomicU64,
    /// Head of the free-slot list (u32 index or `NULL_INDEX`).
    free_list_head: AtomicU64,
    /// Head of the deferred-reclamation list (u32 index or `NULL_INDEX`).
    removed_list_head: AtomicU64,
    reserved2: [u64; 3],
}

const _: () = assert!(std::mem::size_of::<StatsHeader>() == 128);

impl StatsHeader {
    pub(crate) fn new(
        capacity: u64,
        directory_offset: u64,
        directory_capacity: u64,
    ) -> StatsHeader {
        StatsHeader {
            magic: STATS_MAGIC,
            version: STATS_VERSION,
            reserved0: 0,
            capacity,
            reserved1: [0; 3],
            epoch: AtomicU64::new(0),
            in_progress: AtomicU64::new(0),
            directory_offset: AtomicU64::new(directory_offset),
            directory_capacity: AtomicU64::new(directory_capacity),
            initialized_len: AtomicU64::new(0),
            free_list_head: AtomicU64::new(NULL_INDEX),
            removed_list_head: AtomicU64::new(NULL_INDEX),
            reserved2: [0; 3],
        }
    }

    pub(crate) fn directory_offset(&self) -> Offset {
        Offset::new(self.directory_offset.load(Ordering::Relaxed))
    }

    pub(crate) fn directory_capacity(&self) -> u64 {
        self.directory_capacity.load(Ordering::Relaxed)
    }

    pub(crate) fn initialized_len(&self) -> u64 {
        self.initialized_len.load(Ordering::Relaxed)
    }

    pub(crate) fn free_list_head(&self) -> u64 {
        self.free_list_head.load(Ordering::Relaxed)
    }

    pub(crate) fn removed_list_head(&self) -> u64 {
        self.removed_list_head.load(Ordering::Relaxed)
    }

    /// VPP sequence marker: set to 1 before prevalidated publication writes,
    /// cleared to 0 after the epoch is bumped — mirroring
    /// `vlib_stats_segment_lock`/`vlib_stats_segment_unlock`
    /// (stats.c:27,49). This is a protocol field, not a lock;
    /// `&mut StatsMain` is the only writer.
    ///
    /// The mark is a seq_cst store of 1. VPP's plain
    /// `shared_header->in_progress = 1` (stats.c:26-27) is ordered by the
    /// structural spinlock it holds, which supplies the begin boundary;
    /// Hammer omits that lock, so the mark store itself must prevent the
    /// following structural writes from becoming visible before the marker.
    /// A relaxed store would leave them unordered against the marker, and a
    /// reader could then observe the new writes against the old marker and
    /// an un-bumped epoch and accept a partial copy. This is a cold
    /// structural path, so the stronger ordering costs nothing.
    pub(crate) fn mark_in_progress(&self) {
        self.in_progress.store(1, Ordering::SeqCst);
    }

    /// Clears the marker with a release store that publishes the structural
    /// writes between the mark and this clear — the exact analogue of
    /// VPP's `__atomic_store_n (&shared_header->in_progress, 0,
    /// __ATOMIC_RELEASE)` (stats.c:49). Because the epoch bump is sequenced
    /// before this store, a reader whose re-check acquires a zero value
    /// here observes the bumped epoch and every structural write of the
    /// publication.
    pub(crate) fn clear_in_progress(&self) {
        self.in_progress.store(0, Ordering::Release);
    }

    /// Bumps the structure epoch after a completed publication, mirroring
    /// VPP's `epoch++` in `vlib_stats_segment_unlock` (stats.c:48). The
    /// increment is published by the subsequent release clear, exactly as
    /// VPP publishes the plain `epoch++` with its release store.
    pub(crate) fn bump_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn store_directory_offset(&self, offset: Offset) {
        self.directory_offset.store(offset.get(), Ordering::Relaxed);
    }

    pub(crate) fn store_directory_capacity(&self, slots: u64) {
        self.directory_capacity.store(slots, Ordering::Relaxed);
    }

    pub(crate) fn store_initialized_len(&self, len: u64) {
        self.initialized_len.store(len, Ordering::Relaxed);
    }

    pub(crate) fn store_free_list_head(&self, head: u64) {
        self.free_list_head.store(head, Ordering::Relaxed);
    }

    pub(crate) fn store_removed_list_head(&self, head: u64) {
        self.removed_list_head.store(head, Ordering::Relaxed);
    }
}

/// Identity and epoch reads: the documented reader surface of the shared
/// header, exercised by the internal verification test. Production readers
/// (the collector batch) consume them; kept available ahead of that.
#[allow(dead_code)]
impl StatsHeader {
    pub(crate) fn magic(&self) -> u64 {
        self.magic
    }

    pub(crate) fn version(&self) -> u32 {
        self.version
    }

    pub(crate) fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Snapshots the epoch with an acquire load. An acquire read pins the
    /// reader's subsequent copy below it (the copy cannot be hoisted above
    /// the snapshot), and the re-check after the acquire fence closes the
    /// bracket: a publication that overlaps the copy either leaves the
    /// marker set or bumps the epoch past this snapshot, so the re-check
    /// discards the copy. The orderings are the read side of the writer's
    /// release clear (see `clear_in_progress`).
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Reads the marker with an acquire load. The loop-top check uses the
    /// same ordering so the copy cannot bypass it, and the re-check's
    /// acquire load is what synchronizes with the writer's release clear
    /// (see `clear_in_progress`), making a completed publication fully
    /// visible before the copy is accepted.
    pub(crate) fn in_progress(&self) -> u64 {
        self.in_progress.load(Ordering::Acquire)
    }
}

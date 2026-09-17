//! The `/mem` family contract: the column order of one memory heap entry.
//!
//! This module is the Hammer counterpart of VPP's `vlib/stats/provider_mem.c`:
//! the column order and column meaning of `/mem/<name>` live here, not in
//! `StatsSegment`/`StatsMain`. Each heap owner declares its own entry through
//! `#[derive(Stats)]` and registers its own collector; that collector writes
//! its heap's reading through [`update_mem_usage`].

use hammer_infra::mem::HeapUsage;

use crate::protocol::DirectoryEntry;

/// Column indexes of one `/mem/<heap>` entry, VPP's `stat_mem_usage_e` order
/// (`third_party/vpp/src/vlib/stats/provider_mem.c:11-19`). The family owns the
/// order: declarations name their width and aliases through these constants and
/// [`update_mem_usage`] is the only writer.
pub const STAT_MEM_TOTAL: u32 = 0;
pub const STAT_MEM_USED: u32 = 1;
pub const STAT_MEM_FREE: u32 = 2;
pub const STAT_MEM_USED_MMAP: u32 = 3;
pub const STAT_MEM_TOTAL_ALLOC: u32 = 4;
pub const STAT_MEM_FREE_CHUNKS: u32 = 5;
pub const STAT_MEM_RELEASABLE: u32 = 6;
/// Width of one `/mem/<heap>` entry: `STAT_MEM_RELEASABLE + 1`.
pub const STAT_MEM_COLUMNS: u32 = STAT_MEM_RELEASABLE + 1;

/// Writes one heap reading into row 0 of its declared entry, like
/// `stat_provider_mem_usage_update_fn`.
///
/// The columns are VPP's `stat_mem_usage_e` order and only this module knows
/// the mapping; the value is one reading of the heap, not the heap itself.
/// The entry is the owning collector's own published entry, so the write path
/// is the entry-level cell write and there is no error to return.
pub fn update_mem_usage(entry: &DirectoryEntry, usage: HeapUsage) {
    entry.set_simple_counter_cell(0, STAT_MEM_TOTAL, usage.total_bytes);
    entry.set_simple_counter_cell(0, STAT_MEM_USED, usage.used_bytes);
    entry.set_simple_counter_cell(0, STAT_MEM_FREE, usage.free_bytes);
    entry.set_simple_counter_cell(0, STAT_MEM_USED_MMAP, usage.used_mmap_bytes);
    entry.set_simple_counter_cell(0, STAT_MEM_TOTAL_ALLOC, usage.max_allocated_bytes);
    entry.set_simple_counter_cell(0, STAT_MEM_FREE_CHUNKS, usage.free_chunk_count);
    entry.set_simple_counter_cell(0, STAT_MEM_RELEASABLE, usage.releasable_bytes);
}

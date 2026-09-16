//! The `/mem` family contract: one counter vector per memory heap.
//!
//! This module is the Hammer counterpart of VPP's `vlib/stats/provider_mem.c`:
//! the entry shape, the column order and the alias names of `/mem/<name>` live
//! here, not in `StatsSegment`/`StatsMain`. Every heap owner registers its own
//! entry with its own collector function, and the collector writes its heap's
//! reading through [`update_mem_usage`].

use hammer_infra::mem::HeapUsage;

use crate::protocol::DirectoryIndex;
use crate::{CollectorRegistration, StatsMain, StatsResult, StatsSegment};

/// Registers one memory heap entry.
///
/// `/mem/<name>` is a one-row, seven-column simple counter vector plus three
/// symlinks (`total`, `used`, `free`) and the caller's collector. Each heap
/// owner calls this once from its own registration with nothing but its name
/// and its own collector; this module does not know which heaps exist, does not
/// keep heap references and does not touch `StatsSegment` internals.
///
/// A failing step (duplicate name, shape, capacity) returns the existing
/// `StatsResult` error after removing the entries created by this call; the
/// collector is only registered once the entry and its aliases are complete.
pub fn register_mem_heap(
    stats_main: &StatsMain,
    name: &str,
    collect: fn(DirectoryIndex, u32),
) -> StatsResult<()> {
    let mut segment = stats_main.segment.lock();
    let path = format!("/mem/{name}");
    let counter = segment.add_simple_counter(&path)?;
    let index = counter.index;
    if let Err(error) = segment.validate(index, 0, 6) {
        // The segment is not published yet, so cleanup is local to the entries
        // this call created and the primary error is preserved.
        if let Err(cleanup_error) = segment.remove_entry(index) {
            eprintln!(
                "hammer-stats: failed to release mem entry {}: {cleanup_error}",
                index.raw()
            );
        }
        return Err(error);
    }
    let mut created = [index; 3];
    let mut created_count = 0;
    for (column, alias) in [(0_u32, "total"), (1_u32, "used"), (2_u32, "free")] {
        let alias_path = format!("{path}/{alias}");
        match segment.add_symlink(index, column, &alias_path) {
            Ok(created_index) => {
                created[created_count] = created_index;
                created_count += 1;
            }
            Err(error) => {
                // Same local cleanup as above: release the aliases this call
                // already created, then the entry itself, keeping `error`.
                for created_index in created[..created_count].iter().rev() {
                    if let Err(cleanup_error) = segment.remove_entry(*created_index) {
                        eprintln!(
                            "hammer-stats: failed to release mem entry {}: {cleanup_error}",
                            created_index.raw()
                        );
                    }
                }
                if let Err(cleanup_error) = segment.remove_entry(index) {
                    eprintln!(
                        "hammer-stats: failed to release mem entry {}: {cleanup_error}",
                        index.raw()
                    );
                }
                return Err(error);
            }
        }
    }
    drop(segment);
    // The collector runs after the entry and its aliases exist; the table is
    // written during startup only, so no synchronization with rounds is needed.
    stats_main.register_collector(CollectorRegistration {
        collect,
        entry_index: index,
        vector_index: 0,
    });
    Ok(())
}

/// Writes one heap reading into row `row` of a registered entry.
///
/// The columns are VPP's `stat_mem_usage_e` order, the same mapping
/// `stat_provider_mem_usage_update_fn` applies. The value is a reading, not a
/// heap object, and the write path is the segment's cell write, so there is no
/// error to return. The caller is the collector of that heap.
pub fn update_mem_usage(
    segment: &mut StatsSegment,
    index: DirectoryIndex,
    row: u32,
    usage: HeapUsage,
) {
    segment.set_simple_counter(index, row, 0, usage.total_bytes);
    segment.set_simple_counter(index, row, 1, usage.used_bytes);
    segment.set_simple_counter(index, row, 2, usage.free_bytes);
    segment.set_simple_counter(index, row, 3, usage.used_mmap_bytes);
    segment.set_simple_counter(index, row, 4, usage.max_allocated_bytes);
    segment.set_simple_counter(index, row, 5, usage.free_chunk_count);
    segment.set_simple_counter(index, row, 6, usage.releasable_bytes);
}

//! `/sys/node/*` declarations and the registered Node counter collector.
//! Counter storage belongs to each thread's existing Node Runtime slots.

use std::cell::UnsafeCell;

use hammer_component_macros::Stats;
use hammer_core::data_plane::NodeId;
use hammer_infra::bitmap::Bitmap;
use hammer_stats::{Collector, DirectoryIndex, NameVector, SimpleCounter, StatsMain, StatsSegment};

use crate::error::RuntimeResult;
use crate::thread_main::ThreadMain;

/// VPP `node_data`: only thread zero changes node-name and symlink metadata.
struct NodeSymlinks {
    names: UnsafeCell<Vec<Option<&'static str>>>,
    published: UnsafeCell<Vec<(Option<&'static str>, [Option<DirectoryIndex>; 4])>>,
}

// SAFETY: startup and graph publication run on thread zero; the collector
// Process runs on that same thread and never lends this metadata to workers.
unsafe impl Sync for NodeSymlinks {}

static NODE_SYMLINKS: NodeSymlinks = NodeSymlinks {
    names: UnsafeCell::new(Vec::new()),
    published: UnsafeCell::new(Vec::new()),
};

/// The thread-zero graph's current names, published before worker refork.
pub(crate) fn set_node_names(names: &[Option<&'static str>]) {
    crate::ensure_main_thread().expect("Node names are updated on thread zero");
    // SAFETY: the collector Process runs on this same thread and cannot run
    // while synchronous graph publication updates the names.
    unsafe { *NODE_SYMLINKS.names.get() = names.to_vec() };
}

/// One of the four counters of VPP's `node_counters[]` table: the leaf of its
/// directory name and of its `/nodes/<name>/<counter>` alias.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeCounter {
    Clocks,
    Vectors,
    Calls,
    Suspends,
}

impl NodeCounter {
    pub(crate) const ALL: [Self; 4] = [Self::Clocks, Self::Vectors, Self::Calls, Self::Suspends];

    /// VPP's `node_counters[].name`: `clocks|vectors|calls|suspends`.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Clocks => "clocks",
            Self::Vectors => "vectors",
            Self::Calls => "calls",
            Self::Suspends => "suspends",
        }
    }

    /// The entry this counter is published through, taken from the installed
    /// declaration rather than looked up by name again.
    fn entry_index(self, node_stats: &NodeStats) -> DirectoryIndex {
        match self {
            Self::Clocks => node_stats.clocks.index,
            Self::Vectors => node_stats.vectors.index,
            Self::Calls => node_stats.calls.index,
            Self::Suspends => node_stats.suspends.index,
        }
    }
}

/// The five `/sys/node/*` entries of VPP's node collector
/// (`collector.c:18-27,154-176`): the node-name vector and the four
/// thread-by-node counter vectors.
///
/// The declaration is installed by this module's collect registration when
/// `per_node_counters` is on, so the switch decides whether the family exists
/// at all; the row and column widths are graph facts published by
/// [`update_node_counters`] after the graph is materialized, which is why the
/// counter fields carry no `columns` argument.
#[derive(Stats)]
pub(crate) struct NodeStats {
    #[stats(path = "/sys/node/names")]
    names: NameVector,
    #[stats(path = "/sys/node/clocks")]
    pub(crate) clocks: SimpleCounter,
    #[stats(path = "/sys/node/vectors")]
    pub(crate) vectors: SimpleCounter,
    #[stats(path = "/sys/node/calls")]
    pub(crate) calls: SimpleCounter,
    #[stats(path = "/sys/node/suspends")]
    pub(crate) suspends: SimpleCounter,
}

/// Creates the five entries, like the
/// `if (sm->node_counters_enabled)` half of VPP's collector process startup
/// (`collector.c:154-176`).
///
/// The entries are created here rather than by the registration image so the
/// switch can decide their existence. One collector updates the whole family.
#[hammer_component_macros::stats_collect_registration]
fn register_node_stats(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    if !stats_main.segment.node_counters_enabled() {
        return Ok(());
    }
    NodeStats::register_owner(stats_main)?;
    stats_main.register_collector(NodeStats::global());
    Ok(())
}

impl Collector for &'static NodeStats {
    fn entry_index(&self) -> DirectoryIndex {
        self.names.index
    }

    fn collect(&self, segment: &StatsSegment) {
        update_node_counters(self, segment);
    }
}

/// VPP `update_node_counters` (`collector.c:38-128`): update changed names in
/// two passes, then write the four per-thread vectors. Single-writer relaxed
/// counters allow collection without pausing workers or resetting their pending
/// values. Reads may span an owner sync; collection is an approximate sample.
/// Only changed names acquire the stats segment lock. Both passes share one
/// directory epoch so a node-name swap removes every old link before adding
/// any replacement link.
fn update_node_counters(node_stats: &NodeStats, segment: &StatsSegment) {
    crate::ensure_main_thread().expect("Node counter collection executes on thread zero");
    // SAFETY: this Process and graph publication run on thread zero, without
    // an await in this operation. Workers never access either metadata vector.
    let names = unsafe { &*NODE_SYMLINKS.names.get() };
    let published = unsafe { &mut *NODE_SYMLINKS.published.get() };
    let columns = u32::try_from(names.len()).expect("node slots are u32-indexed");
    if published.len() < names.len() {
        published.resize(names.len(), (None, [None; 4]));
    }
    let mut changed = Bitmap::new();
    for (slot, current) in published.iter().enumerate() {
        if current.0 != names.get(slot).copied().flatten() {
            changed.set(slot);
        }
    }
    // VPP passes n_vlib_mains as the inclusive highest row to stats_validate.
    // Auxiliary runtime threads have no DataPlaneMain and are excluded.
    let last_thread = ThreadMain::global().worker_count() + 1;
    if !changed.is_empty() {
        let mut segment = segment.lock();
        for slot in changed.iter_set() {
            let current = &mut published[slot];
            if current.0.is_some() {
                for index in &mut current.1 {
                    if let Some(link) = *index {
                        segment
                            .remove_entry(link)
                            .expect("Node metadata owns a live symlink");
                        *index = None;
                    }
                }
                current.0 = None;
            }
        }
        // Keep this separate: an index can change its name to another index's old
        // name, so adding links during the removal pass would encounter duplicates.
        for slot in changed.iter_set() {
            let Some(name) = names.get(slot).copied().flatten() else {
                segment
                    .set_name(node_stats.names.index, slot as u32, "")
                    .expect("Node name removal updates its registered name vector");
                continue;
            };
            segment
                .set_name(node_stats.names.index, slot as u32, name)
                .expect("Node collector installs its registered name vector element");
            // VPP format_vlib_stats_symlink (`stats/format.c:10-23`).
            let symlink_name = name.replace('/', "_");
            for (index, counter) in NodeCounter::ALL.into_iter().enumerate() {
                let entry = counter.entry_index(node_stats);
                segment
                    .validate(entry, last_thread, columns - 1)
                    .expect("Node collector validates its registered counter vector");
                let path = format!("/nodes/{symlink_name}/{}", counter.name());
                published[slot].1[index] =
                    Some(segment.add_symlink(entry, slot as u32, &path).expect(
                        "Node collector creates a unique symlink after removing old names",
                    ));
            }
            published[slot].0 = Some(name);
        }
    }
    // VPP collector.c:98-124. Process poll counters are written on this same
    // thread through their existing stats cells and are not overwritten here.
    for thread in 0..last_thread {
        let nodes = ThreadMain::global().node_slots(thread);
        for (index, node) in nodes.iter().enumerate() {
            if node.is_process() {
                continue;
            }
            let (calls, vectors, clocks) = node.counter_values();
            let column = index as u32;
            segment.set_simple_counter(node_stats.clocks.index, thread, column, clocks);
            segment.set_simple_counter(node_stats.vectors.index, thread, column, vectors);
            segment.set_simple_counter(node_stats.calls.index, thread, column, calls);
            segment.set_simple_counter(node_stats.suspends.index, thread, column, 0);
        }
    }
}

/// Initialize the existing thread-zero cells before a Process can be polled.
/// Reusing a Process Node must not reuse its previous execution's counters.
pub(crate) fn initialize_process_counters(node: NodeId) -> RuntimeResult<()> {
    let stats = StatsMain::global()?;
    if !stats.segment.node_counters_enabled() {
        return Ok(());
    }
    let counters = NodeStats::global();
    let column = node.slot();
    {
        let mut segment = stats.segment.lock();
        for counter in NodeCounter::ALL {
            segment.validate(counter.entry_index(counters), 0, column)?;
        }
    }
    for counter in NodeCounter::ALL {
        stats
            .segment
            .set_simple_counter(counter.entry_index(counters), 0, column, 0);
    }
    Ok(())
}

/// A thread-zero Process updates the existing Node stats row after each poll.
pub(crate) fn count_process_poll(node: NodeId, pending: bool, clocks: u64) {
    let stats_main = StatsMain::global().expect("Process runtime has initialized stats");
    if !stats_main.segment.node_counters_enabled() {
        return;
    }
    let counters = NodeStats::global();
    let column = node.slot();
    stats_main
        .segment
        .increment_simple_counter(counters.clocks.index, 0, column, clocks);
    let entry = if pending {
        counters.suspends.index
    } else {
        counters.calls.index
    };
    stats_main
        .segment
        .increment_simple_counter(entry, 0, column, 1);
}

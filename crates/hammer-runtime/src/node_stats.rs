//! `/sys/node/*` owner glue: the per-thread node counters, the declaration of
//! the five directory entries, the four collectors and the single publish step
//! that turns the graph's node identities into shape, names and aliases.
//!
//! VPP keeps the same three pieces apart: the per-thread counters live in that
//! thread's `vlib_node_runtime_t` (`node.h:482-503`, written by the dispatch
//! point in `main.c:546-577`), the collector table row is
//! `vlib_stats_collector_t` (`stats.h:51-57`), and the graph facts are
//! published once by the collector process
//! (`collector.c:40-93,154-176`). The mechanism crate knows none of them.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use hammer_component_macros::Stats;
use hammer_stats::{
    Collector, DirectoryEntry, DirectoryIndex, NameVector, SimpleCounter, StatsMain,
};

use crate::DataPlaneMain;
use crate::error::RuntimeResult;
use crate::thread_main::ThreadMain;
use hammer_core::data_plane::NodeId;

/// One node's four cumulative counters: VPP `vlib_node_runtime_t`'s
/// `calls`/`vectors`/`clocks_since_last_overflow` plus a process node's
/// `n_suspends` (`node.h:226-233,482-503`, `main.c:1272`).
///
/// Only the thread that owns the row writes it (the dispatch point writes the
/// three dispatch counters, thread zero's suspend points write `suspends`) and
/// only the round reads it. The cells are therefore `Relaxed` atomics: the
/// published value is a number, not ownership, which is the same discipline
/// `/sys/main_loop_count_per_worker` and the Buffer Pool thread cache lengths
/// use. VPP writes plain 32-bit fields there because C needs no data-race
/// rule; Hammer keeps one row per thread and no shadow field.
#[repr(C)]
pub(crate) struct NodeCounters {
    clocks: AtomicU64,
    vectors: AtomicU64,
    calls: AtomicU64,
    suspends: AtomicU64,
}

impl NodeCounters {
    /// Builds one all-zero row entry, the value a fresh `vlib_node_runtime_t`
    /// starts with.
    const fn new() -> Self {
        Self {
            clocks: AtomicU64::new(0),
            vectors: AtomicU64::new(0),
            calls: AtomicU64::new(0),
            suspends: AtomicU64::new(0),
        }
    }

    /// VPP's `vlib_node_runtime_update_stats` accumulation (`main.c:546-577`):
    /// one dispatch is `calls += 1`, `vectors += n`, `clocks += t - last`.
    #[inline(always)]
    pub(crate) fn update_dispatch(&self, vectors: u64, clocks: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.vectors.fetch_add(vectors, Ordering::Relaxed);
        self.clocks.fetch_add(clocks, Ordering::Relaxed);
    }

    /// VPP's `p->n_suspends += 1` (`main.c:1272`), reached only from thread
    /// zero's process suspend points.
    #[inline]
    pub(crate) fn add_suspend(&self) {
        self.suspends.fetch_add(1, Ordering::Relaxed);
    }

    /// Reads one cell for the round; cells are independent, so the round
    /// publishes a per-cell snapshot rather than one instant.
    #[inline(always)]
    fn value(&self, counter: NodeCounter) -> u64 {
        match counter {
            NodeCounter::Clocks => self.clocks.load(Ordering::Relaxed),
            NodeCounter::Vectors => self.vectors.load(Ordering::Relaxed),
            NodeCounter::Calls => self.calls.load(Ordering::Relaxed),
            NodeCounter::Suspends => self.suspends.load(Ordering::Relaxed),
        }
    }
}

/// Every thread's row: VPP's "thread → `node_main.nodes`" table, which the
/// collector process walks each round.
///
/// The rows are allocated once with the graph's frozen node capacity before
/// any Worker is launched and live for the process, so the table only hands out
/// `'static` rows; it never grows after installation.
pub(crate) struct NodeCounterRows {
    rows: &'static [Box<[NodeCounters]>],
}

/// The published rows table.
///
/// This is the one startup-initialized process global of this family: the rows
/// must exist before any Worker runs, while the collectors that read them are
/// registered earlier, and the counters themselves stay in each thread's graph
/// (`NodeMain::node_counters`) with no handle of their own.
static NODE_COUNTER_ROWS: OnceLock<NodeCounterRows> = OnceLock::new();

impl NodeCounterRows {
    /// Allocates one row per thread at the frozen node capacity and publishes
    /// the table, the same fact `DataPlaneHandoff::with_node_capacity` freezes.
    pub(crate) fn install(thread_count: u32, node_capacity: usize) -> &'static Self {
        let mut rows = Vec::with_capacity(thread_count as usize);
        for _ in 0..thread_count {
            rows.push(
                (0..node_capacity)
                    .map(|_| NodeCounters::new())
                    .collect::<Box<[NodeCounters]>>(),
            );
        }
        let installed = NODE_COUNTER_ROWS.set(Self {
            rows: Box::leak(rows.into_boxed_slice()),
        });
        assert!(
            installed.is_ok(),
            "node counter rows are installed once, before any Worker launch"
        );
        NODE_COUNTER_ROWS
            .get()
            .expect("node counter rows were just installed")
    }

    /// The published table, read by the round.
    pub(crate) fn global() -> &'static Self {
        NODE_COUNTER_ROWS
            .get()
            .expect("node counter rows are installed before the collector process runs")
    }

    /// The row of `thread_index`; indices outside thread zero and the Data
    /// Workers have no node graph and therefore no row.
    pub(crate) fn row(&self, thread_index: u32) -> Option<&'static [NodeCounters]> {
        let rows: &'static [Box<[NodeCounters]>] = self.rows;
        rows.get(thread_index as usize).map(|row| &row[..])
    }
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
/// [`install_node_stats`] after the graph is materialized, which is why the
/// counter fields carry no `columns` argument.
#[derive(Stats)]
pub(crate) struct NodeStats {
    #[stats(path = "/sys/node/names")]
    names: NameVector,
    #[stats(path = "/sys/node/clocks")]
    clocks: SimpleCounter,
    #[stats(path = "/sys/node/vectors")]
    vectors: SimpleCounter,
    #[stats(path = "/sys/node/calls")]
    calls: SimpleCounter,
    #[stats(path = "/sys/node/suspends")]
    suspends: SimpleCounter,
}

/// Publishes one counter's cells for the round: VPP's
/// `stat_provider_node_counters_update_fn` row (`collector.c:105-123`).
///
/// One type serves all four counters; `counter` selects the cell to read and
/// `rows` is the table handed over at installation, so the round resolves
/// nothing and takes no lock.
struct NodeCounterCollector {
    entry_index: DirectoryIndex,
    counter: NodeCounter,
}

impl Collector for NodeCounterCollector {
    fn entry_index(&self) -> DirectoryIndex {
        self.entry_index
    }

    fn collect(&self, entry: &DirectoryEntry) {
        let rows = NodeCounterRows::global();
        for thread_index in 0..=ThreadMain::global().worker_count() {
            let counters = rows
                .row(thread_index)
                .expect("node counter rows cover thread zero and every Data Worker");
            for (slot, node_counters) in counters.iter().enumerate() {
                entry.set_simple_counter_cell(
                    thread_index,
                    u32::try_from(slot).expect("a node slot fits the published column width"),
                    node_counters.value(self.counter),
                );
            }
        }
    }
}

/// Creates the five entries and registers one collector per counter, like the
/// `if (sm->node_counters_enabled)` half of VPP's collector process startup
/// (`collector.c:154-176`).
///
/// The entries are created here rather than by the registration image so the
/// switch can decide their existence; the round also gets its collector rows
/// through this item's four registrations.
#[hammer_component_macros::stats_collect_registration]
fn register_node_stats(stats_main: &mut StatsMain) -> RuntimeResult<()> {
    if !stats_main.segment.node_counters_enabled() {
        return Ok(());
    }
    NodeStats::register_owner(stats_main)?;
    let node_stats = NodeStats::global();
    for counter in NodeCounter::ALL {
        stats_main.register_collector(NodeCounterCollector {
            entry_index: counter.entry_index(node_stats),
            counter,
        });
    }
    Ok(())
}

/// Publishes the graph facts (node count, node names) into `/sys/node/*` and
/// creates the `/nodes/<name>/<counter>` aliases.
///
/// VPP builds the entries in the collector process and validates the shape,
/// writes the name vector and adds the aliases on the first name diff of a
/// round (`collector.c:40-93`). Hammer freezes node identity before the Workers
/// launch, so this runs once in main-loop-enter, after the graph is
/// materialized and before the first round, on thread zero. It changes only
/// directory structure, so it needs no `WorkerBarrier`.
///
/// **Precondition:** a future "add or remove Data Workers" or "renumber nodes
/// at runtime" surface must republish this; the frozen node identity is what
/// makes one publication sufficient today.
#[hammer_component_macros::main_loop_enter_function]
fn install_node_stats(main: &mut DataPlaneMain) -> RuntimeResult<()> {
    let stats_main = StatsMain::global()?;
    if !stats_main.segment.node_counters_enabled() {
        return Ok(());
    }
    let node_stats = NodeStats::global();
    let columns = u32::try_from(main.nodes.node_count()).expect("node slots are u32-indexed");
    if columns == 0 {
        return Ok(());
    }
    // Rows are thread zero plus the Data Workers; no other runtime thread has a
    // node graph.
    let rows = ThreadMain::global().worker_count() + 1;
    for counter in NodeCounter::ALL {
        stats_main
            .segment
            .validate(counter.entry_index(node_stats), rows - 1, columns - 1)?;
    }
    for slot in 0..columns {
        let Some(name) = main.nodes.node_name(NodeId::new(slot))? else {
            continue;
        };
        stats_main
            .segment
            .set_name(node_stats.names.index, slot, name)?;
        for counter in NodeCounter::ALL {
            stats_main.segment.add_symlink(
                counter.entry_index(node_stats),
                slot,
                &format!("/nodes/{name}/{}", counter.name()),
            )?;
        }
    }
    Ok(())
}

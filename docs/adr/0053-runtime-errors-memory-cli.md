# ADR-0053: `runtime`, `errors`, `memory`, `buffer` CLI and clear commands

Status: implemented in source; compilation, tests and CI intentionally not run.

## Scope and VPP sources

The daemon owns `hammer/cli/{runtime,errors,memory,buffer}.rs`. Commands use
ADR-0049's existing `#[cli_command]`, parse a concrete Args structure, and
return an owned value implementing `Display`. Clear commands return unit.
There is no CLI-owned metrics store, sampling Node, or statistics queue.

The accepted counter design uses single-writer `Relaxed` atomic loads and
stores in the existing Node Runtime slots. The collector does not acquire a
worker barrier, use CAS/swap/fetch_add on Node counters, or clear a worker's
pending counters. Explicit runtime queries and clear commands retain the
worker barriers already specified by the CLI design and used by VPP.

| Vendored source under `third_party/vpp/` | Contract |
| --- | --- |
| `src/vlib/node.h`, `src/vlib/main.c:468-580` | Per-thread 32-bit pending calls/vectors/clocks; 64-bit totals and clear baselines; wrap detection; equal-clock max sample updates; explicit Node sync. |
| `src/vlib/node.c:667-725` | `vlib_node_get_nodes` duplicates arrays of Node pointers. With include_stats it synchronizes Nodes; the caller selects whether to use a barrier. |
| `src/vlib/stats/collector.c:38-128` | Changed-name bitmap; one segment lock covering removal of all old aliases before adding replacements; four per-thread `total - last_clear` writes. Numeric publication runs even when names did not change. |
| `src/vlib/stats/collector.c:132-185` | Collector Process startup, periodic updates, registered collector functions, heartbeat and update interval. VPP directly calls its Node routine before its collector table; Hammer registers the whole Node family in the existing collector table as requested. |
| `src/vlib/stats/stats.c:11-51,472-524`, `src/vlib/stats/format.c:10-23` | Segment structural lock/epoch, aligned vector validation, and replacement of slash by underscore in aliases. |
| `src/vlib/node_cli.c:333-660` | Named-node query on thread zero; all-node query copies stats under barrier and sorts/formats after release; clear synchronizes pending values and saves baselines. |
| `src/vlib/error.c:251-370` | Per-thread error counts minus clear baselines, verbose thresholds, wrapping totals and baseline-only clear. |
| `src/vlib/cli.c:772-914` | Heap and VM-map memory diagnostics. |
| `src/vlib/buffer.c:598-684`, `src/vlib/buffer.h:437-444` | Pool identity, NUMA, sizes, total/available/cached/used and optional per-thread cache counts. |
| `src/vlib/drop.c:252-265`, `src/vnet/interface.c` | Independent output/drop/punt/handoff Node flag bits for traffic accounting. |

## Node counter ownership, layout and methods

`NodeMain::inner.nodes` is an `Arc<Vec<NodeRuntimeSlot>>`. The array is the
actual graph's Node Runtime storage. `ThreadMain` holds shared ownership of
these same arrays for thread zero and the Data Workers. Auxiliary threads have
no row. There is one set of counters in each slot; no mirrored counter table,
Node-statistics DTO or additional per-worker stats authority exists.

Slots retain `#[repr(C, align(64))]`. Each scalar has its native atomic
alignment. Pending/max values are 32-bit and totals/baselines are 64-bit.
Dispatch updates the owner slot after restoring its runtime data. It uses
ordinary wrapping arithmetic between relaxed loads and stores. Overflow is
unlikely and synchronizes the owner's pending values into totals once.
`sync_node_stats` also transfers pending Next Frame vectors into the existing
per-arc totals. Refork preserves totals, baselines, max samples and arc totals
for existing identities; new/recycled identities start at zero. Refork also
retains published function, flags, frame layout and source-trace support when
it restores the worker's local runtime state.

The collector samples `total + pending - last_clear`, because it does not
reset another thread's pending values. The result may span an owner sync and
is approximate across fields. Only the owner, or the main thread while the
existing CLI barrier excludes the owner, writes that slot's counters. There
are no atomic read-modify-write operations on those counters.

The slot is crate-private. Its only collector operations expose immutable
Node role information and atomic counter values. Runtime data remains a
`Cell<Option<NodeRuntime>>` used exclusively by the NodeMain owner; collectors
never clone or format slot elements. A Node invocation takes only its function
and runtime data, so it does not clone the Arc or copy the counter fields.

```rust
#[repr(C, align(64))]
pub(crate) struct NodeRuntimeSlot {
    kind: NodeKind,
    flags: NodeFlags,
    process: NodeFunction,
    declared_process: NodeFunction,
    frame_args_size: (u16, u16, u16),
    runtime_data: Cell<Option<NodeRuntime>>,
    trace_supported: bool,
    calls_since_last_overflow: AtomicU32,
    vectors_since_last_overflow: AtomicU32,
    clocks_since_last_overflow: AtomicU32,
    max_clock: AtomicU32,
    max_clock_n: AtomicU32,
    total_calls: AtomicU64,
    total_vectors: AtomicU64,
    total_clocks: AtomicU64,
    last_clear_calls: AtomicU64,
    last_clear_vectors: AtomicU64,
    last_clear_clocks: AtomicU64,
}

// SAFETY: only the NodeMain owner accesses runtime_data. Collectors receive
// shared ownership of these same slots and read only atomic counters and
// immutable metadata. This crate-private type is cloned/debugged only by its
// owner or during graph publication, which excludes runtime_data writes.
// Metadata changes replace the array; collectors never clone its elements.
unsafe impl Sync for NodeRuntimeSlot {}

impl Clone for NodeRuntimeSlot {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            kind: self.kind,
            flags: self.flags,
            process: self.process,
            declared_process: self.declared_process,
            frame_args_size: self.frame_args_size,
            runtime_data: Cell::new(self.runtime_data.get()),
            trace_supported: self.trace_supported,
            calls_since_last_overflow: AtomicU32::new(
                self.calls_since_last_overflow.load(Ordering::Relaxed),
            ),
            vectors_since_last_overflow: AtomicU32::new(
                self.vectors_since_last_overflow.load(Ordering::Relaxed),
            ),
            clocks_since_last_overflow: AtomicU32::new(
                self.clocks_since_last_overflow.load(Ordering::Relaxed),
            ),
            max_clock: AtomicU32::new(self.max_clock.load(Ordering::Relaxed)),
            max_clock_n: AtomicU32::new(self.max_clock_n.load(Ordering::Relaxed)),
            total_calls: AtomicU64::new(self.total_calls.load(Ordering::Relaxed)),
            total_vectors: AtomicU64::new(self.total_vectors.load(Ordering::Relaxed)),
            total_clocks: AtomicU64::new(self.total_clocks.load(Ordering::Relaxed)),
            last_clear_calls: AtomicU64::new(self.last_clear_calls.load(Ordering::Relaxed)),
            last_clear_vectors: AtomicU64::new(self.last_clear_vectors.load(Ordering::Relaxed)),
            last_clear_clocks: AtomicU64::new(self.last_clear_clocks.load(Ordering::Relaxed)),
        }
    }
}

impl std::fmt::Debug for NodeRuntimeSlot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NodeRuntimeSlot")
            .field("kind", &self.kind)
            .field("runtime_data", &self.runtime_data)
            .finish_non_exhaustive()
    }
}

impl NodeRuntimeSlot {
    #[inline]
    fn sync_stats(&self, calls: u64, vectors: u64, clocks: u64) {
        self.total_calls.store(
            self.total_calls
                .load(Ordering::Relaxed)
                .wrapping_add(calls.wrapping_add(u64::from(
                    self.calls_since_last_overflow.load(Ordering::Relaxed),
                ))),
            Ordering::Relaxed,
        );
        self.total_vectors.store(
            self.total_vectors
                .load(Ordering::Relaxed)
                .wrapping_add(vectors.wrapping_add(u64::from(
                    self.vectors_since_last_overflow.load(Ordering::Relaxed),
                ))),
            Ordering::Relaxed,
        );
        self.total_clocks.store(
            self.total_clocks
                .load(Ordering::Relaxed)
                .wrapping_add(clocks.wrapping_add(u64::from(
                    self.clocks_since_last_overflow.load(Ordering::Relaxed),
                ))),
            Ordering::Relaxed,
        );
        self.calls_since_last_overflow.store(0, Ordering::Relaxed);
        self.vectors_since_last_overflow.store(0, Ordering::Relaxed);
        self.clocks_since_last_overflow.store(0, Ordering::Relaxed);
    }

    #[inline(always)]
    fn update_dispatch(&self, vectors: u64, clocks: u64) {
        let old_calls = self.calls_since_last_overflow.load(Ordering::Relaxed);
        let old_vectors = self.vectors_since_last_overflow.load(Ordering::Relaxed);
        let old_clocks = self.clocks_since_last_overflow.load(Ordering::Relaxed);
        let calls = old_calls.wrapping_add(1);
        let next_vectors = old_vectors.wrapping_add(vectors as u32);
        let next_clocks = old_clocks.wrapping_add(clocks as u32);
        self.calls_since_last_overflow
            .store(calls, Ordering::Relaxed);
        self.vectors_since_last_overflow
            .store(next_vectors, Ordering::Relaxed);
        self.clocks_since_last_overflow
            .store(next_clocks, Ordering::Relaxed);
        if self.max_clock.load(Ordering::Relaxed) <= clocks as u32 {
            self.max_clock.store(clocks as u32, Ordering::Relaxed);
            self.max_clock_n.store(vectors as u32, Ordering::Relaxed);
        }
        if crate::unlikely(
            calls < old_calls || next_vectors < old_vectors || next_clocks < old_clocks,
        ) {
            self.calls_since_last_overflow
                .store(old_calls, Ordering::Relaxed);
            self.vectors_since_last_overflow
                .store(old_vectors, Ordering::Relaxed);
            self.clocks_since_last_overflow
                .store(old_clocks, Ordering::Relaxed);
            self.sync_stats(1, vectors, clocks);
        }
    }

    #[inline]
    pub(crate) fn counter_values(&self) -> (u64, u64, u64) {
        (
            self.total_calls
                .load(Ordering::Relaxed)
                .wrapping_add(u64::from(
                    self.calls_since_last_overflow.load(Ordering::Relaxed),
                ))
                .wrapping_sub(self.last_clear_calls.load(Ordering::Relaxed)),
            self.total_vectors
                .load(Ordering::Relaxed)
                .wrapping_add(u64::from(
                    self.vectors_since_last_overflow.load(Ordering::Relaxed),
                ))
                .wrapping_sub(self.last_clear_vectors.load(Ordering::Relaxed)),
            self.total_clocks
                .load(Ordering::Relaxed)
                .wrapping_add(u64::from(
                    self.clocks_since_last_overflow.load(Ordering::Relaxed),
                ))
                .wrapping_sub(self.last_clear_clocks.load(Ordering::Relaxed)),
        )
    }

    pub(crate) fn is_process(&self) -> bool {
        self.kind == NodeKind::Process
    }
}
```

`ThreadMain::node_slots` returns shared ownership of the existing array rather
than a borrow of a running worker's `DataPlaneMain`. Its fixed array directory
is created before launch. Each thread updates its entry during startup/refork,
before the existing refork completion acknowledgement. Function selection is
completed before that acknowledgement. Main-thread collection is synchronous;
it cannot initiate another graph update while traversing the arrays.
Arc references are added at startup/refork and once per thread per collector
round, never once per Node dispatch.

The relevant fields and operations are:

```rust
// Existing owner fields, with unrelated fields omitted.
struct NodeRuntimeInner {
    nodes: Arc<Vec<NodeRuntimeSlot>>,
    // Existing topology, error columns and per-next totals.
}
struct ThreadMain {
    node_slots: UnsafeCell<Vec<UnsafeCell<Arc<Vec<NodeRuntimeSlot>>>>>,
    // Existing worker descriptors and DataPlaneMain ownership.
}
```

```rust
    pub(crate) fn initialize_node_slots(&self, nodes: Vec<Arc<Vec<crate::node::NodeRuntimeSlot>>>) {
        crate::ensure_main_thread().expect("node arrays initialize on thread zero before launch");
        assert_eq!(nodes.len(), self.worker_count as usize + 1);
        // SAFETY: initialization precedes launch and Process scheduling.
        let slots = unsafe { &mut *self.node_slots.get() };
        assert!(slots.is_empty(), "per-thread node arrays initialize once");
        *slots = nodes.into_iter().map(UnsafeCell::new).collect();
    }

    pub(crate) fn set_node_slots(
        &self,
        thread_index: u32,
        nodes: Arc<Vec<crate::node::NodeRuntimeSlot>>,
    ) {
        if thread_index == 0 {
            crate::ensure_main_thread().expect("thread zero owns its node array");
        } else {
            assert!(
                self.thread_by_index(thread_index)
                    .expect("node array belongs to a configured thread")
                    .is_current(),
                "only the owning thread replaces its node array"
            );
        }
        // SAFETY: the fixed collection exists before launch. Thread zero
        // replaces its own entry during synchronous graph publication. Each
        // worker replaces only its entry during startup/refork, before its
        // acknowledgement; collection runs on thread zero after that completes.
        let slots = unsafe { &*self.node_slots.get() };
        if slots.is_empty() {
            return;
        }
        let slot = &slots[thread_index as usize];
        unsafe { *slot.get() = nodes };
    }

    /// Shared ownership of one thread's existing Node Runtime array.
    /// No DataPlaneMain or worker-local protocol state is borrowed.
    pub(crate) fn node_slots(&self, thread_index: u32) -> Arc<Vec<crate::node::NodeRuntimeSlot>> {
        crate::ensure_main_thread().expect("the stats collector executes on thread zero");
        // SAFETY: thread zero cannot initiate a refork while this synchronous
        // lookup runs; prior reforks completed before barrier release returned.
        let slots = unsafe { &*self.node_slots.get() };
        Arc::clone(unsafe { &*slots[thread_index as usize].get() })
    }

```

`NodeMain::sync_node_stats(node)` validates the existing identity, invokes
the owner slot's `sync_stats(0, 0, 0)`, adds each pending Next Frame vector
count to `n_vectors_by_next_node`, and clears that pending value.
`DataPlaneMain::sync_node_stats` delegates to it.

`NodeMain::clear_runtime_stats` synchronizes all Nodes, stores their totals as
clear baselines and resets max_clock. For thread-zero Process Nodes it clears
the existing stats cells. `DataPlaneMain::clear_runtime_stats` also saves
internal-frame rate baselines, resets elapsed time and returns the Unix
timestamp used by `/sys/last_stats_clear`.

## Registered Node collector and segment guard

The existing `NodeStats` declaration installs exactly five entries when
`per_node_counters` is enabled. Its existing owner reference implements
`Collector`; no new collector carrier type is needed.
`register_node_stats` registers that reference. The periodic Process calls
`StatsMain::collect`; it has no Node-specific update call.

`update_node_counters` contains both the name batch and the numeric loop.
Name swaps remove all old symlinks before any new ones are added. Both passes
use one generic `StatsSegmentGuard`. The guard reuses `DirectoryState` and the
existing spin lock and directory epoch. Its transaction ends before unlock.
Numeric cell writes happen after the guard is dropped. Validation retains the
64-byte alignment of the outer counter vector and of every thread row.

The registered collector receives `&StatsSegment`. Each provider resolves
its entry when it writes. This prevents retaining a borrowed DirectoryEntry
while the Node collector grows the directory. Existing heap, worker-loop,
Buffer Pool and API-region collectors use the same revised method:

```rust
pub trait Collector: Send + Sync {
    fn entry_index(&self) -> DirectoryIndex;
    fn collect(&self, segment: &StatsSegment);
}

impl StatsMain {
    pub fn collect(&self) {
        for collector in &self.collectors {
            collector.collect(&self.segment);
        }
        self.segment.advance_heartbeat();
    }
}
```

```rust
#[must_use]
pub struct StatsSegmentGuard<'a> {
    segment: &'a StatsSegment,
    transaction: Option<DirectoryWrite>,
    directory: SpinLockGuard<'a, DirectoryState>,
}

impl StatsSegmentGuard<'_> {
    pub fn validate(&mut self, index: DirectoryIndex, row: u32, column: u32) -> StatsResult<()> {
        self.segment
            .validate_counter(index, row, column, &mut self.transaction)
    }

    pub fn set_name(
        &mut self,
        index: DirectoryIndex,
        element: u32,
        value: &str,
    ) -> StatsResult<()> {
        self.segment
            .set_name_element(index, element, value, &mut self.transaction)
    }

    pub fn add_symlink(
        &mut self,
        target: DirectoryIndex,
        column: u32,
        name: &str,
    ) -> StatsResult<DirectoryIndex> {
        let length = self.segment.directory().len();
        if target.raw() as usize >= length {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: target.raw(),
                length,
            });
        }
        self.segment.create_directory_entry(
            &mut self.directory,
            &mut self.transaction,
            name,
            DirectoryType::Symlink,
            DirectoryData::symlink_index(SymlinkIndex {
                entry_index: target.raw(),
                vector_index: column,
            }),
        )
    }

    pub fn remove_entry(&mut self, index: DirectoryIndex) -> StatsResult<()> {
        self.segment
            .remove_directory_entry(&mut self.directory, &mut self.transaction, index)
    }
}
```

`StatsSegment::lock` returns this guard. Standalone validate/set_name/add_symlink/
remove_entry delegate through it. `lock_refork` opens the same guard's epoch
for the existing graph-refork interval. No new Node-counter lock is added.
Name-vector growth publishes the new outer pointer even for its first
allocation; allocation failure frees the unpublished name. Counter-vector
growth preserves rows above the requested row index.

The complete Node-family declaration and implementation follows:

```rust
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
```

Process Nodes use the existing `Process<S,F>::poll` boundary: every poll adds
CPU ticks; Pending increments the existing thread-zero suspends cell, and
Ready increments calls. The CLI labels these values completions/pending polls/
poll clocks, without inferring a stackful suspension state. The collector
skips Process columns and cannot overwrite those owner-written cells.
Startup validates/resets the cells before polling; clear resets them.
With per-node counters disabled, Process metrics are omitted.

## Command registration, arguments and Display output

The daemon image registers `runtime`, `errors`, `memory`, `buffer`,
`clear runtime` and `clear errors`, plus the five trace commands migrated
from runtime into `hammer/cli/trace.rs`. The existing external client owns its
transport; this ADR adds no client implementation to the server repository.

`runtime` returns a RuntimeReport. Named queries inspect thread zero.
All-thread queries synchronize/copy during the existing barrier, release it,
then sort owned values and format them through Display. Brief is the default;
time, verbose, max and summary select the VPP output behavior. Process rows
use the measured Tokio meanings described above.

The synchronous CLI adapter invokes the handler inside `dispatch`, moves the
owned result into the existing spawned Future, and calls `Display` there after
the main borrow and barrier end. Unit results produce an empty String in that
Future. The handler returns no reference to main, a worker, or a stats entry.

```rust
// Synchronous non-unit handler adapter generated by #[cli_command].
let args: Args = input.parse()?;
let output = command(runtime, args)?;
Ok(hammer_runtime::__private::spawn_local(async move {
    Ok(format!("{output}"))
}))
```

Trace CLI keeps ADR-0051's borrowed-record semantics: `show trace` formats
into its owned String while the barrier protects the trace pool. Its command
registration and Args parsing live in the daemon; TraceMain, TraceHeader and
record formatting remain runtime mechanisms. `trace_main()` and
`trace_main_mut()` return direct borrows of the existing state. No trace pool,
clock authority or client is introduced by the move.

```rust
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use hammer_core::data_plane::{NodeId, NodeKind};
use hammer_runtime::cli::CliError;
use hammer_runtime::node::NodeFlags;
use hammer_runtime::{DataPlaneMain, DirectoryType, StatsMain, ThreadMain};

struct RuntimeArgs {
    node: Option<String>,
    verbose: bool,
    time: bool,
    max: bool,
    summary: bool,
}

struct ClearRuntimeArgs;

impl FromStr for ClearRuntimeArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.trim().is_empty() {
            return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            });
        }
        Ok(Self)
    }
}

impl FromStr for RuntimeArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut args = Self {
            node: None,
            verbose: false,
            time: false,
            max: false,
            summary: false,
        };
        let mut brief = false;
        for word in input.split_whitespace() {
            match word {
                "brief" | "b" if !brief && !args.verbose && args.node.is_none() => brief = true,
                "verbose" | "v" if !brief && !args.verbose && args.node.is_none() => {
                    args.verbose = true
                }
                "time" | "t" if !args.time && args.node.is_none() => args.time = true,
                "max" | "m" if !args.max && args.node.is_none() => args.max = true,
                "summary" | "sum" | "su" if !args.summary && args.node.is_none() => {
                    args.summary = true;
                }
                name if args.node.is_none()
                    && !brief
                    && !args.verbose
                    && !args.time
                    && !args.max
                    && !args.summary =>
                {
                    args.node = Some(name.to_owned());
                }
                _ => {
                    return Err(CliError::InvalidArgument {
                        argument: input.to_owned(),
                    });
                }
            }
        }
        Ok(args)
    }
}

struct RuntimeReport {
    args: RuntimeArgs,
    threads: Vec<(u32, &'static str, Option<u32>, f64, f64, f64)>,
    rows: Vec<(
        u32,
        &'static str,
        NodeKind,
        &'static str,
        NodeFlags,
        u64,
        u64,
        u64,
        u32,
        u32,
    )>,
    process_rows: Vec<(u32, &'static str, u64, u64, u64)>,
    seconds_per_clock: f64,
    update_interval: f64,
}

fn sync_thread_node_stats(main: &mut DataPlaneMain, report: &mut RuntimeReport) {
    let thread_index = main.thread_index();
    let descriptor = ThreadMain::global()
        .thread_by_index(thread_index)
        .expect("each DataPlaneMain has a WorkerThread descriptor");
    report.threads.push((
        thread_index,
        descriptor.name(),
        descriptor.cpu_index(),
        main.internal_node_vector_rate(),
        main.loops_per_second(),
        main.runtime_stats_elapsed_seconds(),
    ));

    for slot in 0..main.nodes().node_count() {
        let node = NodeId::new(slot as u32);
        main.sync_node_stats(node).expect("registered Node syncs");
        let nodes = main.nodes();
        let Some(name) = nodes.node_name(node).expect("registered Node slot") else {
            continue;
        };
        let kind = nodes.node_kind(node).expect("registered Node kind");
        if kind == NodeKind::Process {
            if thread_index != 0
                || !StatsMain::global()
                    .expect("stats is initialized")
                    .segment
                    .node_counters_enabled()
            {
                continue;
            }
            let (completions, _, clocks, pending, _, _) = nodes
                .node_stats(node)
                .expect("started Process has a validated stats column");
            report
                .process_rows
                .push((thread_index, name, completions, pending, clocks));
            continue;
        }
        let (calls, vectors, clocks, _, max_clock, max_clock_n) =
            nodes.node_stats(node).expect("registered Node has stats");
        report.rows.push((
            thread_index,
            name,
            kind,
            nodes
                .node_display_state(node)
                .expect("registered Node state"),
            nodes.node_flags(node).expect("registered Node flags"),
            calls,
            vectors,
            clocks,
            max_clock,
            max_clock_n,
        ));
    }
}

impl Display for RuntimeReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (thread_index, name, cpu, internal_rate, loops, elapsed) in &self.threads {
            if self.args.node.is_none() {
                if let Some(cpu) = cpu {
                    writeln!(f, "Thread {thread_index} {name} (lcore {cpu})")?;
                } else {
                    writeln!(f, "Thread {thread_index} {name}")?;
                }
                let mut traffic = (0_u64, 0_u64, 0_u64, 0_u64);
                for row in self.rows.iter().filter(|row| row.0 == *thread_index) {
                    let (_, _, kind, _, flags, _, vectors, ..) = row;
                    if *kind == NodeKind::Driver || flags.contains(NodeFlags::IS_HANDOFF) {
                        traffic.0 = traffic.0.wrapping_add(*vectors);
                    }
                    if flags.contains(NodeFlags::IS_OUTPUT) {
                        traffic.1 = traffic.1.wrapping_add(*vectors);
                    }
                    if flags.contains(NodeFlags::IS_DROP) {
                        traffic.2 = traffic.2.wrapping_add(*vectors);
                    }
                    if flags.contains(NodeFlags::IS_PUNT) {
                        traffic.3 = traffic.3.wrapping_add(*vectors);
                    }
                }
                let rate = |count| {
                    if *elapsed > 0.0 {
                        count as f64 / *elapsed
                    } else {
                        0.0
                    }
                };
                writeln!(
                    f,
                    "Time {elapsed:.1}, {:.6} sec internal node vector rate {internal_rate:.2} loops/sec {loops:.2}",
                    self.update_interval
                )?;
                writeln!(
                    f,
                    "  vector rates in {:.4e}, out {:.4e}, drop {:.4e}, punt {:.4e}",
                    rate(traffic.0),
                    rate(traffic.1),
                    rate(traffic.2),
                    rate(traffic.3)
                )?;
            }
            if self.args.summary {
                continue;
            }
            if self.args.max {
                writeln!(
                    f,
                    "{:<30}{:>17}{:>16}{:>16}{:>16}{:>16}",
                    "Name",
                    "Max Node Clocks",
                    "Vectors at Max",
                    "Max Clocks",
                    if self.args.time {
                        "Avg Time (ns)"
                    } else {
                        "Avg Clocks"
                    },
                    "Avg Vectors/Call"
                )?;
            } else {
                writeln!(
                    f,
                    "{:<30}{:>12}{:>16}{:>16}{:>16}{:>16}{:>16}",
                    "Name",
                    "State",
                    "Calls",
                    "Vectors",
                    "Suspends",
                    if self.args.time {
                        "Packet-Time"
                    } else {
                        "Packet-Clocks"
                    },
                    "Vectors/Call"
                )?;
            }
            for row in self.rows.iter().filter(|row| row.0 == *thread_index) {
                let (_, node_name, _, state, _, calls, vectors, clocks, max_clock, max_clock_n) =
                    row;
                if !self.args.verbose && *calls == 0 && self.args.node.is_none() {
                    continue;
                }
                let denominator = if *vectors != 0 { *vectors } else { *calls };
                let mut average = if denominator == 0 {
                    0.0
                } else {
                    *clocks as f64 / denominator as f64
                };
                if self.args.time {
                    average *= 1e9 * self.seconds_per_clock;
                }
                let vectors_per_call = if *calls == 0 {
                    0.0
                } else {
                    *vectors as f64 / *calls as f64
                };
                if self.args.max {
                    let per_vector = if *max_clock_n == 0 {
                        0.0
                    } else {
                        *max_clock as f64 / *max_clock_n as f64
                    };
                    writeln!(
                        f,
                        "{node_name:<30}{max_clock:>17}{max_clock_n:>16}{per_vector:>16.2}{average:>16.2}{vectors_per_call:>16.2}"
                    )?;
                } else {
                    writeln!(
                        f,
                        "{node_name:<30}{state:>12}{calls:>16}{vectors:>16}{:>16}{average:>16.2}{vectors_per_call:>16.2}",
                        0_u64
                    )?;
                }
            }
            if self.process_rows.iter().any(|row| row.0 == *thread_index) {
                writeln!(
                    f,
                    "{:<30}{:>16}{:>16}{:>18}",
                    "Process",
                    "Completions",
                    "Pending polls",
                    if self.args.time {
                        "Poll time (ns)"
                    } else {
                        "Poll clocks"
                    }
                )?;
                for (_, name, completions, pending, clocks) in self
                    .process_rows
                    .iter()
                    .filter(|row| row.0 == *thread_index)
                {
                    if !self.args.verbose
                        && *completions == 0
                        && *pending == 0
                        && self.args.node.is_none()
                    {
                        continue;
                    }
                    let measured = if self.args.time {
                        *clocks as f64 * self.seconds_per_clock * 1e9
                    } else {
                        *clocks as f64
                    };
                    writeln!(
                        f,
                        "{name:<30}{completions:>16}{pending:>16}{measured:>18.2}"
                    )?;
                }
            }
        }
        Ok(())
    }
}

#[hammer_component_macros::cli_command(
    path = "runtime",
    args = RuntimeArgs,
    short_help = "runtime [node|time|brief|verbose|max|summary]",
    mp_safe = true,
)]
fn runtime(main: &mut DataPlaneMain, args: RuntimeArgs) -> Result<RuntimeReport, CliError> {
    let mut report = RuntimeReport {
        args,
        threads: Vec::new(),
        rows: Vec::new(),
        process_rows: Vec::new(),
        seconds_per_clock: main.seconds_per_cpu_tick(),
        update_interval: StatsMain::global()
            .expect("stats is initialized")
            .segment
            .update_interval()
            .as_secs_f64(),
    };
    if let Some(name) = report.args.node.as_deref() {
        let node = main
            .nodes()
            .node_by_name(name)
            .ok_or_else(|| CliError::InvalidArgument {
                argument: name.to_owned(),
            })?;
        main.sync_node_stats(node).expect("named Node syncs");
        let descriptor = ThreadMain::global()
            .thread_by_index(0)
            .expect("thread-zero descriptor exists");
        report.threads.push((
            0,
            descriptor.name(),
            descriptor.cpu_index(),
            main.internal_node_vector_rate(),
            main.loops_per_second(),
            main.runtime_stats_elapsed_seconds(),
        ));
        let nodes = main.nodes();
        let node_name = nodes
            .node_name(node)
            .expect("named Node slot")
            .expect("named Node has a name");
        if nodes.node_kind(node).expect("named Node kind") == NodeKind::Process {
            if StatsMain::global()
                .expect("stats is initialized")
                .segment
                .node_counters_enabled()
            {
                let (completions, _, clocks, pending, _, _) = nodes
                    .node_stats(node)
                    .expect("started Process has a validated stats column");
                report
                    .process_rows
                    .push((0, node_name, completions, pending, clocks));
            }
        } else {
            let (calls, vectors, clocks, _, max_clock, max_clock_n) =
                nodes.node_stats(node).expect("named Node has stats");
            report.rows.push((
                0,
                node_name,
                nodes.node_kind(node).expect("named Node kind"),
                nodes.node_display_state(node).expect("named Node state"),
                nodes.node_flags(node).expect("named Node flags"),
                calls,
                vectors,
                clocks,
                max_clock,
                max_clock_n,
            ));
        }
        report.args.time = false;
        report.args.max = false;
        report.args.summary = false;
        return Ok(report);
    }
    hammer_runtime::worker_thread_barrier_sync!(main, {
        sync_thread_node_stats(main, &mut report);
        for thread_index in 1..=ThreadMain::global().worker_count() {
            let worker = ThreadMain::global()
                .thread_by_index(thread_index)
                .expect("Data Worker has a thread descriptor");
            // SAFETY: the barrier has stopped this worker; the mutable borrow
            // ends before release and cannot overlap the worker's own borrow.
            let owner = unsafe { ThreadMain::global().worker_main_at_barrier(worker) };
            sync_thread_node_stats(owner, &mut report);
        }
    });
    report
        .rows
        .sort_unstable_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    report
        .process_rows
        .sort_unstable_by(|left, right| (left.0, left.1).cmp(&(right.0, right.1)));
    Ok(report)
}

#[hammer_component_macros::cli_command(
    path = "clear runtime",
    args = ClearRuntimeArgs,
    short_help = "clear runtime",
    mp_safe = false,
)]
fn clear_runtime(main: &mut DataPlaneMain, _: ClearRuntimeArgs) -> Result<(), CliError> {
    let threads = ThreadMain::global();
    let timestamp = main.clear_runtime_stats();
    for worker in threads.data_workers() {
        // SAFETY: the CLI directory holds WorkerBarrier until this handler
        // returns, so the worker cannot borrow its DataPlaneMain here.
        unsafe { threads.worker_main_at_barrier(worker) }.clear_runtime_stats();
    }
    let segment = &StatsMain::global()
        .expect("stats is initialized before CLI dispatch")
        .segment;
    let entry = segment
        .find("/sys/last_stats_clear", DirectoryType::ScalarIndex)
        .expect("the system stats declaration installs last_stats_clear");
    segment.set_timestamp(entry, timestamp);
    Ok(())
}
```

`errors` and `clear errors` reuse `/node/errors`. Each DataPlaneMain owns
`error_counters_last_clear: Vec<u64>`. Counting remains a write to the current
thread's published error row. `node_error_count_since_clear` subtracts the
corresponding baseline, defaulting a new column's baseline to zero.
`clear_node_error_counters` copies the existing row into that vector without
zeroing live counters. The command's mp_safe=false declaration supplies the
existing barrier for the all-thread read/clear. All arithmetic wraps as u64.

```rust
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use hammer_core::data_plane::{NodeErrorIndex, NodeId};
use hammer_runtime::cli::CliError;
use hammer_runtime::node::{NodeErrorDescriptor, NodeErrorSeverity};
use hammer_runtime::{DataPlaneMain, ThreadMain};

struct ErrorsArgs {
    verbose: u32,
}

struct ClearErrorsArgs;

impl FromStr for ClearErrorsArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if !input.trim().is_empty() {
            return Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            });
        }
        Ok(Self)
    }
}

impl FromStr for ErrorsArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut words = input.split_whitespace();
        match words.next() {
            None => Ok(Self { verbose: 0 }),
            Some("verbose") => {
                let verbose = match words.next() {
                    Some(level) => level.parse().map_err(|_| CliError::InvalidArgument {
                        argument: input.to_owned(),
                    })?,
                    None => 1,
                };
                if words.next().is_some() {
                    return Err(CliError::InvalidArgument {
                        argument: input.to_owned(),
                    });
                }
                Ok(Self { verbose })
            }
            Some(_) => Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            }),
        }
    }
}

struct ErrorsReport {
    verbose: u32,
    threads: Vec<(u32, &'static str)>,
    rows: Vec<(
        u32,
        NodeId,
        &'static str,
        NodeErrorDescriptor,
        NodeErrorIndex,
        u64,
    )>,
    totals: Vec<(&'static str, NodeErrorDescriptor, NodeErrorIndex, u64)>,
}

impl Display for ErrorsReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.verbose == 0 {
            writeln!(
                f,
                "{:>10} {:<35} {:<35} {:<10}",
                "Count", "Node", "Reason", "Severity"
            )?;
        } else {
            writeln!(
                f,
                "{:>10} {:<35} {:<35} {:<10} {:>6}",
                "Count", "Node", "Reason", "Severity", "Index"
            )?;
        }
        for (thread, thread_name) in &self.threads {
            if self.verbose != 0 {
                writeln!(f, "Thread {thread} ({thread_name}):")?;
            }
            for (_, _, name, descriptor, index, count) in
                self.rows.iter().filter(|row| row.0 == *thread)
            {
                let severity = match descriptor.severity {
                    NodeErrorSeverity::Error => "error",
                    NodeErrorSeverity::Warn => "warn",
                    NodeErrorSeverity::Info => "info",
                };
                if self.verbose == 0 {
                    writeln!(
                        f,
                        "{count:>10} {name:<35} {:<35} {severity:<10}",
                        descriptor.description
                    )?;
                } else {
                    writeln!(
                        f,
                        "{count:>10} {name:<35} {:<35} {severity:<10} {:>6}",
                        descriptor.description,
                        index.get()
                    )?;
                }
            }
        }
        if self.verbose != 0 {
            writeln!(f, "Total:")?;
            for (name, descriptor, index, count) in &self.totals {
                writeln!(
                    f,
                    "{count:>10} {name:<40} {:<20} {:>10}",
                    descriptor.description,
                    index.get()
                )?;
            }
        }
        Ok(())
    }
}

/// VPP `show_errors`: read published per-thread error columns while the CLI
/// directory holds the Worker Barrier, then format the copied values.
#[hammer_component_macros::cli_command(
    path = "errors",
    args = ErrorsArgs,
    short_help = "errors [verbose [level]]",
    mp_safe = false,
)]
fn errors(main: &mut DataPlaneMain, args: ErrorsArgs) -> Result<ErrorsReport, CliError> {
    let nodes = main.nodes();
    let threads = ThreadMain::global();
    let mut report = ErrorsReport {
        verbose: args.verbose,
        threads: (0..=threads.worker_count())
            .map(|thread| {
                let descriptor = threads
                    .thread_by_index(thread)
                    .expect("each graph thread has a descriptor");
                (thread, descriptor.name())
            })
            .collect(),
        rows: Vec::new(),
        totals: Vec::new(),
    };
    let mut declared = false;
    for slot in 0..nodes.node_count() {
        if !nodes
            .node_error_descriptors(NodeId::new(slot as u32))
            .expect("registered Node has an error declaration")
            .is_empty()
        {
            declared = true;
            break;
        }
    }
    if !declared {
        return Ok(report);
    }
    for slot in 0..nodes.node_count() {
        let node = NodeId::new(slot as u32);
        let descriptors = nodes
            .node_error_descriptors(node)
            .expect("registered Node has an error declaration");
        if descriptors.is_empty() {
            continue;
        }
        let name = nodes
            .node_name(node)
            .expect("registered Node has a name entry")
            .expect("Node declaring errors is named");
        for (code, descriptor) in descriptors.iter().copied().enumerate() {
            let index = nodes
                .node_error_index(node, code as u16)
                .expect("declared error owns a column");
            let mut total = 0u64;
            for thread in 0..=threads.worker_count() {
                let count = if thread == 0 {
                    main.node_error_count_since_clear(index)
                } else {
                    let worker = threads
                        .thread_by_index(thread)
                        .expect("Data Worker has a thread descriptor");
                    // SAFETY: this non-MP-safe CLI holds WorkerBarrier until
                    // the handler returns; the worker is not borrowing its main.
                    unsafe { threads.worker_main_at_barrier(worker) }
                        .node_error_count_since_clear(index)
                };
                total = total.wrapping_add(count);
                if count != 0 || args.verbose >= 2 {
                    report
                        .rows
                        .push((thread, node, name, descriptor, index, count));
                }
            }
            if total != 0 && args.verbose != 0 {
                report.totals.push((name, descriptor, index, total));
            }
        }
    }
    report
        .rows
        .sort_unstable_by_key(|(thread, node, _, _, index, _)| (*thread, node.slot(), index.get()));
    Ok(report)
}

#[hammer_component_macros::cli_command(
    path = "clear errors",
    args = ClearErrorsArgs,
    short_help = "clear errors",
    mp_safe = false,
)]
fn clear_errors(main: &mut DataPlaneMain, _: ClearErrorsArgs) -> Result<(), CliError> {
    let threads = ThreadMain::global();
    main.clear_node_error_counters();
    for worker in threads.data_workers() {
        // SAFETY: this non-MP-safe CLI holds WorkerBarrier for the full pass.
        unsafe { threads.worker_main_at_barrier(worker) }.clear_node_error_counters();
    }
    Ok(())
}
```

`memory` copies heap and VM-map inventory through the existing owners.
API-region locking lasts only until copied name/range/usage is obtained.
The report owns its values before Display runs; region lock errors retain
their original source through the already-approved CliError category.

```rust
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use hammer_infra::mem::{HeapUsage, MemMain};
use hammer_ipc::binary_api::ApiMain;
use hammer_runtime::cli::CliError;
use hammer_runtime::{DataPlaneMain, StatsMain};

struct MemoryArgs {
    api_segment: bool,
    stats_segment: bool,
    main_heap: bool,
    map: bool,
    verbose: bool,
}

impl FromStr for MemoryArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let mut args = Self {
            api_segment: false,
            stats_segment: false,
            main_heap: false,
            map: false,
            verbose: false,
        };
        for word in input.split_whitespace() {
            let selected = match word {
                "api-segment" => &mut args.api_segment,
                "stats-segment" => &mut args.stats_segment,
                "main-heap" => &mut args.main_heap,
                "map" => &mut args.map,
                "verbose" => &mut args.verbose,
                _ => {
                    return Err(CliError::InvalidArgument {
                        argument: input.to_owned(),
                    });
                }
            };
            if *selected {
                return Err(CliError::InvalidArgument {
                    argument: input.to_owned(),
                });
            }
            *selected = true;
        }
        Ok(args)
    }
}

struct MemoryReport {
    verbose: bool,
    show_map: bool,
    heaps: Vec<(String, usize, usize, HeapUsage)>,
    mappings: Vec<(
        usize,
        usize,
        i32,
        u8,
        usize,
        Vec<(u32, u64)>,
        u64,
        u64,
        String,
    )>,
}

impl Display for MemoryReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for (name, base, size, usage) in &self.heaps {
            writeln!(f, "base {base:#x}, size {size}, name '{name}'")?;
            writeln!(
                f,
                "  total: {}, used: {}, free: {}, trimmable: {}",
                usage.total_bytes, usage.used_bytes, usage.free_bytes, usage.releasable_bytes
            )?;
            if self.verbose {
                writeln!(
                    f,
                    "  free chunks {}, max allocated {}",
                    usage.free_chunk_count, usage.max_allocated_bytes
                )?;
            }
            writeln!(f)?;
        }
        if self.show_map {
            writeln!(
                f,
                "StartAddr        size   FD  PageSz  Pages  NotPop  Unknown Name"
            )?;
            for (base, size, fd, page_log2, pages, per_numa, not_populated, unknown, name) in
                &self.mappings
            {
                write!(
                    f,
                    "{base:016x} {size:>7} {fd:>4} {:>7} {pages:>6} {not_populated:>7} {unknown:>8} {name}",
                    1usize << *page_log2
                )?;
                for (node, count) in per_numa {
                    write!(f, " Numa{node}:{count}")?;
                }
                writeln!(f)?;
            }
        }
        Ok(())
    }
}

/// VPP `show_memory_usage`: select the current owner heap or VM map and copy
/// its reading before the CLI command returns a formatter result.
#[hammer_component_macros::cli_command(
    path = "memory",
    args = MemoryArgs,
    short_help = "memory [api-segment] [stats-segment] [main-heap] [map] [verbose]",
    mp_safe = false,
)]
fn memory(_: &mut DataPlaneMain, args: MemoryArgs) -> Result<MemoryReport, CliError> {
    let mut report = MemoryReport {
        verbose: args.verbose,
        show_map: args.map,
        heaps: Vec::new(),
        mappings: Vec::new(),
    };
    if !args.api_segment && !args.stats_segment && !args.main_heap && !args.map {
        report.heaps = MemMain::heap_usages();
        report.verbose = true;
        return Ok(report);
    }
    if args.api_segment {
        report
            .heaps
            .push(ApiMain::current().api_segment_heap_usage());
    }
    if args.stats_segment {
        let heap = StatsMain::global()
            .expect("CLI starts after stats initialization")
            .segment
            .heap();
        report.heaps.push((
            heap.name().to_owned(),
            heap.base().as_ptr() as usize,
            heap.size(),
            heap.usage(),
        ));
    }
    if args.main_heap {
        let heap = MemMain::main_heap();
        report.heaps.push((
            heap.name().to_owned(),
            heap.base().as_ptr() as usize,
            heap.size(),
            heap.usage(),
        ));
    }
    if args.map {
        report.mappings =
            MemMain::mappings().map_err(|source| CliError::MemoryMapping { source })?;
    }
    Ok(report)
}
```

`buffer` reads the process-global BufferMain and is mp_safe=true.
Existing `pool_usage` supplies total/available/cached/used.
`pool_properties` supplies NUMA and header-plus-data/data sizes;
`pool_cached_count` reads each existing thread cache's count for detail.
There is no worker barrier and no Buffer Pool wrapper or second counter store.

```rust
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use hammer_core::buffer::{BufferMain, BufferPoolUsage};
use hammer_runtime::DataPlaneMain;
use hammer_runtime::ThreadMain;
use hammer_runtime::cli::CliError;

struct BufferArgs {
    detail: bool,
}

impl FromStr for BufferArgs {
    type Err = CliError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.trim() {
            "" => Ok(Self { detail: false }),
            "detail" => Ok(Self { detail: true }),
            _ => Err(CliError::InvalidArgument {
                argument: input.to_owned(),
            }),
        }
    }
}

struct BufferReport {
    detail: bool,
    rows: Vec<(u8, String, u32, usize, usize, BufferPoolUsage, Vec<u32>)>,
}

impl Display for BufferReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{:<20}{:>6}{:>6}{:>6}{:>11}{:>6}{:>8}{:>8}{:>8}",
            "Pool Name", "Index", "NUMA", "Size", "Data Size", "Total", "Avail", "Cached", "Used"
        )?;
        for (index, name, numa, size, data_size, usage, per_thread) in &self.rows {
            writeln!(
                f,
                "{name:<20}{index:>6}{numa:>6}{size:>6}{data_size:>11}{:>6}{:>8}{:>8}{:>8}",
                usage.buffer_count,
                usage.available,
                usage.cached,
                usage.used(),
            )?;
            if self.detail {
                for (thread, cached) in per_thread.iter().enumerate() {
                    writeln!(f, "{:>20}{thread:>6}{:>37}{cached:>8}", "thread", "")?;
                }
            }
        }
        Ok(())
    }
}

#[hammer_component_macros::cli_command(
    path = "buffer",
    args = BufferArgs,
    short_help = "buffer [detail]",
    mp_safe = true,
)]
fn buffer(_: &mut DataPlaneMain, args: BufferArgs) -> Result<BufferReport, CliError> {
    let main = BufferMain::global();
    let thread_count = ThreadMain::global().worker_count() + 1;
    let mut report = BufferReport {
        detail: args.detail,
        rows: Vec::with_capacity(main.pool_count()),
    };
    for pool_index in 0..main.pool_count() {
        let index = u8::try_from(pool_index).expect("Pool indices fit u8");
        let (numa, size, data_size) = main.pool_properties(index);
        let usage = main.pool_usage(index);
        let per_thread = if args.detail {
            (0..thread_count)
                .map(|thread| main.pool_cached_count(index, thread))
                .collect()
        } else {
            Vec::new()
        };
        report.rows.push((
            index,
            main.pool_name(index).to_owned(),
            numa,
            size,
            data_size,
            usage,
            per_thread,
        ));
    }
    Ok(report)
}
```

## Layer boundaries and error semantics

- Runtime owns Node accounting, graph/refork lifetime, Process polling and
  per-main clear baselines. Packet Nodes declare accounting bits at registration.
- Stats owns the segment heap, directory guard, entry identities, aligned
  vectors and collector registration. Numeric writes use existing published
  cells and do not allocate.
- Core owns Buffer Main Pool facts; infra owns heap/map inventory.
- Hammer owns Args parsing and Display reports. Formatting happens after
  worker borrows, barriers and region guards end. Trace display retains
  ADR-0051's formatting inside its handler while borrowing pool records.

Malformed command arguments use existing CliError::InvalidArgument before
sampling. Existing identity/shape violations are owner bugs and assert with
the affected identity. A failed structural collector batch terminates its
Process scope instead of returning success with partial aliases.
Allocation/mapping errors retain their existing owner error category.
No new packet error family or general catch-all error is introduced.

## Migration inventory and verification

| Surface | Replacement / retained behavior |
| --- | --- |
| Old NodeCounterRows/NodeCounters global mirror | Removed; pending/total/baseline/max stay in the existing per-thread NodeRuntimeSlot. |
| update_node_names / publish_node_stats | Removed; registered update_node_counters updates metadata and numeric vectors in one round. |
| Sampling driver Node, counter queues, ProcessStats, TrafficRole | Absent. |
| StatsMain collector trait and providers | Receive StatsSegment; resolve entries after structural mutation. |
| Graph refork | Share actual arrays; refresh each owner's array reference before the existing completion acknowledgement. |
| Runtime commands | Existing stopped-worker borrowing is general runtime access; copy under barrier, Display afterward. |
| CLI adapter | Move owned handler results into the Future; invoke Display after dispatch releases its main borrow/barrier. |
| Trace CLI | Five registrations and command handlers move to hammer/cli/trace.rs; runtime retains trace storage and record formatting. |
| Error commands | Per-main clear baseline; wrapping difference and wrapping per-error totals. |
| Buffer command | Existing Pool usage and narrow Pool property/cache reads. |
| Memory command | Existing inventory and copied API-region heap usage. |

The Node update/sync hot paths retain VPP's inline intent. Overflow uses the
existing unlikely hint. Collector/CLI formatting remains cold. A 64-byte
aligned slot and counter rows isolate per-thread stores; relaxed sampling
does not promise a cross-field snapshot.

Source review checks declarations, registrations, all callers, startup,
refork, clear baselines, alias swaps and absence of removed symbols.
Rustfmt and git diff --check are permitted source checks.
No compilation, tests, CI or daemon launch are performed under the current
instruction. Runtime correctness and performance measurements remain
unverified; source completion is not a performance claim.

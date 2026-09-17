//! VPP-shaped stats segment ownership and its control-plane surface.
//!
//! The process has one [`StatsMain`], which owns the shared segment mapping,
//! its heap and the directory that names every metric. Metric owners register
//! through the [`StatsSegment`] operations; the runtime owns the listener that
//! hands the segment descriptor to readers.

use std::fmt;
use std::io;
use std::sync::OnceLock;
use std::time::Duration;

use hammer_infra::mem::{MemError, PageSize};

pub mod mem;
mod metric;
mod protocol;
mod segment;

pub use metric::{
    CombinedCounter, Gauge, Histogram, NameVector, Ring, RingSchema, SimpleCounter, Timestamp,
};
pub use protocol::{
    Counter, DirectoryEntry, DirectoryIndex, DirectoryType, ProtocolError, RingBufferHeader,
    RingConfig, STAT_COUNTER_BOOTTIME, STAT_COUNTER_HEARTBEAT, STAT_COUNTER_LAST_STATS_CLEAR,
    SharedHeader, ring_layout,
};
pub use segment::StatsSegment;

/// One collector, the Rust form of one row of VPP's collector table.
///
/// VPP's row keeps the update logic and the provider's own state in two cells
/// (`vlib_stats_collector_t { fn, entry_index, vector_index, private_data }`,
/// `stats.h:51-57`); a concrete type implementing this trait carries both: the
/// method body is the `fn` cell, and the type's own fields are the
/// `private_data` cell (VPP stores a heap or pool index there, Hammer stores
/// the provider's own reference or index). Field names state the domain fact
/// (`heap`, `threads`, `api_main`), because `private_data` is only the cell
/// name every provider interprets for itself.
///
/// This is the repository's approved dynamic-dispatch exception: the table is a
/// homogeneous `Vec<Box<dyn Collector>>`, dispatch happens a few times per stats
/// round and never on the packet path, and the mechanism stays ignorant of every
/// family type.
/// The table lives in the process-global [`StatsMain`] and is read by the
/// stats round, so every collector must be shareable between threads; the
/// instances themselves only hold process-lifetime references and indices.
pub trait Collector: Send + Sync {
    /// The directory entry this row fills (VPP's `entry_index` cell).
    fn entry_index(&self) -> DirectoryIndex;

    /// The round's one call, the Rust form of VPP's `c->fn (&data)`
    /// (`collector.c:137-146`).
    ///
    /// The argument is the shared borrow of that entry, matching VPP's
    /// `data.entry = sm->directory_vector + c->entry_index`: a collector writes
    /// only its own cells, never the header, a segment lock or a global lookup.
    fn collect(&self, entry: &DirectoryEntry);
}

/// The single process stats owner, corresponding to VPP's `vlib_stats_main_t`.
pub struct StatsMain {
    /// The statistics segment.
    pub segment: StatsSegment,
    /// Collectors called once per round, in registration order.
    ///
    /// The table is process-private and never enters the shared segment (VPP
    /// keeps `sm->collectors` the same way). It grows only during startup, when
    /// the owner is unpublished and the round cannot run, so it needs no lock.
    collectors: Vec<Box<dyn Collector>>,
}

static STATS_MAIN: OnceLock<StatsMain> = OnceLock::new();

impl StatsMain {
    /// Creates the stats segment and its owner without publishing it.
    ///
    /// Startup order is `create` → fixed slots → registration image →
    /// [`StatsMain::publish`] → listener, like `vlib_stats_init`: registrations
    /// need `&mut StatsMain`, so they run before the owner is visible to rounds
    /// and readers.
    pub fn create(
        name: &str,
        size: usize,
        page_size: PageSize,
        update_interval: Duration,
        node_counters_enabled: bool,
    ) -> StatsResult<Self> {
        if STATS_MAIN.get().is_some() {
            return Err(StatsError::AlreadyInitialized);
        }
        Ok(Self {
            segment: StatsSegment::create(
                name,
                size,
                page_size,
                update_interval,
                node_counters_enabled,
            )?,
            collectors: Vec::new(),
        })
    }

    /// Publishes the owner as the process stats authority.
    ///
    /// A concurrent second owner is a startup programming error: the raced-in
    /// owner stays published and the caller ends the process instead of
    /// continuing with two owners.
    pub fn publish(self) -> StatsResult<()> {
        STATS_MAIN
            .set(self)
            .map_err(|_| StatsError::AlreadyInitialized)
    }

    pub fn global() -> StatsResult<&'static Self> {
        STATS_MAIN.get().ok_or(StatsError::NotInitialized)
    }

    /// Registers one collector, like `vlib_stats_register_collector_fn`.
    ///
    /// Only startup calls this: the table grows once, before the
    /// `statseg-collector-process` node and the Data Workers run, and rounds
    /// only read it. There is no recoverable failure, so the operation returns
    /// `()`; the table is ordinary Main Heap growth and an allocation failure
    /// ends the process through the allocator's failure path. `+ 'static` says
    /// the registration lives until the process exits, not that the state is
    /// erased into a pointer.
    pub fn register_collector(&mut self, collector: impl Collector + 'static) {
        self.collectors.push(Box::new(collector));
    }

    /// Runs one collector round, like `do_stat_segment_updates`.
    ///
    /// Every registered collector runs once in registration order, then the
    /// heartbeat advances. Neither the table nor the segment is locked: writes
    /// land on published cells of the shared directory, exactly like VPP's
    /// loop. A collector that names a missing entry is a declaration/write
    /// mismatch, which asserts; the round has no recoverable failure and no
    /// family names.
    pub fn collect(&self) {
        for collector in &self.collectors {
            // VPP: `data.entry = sm->directory_vector + c->entry_index`
            // (`collector.c:139`): the mechanism resolves the entry and hands it
            // to the collector.
            let entry = self
                .segment
                .entry(collector.entry_index())
                .expect("a registered collector names a live directory entry");
            collector.collect(entry);
        }
        // The last statement takes the heartbeat entry and writes its value + 1,
        // the same "take entry → write value" shape as every collector above
        // (`collector.c:149-150`).
        self.segment.advance_heartbeat();
    }
}

pub type StatsResult<T> = Result<T, StatsError>;

/// Failure categories of the stats segment control plane.
///
/// Each variant names the authority that failed and carries the facts a caller
/// needs to react; transient per-packet outcomes are node errors, not these.
#[derive(Debug)]
pub enum StatsError {
    /// A shared name, vector prefix, ring layout or directory value is
    /// malformed; the source names the exact protocol category.
    Protocol { source: ProtocolError },
    /// A `MemMain` mapping or `MemHeap` operation failed.
    Memory { source: MemError },
    /// The shared backing file could not be sized.
    BackingResize { size: usize, source: io::Error },
    /// The configured segment size cannot hold the header page and directory.
    CapacityTooSmall { requested: usize, minimum: usize },
    /// The directory index does not name a slot of the current directory.
    DirectoryIndexOutOfBounds { index: u32, length: usize },
    /// A metric name is already owned by another directory entry.
    DuplicateName { name: String },
    /// No directory entry owns the requested metric name.
    MetricNotFound { name: String },
    /// The directory entry exists but holds another metric family.
    MetricTypeMismatch {
        expected: DirectoryType,
        actual: DirectoryType,
    },
    /// The entry does not carry the vector shape the operation requires.
    InvalidShape,
    /// The ring declaration disagrees with its typed schema.
    InvalidRingSchema { expected: usize, actual: usize },
    /// The stats segment is already published.
    AlreadyInitialized,
    /// The stats segment is not published yet.
    NotInitialized,
}

impl fmt::Display for StatsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol { source } => write!(formatter, "stats protocol error: {source}"),
            Self::Memory { source } => write!(formatter, "stats memory error: {source}"),
            Self::BackingResize { size, source } => {
                write!(
                    formatter,
                    "failed to size stats backing to {size} bytes: {source}"
                )
            }
            Self::CapacityTooSmall { requested, minimum } => write!(
                formatter,
                "stats segment size {requested} is below minimum {minimum}"
            ),
            Self::DirectoryIndexOutOfBounds { index, length } => write!(
                formatter,
                "stats directory index {index} is outside length {length}"
            ),
            Self::DuplicateName { name } => {
                write!(formatter, "stats metric `{name}` is already registered")
            }
            Self::MetricNotFound { name } => {
                write!(formatter, "stats metric `{name}` is not registered")
            }
            Self::MetricTypeMismatch { expected, actual } => write!(
                formatter,
                "stats metric has type `{}`, expected `{}`",
                <&str>::from(*actual),
                <&str>::from(*expected)
            ),
            Self::InvalidShape => formatter.write_str("stats metric has an unexpected shape"),
            Self::InvalidRingSchema { expected, actual } => write!(
                formatter,
                "invalid stats ring schema: expected {expected}, got {actual}"
            ),
            Self::AlreadyInitialized => formatter.write_str("stats main is already initialized"),
            Self::NotInitialized => formatter.write_str("stats main is not initialized"),
        }
    }
}

impl std::error::Error for StatsError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Protocol { source } => Some(source),
            Self::Memory { source } => Some(source),
            Self::BackingResize { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<ProtocolError> for StatsError {
    fn from(source: ProtocolError) -> Self {
        Self::Protocol { source }
    }
}

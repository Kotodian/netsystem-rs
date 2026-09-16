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
use hammer_infra::sync::SpinLock;

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

/// One collector registration, corresponding to VPP's
/// `vlib_stats_collector_reg_t`.
///
/// The owner states which entry (and which row of it) its function fills; the
/// mechanism does not interpret either value.
#[derive(Clone, Copy)]
pub struct CollectorRegistration {
    /// Called once per round; the second argument is the row inside the entry.
    pub collect: fn(DirectoryIndex, u32),
    /// The directory entry this collector fills.
    pub entry_index: DirectoryIndex,
    /// The row of that entry this collector fills.
    pub vector_index: u32,
}

/// One row of the process collector table, corresponding to VPP's
/// `vlib_stats_collector_t`. The table is private to the process and never
/// enters the shared segment.
#[derive(Clone, Copy)]
struct Collector {
    collect: fn(DirectoryIndex, u32),
    entry_index: DirectoryIndex,
    vector_index: u32,
}

/// The single process stats owner, corresponding to VPP's `vlib_stats_main_t`.
pub struct StatsMain {
    /// The statistics segment, protected by one structural lock.
    pub segment: SpinLock<StatsSegment>,
    /// Collectors called once per round, in registration order.
    collectors: SpinLock<Vec<Collector>>,
}

static STATS_MAIN: OnceLock<StatsMain> = OnceLock::new();

impl StatsMain {
    /// Creates the stats segment and publishes it as the process owner.
    pub fn init(
        name: &str,
        size: usize,
        page_size: PageSize,
        update_interval: Duration,
        node_counters_enabled: bool,
    ) -> StatsResult<()> {
        if STATS_MAIN.get().is_some() {
            return Err(StatsError::AlreadyInitialized);
        }
        let segment = StatsSegment::create(
            name,
            size,
            page_size,
            update_interval,
            node_counters_enabled,
        )?;
        if STATS_MAIN
            .set(Self {
                segment: SpinLock::new(segment),
                collectors: SpinLock::new(Vec::new()),
            })
            .is_err()
        {
            // A concurrent second initialization is a startup programming
            // error: the raced-in owner stays published and the caller ends the
            // process instead of continuing with two owners.
            return Err(StatsError::AlreadyInitialized);
        }
        Ok(())
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
    /// ends the process through the allocator's failure path.
    pub fn register_collector(&self, registration: CollectorRegistration) {
        self.collectors.lock().push(Collector {
            collect: registration.collect,
            entry_index: registration.entry_index,
            vector_index: registration.vector_index,
        });
    }

    /// Runs one collector round, like `do_stat_segment_updates`.
    ///
    /// Every registered collector runs once in registration order, then the
    /// heartbeat advances. A collector takes the segment lock itself: the table
    /// lock is released before the call, so the two locks never nest. The round
    /// has no recoverable failure, matching the VPP loop.
    pub fn collect(&self) {
        let mut position = 0;
        loop {
            let collector = {
                let collectors = self.collectors.lock();
                match collectors.get(position) {
                    Some(collector) => *collector,
                    None => break,
                }
            };
            (collector.collect)(collector.entry_index, collector.vector_index);
            position += 1;
        }
        self.segment.lock().advance_heartbeat();
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

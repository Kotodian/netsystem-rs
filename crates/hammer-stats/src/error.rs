//! Typed errors for the stats segment.

use std::error::Error;
use std::fmt;

use crate::directory::{DirectoryType, PrometheusType};
use crate::stats::EntryId;

/// Errors produced by [`crate::StatsMain`] and its metrics.
#[derive(Debug)]
pub enum StatsError {
    /// An underlying shared-segment I/O error (creation, mapping, claim).
    Io(std::io::Error),
    /// The requested segment capacity cannot hold the reserved first page,
    /// the shared header, the initial directory, and one metric.
    CapacityTooSmall {
        /// Smallest accepted capacity, in bytes.
        minimum: usize,
        /// The rejected request, in bytes.
        requested: usize,
    },
    /// The segment arena has no room for another allocation.
    SegmentFull,
    /// A conversion rejected an entry id pair: generation 0 is never
    /// published, so it cannot name a real entry.
    InvalidEntryId { index: u32, generation: u64 },
    /// The metric path is not a valid NUL-terminated directory name of at
    /// most 127 bytes.
    InvalidPath(String),
    /// The Prometheus descriptor cannot be represented in a metric block
    /// (bad fq name or help, variable labels, or size bounds).
    InvalidDescriptor(String),
    /// A metric with this directory name already exists.
    DuplicateName(String),
    /// No active entry exists with this id.
    NotFound { id: EntryId },
    /// The entry exists but its generation no longer matches the id's:
    /// the metric was removed and its slot possibly reused.
    StaleEntry { id: EntryId },
    /// An offset or span exceeded the mapped segment.
    OutOfBounds,
    /// An offset did not satisfy its required alignment.
    Misaligned,
    /// A mapped byte pattern violated an encoding invariant.
    InvalidState(u8),
    /// A size/align pair could not be laid out.
    InvalidLayout,
    /// The entry generation would wrap `u64::MAX`.
    GenerationOverflow,
    /// A `list` pattern could not be compiled as a regular expression.
    InvalidPattern {
        /// The exact pattern that failed to compile.
        pattern: String,
        /// The regex engine's error, exposed via [`Error::source`].
        source: regex::Error,
    },
    /// A reader retried the maximum number of times while the segment was
    /// continuously being republished.
    ReadBusy,
    /// A registration layout has not yet been implemented by the public
    /// protocol-neutral boundary.
    UnsupportedLayout,
    /// A directory entry carries a type combination that cannot be decoded
    /// into a `DumpValue` (internal corruption). The typed enums are already
    /// checked decodes of the mapped bytes.
    IncompatibleType {
        /// Id of the offending entry.
        id: EntryId,
        /// The decoded Prometheus kind.
        prometheus_type: PrometheusType,
        /// The decoded directory type.
        directory_type: DirectoryType,
    },
}

impl fmt::Display for StatsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StatsError::Io(error) => write!(f, "stats segment I/O error: {error}"),
            StatsError::CapacityTooSmall { minimum, requested } => write!(
                f,
                "stats segment capacity {requested} is below the minimum {minimum} bytes"
            ),
            StatsError::SegmentFull => write!(f, "stats segment arena is exhausted"),
            StatsError::InvalidEntryId { index, generation } => write!(
                f,
                "invalid stats entry id (index {index}, generation {generation}): \
                 generation must be non-zero"
            ),
            StatsError::InvalidPath(path) => {
                write!(
                    f,
                    "invalid stats path {path:?}: must be 1..=127 bytes without NUL"
                )
            }
            StatsError::InvalidDescriptor(detail) => {
                write!(f, "invalid Prometheus descriptor: {detail}")
            }
            StatsError::DuplicateName(name) => write!(f, "duplicate stats metric name {name:?}"),
            StatsError::NotFound { id } => write!(f, "no stats entry with {id}"),
            StatsError::StaleEntry { id } => {
                write!(
                    f,
                    "stats entry {id} has a newer generation; object is stale"
                )
            }
            StatsError::OutOfBounds => write!(f, "offset or span outside the stats segment"),
            StatsError::Misaligned => write!(f, "offset not aligned for its record"),
            StatsError::InvalidState(state) => write!(f, "invalid mapped state byte 0x{state:02x}"),
            StatsError::InvalidLayout => write!(f, "invalid size/alignment layout"),
            StatsError::GenerationOverflow => write!(f, "metric generation overflow"),
            StatsError::InvalidPattern { pattern, source } => {
                write!(f, "invalid stats list pattern {pattern:?}: {source}")
            }
            StatsError::ReadBusy => {
                write!(f, "stats segment busy republishing; retry the read later")
            }
            StatsError::UnsupportedLayout => {
                write!(f, "stats registration layout is not implemented")
            }
            StatsError::IncompatibleType {
                id,
                prometheus_type,
                directory_type,
            } => write!(
                f,
                "stats entry {id} mixes Prometheus type {prometheus_type:?} \
                 with directory type {directory_type:?}"
            ),
        }
    }
}

impl Error for StatsError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            StatsError::Io(error) => Some(error),
            StatsError::InvalidPattern { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StatsError {
    fn from(error: std::io::Error) -> StatsError {
        StatsError::Io(error)
    }
}

impl From<prometheus::Error> for StatsError {
    fn from(error: prometheus::Error) -> StatsError {
        StatsError::InvalidDescriptor(error.to_string())
    }
}

impl From<hammer_infra::segment::SegmentAllocationError> for StatsError {
    fn from(error: hammer_infra::segment::SegmentAllocationError) -> StatsError {
        use hammer_infra::segment::SegmentAllocationError;
        match error {
            SegmentAllocationError::EmptyLayout => StatsError::InvalidLayout,
            SegmentAllocationError::Exhausted => StatsError::SegmentFull,
            SegmentAllocationError::OutOfBounds => StatsError::OutOfBounds,
            SegmentAllocationError::Misaligned => StatsError::Misaligned,
        }
    }
}

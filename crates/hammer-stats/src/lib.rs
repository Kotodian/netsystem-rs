use std::fmt;
use std::io;

use hammer_infra::segment::SegmentAllocationError;

mod protocol;
mod segment;

pub(crate) type StatsResult<T> = Result<T, StatsError>;

#[derive(Debug)]
pub(crate) enum StatsError {
    Protocol(protocol::Error),
    Io(io::Error),
    Allocation(SegmentAllocationError),
    CapacityTooSmall { requested: usize, minimum: usize },
    InvalidLayout,
    CollectionCapacity,
    DuplicateName(protocol::NameBytes),
    DirectoryIndexOutOfBounds { index: u32, length: usize },
    DirectoryEntryUnavailable { index: u32 },
    Teardown,
    WorkerNotQuiescent,
    InvalidShape,
    InvalidRingSchema { expected: usize, actual: usize },
    PublicationFailed,
}

impl fmt::Display for StatsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => write!(formatter, "stats protocol error: {error:?}"),
            Self::Io(error) => write!(formatter, "stats mapping I/O error: {error}"),
            Self::Allocation(error) => write!(formatter, "stats allocation error: {error}"),
            Self::CapacityTooSmall { requested, minimum } => write!(
                formatter,
                "stats mapping capacity {requested} is below minimum {minimum}"
            ),
            Self::InvalidLayout => formatter.write_str("invalid stats allocation layout"),
            Self::CollectionCapacity => {
                formatter.write_str("stats owner collection capacity failed")
            }
            Self::DuplicateName(_) => formatter.write_str("duplicate stats directory name"),
            Self::DirectoryIndexOutOfBounds { index, length } => write!(
                formatter,
                "stats directory index {index} is outside length {length}"
            ),
            Self::DirectoryEntryUnavailable { index } => {
                write!(formatter, "stats directory entry {index} is unavailable")
            }
            Self::Teardown => formatter.write_str("stats segment is tearing down"),
            Self::WorkerNotQuiescent => formatter.write_str("stats workers are not quiescent"),
            Self::InvalidShape => formatter.write_str("invalid stats shape"),
            Self::InvalidRingSchema { expected, actual } => write!(
                formatter,
                "invalid stats ring schema: expected {expected} bytes, got {actual} bytes"
            ),
            Self::PublicationFailed => formatter.write_str("stats publication failed"),
        }
    }
}

impl std::error::Error for StatsError {}

impl From<protocol::Error> for StatsError {
    fn from(error: protocol::Error) -> Self {
        Self::Protocol(error)
    }
}

impl From<io::Error> for StatsError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SegmentAllocationError> for StatsError {
    fn from(error: SegmentAllocationError) -> Self {
        Self::Allocation(error)
    }
}

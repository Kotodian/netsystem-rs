use std::fmt;
use std::io;

use hammer_infra::segment::SegmentAllocationError;

mod metric;
mod protocol;
mod segment;

use segment::StatsSegment;
use protocol::{DirectoryIndex, DirectoryType, NameBytes};

pub use metric::{
    CombinedCounter, Gauge, Histogram, NameVector, Ring, RingSchema, SimpleCounter, Timestamp,
};
pub use protocol::{Counter, RingConfig};

pub struct StatsMain {
    segment: StatsSegment,
}

// SAFETY: StatsSegmentState contains mapped raw pointers, but every access to
// the directory and payload ownership is serialized by its SpinLock. Public
// StatsMain operations never expose those pointers and teardown requires the
// last segment owner, so sharing the owner through RuntimeRegistry preserves
// the mapping and publication invariants.
unsafe impl Send for StatsMain {}
unsafe impl Sync for StatsMain {}

impl StatsMain {
    pub fn create(name: &str, size: usize) -> StatsResult<Self> {
        Ok(Self {
            segment: StatsSegment::create(name, size)?,
        })
    }

    pub(crate) fn bind_index(
        &self,
        path: &str,
        expected: DirectoryType,
    ) -> StatsResult<DirectoryIndex> {
        let name = NameBytes::try_from(path)?;
        self.segment.find(name, path, expected)
    }

    pub(crate) fn store_timestamp(&self, index: DirectoryIndex, value: u64) -> StatsResult<()> {
        self.segment.store_timestamp(index, value)
    }

    pub(crate) fn increment_timestamp(&self, index: DirectoryIndex) -> StatsResult<()> {
        self.segment.increment_timestamp(index)
    }

    pub(crate) fn store_gauge(&self, index: DirectoryIndex, value: f64) -> StatsResult<()> {
        self.segment.store_gauge(index, value)
    }

    pub(crate) fn validate_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
    ) -> StatsResult<()> {
        self.segment.validate(index, row, column)
    }

    pub(crate) fn write_simple_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        value: u64,
    ) -> StatsResult<()> {
        self.segment.add_simple_counter(index, row, column, value)
    }

    pub(crate) fn write_combined_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        value: Counter,
    ) -> StatsResult<()> {
        self.segment
            .add_combined_counter(index, row, column, value)
    }

    pub(crate) fn write_histogram(
        &self,
        index: DirectoryIndex,
        row: u32,
        bucket: u32,
        value: u64,
    ) -> StatsResult<()> {
        self.segment.add_histogram(index, row, bucket, value)
    }

    pub fn add_gauge(&self, descriptor: Gauge) -> StatsResult<()> {
        let layout = metric::layout::Scalar::<protocol::Gauge>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_timestamp(&self, descriptor: Timestamp) -> StatsResult<()> {
        let layout = metric::layout::Scalar::<protocol::ScalarBits>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_simple_counter(&self, descriptor: SimpleCounter) -> StatsResult<()> {
        let layout = metric::layout::Simple::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_combined_counter(&self, descriptor: CombinedCounter) -> StatsResult<()> {
        let layout = metric::layout::Combined::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_name_vector(&self, descriptor: NameVector) -> StatsResult<()> {
        let layout = metric::layout::NameVector::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_histogram(&self, descriptor: Histogram) -> StatsResult<()> {
        let layout = metric::layout::Histogram::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub fn add_ring<T>(&self, descriptor: Ring<T>) -> StatsResult<()>
    where
        T: RingSchema,
    {
        let layout = metric::layout::Ring::<T>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }
}

pub type StatsResult<T> = Result<T, StatsError>;

#[derive(Debug)]
pub enum StatsError {
    Protocol,
    Io(io::Error),
    Allocation(SegmentAllocationError),
    CapacityTooSmall { requested: usize, minimum: usize },
    InvalidLayout,
    CollectionCapacity,
    DuplicateName,
    MetricNotFound { name: String },
    MetricTypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    MetricUnbound,
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
            Self::Protocol => formatter.write_str("stats protocol error"),
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
            Self::DuplicateName => formatter.write_str("duplicate stats directory name"),
            Self::MetricNotFound { name } => {
                write!(formatter, "stats metric `{name}` is not registered")
            }
            Self::MetricTypeMismatch { expected, actual } => write!(
                formatter,
                "stats metric has type `{actual}`, expected `{expected}`"
            ),
            Self::MetricUnbound => formatter.write_str("stats metric is not bound to a directory entry"),
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
    fn from(_error: protocol::Error) -> Self {
        Self::Protocol
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

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRingEntry;

    impl metric::RingSchema for TestRingEntry {
        const ENTRY_SIZE: u32 = 4;
        const SCHEMA_VERSION: u32 = 1;

        fn schema() -> &'static [u8] {
            &[1, 2]
        }

        fn encode(&self, destination: &mut [u8]) -> StatsResult<()> {
            if destination.len() != Self::ENTRY_SIZE as usize {
                return Err(StatsError::InvalidShape);
            }
            destination.fill(0);
            Ok(())
        }

        fn decode(source: &[u8]) -> StatsResult<Self> {
            if source.len() != Self::ENTRY_SIZE as usize {
                return Err(StatsError::InvalidShape);
            }
            Ok(Self)
        }
    }

    #[test]
    fn add_boundaries_convert_and_delegate_all_metric_families() -> StatsResult<()> {
        let stats = StatsMain::create("st-facade", 2 * 1024 * 1024)?;
        stats.add_gauge(metric::Gauge::new("/facade/gauge"))?;
        stats.add_timestamp(metric::Timestamp::new("/facade/timestamp"))?;
        stats.add_simple_counter(metric::SimpleCounter::new("/facade/simple"))?;
        stats.add_combined_counter(metric::CombinedCounter::new("/facade/combined"))?;
        stats.add_name_vector(metric::NameVector {
            name: "/facade/names".to_owned(),
            length: 2,
        })?;
        stats.add_histogram(metric::Histogram::new("/facade/histogram"))?;
        stats.add_ring(metric::Ring::<TestRingEntry>::new(
            "/facade/ring".to_owned(),
            protocol::RingConfig::new(
                <TestRingEntry as metric::RingSchema>::ENTRY_SIZE,
                2,
                1,
                <TestRingEntry as metric::RingSchema>::schema().len() as u32,
                <TestRingEntry as metric::RingSchema>::SCHEMA_VERSION,
            ),
            <TestRingEntry as metric::RingSchema>::schema().into(),
        ))?;

        assert_eq!(stats.segment.directory_vector_len(), 7);
        assert!(matches!(
            stats.add_gauge(metric::Gauge::new("/facade/gauge")),
            Err(StatsError::DuplicateName)
        ));
        Ok(())
    }

}

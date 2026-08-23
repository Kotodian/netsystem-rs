use std::fmt;
use std::io;

use hammer_infra::segment::SegmentAllocationError;

mod metric;
mod protocol;
mod segment;

use segment::StatsSegment;

pub(crate) struct StatsMain {
    segment: StatsSegment,
}

impl StatsMain {
    pub(crate) fn create(name: &str, size: usize) -> StatsResult<Self> {
        Ok(Self {
            segment: StatsSegment::create(name, size)?,
        })
    }

    pub(crate) fn add_gauge(&self, descriptor: metric::Gauge) -> StatsResult<()> {
        let layout = metric::layout::Scalar::<protocol::Gauge>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub(crate) fn add_timestamp(&self, descriptor: metric::Timestamp) -> StatsResult<()> {
        let layout = metric::layout::Scalar::<protocol::ScalarBits>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub(crate) fn add_simple_counter(&self, descriptor: metric::SimpleCounter) -> StatsResult<()> {
        let layout = metric::layout::Simple::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub(crate) fn add_combined_counter(
        &self,
        descriptor: metric::CombinedCounter,
    ) -> StatsResult<()> {
        let layout = metric::layout::Combined::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub(crate) fn add_name_vector(&self, descriptor: metric::NameVector) -> StatsResult<()> {
        let layout = metric::layout::NameVector::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub(crate) fn add_histogram(&self, descriptor: metric::Histogram) -> StatsResult<()> {
        let layout = metric::layout::Histogram::<protocol::Counter>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }

    pub(crate) fn add_ring<T>(&self, descriptor: metric::Ring<T>) -> StatsResult<()>
    where
        T: metric::RingSchema,
    {
        let layout = metric::layout::Ring::<T>::try_from(descriptor)?;
        self.segment.register(layout).map(|_| ())
    }
}

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
        stats.add_gauge(metric::Gauge {
            name: "/facade/gauge".to_owned(),
        })?;
        stats.add_timestamp(metric::Timestamp {
            name: "/facade/timestamp".to_owned(),
        })?;
        stats.add_simple_counter(metric::SimpleCounter {
            name: "/facade/simple".to_owned(),
        })?;
        stats.add_combined_counter(metric::CombinedCounter {
            name: "/facade/combined".to_owned(),
        })?;
        stats.add_name_vector(metric::NameVector {
            name: "/facade/names".to_owned(),
            length: 2,
        })?;
        stats.add_histogram(metric::Histogram {
            name: "/facade/histogram".to_owned(),
        })?;
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

        assert_eq!(stats.segment.directory_vector_len(), 10);
        assert!(matches!(
            stats.add_gauge(metric::Gauge {
                name: "/facade/gauge".to_owned(),
            }),
            Err(StatsError::DuplicateName(_))
        ));
        Ok(())
    }
}

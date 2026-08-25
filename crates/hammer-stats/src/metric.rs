use crate::protocol::{
    Counter, DirectoryData, DirectoryDataPointer, DirectoryEntry, DirectoryIndex, DirectoryType,
    Gauge as ProtocolGauge, NameBytes, RingBufferHeader, RingConfig, RingMetadata, ScalarBits,
    StringVectorPointer, ring_layout,
};
use crate::segment::StatsSegmentState;
use crate::{StatsError, StatsMain, StatsResult};
use hammer_infra::segment::SegmentAllocation;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::size_of;
use std::ptr;

pub struct Gauge {
    pub(crate) name: String,
    index: Option<DirectoryIndex>,
}

pub struct Timestamp {
    pub(crate) name: String,
    index: Option<DirectoryIndex>,
}

pub struct SimpleCounter {
    pub(crate) name: String,
    index: Option<DirectoryIndex>,
}

pub struct CombinedCounter {
    pub(crate) name: String,
    index: Option<DirectoryIndex>,
}

pub struct NameVector {
    pub(crate) name: String,
    pub(crate) length: u32,
}

pub struct Histogram {
    pub(crate) name: String,
    index: Option<DirectoryIndex>,
}

pub struct Ring<T> {
    pub(crate) name: String,
    pub(crate) config: RingConfig,
    pub(crate) schema: Box<[u8]>,
    marker: PhantomData<fn() -> T>,
}

impl Gauge {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index: None,
        }
    }

    pub fn bind(stats_main: &StatsMain, name: impl Into<String>) -> StatsResult<Self> {
        let name = name.into();
        let index = stats_main.bind_index(&name, DirectoryType::Gauge)?;
        Ok(Self {
            name,
            index: Some(index),
        })
    }

    pub fn store(&self, stats_main: &StatsMain, value: f64) -> StatsResult<()> {
        stats_main.store_gauge(self.index.ok_or(StatsError::MetricUnbound)?, value)
    }
}

impl Timestamp {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index: None,
        }
    }

    pub fn bind(stats_main: &StatsMain, name: impl Into<String>) -> StatsResult<Self> {
        let name = name.into();
        let index = stats_main.bind_index(&name, DirectoryType::ScalarIndex)?;
        Ok(Self {
            name,
            index: Some(index),
        })
    }

    pub fn store(&self, stats_main: &StatsMain, value: u64) -> StatsResult<()> {
        stats_main.store_timestamp(self.index.ok_or(StatsError::MetricUnbound)?, value)
    }

    pub fn increment(&self, stats_main: &StatsMain) -> StatsResult<()> {
        stats_main.increment_timestamp(self.index.ok_or(StatsError::MetricUnbound)?)
    }
}

impl SimpleCounter {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index: None,
        }
    }

    pub fn bind(stats_main: &StatsMain, name: impl Into<String>) -> StatsResult<Self> {
        let name = name.into();
        let index = stats_main.bind_index(&name, DirectoryType::CounterVectorSimple)?;
        Ok(Self {
            name,
            index: Some(index),
        })
    }

    pub fn validate(&self, stats_main: &StatsMain, row: u32, column: u32) -> StatsResult<()> {
        stats_main.validate_counter(self.index.ok_or(StatsError::MetricUnbound)?, row, column)
    }

    pub fn add(
        &self,
        stats_main: &StatsMain,
        row: u32,
        column: u32,
        value: u64,
    ) -> StatsResult<()> {
        stats_main.write_simple_counter(
            self.index.ok_or(StatsError::MetricUnbound)?,
            row,
            column,
            value,
        )
    }
}

impl CombinedCounter {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index: None,
        }
    }

    pub fn bind(stats_main: &StatsMain, name: impl Into<String>) -> StatsResult<Self> {
        let name = name.into();
        let index = stats_main.bind_index(&name, DirectoryType::CounterVectorCombined)?;
        Ok(Self {
            name,
            index: Some(index),
        })
    }

    pub fn validate(&self, stats_main: &StatsMain, row: u32, column: u32) -> StatsResult<()> {
        stats_main.validate_counter(self.index.ok_or(StatsError::MetricUnbound)?, row, column)
    }

    pub fn add(
        &self,
        stats_main: &StatsMain,
        row: u32,
        column: u32,
        value: Counter,
    ) -> StatsResult<()> {
        stats_main.write_combined_counter(
            self.index.ok_or(StatsError::MetricUnbound)?,
            row,
            column,
            value,
        )
    }
}

impl NameVector {
    pub fn new(name: impl Into<String>, length: u32) -> Self {
        Self {
            name: name.into(),
            length,
        }
    }
}

impl Histogram {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            index: None,
        }
    }

    pub fn bind(stats_main: &StatsMain, name: impl Into<String>) -> StatsResult<Self> {
        let name = name.into();
        let index = stats_main.bind_index(&name, DirectoryType::HistogramLog2)?;
        Ok(Self {
            name,
            index: Some(index),
        })
    }

    pub fn validate(&self, stats_main: &StatsMain, row: u32, bucket: u32) -> StatsResult<()> {
        stats_main.validate_counter(self.index.ok_or(StatsError::MetricUnbound)?, row, bucket)
    }

    pub fn add(
        &self,
        stats_main: &StatsMain,
        row: u32,
        bucket: u32,
        value: u64,
    ) -> StatsResult<()> {
        stats_main.write_histogram(
            self.index.ok_or(StatsError::MetricUnbound)?,
            row,
            bucket,
            value,
        )
    }
}

impl<T> Ring<T> {
    pub fn new(name: impl Into<String>, config: RingConfig, schema: Box<[u8]>) -> Self {
        Self {
            name: name.into(),
            config,
            schema,
            marker: PhantomData,
        }
    }
}

pub trait RingSchema: Sized {
    const ENTRY_SIZE: u32;
    const SCHEMA_VERSION: u32;

    fn schema() -> &'static [u8];
    fn encode(&self, destination: &mut [u8]) -> StatsResult<()>;
    fn decode(source: &[u8]) -> StatsResult<Self>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetricValue {
    Scalar(ScalarBits),
    Gauge(ProtocolGauge),
    Simple(Vec<Vec<u64>>),
    Combined(Vec<Vec<Counter>>),
    Names(Vec<String>),
    Histogram(Vec<Vec<u64>>),
    Ring(Vec<Vec<u8>>),
}

pub(crate) mod layout {
    use super::*;

    pub(crate) struct Scalar<M> {
        pub(super) name: NameBytes,
        pub(super) marker: PhantomData<fn() -> M>,
    }

    pub(crate) struct Simple<M> {
        pub(super) name: NameBytes,
        pub(super) marker: PhantomData<fn() -> M>,
    }

    pub(crate) struct Combined<M> {
        pub(super) name: NameBytes,
        pub(super) marker: PhantomData<fn() -> M>,
    }

    pub(crate) struct NameVector {
        pub(super) name: NameBytes,
        pub(super) length: u32,
    }

    pub(crate) struct Histogram<M> {
        pub(super) name: NameBytes,
        pub(super) marker: PhantomData<fn() -> M>,
    }

    pub(crate) struct Ring<T> {
        pub(super) name: NameBytes,
        pub(super) config: RingConfig,
        pub(super) schema: Box<[u8]>,
        pub(super) marker: PhantomData<fn() -> T>,
    }
}

impl TryFrom<Gauge> for layout::Scalar<ProtocolGauge> {
    type Error = StatsError;

    fn try_from(metric: Gauge) -> StatsResult<Self> {
        Ok(Self {
            name: NameBytes::try_from(metric.name.as_str())?,
            marker: PhantomData,
        })
    }
}

impl TryFrom<Timestamp> for layout::Scalar<ScalarBits> {
    type Error = StatsError;

    fn try_from(metric: Timestamp) -> StatsResult<Self> {
        Ok(Self {
            name: NameBytes::try_from(metric.name.as_str())?,
            marker: PhantomData,
        })
    }
}

impl TryFrom<SimpleCounter> for layout::Simple<Counter> {
    type Error = StatsError;

    fn try_from(metric: SimpleCounter) -> StatsResult<Self> {
        Ok(Self {
            name: NameBytes::try_from(metric.name.as_str())?,
            marker: PhantomData,
        })
    }
}

impl TryFrom<CombinedCounter> for layout::Combined<Counter> {
    type Error = StatsError;

    fn try_from(metric: CombinedCounter) -> StatsResult<Self> {
        Ok(Self {
            name: NameBytes::try_from(metric.name.as_str())?,
            marker: PhantomData,
        })
    }
}

impl TryFrom<NameVector> for layout::NameVector {
    type Error = StatsError;

    fn try_from(metric: NameVector) -> StatsResult<Self> {
        Ok(Self {
            name: NameBytes::try_from(metric.name.as_str())?,
            length: metric.length,
        })
    }
}

impl TryFrom<Histogram> for layout::Histogram<Counter> {
    type Error = StatsError;

    fn try_from(metric: Histogram) -> StatsResult<Self> {
        Ok(Self {
            name: NameBytes::try_from(metric.name.as_str())?,
            marker: PhantomData,
        })
    }
}

impl<T> TryFrom<Ring<T>> for layout::Ring<T>
where
    T: RingSchema,
{
    type Error = StatsError;

    fn try_from(metric: Ring<T>) -> StatsResult<Self> {
        if metric.config.ring_size() == 0 || T::ENTRY_SIZE == 0 {
            return Err(StatsError::InvalidShape);
        }
        if metric.config.entry_size() != T::ENTRY_SIZE {
            return Err(StatsError::InvalidRingSchema {
                expected: usize::try_from(T::ENTRY_SIZE)
                    .map_err(|_| StatsError::PublicationFailed)?,
                actual: usize::try_from(metric.config.entry_size())
                    .map_err(|_| StatsError::PublicationFailed)?,
            });
        }
        if metric.config.schema_version() != T::SCHEMA_VERSION {
            return Err(StatsError::InvalidRingSchema {
                expected: usize::try_from(T::SCHEMA_VERSION)
                    .map_err(|_| StatsError::PublicationFailed)?,
                actual: usize::try_from(metric.config.schema_version())
                    .map_err(|_| StatsError::PublicationFailed)?,
            });
        }
        let expected = usize::try_from(metric.config.schema_size())
            .map_err(|_| StatsError::PublicationFailed)?;
        if expected != metric.schema.len() || metric.schema.as_ref() != T::schema() {
            return Err(StatsError::InvalidRingSchema {
                expected: T::schema().len(),
                actual: metric.schema.len(),
            });
        }
        if metric.config.n_threads() == 0 {
            return Err(StatsError::InvalidShape);
        }
        Ok(Self {
            name: NameBytes::try_from(metric.name.as_str())?,
            config: metric.config,
            schema: metric.schema,
            marker: PhantomData,
        })
    }
}

pub(crate) trait RecordKind: Sized {
    type Storage: IntoIterator<Item = SegmentAllocation>;
    type Handle;

    fn name(&self) -> NameBytes;
    fn prepare(
        state: &StatsSegmentState,
        index: DirectoryIndex,
        layout: Self,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)>;
}

impl RecordKind for layout::Scalar<ProtocolGauge> {
    type Storage = [SegmentAllocation; 0];
    type Handle = Self;

    fn name(&self) -> NameBytes {
        self.name
    }

    fn prepare(
        _state: &StatsSegmentState,
        _index: DirectoryIndex,
        layout: Self,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        Ok((
            DirectoryEntry::new(
                DirectoryType::Gauge.into(),
                layout.name,
                DirectoryData::from(ProtocolGauge::from(0)),
            ),
            [],
            layout,
        ))
    }
}

impl RecordKind for layout::Scalar<ScalarBits> {
    type Storage = [SegmentAllocation; 0];
    type Handle = Self;

    fn name(&self) -> NameBytes {
        self.name
    }

    fn prepare(
        _state: &StatsSegmentState,
        _index: DirectoryIndex,
        layout: Self,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        Ok((
            DirectoryEntry::new(
                DirectoryType::ScalarIndex.into(),
                layout.name,
                DirectoryData::from(ScalarBits::from(0_u64)),
            ),
            [],
            layout,
        ))
    }
}

impl RecordKind for layout::Simple<Counter> {
    type Storage = [SegmentAllocation; 0];
    type Handle = Self;

    fn name(&self) -> NameBytes {
        self.name
    }

    fn prepare(
        _state: &StatsSegmentState,
        _index: DirectoryIndex,
        layout: Self,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let entry = DirectoryEntry::new(
            DirectoryType::CounterVectorSimple.into(),
            layout.name,
            DirectoryData::from(DirectoryDataPointer::from(ptr::null_mut())),
        );
        Ok((entry, [], layout))
    }
}

impl RecordKind for layout::Combined<Counter> {
    type Storage = [SegmentAllocation; 0];
    type Handle = Self;

    fn name(&self) -> NameBytes {
        self.name
    }

    fn prepare(
        _state: &StatsSegmentState,
        _index: DirectoryIndex,
        layout: Self,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let entry = DirectoryEntry::new(
            DirectoryType::CounterVectorCombined.into(),
            layout.name,
            DirectoryData::from(DirectoryDataPointer::from(ptr::null_mut())),
        );
        Ok((entry, [], layout))
    }
}

impl RecordKind for layout::NameVector {
    type Storage = [SegmentAllocation; 1];
    type Handle = Self;

    fn name(&self) -> NameBytes {
        self.name
    }

    fn prepare(
        state: &StatsSegmentState,
        _index: DirectoryIndex,
        layout: Self,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let length = usize::try_from(layout.length).map_err(|_| StatsError::PublicationFailed)?;
        if length == 0 {
            return Err(StatsError::InvalidShape);
        }
        let (allocation, pointer) =
            state.allocate_vector::<*mut u8>(length, Some(_index), ptr::null_mut())?;
        let entry = DirectoryEntry::new(
            DirectoryType::NameVector.into(),
            layout.name,
            DirectoryData::from(StringVectorPointer::from(pointer)),
        );
        Ok((entry, [allocation], layout))
    }
}

impl RecordKind for layout::Histogram<Counter> {
    type Storage = [SegmentAllocation; 0];
    type Handle = Self;

    fn name(&self) -> NameBytes {
        self.name
    }

    fn prepare(
        _state: &StatsSegmentState,
        _index: DirectoryIndex,
        layout: Self,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let entry = DirectoryEntry::new(
            DirectoryType::HistogramLog2.into(),
            layout.name,
            DirectoryData::from(DirectoryDataPointer::from(ptr::null_mut())),
        );
        Ok((entry, [], layout))
    }
}

impl<T> RecordKind for layout::Ring<T>
where
    T: RingSchema,
{
    type Storage = [SegmentAllocation; 1];
    type Handle = Self;

    fn name(&self) -> NameBytes {
        self.name
    }

    fn prepare(
        state: &StatsSegmentState,
        _index: DirectoryIndex,
        layout: Self,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let expected = usize::try_from(layout.config.schema_size())
            .map_err(|_| StatsError::PublicationFailed)?;
        let (header, total) = ring_layout(layout.config, 64, state.mapping_size())?;
        let allocation_layout = std::alloc::Layout::from_size_align(total, 64)
            .map_err(|_| StatsError::InvalidLayout)?;
        let allocation = state.allocate_block(allocation_layout)?;
        let base_address = state.allocation_address(&allocation)?;
        let allocation_end = base_address
            .checked_add(allocation.len())
            .ok_or(StatsError::PublicationFailed)?;
        let data_end = base_address
            .checked_add(total)
            .ok_or(StatsError::PublicationFailed)?;
        if data_end > allocation_end || !base_address.is_multiple_of(64) {
            return Err(StatsError::PublicationFailed);
        }
        let base = base_address as *mut u8;
        let config = header.config();
        let metadata_offset =
            usize::try_from(header.metadata_offset()).map_err(|_| StatsError::PublicationFailed)?;
        let n_threads =
            usize::try_from(config.n_threads()).map_err(|_| StatsError::PublicationFailed)?;
        let metadata_size = n_threads
            .checked_mul(size_of::<RingMetadata>())
            .ok_or(StatsError::PublicationFailed)?;
        let schema_offset = if expected == 0 {
            0
        } else {
            metadata_offset
                .checked_add(metadata_size)
                .ok_or(StatsError::PublicationFailed)?
        };
        let schema_offset =
            u32::try_from(schema_offset).map_err(|_| StatsError::PublicationFailed)?;
        unsafe {
            ptr::write_bytes(base, 0, total);
            ptr::write(base.cast::<RingBufferHeader>(), header);
        }
        if expected != 0 {
            for thread_index in 0..n_threads {
                let offset = metadata_offset
                    .checked_add(
                        thread_index
                            .checked_mul(size_of::<RingMetadata>())
                            .ok_or(StatsError::PublicationFailed)?,
                    )
                    .ok_or(StatsError::PublicationFailed)?;
                let metadata =
                    RingMetadata::new(config.schema_version(), schema_offset, config.schema_size());
                unsafe {
                    ptr::write(base.add(offset).cast::<RingMetadata>(), metadata);
                }
            }
            unsafe {
                ptr::copy_nonoverlapping(
                    layout.schema.as_ptr(),
                    base.add(
                        usize::try_from(schema_offset)
                            .map_err(|_| StatsError::PublicationFailed)?,
                    ),
                    layout.schema.len(),
                );
            }
        }
        let entry = DirectoryEntry::new(
            DirectoryType::RingBuffer.into(),
            layout.name,
            DirectoryData::from(DirectoryDataPointer::from(base.cast::<c_void>())),
        );
        Ok((entry, [allocation], layout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestRingSchema;

    impl RingSchema for TestRingSchema {
        const ENTRY_SIZE: u32 = 4;
        const SCHEMA_VERSION: u32 = 1;

        fn schema() -> &'static [u8] {
            &[1, 2]
        }

        fn encode(&self, destination: &mut [u8]) -> StatsResult<()> {
            destination.fill(0);
            Ok(())
        }

        fn decode(_source: &[u8]) -> StatsResult<Self> {
            Ok(Self)
        }
    }

    struct ZeroEntryRingSchema;

    impl RingSchema for ZeroEntryRingSchema {
        const ENTRY_SIZE: u32 = 0;
        const SCHEMA_VERSION: u32 = 1;

        fn schema() -> &'static [u8] {
            &[]
        }

        fn encode(&self, _destination: &mut [u8]) -> StatsResult<()> {
            Ok(())
        }

        fn decode(_source: &[u8]) -> StatsResult<Self> {
            Ok(Self)
        }
    }

    #[test]
    fn ring_conversion_rejects_zero_ring_size() {
        let metric = Ring::<TestRingSchema>::new(
            "/zero-ring-size".to_owned(),
            RingConfig::new(
                TestRingSchema::ENTRY_SIZE,
                0,
                1,
                TestRingSchema::schema().len() as u32,
                TestRingSchema::SCHEMA_VERSION,
            ),
            TestRingSchema::schema().into(),
        );

        assert!(matches!(
            layout::Ring::<TestRingSchema>::try_from(metric),
            Err(StatsError::InvalidShape)
        ));
    }

    #[test]
    fn ring_conversion_rejects_zero_entry_size() {
        let metric = Ring::<ZeroEntryRingSchema>::new(
            "/zero-entry-size".to_owned(),
            RingConfig::new(1, 1, 1, 0, ZeroEntryRingSchema::SCHEMA_VERSION),
            ZeroEntryRingSchema::schema().into(),
        );

        assert!(matches!(
            layout::Ring::<ZeroEntryRingSchema>::try_from(metric),
            Err(StatsError::InvalidShape)
        ));
    }
}

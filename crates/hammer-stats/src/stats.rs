//! The public stats segment API.

use std::alloc::Layout;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use regex::Regex;

use hammer_infra::segment::{Segment, SegmentAllocation};

use crate::directory::{
    DirectorySlot, DirectoryType, EntryState, NULL_INDEX, PrometheusType, SLOT_SIZE, encode_name,
};
use crate::error::StatsError;
use crate::header::StatsHeader;
use crate::mapping::Mapping;
use crate::metric_value::MetricValue;
use crate::offset::Offset;
use crate::read::{DirectoryEntry, DumpEntry, DumpValue};

/// Default stats segment size, mirroring VPP's 32 MiB default
/// (`STAT_SEGMENT_DEFAULT_SIZE`).
pub const DEFAULT_CAPACITY: usize = 32 << 20;

/// Initial directory slot count: one 2 KiB block.
const INITIAL_DIRECTORY_SLOTS: u64 = 8;

/// Maximum stable-read attempts before a reader gives up on a segment that
/// is being continuously republished.
const MAX_READ_ATTEMPTS: usize = 4;

/// Bytes required beyond the reserved first page: the header record, the
/// initial directory block, and the smallest possible metric block.
const MIN_TAIL_BYTES: usize = std::mem::size_of::<StatsHeader>()
    + (INITIAL_DIRECTORY_SLOTS as usize) * SLOT_SIZE
    + crate::descriptor::MIN_BLOCK_BYTES as usize;

/// Identifies one directory entry across slot reuse.
///
/// Captured at add time; `remove_entry` accepts exactly this pair, so a
/// stale `EntryId` (whose slot has since been reused) is rejected instead
/// of acting on a different metric.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EntryId {
    /// Slot index in the current directory block.
    index: u32,
    /// Slot generation captured at add time; mismatch means stale.
    generation: u64,
}

impl EntryId {
    /// Slot index in the current directory block.
    pub const fn index(self) -> u32 {
        self.index
    }

    /// Slot generation captured at add time; mismatch means stale.
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

/// Builds an [`EntryId`] from a raw `(index, generation)` pair. Generation 0
/// is never published by the segment, so it is rejected typed.
impl TryFrom<(u32, u64)> for EntryId {
    type Error = StatsError;

    fn try_from((index, generation): (u32, u64)) -> Result<Self, Self::Error> {
        if generation == 0 {
            return Err(StatsError::InvalidEntryId { index, generation });
        }
        Ok(Self { index, generation })
    }
}

impl std::fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "index {} generation {}", self.index, self.generation)
    }
}

/// Borrowed Prometheus metadata used by protocol-neutral registration.
#[derive(Clone, Copy, Debug)]
pub struct StatsDescriptor<'a> {
    /// Fully qualified Prometheus metric name.
    pub fq_name: &'a str,
    /// Human-readable metric help text.
    pub help: &'a str,
    /// Borrowed const label pairs in registration order.
    pub const_labels: &'a [(&'a str, &'a str)],
}

/// One protocol-neutral structural stats registration.
#[derive(Clone, Copy, Debug)]
pub struct StatsRegistration<'a> {
    /// Stats Directory path.
    pub path: &'a str,
    /// Prometheus descriptor metadata.
    pub descriptor: StatsDescriptor<'a>,
    /// Concrete VPP-compatible value layout.
    pub value: StatsValueLayout<'a>,
}

/// Value layouts accepted by [`StatsMain::register`].
#[derive(Clone, Copy, Debug)]
pub enum StatsValueLayout<'a> {
    /// One monotonically increasing integer.
    Counter,
    /// One floating-point value.
    Gauge,
    /// One timestamp integer.
    Timestamp,
    /// Per-row, per-column simple counters.
    CounterVectorSimple { rows: u32, columns: u32 },
    /// Per-row, per-column combined counters.
    CounterVectorCombined { rows: u32, columns: u32 },
    /// Per-index strings.
    NameVector { length: u32 },
    /// Per-row log2 histogram bins.
    HistogramLog2 { rows: u32 },
    /// Per-row ring buffers with optional schema bytes.
    RingBuffer {
        rows: u32,
        capacity: u32,
        entry_size: u32,
        schema: &'a [u8],
    },
}

/// A direct value handle returned by cold-path registration.
///
/// Workers match this enum once while installing their owner-local handle;
/// updates thereafter call the concrete handle directly.
pub enum StatsEntry {
    /// Direct scalar counter handle.
    Counter { id: EntryId, handle: Counter },
    /// Direct scalar gauge handle.
    Gauge { id: EntryId, handle: Gauge },
    /// Direct scalar timestamp handle.
    Timestamp { id: EntryId, handle: Timestamp },
    /// Direct row/column simple counter-vector handle.
    CounterVectorSimple {
        id: EntryId,
        handle: CounterVectorSimple,
    },
    /// Direct row/column combined packet/byte counter-vector handle.
    CounterVectorCombined {
        id: EntryId,
        handle: CounterVectorCombined,
    },
    /// Direct fixed-slot string vector handle.
    NameVector { id: EntryId, handle: NameVector },
    /// Direct fixed-bin log2 histogram handle.
    HistogramLog2 { id: EntryId, handle: HistogramLog2 },
    /// Direct fixed-size per-row ring-buffer handle.
    RingBuffer { id: EntryId, handle: RingBuffer },
}

#[derive(Clone, Copy)]
enum RegistrationKind<'a> {
    Value(StatsValueLayout<'a>),
    Symlink { target: EntryId, vector_index: u32 },
}

struct TargetRuntimeInfo {
    id: EntryId,
    prometheus_type: PrometheusType,
}

enum RegisteredEntry {
    Handle(StatsEntry),
    Symlink(EntryId),
}

/// Layout marker for [`Counter`].
#[derive(Clone, Copy, Debug, Default)]
pub struct CounterLayout;

/// Layout marker for [`Gauge`].
#[derive(Clone, Copy, Debug, Default)]
pub struct GaugeLayout;

/// Layout marker for [`Timestamp`].
#[derive(Clone, Copy, Debug, Default)]
pub struct TimestampLayout;

/// Mapped layout for a simple counter vector.
#[derive(Clone, Copy, Debug)]
pub struct CounterVectorSimpleLayout {
    data_offset: Offset,
    row_stride: u64,
    rows: u32,
    columns: u32,
}

/// Mapped layout for a combined packet-and-byte counter vector.
#[derive(Clone, Copy, Debug)]
pub struct CounterVectorCombinedLayout {
    data_offset: Offset,
    row_stride: u64,
    rows: u32,
    columns: u32,
}

/// Mapped layout for a bounded name vector.
#[derive(Clone, Copy, Debug)]
pub struct NameVectorLayout {
    data_offset: Offset,
    length: u32,
}

/// Mapped layout for a log2 histogram.
#[derive(Clone, Copy, Debug)]
pub struct HistogramLog2Layout {
    data_offset: Offset,
    row_stride: u64,
    rows: u32,
}

/// Mapped layout for a fixed-size ring buffer.
#[derive(Clone, Copy, Debug)]
pub struct RingBufferLayout {
    data_offset: Offset,
    metadata_offset: Offset,
    schema_offset: Option<Offset>,
    schema_size: usize,
    slot_stride: u64,
    rows: u32,
    capacity: u32,
    entry_size: u32,
}

struct LayoutBlock<L> {
    allocation: SegmentAllocation,
    value_offset: Offset,
    layout: L,
}

trait MetricLayout: Copy + 'static {
    const DIRECTORY_TYPE: DirectoryType;
    const PROMETHEUS_TYPE: PrometheusType;

    fn allocate<'a>(
        segment: &Segment,
        descriptor: &crate::descriptor::NormalizedDescriptor,
        generation: u64,
        value: StatsValueLayout<'a>,
    ) -> Result<LayoutBlock<Self>, StatsError>;

    fn register<'a>(
        segment: &Segment,
        path: &str,
        id: EntryId,
        descriptor: &crate::descriptor::NormalizedDescriptor,
        value: StatsValueLayout<'a>,
    ) -> Result<(DirectorySlot, RegisteredEntry, SegmentAllocation), StatsError> {
        let block = Self::allocate(segment, descriptor, id.generation, value)?;
        let block_offset = block_offset(&block.allocation, 0)?;
        let entry = DirectorySlot::new_active(
            encode_name(path)?,
            id.generation,
            Self::DIRECTORY_TYPE,
            Self::PROMETHEUS_TYPE,
            block_offset,
            block.value_offset,
        );
        let registered =
            RegisteredEntry::Handle(Self::registered(segment.clone(), id, block.layout));
        Ok((entry, registered, block.allocation))
    }

    fn decode(mapping: &Mapping, slot: &DirectorySlot) -> Result<Self, StatsError>;

    fn dump(
        &self,
        mapping: &Mapping,
        id: EntryId,
        slot: &DirectorySlot,
        vector_index: Option<u32>,
    ) -> Result<DumpValue, StatsError>;

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry;
}

/// Shared validation core for every public stats handle.
///
/// `L` contains only operation-specific offsets and dimensions. The handle
/// keeps the segment mapping and directory identity; the current value offset
/// is read from the published directory slot after generation validation.
/// Dispatch remains static after monomorphization.
#[derive(Clone)]
pub struct MetricHandle<L> {
    segment: Segment,
    id: EntryId,
    layout: L,
}

/// Counter handle alias retained for the public API.
pub type Counter = MetricHandle<CounterLayout>;
/// Gauge handle alias retained for the public API.
pub type Gauge = MetricHandle<GaugeLayout>;
/// Timestamp handle alias retained for the public API.
pub type Timestamp = MetricHandle<TimestampLayout>;
/// Simple counter-vector handle alias retained for the public API.
pub type CounterVectorSimple = MetricHandle<CounterVectorSimpleLayout>;
/// Combined counter-vector handle alias retained for the public API.
pub type CounterVectorCombined = MetricHandle<CounterVectorCombinedLayout>;
/// Name-vector handle alias retained for the public API.
pub type NameVector = MetricHandle<NameVectorLayout>;
/// Log2 histogram handle alias retained for the public API.
pub type HistogramLog2 = MetricHandle<HistogramLog2Layout>;
/// Ring-buffer handle alias retained for the public API.
pub type RingBuffer = MetricHandle<RingBufferLayout>;

impl<L> MetricHandle<L> {
    fn new(segment: Segment, id: EntryId, layout: L) -> Self {
        Self {
            segment,
            id,
            layout,
        }
    }

    /// Runs an operation after validating the current directory slot.
    ///
    /// The slot index is stable across directory relocation. Looking up the
    /// current slot before reading the value also lets a stale handle report a
    /// typed error after its old metric block has been retired.
    fn with_mapping_value<T>(
        &self,
        op: impl FnOnce(&Mapping, &MetricValue) -> Result<T, StatsError>,
    ) -> Result<T, StatsError> {
        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        if u64::from(self.id.index) >= header.initialized_len() {
            return Err(StatsError::NotFound { id: self.id });
        }
        let slot = mapping.entry(header.directory_offset(), self.id.index)?;
        match slot.state()? {
            EntryState::Active => {}
            EntryState::Free | EntryState::Removed => {
                return Err(StatsError::NotFound { id: self.id });
            }
        }
        if slot.generation() != self.id.generation {
            return Err(StatsError::StaleEntry { id: self.id });
        }
        let value = mapping.metric_value(slot.value_offset())?;
        if value.generation() != self.id.generation {
            return Err(StatsError::StaleEntry { id: self.id });
        }
        op(&mapping, value)
    }
}

fn block_offset(allocation: &SegmentAllocation, relative: u64) -> Result<Offset, StatsError> {
    Offset::new(allocation.offset())
        .checked_add(relative)
        .ok_or(StatsError::OutOfBounds)
}

fn relative_offset(base: Offset, relative: usize) -> Result<Offset, StatsError> {
    base.checked_add(u64::try_from(relative).map_err(|_| StatsError::OutOfBounds)?)
        .ok_or(StatsError::OutOfBounds)
}

trait ScalarMetricLayout: Copy + Default + 'static {
    const DIRECTORY_TYPE: DirectoryType;
    const PROMETHEUS_TYPE: PrometheusType;

    fn accepts(value: StatsValueLayout<'_>) -> bool;
    fn dump_value(value: u64) -> DumpValue;
    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry;
}

impl<L: ScalarMetricLayout> MetricLayout for L {
    const DIRECTORY_TYPE: DirectoryType = L::DIRECTORY_TYPE;
    const PROMETHEUS_TYPE: PrometheusType = L::PROMETHEUS_TYPE;

    fn allocate<'a>(
        segment: &Segment,
        descriptor: &crate::descriptor::NormalizedDescriptor,
        generation: u64,
        value: StatsValueLayout<'a>,
    ) -> Result<LayoutBlock<Self>, StatsError> {
        if !L::accepts(value) {
            return Err(StatsError::UnsupportedLayout);
        }
        let layout = crate::descriptor::block_layout(descriptor)?;
        let mut allocation = segment.allocate(layout)?;
        let value_offset =
            crate::descriptor::write_block(&mut allocation.bytes_mut(), descriptor, generation)?;
        Ok(LayoutBlock {
            value_offset: block_offset(&allocation, value_offset)?,
            allocation,
            layout: L::default(),
        })
    }

    fn decode(mapping: &Mapping, slot: &DirectorySlot) -> Result<Self, StatsError> {
        mapping.metric_value(slot.value_offset())?;
        Ok(L::default())
    }

    fn dump(
        &self,
        mapping: &Mapping,
        _id: EntryId,
        slot: &DirectorySlot,
        _vector_index: Option<u32>,
    ) -> Result<DumpValue, StatsError> {
        Ok(L::dump_value(
            mapping.metric_value(slot.value_offset())?.load_value(),
        ))
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        L::registered(segment, id, layout)
    }
}

impl ScalarMetricLayout for CounterLayout {
    const DIRECTORY_TYPE: DirectoryType = DirectoryType::ScalarIndex;
    const PROMETHEUS_TYPE: PrometheusType = PrometheusType::Counter;

    fn accepts(value: StatsValueLayout<'_>) -> bool {
        matches!(value, StatsValueLayout::Counter)
    }

    fn dump_value(value: u64) -> DumpValue {
        DumpValue::Counter(value)
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        StatsEntry::Counter {
            id,
            handle: MetricHandle::new(segment, id, layout),
        }
    }
}

impl ScalarMetricLayout for GaugeLayout {
    const DIRECTORY_TYPE: DirectoryType = DirectoryType::Gauge;
    const PROMETHEUS_TYPE: PrometheusType = PrometheusType::Gauge;

    fn accepts(value: StatsValueLayout<'_>) -> bool {
        matches!(value, StatsValueLayout::Gauge)
    }

    fn dump_value(value: u64) -> DumpValue {
        DumpValue::Gauge(f64::from_bits(value))
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        StatsEntry::Gauge {
            id,
            handle: MetricHandle::new(segment, id, layout),
        }
    }
}

impl ScalarMetricLayout for TimestampLayout {
    const DIRECTORY_TYPE: DirectoryType = DirectoryType::ScalarIndex;
    const PROMETHEUS_TYPE: PrometheusType = PrometheusType::Gauge;

    fn accepts(value: StatsValueLayout<'_>) -> bool {
        matches!(value, StatsValueLayout::Timestamp)
    }

    fn dump_value(value: u64) -> DumpValue {
        DumpValue::Gauge(value as f64)
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        StatsEntry::Timestamp {
            id,
            handle: MetricHandle::new(segment, id, layout),
        }
    }
}

impl MetricLayout for CounterVectorSimpleLayout {
    const DIRECTORY_TYPE: DirectoryType = DirectoryType::CounterVectorSimple;
    const PROMETHEUS_TYPE: PrometheusType = PrometheusType::Counter;

    fn allocate<'a>(
        segment: &Segment,
        descriptor: &crate::descriptor::NormalizedDescriptor,
        generation: u64,
        value: StatsValueLayout<'a>,
    ) -> Result<LayoutBlock<Self>, StatsError> {
        let StatsValueLayout::CounterVectorSimple { rows, columns } = value else {
            return Err(StatsError::UnsupportedLayout);
        };
        let layout =
            crate::descriptor::counter_vector_simple_block_layout(descriptor, rows, columns)?;
        let mut allocation = segment.allocate(layout)?;
        let (value_offset, data_offset) = crate::descriptor::write_counter_vector_simple_block(
            &mut allocation.bytes_mut(),
            descriptor,
            generation,
            rows,
            columns,
        )?;
        let row_stride = u64::try_from(crate::descriptor::counter_vector_row_stride(
            columns,
            std::mem::size_of::<AtomicU64>(),
        )?)
        .map_err(|_| StatsError::OutOfBounds)?;
        Ok(LayoutBlock {
            value_offset: block_offset(&allocation, value_offset)?,
            layout: Self {
                data_offset: block_offset(&allocation, data_offset)?,
                row_stride,
                rows,
                columns,
            },
            allocation,
        })
    }

    fn decode(mapping: &Mapping, slot: &DirectorySlot) -> Result<Self, StatsError> {
        let block = mapping.descriptor_block(slot.descriptor_offset())?;
        let layout = crate::descriptor::decode_counter_vector_simple_layout(block)?;
        Ok(Self {
            data_offset: relative_offset(slot.descriptor_offset(), layout.data_offset)?,
            row_stride: u64::try_from(layout.row_stride).map_err(|_| StatsError::OutOfBounds)?,
            rows: layout.rows,
            columns: layout.columns,
        })
    }

    fn dump(
        &self,
        mapping: &Mapping,
        id: EntryId,
        _slot: &DirectorySlot,
        vector_index: Option<u32>,
    ) -> Result<DumpValue, StatsError> {
        let selected = vector_index.unwrap_or(0);
        if self.rows != 0 && selected >= self.columns {
            return Err(StatsError::IncompatibleType {
                id,
                prometheus_type: Self::PROMETHEUS_TYPE,
                directory_type: Self::DIRECTORY_TYPE,
            });
        }
        let columns = vector_index.map_or(self.columns, |_| 1);
        let mut rows = Vec::with_capacity(self.rows as usize);
        for row in 0..self.rows {
            let mut values = Vec::with_capacity(columns as usize);
            for column in 0..columns {
                let actual = vector_index.map_or(column, |_| selected);
                let offset = self
                    .data_offset
                    .get()
                    .checked_add(
                        u64::from(row)
                            .checked_mul(self.row_stride)
                            .and_then(|base| {
                                u64::from(actual)
                                    .checked_mul(std::mem::size_of::<AtomicU64>() as u64)
                                    .and_then(|column_offset| base.checked_add(column_offset))
                            })
                            .ok_or(StatsError::OutOfBounds)?,
                    )
                    .ok_or(StatsError::OutOfBounds)?;
                values.push(
                    mapping
                        .atomic_u64(Offset::new(offset))?
                        .load(Ordering::Relaxed),
                );
            }
            rows.push(values);
        }
        Ok(DumpValue::CounterVectorSimple(rows))
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        StatsEntry::CounterVectorSimple {
            id,
            handle: MetricHandle::new(segment, id, layout),
        }
    }
}

impl MetricLayout for CounterVectorCombinedLayout {
    const DIRECTORY_TYPE: DirectoryType = DirectoryType::CounterVectorCombined;
    const PROMETHEUS_TYPE: PrometheusType = PrometheusType::Counter;

    fn allocate<'a>(
        segment: &Segment,
        descriptor: &crate::descriptor::NormalizedDescriptor,
        generation: u64,
        value: StatsValueLayout<'a>,
    ) -> Result<LayoutBlock<Self>, StatsError> {
        let StatsValueLayout::CounterVectorCombined { rows, columns } = value else {
            return Err(StatsError::UnsupportedLayout);
        };
        let layout =
            crate::descriptor::counter_vector_combined_block_layout(descriptor, rows, columns)?;
        let mut allocation = segment.allocate(layout)?;
        let (value_offset, data_offset) = crate::descriptor::write_counter_vector_combined_block(
            &mut allocation.bytes_mut(),
            descriptor,
            generation,
            rows,
            columns,
        )?;
        let row_stride = u64::try_from(crate::descriptor::counter_vector_row_stride(
            columns,
            2 * std::mem::size_of::<AtomicU64>(),
        )?)
        .map_err(|_| StatsError::OutOfBounds)?;
        Ok(LayoutBlock {
            value_offset: block_offset(&allocation, value_offset)?,
            layout: Self {
                data_offset: block_offset(&allocation, data_offset)?,
                row_stride,
                rows,
                columns,
            },
            allocation,
        })
    }

    fn decode(mapping: &Mapping, slot: &DirectorySlot) -> Result<Self, StatsError> {
        let block = mapping.descriptor_block(slot.descriptor_offset())?;
        let layout = crate::descriptor::decode_counter_vector_combined_layout(block)?;
        Ok(Self {
            data_offset: relative_offset(slot.descriptor_offset(), layout.data_offset)?,
            row_stride: u64::try_from(layout.row_stride).map_err(|_| StatsError::OutOfBounds)?,
            rows: layout.rows,
            columns: layout.columns,
        })
    }

    fn dump(
        &self,
        mapping: &Mapping,
        id: EntryId,
        _slot: &DirectorySlot,
        vector_index: Option<u32>,
    ) -> Result<DumpValue, StatsError> {
        let selected = vector_index.unwrap_or(0);
        if selected >= self.columns {
            return Err(StatsError::IncompatibleType {
                id,
                prometheus_type: Self::PROMETHEUS_TYPE,
                directory_type: Self::DIRECTORY_TYPE,
            });
        }
        let columns = vector_index.map_or(self.columns, |_| 1);
        let mut rows = Vec::with_capacity(self.rows as usize);
        for row in 0..self.rows {
            let mut values = Vec::with_capacity(columns as usize);
            for column in 0..columns {
                let actual = vector_index.map_or(column, |_| selected);
                let offset = self
                    .data_offset
                    .get()
                    .checked_add(
                        u64::from(row)
                            .checked_mul(self.row_stride)
                            .and_then(|base| {
                                u64::from(actual)
                                    .checked_mul(2 * std::mem::size_of::<AtomicU64>() as u64)
                                    .and_then(|column_offset| base.checked_add(column_offset))
                            })
                            .ok_or(StatsError::OutOfBounds)?,
                    )
                    .ok_or(StatsError::OutOfBounds)?;
                let packets = mapping
                    .atomic_u64(Offset::new(offset))?
                    .load(Ordering::Relaxed);
                let bytes = mapping
                    .atomic_u64(Offset::new(
                        offset
                            .checked_add(std::mem::size_of::<AtomicU64>() as u64)
                            .ok_or(StatsError::OutOfBounds)?,
                    ))?
                    .load(Ordering::Relaxed);
                values.push((packets, bytes));
            }
            rows.push(values);
        }
        Ok(DumpValue::CounterVectorCombined(rows))
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        StatsEntry::CounterVectorCombined {
            id,
            handle: MetricHandle::new(segment, id, layout),
        }
    }
}

impl MetricLayout for NameVectorLayout {
    const DIRECTORY_TYPE: DirectoryType = DirectoryType::NameVector;
    const PROMETHEUS_TYPE: PrometheusType = PrometheusType::Gauge;

    fn allocate<'a>(
        segment: &Segment,
        descriptor: &crate::descriptor::NormalizedDescriptor,
        generation: u64,
        value: StatsValueLayout<'a>,
    ) -> Result<LayoutBlock<Self>, StatsError> {
        let StatsValueLayout::NameVector { length } = value else {
            return Err(StatsError::UnsupportedLayout);
        };
        let layout = crate::descriptor::name_vector_block_layout(descriptor, length)?;
        let mut allocation = segment.allocate(layout)?;
        let (value_offset, data_offset) = crate::descriptor::write_name_vector_block(
            &mut allocation.bytes_mut(),
            descriptor,
            generation,
            length,
        )?;
        Ok(LayoutBlock {
            value_offset: block_offset(&allocation, value_offset)?,
            layout: Self {
                data_offset: block_offset(&allocation, data_offset)?,
                length,
            },
            allocation,
        })
    }

    fn decode(mapping: &Mapping, slot: &DirectorySlot) -> Result<Self, StatsError> {
        let block = mapping.descriptor_block(slot.descriptor_offset())?;
        let layout = crate::descriptor::decode_name_vector_layout(block)?;
        Ok(Self {
            data_offset: relative_offset(slot.descriptor_offset(), layout.data_offset)?,
            length: layout.length,
        })
    }

    fn dump(
        &self,
        mapping: &Mapping,
        _id: EntryId,
        _slot: &DirectorySlot,
        _vector_index: Option<u32>,
    ) -> Result<DumpValue, StatsError> {
        let mut names = Vec::with_capacity(self.length as usize);
        for index in 0..self.length {
            names.push(read_name_slot_at(
                mapping,
                self.data_offset,
                self.length,
                index,
            )?);
        }
        Ok(DumpValue::NameVector(names))
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        StatsEntry::NameVector {
            id,
            handle: MetricHandle::new(segment, id, layout),
        }
    }
}

impl MetricLayout for HistogramLog2Layout {
    const DIRECTORY_TYPE: DirectoryType = DirectoryType::HistogramLog2;
    const PROMETHEUS_TYPE: PrometheusType = PrometheusType::Counter;

    fn allocate<'a>(
        segment: &Segment,
        descriptor: &crate::descriptor::NormalizedDescriptor,
        generation: u64,
        value: StatsValueLayout<'a>,
    ) -> Result<LayoutBlock<Self>, StatsError> {
        let StatsValueLayout::HistogramLog2 { rows } = value else {
            return Err(StatsError::UnsupportedLayout);
        };
        let layout = crate::descriptor::histogram_log2_block_layout(descriptor, rows)?;
        let mut allocation = segment.allocate(layout)?;
        let (value_offset, data_offset) = crate::descriptor::write_histogram_log2_block(
            &mut allocation.bytes_mut(),
            descriptor,
            generation,
            rows,
        )?;
        let row_stride = u64::try_from(crate::descriptor::counter_vector_row_stride(
            crate::descriptor::HISTOGRAM_BIN_COUNT,
            std::mem::size_of::<AtomicU64>(),
        )?)
        .map_err(|_| StatsError::OutOfBounds)?;
        Ok(LayoutBlock {
            value_offset: block_offset(&allocation, value_offset)?,
            layout: Self {
                data_offset: block_offset(&allocation, data_offset)?,
                row_stride,
                rows,
            },
            allocation,
        })
    }

    fn decode(mapping: &Mapping, slot: &DirectorySlot) -> Result<Self, StatsError> {
        let block = mapping.descriptor_block(slot.descriptor_offset())?;
        let layout = crate::descriptor::decode_histogram_log2_layout(block)?;
        Ok(Self {
            data_offset: relative_offset(slot.descriptor_offset(), layout.data_offset)?,
            row_stride: u64::try_from(layout.row_stride).map_err(|_| StatsError::OutOfBounds)?,
            rows: layout.rows,
        })
    }

    fn dump(
        &self,
        mapping: &Mapping,
        _id: EntryId,
        _slot: &DirectorySlot,
        _vector_index: Option<u32>,
    ) -> Result<DumpValue, StatsError> {
        let mut rows = Vec::with_capacity(self.rows as usize);
        for row in 0..self.rows {
            let mut bins = Vec::with_capacity(crate::descriptor::HISTOGRAM_BIN_COUNT as usize);
            for bin in 0..crate::descriptor::HISTOGRAM_BIN_COUNT {
                let offset = self
                    .data_offset
                    .get()
                    .checked_add(
                        u64::from(row)
                            .checked_mul(self.row_stride)
                            .and_then(|base| {
                                base.checked_add(
                                    u64::from(bin) * std::mem::size_of::<AtomicU64>() as u64,
                                )
                            })
                            .ok_or(StatsError::OutOfBounds)?,
                    )
                    .ok_or(StatsError::OutOfBounds)?;
                bins.push(
                    mapping
                        .atomic_u64(Offset::new(offset))?
                        .load(Ordering::Relaxed),
                );
            }
            rows.push(bins);
        }
        Ok(DumpValue::HistogramLog2(rows))
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        StatsEntry::HistogramLog2 {
            id,
            handle: MetricHandle::new(segment, id, layout),
        }
    }
}

impl MetricLayout for RingBufferLayout {
    const DIRECTORY_TYPE: DirectoryType = DirectoryType::RingBuffer;
    const PROMETHEUS_TYPE: PrometheusType = PrometheusType::Counter;

    fn allocate<'a>(
        segment: &Segment,
        descriptor: &crate::descriptor::NormalizedDescriptor,
        generation: u64,
        value: StatsValueLayout<'a>,
    ) -> Result<LayoutBlock<Self>, StatsError> {
        let StatsValueLayout::RingBuffer {
            rows,
            capacity,
            entry_size,
            schema,
        } = value
        else {
            return Err(StatsError::UnsupportedLayout);
        };
        let layout = crate::descriptor::ring_buffer_block_layout(
            descriptor,
            rows,
            capacity,
            entry_size,
            schema.len(),
        )?;
        let mut allocation = segment.allocate(layout)?;
        let (value_offset, data_offset, metadata_offset, schema_offset, slot_stride) =
            crate::descriptor::write_ring_buffer_block(
                &mut allocation.bytes_mut(),
                descriptor,
                generation,
                rows,
                capacity,
                entry_size,
                schema,
            )?;
        Ok(LayoutBlock {
            value_offset: block_offset(&allocation, value_offset)?,
            layout: Self {
                data_offset: block_offset(&allocation, data_offset)?,
                metadata_offset: block_offset(&allocation, metadata_offset)?,
                schema_offset: schema_offset
                    .map(|offset| block_offset(&allocation, offset))
                    .transpose()?,
                schema_size: schema.len(),
                slot_stride,
                rows,
                capacity,
                entry_size,
            },
            allocation,
        })
    }

    fn decode(mapping: &Mapping, slot: &DirectorySlot) -> Result<Self, StatsError> {
        let block = mapping.descriptor_block(slot.descriptor_offset())?;
        let layout = crate::descriptor::decode_ring_buffer_layout(block)?;
        Ok(Self {
            data_offset: relative_offset(slot.descriptor_offset(), layout.data_offset)?,
            metadata_offset: relative_offset(slot.descriptor_offset(), layout.metadata_offset)?,
            schema_offset: layout
                .schema_offset
                .map(|offset| relative_offset(slot.descriptor_offset(), offset))
                .transpose()?,
            schema_size: layout.schema_size,
            slot_stride: u64::try_from(layout.slot_stride).map_err(|_| StatsError::OutOfBounds)?,
            rows: layout.rows,
            capacity: layout.capacity,
            entry_size: layout.entry_size,
        })
    }

    fn dump(
        &self,
        mapping: &Mapping,
        _id: EntryId,
        _slot: &DirectorySlot,
        _vector_index: Option<u32>,
    ) -> Result<DumpValue, StatsError> {
        let mut snapshots = Vec::with_capacity(self.rows as usize);
        for row in 0..self.rows {
            let metadata = self
                .metadata_offset
                .checked_add(
                    u64::from(row)
                        .checked_mul(crate::descriptor::RING_METADATA_BYTES as u64)
                        .ok_or(StatsError::OutOfBounds)?,
                )
                .ok_or(StatsError::OutOfBounds)?;
            let sequence_cell = mapping.atomic_u64(Offset::new(
                metadata
                    .get()
                    .checked_add(8)
                    .ok_or(StatsError::OutOfBounds)?,
            ))?;
            let publication_cell = mapping.atomic_u64(Offset::new(
                metadata
                    .get()
                    .checked_add(24)
                    .ok_or(StatsError::OutOfBounds)?,
            ))?;
            let snapshot = {
                let mut snapshot = None;
                for _ in 0..MAX_READ_ATTEMPTS {
                    let publication = publication_cell.load(Ordering::Acquire);
                    if publication & 1 != 0 {
                        std::hint::spin_loop();
                        continue;
                    }
                    let sequence = sequence_cell.load(Ordering::Acquire);
                    let mut entries = Vec::with_capacity(self.capacity as usize);
                    for slot_index in 0..self.capacity {
                        let offset = ring_slot_offset(
                            self.data_offset,
                            self.slot_stride,
                            self.rows,
                            self.capacity,
                            row,
                            slot_index,
                        )?;
                        entries.push(read_ring_slot_bytes(
                            mapping,
                            offset,
                            self.entry_size as usize,
                        )?);
                    }
                    let after_publication = publication_cell.load(Ordering::Acquire);
                    let after_sequence = sequence_cell.load(Ordering::Acquire);
                    if publication == after_publication
                        && after_publication & 1 == 0
                        && sequence == after_sequence
                    {
                        snapshot = Some(crate::read::RingBufferSnapshot { sequence, entries });
                        break;
                    }
                    std::hint::spin_loop();
                }
                snapshot.ok_or(StatsError::ReadBusy)?
            };
            snapshots.push(snapshot);
        }
        Ok(DumpValue::RingBuffer(snapshots))
    }

    fn registered(segment: Segment, id: EntryId, layout: Self) -> StatsEntry {
        StatsEntry::RingBuffer {
            id,
            handle: MetricHandle::new(segment, id, layout),
        }
    }
}

impl MetricHandle<CounterLayout> {
    /// Increments the value by one.
    pub fn increment(&self) -> Result<(), StatsError> {
        self.with_mapping_value(|_, value| {
            value.add_value(1);
            Ok(())
        })
    }

    /// Increments the value by `delta`.
    pub fn increment_by(&self, delta: u64) -> Result<(), StatsError> {
        self.with_mapping_value(|_, value| {
            value.add_value(delta);
            Ok(())
        })
    }

    /// Reads the current value.
    pub fn get(&self) -> Result<u64, StatsError> {
        self.with_mapping_value(|_, value| Ok(value.load_value()))
    }
}

impl MetricHandle<GaugeLayout> {
    /// Sets the value.
    pub fn set(&self, value: f64) -> Result<(), StatsError> {
        self.with_mapping_value(|_, record| {
            record.store_value(value.to_bits());
            Ok(())
        })
    }

    /// Reads the current value.
    pub fn get(&self) -> Result<f64, StatsError> {
        self.with_mapping_value(|_, record| Ok(f64::from_bits(record.load_value())))
    }
}

impl MetricHandle<TimestampLayout> {
    /// Sets the value (e.g. a `SystemTime` epoch second).
    pub fn set(&self, value: u64) -> Result<(), StatsError> {
        self.with_mapping_value(|_, record| {
            record.store_value(value);
            Ok(())
        })
    }

    /// Reads the current value.
    pub fn get(&self) -> Result<u64, StatsError> {
        self.with_mapping_value(|_, record| Ok(record.load_value()))
    }
}

/// A direct handle to a fixed-size row/column simple counter vector.
///
/// Updates perform one generation check and one atomic cell operation. They do
/// not look up the directory, allocate, resize, or acquire a lock.
impl MetricHandle<CounterVectorSimpleLayout> {
    /// Number of rows allocated for this vector.
    pub const fn rows(&self) -> u32 {
        self.layout.rows
    }

    /// Number of columns allocated for this vector.
    pub const fn columns(&self) -> u32 {
        self.layout.columns
    }

    /// Increments one row/column cell by one.
    pub fn increment(&self, row: u32, column: u32) -> Result<(), StatsError> {
        self.increment_by(row, column, 1)
    }

    /// Increments one row/column cell by `delta`.
    pub fn increment_by(&self, row: u32, column: u32, delta: u64) -> Result<(), StatsError> {
        self.with_cell(row, column, |cell| {
            cell.fetch_add(delta, Ordering::Relaxed);
            Ok(())
        })
    }

    /// Reads one row/column cell.
    pub fn get(&self, row: u32, column: u32) -> Result<u64, StatsError> {
        self.with_cell(row, column, |cell| Ok(cell.load(Ordering::Relaxed)))
    }

    fn with_cell<T>(
        &self,
        row: u32,
        column: u32,
        op: impl FnOnce(&AtomicU64) -> Result<T, StatsError>,
    ) -> Result<T, StatsError> {
        if row >= self.layout.rows || column >= self.layout.columns {
            return Err(StatsError::OutOfBounds);
        }
        let row_offset = u64::from(row)
            .checked_mul(self.layout.row_stride)
            .ok_or(StatsError::OutOfBounds)?;
        let cell_offset = row_offset
            .checked_add(
                u64::from(column)
                    .checked_mul(std::mem::size_of::<AtomicU64>() as u64)
                    .ok_or(StatsError::OutOfBounds)?,
            )
            .and_then(|relative| self.layout.data_offset.get().checked_add(relative))
            .ok_or(StatsError::OutOfBounds)?;
        self.with_mapping_value(|mapping, _| op(mapping.atomic_u64(Offset::new(cell_offset))?))
    }
}

/// A direct handle to a fixed-size row/column packet-and-byte counter vector.
///
/// Each cell contains two independent relaxed atomics, matching VPP's
/// `vlib_counter_t { packets, bytes }`; a row starts on its own 64-byte
/// boundary to preserve worker ownership and cache separation.
impl MetricHandle<CounterVectorCombinedLayout> {
    /// Number of rows allocated for this vector.
    pub const fn rows(&self) -> u32 {
        self.layout.rows
    }

    /// Number of columns allocated for this vector.
    pub const fn columns(&self) -> u32 {
        self.layout.columns
    }

    /// Increments one packet/byte cell by one pair.
    pub fn increment(&self, row: u32, column: u32) -> Result<(), StatsError> {
        self.increment_by(row, column, 1, 1)
    }

    /// Increments one cell by packet and byte deltas.
    pub fn increment_by(
        &self,
        row: u32,
        column: u32,
        packets: u64,
        bytes: u64,
    ) -> Result<(), StatsError> {
        self.with_cell(row, column, |packets_cell, bytes_cell| {
            packets_cell.fetch_add(packets, Ordering::Relaxed);
            bytes_cell.fetch_add(bytes, Ordering::Relaxed);
            Ok(())
        })
    }

    /// Reads one packet/byte cell.
    pub fn get(&self, row: u32, column: u32) -> Result<(u64, u64), StatsError> {
        self.with_cell(row, column, |packets_cell, bytes_cell| {
            Ok((
                packets_cell.load(Ordering::Relaxed),
                bytes_cell.load(Ordering::Relaxed),
            ))
        })
    }

    fn with_cell<T>(
        &self,
        row: u32,
        column: u32,
        op: impl FnOnce(&AtomicU64, &AtomicU64) -> Result<T, StatsError>,
    ) -> Result<T, StatsError> {
        if row >= self.layout.rows || column >= self.layout.columns {
            return Err(StatsError::OutOfBounds);
        }
        let row_offset = u64::from(row)
            .checked_mul(self.layout.row_stride)
            .ok_or(StatsError::OutOfBounds)?;
        let cell_offset = row_offset
            .checked_add(
                u64::from(column)
                    .checked_mul(2 * std::mem::size_of::<AtomicU64>() as u64)
                    .ok_or(StatsError::OutOfBounds)?,
            )
            .and_then(|relative| self.layout.data_offset.get().checked_add(relative))
            .ok_or(StatsError::OutOfBounds)?;
        let bytes_offset = cell_offset
            .checked_add(std::mem::size_of::<AtomicU64>() as u64)
            .ok_or(StatsError::OutOfBounds)?;
        self.with_mapping_value(|mapping, _| {
            op(
                mapping.atomic_u64(Offset::new(cell_offset))?,
                mapping.atomic_u64(Offset::new(bytes_offset))?,
            )
        })
    }
}

/// A direct handle to a bounded fixed-slot name vector.
impl MetricHandle<NameVectorLayout> {
    pub const fn len(&self) -> u32 {
        self.layout.length
    }

    /// Stores one bounded UTF-8 name without allocating after registration.
    pub fn set(&self, index: u32, name: &str) -> Result<(), StatsError> {
        let bytes = name.as_bytes();
        if bytes.len() > crate::descriptor::NAME_VECTOR_MAX_BYTES {
            return Err(StatsError::InvalidDescriptor(format!(
                "name vector value exceeds {} bytes",
                crate::descriptor::NAME_VECTOR_MAX_BYTES
            )));
        }
        self.write_slot(index, Some(bytes))
    }

    /// Clears one name slot.
    pub fn clear(&self, index: u32) -> Result<(), StatsError> {
        self.write_slot(index, None)
    }

    /// Reads one name slot into an owned string.
    pub fn get(&self, index: u32) -> Result<Option<String>, StatsError> {
        if index >= self.layout.length {
            return Err(StatsError::OutOfBounds);
        }
        self.with_mapping_value(|mapping, _| {
            read_name_slot_at(mapping, self.layout.data_offset, self.layout.length, index)
        })
    }

    fn write_slot(&self, index: u32, bytes: Option<&[u8]>) -> Result<(), StatsError> {
        if index >= self.layout.length {
            return Err(StatsError::OutOfBounds);
        }
        let slot = self
            .layout
            .data_offset
            .get()
            .checked_add(
                u64::from(index)
                    .checked_mul(crate::descriptor::NAME_VECTOR_SLOT_BYTES as u64)
                    .ok_or(StatsError::OutOfBounds)?,
            )
            .ok_or(StatsError::OutOfBounds)?;
        self.with_mapping_value(|mapping, _| {
            let sequence = mapping.atomic_u64(Offset::new(slot))?;
            let length = mapping.atomic_u64(Offset::new(
                slot.checked_add(8).ok_or(StatsError::OutOfBounds)?,
            ))?;
            let current = sequence.load(Ordering::Acquire);
            if current & 1 != 0 {
                std::hint::spin_loop();
            }
            let next = current
                .checked_add(2)
                .ok_or(StatsError::GenerationOverflow)?;
            sequence.store(current | 1, Ordering::Relaxed);
            for word in 0..(crate::descriptor::NAME_VECTOR_MAX_BYTES / 8) {
                let start = word * 8;
                let mut bytes_word = [0u8; 8];
                if let Some(bytes) = bytes {
                    let end = (start + 8).min(bytes.len());
                    if start < end {
                        bytes_word[..end - start].copy_from_slice(&bytes[start..end]);
                    }
                }
                mapping
                    .atomic_u64(Offset::new(
                        slot.checked_add(16 + (word * 8) as u64)
                            .ok_or(StatsError::OutOfBounds)?,
                    ))?
                    .store(u64::from_le_bytes(bytes_word), Ordering::Relaxed);
            }
            length.store(
                bytes.map_or(0, |value| value.len() as u64),
                Ordering::Release,
            );
            sequence.store(next, Ordering::Release);
            Ok(())
        })
    }
}

fn read_name_slot_at(
    mapping: &Mapping,
    data_offset: Offset,
    length: u32,
    index: u32,
) -> Result<Option<String>, StatsError> {
    if index >= length {
        return Err(StatsError::OutOfBounds);
    }
    let slot = data_offset
        .get()
        .checked_add(
            u64::from(index)
                .checked_mul(crate::descriptor::NAME_VECTOR_SLOT_BYTES as u64)
                .ok_or(StatsError::OutOfBounds)?,
        )
        .ok_or(StatsError::OutOfBounds)?;
    for _ in 0..MAX_READ_ATTEMPTS {
        let sequence = mapping
            .atomic_u64(Offset::new(slot))?
            .load(Ordering::Acquire);
        if sequence & 1 != 0 {
            std::hint::spin_loop();
            continue;
        }
        let value_len = mapping
            .atomic_u64(Offset::new(
                slot.checked_add(8).ok_or(StatsError::OutOfBounds)?,
            ))?
            .load(Ordering::Acquire);
        if value_len as usize > crate::descriptor::NAME_VECTOR_MAX_BYTES {
            return Err(StatsError::InvalidDescriptor(
                "name vector slot length is invalid".to_owned(),
            ));
        }
        let mut bytes = Vec::with_capacity(value_len as usize);
        for word in 0..(crate::descriptor::NAME_VECTOR_MAX_BYTES / 8) {
            bytes.extend_from_slice(
                &mapping
                    .atomic_u64(Offset::new(
                        slot.checked_add(16 + (word * 8) as u64)
                            .ok_or(StatsError::OutOfBounds)?,
                    ))?
                    .load(Ordering::Relaxed)
                    .to_le_bytes(),
            );
        }
        let end = mapping
            .atomic_u64(Offset::new(slot))?
            .load(Ordering::Acquire);
        if end != sequence || end & 1 != 0 {
            std::hint::spin_loop();
            continue;
        }
        bytes.truncate(value_len as usize);
        if bytes.is_empty() {
            return Ok(None);
        }
        return String::from_utf8(bytes).map(Some).map_err(|_| {
            StatsError::InvalidDescriptor("name vector slot is not UTF-8".to_owned())
        });
    }
    Err(StatsError::ReadBusy)
}

/// A direct handle to a fixed 64-bin per-row log2 histogram.
impl MetricHandle<HistogramLog2Layout> {
    pub const fn rows(&self) -> u32 {
        self.layout.rows
    }

    pub const fn bins(&self) -> u32 {
        crate::descriptor::HISTOGRAM_BIN_COUNT
    }

    pub fn increment_bin(&self, row: u32, bin: u32, delta: u64) -> Result<(), StatsError> {
        self.with_bin(row, bin, |cell| {
            cell.fetch_add(delta, Ordering::Relaxed);
            Ok(())
        })
    }

    /// Increments the clamped log2 bin for `value`.
    pub fn increment_value(&self, row: u32, value: u32, delta: u64) -> Result<(), StatsError> {
        let bin = if value == 0 {
            0
        } else {
            (u32::BITS - 1 - value.leading_zeros()).min(crate::descriptor::HISTOGRAM_BIN_COUNT - 1)
        };
        self.increment_bin(row, bin, delta)
    }

    pub fn get(&self, row: u32, bin: u32) -> Result<u64, StatsError> {
        self.with_bin(row, bin, |cell| Ok(cell.load(Ordering::Relaxed)))
    }

    fn with_bin<T>(
        &self,
        row: u32,
        bin: u32,
        op: impl FnOnce(&AtomicU64) -> Result<T, StatsError>,
    ) -> Result<T, StatsError> {
        if row >= self.layout.rows || bin >= crate::descriptor::HISTOGRAM_BIN_COUNT {
            return Err(StatsError::OutOfBounds);
        }
        let offset = self
            .layout
            .data_offset
            .get()
            .checked_add(
                u64::from(row)
                    .checked_mul(self.layout.row_stride)
                    .and_then(|base| base.checked_add(u64::from(bin) * 8))
                    .ok_or(StatsError::OutOfBounds)?,
            )
            .ok_or(StatsError::OutOfBounds)?;
        self.with_mapping_value(|mapping, _| op(mapping.atomic_u64(Offset::new(offset))?))
    }
}

/// A direct fixed-size per-row overwrite ring buffer.
impl MetricHandle<RingBufferLayout> {
    pub const fn rows(&self) -> u32 {
        self.layout.rows
    }

    pub const fn capacity(&self) -> u32 {
        self.layout.capacity
    }

    pub const fn entry_size(&self) -> u32 {
        self.layout.entry_size
    }

    /// Copies one exact-size entry into the current producer slot and returns
    /// the newly published row sequence.
    pub fn produce(&self, row: u32, data: &[u8]) -> Result<u64, StatsError> {
        if row >= self.layout.rows || data.len() != self.layout.entry_size as usize {
            return Err(StatsError::OutOfBounds);
        }
        let metadata = self.metadata_offset_for(row)?;
        self.with_mapping_value(|mapping, _| {
            let head_cell = mapping.atomic_u64(metadata)?;
            let sequence_cell = mapping.atomic_u64(Offset::new(
                metadata
                    .get()
                    .checked_add(8)
                    .ok_or(StatsError::OutOfBounds)?,
            ))?;
            let publication_cell = mapping.atomic_u64(Offset::new(
                metadata
                    .get()
                    .checked_add(24)
                    .ok_or(StatsError::OutOfBounds)?,
            ))?;
            let head_word = head_cell.load(Ordering::Relaxed);
            let head = (head_word & u64::from(u32::MAX)) % u64::from(self.layout.capacity);
            let sequence = sequence_cell.load(Ordering::Relaxed);
            let publication = publication_cell.load(Ordering::Relaxed);
            if publication & 1 != 0 {
                return Err(StatsError::ReadBusy);
            }
            let next_sequence = sequence
                .checked_add(1)
                .ok_or(StatsError::GenerationOverflow)?;
            let next_publication = publication
                .checked_add(2)
                .ok_or(StatsError::GenerationOverflow)?;
            let slot = self.slot_offset(row, head as u32)?;
            let target = mapping.byte_write_target(slot, data.len())?;
            publication_cell.store(
                publication
                    .checked_add(1)
                    .ok_or(StatsError::GenerationOverflow)?,
                Ordering::Relaxed,
            );
            // SAFETY: `target` was bounds-checked before the publication
            // marker was set and the marker excludes stable readers.
            unsafe { Mapping::write_bytes(target, data) };
            let next_head = (head + 1) % u64::from(self.layout.capacity);
            head_cell.store(
                (head_word & !u64::from(u32::MAX)) | next_head,
                Ordering::Relaxed,
            );
            sequence_cell.store(next_sequence, Ordering::Release);
            publication_cell.store(next_publication, Ordering::Release);
            Ok(next_sequence)
        })
    }

    /// Reads one physical slot into owned bytes.
    pub fn get(&self, row: u32, slot: u32) -> Result<Vec<u8>, StatsError> {
        self.with_mapping_value(|mapping, _| self.read_slot(mapping, row, slot))
    }

    /// Reads the newest committed slot for a row, or `None` before its first
    /// publication.
    pub fn latest(&self, row: u32) -> Result<Option<Vec<u8>>, StatsError> {
        if row >= self.layout.rows {
            return Err(StatsError::OutOfBounds);
        }
        let metadata = self.metadata_offset_for(row)?;
        self.with_mapping_value(|mapping, _| {
            let head_cell = mapping.atomic_u64(metadata)?;
            let sequence_cell = mapping.atomic_u64(Offset::new(metadata.get() + 8))?;
            let publication_cell = mapping.atomic_u64(Offset::new(metadata.get() + 24))?;
            for _ in 0..MAX_READ_ATTEMPTS {
                let publication = publication_cell.load(Ordering::Acquire);
                if publication & 1 != 0 {
                    std::hint::spin_loop();
                    continue;
                }
                let sequence = sequence_cell.load(Ordering::Acquire);
                if sequence == 0 {
                    return Ok(None);
                }
                let head = (head_cell.load(Ordering::Acquire) & u64::from(u32::MAX))
                    % u64::from(self.layout.capacity);
                let slot = if head == 0 {
                    self.layout.capacity - 1
                } else {
                    head as u32 - 1
                };
                let value = self.read_slot(mapping, row, slot)?;
                let after_publication = publication_cell.load(Ordering::Acquire);
                let after_sequence = sequence_cell.load(Ordering::Acquire);
                if publication == after_publication
                    && after_publication & 1 == 0
                    && sequence == after_sequence
                {
                    return Ok(Some(value));
                }
                std::hint::spin_loop();
            }
            Err(StatsError::ReadBusy)
        })
    }

    /// Captures all physical slots for one row with a stable sequence.
    pub fn snapshot(&self, row: u32) -> Result<crate::read::RingBufferSnapshot, StatsError> {
        if row >= self.layout.rows {
            return Err(StatsError::OutOfBounds);
        }
        let metadata = self.metadata_offset_for(row)?;
        self.with_mapping_value(|mapping, _| {
            let sequence_cell = mapping.atomic_u64(Offset::new(metadata.get() + 8))?;
            let publication_cell = mapping.atomic_u64(Offset::new(metadata.get() + 24))?;
            for _ in 0..MAX_READ_ATTEMPTS {
                let before_publication = publication_cell.load(Ordering::Acquire);
                if before_publication & 1 != 0 {
                    std::hint::spin_loop();
                    continue;
                }
                let before_sequence = sequence_cell.load(Ordering::Acquire);
                let mut entries = Vec::with_capacity(self.layout.capacity as usize);
                for slot in 0..self.layout.capacity {
                    entries.push(self.read_slot(mapping, row, slot)?);
                }
                let after_publication = publication_cell.load(Ordering::Acquire);
                let after_sequence = sequence_cell.load(Ordering::Acquire);
                if before_publication == after_publication
                    && after_publication & 1 == 0
                    && before_sequence == after_sequence
                {
                    return Ok(crate::read::RingBufferSnapshot {
                        sequence: after_sequence,
                        entries,
                    });
                }
                std::hint::spin_loop();
            }
            Err(StatsError::ReadBusy)
        })
    }

    /// Copies the immutable registration schema, if one was supplied.
    pub fn schema(&self) -> Result<Option<Vec<u8>>, StatsError> {
        let Some(schema_offset) = self.layout.schema_offset else {
            return self.with_mapping_value(|_, _| Ok(None));
        };
        self.with_mapping_value(|mapping, _| {
            Ok(Some(
                mapping.read_bytes(schema_offset, self.layout.schema_size)?,
            ))
        })
    }

    fn metadata_offset_for(&self, row: u32) -> Result<Offset, StatsError> {
        self.layout
            .metadata_offset
            .checked_add(
                u64::from(row)
                    .checked_mul(crate::descriptor::RING_METADATA_BYTES as u64)
                    .ok_or(StatsError::OutOfBounds)?,
            )
            .ok_or(StatsError::OutOfBounds)
    }

    fn slot_offset(&self, row: u32, slot: u32) -> Result<Offset, StatsError> {
        ring_slot_offset(
            self.layout.data_offset,
            self.layout.slot_stride,
            self.layout.rows,
            self.layout.capacity,
            row,
            slot,
        )
    }

    fn read_slot(&self, mapping: &Mapping, row: u32, slot: u32) -> Result<Vec<u8>, StatsError> {
        let offset = self.slot_offset(row, slot)?;
        read_ring_slot_bytes(mapping, offset, self.layout.entry_size as usize)
    }
}

/// The stats segment: header, directory, and metric values in shared memory.
///
/// The segment is backed by one shared-memory mapping whose first page is
/// reserved for the versioned [`crate::header::StatsHeader`]. The directory
/// and every metric block are owned `SegmentAllocation`s; `&mut StatsMain`
/// is the sole structural writer, publishing each change atomically under
/// the header's `in_progress` sequence marker.
pub struct StatsMain {
    segment: Segment,
    /// Process-local name index mirroring VPP's `directory_vector_by_name`
    /// (stats.c:78-123,196): active name -> (index, generation).
    ///
    /// VPP keeps that hash in the process-local stats segment structure, not
    /// in shared memory; the authoritative name always lives in the
    /// directory entry inside the segment. This map is a rebuildable
    /// acceleration index giving O(1)-expected duplicate detection on add,
    /// kept in step on successful add and remove. It can be rebuilt from
    /// the segment by scanning active entries and is never read by readers
    /// of the shared segment.
    names: HashMap<Box<str>, EntryId>,
    /// Registered collector closures, run by [`StatsMain::collect`] in
    /// registration order.
    ///
    /// The `FnMut` bound makes `StatsMain` `!Sync`, matching its role: this
    /// handle is the sole structural writer of its segment, so any reader
    /// is an alias within one thread and needs no cross-thread locking.
    collectors: Vec<Box<dyn FnMut() -> Result<(), StatsError> + Send + 'static>>,
    /// The currently published directory allocation. It is kept in a vector
    /// so directory-vector replacement and retirement use the same process-
    /// local ownership discipline as metric blocks.
    directory_blocks: Vec<SegmentAllocation>,
    /// Old directory blocks stay owned for the lifetime of the segment. A
    /// reader may have captured the previous offset before it observes the
    /// publication epoch, so freeing immediately after the header switch
    /// would permit a use-after-free during that reader's re-check window.
    retired_directories: Vec<SegmentAllocation>,
    /// Active metric allocations indexed by the VPP-compatible directory slot.
    metric_blocks: Vec<Option<SegmentAllocation>>,
    /// Replaced or removed metric blocks stay owned until this StatsMain is
    /// dropped. This protects readers that began before a publication and
    /// keeps direct handles free of allocation ownership and refcounts.
    retired_metric_blocks: Vec<SegmentAllocation>,
}

impl StatsMain {
    /// Creates a stats segment of [`DEFAULT_CAPACITY`] bytes.
    pub fn new() -> Result<StatsMain, StatsError> {
        StatsMain::with_capacity(DEFAULT_CAPACITY)
    }

    /// Creates a stats segment of at least `capacity` bytes, page-rounded.
    ///
    /// The capacity must hold the reserved first page, the shared header,
    /// the initial directory, and at least one metric; smaller requests are
    /// rejected with [`StatsError::CapacityTooSmall`].
    pub fn with_capacity(capacity: usize) -> Result<StatsMain, StatsError> {
        let page = hammer_infra::page_size()?;
        let minimum = page + MIN_TAIL_BYTES;
        if capacity < minimum {
            return Err(StatsError::CapacityTooSmall {
                minimum,
                requested: capacity,
            });
        }
        if page == 0 || !page.is_power_of_two() {
            return Err(StatsError::InvalidLayout);
        }
        let total = capacity
            .checked_next_multiple_of(page)
            .ok_or(StatsError::OutOfBounds)?;
        let segment = Segment::shared_with_reserved_prefix(&unique_segment_name(), total, page)?;

        // The initial directory is the first arena allocation, so it lands
        // directly after the reserved first page, 64-byte aligned.
        let directory_layout =
            Layout::from_size_align((INITIAL_DIRECTORY_SLOTS as usize) * SLOT_SIZE, 64)
                .map_err(|_| StatsError::InvalidLayout)?;
        let directory = segment.allocate(directory_layout)?;
        let directory_offset = Offset::new(directory.offset());

        let mapping = Mapping::new(&segment);
        mapping.write_header(StatsHeader::new(
            total as u64,
            directory_offset.get(),
            INITIAL_DIRECTORY_SLOTS,
        ));

        Ok(StatsMain {
            segment,
            names: HashMap::new(),
            collectors: Vec::new(),
            directory_blocks: vec![directory],
            retired_directories: Vec::new(),
            metric_blocks: Vec::new(),
            retired_metric_blocks: Vec::new(),
        })
    }

    /// Registers a batch of protocol-neutral Stats Directory entries.
    ///
    /// Each descriptor, directory slot, and value block is published through
    /// one VPP-style registration boundary. A failed allocation leaves the
    /// current directory and header unchanged for that entry.
    pub fn register<'a>(
        &mut self,
        registrations: &[StatsRegistration<'a>],
    ) -> Result<Vec<StatsEntry>, StatsError> {
        let mut seen_paths = HashSet::with_capacity(registrations.len());
        let mut prepared = Vec::with_capacity(registrations.len());
        for registration in registrations {
            let kind = RegistrationKind::Value(registration.value);
            encode_name(registration.path)?;
            if self.names.contains_key(registration.path) || !seen_paths.insert(registration.path) {
                return Err(StatsError::DuplicateName(registration.path.to_owned()));
            }
            let prometheus_type = match registration.value {
                StatsValueLayout::Counter
                | StatsValueLayout::CounterVectorSimple { .. }
                | StatsValueLayout::CounterVectorCombined { .. }
                | StatsValueLayout::HistogramLog2 { .. }
                | StatsValueLayout::RingBuffer { .. } => PrometheusType::Counter,
                StatsValueLayout::Gauge
                | StatsValueLayout::Timestamp
                | StatsValueLayout::NameVector { .. } => PrometheusType::Gauge,
            };
            let mut opts = prometheus::Opts::new(
                registration.descriptor.fq_name,
                registration.descriptor.help,
            );
            for &(name, value) in registration.descriptor.const_labels {
                opts = opts.const_label(name, value);
            }
            let descriptor = crate::descriptor::normalize(&opts, prometheus_type)?;
            prepared.push((registration.path, kind, descriptor));
        }

        self.register_batch(prepared)?
            .into_iter()
            .map(|entry| match entry {
                RegisteredEntry::Handle(handle) => Ok(handle),
                RegisteredEntry::Symlink(_) => {
                    Err(StatsError::InvalidState(DirectoryType::SYMLINK))
                }
            })
            .collect()
    }

    /// Registers VPP-compatible directory symlinks in one publication batch.
    ///
    /// Each tuple contains `(path, descriptor, target, vector_index)`. Links
    /// have no public value handle; callers retain only the returned entry ids
    /// for list/dump operations. The target must already be an active fixed
    /// vector value, matching VPP's `vlib_stats_add_symlink` contract.
    pub fn register_symlinks<'a>(
        &mut self,
        registrations: &[(&'a str, StatsDescriptor<'a>, &'a str, u32)],
    ) -> Result<Vec<EntryId>, StatsError> {
        let mut seen_paths = HashSet::with_capacity(registrations.len());
        let mapping = Mapping::new(&self.segment);
        let mut prepared = Vec::with_capacity(registrations.len());
        for &(path, descriptor, target, vector_index) in registrations {
            encode_name(path)?;
            if self.names.contains_key(path) || !seen_paths.insert(path) {
                return Err(StatsError::DuplicateName(path.to_owned()));
            }
            let target_runtime = self.resolve_target_runtime(&mapping, target, vector_index)?;
            let mut opts = prometheus::Opts::new(descriptor.fq_name, descriptor.help);
            for &(name, value) in descriptor.const_labels {
                opts = opts.const_label(name, value);
            }
            let descriptor = crate::descriptor::normalize(&opts, target_runtime.prometheus_type)?;
            prepared.push((
                path,
                RegistrationKind::Symlink {
                    target: target_runtime.id,
                    vector_index,
                },
                descriptor,
            ));
        }

        self.register_batch(prepared)?
            .into_iter()
            .map(|entry| match entry {
                RegisteredEntry::Symlink(id) => Ok(id),
                RegisteredEntry::Handle(_) => Err(StatsError::InvalidState(DirectoryType::SYMLINK)),
            })
            .collect()
    }

    fn resolve_target_runtime(
        &self,
        mapping: &Mapping,
        target: &str,
        vector_index: u32,
    ) -> Result<TargetRuntimeInfo, StatsError> {
        let id = *self.names.get(target).ok_or_else(|| {
            StatsError::InvalidDescriptor(format!("symlink target {target:?} was not found"))
        })?;
        let header = mapping.header();
        let slot = mapping.entry(header.directory_offset(), id.index)?;
        if slot.state()? != EntryState::Active || slot.generation() != id.generation {
            return Err(StatsError::InvalidDescriptor(format!(
                "symlink target {target:?} is not active"
            )));
        }
        let directory_type = slot.directory_type()?;
        let prometheus_type = slot.prometheus_type()?;
        let block = mapping.descriptor_block(slot.descriptor_offset())?;
        let columns = match directory_type {
            DirectoryType::ScalarIndex | DirectoryType::Gauge => 1,
            DirectoryType::CounterVectorSimple => {
                crate::descriptor::decode_counter_vector_simple_layout(block)?.columns
            }
            DirectoryType::CounterVectorCombined => {
                crate::descriptor::decode_counter_vector_combined_layout(block)?.columns
            }
            DirectoryType::NameVector => {
                crate::descriptor::decode_name_vector_layout(block)?.length
            }
            DirectoryType::HistogramLog2 => {
                crate::descriptor::decode_histogram_log2_layout(block)?.bins
            }
            DirectoryType::Symlink | DirectoryType::RingBuffer | DirectoryType::Empty => {
                return Err(StatsError::InvalidDescriptor(
                    "symlink target must be a fixed vector value".to_owned(),
                ));
            }
            DirectoryType::Illegal => return Err(StatsError::InvalidState(DirectoryType::ILLEGAL)),
        };
        if vector_index >= columns {
            return Err(StatsError::InvalidDescriptor(format!(
                "symlink target {target:?} index {vector_index} is outside 0..{columns}"
            )));
        }
        Ok(TargetRuntimeInfo {
            id,
            prometheus_type,
        })
    }

    fn register_batch<'a>(
        &mut self,
        prepared: Vec<(
            &'a str,
            RegistrationKind<'a>,
            crate::descriptor::NormalizedDescriptor,
        )>,
    ) -> Result<Vec<RegisteredEntry>, StatsError> {
        if prepared.is_empty() {
            return Ok(Vec::new());
        }

        let mut registered_entries = Vec::with_capacity(prepared.len());

        for (path, kind, descriptor) in prepared {
            let mapping = Mapping::new(&self.segment);
            let header = mapping.header();
            let old_directory_offset = header.directory_offset();
            let old_directory_capacity = header.directory_capacity();
            let initialized = header.initialized_len();
            if old_directory_capacity == 0 || initialized > old_directory_capacity {
                return Err(StatsError::InvalidState(0xFF));
            }

            let current_free_head = header.free_list_head();
            let (index, generation, free_head, next_initialized, next_capacity) =
                if current_free_head != NULL_INDEX {
                    if current_free_head >= old_directory_capacity {
                        return Err(StatsError::InvalidState(0xFF));
                    }
                    let free_index =
                        u32::try_from(current_free_head).map_err(|_| StatsError::OutOfBounds)?;
                    let free_entry = mapping.entry(old_directory_offset, free_index)?;
                    if free_entry.state()? != EntryState::Free {
                        return Err(StatsError::InvalidState(free_entry.state_byte()));
                    }
                    (
                        free_index,
                        free_entry.generation(),
                        free_entry.link(),
                        initialized,
                        old_directory_capacity,
                    )
                } else {
                    if initialized >= u64::from(u32::MAX) {
                        return Err(StatsError::OutOfBounds);
                    }
                    let next_initialized =
                        initialized.checked_add(1).ok_or(StatsError::OutOfBounds)?;
                    let next_capacity = if initialized >= old_directory_capacity {
                        old_directory_capacity
                            .checked_mul(2)
                            .ok_or(StatsError::OutOfBounds)?
                    } else {
                        old_directory_capacity
                    };
                    (
                        initialized as u32,
                        1,
                        NULL_INDEX,
                        next_initialized,
                        next_capacity,
                    )
                };

            let directory_grew = next_capacity != old_directory_capacity;
            let directory_allocation = if directory_grew {
                let new_capacity =
                    usize::try_from(next_capacity).map_err(|_| StatsError::OutOfBounds)?;
                let new_bytes = new_capacity
                    .checked_mul(SLOT_SIZE)
                    .ok_or(StatsError::OutOfBounds)?;
                let layout = Layout::from_size_align(new_bytes, 64)
                    .map_err(|_| StatsError::InvalidLayout)?;
                Some(self.segment.allocate(layout)?)
            } else {
                None
            };
            let directory_offset = directory_allocation
                .as_ref()
                .map(|allocation| Offset::new(allocation.offset()))
                .unwrap_or(old_directory_offset);

            if let Some(allocation) = directory_allocation.as_ref() {
                let initialized_count =
                    u32::try_from(initialized).map_err(|_| StatsError::OutOfBounds)?;
                let mut old_entries = Vec::with_capacity(initialized_count as usize);
                for old_index in 0..initialized_count {
                    old_entries.push(mapping.entry(old_directory_offset, old_index)?);
                }
                mapping.write_directory_entries(Offset::new(allocation.offset()), &old_entries)?;
            }

            let id = EntryId { index, generation };
            let (entry, registered, allocation) = match kind {
                RegistrationKind::Value(value) => match value {
                    StatsValueLayout::Counter => {
                        CounterLayout::register(&self.segment, path, id, &descriptor, value)?
                    }
                    StatsValueLayout::Gauge => {
                        GaugeLayout::register(&self.segment, path, id, &descriptor, value)?
                    }
                    StatsValueLayout::Timestamp => {
                        TimestampLayout::register(&self.segment, path, id, &descriptor, value)?
                    }
                    StatsValueLayout::CounterVectorSimple { .. } => {
                        CounterVectorSimpleLayout::register(
                            &self.segment,
                            path,
                            id,
                            &descriptor,
                            value,
                        )?
                    }
                    StatsValueLayout::CounterVectorCombined { .. } => {
                        CounterVectorCombinedLayout::register(
                            &self.segment,
                            path,
                            id,
                            &descriptor,
                            value,
                        )?
                    }
                    StatsValueLayout::NameVector { .. } => {
                        NameVectorLayout::register(&self.segment, path, id, &descriptor, value)?
                    }
                    StatsValueLayout::HistogramLog2 { .. } => {
                        HistogramLog2Layout::register(&self.segment, path, id, &descriptor, value)?
                    }
                    StatsValueLayout::RingBuffer { .. } => {
                        RingBufferLayout::register(&self.segment, path, id, &descriptor, value)?
                    }
                },
                RegistrationKind::Symlink {
                    target,
                    vector_index,
                } => {
                    let layout = crate::descriptor::block_layout(&descriptor)?;
                    let mut allocation = self.segment.allocate(layout)?;
                    let value_offset_in_block = crate::descriptor::write_block(
                        &mut allocation.bytes_mut(),
                        &descriptor,
                        id.generation,
                    )?;
                    let value_offset = block_offset(&allocation, value_offset_in_block)?;
                    let entry = DirectorySlot::new_symlink(
                        encode_name(path)?,
                        id.generation,
                        descriptor.kind,
                        Offset::new(allocation.offset()),
                        value_offset,
                        target.index,
                        target.generation,
                        vector_index,
                    );
                    (entry, RegisteredEntry::Symlink(id), allocation)
                }
            };

            let target = mapping.entry_write_target(directory_offset, index)?;
            if directory_grew {
                let old_directory = self
                    .directory_blocks
                    .last()
                    .ok_or(StatsError::InvalidState(0xFF))?;
                if old_directory.offset() != old_directory_offset.get() {
                    return Err(StatsError::InvalidState(0xFF));
                }
            }
            let planned_len =
                usize::try_from(next_initialized).map_err(|_| StatsError::OutOfBounds)?;
            if self.metric_blocks.len() < planned_len {
                self.metric_blocks.resize_with(planned_len, || None);
            }
            if self
                .metric_blocks
                .get(index as usize)
                .ok_or(StatsError::OutOfBounds)?
                .is_some()
            {
                return Err(StatsError::InvalidState(0xFF));
            }

            let old_directory_allocation = if directory_grew {
                match self.directory_blocks.pop() {
                    Some(allocation) => Some(allocation),
                    None => return Err(StatsError::InvalidState(0xFF)),
                }
            } else {
                None
            };

            if directory_grew {
                // The replacement directory is unreachable until the header
                // switches to its offset, so its new slot can be populated now.
                unsafe { mapping.write_entry(target, entry) };
            }

            if directory_grew {
                let new_directory = match directory_allocation {
                    Some(allocation) => allocation,
                    None => return Err(StatsError::InvalidState(0xFF)),
                };
                self.directory_blocks.push(new_directory);
                if let Some(old_directory) = old_directory_allocation {
                    self.retired_directories.push(old_directory);
                }
            }
            self.names.insert(path.into(), id);
            self.metric_blocks[index as usize] = Some(allocation);

            header.mark_in_progress();
            if directory_grew {
                header.store_directory_offset(directory_offset);
                header.store_directory_capacity(next_capacity);
            } else {
                // SAFETY: the slot target was bounds-checked immediately
                // before this publication tail.
                unsafe { mapping.write_entry(target, entry) };
            }
            header.store_free_list_head(free_head);
            header.store_initialized_len(next_initialized);
            header.bump_epoch();
            header.clear_in_progress();

            registered_entries.push(registered);
        }

        Ok(registered_entries)
    }

    /// Replaces an existing simple counter vector with a checked layout of
    /// `rows × columns`, preserving the overlap of the old values. The slot
    /// remains at the same directory index, while its generation advances so
    /// older direct handles become stale when VPP-style vector replacement
    /// repoints the entry to the new block.
    ///
    /// StatsMain retains the old allocation until it is dropped. This keeps
    /// the non-owning handle and reader paths independent of block ownership
    /// while matching VPP's validate-and-repoint directory boundary.
    pub fn replace_counter_vector_simple(
        &mut self,
        path: &str,
        rows: u32,
        columns: u32,
    ) -> Result<(EntryId, CounterVectorSimple), StatsError> {
        if rows == 0 || columns == 0 {
            return Err(StatsError::InvalidDescriptor(
                "counter vector dimensions must be non-zero".to_owned(),
            ));
        }
        let id = *self.names.get(path).ok_or_else(|| {
            StatsError::InvalidDescriptor(format!("counter vector path {path:?} was not found"))
        })?;
        let name = encode_name(path)?;
        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        if u64::from(id.index) >= header.initialized_len() {
            return Err(StatsError::NotFound { id });
        }
        let directory_offset = header.directory_offset();
        let slot = mapping.entry(directory_offset, id.index)?;
        if slot.state()? != EntryState::Active || slot.generation() != id.generation {
            return Err(StatsError::StaleEntry { id });
        }
        let directory_type = slot.directory_type()?;
        let prometheus_type = slot.prometheus_type()?;
        if directory_type != DirectoryType::CounterVectorSimple
            || prometheus_type != PrometheusType::Counter
        {
            return Err(StatsError::IncompatibleType {
                id,
                prometheus_type,
                directory_type,
            });
        }

        let active_block = self
            .metric_blocks
            .get(id.index as usize)
            .and_then(Option::as_ref)
            .ok_or(StatsError::InvalidState(0xFF))?;
        if active_block.offset() != slot.descriptor_offset().get() {
            return Err(StatsError::InvalidState(0xFF));
        }
        let next_generation = id
            .generation
            .checked_add(1)
            .ok_or(StatsError::GenerationOverflow)?;
        let old_layout = CounterVectorSimpleLayout::decode(&mapping, &slot)?;
        let decoded = crate::descriptor::decode_descriptor(
            mapping.descriptor_block(slot.descriptor_offset())?,
        )?;
        let descriptor = crate::descriptor::NormalizedDescriptor {
            kind: PrometheusType::Counter,
            fq_name: decoded.fq_name,
            help: decoded.help,
            labels: decoded
                .const_labels
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
        };
        let new_layout =
            crate::descriptor::counter_vector_simple_block_layout(&descriptor, rows, columns)?;
        let mut allocation = self.segment.allocate(new_layout)?;
        let (value_offset_in_block, data_offset_in_block) =
            crate::descriptor::write_counter_vector_simple_block(
                &mut allocation.bytes_mut(),
                &descriptor,
                next_generation,
                rows,
                columns,
            )?;
        let block_offset = Offset::new(allocation.offset());
        let value_offset = block_offset
            .checked_add(value_offset_in_block)
            .ok_or(StatsError::OutOfBounds)?;
        let data_offset = block_offset
            .checked_add(data_offset_in_block)
            .ok_or(StatsError::OutOfBounds)?;
        let new_row_stride = u64::try_from(crate::descriptor::counter_vector_row_stride(
            columns,
            std::mem::size_of::<AtomicU64>(),
        )?)
        .map_err(|_| StatsError::OutOfBounds)?;
        let old_data_offset = old_layout.data_offset;
        let overlap_rows = rows.min(old_layout.rows);
        let overlap_columns = columns.min(old_layout.columns);
        for row in 0..overlap_rows {
            for column in 0..overlap_columns {
                let old_offset = old_data_offset
                    .get()
                    .checked_add(
                        u64::from(row)
                            .checked_mul(old_layout.row_stride as u64)
                            .and_then(|base| base.checked_add(u64::from(column) * 8))
                            .ok_or(StatsError::OutOfBounds)?,
                    )
                    .ok_or(StatsError::OutOfBounds)?;
                let new_offset = data_offset
                    .get()
                    .checked_add(
                        u64::from(row)
                            .checked_mul(new_row_stride)
                            .and_then(|base| base.checked_add(u64::from(column) * 8))
                            .ok_or(StatsError::OutOfBounds)?,
                    )
                    .ok_or(StatsError::OutOfBounds)?;
                let value = mapping
                    .atomic_u64(Offset::new(old_offset))?
                    .load(Ordering::Relaxed);
                mapping
                    .atomic_u64(Offset::new(new_offset))?
                    .store(value, Ordering::Relaxed);
            }
        }

        let new_id = EntryId {
            index: id.index,
            generation: next_generation,
        };
        let new_entry = DirectorySlot::new_active(
            name,
            next_generation,
            DirectoryType::CounterVectorSimple,
            PrometheusType::Counter,
            block_offset,
            value_offset,
        );
        let vector_target = mapping.entry_write_target(directory_offset, id.index)?;
        let initialized = u32::try_from(header.initialized_len().min(u64::from(u32::MAX)))
            .map_err(|_| StatsError::OutOfBounds)?;
        let mut symlink_targets = Vec::new();
        for index in 0..initialized {
            let candidate = mapping.entry(directory_offset, index)?;
            if candidate.state()? != EntryState::Active
                || candidate.directory_type()? != DirectoryType::Symlink
                || candidate.symlink_target_index()? != id.index
                || candidate.symlink_target_generation() != id.generation
            {
                continue;
            }
            let vector_index = candidate.symlink_vector_index()?;
            if vector_index >= columns {
                return Err(StatsError::InvalidDescriptor(format!(
                    "symlink target {path:?} index {vector_index} is outside 0..{columns}"
                )));
            }
            let mut updated = candidate;
            updated.set_symlink_target(id.index, next_generation, vector_index);
            symlink_targets.push((
                mapping.entry_write_target(directory_offset, index)?,
                updated,
            ));
        }

        // Move the old block to the retired set and install the new active
        // owner before publication, so every published block remains owned by
        // StatsMain throughout the directory switch.
        let old_allocation = {
            let owner = self
                .metric_blocks
                .get_mut(id.index as usize)
                .ok_or(StatsError::OutOfBounds)?;
            let old = owner.take().ok_or(StatsError::InvalidState(0xFF))?;
            *owner = Some(allocation);
            old
        };

        // VPP's stats publication uses one structural sequence around the
        // directory writes; all checked and fallible work is complete above.
        header.mark_in_progress();
        // SAFETY: both write targets were bounds-checked before publication.
        unsafe { mapping.write_entry(vector_target, new_entry) };
        for (target, entry) in symlink_targets {
            // SAFETY: each target was bounds-checked before publication.
            unsafe { mapping.write_entry(target, entry) };
        }
        header.bump_epoch();
        header.clear_in_progress();

        self.names.insert(path.into(), new_id);
        self.retired_metric_blocks.push(old_allocation);
        Ok((
            new_id,
            MetricHandle::new(
                self.segment.clone(),
                new_id,
                CounterVectorSimpleLayout {
                    data_offset,
                    row_stride: new_row_stride,
                    rows,
                    columns,
                },
            ),
        ))
    }

    /// Adds a counter metric, mirroring VPP's `vlib_stats_add_counter_vector`.
    ///
    /// Publishes a directory entry and returns an [`EntryId`] plus a
    /// direct [`Counter`] handle. The `Opts` must carry a
    /// valid fq name and help; variable labels are rejected. The entry is a
    /// scalar (`DirectoryType::ScalarIndex`), as are VPP's `/sys` heartbeat,
    /// boottime, and last-stats-clear metrics (stats.h:29-31, stats.c:281).
    pub fn add_counter(
        &mut self,
        path: &str,
        opts: prometheus::Opts,
    ) -> Result<(EntryId, Counter), StatsError> {
        let id = self.add_metric(
            path,
            &opts,
            PrometheusType::Counter,
            DirectoryType::ScalarIndex,
        )?;
        Ok((
            id,
            MetricHandle::new(self.segment.clone(), id, CounterLayout),
        ))
    }

    /// Adds a gauge metric, mirroring VPP's `vlib_stats_add_gauge`.
    ///
    /// Same contract as [`StatsMain::add_counter`]; the returned [`Gauge`]
    /// stores an `f64` value.
    pub fn add_gauge(
        &mut self,
        path: &str,
        opts: prometheus::Opts,
    ) -> Result<(EntryId, Gauge), StatsError> {
        let id = self.add_metric(path, &opts, PrometheusType::Gauge, DirectoryType::Gauge)?;
        Ok((id, MetricHandle::new(self.segment.clone(), id, GaugeLayout)))
    }

    /// Adds a timestamp scalar, mirroring VPP's `/sys` boottime, heartbeat,
    /// and last-stats-clear metrics (stats.h:29-31, stats.c:281).
    ///
    /// The metric is a Prometheus gauge whose value is a plain integer
    /// (`PrometheusType::Gauge` with `DirectoryType::ScalarIndex`).
    pub fn add_timestamp(
        &mut self,
        path: &str,
        opts: prometheus::Opts,
    ) -> Result<(EntryId, Timestamp), StatsError> {
        let id = self.add_metric(
            path,
            &opts,
            PrometheusType::Gauge,
            DirectoryType::ScalarIndex,
        )?;
        Ok((
            id,
            MetricHandle::new(self.segment.clone(), id, TimestampLayout),
        ))
    }

    /// Registers a collector closure, run by [`StatsMain::collect`].
    ///
    /// Collectors capture the metric handles they update (`Counter`,
    /// `Gauge`, `Timestamp`) — the update capability VPP gives a collector
    /// through its entry index (stats.c:590-604).
    ///
    /// Complexity: O(1) amortized plus one box.
    pub fn register_collector(
        &mut self,
        collector: impl FnMut() -> Result<(), StatsError> + Send + 'static,
    ) {
        self.collectors.push(Box::new(collector));
    }

    /// Runs every registered collector once, in registration order.
    ///
    /// No directory, epoch, or allocation work: collectors update their
    /// captured handles directly, as in VPP's `do_stat_segment_updates`
    /// pass (collector.c:135-158). Every collector runs even when an
    /// earlier one failed; the first error in registration order is
    /// returned once the pass completes.
    ///
    /// Complexity: O(collectors), no allocation.
    pub fn collect(&mut self) -> Result<(), StatsError> {
        let mut first_error = None;
        for collector in &mut self.collectors {
            if let Err(error) = collector() {
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Returns owned copies of the active directory entries whose paths
    /// match any of `patterns` (empty patterns select all; OR semantics),
    /// in ascending directory-index order.
    ///
    /// The patterns are compiled once before any segment read; an invalid
    /// pattern is a typed [`StatsError::InvalidPattern`] carrying the exact
    /// pattern and the regex [`std::error::Error`] as its source. Each
    /// entry's descriptor block is bounds-checked and decoded into owned
    /// strings, so no borrowed data escapes the call.
    ///
    /// The stability loop mirrors VPP's client protocol
    /// (stat_client.c:370-404, 429): `in_progress` and the epoch bracket
    /// the owned-copy build, and a bounded retry replaces VPP's "Epoch
    /// changed while reading, invalid results" failure with
    /// [`StatsError::ReadBusy`]. `StatsMain` is `!Sync`, so a reader is
    /// always an alias within the writing thread and cannot race a
    /// structural write; the epoch check guards against republish windows
    /// that would make the copy internally inconsistent.
    ///
    /// Complexity: O(patterns compiled) + O(initialized slots x patterns) +
    /// O(copied descriptor bytes); the result is fully owned.
    pub fn list(&self, patterns: &[String]) -> Result<Vec<DirectoryEntry>, StatsError> {
        let regexes: Vec<Regex> = patterns
            .iter()
            .map(|pattern| {
                Regex::new(pattern).map_err(|error| StatsError::InvalidPattern {
                    pattern: pattern.clone(),
                    source: error,
                })
            })
            .collect::<Result<_, _>>()?;

        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let mut result: Vec<DirectoryEntry> = Vec::new();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let epoch = header.epoch();
            if header.in_progress() != 0 {
                if attempts >= MAX_READ_ATTEMPTS {
                    return Err(StatsError::ReadBusy);
                }
                std::hint::spin_loop();
                continue;
            }

            let directory_offset = header.directory_offset();
            let initialized_len = header.initialized_len();
            if initialized_len > header.directory_capacity() {
                return Err(StatsError::InvalidState(0xFF));
            }
            let initialized =
                u32::try_from(initialized_len).map_err(|_| StatsError::OutOfBounds)?;
            result.clear();
            for index in 0..initialized {
                let slot = mapping.entry(directory_offset, index)?;
                if slot.state()? != EntryState::Active {
                    continue;
                }
                let path = std::str::from_utf8(slot.name())
                    .map_err(|_| StatsError::InvalidState(slot.state_byte()))?
                    .to_owned();
                let matched = regexes.is_empty() || regexes.iter().any(|re| re.is_match(&path));
                if !matched {
                    continue;
                }
                let decoded = crate::descriptor::decode_descriptor(
                    mapping.descriptor_block(slot.descriptor_offset())?,
                )?;
                result.push(DirectoryEntry {
                    id: EntryId {
                        index,
                        generation: slot.generation(),
                    },
                    path,
                    directory_type: slot.directory_type()?,
                    prometheus_type: slot.prometheus_type()?,
                    fq_name: decoded.fq_name,
                    help: decoded.help,
                    const_labels: decoded.const_labels,
                });
            }

            // The acquire fence orders the copied reads before the epoch
            // re-check. The snapshot at the loop top is an acquire read, so
            // the copy stays between the two checks; the re-check is the
            // read side of the writer's release clear
            // (`clear_in_progress`, the analogue of VPP's
            // `__atomic_store_n (&in_progress, 0, __ATOMIC_RELEASE)`,
            // stats.c:49), so a publication observed by the re-check is
            // fully visible and any overlap discards the copy instead of
            // returning it. The writer's `mark_in_progress` is a seq_cst
            // store — the begin boundary VPP's structural spinlock
            // supplies — so no structural write can become visible before
            // the marker.
            std::sync::atomic::fence(Ordering::Acquire);
            if header.in_progress() == 0 && header.epoch() == epoch {
                return Ok(result);
            }
            if attempts >= MAX_READ_ATTEMPTS {
                return Err(StatsError::ReadBusy);
            }
            std::hint::spin_loop();
        }
    }

    /// Returns owned point-in-time copies of the entries named by `ids`,
    /// preserving input order and duplicates.
    ///
    /// Each id is validated against the directory (index bounds, slot
    /// state, entry generation) and the value record (generation), so
    /// missing and stale ids stay typed
    /// [`StatsError::NotFound`]/[`StatsError::StaleEntry`]. The type pair
    /// is decoded into the [`DumpValue`] for the metric:
    /// Counter+ScalarIndex -> `u64`, Gauge+ScalarIndex (a timestamp) ->
    /// `u64 as f64`, Gauge+Gauge -> `f64::from_bits`; any other combination
    /// is a typed [`StatsError::IncompatibleType`].
    ///
    /// Same stable-epoch protocol as [`StatsMain::list`]; no descriptor
    /// parse and no collector work.
    ///
    /// Complexity: O(ids).
    pub fn dump(&self, ids: &[EntryId]) -> Result<Vec<DumpEntry>, StatsError> {
        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let mut result: Vec<DumpEntry> = Vec::new();
        let mut attempts = 0;
        loop {
            attempts += 1;
            let epoch = header.epoch();
            if header.in_progress() != 0 {
                if attempts >= MAX_READ_ATTEMPTS {
                    return Err(StatsError::ReadBusy);
                }
                std::hint::spin_loop();
                continue;
            }

            let directory_offset = header.directory_offset();
            let initialized = header.initialized_len();
            if initialized > header.directory_capacity() {
                return Err(StatsError::InvalidState(0xFF));
            }
            result.clear();
            for &id in ids {
                if u64::from(id.index) >= initialized {
                    return Err(StatsError::NotFound { id });
                }
                let slot = mapping.entry(directory_offset, id.index)?;
                if slot.state()? != EntryState::Active {
                    return Err(StatsError::NotFound { id });
                }
                if slot.generation() != id.generation {
                    return Err(StatsError::StaleEntry { id });
                }
                // Active-generation invariant: the slot, the id, and the
                // value record all carry exactly the same generation
                // (removal advances slot and record together; reuse keeps
                // them equal), so a mismatch means the entry changed under
                // the read.
                let record = mapping.metric_value(slot.value_offset())?;
                if record.generation() != id.generation {
                    return Err(StatsError::StaleEntry { id });
                }
                let prometheus_type = slot.prometheus_type()?;
                let directory_type = slot.directory_type()?;
                let (value_slot, value_directory_type, value_prometheus_type, vector_index) =
                    if directory_type == DirectoryType::Symlink {
                        let target_index = slot.symlink_target_index()?;
                        if u64::from(target_index) >= initialized {
                            return Err(StatsError::NotFound {
                                id: EntryId {
                                    index: target_index,
                                    generation: slot.symlink_target_generation(),
                                },
                            });
                        }
                        let target = mapping.entry(directory_offset, target_index)?;
                        if target.state()? != EntryState::Active
                            || target.generation() != slot.symlink_target_generation()
                        {
                            return Err(StatsError::StaleEntry {
                                id: EntryId {
                                    index: target_index,
                                    generation: slot.symlink_target_generation(),
                                },
                            });
                        }
                        (
                            target,
                            target.directory_type()?,
                            target.prometheus_type()?,
                            Some(slot.symlink_vector_index()?),
                        )
                    } else {
                        (slot, directory_type, prometheus_type, None)
                    };
                let target_record = mapping.metric_value(value_slot.value_offset())?;
                if target_record.generation()
                    != if directory_type == DirectoryType::Symlink {
                        value_slot.generation()
                    } else {
                        id.generation
                    }
                {
                    return Err(StatsError::StaleEntry { id });
                }
                let dump_value = read_dump_value(
                    &mapping,
                    id,
                    &value_slot,
                    value_prometheus_type,
                    value_directory_type,
                    vector_index,
                )?;
                result.push(DumpEntry {
                    id,
                    path: std::str::from_utf8(slot.name())
                        .map_err(|_| StatsError::InvalidState(slot.state_byte()))?
                        .to_owned(),
                    directory_type,
                    prometheus_type,
                    value: dump_value,
                });
            }

            // Same acquire fence as `list`: the copy stays between the
            // acquire snapshot and this re-check, whose zero read
            // synchronizes with the writer's release clear (the seq_cst
            // mark supplies the begin boundary on the writer side).
            std::sync::atomic::fence(Ordering::Acquire);
            if header.in_progress() == 0 && header.epoch() == epoch {
                return Ok(result);
            }
            if attempts >= MAX_READ_ATTEMPTS {
                return Err(StatsError::ReadBusy);
            }
            std::hint::spin_loop();
        }
    }

    /// Removes the entry identified by `id`, mirroring VPP's
    /// `vlib_stats_remove_entry`.
    ///
    /// The entry is hidden, its generation advances, and the slot joins the
    /// free list. The detached metric block remains in the retired allocation
    /// set until `StatsMain` drops, protecting readers and offset-free handles.
    pub fn remove_entry(&mut self, id: EntryId) -> Result<(), StatsError> {
        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();
        let capacity = header.directory_capacity();
        if u64::from(id.index) >= capacity {
            return Err(StatsError::NotFound { id });
        }
        let entry = mapping.entry(directory_offset, id.index)?;
        match entry.state()? {
            EntryState::Active => {}
            EntryState::Free | EntryState::Removed => {
                return Err(StatsError::NotFound { id });
            }
        }
        if entry.generation() != id.generation {
            return Err(StatsError::StaleEntry { id });
        }
        let active_block = self
            .metric_blocks
            .get(id.index as usize)
            .and_then(Option::as_ref)
            .ok_or(StatsError::InvalidState(0xFF))?;
        if active_block.offset() != entry.descriptor_offset().get() {
            return Err(StatsError::InvalidState(0xFF));
        }

        // Preparation: compute the next slot generation and every checked
        // write target before the publication tail.
        let next_generation = entry
            .generation()
            .checked_add(1)
            .ok_or(StatsError::GenerationOverflow)?;
        let target = mapping.entry_write_target(directory_offset, id.index)?;
        // The segment name is always NUL-terminated UTF-8 written from a
        // `&str`; a decode failure can only mean a corrupt slot.
        let removed_name: Box<str> = std::str::from_utf8(entry.name())
            .map_err(|_| StatsError::InvalidState(entry.state_byte()))?
            .into();

        let mut removed = entry;
        removed.set_generation(next_generation);
        removed.set_state(EntryState::Free);
        removed.set_link(header.free_list_head());

        // Publication: VPP header sequence stores (stats.c:27,48-49) — set
        // `in_progress`, prevalidated infallible writes, bump `epoch`, clear
        // `in_progress`.
        header.mark_in_progress();
        // SAFETY: `target` was computed by `entry_write_target` for this
        // exact slot during this preparation phase.
        unsafe { mapping.write_entry(target, removed) };
        header.store_free_list_head(u64::from(id.index));
        header.bump_epoch();
        header.clear_in_progress();

        // The name is free the moment the entry is hidden; the index must
        // not outlive the entry it names.
        self.names.remove(&removed_name);

        let allocation = self
            .metric_blocks
            .get_mut(id.index as usize)
            .and_then(Option::take)
            .ok_or(StatsError::InvalidState(0xFF))?;
        self.retired_metric_blocks.push(allocation);
        Ok(())
    }

    /// The shared add path: normalize, duplicate-check, select a slot,
    /// allocate and write the metric block, then publish.
    ///
    /// Slot selection mirrors VPP's `vlib_stats_create_counter`: reuse the
    /// free-list head first, else append at the vector high-water mark.
    /// All fallible and checked work (duplicate check, layout, allocation,
    /// block write, slot validation) happens before `in_progress` is set;
    /// the publication tail performs no arithmetic and no fallible work.
    fn add_metric(
        &mut self,
        path: &str,
        opts: &prometheus::Opts,
        kind: PrometheusType,
        directory_type: DirectoryType,
    ) -> Result<EntryId, StatsError> {
        let descriptor = crate::descriptor::normalize(opts, kind)?;
        let name_key: Box<str> = path.into();
        let name = encode_name(&name_key)?;

        let mapping = Mapping::new(&self.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();
        let capacity = header.directory_capacity();
        let initialized = header.initialized_len();

        // Duplicate-name rejection: O(1)-expected via the process-local name
        // index, mirroring VPP's `directory_vector_by_name` lookup in
        // `vlib_stats_find_entry_index` (stats.c:78-81), with the generation
        // carried so a rebuilt index can never shadow a fresh name.
        if self.names.contains_key(path) {
            return Err(StatsError::DuplicateName(path.to_owned()));
        }

        // Slot selection: free-list reuse first (VPP's
        // `dir_vector_first_free_elt`), else append at the high-water mark,
        // growing the directory if full. The effective directory offset is
        // part of the result because replacement relocates the block.
        let free_head = header.free_list_head();
        let (directory_offset, index, generation, appended, next_free_head) =
            if free_head != NULL_INDEX {
                if free_head >= capacity {
                    return Err(StatsError::InvalidState(0xFF));
                }
                let free_entry = mapping.entry(directory_offset, free_head as u32)?;
                if free_entry.state()? != EntryState::Free {
                    return Err(StatsError::InvalidState(free_entry.state_byte()));
                }
                // Reuse publishes the slot's already-advanced generation
                // (advanced exactly once at removal); never-used appended
                // slots start at 1.
                let generation = free_entry.generation();
                (
                    directory_offset,
                    free_head as u32,
                    generation,
                    false,
                    Some(free_entry.link()),
                )
            } else {
                let (directory_offset, capacity, initialized) = if initialized < capacity {
                    (directory_offset, capacity, initialized)
                } else {
                    self.replace_directory(&mapping, &header)?;
                    (
                        header.directory_offset(),
                        header.directory_capacity(),
                        header.initialized_len(),
                    )
                };
                if initialized >= capacity {
                    return Err(StatsError::SegmentFull);
                }
                (directory_offset, initialized as u32, 1, true, None)
            };

        // Allocate and write the metric block.
        let layout = crate::descriptor::block_layout(&descriptor)?;
        let mut block = self.segment.allocate(layout)?;
        let value_offset =
            crate::descriptor::write_block(&mut block.bytes_mut(), &descriptor, generation)?;
        let block_offset = Offset::new(block.offset());
        // The entry and every handle use the mapping-relative value offset.
        let value_offset = block_offset
            .checked_add(value_offset)
            .ok_or(StatsError::OutOfBounds)?;

        // Preparation: the entry value, the checked write target, and the
        // checked successor length. Everything checked happens before
        // `in_progress` is set.
        let entry = DirectorySlot::new_active(
            name,
            generation,
            directory_type,
            kind,
            block_offset,
            value_offset,
        );
        let target = mapping.entry_write_target(directory_offset, index)?;
        let next_initialized = initialized.checked_add(1).ok_or(StatsError::OutOfBounds)?;
        let index_usize = index as usize;
        if self.metric_blocks.len() <= index_usize {
            self.metric_blocks.resize_with(index_usize + 1, || None);
        }
        if self.metric_blocks[index_usize].is_some() {
            return Err(StatsError::InvalidState(0xFF));
        }

        // Publication: VPP header sequence stores (stats.c:27,48-49) — set
        // `in_progress`, prevalidated infallible writes, bump `epoch`, clear
        // `in_progress`.
        header.mark_in_progress();
        // SAFETY: `target` was computed by `entry_write_target` for this
        // exact slot during this preparation phase.
        unsafe { mapping.write_entry(target, entry) };
        if let Some(head) = next_free_head {
            header.store_free_list_head(head);
        }
        if appended {
            header.store_initialized_len(next_initialized);
        }
        header.bump_epoch();
        header.clear_in_progress();

        // The name is active only once the entry is published; keeping the
        // index in step here keeps it a pure cache of the segment state.
        let id = EntryId { index, generation };
        self.names.insert(name_key, id);
        self.metric_blocks[index_usize] = Some(block);

        Ok(id)
    }

    /// Grows the directory to twice its slot count, copying every
    /// initialized slot (active, free, and removed alike) by value into the
    /// new block. Mirrors VPP's vector growth: slot indices and list links
    /// are index-based, so relocation is invisible to entries.
    fn replace_directory(
        &mut self,
        mapping: &Mapping,
        header: &StatsHeader,
    ) -> Result<(), StatsError> {
        let old_offset = header.directory_offset();
        let old_capacity = header.directory_capacity();
        let initialized = header.initialized_len();
        let new_capacity = old_capacity.checked_mul(2).ok_or(StatsError::OutOfBounds)?;
        let new_bytes = (new_capacity as usize)
            .checked_mul(SLOT_SIZE)
            .ok_or(StatsError::OutOfBounds)?;

        // Copy initialized slots by value (validated reads).
        let mut entries = Vec::with_capacity(initialized as usize);
        let count = initialized.min(u64::from(u32::MAX)) as u32;
        for index in 0..count {
            entries.push(mapping.entry(old_offset, index)?);
        }

        let new_layout =
            Layout::from_size_align(new_bytes, 64).map_err(|_| StatsError::InvalidLayout)?;
        let new_allocation = self.segment.allocate(new_layout)?;
        mapping.write_directory_entries(Offset::new(new_allocation.offset()), &entries)?;

        let old_directory = self
            .directory_blocks
            .last()
            .ok_or(StatsError::InvalidState(0xFF))?;
        if old_directory.offset() != old_offset.get() {
            return Err(StatsError::InvalidState(0xFF));
        }
        let old_allocation = self
            .directory_blocks
            .pop()
            .ok_or(StatsError::InvalidState(0xFF))?;
        let new_offset = Offset::new(new_allocation.offset());

        // Publication: VPP header sequence stores (stats.c:27,48-49) around
        // the directory switch — set `in_progress`, swap offset and
        // capacity, bump `epoch`, clear `in_progress` — then release the
        // old block.
        header.mark_in_progress();
        header.store_directory_offset(new_offset);
        header.store_directory_capacity(new_capacity);
        header.bump_epoch();
        header.clear_in_progress();
        self.directory_blocks.push(new_allocation);
        // Retain the old directory while any reader can still be finishing
        // its pre-publication offset read; reclaim at StatsMain drop.
        self.retired_directories.push(old_allocation);
        Ok(())
    }
}

fn read_dump_value(
    mapping: &Mapping,
    id: EntryId,
    slot: &DirectorySlot,
    prometheus_type: PrometheusType,
    directory_type: DirectoryType,
    vector_index: Option<u32>,
) -> Result<DumpValue, StatsError> {
    match (prometheus_type, directory_type) {
        (PrometheusType::Counter, DirectoryType::ScalarIndex) => {
            CounterLayout::default().dump(mapping, id, slot, vector_index)
        }
        (PrometheusType::Gauge, DirectoryType::ScalarIndex) => {
            TimestampLayout::default().dump(mapping, id, slot, vector_index)
        }
        (PrometheusType::Gauge, DirectoryType::Gauge) => {
            GaugeLayout::default().dump(mapping, id, slot, vector_index)
        }
        (PrometheusType::Counter, DirectoryType::CounterVectorSimple) => {
            CounterVectorSimpleLayout::decode(mapping, slot)?.dump(mapping, id, slot, vector_index)
        }
        (PrometheusType::Counter, DirectoryType::CounterVectorCombined) => {
            CounterVectorCombinedLayout::decode(mapping, slot)?.dump(
                mapping,
                id,
                slot,
                vector_index,
            )
        }
        (PrometheusType::Gauge, DirectoryType::NameVector) => {
            NameVectorLayout::decode(mapping, slot)?.dump(mapping, id, slot, vector_index)
        }
        (PrometheusType::Counter, DirectoryType::HistogramLog2) => {
            HistogramLog2Layout::decode(mapping, slot)?.dump(mapping, id, slot, vector_index)
        }
        (PrometheusType::Counter, DirectoryType::RingBuffer) => {
            RingBufferLayout::decode(mapping, slot)?.dump(mapping, id, slot, vector_index)
        }
        _ => Err(StatsError::IncompatibleType {
            id,
            prometheus_type,
            directory_type,
        }),
    }
}

fn ring_slot_offset(
    data_offset: Offset,
    slot_stride: u64,
    rows: u32,
    capacity: u32,
    row: u32,
    slot: u32,
) -> Result<Offset, StatsError> {
    if row >= rows || slot >= capacity {
        return Err(StatsError::OutOfBounds);
    }
    let physical = u64::from(row)
        .checked_mul(u64::from(capacity))
        .and_then(|base| base.checked_add(u64::from(slot)))
        .ok_or(StatsError::OutOfBounds)?;
    data_offset
        .checked_add(
            physical
                .checked_mul(slot_stride)
                .ok_or(StatsError::OutOfBounds)?,
        )
        .ok_or(StatsError::OutOfBounds)
}

fn read_ring_slot_bytes(
    mapping: &Mapping,
    offset: Offset,
    entry_size: usize,
) -> Result<Vec<u8>, StatsError> {
    mapping.read_bytes(offset, entry_size)
}

/// A per-instance unique shared-memory name, so concurrent `StatsMain`
/// instances never collide on the same OS object.
fn unique_segment_name() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let serial = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("hammer-stats-{}-{}", std::process::id(), serial)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_occupies_the_reserved_first_page() {
        let page = hammer_infra::page_size().expect("page size must be queryable");
        let stats = StatsMain::with_capacity(2 * page).expect("two pages construct");
        let mapping = Mapping::new(&stats.segment);
        let header = mapping.header();
        assert_eq!(header.magic(), crate::header::STATS_MAGIC);
        assert_eq!(header.version(), crate::header::STATS_VERSION);
        assert_eq!(header.capacity(), (2 * page) as u64);
        assert_eq!(header.epoch(), 0);
        assert_eq!(header.in_progress(), 0);
        assert_eq!(header.directory_capacity(), INITIAL_DIRECTORY_SLOTS);
        assert_eq!(header.initialized_len(), 0);
        assert_eq!(header.free_list_head(), NULL_INDEX);
        let directory_offset = header.directory_offset();
        assert_eq!(
            directory_offset.get() % 64,
            0,
            "directory must be 64-byte aligned"
        );
        assert!(
            directory_offset.get() >= page as u64,
            "directory must start after the reserved first page"
        );
    }

    #[test]
    fn register_uses_free_slot_before_capacity_failure() {
        let page = hammer_infra::page_size().expect("page size must be queryable");
        let mut stats = StatsMain::with_capacity(2 * page).expect("two pages construct");
        let (id, handle) = stats
            .add_counter("/removed", prometheus::Opts::new("removed", "removed"))
            .expect("counter");
        stats.remove_entry(id).expect("remove");
        drop(handle);

        let before = {
            let mapping = Mapping::new(&stats.segment);
            let header = mapping.header();
            (header.epoch(), header.free_list_head())
        };

        let help = "x".repeat(1_000);
        let count = page / 256 + 32;
        let paths: Vec<String> = (0..count).map(|index| format!("/large/{index}")).collect();
        let names: Vec<String> = (0..count).map(|index| format!("large_{index}")).collect();
        let registrations: Vec<StatsRegistration<'_>> = paths
            .iter()
            .zip(&names)
            .map(|(path, name)| StatsRegistration {
                path,
                descriptor: StatsDescriptor {
                    fq_name: name,
                    help: &help,
                    const_labels: &[],
                },
                value: StatsValueLayout::Counter,
            })
            .collect();

        let Err(error) = stats.register(&registrations) else {
            panic!("capacity-constrained batch unexpectedly succeeded");
        };
        assert!(matches!(error, StatsError::SegmentFull));

        let after = {
            let mapping = Mapping::new(&stats.segment);
            let header = mapping.header();
            (header.epoch(), header.free_list_head())
        };
        assert_eq!(before.1, u64::from(id.index()));
        assert!(after.0 > before.0, "registration must publish entries");
        assert_eq!(after.1, NULL_INDEX, "the free slot must be consumed first");
    }

    /// Internal-corruption probe: a slot whose raw type bytes are each
    /// valid but combine incompatibly (a Prometheus counter on a gauge
    /// directory entry) must surface as a typed error, not a misread.
    #[test]
    fn dump_rejects_incompatible_raw_type_combination() {
        let mut stats = StatsMain::new().expect("default construction");
        let (id, _) = stats
            .add_counter("/x", prometheus::Opts::new("x", "x"))
            .expect("counter");

        let mapping = Mapping::new(&stats.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();
        let slot = mapping
            .entry(directory_offset, id.index)
            .expect("live slot read");
        // Test-only corruption of a slot no other reader observes: a gauge
        // directory entry carrying a Prometheus counter kind.
        let corrupted = DirectorySlot::new_active(
            encode_name("/x").expect("name"),
            slot.generation(),
            DirectoryType::Gauge,
            PrometheusType::Counter,
            slot.descriptor_offset(),
            slot.value_offset(),
        );
        let target = mapping
            .entry_write_target(directory_offset, id.index)
            .expect("write target");
        // SAFETY: single-threaded test; the corruption is the point.
        unsafe { mapping.write_entry(target, corrupted) };

        let err = stats
            .dump(&[id])
            .err()
            .expect("incompatible types rejected");
        assert!(
            matches!(
                err,
                StatsError::IncompatibleType {
                    id: got,
                    prometheus_type: PrometheusType::Counter,
                    directory_type: DirectoryType::Gauge,
                } if got == id
            ),
            "unexpected error: {err}"
        );
    }

    /// The mapping boundary rejects null or non-64-byte-aligned directory
    /// offsets before any pointer arithmetic, mirroring the same check on
    /// descriptor and value offsets. Without it, `entry`'s slot read and
    /// `entry_write_target`'s slot write could address an unaligned
    /// `DirectorySlot`.
    #[test]
    fn entry_and_write_target_reject_misaligned_directory_offsets() {
        let stats = StatsMain::new().expect("default construction");
        let mapping = Mapping::new(&stats.segment);
        for bad in [Offset::new(0), Offset::new(1), Offset::new(64 + 8)] {
            assert!(matches!(mapping.entry(bad, 0), Err(StatsError::Misaligned)));
            assert!(matches!(
                mapping.entry_write_target(bad, 0),
                Err(StatsError::Misaligned)
            ));
        }
        // A valid offset still resolves: the directory is 64-byte aligned.
        let directory_offset = mapping.header().directory_offset();
        assert!(mapping.entry(directory_offset, 0).is_ok());
        assert!(mapping.entry_write_target(directory_offset, 0).is_ok());
    }

    /// Active-generation invariant: for every active entry, the slot, the
    /// `EntryId`, the handle expectation, and the value record all carry
    /// exactly one generation; removal advances the slot exactly once, and
    /// free-list reuse publishes that advanced generation without a second
    /// increment.
    #[test]
    fn active_entry_generations_are_equal() {
        let mut stats = StatsMain::new().expect("default construction");
        let (id0, counter) = stats
            .add_counter("/a", prometheus::Opts::new("a", "a"))
            .expect("counter");
        let mapping = Mapping::new(&stats.segment);
        let header = mapping.header();
        let directory_offset = header.directory_offset();

        let slot = mapping
            .entry(directory_offset, id0.index())
            .expect("live slot");
        let record = mapping
            .metric_value(slot.value_offset())
            .expect("live record");
        assert_eq!(slot.generation(), id0.generation);
        assert_eq!(record.generation(), id0.generation);
        assert_eq!(counter.get().expect("live value"), 0);

        // Removal advances the directory slot once and makes the non-owning
        // handle fail before it can touch the retired value block.
        stats.remove_entry(id0).expect("remove");
        let removed = mapping
            .entry(directory_offset, id0.index)
            .expect("removed slot");
        assert_eq!(removed.generation(), id0.generation + 1);
        assert!(counter.get().is_err(), "removed handles must be rejected");

        // Reuse publishes the slot's advanced generation as-is: no second
        // increment, and the fresh value record matches it.
        drop(counter);
        let (id1, counter1) = stats
            .add_counter("/a", prometheus::Opts::new("a", "a"))
            .expect("re-add");
        assert_eq!(id1.index, id0.index);
        assert_eq!(id1.generation, id0.generation + 1);
        let slot1 = mapping
            .entry(directory_offset, id1.index)
            .expect("reused slot");
        let record1 = mapping
            .metric_value(slot1.value_offset())
            .expect("reused record");
        assert_eq!(slot1.generation(), id1.generation);
        assert_eq!(record1.generation(), id1.generation);
        counter1.increment().expect("increment reused handle");
    }
}

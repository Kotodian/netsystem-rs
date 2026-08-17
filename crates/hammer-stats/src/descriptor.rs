//! Prometheus descriptor normalization and the versioned metric block.
//!
//! Each metric gets one 64-byte-aligned `SegmentAllocation`: a versioned
//! descriptor header recording the total size and the bounded fq name,
//! help, and const label bytes, padding to a 64-byte boundary, and the
//! trailing [`crate::metric_value::MetricValue`] record. The complete block
//! remains inside its `SegmentAllocation`, so StatsMain can retain retired
//! blocks without reconstructing ownership from mapped bytes.

use std::alloc::Layout;
use std::mem::MaybeUninit;
use std::sync::atomic::AtomicU64;

use prometheus::core::Describer;

use crate::directory::PrometheusType;
use crate::error::StatsError;
use crate::metric_value::{MetricValue, VALUE_RECORD_BYTES};
use crate::read::ConstLabel;

/// Layout version of the metric block.
pub(crate) const DESCRIPTOR_VERSION: u32 = 1;
/// Size of [`MetricDescriptorHeader`] in bytes.
pub(crate) const DESCRIPTOR_HEADER_SIZE: u64 = 32;
/// Maximum fq name bytes (excluding the NUL terminator).
pub(crate) const MAX_FQ_NAME: usize = 127;
/// Maximum help bytes (excluding the NUL terminator).
pub(crate) const MAX_HELP: usize = 1024;
/// Maximum number of const label pairs.
pub(crate) const MAX_LABELS: usize = 16;
/// Maximum label name bytes (excluding the NUL terminator).
pub(crate) const MAX_LABEL_NAME: usize = 127;
/// Maximum label value bytes (excluding the NUL terminator).
pub(crate) const MAX_LABEL_VALUE: usize = 255;
/// Maximum total block size in bytes, including bounded ring payloads.
///
/// Ring registration remains fixed-size and rejects any checked layout above
/// this bound; readers use the same bound when validating a block header.
pub(crate) const MAX_BLOCK_BYTES: usize = 64 * 1024;

/// Smallest valid block size: the value record plus a 64-byte-aligned
/// descriptor area (header plus the shortest name/help pair).
pub(crate) const MIN_BLOCK_BYTES: u64 = 2 * VALUE_RECORD_BYTES;

/// Versioned header at the base of every metric block.
#[repr(C)]
pub(crate) struct MetricDescriptorHeader {
    version: u32,
    prometheus_type: u8,
    reserved: [u8; 3],
    /// Total block size, including the trailing value record.
    total_size: u64,
    name_len: u32,
    help_len: u32,
    label_count: u32,
    /// Relative offset of the layout payload within this block.
    payload_offset: u32,
}

const _: () = {
    assert!(std::mem::size_of::<MetricDescriptorHeader>() == DESCRIPTOR_HEADER_SIZE as usize);
};

impl MetricDescriptorHeader {
    pub(crate) fn total_size(&self) -> u64 {
        self.total_size
    }
}

/// Normalized, bounded metric definition ready for the mapped block.
pub(crate) struct NormalizedDescriptor {
    pub(crate) kind: PrometheusType,
    pub(crate) fq_name: String,
    pub(crate) help: String,
    pub(crate) labels: Vec<(String, String)>,
}

/// Normalizes a `prometheus::Opts` into a bounded descriptor.
///
/// `describe()` already validates the fq name and label names against the
/// Prometheus text-format rules; Hammer additionally rejects variable
/// labels (unsupported for single-value metrics) and applies size bounds so
/// a descriptor always fits a bounded block.
pub(crate) fn normalize(
    opts: &prometheus::Opts,
    kind: PrometheusType,
) -> Result<NormalizedDescriptor, StatsError> {
    let desc = opts.describe()?;
    if !desc.variable_labels.is_empty() {
        return Err(StatsError::InvalidDescriptor(
            "variable labels are not supported for single-value metrics".to_owned(),
        ));
    }
    if desc.fq_name.is_empty() || desc.fq_name.len() > MAX_FQ_NAME {
        return Err(StatsError::InvalidDescriptor(format!(
            "fq name {:?} must be 1..={MAX_FQ_NAME} bytes",
            desc.fq_name
        )));
    }
    if desc.help.is_empty() || desc.help.len() > MAX_HELP {
        return Err(StatsError::InvalidDescriptor(format!(
            "help for {:?} must be 1..={MAX_HELP} bytes",
            desc.fq_name
        )));
    }
    if desc.fq_name.as_bytes().contains(&0) || desc.help.as_bytes().contains(&0) {
        return Err(StatsError::InvalidDescriptor(
            "descriptor name and help must not contain NUL".to_owned(),
        ));
    }
    if desc.const_label_pairs.len() > MAX_LABELS {
        return Err(StatsError::InvalidDescriptor(format!(
            "more than {MAX_LABELS} const labels for {:?}",
            desc.fq_name
        )));
    }
    let mut labels = Vec::with_capacity(desc.const_label_pairs.len());
    for pair in &desc.const_label_pairs {
        let name = pair.name();
        let value = pair.value();
        if name.is_empty() || name.len() > MAX_LABEL_NAME {
            return Err(StatsError::InvalidDescriptor(format!(
                "label name {name:?} must be 1..={MAX_LABEL_NAME} bytes"
            )));
        }
        if name.as_bytes().contains(&0) || value.as_bytes().contains(&0) {
            return Err(StatsError::InvalidDescriptor(format!(
                "label {name:?} and its value must not contain NUL"
            )));
        }
        if value.len() > MAX_LABEL_VALUE {
            return Err(StatsError::InvalidDescriptor(format!(
                "label value for {name:?} exceeds {MAX_LABEL_VALUE} bytes"
            )));
        }
        labels.push((name.to_owned(), value.to_owned()));
    }
    Ok(NormalizedDescriptor {
        kind,
        fq_name: desc.fq_name,
        help: desc.help,
        labels,
    })
}

/// Size of the fixed payload header for a simple counter vector.
pub(crate) const SIMPLE_VECTOR_HEADER_BYTES: usize = 64;
const SIMPLE_VECTOR_VERSION: u32 = 1;

/// Decoded layout metadata for a simple counter vector.
pub(crate) struct CounterVectorSimpleLayout {
    pub(crate) rows: u32,
    pub(crate) columns: u32,
    pub(crate) row_stride: usize,
    pub(crate) data_offset: usize,
}

/// Decoded layout metadata for a combined counter vector.
pub(crate) struct CounterVectorCombinedLayout {
    pub(crate) rows: u32,
    pub(crate) columns: u32,
    pub(crate) row_stride: usize,
    pub(crate) data_offset: usize,
}

/// Decoded metadata for a fixed NameVector payload.
pub(crate) struct NameVectorLayout {
    pub(crate) length: u32,
    pub(crate) data_offset: usize,
}

/// Decoded metadata for a fixed HistogramLog2 payload.
pub(crate) struct HistogramLog2Layout {
    pub(crate) rows: u32,
    pub(crate) bins: u32,
    pub(crate) row_stride: usize,
    pub(crate) data_offset: usize,
}

/// Decoded metadata for a fixed RingBuffer payload.
pub(crate) struct RingBufferLayout {
    pub(crate) rows: u32,
    pub(crate) capacity: u32,
    pub(crate) entry_size: u32,
    pub(crate) slot_stride: usize,
    pub(crate) data_offset: usize,
    pub(crate) metadata_offset: usize,
    pub(crate) schema_offset: Option<usize>,
    pub(crate) schema_size: usize,
}

/// Computes the 64-byte-aligned block layout for a normalized descriptor.
///
/// The total size is `align_up(descriptor_bytes, 64) + payload + value`, so
/// both the layout payload and trailing generation record remain aligned.
pub(crate) fn block_layout(desc: &NormalizedDescriptor) -> Result<Layout, StatsError> {
    Ok(layout_with_payload(desc, 0)?.0)
}

/// Computes the block layout for a simple counter vector.
pub(crate) fn counter_vector_simple_block_layout(
    desc: &NormalizedDescriptor,
    rows: u32,
    columns: u32,
) -> Result<Layout, StatsError> {
    let payload_bytes = simple_vector_payload_bytes(rows, columns)?;
    Ok(layout_with_payload(desc, payload_bytes)?.0)
}

/// Computes the block layout for a combined counter vector.
pub(crate) fn counter_vector_combined_block_layout(
    desc: &NormalizedDescriptor,
    rows: u32,
    columns: u32,
) -> Result<Layout, StatsError> {
    let payload_bytes = combined_vector_payload_bytes(rows, columns)?;
    Ok(layout_with_payload(desc, payload_bytes)?.0)
}

pub(crate) fn name_vector_block_layout(
    desc: &NormalizedDescriptor,
    length: u32,
) -> Result<Layout, StatsError> {
    let slots = usize::try_from(length)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(NAME_VECTOR_SLOT_BYTES)
        .ok_or(StatsError::OutOfBounds)?;
    let payload_bytes = NAME_VECTOR_HEADER_BYTES
        .checked_add(slots)
        .ok_or(StatsError::OutOfBounds)?;
    Ok(layout_with_payload(desc, payload_bytes)?.0)
}

pub(crate) fn histogram_log2_block_layout(
    desc: &NormalizedDescriptor,
    rows: u32,
) -> Result<Layout, StatsError> {
    if rows == 0 {
        return Err(StatsError::InvalidDescriptor(
            "histogram must have at least one row".to_owned(),
        ));
    }
    let payload_bytes = vector_payload_bytes(
        rows,
        HISTOGRAM_BIN_COUNT,
        std::mem::size_of::<AtomicU64>(),
        HISTOGRAM_HEADER_BYTES,
    )?;
    Ok(layout_with_payload(desc, payload_bytes)?.0)
}

fn ring_layout_parts(
    rows: u32,
    capacity: u32,
    entry_size: u32,
    schema_size: usize,
) -> Result<(usize, usize, usize, Option<usize>, usize), StatsError> {
    if rows == 0 || capacity == 0 || entry_size == 0 {
        return Err(StatsError::InvalidDescriptor(
            "ring rows, capacity, and entry size must be non-zero".to_owned(),
        ));
    }
    let entry_size = usize::try_from(entry_size).map_err(|_| StatsError::OutOfBounds)?;
    let slot_stride = entry_size;
    let data_bytes = usize::try_from(rows)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(usize::try_from(capacity).map_err(|_| StatsError::OutOfBounds)?)
        .and_then(|count| count.checked_mul(slot_stride))
        .ok_or(StatsError::OutOfBounds)?;
    let data_offset = RING_HEADER_BYTES;
    let metadata_offset = data_offset
        .checked_add(data_bytes)
        .ok_or(StatsError::OutOfBounds)?
        .checked_next_multiple_of(64)
        .ok_or(StatsError::OutOfBounds)?;
    let metadata_bytes = usize::try_from(rows)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(RING_METADATA_BYTES)
        .ok_or(StatsError::OutOfBounds)?;
    let schema_offset = if schema_size == 0 {
        None
    } else {
        Some(
            metadata_offset
                .checked_add(metadata_bytes)
                .ok_or(StatsError::OutOfBounds)?,
        )
    };
    let payload_bytes = if let Some(offset) = schema_offset {
        offset.checked_add(schema_size)
    } else {
        metadata_offset.checked_add(metadata_bytes)
    }
    .ok_or(StatsError::OutOfBounds)?;
    Ok((
        slot_stride,
        data_offset,
        metadata_offset,
        schema_offset,
        payload_bytes,
    ))
}

pub(crate) fn ring_buffer_block_layout(
    desc: &NormalizedDescriptor,
    rows: u32,
    capacity: u32,
    entry_size: u32,
    schema_size: usize,
) -> Result<Layout, StatsError> {
    let (_, _, _, _, payload_bytes) = ring_layout_parts(rows, capacity, entry_size, schema_size)?;
    Ok(layout_with_payload(desc, payload_bytes)?.0)
}

fn layout_with_payload(
    desc: &NormalizedDescriptor,
    payload_bytes: usize,
) -> Result<(Layout, usize, usize), StatsError> {
    let raw = descriptor_bytes(desc)?;
    let payload_offset = raw
        .checked_next_multiple_of(64)
        .ok_or(StatsError::InvalidDescriptor(
            "metric block size overflow".to_owned(),
        ))?;
    let payload_end =
        payload_offset
            .checked_add(payload_bytes)
            .ok_or(StatsError::InvalidDescriptor(
                "metric block size overflow".to_owned(),
            ))?;
    let value_offset =
        payload_end
            .checked_next_multiple_of(64)
            .ok_or(StatsError::InvalidDescriptor(
                "metric block size overflow".to_owned(),
            ))?;
    let total = value_offset
        .checked_add(VALUE_RECORD_BYTES as usize)
        .ok_or(StatsError::InvalidDescriptor(
            "metric block size overflow".to_owned(),
        ))?;
    if total > MAX_BLOCK_BYTES {
        return Err(StatsError::InvalidDescriptor(
            "metric block exceeds the size bound".to_owned(),
        ));
    }
    let layout = Layout::from_size_align(total, 64).map_err(|_| StatsError::InvalidLayout)?;
    Ok((layout, payload_offset, value_offset))
}

pub(crate) fn counter_vector_row_stride(
    columns: u32,
    cell_bytes: usize,
) -> Result<usize, StatsError> {
    let bytes = usize::try_from(columns)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(cell_bytes)
        .ok_or(StatsError::OutOfBounds)?;
    bytes
        .checked_add(63)
        .map(|value| value & !63)
        .ok_or(StatsError::OutOfBounds)
}

fn vector_payload_bytes(
    rows: u32,
    columns: u32,
    cell_bytes: usize,
    header_bytes: usize,
) -> Result<usize, StatsError> {
    let row_stride = counter_vector_row_stride(columns, cell_bytes)?;
    let data_bytes = usize::try_from(rows)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(row_stride)
        .ok_or(StatsError::OutOfBounds)?;
    header_bytes
        .checked_add(data_bytes)
        .ok_or(StatsError::InvalidDescriptor(
            "counter vector size overflow".to_owned(),
        ))
}

fn simple_vector_payload_bytes(rows: u32, columns: u32) -> Result<usize, StatsError> {
    vector_payload_bytes(
        rows,
        columns,
        std::mem::size_of::<AtomicU64>(),
        SIMPLE_VECTOR_HEADER_BYTES,
    )
}

fn combined_vector_payload_bytes(rows: u32, columns: u32) -> Result<usize, StatsError> {
    vector_payload_bytes(
        rows,
        columns,
        2 * std::mem::size_of::<AtomicU64>(),
        SIMPLE_VECTOR_HEADER_BYTES,
    )
}

fn write_descriptor_prefix(
    bytes: &mut [MaybeUninit<u8>],
    desc: &NormalizedDescriptor,
    total_size: usize,
    payload_offset: usize,
) -> Result<(), StatsError> {
    let payload_offset = u32::try_from(payload_offset).map_err(|_| StatsError::OutOfBounds)?;
    let header = MetricDescriptorHeader {
        version: DESCRIPTOR_VERSION,
        prometheus_type: desc.kind.as_u8(),
        reserved: [0; 3],
        total_size: total_size as u64,
        name_len: desc.fq_name.len() as u32,
        help_len: desc.help.len() as u32,
        label_count: desc.labels.len() as u32,
        payload_offset,
    };
    // SAFETY: the allocation starts at a 64-byte-aligned address and the
    // header contains only plain initialized fields.
    unsafe {
        bytes
            .as_mut_ptr()
            .cast::<MetricDescriptorHeader>()
            .write(header)
    };

    let mut cursor = std::mem::size_of::<MetricDescriptorHeader>();
    cursor = put_string(bytes, cursor, desc.fq_name.as_bytes())?;
    cursor = put_string(bytes, cursor, desc.help.as_bytes())?;
    for (name, value) in &desc.labels {
        cursor = put_string(bytes, cursor, name.as_bytes())?;
        cursor = put_string(bytes, cursor, value.as_bytes())?;
    }
    Ok(())
}

/// Writes the versioned block into `bytes`; returns the value record offset.
pub(crate) fn write_block(
    bytes: &mut [MaybeUninit<u8>],
    desc: &NormalizedDescriptor,
    generation: u64,
) -> Result<u64, StatsError> {
    let (layout, payload_offset, value_off) = layout_with_payload(desc, 0)?;
    if bytes.len() != layout.size() {
        return Err(StatsError::InvalidLayout);
    }
    write_descriptor_prefix(bytes, desc, layout.size(), payload_offset)?;

    let value = MetricValue::new(generation);
    // SAFETY: `value_off` is 64-byte aligned by construction and the record
    // is fully contained in the allocation.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(value_off)
            .cast::<MetricValue>()
            .write(value);
    }
    Ok(value_off as u64)
}

/// Writes a simple counter-vector block and returns `(value_offset, data_offset)`.
pub(crate) fn write_counter_vector_simple_block(
    bytes: &mut [MaybeUninit<u8>],
    desc: &NormalizedDescriptor,
    generation: u64,
    rows: u32,
    columns: u32,
) -> Result<(u64, u64), StatsError> {
    let payload_bytes = simple_vector_payload_bytes(rows, columns)?;
    let (layout, payload_offset, value_off) = layout_with_payload(desc, payload_bytes)?;
    if bytes.len() != layout.size() {
        return Err(StatsError::InvalidLayout);
    }
    write_descriptor_prefix(bytes, desc, layout.size(), payload_offset)?;

    let header = CounterVectorSimpleHeader {
        version: SIMPLE_VECTOR_VERSION,
        rows,
        columns,
        row_stride: u32::try_from(counter_vector_row_stride(
            columns,
            std::mem::size_of::<AtomicU64>(),
        )?)
        .map_err(|_| StatsError::OutOfBounds)?,
        reserved: [0; 12],
    };
    // SAFETY: the payload offset is 64-byte aligned and the fixed header fits
    // before the value record by the layout calculation above.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(payload_offset)
            .cast::<CounterVectorSimpleHeader>()
            .write(header);
    }

    let data_offset = payload_offset + SIMPLE_VECTOR_HEADER_BYTES;
    let row_stride = counter_vector_row_stride(columns, std::mem::size_of::<AtomicU64>())?;
    let columns = usize::try_from(columns).map_err(|_| StatsError::OutOfBounds)?;
    let rows = usize::try_from(rows).map_err(|_| StatsError::OutOfBounds)?;
    for row in 0..rows {
        for column in 0..columns {
            // SAFETY: every cell lies in the checked payload span and each
            // row starts at a 64-byte boundary.
            unsafe {
                bytes
                    .as_mut_ptr()
                    .add(data_offset + row * row_stride + column * std::mem::size_of::<AtomicU64>())
                    .cast::<AtomicU64>()
                    .write(AtomicU64::new(0));
            }
        }
    }

    let value = MetricValue::new(generation);
    // SAFETY: the value record is the final aligned record in the allocation.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(value_off)
            .cast::<MetricValue>()
            .write(value);
    }
    Ok((value_off as u64, data_offset as u64))
}

/// Writes a fixed NameVector block and returns `(value_offset, data_offset)`.
pub(crate) fn write_name_vector_block(
    bytes: &mut [MaybeUninit<u8>],
    desc: &NormalizedDescriptor,
    generation: u64,
    length: u32,
) -> Result<(u64, u64), StatsError> {
    let slots = usize::try_from(length)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(NAME_VECTOR_SLOT_BYTES)
        .ok_or(StatsError::OutOfBounds)?;
    let payload_bytes = NAME_VECTOR_HEADER_BYTES
        .checked_add(slots)
        .ok_or(StatsError::OutOfBounds)?;
    let (layout, payload_offset, value_off) = layout_with_payload(desc, payload_bytes)?;
    if bytes.len() != layout.size() {
        return Err(StatsError::InvalidLayout);
    }
    write_descriptor_prefix(bytes, desc, layout.size(), payload_offset)?;
    let header = NameVectorHeader {
        version: NAME_VECTOR_VERSION,
        length,
        slot_stride: NAME_VECTOR_SLOT_BYTES as u32,
        max_bytes: NAME_VECTOR_MAX_BYTES as u32,
        reserved: [0; 12],
    };
    // SAFETY: the payload header is within the checked allocation and aligned.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(payload_offset)
            .cast::<NameVectorHeader>()
            .write(header);
    }
    let data_offset = payload_offset + NAME_VECTOR_HEADER_BYTES;
    let length = usize::try_from(length).map_err(|_| StatsError::OutOfBounds)?;
    for slot in 0..length {
        let slot_offset = data_offset + slot * NAME_VECTOR_SLOT_BYTES;
        // SAFETY: each slot is fully contained in the checked payload and is
        // initialized as atomic words before publication.
        unsafe {
            bytes
                .as_mut_ptr()
                .add(slot_offset)
                .cast::<AtomicU64>()
                .write(AtomicU64::new(0));
            bytes
                .as_mut_ptr()
                .add(slot_offset + 8)
                .cast::<AtomicU64>()
                .write(AtomicU64::new(0));
            for word in 0..(NAME_VECTOR_MAX_BYTES / 8) {
                bytes
                    .as_mut_ptr()
                    .add(slot_offset + 16 + word * 8)
                    .cast::<AtomicU64>()
                    .write(AtomicU64::new(0));
            }
        }
    }
    let value = MetricValue::new(generation);
    // SAFETY: the value record is the final aligned record in the allocation.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(value_off)
            .cast::<MetricValue>()
            .write(value);
    }
    Ok((value_off as u64, data_offset as u64))
}

/// Writes a fixed HistogramLog2 block and returns `(value_offset, data_offset)`.
pub(crate) fn write_histogram_log2_block(
    bytes: &mut [MaybeUninit<u8>],
    desc: &NormalizedDescriptor,
    generation: u64,
    rows: u32,
) -> Result<(u64, u64), StatsError> {
    if rows == 0 {
        return Err(StatsError::InvalidDescriptor(
            "histogram must have at least one row".to_owned(),
        ));
    }
    let payload_bytes = vector_payload_bytes(
        rows,
        HISTOGRAM_BIN_COUNT,
        std::mem::size_of::<AtomicU64>(),
        HISTOGRAM_HEADER_BYTES,
    )?;
    let (layout, payload_offset, value_off) = layout_with_payload(desc, payload_bytes)?;
    if bytes.len() != layout.size() {
        return Err(StatsError::InvalidLayout);
    }
    write_descriptor_prefix(bytes, desc, layout.size(), payload_offset)?;
    let row_stride =
        counter_vector_row_stride(HISTOGRAM_BIN_COUNT, std::mem::size_of::<AtomicU64>())?;
    let header = HistogramHeader {
        version: HISTOGRAM_VERSION,
        rows,
        bins: HISTOGRAM_BIN_COUNT,
        row_stride: u32::try_from(row_stride).map_err(|_| StatsError::OutOfBounds)?,
        reserved: [0; 12],
    };
    // SAFETY: the payload header is within the checked allocation and aligned.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(payload_offset)
            .cast::<HistogramHeader>()
            .write(header);
    }
    let data_offset = payload_offset + HISTOGRAM_HEADER_BYTES;
    let rows = usize::try_from(rows).map_err(|_| StatsError::OutOfBounds)?;
    for row in 0..rows {
        for bin in 0..HISTOGRAM_BIN_COUNT as usize {
            // SAFETY: every bin lies in the checked payload span.
            unsafe {
                bytes
                    .as_mut_ptr()
                    .add(data_offset + row * row_stride + bin * 8)
                    .cast::<AtomicU64>()
                    .write(AtomicU64::new(0));
            }
        }
    }
    let value = MetricValue::new(generation);
    // SAFETY: the value record is the final aligned record in the allocation.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(value_off)
            .cast::<MetricValue>()
            .write(value);
    }
    Ok((value_off as u64, data_offset as u64))
}

/// Writes a bounded fixed RingBuffer block and returns its key offsets.
pub(crate) fn write_ring_buffer_block(
    bytes: &mut [MaybeUninit<u8>],
    desc: &NormalizedDescriptor,
    generation: u64,
    rows: u32,
    capacity: u32,
    entry_size: u32,
    schema: &[u8],
) -> Result<(u64, u64, u64, Option<u64>, u64), StatsError> {
    let (slot_stride, data_rel, metadata_rel, schema_rel, payload_bytes) =
        ring_layout_parts(rows, capacity, entry_size, schema.len())?;
    let (layout, payload_offset, value_off) = layout_with_payload(desc, payload_bytes)?;
    if bytes.len() != layout.size() {
        return Err(StatsError::InvalidLayout);
    }
    write_descriptor_prefix(bytes, desc, layout.size(), payload_offset)?;
    let header = RingBufferHeader {
        config: RingBufferConfig {
            entry_size,
            ring_size: capacity,
            n_threads: rows,
            schema_size: u32::try_from(schema.len()).map_err(|_| StatsError::OutOfBounds)?,
            schema_version: RING_SCHEMA_VERSION,
        },
        metadata_offset: u32::try_from(metadata_rel).map_err(|_| StatsError::OutOfBounds)?,
        data_offset: u32::try_from(data_rel).map_err(|_| StatsError::OutOfBounds)?,
    };
    // SAFETY: the payload header is within the checked allocation and aligned.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(payload_offset)
            .cast::<RingBufferHeader>()
            .write(header);
    }
    let data_offset = payload_offset + data_rel;
    let data_bytes = usize::try_from(rows)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(usize::try_from(capacity).map_err(|_| StatsError::OutOfBounds)?)
        .and_then(|count| count.checked_mul(slot_stride))
        .ok_or(StatsError::OutOfBounds)?;
    // SAFETY: the complete data span was included in the checked payload.
    unsafe {
        std::ptr::write_bytes(
            bytes.as_mut_ptr().add(data_offset).cast::<u8>(),
            0,
            data_bytes,
        );
    }
    let metadata_offset = payload_offset + metadata_rel;
    for row in 0..usize::try_from(rows).map_err(|_| StatsError::OutOfBounds)? {
        let offset = metadata_offset + row * RING_METADATA_BYTES;
        let metadata = RingMetadata {
            head: 0,
            schema_version: RING_SCHEMA_VERSION,
            sequence: AtomicU64::new(0),
            schema_offset: u32::try_from(schema_rel.unwrap_or(0))
                .map_err(|_| StatsError::OutOfBounds)?,
            schema_size: u32::try_from(schema.len()).map_err(|_| StatsError::OutOfBounds)?,
            publication: AtomicU64::new(0),
            reserved: [0; 4],
        };
        // SAFETY: each metadata record is a complete cache-line-sized record.
        unsafe {
            bytes
                .as_mut_ptr()
                .add(offset)
                .cast::<RingMetadata>()
                .write(metadata);
        }
    }
    if let Some(schema_rel) = schema_rel {
        let schema_words = schema
            .len()
            .checked_next_multiple_of(8)
            .ok_or(StatsError::OutOfBounds)?;
        // Zero the rounded span first so a reader copying whole words never
        // observes uninitialized bytes in the final partial word.
        unsafe {
            std::ptr::write_bytes(
                bytes
                    .as_mut_ptr()
                    .add(payload_offset + schema_rel)
                    .cast::<u8>(),
                0,
                schema_words,
            );
            // SAFETY: schema bytes are copied into the checked immutable span.
            std::ptr::copy_nonoverlapping(
                schema.as_ptr(),
                bytes
                    .as_mut_ptr()
                    .add(payload_offset + schema_rel)
                    .cast::<u8>(),
                schema.len(),
            );
        }
    }
    let value = MetricValue::new(generation);
    // SAFETY: the value record is the final aligned record in the allocation.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(value_off)
            .cast::<MetricValue>()
            .write(value);
    }
    Ok((
        value_off as u64,
        data_offset as u64,
        metadata_offset as u64,
        schema_rel.map(|offset| (payload_offset + offset) as u64),
        slot_stride as u64,
    ))
}

#[repr(C)]
struct NameVectorHeader {
    version: u32,
    length: u32,
    slot_stride: u32,
    max_bytes: u32,
    reserved: [u32; 12],
}

#[repr(C)]
struct HistogramHeader {
    version: u32,
    rows: u32,
    bins: u32,
    row_stride: u32,
    reserved: [u32; 12],
}

#[repr(C, packed)]
struct RingBufferConfig {
    entry_size: u32,
    ring_size: u32,
    n_threads: u32,
    schema_size: u32,
    schema_version: u32,
}

#[repr(C, packed)]
struct RingBufferHeader {
    config: RingBufferConfig,
    metadata_offset: u32,
    data_offset: u32,
}

#[repr(C, align(64))]
struct RingMetadata {
    head: u32,
    schema_version: u32,
    sequence: AtomicU64,
    schema_offset: u32,
    schema_size: u32,
    publication: AtomicU64,
    reserved: [u64; 4],
}

const _: () = {
    assert!(std::mem::size_of::<NameVectorHeader>() == NAME_VECTOR_HEADER_BYTES);
    assert!(std::mem::size_of::<HistogramHeader>() == HISTOGRAM_HEADER_BYTES);
    assert!(std::mem::size_of::<RingBufferConfig>() == 20);
    assert!(std::mem::size_of::<RingBufferHeader>() == RING_HEADER_BYTES);
    assert!(std::mem::size_of::<RingMetadata>() == RING_METADATA_BYTES);
};

/// Writes a combined counter-vector block and returns `(value_offset, data_offset)`.
pub(crate) fn write_counter_vector_combined_block(
    bytes: &mut [MaybeUninit<u8>],
    desc: &NormalizedDescriptor,
    generation: u64,
    rows: u32,
    columns: u32,
) -> Result<(u64, u64), StatsError> {
    let payload_bytes = combined_vector_payload_bytes(rows, columns)?;
    let (layout, payload_offset, value_off) = layout_with_payload(desc, payload_bytes)?;
    if bytes.len() != layout.size() {
        return Err(StatsError::InvalidLayout);
    }
    write_descriptor_prefix(bytes, desc, layout.size(), payload_offset)?;

    let row_stride = counter_vector_row_stride(columns, 2 * std::mem::size_of::<AtomicU64>())?;
    let header = CounterVectorSimpleHeader {
        version: SIMPLE_VECTOR_VERSION,
        rows,
        columns,
        row_stride: u32::try_from(row_stride).map_err(|_| StatsError::OutOfBounds)?,
        reserved: [0; 12],
    };
    // SAFETY: the payload offset is 64-byte aligned and the fixed header fits
    // before the value record by the layout calculation above.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(payload_offset)
            .cast::<CounterVectorSimpleHeader>()
            .write(header);
    }

    let data_offset = payload_offset + SIMPLE_VECTOR_HEADER_BYTES;
    let rows = usize::try_from(rows).map_err(|_| StatsError::OutOfBounds)?;
    let columns = usize::try_from(columns).map_err(|_| StatsError::OutOfBounds)?;
    for row in 0..rows {
        for column in 0..columns {
            let cell_offset =
                data_offset + row * row_stride + column * 2 * std::mem::size_of::<AtomicU64>();
            // SAFETY: both fields lie in the checked payload span and are
            // naturally aligned AtomicU64 records.
            unsafe {
                bytes
                    .as_mut_ptr()
                    .add(cell_offset)
                    .cast::<AtomicU64>()
                    .write(AtomicU64::new(0));
                bytes
                    .as_mut_ptr()
                    .add(cell_offset + std::mem::size_of::<AtomicU64>())
                    .cast::<AtomicU64>()
                    .write(AtomicU64::new(0));
            }
        }
    }

    let value = MetricValue::new(generation);
    // SAFETY: the value record is the final aligned record in the allocation.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(value_off)
            .cast::<MetricValue>()
            .write(value);
    }
    Ok((value_off as u64, data_offset as u64))
}

#[repr(C)]
struct CounterVectorSimpleHeader {
    version: u32,
    rows: u32,
    columns: u32,
    row_stride: u32,
    reserved: [u32; 12],
}

const _: () =
    assert!(std::mem::size_of::<CounterVectorSimpleHeader>() == SIMPLE_VECTOR_HEADER_BYTES);

/// Fixed bounded NameVector representation. Each slot is 128 bytes: a
/// sequence/length pair followed by 112 bytes of UTF-8 payload stored as
/// fourteen atomic words. This keeps direct updates allocation-free and gives
/// readers a seqlock around concurrent slot writes.
pub(crate) const NAME_VECTOR_SLOT_BYTES: usize = 128;
pub(crate) const NAME_VECTOR_MAX_BYTES: usize = 112;
pub(crate) const NAME_VECTOR_HEADER_BYTES: usize = 64;
const NAME_VECTOR_VERSION: u32 = 1;

/// HistogramLog2 has a fixed 64-bin table because the borrowed registration
/// contract carries rows but no dynamic bin count. Bin zero represents zero
/// and values above the last bin saturate there, matching VPP's clamped bins.
pub(crate) const HISTOGRAM_BIN_COUNT: u32 = 64;
pub(crate) const HISTOGRAM_HEADER_BYTES: usize = 64;
const HISTOGRAM_VERSION: u32 = 1;

/// Ring metadata is one cache line per row. The first 24 bytes match
/// VPP's `vlib_stats_ring_metadata_t`; the publication marker occupies the
/// first eight bytes of its reserved padding.
pub(crate) const RING_HEADER_BYTES: usize = 28;
pub(crate) const RING_METADATA_BYTES: usize = 64;
pub(crate) const RING_SCHEMA_VERSION: u32 = 1;

/// Writes `bytes` plus a NUL terminator at `offset`; returns the new cursor.
fn put_string(
    dst: &mut [MaybeUninit<u8>],
    offset: usize,
    bytes: &[u8],
) -> Result<usize, StatsError> {
    let end = offset
        .checked_add(bytes.len() + 1)
        .ok_or(StatsError::OutOfBounds)?;
    let slot = dst.get_mut(offset..end).ok_or(StatsError::OutOfBounds)?;
    // SAFETY: `slot` spans exactly `bytes.len() + 1` elements, so the copy
    // fills the first `bytes.len()` and the terminator write below lands on
    // the last element. The NUL is written explicitly: a reused block's
    // tail holds stale bytes, never zeros, so decode must be able to rely
    // on a freshly written terminator.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), slot.as_mut_ptr().cast::<u8>(), bytes.len());
        slot.as_mut_ptr().add(bytes.len()).cast::<u8>().write(0);
    }
    Ok(end)
}

/// Serialized descriptor bytes (header + all strings), before padding.
fn descriptor_bytes(desc: &NormalizedDescriptor) -> Result<usize, StatsError> {
    let mut total = std::mem::size_of::<MetricDescriptorHeader>();
    total = total
        .checked_add(desc.fq_name.len() + 1)
        .ok_or(StatsError::InvalidDescriptor(
            "descriptor size overflow".to_owned(),
        ))?;
    total = total
        .checked_add(desc.help.len() + 1)
        .ok_or(StatsError::InvalidDescriptor(
            "descriptor size overflow".to_owned(),
        ))?;
    for (name, value) in &desc.labels {
        total = total.checked_add(name.len() + 1 + value.len() + 1).ok_or(
            StatsError::InvalidDescriptor("descriptor size overflow".to_owned()),
        )?;
    }
    Ok(total)
}

/// A metric descriptor decoded from a mapped block into owned strings.
pub(crate) struct DecodedDescriptor {
    pub(crate) fq_name: String,
    pub(crate) help: String,
    pub(crate) const_labels: Vec<ConstLabel>,
}

/// Decodes a mapped metric block into owned descriptor strings.
///
/// The exact inverse of [`write_block`]: validates the version, the total
/// size against the block bound, every header length and count, NUL
/// termination and UTF-8 of every string, and consumes exactly
/// `label_count` label pairs without reading past the trailing value
/// record. Used by `StatsMain::list`; any failure is a typed
/// [`StatsError::InvalidDescriptor`].
pub(crate) fn decode_descriptor(block: &[u8]) -> Result<DecodedDescriptor, StatsError> {
    let header = parse_header(block)?;
    if header.version != DESCRIPTOR_VERSION {
        return Err(StatsError::InvalidDescriptor(
            "corrupt metric block version".to_owned(),
        ));
    }
    if header.total_size < MIN_BLOCK_BYTES || header.total_size > MAX_BLOCK_BYTES as u64 {
        return Err(StatsError::InvalidDescriptor(
            "corrupt metric block size".to_owned(),
        ));
    }
    if header.total_size != block.len() as u64 {
        return Err(StatsError::InvalidDescriptor(
            "descriptor span does not match the recorded size".to_owned(),
        ));
    }
    if header.label_count as usize > MAX_LABELS {
        return Err(StatsError::InvalidDescriptor(
            "more than the maximum const labels".to_owned(),
        ));
    }
    let mut cursor = DESCRIPTOR_HEADER_SIZE as usize;
    let (fq_name, name_len) = take_string(block, &mut cursor, MAX_FQ_NAME)?;
    if header.name_len as usize != name_len {
        return Err(StatsError::InvalidDescriptor(
            "fq name length does not match the header".to_owned(),
        ));
    }
    let (help, help_len) = take_string(block, &mut cursor, MAX_HELP)?;
    if header.help_len as usize != help_len {
        return Err(StatsError::InvalidDescriptor(
            "help length does not match the header".to_owned(),
        ));
    }
    let mut const_labels = Vec::with_capacity(header.label_count as usize);
    for _ in 0..header.label_count {
        let (name, _) = take_string(block, &mut cursor, MAX_LABEL_NAME)?;
        let (value, _) = take_string(block, &mut cursor, MAX_LABEL_VALUE)?;
        const_labels.push(ConstLabel { name, value });
    }
    // The padding between the last string and the value record is never
    // read; the cursor must not have overrun the value record.
    let value_start = header.total_size as usize - VALUE_RECORD_BYTES as usize;
    if cursor > value_start {
        return Err(StatsError::InvalidDescriptor(
            "descriptor strings overrun the value record".to_owned(),
        ));
    }
    Ok(DecodedDescriptor {
        fq_name,
        help,
        const_labels,
    })
}

fn decode_vector_layout(
    block: &[u8],
    cell_bytes: usize,
) -> Result<(u32, u32, usize, usize), StatsError> {
    let header = parse_header(block)?;
    if header.version != DESCRIPTOR_VERSION
        || header.total_size < MIN_BLOCK_BYTES
        || header.total_size > MAX_BLOCK_BYTES as u64
        || header.total_size != block.len() as u64
    {
        return Err(StatsError::InvalidDescriptor(
            "corrupt counter vector metric block".to_owned(),
        ));
    }
    let payload_offset =
        usize::try_from(header.payload_offset).map_err(|_| StatsError::OutOfBounds)?;
    if payload_offset % 64 != 0 || payload_offset < DESCRIPTOR_HEADER_SIZE as usize {
        return Err(StatsError::InvalidDescriptor(
            "counter vector payload is misaligned".to_owned(),
        ));
    }
    let payload_header = block
        .get(payload_offset..payload_offset + SIMPLE_VECTOR_HEADER_BYTES)
        .ok_or(StatsError::InvalidDescriptor(
            "counter vector payload header is truncated".to_owned(),
        ))?;
    let u32_at = |offset: usize| -> Result<u32, StatsError> {
        payload_header[offset..offset + 4]
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| {
                StatsError::InvalidDescriptor("truncated counter vector header".to_owned())
            })
    };
    if u32_at(0)? != SIMPLE_VECTOR_VERSION {
        return Err(StatsError::InvalidDescriptor(
            "unsupported counter vector payload version".to_owned(),
        ));
    }
    let rows = u32_at(4)?;
    let columns = u32_at(8)?;
    let row_stride = usize::try_from(u32_at(12)?).map_err(|_| StatsError::OutOfBounds)?;
    let expected_stride = counter_vector_row_stride(columns, cell_bytes)?;
    if row_stride != expected_stride || row_stride % 64 != 0 {
        return Err(StatsError::InvalidDescriptor(
            "counter vector row stride is invalid".to_owned(),
        ));
    }
    let data_offset = payload_offset + SIMPLE_VECTOR_HEADER_BYTES;
    let data_end = data_offset
        .checked_add(
            usize::try_from(rows)
                .map_err(|_| StatsError::OutOfBounds)?
                .checked_mul(row_stride)
                .ok_or(StatsError::OutOfBounds)?,
        )
        .ok_or(StatsError::OutOfBounds)?;
    let value_start = block.len() - VALUE_RECORD_BYTES as usize;
    if data_end > value_start {
        return Err(StatsError::InvalidDescriptor(
            "counter vector payload overruns value record".to_owned(),
        ));
    }
    Ok((rows, columns, row_stride, data_offset))
}

/// Decodes the fixed metadata and data offset for a simple counter vector.
pub(crate) fn decode_counter_vector_simple_layout(
    block: &[u8],
) -> Result<CounterVectorSimpleLayout, StatsError> {
    let (rows, columns, row_stride, data_offset) =
        decode_vector_layout(block, std::mem::size_of::<AtomicU64>())?;
    Ok(CounterVectorSimpleLayout {
        rows,
        columns,
        row_stride,
        data_offset,
    })
}

/// Decodes the fixed metadata and data offset for a combined counter vector.
pub(crate) fn decode_counter_vector_combined_layout(
    block: &[u8],
) -> Result<CounterVectorCombinedLayout, StatsError> {
    let (rows, columns, row_stride, data_offset) =
        decode_vector_layout(block, 2 * std::mem::size_of::<AtomicU64>())?;
    Ok(CounterVectorCombinedLayout {
        rows,
        columns,
        row_stride,
        data_offset,
    })
}

pub(crate) fn decode_name_vector_layout(block: &[u8]) -> Result<NameVectorLayout, StatsError> {
    let header = parse_header(block)?;
    validate_payload_block(&header, block, "name vector")?;
    let payload_offset =
        usize::try_from(header.payload_offset).map_err(|_| StatsError::OutOfBounds)?;
    let payload = block
        .get(payload_offset..payload_offset + NAME_VECTOR_HEADER_BYTES)
        .ok_or(StatsError::InvalidDescriptor(
            "name vector payload header is truncated".to_owned(),
        ))?;
    let u32_at = |offset: usize| -> Result<u32, StatsError> {
        payload[offset..offset + 4]
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| StatsError::InvalidDescriptor("truncated name vector header".to_owned()))
    };
    if u32_at(0)? != NAME_VECTOR_VERSION
        || u32_at(8)? as usize != NAME_VECTOR_SLOT_BYTES
        || u32_at(12)? as usize != NAME_VECTOR_MAX_BYTES
    {
        return Err(StatsError::InvalidDescriptor(
            "invalid name vector payload header".to_owned(),
        ));
    }
    let length = u32_at(4)?;
    let data_offset = payload_offset + NAME_VECTOR_HEADER_BYTES;
    let end = data_offset
        .checked_add(
            usize::try_from(length)
                .map_err(|_| StatsError::OutOfBounds)?
                .checked_mul(NAME_VECTOR_SLOT_BYTES)
                .ok_or(StatsError::OutOfBounds)?,
        )
        .ok_or(StatsError::OutOfBounds)?;
    if end > block.len() - VALUE_RECORD_BYTES as usize {
        return Err(StatsError::InvalidDescriptor(
            "name vector payload overruns value record".to_owned(),
        ));
    }
    Ok(NameVectorLayout {
        length,
        data_offset,
    })
}

pub(crate) fn decode_histogram_log2_layout(
    block: &[u8],
) -> Result<HistogramLog2Layout, StatsError> {
    let header = parse_header(block)?;
    validate_payload_block(&header, block, "histogram")?;
    let payload_offset =
        usize::try_from(header.payload_offset).map_err(|_| StatsError::OutOfBounds)?;
    let payload = block
        .get(payload_offset..payload_offset + HISTOGRAM_HEADER_BYTES)
        .ok_or(StatsError::InvalidDescriptor(
            "histogram payload header is truncated".to_owned(),
        ))?;
    let u32_at = |offset: usize| -> Result<u32, StatsError> {
        payload[offset..offset + 4]
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| StatsError::InvalidDescriptor("truncated histogram header".to_owned()))
    };
    let rows = u32_at(4)?;
    let bins = u32_at(8)?;
    let row_stride = usize::try_from(u32_at(12)?).map_err(|_| StatsError::OutOfBounds)?;
    if u32_at(0)? != HISTOGRAM_VERSION
        || bins != HISTOGRAM_BIN_COUNT
        || row_stride != counter_vector_row_stride(bins, std::mem::size_of::<AtomicU64>())?
    {
        return Err(StatsError::InvalidDescriptor(
            "invalid histogram payload header".to_owned(),
        ));
    }
    let data_offset = payload_offset + HISTOGRAM_HEADER_BYTES;
    let end = data_offset
        .checked_add(
            usize::try_from(rows)
                .map_err(|_| StatsError::OutOfBounds)?
                .checked_mul(row_stride)
                .ok_or(StatsError::OutOfBounds)?,
        )
        .ok_or(StatsError::OutOfBounds)?;
    if end > block.len() - VALUE_RECORD_BYTES as usize {
        return Err(StatsError::InvalidDescriptor(
            "histogram payload overruns value record".to_owned(),
        ));
    }
    Ok(HistogramLog2Layout {
        rows,
        bins,
        row_stride,
        data_offset,
    })
}

pub(crate) fn decode_ring_buffer_layout(block: &[u8]) -> Result<RingBufferLayout, StatsError> {
    let header = parse_header(block)?;
    validate_payload_block(&header, block, "ring buffer")?;
    let payload_offset =
        usize::try_from(header.payload_offset).map_err(|_| StatsError::OutOfBounds)?;
    let value_end = block.len().checked_sub(VALUE_RECORD_BYTES as usize).ok_or(
        StatsError::InvalidDescriptor("ring buffer value record is truncated".to_owned()),
    )?;
    let payload = block
        .get(payload_offset..value_end)
        .ok_or(StatsError::InvalidDescriptor(
            "ring buffer payload is truncated".to_owned(),
        ))?;
    let u32_at = |bytes: &[u8], offset: usize| -> Result<u32, StatsError> {
        let end = offset.checked_add(4).ok_or(StatsError::OutOfBounds)?;
        bytes
            .get(offset..end)
            .ok_or(StatsError::InvalidDescriptor(
                "truncated ring buffer field".to_owned(),
            ))?
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| StatsError::InvalidDescriptor("truncated ring buffer field".to_owned()))
    };
    if payload.len() < RING_HEADER_BYTES {
        return Err(StatsError::InvalidDescriptor(
            "ring buffer payload header is truncated".to_owned(),
        ));
    }
    let entry_size = u32_at(payload, 0)?;
    let capacity = u32_at(payload, 4)?;
    let rows = u32_at(payload, 8)?;
    let schema_size = usize::try_from(u32_at(payload, 12)?).map_err(|_| StatsError::OutOfBounds)?;
    let schema_version = u32_at(payload, 16)?;
    let metadata_rel =
        usize::try_from(u32_at(payload, 20)?).map_err(|_| StatsError::OutOfBounds)?;
    let data_rel = usize::try_from(u32_at(payload, 24)?).map_err(|_| StatsError::OutOfBounds)?;
    if schema_version != RING_SCHEMA_VERSION
        || rows == 0
        || capacity == 0
        || entry_size == 0
        || data_rel != RING_HEADER_BYTES
        || metadata_rel % 64 != 0
    {
        return Err(StatsError::InvalidDescriptor(
            "invalid VPP ring buffer header".to_owned(),
        ));
    }
    let slot_stride = usize::try_from(entry_size).map_err(|_| StatsError::OutOfBounds)?;
    let data_bytes = usize::try_from(rows)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(usize::try_from(capacity).map_err(|_| StatsError::OutOfBounds)?)
        .and_then(|count| count.checked_mul(slot_stride))
        .ok_or(StatsError::OutOfBounds)?;
    let data_end = data_rel
        .checked_add(data_bytes)
        .ok_or(StatsError::OutOfBounds)?;
    if metadata_rel < data_end {
        return Err(StatsError::InvalidDescriptor(
            "ring metadata overlaps data".to_owned(),
        ));
    }
    let metadata_bytes = usize::try_from(rows)
        .map_err(|_| StatsError::OutOfBounds)?
        .checked_mul(RING_METADATA_BYTES)
        .ok_or(StatsError::OutOfBounds)?;
    let metadata_end = metadata_rel
        .checked_add(metadata_bytes)
        .ok_or(StatsError::OutOfBounds)?;
    if metadata_end > payload.len() {
        return Err(StatsError::InvalidDescriptor(
            "ring metadata overruns value record".to_owned(),
        ));
    }

    let schema_rel = if schema_size == 0 {
        None
    } else {
        Some(metadata_end)
    };
    for row in 0..usize::try_from(rows).map_err(|_| StatsError::OutOfBounds)? {
        let metadata = metadata_rel
            .checked_add(
                row.checked_mul(RING_METADATA_BYTES)
                    .ok_or(StatsError::OutOfBounds)?,
            )
            .ok_or(StatsError::OutOfBounds)?;
        if u32_at(payload, metadata + 4)? != schema_version
            || u32_at(payload, metadata + 20)? as usize != schema_size
            || u32_at(payload, metadata + 16)? as usize != schema_rel.unwrap_or(0)
        {
            return Err(StatsError::InvalidDescriptor(
                "inconsistent VPP ring metadata".to_owned(),
            ));
        }
    }
    if let Some(schema_rel) = schema_rel {
        let schema_end = schema_rel
            .checked_add(schema_size)
            .ok_or(StatsError::OutOfBounds)?;
        if schema_end > payload.len() {
            return Err(StatsError::InvalidDescriptor(
                "ring schema overruns value record".to_owned(),
            ));
        }
    }
    Ok(RingBufferLayout {
        rows,
        capacity,
        entry_size,
        slot_stride,
        data_offset: payload_offset + data_rel,
        metadata_offset: payload_offset + metadata_rel,
        schema_offset: schema_rel.map(|offset| payload_offset + offset),
        schema_size,
    })
}

fn validate_payload_block(
    header: &ParsedHeader,
    block: &[u8],
    kind: &str,
) -> Result<(), StatsError> {
    if header.version != DESCRIPTOR_VERSION
        || header.total_size < MIN_BLOCK_BYTES
        || header.total_size > MAX_BLOCK_BYTES as u64
        || header.total_size != block.len() as u64
    {
        return Err(StatsError::InvalidDescriptor(format!(
            "corrupt {kind} metric block"
        )));
    }
    let payload_offset =
        usize::try_from(header.payload_offset).map_err(|_| StatsError::OutOfBounds)?;
    if payload_offset % 64 != 0 || payload_offset < DESCRIPTOR_HEADER_SIZE as usize {
        return Err(StatsError::InvalidDescriptor(format!(
            "{kind} payload is misaligned"
        )));
    }
    Ok(())
}

/// Header fields decoded from the block's first `DESCRIPTOR_HEADER_SIZE`
/// bytes.
struct ParsedHeader {
    version: u32,
    total_size: u64,
    name_len: u32,
    help_len: u32,
    label_count: u32,
    payload_offset: u32,
}

/// Decodes [`ParsedHeader`] from the block prefix with a bounds check.
fn parse_header(block: &[u8]) -> Result<ParsedHeader, StatsError> {
    if block.len() < DESCRIPTOR_HEADER_SIZE as usize {
        return Err(StatsError::InvalidDescriptor(
            "metric block smaller than its header".to_owned(),
        ));
    }
    let truncated = |_| StatsError::InvalidDescriptor("truncated metric block header".to_owned());
    let u32_at = |offset: usize| -> Result<u32, StatsError> {
        block[offset..offset + 4]
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(truncated)
    };
    Ok(ParsedHeader {
        version: u32_at(0)?,
        total_size: u64::from_le_bytes(block[8..16].try_into().map_err(truncated)?),
        name_len: u32_at(16)?,
        help_len: u32_at(20)?,
        label_count: u32_at(24)?,
        payload_offset: u32_at(28)?,
    })
}

/// Reads a NUL-terminated UTF-8 string of at most `max_bytes` bytes (plus
/// its terminator) starting at `cursor`, advancing the cursor past the
/// terminator. Returns the string and its byte length.
fn take_string(
    block: &[u8],
    cursor: &mut usize,
    max_bytes: usize,
) -> Result<(String, usize), StatsError> {
    let start = *cursor;
    let end = block
        .get(start..)
        .ok_or(StatsError::InvalidDescriptor(
            "descriptor string starts past the block end".to_owned(),
        ))?
        .iter()
        .position(|&byte| byte == 0)
        .map(|position| start + position)
        .ok_or(StatsError::InvalidDescriptor(
            "descriptor string missing its NUL terminator".to_owned(),
        ))?;
    // Empty strings are legal: `normalize` rejects empty fq names, help,
    // and label names, but an empty label value is writable.
    let len = end - start;
    if len > max_bytes {
        return Err(StatsError::InvalidDescriptor(format!(
            "descriptor string of {len} bytes is out of bounds"
        )));
    }
    *cursor = end + 1;
    let string = std::str::from_utf8(&block[start..end])
        .map_err(|_| StatsError::InvalidDescriptor("descriptor string is not UTF-8".to_owned()))?
        .to_owned();
    Ok((string, len))
}

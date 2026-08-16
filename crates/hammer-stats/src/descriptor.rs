//! Prometheus descriptor normalization and the versioned metric block.
//!
//! Each metric gets one 64-byte-aligned `SegmentAllocation`: a versioned
//! descriptor header recording the total size and the bounded fq name,
//! help, and const label bytes, padding to a 64-byte boundary, and the
//! trailing [`crate::metric_value::MetricValue`] record. The block's total
//! size is read back during deferred reclamation to reconstruct the exact
//! allocation for `Segment::free`.

use std::alloc::Layout;
use std::mem::MaybeUninit;

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
/// Maximum total block size in bytes.
pub(crate) const MAX_BLOCK_BYTES: usize = 4096;

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
    reserved2: u32,
}

const _: () = {
    assert!(std::mem::size_of::<MetricDescriptorHeader>() == DESCRIPTOR_HEADER_SIZE as usize);
};

impl MetricDescriptorHeader {
    pub(crate) fn version(&self) -> u32 {
        self.version
    }

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

/// Computes the 64-byte-aligned block layout for a normalized descriptor.
///
/// The total size is `align_up(descriptor_bytes, 64) + VALUE_RECORD_BYTES`,
/// so the trailing value record always starts at a 64-byte boundary and the
/// block itself is 64-byte aligned.
pub(crate) fn block_layout(desc: &NormalizedDescriptor) -> Result<Layout, StatsError> {
    let raw = descriptor_bytes(desc)?;
    let total = align_up(raw, 64)
        .checked_add(VALUE_RECORD_BYTES as usize)
        .ok_or(StatsError::InvalidDescriptor(
            "metric block size overflow".to_owned(),
        ))?;
    if total > MAX_BLOCK_BYTES {
        return Err(StatsError::InvalidDescriptor(
            "metric block exceeds the size bound".to_owned(),
        ));
    }
    Layout::from_size_align(total, 64).map_err(|_| StatsError::InvalidLayout)
}

/// The value record offset within a block of `total_size` bytes. Valid for
/// blocks built by [`block_layout`], where the value record is the last
/// `VALUE_RECORD_BYTES` bytes.
pub(crate) fn value_offset(total_size: u64) -> u64 {
    total_size - VALUE_RECORD_BYTES
}

/// Rounds `value` up to a multiple of 64.
fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// Writes the versioned block into `bytes`; returns the value record offset.
///
/// `bytes` must match the layout computed by [`block_layout`]. The value
/// record starts with `generation` (1 for never-used blocks, the slot's
/// advanced generation on reuse) and one reference (the returned handle
/// owns it), so the active-generation invariant
/// slot == id == handle == record holds for every entry.
pub(crate) fn write_block(
    bytes: &mut [MaybeUninit<u8>],
    desc: &NormalizedDescriptor,
    generation: u64,
) -> Result<u64, StatsError> {
    let layout = block_layout(desc)?;
    if bytes.len() != layout.size() {
        return Err(StatsError::InvalidLayout);
    }
    let value_off = value_offset(layout.size() as u64);

    // Versioned header at offset 0 (the block is 64-byte aligned).
    let header = MetricDescriptorHeader {
        version: DESCRIPTOR_VERSION,
        prometheus_type: desc.kind.as_u8(),
        reserved: [0; 3],
        total_size: layout.size() as u64,
        name_len: desc.fq_name.len() as u32,
        help_len: desc.help.len() as u32,
        label_count: desc.labels.len() as u32,
        reserved2: 0,
    };
    // SAFETY: offset 0 of a 64-aligned block; the header is fully written
    // plain data (no padding gaps: every field is u32/u8/u64).
    unsafe {
        bytes
            .as_mut_ptr()
            .cast::<MetricDescriptorHeader>()
            .write(header);
    }

    // Bounded string fields, each NUL-terminated.
    let mut cursor = std::mem::size_of::<MetricDescriptorHeader>();
    cursor = put_string(bytes, cursor, desc.fq_name.as_bytes())?;
    cursor = put_string(bytes, cursor, desc.help.as_bytes())?;
    for (name, value) in &desc.labels {
        cursor = put_string(bytes, cursor, name.as_bytes())?;
        cursor = put_string(bytes, cursor, value.as_bytes())?;
    }

    // Trailing value record at the 64-byte boundary.
    let value = MetricValue::new(generation, 1);
    // SAFETY: `value_off` is 64-byte aligned by construction and the record
    // is 64 bytes, so `value_off + 64 == len` stays inside the block.
    unsafe {
        bytes
            .as_mut_ptr()
            .add(value_off as usize)
            .cast::<MetricValue>()
            .write(value);
    }
    Ok(value_off)
}

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

/// Header fields decoded from the block's first `DESCRIPTOR_HEADER_SIZE`
/// bytes.
struct ParsedHeader {
    version: u32,
    total_size: u64,
    name_len: u32,
    help_len: u32,
    label_count: u32,
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

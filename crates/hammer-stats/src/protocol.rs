use std::ffi::CStr;
use std::ffi::c_void;
use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};
pub(crate) const MAX_NAME_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProtocolError {
    UnknownDirectoryType {
        raw: u32,
    },
    InvalidNameNul,
    MissingNameTerminator,
    InvalidNamePadding,
    NameTooLong {
        length: usize,
    },
    InvalidVersion {
        actual: u64,
    },
    InvalidRingConfig,
    InvalidVectorHeader,
    RingSizeOverflow,
    MappingCapacityExceeded {
        required: usize,
        capacity: usize,
    },
    SymlinkTargetOutOfBounds {
        index: u32,
    },
    SymlinkCycle {
        index: u32,
        depth: u32,
    },
    EncodedValueOverflow {
        value: usize,
    },
    DirectoryDataTypeMismatch {
        expected: DirectoryType,
        actual: DirectoryType,
    },
    ElementSizeZero,
    ElementOutOfBounds {
        index: usize,
        length: usize,
    },
    OffsetOverflow,
    OffsetOutOfBounds {
        offset: usize,
        capacity: usize,
    },
    SpanOutOfBounds {
        offset: usize,
        length: usize,
        capacity: usize,
    },
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownDirectoryType { raw } => {
                write!(formatter, "unknown stats directory type {raw}")
            }
            Self::InvalidNameNul => formatter.write_str("stats name contains an interior NUL"),
            Self::MissingNameTerminator => formatter.write_str("stats name is not NUL-terminated"),
            Self::InvalidNamePadding => {
                formatter.write_str("stats name has non-zero bytes after its terminator")
            }
            Self::NameTooLong { length } => {
                write!(
                    formatter,
                    "stats name length {length} exceeds {MAX_NAME_BYTES}"
                )
            }
            Self::InvalidVersion { actual } => write!(
                formatter,
                "stats segment version {actual} is not version {STAT_SEGMENT_VERSION}"
            ),
            Self::InvalidRingConfig => formatter.write_str("invalid stats ring configuration"),
            Self::InvalidVectorHeader => formatter.write_str("invalid stats vector header"),
            Self::RingSizeOverflow => formatter.write_str("stats ring size overflow"),
            Self::MappingCapacityExceeded { required, capacity } => write!(
                formatter,
                "stats allocation needs {required} bytes but the mapping holds {capacity}"
            ),
            Self::SymlinkTargetOutOfBounds { index } => {
                write!(formatter, "stats symlink target {index} is out of bounds")
            }
            Self::SymlinkCycle { index, depth } => write!(
                formatter,
                "stats symlink at {index} forms a cycle at depth {depth}"
            ),
            Self::EncodedValueOverflow { value } => {
                write!(
                    formatter,
                    "stats encoded value {value} does not fit the protocol field"
                )
            }
            Self::DirectoryDataTypeMismatch { expected, actual } => write!(
                formatter,
                "stats directory entry has type `{actual}`, expected `{expected}`"
            ),
            Self::ElementSizeZero => formatter.write_str("stats vector element size is zero"),
            Self::ElementOutOfBounds { index, length } => write!(
                formatter,
                "stats vector element {index} is outside length {length}"
            ),
            Self::OffsetOverflow => formatter.write_str("stats offset arithmetic overflow"),
            Self::OffsetOutOfBounds { offset, capacity } => write!(
                formatter,
                "stats offset {offset} is outside mapping capacity {capacity}"
            ),
            Self::SpanOutOfBounds {
                offset,
                length,
                capacity,
            } => write!(
                formatter,
                "stats span {offset}+{length} exceeds mapping capacity {capacity}"
            ),
        }
    }
}

impl std::error::Error for ProtocolError {}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NameBytes([u8; MAX_NAME_BYTES]);

impl TryFrom<&[u8]> for NameBytes {
    type Error = ProtocolError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() > MAX_NAME_BYTES {
            return Err(ProtocolError::NameTooLong {
                length: value.len(),
            });
        }

        if let Some(nul) = value.iter().position(|byte| *byte == 0) {
            if nul > MAX_NAME_BYTES - 2 {
                return Err(ProtocolError::NameTooLong { length: nul });
            }
            if value[nul + 1..].iter().any(|byte| *byte != 0) {
                return Err(ProtocolError::InvalidNameNul);
            }

            let mut bytes = [0u8; MAX_NAME_BYTES];
            bytes[..=nul].copy_from_slice(&value[..=nul]);
            return Ok(Self(bytes));
        }

        if value.len() > MAX_NAME_BYTES - 2 {
            if value.len() == MAX_NAME_BYTES - 1 {
                return Err(ProtocolError::NameTooLong {
                    length: value.len(),
                });
            }
            return Err(ProtocolError::MissingNameTerminator);
        }

        let mut bytes = [0u8; MAX_NAME_BYTES];
        bytes[..value.len()].copy_from_slice(value);
        Ok(Self(bytes))
    }
}

impl TryFrom<&str> for NameBytes {
    type Error = ProtocolError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.as_bytes().contains(&0) {
            return Err(ProtocolError::InvalidNameNul);
        }
        Self::try_from(value.as_bytes())
    }
}

impl AsRef<[u8]> for NameBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl NameBytes {
    pub(crate) fn as_c_str(&self) -> Result<&CStr, ProtocolError> {
        let Some(nul) = self.0.iter().position(|byte| *byte == 0) else {
            return Err(ProtocolError::MissingNameTerminator);
        };
        if nul > MAX_NAME_BYTES - 2 {
            return Err(ProtocolError::NameTooLong { length: nul });
        }
        if self.0[nul + 1..].iter().any(|byte| *byte != 0) {
            return Err(ProtocolError::InvalidNamePadding);
        }
        CStr::from_bytes_until_nul(&self.0).map_err(|_| ProtocolError::MissingNameTerminator)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TypeCode(u32);

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DirectoryType {
    Illegal = 0,
    ScalarIndex = 1,
    CounterVectorSimple = 2,
    CounterVectorCombined = 3,
    NameVector = 4,
    Empty = 5,
    Symlink = 6,
    HistogramLog2 = 7,
    RingBuffer = 8,
    Gauge = 9,
}

impl From<u32> for TypeCode {
    #[inline]
    fn from(raw: u32) -> Self {
        Self(raw)
    }
}

impl From<TypeCode> for u32 {
    #[inline]
    fn from(code: TypeCode) -> Self {
        code.0
    }
}

impl From<DirectoryType> for TypeCode {
    #[inline]
    fn from(kind: DirectoryType) -> Self {
        Self(kind as u32)
    }
}

impl From<DirectoryType> for u32 {
    #[inline]
    fn from(kind: DirectoryType) -> Self {
        kind as u32
    }
}

impl From<DirectoryType> for &'static str {
    #[inline]
    fn from(kind: DirectoryType) -> Self {
        match kind {
            DirectoryType::Illegal => "illegal",
            DirectoryType::ScalarIndex => "scalar_index",
            DirectoryType::CounterVectorSimple => "counter_vector_simple",
            DirectoryType::CounterVectorCombined => "counter_vector_combined",
            DirectoryType::NameVector => "name_vector",
            DirectoryType::Empty => "empty",
            DirectoryType::Symlink => "symlink",
            DirectoryType::HistogramLog2 => "histogram_log2",
            DirectoryType::RingBuffer => "ring_buffer",
            DirectoryType::Gauge => "gauge",
        }
    }
}

impl std::fmt::Display for DirectoryType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(<&str>::from(*self))
    }
}

impl TryFrom<u32> for DirectoryType {
    type Error = ProtocolError;

    #[inline]
    fn try_from(raw: u32) -> Result<Self, Self::Error> {
        Self::try_from(TypeCode::from(raw))
    }
}

impl TryFrom<TypeCode> for DirectoryType {
    type Error = ProtocolError;

    #[inline]
    fn try_from(code: TypeCode) -> Result<Self, Self::Error> {
        match code.raw() {
            0 => Ok(Self::Illegal),
            1 => Ok(Self::ScalarIndex),
            2 => Ok(Self::CounterVectorSimple),
            3 => Ok(Self::CounterVectorCombined),
            4 => Ok(Self::NameVector),
            5 => Ok(Self::Empty),
            6 => Ok(Self::Symlink),
            7 => Ok(Self::HistogramLog2),
            8 => Ok(Self::RingBuffer),
            9 => Ok(Self::Gauge),
            raw => Err(ProtocolError::UnknownDirectoryType { raw }),
        }
    }
}

impl TypeCode {
    #[inline]
    pub(crate) const fn raw(self) -> u32 {
        self.0
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SymlinkIndex {
    pub(crate) entry_index: u32,
    pub(crate) vector_index: u32,
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DirectoryIndex(u32);

impl DirectoryIndex {
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl From<u32> for DirectoryIndex {
    #[inline]
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl From<DirectoryIndex> for u32 {
    #[inline]
    fn from(value: DirectoryIndex) -> Self {
        value.raw()
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) union DirectoryData {
    indices: SymlinkIndex,
    index: u64,
    value: u64,
    data: *mut core::ffi::c_void,
    string_vector: *mut *mut u8,
}

impl DirectoryData {
    #[inline]
    pub(crate) const fn index(index: u64) -> Self {
        Self { index }
    }

    #[inline]
    pub(crate) const fn symlink_index(indices: SymlinkIndex) -> Self {
        Self { indices }
    }

    #[inline]
    pub(crate) const fn value(value: u64) -> Self {
        Self { value }
    }

    #[inline]
    pub(crate) const fn data(data: *mut core::ffi::c_void) -> Self {
        Self { data }
    }

    #[inline]
    pub(crate) const fn string_vector(string_vector: *mut *mut u8) -> Self {
        Self { string_vector }
    }
}

#[repr(C)]
// Not `Copy`: a directory entry is shared state, and copying one silently
// detached writers from the published slot. Callers borrow the entry, or copy
// the small facts they need (`directory_type()`, `name_bytes()`,
// `data_pointer()`, `scalar_value()`) before taking a mutable borrow.
#[derive(Clone)]
pub struct DirectoryEntry {
    directory_type: TypeCode,
    data: DirectoryData,
    name: [u8; MAX_NAME_BYTES],
}

impl DirectoryEntry {
    #[inline]
    pub(crate) fn new(directory_type: TypeCode, name: NameBytes, data: DirectoryData) -> Self {
        Self {
            directory_type,
            data,
            name: name.0,
        }
    }

    pub(crate) fn name_bytes(&self) -> Result<NameBytes, ProtocolError> {
        let name = NameBytes(self.name);
        name.as_c_str()?;
        Ok(name)
    }

    /// The entry's own published slot address inside the segment mapping.
    ///
    /// Directory entries are shared-memory records: a write must land where the
    /// entry is published, never in a copy. This is the Rust address form of
    /// VPP's `sm->directory_vector[index].value = value` (`stats.c:263-269`) and
    /// `cb[column] = value` (`collector.c:131-151`).
    fn slot(&self) -> *mut Self {
        (self as *const Self).cast_mut()
    }

    pub(crate) fn set_name(&self, name: NameBytes) {
        let slot = self.slot();
        // SAFETY: `slot` is this entry's published address; the structural
        // caller holds the segment lock, so no other structure writer runs.
        unsafe { ptr::write(ptr::addr_of_mut!((*slot).name), name.0) };
    }

    #[inline]
    pub(crate) fn directory_type(&self) -> Result<DirectoryType, ProtocolError> {
        DirectoryType::try_from(self.directory_type)
    }

    pub(crate) fn set_data(&self, data: DirectoryData) {
        let slot = self.slot();
        // SAFETY: `slot` is this entry's published address; the structural
        // caller holds the segment lock, so no other structure writer runs.
        unsafe { ptr::write(ptr::addr_of_mut!((*slot).data), data) };
    }

    #[inline]
    pub(crate) fn directory_index(&self) -> Result<DirectoryIndex, ProtocolError> {
        require_directory_type(self, DirectoryType::Empty)?;
        // SAFETY: the checked Empty kind selects the `index` arm written by the
        // directory owner for the free-slot chain.
        let raw = unsafe { self.data.index };
        let value = u32::try_from(raw)
            .map_err(|_| ProtocolError::EncodedValueOverflow { value: usize::MAX })?;
        Ok(DirectoryIndex(value))
    }

    #[inline]
    pub(crate) fn scalar_value(&self) -> Result<u64, ProtocolError> {
        let actual = self.directory_type()?;
        match actual {
            DirectoryType::ScalarIndex | DirectoryType::Gauge => {
                // SAFETY: the checked scalar kinds select the `value` arm.
                Ok(unsafe { self.data.value })
            }
            actual => Err(ProtocolError::DirectoryDataTypeMismatch {
                expected: DirectoryType::ScalarIndex,
                actual,
            }),
        }
    }

    /// Writes one scalar value into its published slot, like VPP's
    /// `sm->directory_vector[index].value = value`.
    pub fn set_scalar(&self, value: u64) {
        let slot = self.slot();
        // SAFETY: the checked scalar kinds publish their value in this arm; the
        // relaxed store is the whole update, and readers are not promised a
        // cross-entry snapshot.
        unsafe {
            AtomicU64::from_ptr(ptr::addr_of_mut!((*slot).data.value))
                .store(value, Ordering::Relaxed);
        }
    }

    /// Writes one cell of a published simple counter vector, like the cell write
    /// `vlib_stats_set_simple_counter` performs through the published vector.
    ///
    /// The shape must already be published by `validate`: this operation never
    /// expands rows or columns and never allocates. A row or column outside the
    /// published shape is a bug in the collector that owns the entry, so it
    /// asserts with the row and column instead of returning a `Result`.
    pub fn set_simple_counter_cell(&self, row: u32, column: u32, value: u64) {
        let outer = match self.data_pointer() {
            Ok(pointer) => pointer.cast::<*mut u8>(),
            Err(error) => panic!(
                "set_simple_counter: row {row} column {column} is not a simple counter vector: {error}"
            ),
        };
        if outer.is_null() {
            panic!("set_simple_counter: row {row} column {column} has no published rows");
        }
        // SAFETY: a published counter entry owns its outer vector.
        let outer_length = unsafe { crate::segment::vector_length(outer.cast::<u8>()) } as usize;
        if row as usize >= outer_length {
            panic!("set_simple_counter: row {row} is outside the {outer_length} published rows");
        }
        // SAFETY: `row` is inside the outer vector.
        let row_pointer = unsafe { ptr::read(outer.add(row as usize)) };
        if row_pointer.is_null() {
            panic!("set_simple_counter: row {row} is not published");
        }
        // SAFETY: the published outer vector owns this row.
        let row_length = unsafe { crate::segment::vector_length(row_pointer) } as usize;
        if column as usize >= row_length {
            panic!(
                "set_simple_counter: row {row} column {column} is outside the {row_length} published columns"
            );
        }
        // SAFETY: `column` is inside the row vector of `u64` cells; the relaxed
        // store is the whole update and readers are not promised a cross-column
        // snapshot.
        unsafe {
            AtomicU64::from_ptr(row_pointer.cast::<u64>().add(column as usize))
                .store(value, Ordering::Relaxed);
        }
    }

    pub(crate) fn data_pointer(&self) -> Result<*mut c_void, ProtocolError> {
        let actual = self.directory_type()?;
        match actual {
            DirectoryType::CounterVectorSimple
            | DirectoryType::CounterVectorCombined
            | DirectoryType::HistogramLog2
            | DirectoryType::RingBuffer => {
                // SAFETY: the checked data-bearing kinds select the `data` arm.
                Ok(unsafe { self.data.data })
            }
            actual => Err(ProtocolError::DirectoryDataTypeMismatch {
                expected: DirectoryType::CounterVectorSimple,
                actual,
            }),
        }
    }

    pub(crate) fn string_vector_pointer(&self) -> Result<*mut *mut u8, ProtocolError> {
        require_directory_type(self, DirectoryType::NameVector)?;
        // SAFETY: the checked NameVector kind selects the `string_vector` arm.
        Ok(unsafe { self.data.string_vector })
    }
}

fn require_directory_type(
    entry: &DirectoryEntry,
    expected: DirectoryType,
) -> Result<(), ProtocolError> {
    let actual = entry.directory_type()?;
    if actual == expected {
        Ok(())
    } else {
        Err(ProtocolError::DirectoryDataTypeMismatch { expected, actual })
    }
}

pub(crate) const VEC_MIN_ALIGN: usize = hammer_infra::align::VEC_MIN_ALIGN;

#[inline]
pub(crate) const fn vec_header_bytes(
    len: u32,
    hdr_size: u8,
    log2_align: u8,
    default_heap: bool,
    grow_elts: u8,
    vpad: u8,
) -> [u8; 8] {
    let len = len.to_ne_bytes();
    let default_heap_bit = if default_heap { 0x80 } else { 0 };
    [
        len[0],
        len[1],
        len[2],
        len[3],
        hdr_size,
        (log2_align & 0x7f) | default_heap_bit,
        grow_elts,
        vpad,
    ]
}

#[inline]
pub(crate) fn vec_len(header: Option<&[u8; 8]>) -> u32 {
    match header {
        Some(header) => u32::from_ne_bytes([header[0], header[1], header[2], header[3]]),
        None => 0,
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct RingConfig {
    entry_size: u32,
    ring_size: u32,
    n_threads: u32,
    schema_size: u32,
    schema_version: u32,
}

impl RingConfig {
    #[inline]
    pub const fn new(
        entry_size: u32,
        ring_size: u32,
        n_threads: u32,
        schema_size: u32,
        schema_version: u32,
    ) -> Self {
        Self {
            entry_size,
            ring_size,
            n_threads,
            schema_size,
            schema_version,
        }
    }

    #[inline]
    pub fn entry_size(&self) -> u32 {
        // SAFETY: `entry_size` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.entry_size).read_unaligned() }
    }

    #[inline]
    pub fn ring_size(&self) -> u32 {
        // SAFETY: `ring_size` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.ring_size).read_unaligned() }
    }

    #[inline]
    pub fn n_threads(&self) -> u32 {
        // SAFETY: `n_threads` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.n_threads).read_unaligned() }
    }

    #[inline]
    pub fn schema_size(&self) -> u32 {
        // SAFETY: `schema_size` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.schema_size).read_unaligned() }
    }

    #[inline]
    pub(crate) fn schema_version(&self) -> u32 {
        // SAFETY: `schema_version` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.schema_version).read_unaligned() }
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct RingBufferHeader {
    config: RingConfig,
    metadata_offset: u32,
    data_offset: u32,
}

impl RingBufferHeader {
    #[inline]
    pub fn config(&self) -> RingConfig {
        // SAFETY: `config` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.config).read_unaligned() }
    }

    #[inline]
    pub(crate) fn metadata_offset(&self) -> u32 {
        // SAFETY: `metadata_offset` is a field of a live packed record; the
        // copy is explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.metadata_offset).read_unaligned() }
    }

    #[inline]
    pub fn data_offset(&self) -> u32 {
        // SAFETY: `data_offset` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.data_offset).read_unaligned() }
    }
}

#[repr(C)]
pub(crate) struct RingMetadata {
    cacheline0: hammer_infra::align::CacheLineAlignMark,
    head: u32,
    schema_version: u32,
    sequence: u64,
    schema_offset: u32,
    schema_size: u32,
    padding: [u8; hammer_infra::align::CACHE_LINE - 24],
}

impl RingMetadata {
    #[inline]
    pub(crate) const fn new(schema_version: u32, schema_offset: u32, schema_size: u32) -> Self {
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
            head: 0,
            schema_version,
            sequence: 0,
            schema_offset,
            schema_size,
            padding: [0; hammer_infra::align::CACHE_LINE - 24],
        }
    }
}

pub fn ring_layout(
    config: RingConfig,
    mapping_capacity: usize,
) -> Result<(RingBufferHeader, usize), ProtocolError> {
    let cache_line_bytes = hammer_infra::align::CACHE_LINE;
    let entry_size = usize::try_from(config.entry_size())
        .map_err(|_| ProtocolError::EncodedValueOverflow { value: usize::MAX })?;
    let ring_size = usize::try_from(config.ring_size())
        .map_err(|_| ProtocolError::EncodedValueOverflow { value: usize::MAX })?;
    let n_threads = usize::try_from(config.n_threads())
        .map_err(|_| ProtocolError::EncodedValueOverflow { value: usize::MAX })?;
    let schema_size = usize::try_from(config.schema_size())
        .map_err(|_| ProtocolError::EncodedValueOverflow { value: usize::MAX })?;

    if entry_size == 0 || ring_size == 0 {
        return Err(ProtocolError::InvalidRingConfig);
    }
    if core::mem::size_of::<RingMetadata>() != cache_line_bytes
        || core::mem::align_of::<RingMetadata>() != cache_line_bytes
    {
        return Err(ProtocolError::InvalidVectorHeader);
    }

    let data_offset = core::mem::size_of::<RingBufferHeader>();
    let data_size = n_threads
        .checked_mul(ring_size)
        .and_then(|size| size.checked_mul(entry_size))
        .ok_or(ProtocolError::RingSizeOverflow)?;
    let data_end = data_offset
        .checked_add(data_size)
        .ok_or(ProtocolError::RingSizeOverflow)?;
    let metadata_offset = data_end
        .checked_add(cache_line_bytes - 1)
        .ok_or(ProtocolError::RingSizeOverflow)?
        & !(cache_line_bytes - 1);
    let metadata_size = n_threads
        .checked_mul(core::mem::size_of::<RingMetadata>())
        .ok_or(ProtocolError::RingSizeOverflow)?;
    let metadata_end = metadata_offset
        .checked_add(metadata_size)
        .ok_or(ProtocolError::RingSizeOverflow)?;
    let schema_offset = if schema_size == 0 { 0 } else { metadata_end };
    let total = if schema_size == 0 {
        metadata_end
    } else {
        schema_offset
            .checked_add(schema_size)
            .ok_or(ProtocolError::RingSizeOverflow)?
    };

    if total > mapping_capacity {
        return Err(ProtocolError::MappingCapacityExceeded {
            required: total,
            capacity: mapping_capacity,
        });
    }

    let data_offset = u32::try_from(data_offset)
        .map_err(|_| ProtocolError::EncodedValueOverflow { value: data_offset })?;
    let metadata_offset =
        u32::try_from(metadata_offset).map_err(|_| ProtocolError::EncodedValueOverflow {
            value: metadata_offset,
        })?;
    if schema_size != 0 {
        let _schema_offset =
            u32::try_from(schema_offset).map_err(|_| ProtocolError::EncodedValueOverflow {
                value: schema_offset,
            })?;
    }
    let _total =
        u32::try_from(total).map_err(|_| ProtocolError::EncodedValueOverflow { value: total })?;

    let header = RingBufferHeader {
        config,
        metadata_offset,
        data_offset,
    };
    Ok((header, total))
}

pub(crate) const STAT_SEGMENT_VERSION: u64 = 2;
pub(crate) const STAT_SEGMENT_INDEX_INVALID: u32 = u32::MAX;

/// Directory index of the fixed heartbeat slot, as in `STAT_COUNTER_HEARTBEAT`.
pub const STAT_COUNTER_HEARTBEAT: u32 = 0;
/// Directory index of the fixed last-clear slot, as in
/// `STAT_COUNTER_LAST_STATS_CLEAR`.
pub const STAT_COUNTER_LAST_STATS_CLEAR: u32 = 1;
/// Directory index of the fixed boot-time slot, as in `STAT_COUNTER_BOOTTIME`.
pub const STAT_COUNTER_BOOTTIME: u32 = 2;
/// Number of fixed slots the segment owns, as in `STAT_COUNTERS`.
pub(crate) const STAT_COUNTERS: u32 = 3;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SharedHeader {
    version: u64,
    base: *mut core::ffi::c_void,
    pub(crate) epoch: u64,
    pub(super) in_progress: u64,
    pub(crate) directory_vector: *mut DirectoryEntry,
}

impl SharedHeader {
    #[inline]
    pub(crate) const fn new(base: *mut core::ffi::c_void) -> Self {
        Self {
            version: STAT_SEGMENT_VERSION,
            base,
            epoch: 1,
            in_progress: 0,
            directory_vector: core::ptr::null_mut(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Counter {
    pub packets: u64,
    pub bytes: u64,
}

#[cfg(target_pointer_width = "64")]
const _: () = {
    use core::mem::{align_of, offset_of, size_of};

    assert!(size_of::<DirectoryData>() == 8);
    assert!(align_of::<DirectoryData>() == 8);
    assert!(size_of::<DirectoryEntry>() == 144);
    assert!(align_of::<DirectoryEntry>() == 8);
    assert!(offset_of!(DirectoryEntry, directory_type) == 0);
    assert!(offset_of!(DirectoryEntry, data) == 8);
    assert!(offset_of!(DirectoryEntry, name) == 16);
    assert!(size_of::<NameBytes>() == 128);
    assert!(size_of::<SharedHeader>() == 40);
    assert!(align_of::<SharedHeader>() == 8);
    assert!(offset_of!(SharedHeader, version) == 0);
    assert!(offset_of!(SharedHeader, base) == 8);
    assert!(offset_of!(SharedHeader, epoch) == 16);
    assert!(offset_of!(SharedHeader, in_progress) == 24);
    assert!(offset_of!(SharedHeader, directory_vector) == 32);
    assert!(size_of::<Counter>() == 16);
    assert!(offset_of!(Counter, packets) == 0);
    assert!(offset_of!(Counter, bytes) == 8);
    assert!(size_of::<RingConfig>() == 20);
    assert!(align_of::<RingConfig>() == 1);
    assert!(size_of::<RingBufferHeader>() == 28);
    assert!(align_of::<RingBufferHeader>() == 1);
    assert!(offset_of!(RingBufferHeader, config) == 0);
    assert!(offset_of!(RingBufferHeader, metadata_offset) == 20);
    assert!(offset_of!(RingBufferHeader, data_offset) == 24);
    assert!(size_of::<RingMetadata>() == 64);
    assert!(align_of::<RingMetadata>() == 64);
    assert!(size_of::<hammer_infra::align::CacheLineAlignMark>() == 0);
    assert!(align_of::<hammer_infra::align::CacheLineAlignMark>() == 64);
    assert!(offset_of!(RingMetadata, head) == 0);
    assert!(offset_of!(RingMetadata, sequence) == 8);
    assert!(offset_of!(RingMetadata, schema_offset) == 16);
    assert!(size_of::<[u8; 8]>() == 8);
    assert!(VEC_MIN_ALIGN == 8);
    assert!(VEC_MIN_ALIGN == hammer_infra::align::VEC_MIN_ALIGN);
};

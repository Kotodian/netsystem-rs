use std::ffi::CStr;
pub(crate) const MAX_NAME_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Error {
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
    InvalidCacheLine,
    RingSizeOverflow,
    MappingCapacityExceeded {
        required: usize,
        capacity: usize,
    },
    WireValueOverflow {
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

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NameBytes([u8; MAX_NAME_BYTES]);

impl TryFrom<&[u8]> for NameBytes {
    type Error = Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() > MAX_NAME_BYTES {
            return Err(Error::NameTooLong {
                length: value.len(),
            });
        }

        if let Some(nul) = value.iter().position(|byte| *byte == 0) {
            if nul > MAX_NAME_BYTES - 2 {
                return Err(Error::NameTooLong { length: nul });
            }
            if value[nul + 1..].iter().any(|byte| *byte != 0) {
                return Err(Error::InvalidNameNul);
            }

            let mut bytes = [0u8; MAX_NAME_BYTES];
            bytes[..=nul].copy_from_slice(&value[..=nul]);
            return Ok(Self(bytes));
        }

        if value.len() > MAX_NAME_BYTES - 2 {
            if value.len() == MAX_NAME_BYTES - 1 {
                return Err(Error::NameTooLong {
                    length: value.len(),
                });
            }
            return Err(Error::MissingNameTerminator);
        }

        let mut bytes = [0u8; MAX_NAME_BYTES];
        bytes[..value.len()].copy_from_slice(value);
        Ok(Self(bytes))
    }
}

impl TryFrom<&str> for NameBytes {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.as_bytes().contains(&0) {
            return Err(Error::InvalidNameNul);
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
    pub(crate) fn as_c_str(&self) -> Result<&CStr, Error> {
        let Some(nul) = self.0.iter().position(|byte| *byte == 0) else {
            return Err(Error::MissingNameTerminator);
        };
        if nul > MAX_NAME_BYTES - 2 {
            return Err(Error::NameTooLong { length: nul });
        }
        if self.0[nul + 1..].iter().any(|byte| *byte != 0) {
            return Err(Error::InvalidNamePadding);
        }
        CStr::from_bytes_until_nul(&self.0).map_err(|_| Error::MissingNameTerminator)
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self.0.iter().position(|byte| *byte == 0) {
            Some(length) => length,
            None => MAX_NAME_BYTES,
        }
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TypeCode(u32);

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum DirectoryType {
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

impl TryFrom<u32> for DirectoryType {
    type Error = Error;

    #[inline]
    fn try_from(raw: u32) -> Result<Self, Self::Error> {
        Self::try_from(TypeCode::from(raw))
    }
}

impl TryFrom<TypeCode> for DirectoryType {
    type Error = Error;

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
            raw => Err(Error::UnknownDirectoryType { raw }),
        }
    }
}

impl TypeCode {
    #[inline]
    pub(crate) const fn raw(self) -> u32 {
        self.0
    }

    #[inline]
    pub(crate) const fn is_known(self) -> bool {
        self.0 <= DirectoryType::Gauge as u32
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
pub(crate) struct DirectoryIndex(u32);

impl DirectoryIndex {
    #[inline]
    pub(crate) const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline]
    pub(crate) const fn raw(self) -> u32 {
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

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct Gauge(u64);

impl From<u64> for Gauge {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DirectoryDataPointer(*mut core::ffi::c_void);

impl DirectoryDataPointer {
    #[inline]
    pub(crate) const fn as_ptr(self) -> *mut core::ffi::c_void {
        self.0
    }
}

impl From<*mut core::ffi::c_void> for DirectoryDataPointer {
    #[inline]
    fn from(value: *mut core::ffi::c_void) -> Self {
        Self(value)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StringVectorPointer(*mut *mut u8);

impl From<*mut *mut u8> for StringVectorPointer {
    #[inline]
    fn from(value: *mut *mut u8) -> Self {
        Self(value)
    }
}

impl StringVectorPointer {
    #[inline]
    pub(crate) const fn as_ptr(self) -> *mut *mut u8 {
        self.0
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

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct DirectoryEntry {
    directory_type: TypeCode,
    data: DirectoryData,
    name: [u8; MAX_NAME_BYTES],
}

impl From<DirectoryIndex> for DirectoryData {
    #[inline]
    fn from(value: DirectoryIndex) -> Self {
        Self {
            index: u64::from(value.0),
        }
    }
}

impl From<SymlinkIndex> for DirectoryData {
    #[inline]
    fn from(value: SymlinkIndex) -> Self {
        Self { indices: value }
    }
}

impl From<ScalarBits> for DirectoryData {
    #[inline]
    fn from(value: ScalarBits) -> Self {
        Self { value: value.0 }
    }
}

impl From<Gauge> for DirectoryData {
    #[inline]
    fn from(value: Gauge) -> Self {
        Self { value: value.0 }
    }
}

impl From<DirectoryDataPointer> for DirectoryData {
    #[inline]
    fn from(value: DirectoryDataPointer) -> Self {
        Self { data: value.0 }
    }
}

impl From<StringVectorPointer> for DirectoryData {
    #[inline]
    fn from(value: StringVectorPointer) -> Self {
        Self {
            string_vector: value.0,
        }
    }
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

    #[inline]
    pub(crate) fn kind(&self) -> TypeCode {
        self.directory_type
    }

    pub(crate) fn name(&self) -> Result<&CStr, Error> {
        let checked = NameBytes(self.name);
        checked.as_c_str()?;
        CStr::from_bytes_until_nul(&self.name).map_err(|_| Error::MissingNameTerminator)
    }

    pub(crate) fn name_bytes(&self) -> Result<NameBytes, Error> {
        let name = NameBytes(self.name);
        name.as_c_str()?;
        Ok(name)
    }

    #[inline]
    pub(crate) fn set_name(&mut self, name: NameBytes) {
        self.name = name.0;
    }
}

fn require_directory_type(entry: &DirectoryEntry, expected: DirectoryType) -> Result<(), Error> {
    let actual = DirectoryType::try_from(entry.kind())?;
    if actual == expected {
        Ok(())
    } else {
        Err(Error::DirectoryDataTypeMismatch { expected, actual })
    }
}

impl TryFrom<&DirectoryEntry> for DirectoryIndex {
    type Error = Error;

    #[inline]
    fn try_from(entry: &DirectoryEntry) -> Result<Self, Self::Error> {
        require_directory_type(entry, DirectoryType::Empty)?;
        // SAFETY: The entry is a live, aligned mapped record and its checked
        // Empty kind selects the VPP `index` arm initialized by the writer.
        let raw = unsafe { entry.data.index };
        let value =
            usize::try_from(raw).map_err(|_| Error::WireValueOverflow { value: usize::MAX })?;
        let value = u32::try_from(value).map_err(|_| Error::WireValueOverflow { value })?;
        Ok(Self(value))
    }
}

impl TryFrom<&DirectoryEntry> for SymlinkIndex {
    type Error = Error;

    #[inline]
    fn try_from(entry: &DirectoryEntry) -> Result<Self, Self::Error> {
        require_directory_type(entry, DirectoryType::Symlink)?;
        // SAFETY: The entry is a live, aligned mapped record and its checked
        // Symlink kind selects the initialized `indices` arm.
        Ok(unsafe { entry.data.indices })
    }
}

impl TryFrom<&DirectoryEntry> for ScalarBits {
    type Error = Error;

    #[inline]
    fn try_from(entry: &DirectoryEntry) -> Result<Self, Self::Error> {
        require_directory_type(entry, DirectoryType::ScalarIndex)?;
        // SAFETY: The entry is a live, aligned mapped record and its checked
        // ScalarIndex kind selects the VPP timestamp `value` arm.
        Ok(Self(unsafe { entry.data.value }))
    }
}

impl TryFrom<&DirectoryEntry> for Gauge {
    type Error = Error;

    #[inline]
    fn try_from(entry: &DirectoryEntry) -> Result<Self, Self::Error> {
        require_directory_type(entry, DirectoryType::Gauge)?;
        // SAFETY: The entry is a live, aligned mapped record and its checked
        // Gauge kind selects the initialized `value` arm.
        Ok(Self(unsafe { entry.data.value }))
    }
}

impl TryFrom<&DirectoryEntry> for DirectoryDataPointer {
    type Error = Error;

    #[inline]
    fn try_from(entry: &DirectoryEntry) -> Result<Self, Self::Error> {
        let actual = DirectoryType::try_from(entry.kind())?;
        match actual {
            DirectoryType::CounterVectorSimple
            | DirectoryType::CounterVectorCombined
            | DirectoryType::HistogramLog2
            | DirectoryType::RingBuffer => {
                // SAFETY: The entry is a live, aligned mapped record and its
                // checked data-bearing kind selects the initialized `data` arm.
                Ok(Self(unsafe { entry.data.data }))
            }
            actual => Err(Error::DirectoryDataTypeMismatch {
                expected: DirectoryType::CounterVectorSimple,
                actual,
            }),
        }
    }
}

impl TryFrom<&DirectoryEntry> for StringVectorPointer {
    type Error = Error;

    #[inline]
    fn try_from(entry: &DirectoryEntry) -> Result<Self, Self::Error> {
        require_directory_type(entry, DirectoryType::NameVector)?;
        // SAFETY: The entry is a live, aligned mapped record and its checked
        // NameVector kind selects the initialized `string_vector` arm.
        Ok(Self(unsafe { entry.data.string_vector }))
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ScalarBits(u64);

impl From<f64> for ScalarBits {
    #[inline]
    fn from(value: f64) -> Self {
        Self(value.to_bits())
    }
}

impl From<ScalarBits> for f64 {
    #[inline]
    fn from(bits: ScalarBits) -> Self {
        f64::from_bits(bits.0)
    }
}

impl From<u64> for ScalarBits {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<ScalarBits> for u64 {
    #[inline]
    fn from(bits: ScalarBits) -> Self {
        bits.0
    }
}

pub(crate) const VEC_MIN_ALIGN: usize = 8;

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

pub(crate) fn vector_element_offset(
    header_offset: usize,
    vector_offset: usize,
    header: &[u8; 8],
    index: usize,
    element_size: usize,
    mapping_capacity: usize,
) -> Result<usize, Error> {
    let log2_align = u32::from(header[5] & 0x7f);
    if log2_align >= usize::BITS {
        return Err(Error::InvalidVectorHeader);
    }
    let vector_alignment = 1usize << log2_align;
    if vector_alignment < VEC_MIN_ALIGN
        || !header_offset.is_multiple_of(VEC_MIN_ALIGN)
        || !vector_offset.is_multiple_of(vector_alignment)
    {
        return Err(Error::InvalidVectorHeader);
    }
    let header_size = if header[4] == 0 {
        return Err(Error::InvalidVectorHeader);
    } else {
        usize::from(header[4])
            .checked_mul(VEC_MIN_ALIGN)
            .ok_or(Error::OffsetOverflow)?
    };
    let header_end = header_offset
        .checked_add(header_size)
        .ok_or(Error::OffsetOverflow)?;
    if header_end != vector_offset {
        return Err(Error::OffsetOutOfBounds {
            offset: vector_offset,
            capacity: mapping_capacity,
        });
    }
    if header_end > mapping_capacity {
        return Err(Error::SpanOutOfBounds {
            offset: header_offset,
            length: header_size,
            capacity: mapping_capacity,
        });
    }
    if element_size == 0 {
        return Err(Error::ElementSizeZero);
    }
    let length = usize::try_from(vec_len(Some(header)))
        .map_err(|_| Error::WireValueOverflow { value: usize::MAX })?;
    let vector_size = length
        .checked_mul(element_size)
        .ok_or(Error::OffsetOverflow)?;
    let vector_end = vector_offset
        .checked_add(vector_size)
        .ok_or(Error::OffsetOverflow)?;
    if vector_end > mapping_capacity {
        return Err(Error::SpanOutOfBounds {
            offset: vector_offset,
            length: vector_size,
            capacity: mapping_capacity,
        });
    }
    if index >= length {
        return Err(Error::ElementOutOfBounds { index, length });
    }
    let offset = index
        .checked_mul(element_size)
        .and_then(|element_offset| vector_offset.checked_add(element_offset))
        .ok_or(Error::OffsetOverflow)?;
    let element_end = offset
        .checked_add(element_size)
        .ok_or(Error::OffsetOverflow)?;
    if element_end > mapping_capacity {
        return Err(Error::SpanOutOfBounds {
            offset,
            length: element_size,
            capacity: mapping_capacity,
        });
    }
    Ok(offset)
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct RingConfig {
    entry_size: u32,
    ring_size: u32,
    n_threads: u32,
    schema_size: u32,
    schema_version: u32,
}

impl RingConfig {
    #[inline]
    pub(crate) const fn new(
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
    pub(crate) fn entry_size(&self) -> u32 {
        // SAFETY: `entry_size` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.entry_size).read_unaligned() }
    }

    #[inline]
    pub(crate) fn set_entry_size(&mut self, value: u32) {
        // SAFETY: `entry_size` is a field of a live packed record; the write is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.entry_size).write_unaligned(value) }
    }

    #[inline]
    pub(crate) fn ring_size(&self) -> u32 {
        // SAFETY: `ring_size` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.ring_size).read_unaligned() }
    }

    #[inline]
    pub(crate) fn set_ring_size(&mut self, value: u32) {
        // SAFETY: `ring_size` is a field of a live packed record; the write is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.ring_size).write_unaligned(value) }
    }

    #[inline]
    pub(crate) fn n_threads(&self) -> u32 {
        // SAFETY: `n_threads` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.n_threads).read_unaligned() }
    }

    #[inline]
    pub(crate) fn set_n_threads(&mut self, value: u32) {
        // SAFETY: `n_threads` is a field of a live packed record; the write is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.n_threads).write_unaligned(value) }
    }

    #[inline]
    pub(crate) fn schema_size(&self) -> u32 {
        // SAFETY: `schema_size` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.schema_size).read_unaligned() }
    }

    #[inline]
    pub(crate) fn set_schema_size(&mut self, value: u32) {
        // SAFETY: `schema_size` is a field of a live packed record; the write is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.schema_size).write_unaligned(value) }
    }

    #[inline]
    pub(crate) fn schema_version(&self) -> u32 {
        // SAFETY: `schema_version` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.schema_version).read_unaligned() }
    }

    #[inline]
    pub(crate) fn set_schema_version(&mut self, value: u32) {
        // SAFETY: `schema_version` is a field of a live packed record; the write is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.schema_version).write_unaligned(value) }
    }
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
pub(crate) struct RingBufferHeader {
    config: RingConfig,
    metadata_offset: u32,
    data_offset: u32,
}

impl RingBufferHeader {
    #[inline]
    pub(crate) const fn new(config: RingConfig, metadata_offset: u32, data_offset: u32) -> Self {
        Self {
            config,
            metadata_offset,
            data_offset,
        }
    }

    #[inline]
    pub(crate) fn config(&self) -> RingConfig {
        // SAFETY: `config` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.config).read_unaligned() }
    }

    #[inline]
    pub(crate) fn set_config(&mut self, value: RingConfig) {
        // SAFETY: `config` is a field of a live packed record; the write is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.config).write_unaligned(value) }
    }

    #[inline]
    pub(crate) fn metadata_offset(&self) -> u32 {
        // SAFETY: `metadata_offset` is a field of a live packed record; the
        // copy is explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.metadata_offset).read_unaligned() }
    }

    #[inline]
    pub(crate) fn set_metadata_offset(&mut self, value: u32) {
        // SAFETY: `metadata_offset` is a field of a live packed record; the
        // write is explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.metadata_offset).write_unaligned(value) }
    }

    #[inline]
    pub(crate) fn data_offset(&self) -> u32 {
        // SAFETY: `data_offset` is a field of a live packed record; the copy is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of!(self.data_offset).read_unaligned() }
    }

    #[inline]
    pub(crate) fn set_data_offset(&mut self, value: u32) {
        // SAFETY: `data_offset` is a field of a live packed record; the write is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.data_offset).write_unaligned(value) }
    }
}

#[repr(C, align(64))]
pub(crate) struct RingMetadata {
    head: u32,
    schema_version: u32,
    sequence: u64,
    schema_offset: u32,
    schema_size: u32,
    padding: [u8; 40],
}

impl RingMetadata {
    #[inline]
    pub(crate) const fn new(schema_version: u32, schema_offset: u32, schema_size: u32) -> Self {
        Self {
            head: 0,
            schema_version,
            sequence: 0,
            schema_offset,
            schema_size,
            padding: [0; 40],
        }
    }

    #[inline]
    pub(crate) fn head(&self) -> u32 {
        self.head
    }

    #[inline]
    pub(crate) fn set_head(&mut self, value: u32) {
        self.head = value;
    }

    #[inline]
    pub(crate) fn schema_version(&self) -> u32 {
        self.schema_version
    }

    #[inline]
    pub(crate) fn set_schema_version(&mut self, value: u32) {
        self.schema_version = value;
    }

    #[inline]
    pub(crate) fn sequence(&self) -> u64 {
        self.sequence
    }

    #[inline]
    pub(crate) fn set_sequence(&mut self, value: u64) {
        self.sequence = value;
    }

    #[inline]
    pub(crate) fn schema_offset(&self) -> u32 {
        self.schema_offset
    }

    #[inline]
    pub(crate) fn set_schema_offset(&mut self, value: u32) {
        self.schema_offset = value;
    }

    #[inline]
    pub(crate) fn schema_size(&self) -> u32 {
        self.schema_size
    }

    #[inline]
    pub(crate) fn set_schema_size(&mut self, value: u32) {
        self.schema_size = value;
    }
}

pub(crate) fn ring_layout(
    config: RingConfig,
    cache_line_bytes: usize,
    mapping_capacity: usize,
) -> Result<(RingBufferHeader, usize), Error> {
    let entry_size = usize::try_from(config.entry_size())
        .map_err(|_| Error::WireValueOverflow { value: usize::MAX })?;
    let ring_size = usize::try_from(config.ring_size())
        .map_err(|_| Error::WireValueOverflow { value: usize::MAX })?;
    let n_threads = usize::try_from(config.n_threads())
        .map_err(|_| Error::WireValueOverflow { value: usize::MAX })?;
    let schema_size = usize::try_from(config.schema_size())
        .map_err(|_| Error::WireValueOverflow { value: usize::MAX })?;

    if entry_size == 0 || ring_size == 0 {
        return Err(Error::InvalidRingConfig);
    }
    if cache_line_bytes < core::mem::align_of::<RingMetadata>()
        || !cache_line_bytes.is_power_of_two()
    {
        return Err(Error::InvalidCacheLine);
    }

    let data_offset = core::mem::size_of::<RingBufferHeader>();
    let data_size = n_threads
        .checked_mul(ring_size)
        .and_then(|size| size.checked_mul(entry_size))
        .ok_or(Error::RingSizeOverflow)?;
    let data_end = data_offset
        .checked_add(data_size)
        .ok_or(Error::RingSizeOverflow)?;
    let metadata_offset = data_end
        .checked_add(cache_line_bytes - 1)
        .ok_or(Error::RingSizeOverflow)?
        & !(cache_line_bytes - 1);
    let metadata_size = n_threads
        .checked_mul(core::mem::size_of::<RingMetadata>())
        .ok_or(Error::RingSizeOverflow)?;
    let metadata_end = metadata_offset
        .checked_add(metadata_size)
        .ok_or(Error::RingSizeOverflow)?;
    let schema_offset = if schema_size == 0 { 0 } else { metadata_end };
    let total = if schema_size == 0 {
        metadata_end
    } else {
        schema_offset
            .checked_add(schema_size)
            .ok_or(Error::RingSizeOverflow)?
    };

    if total > mapping_capacity {
        return Err(Error::MappingCapacityExceeded {
            required: total,
            capacity: mapping_capacity,
        });
    }

    let data_offset =
        u32::try_from(data_offset).map_err(|_| Error::WireValueOverflow { value: data_offset })?;
    let metadata_offset = u32::try_from(metadata_offset).map_err(|_| Error::WireValueOverflow {
        value: metadata_offset,
    })?;
    if schema_size != 0 {
        let _schema_offset =
            u32::try_from(schema_offset).map_err(|_| Error::WireValueOverflow {
                value: schema_offset,
            })?;
    }
    let _total = u32::try_from(total).map_err(|_| Error::WireValueOverflow { value: total })?;

    let header = RingBufferHeader {
        config,
        metadata_offset,
        data_offset,
    };
    Ok((header, total))
}

pub(crate) const STAT_SEGMENT_VERSION: u64 = 2;
pub(crate) const STAT_SEGMENT_INDEX_INVALID: u32 = u32::MAX;
pub(crate) const STAT_COUNTER_HEARTBEAT: u32 = 0;
pub(crate) const STAT_COUNTER_LAST_STATS_CLEAR: u32 = 1;
pub(crate) const STAT_COUNTER_BOOTTIME: u32 = 2;

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct SharedHeader {
    version: u64,
    base: *mut core::ffi::c_void,
    epoch: u64,
    pub(super) in_progress: u64,
    directory_vector: *mut DirectoryEntry,
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

    #[inline]
    pub(crate) fn validate_version(&self) -> Result<(), Error> {
        if self.version == STAT_SEGMENT_VERSION {
            Ok(())
        } else {
            Err(Error::InvalidVersion {
                actual: self.version,
            })
        }
    }

    #[inline]
    pub(crate) fn is_write_in_progress(&self) -> bool {
        self.in_progress != 0
    }

    #[inline]
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    #[inline]
    pub(crate) fn set_directory_vector(&mut self, value: *mut DirectoryEntry) {
        self.directory_vector = value;
    }

    #[inline]
    pub(crate) fn set_in_progress(&mut self, writing: bool) {
        self.in_progress = u64::from(writing);
    }

    #[inline]
    pub(crate) fn set_epoch(&mut self, value: u64) {
        self.epoch = value;
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Counter {
    pub(crate) packets: u64,
    pub(crate) bytes: u64,
}

impl Counter {
    #[inline]
    pub(crate) fn wrapping_add(self, other: Self) -> Self {
        Self {
            packets: self.packets.wrapping_add(other.packets),
            bytes: self.bytes.wrapping_add(other.bytes),
        }
    }
}

#[cfg(target_pointer_width = "64")]
const _: () = {
    use core::mem::{align_of, offset_of, size_of};

    assert!(size_of::<DirectoryData>() == 8);
    assert!(align_of::<DirectoryData>() == 8);
    assert!(size_of::<DirectoryEntry>() == 144);
    assert!(offset_of!(DirectoryEntry, directory_type) == 0);
    assert!(offset_of!(DirectoryEntry, data) == 8);
    assert!(offset_of!(DirectoryEntry, name) == 16);
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
    assert!(size_of::<RingMetadata>() == 64);
    assert!(align_of::<RingMetadata>() == 64);
    assert!(size_of::<[u8; 8]>() == 8);
    assert!(VEC_MIN_ALIGN == 8);
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_preserve_bytes_and_enforce_vpp_terminator_rules() {
        let empty = NameBytes::try_from(&[][..]).expect("empty name is valid");
        assert_eq!(empty.len(), 0);
        assert_eq!(
            empty.as_c_str().expect("name is terminated").to_bytes(),
            b""
        );
        assert!(empty.as_ref()[1..].iter().all(|byte| *byte == 0));

        let raw = [0x80, 0xff, 0x01, 0x7f];
        let name = NameBytes::try_from(&raw[..]).expect("byte names are not UTF-8 constrained");
        assert_eq!(name.len(), raw.len());
        assert_eq!(&name.as_ref()[..raw.len()], &raw);
        assert_eq!(name.as_ref()[raw.len()], 0);
        assert!(name.as_ref()[raw.len() + 1..].iter().all(|byte| *byte == 0));

        let max = [b'x'; 126];
        let max_name = NameBytes::try_from(&max[..]).expect("126 bytes is valid");
        assert_eq!(max_name.len(), 126);
        assert_eq!(max_name.as_ref()[126], 0);
        assert!(NameBytes::try_from(&[b'x'; 127][..]).is_err());

        let mut wire = [0u8; 128];
        wire[..126].fill(b'w');
        assert_eq!(
            NameBytes::try_from(&wire[..])
                .expect("wire padding is valid")
                .len(),
            126
        );

        assert!(NameBytes::try_from(&[b'a', 0, b'b'][..]).is_err());
        assert!(NameBytes::try_from(&[b'a'; 128][..]).is_err());
        assert!(NameBytes::try_from(&[b'a'; 129][..]).is_err());
        assert_eq!(
            NameBytes::try_from("hello").expect("str conversion").len(),
            5
        );
    }

    #[test]
    fn packed_ring_layout_matches_vpp_offsets_and_checks_sizes() {
        let mut config = RingConfig::new(16, 4, 2, 5, 7);
        config.set_entry_size(16);
        assert_eq!(config.entry_size(), 16);
        assert_eq!(config.ring_size(), 4);
        assert_eq!(config.n_threads(), 2);
        assert_eq!(config.schema_size(), 5);
        assert_eq!(config.schema_version(), 7);

        let (header, total) = ring_layout(config, 64, 512).expect("valid ring layout");
        assert_eq!(header.config().entry_size(), 16);
        assert_eq!(
            header.data_offset(),
            std::mem::size_of::<RingBufferHeader>() as u32
        );
        assert_eq!(header.metadata_offset(), 192);
        assert_eq!(total, 325);

        let schema_offset = header.metadata_offset() as usize
            + config.n_threads() as usize * std::mem::size_of::<RingMetadata>();
        assert_eq!(schema_offset, 320);
        let mut metadata = RingMetadata::new(
            config.schema_version(),
            schema_offset as u32,
            config.schema_size(),
        );
        metadata.set_head(3);
        metadata.set_sequence(9);
        assert_eq!(metadata.head(), 3);
        assert_eq!(metadata.schema_version(), 7);
        assert_eq!(metadata.sequence(), 9);
        assert_eq!(metadata.schema_offset(), 320);
        assert_eq!(metadata.schema_size(), 5);

        assert!(matches!(
            ring_layout(RingConfig::new(0, 4, 2, 0, 0), 64, 512),
            Err(Error::InvalidRingConfig)
        ));
        assert!(matches!(
            ring_layout(RingConfig::new(16, 0, 2, 0, 0), 64, 512),
            Err(Error::InvalidRingConfig)
        ));
        assert!(matches!(
            ring_layout(config, 0, 512),
            Err(Error::InvalidCacheLine)
        ));
        assert!(matches!(
            ring_layout(config, 64, 324),
            Err(Error::MappingCapacityExceeded {
                required: 325,
                capacity: 324,
            })
        ));
        assert!(matches!(
            ring_layout(
                RingConfig::new(u32::MAX, u32::MAX, u32::MAX, 0, 0),
                64,
                usize::MAX
            ),
            Err(Error::RingSizeOverflow)
        ));
    }

    #[test]
    fn vector_header_bytes_lengths_and_bounds_follow_vpp() {
        let bytes = vec_header_bytes(3, 1, 3, true, 0, 0);
        assert_eq!(&bytes[..4], &3u32.to_ne_bytes());
        assert_eq!(bytes[4], 1);
        assert_eq!(bytes[5], 0x83);
        assert_eq!(bytes[6], 0);
        assert_eq!(bytes[7], 0);
        let over_aligned = vec_header_bytes(3, 1, 4, true, 0, 0);
        assert!(vector_element_offset(0, 8, &over_aligned, 0, 4, 20).is_err());
        let under_aligned = vec_header_bytes(3, 1, 2, true, 0, 0);
        assert!(vector_element_offset(0, 8, &under_aligned, 0, 4, 20).is_err());
        let invalid_alignment = vec_header_bytes(3, 1, 0x7f, true, 0, 0);
        assert!(vector_element_offset(0, 8, &invalid_alignment, 0, 4, 20).is_err());
        assert_eq!(vec_len(None), 0);
        assert_eq!(vec_len(Some(&bytes)), 3);

        assert_eq!(vector_element_offset(0, 8, &bytes, 0, 4, 20), Ok(8));
        assert_eq!(vector_element_offset(0, 8, &bytes, 2, 4, 20), Ok(16));
        assert!(vector_element_offset(0, 8, &bytes, 3, 4, 20).is_err());
        assert!(vector_element_offset(0, 7, &bytes, 0, 4, 20).is_err());
        assert!(vector_element_offset(0, 8, &bytes, 0, 0, 20).is_err());
        assert!(vector_element_offset(0, 8, &bytes, 2, 4, 19).is_err());
    }

    #[test]
    fn shared_header_and_counter_follow_vpp_wire_semantics() {
        assert_eq!(STAT_SEGMENT_VERSION, 2);
        assert_eq!(STAT_SEGMENT_INDEX_INVALID, u32::MAX);
        assert_eq!(STAT_COUNTER_HEARTBEAT, 0);
        assert_eq!(STAT_COUNTER_LAST_STATS_CLEAR, 1);
        assert_eq!(STAT_COUNTER_BOOTTIME, 2);

        let header = SharedHeader {
            version: STAT_SEGMENT_VERSION,
            base: std::ptr::null_mut(),
            epoch: 17,
            in_progress: 0,
            directory_vector: std::ptr::null_mut(),
        };
        assert_eq!(header.validate_version(), Ok(()));
        assert!(!header.is_write_in_progress());
        assert_eq!(header.epoch(), 17);

        let writing = SharedHeader {
            in_progress: 1,
            ..header
        };
        assert!(writing.is_write_in_progress());

        let invalid = SharedHeader {
            version: STAT_SEGMENT_VERSION + 1,
            ..header
        };
        assert_eq!(
            invalid.validate_version(),
            Err(Error::InvalidVersion {
                actual: STAT_SEGMENT_VERSION + 1
            })
        );

        let left = Counter {
            packets: u64::MAX,
            bytes: u64::MAX - 1,
        };
        let right = Counter {
            packets: 2,
            bytes: 3,
        };
        assert_eq!(
            left.wrapping_add(right),
            Counter {
                packets: 1,
                bytes: 1,
            }
        );
    }

    #[test]
    fn directory_payloads_are_checked_by_kind() {
        let name = NameBytes::try_from("alpha").expect("test name is valid");

        let symlink = SymlinkIndex {
            entry_index: 11,
            vector_index: 3,
        };
        let symlink_entry =
            DirectoryEntry::new(DirectoryType::Symlink.into(), name, symlink.into());
        assert_eq!(SymlinkIndex::try_from(&symlink_entry), Ok(symlink));
        assert!(Gauge::try_from(&symlink_entry).is_err());

        let index_entry =
            DirectoryEntry::new(DirectoryType::Empty.into(), name, DirectoryIndex(42).into());
        assert_eq!(
            DirectoryIndex::try_from(&index_entry),
            Ok(DirectoryIndex(42))
        );

        let scalar = ScalarBits::from(f64::from_bits(0x7ff8_0000_0000_0042));
        let scalar_entry =
            DirectoryEntry::new(DirectoryType::ScalarIndex.into(), name, scalar.into());
        assert_eq!(ScalarBits::try_from(&scalar_entry), Ok(scalar));

        let gauge = Gauge(7);
        let gauge_entry = DirectoryEntry::new(DirectoryType::Gauge.into(), name, gauge.into());
        assert_eq!(Gauge::try_from(&gauge_entry), Ok(gauge));

        let data_pointer = DirectoryDataPointer(std::ptr::null_mut());
        let data_entry = DirectoryEntry::new(
            DirectoryType::CounterVectorSimple.into(),
            name,
            data_pointer.into(),
        );
        assert_eq!(
            DirectoryDataPointer::try_from(&data_entry),
            Ok(data_pointer)
        );

        let string_pointer = StringVectorPointer(std::ptr::null_mut());
        let string_entry = DirectoryEntry::new(
            DirectoryType::NameVector.into(),
            name,
            string_pointer.into(),
        );
        assert_eq!(
            StringVectorPointer::try_from(&string_entry),
            Ok(string_pointer)
        );

        assert_eq!(string_entry.kind(), DirectoryType::NameVector.into());
        assert_eq!(
            string_entry
                .name()
                .expect("stored name is valid")
                .to_bytes(),
            b"alpha"
        );
        assert!(DirectoryIndex::try_from(&string_entry).is_err());
        let mut renamed = string_entry;
        renamed.set_name(NameBytes::try_from("beta").expect("test name is valid"));
        assert_eq!(
            renamed.name().expect("renamed name is valid").to_bytes(),
            b"beta"
        );
    }

    #[test]
    fn scalar_bits_preserve_ieee_bits_through_all_interfaces() {
        let values = [
            0.0f64,
            -0.0,
            1.5,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::from_bits(0x7ff8_0000_0000_0042),
        ];

        for value in values {
            let bits = ScalarBits::from(value);
            assert_eq!(u64::from(bits), value.to_bits());
            assert_eq!(f64::from(bits).to_bits(), value.to_bits());
        }

        let raw = 0x0123_4567_89ab_cdef;
        let bits = ScalarBits::from(raw);
        assert_eq!(u64::from(bits), raw);
    }

    #[test]
    fn directory_kinds_preserve_raw_words_and_names() {
        let kinds = [
            DirectoryType::Illegal,
            DirectoryType::ScalarIndex,
            DirectoryType::CounterVectorSimple,
            DirectoryType::CounterVectorCombined,
            DirectoryType::NameVector,
            DirectoryType::Empty,
            DirectoryType::Symlink,
            DirectoryType::HistogramLog2,
            DirectoryType::RingBuffer,
            DirectoryType::Gauge,
        ];

        for (raw, kind) in kinds.into_iter().enumerate() {
            let code = TypeCode::from(raw as u32);
            assert_eq!(u32::from(code), raw as u32);
            assert_eq!(DirectoryType::try_from(code), Ok(kind));
            assert_eq!(<&'static str>::from(kind), kind_name(raw as u32));
        }

        let unknown = TypeCode::from(99);
        assert_eq!(unknown.raw(), 99);
        assert!(!unknown.is_known());
        assert!(DirectoryType::try_from(unknown).is_err());
    }

    fn kind_name(raw: u32) -> &'static str {
        match raw {
            0 => "illegal",
            1 => "scalar_index",
            2 => "counter_vector_simple",
            3 => "counter_vector_combined",
            4 => "name_vector",
            5 => "empty",
            6 => "symlink",
            7 => "histogram_log2",
            8 => "ring_buffer",
            9 => "gauge",
            _ => "unknown",
        }
    }
}

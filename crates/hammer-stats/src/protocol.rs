use std::ffi::CStr;
pub(crate) const MAX_NAME_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
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
pub struct Gauge(u64);

impl From<u64> for Gauge {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DirectoryDataPointer(*mut core::ffi::c_void);

impl DirectoryDataPointer {
    #[inline]
    pub const fn as_ptr(self) -> *mut core::ffi::c_void {
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
pub struct StringVectorPointer(*mut *mut u8);

impl From<*mut *mut u8> for StringVectorPointer {
    #[inline]
    fn from(value: *mut *mut u8) -> Self {
        Self(value)
    }
}

impl StringVectorPointer {
    #[inline]
    pub const fn as_ptr(self) -> *mut *mut u8 {
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
pub struct DirectoryEntry {
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
    pub fn kind(&self) -> u32 {
        self.directory_type.raw()
    }

    pub fn name(&self) -> Result<&CStr, Error> {
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

    #[inline]
    pub(crate) fn scalar_value(&self) -> u64 {
        // SAFETY: Scalar owners call this accessor only after validating the
        // directory family and selecting the scalar value arm.
        unsafe { self.data.value }
    }

    #[inline]
    pub(crate) fn set_scalar_value(&mut self, value: u64) {
        self.data.value = value;
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
pub struct ScalarBits(u64);

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
pub fn vec_len(header: Option<&[u8; 8]>) -> u32 {
    match header {
        Some(header) => u32::from_ne_bytes([header[0], header[1], header[2], header[3]]),
        None => 0,
    }
}

pub fn vector_element_offset(
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
    pub(crate) fn set_entry_size(&mut self, value: u32) {
        // SAFETY: `entry_size` is a field of a live packed record; the write is
        // explicitly unaligned and does not create a field reference.
        unsafe { core::ptr::addr_of_mut!(self.entry_size).write_unaligned(value) }
    }

    #[inline]
    pub fn ring_size(&self) -> u32 {
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
    pub fn n_threads(&self) -> u32 {
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
    pub fn schema_size(&self) -> u32 {
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
pub struct RingBufferHeader {
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
    pub fn config(&self) -> RingConfig {
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
    pub fn data_offset(&self) -> u32 {
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

pub fn ring_layout(
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
pub struct SharedHeader {
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
    pub fn validate_version(&self) -> Result<(), Error> {
        if self.version == STAT_SEGMENT_VERSION {
            Ok(())
        } else {
            Err(Error::InvalidVersion {
                actual: self.version,
            })
        }
    }

    #[inline]
    pub fn is_write_in_progress(&self) -> bool {
        self.in_progress != 0
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    #[inline]
    pub fn base(&self) -> *mut core::ffi::c_void {
        self.base
    }

    #[inline]
    pub fn directory_vector(&self) -> *mut DirectoryEntry {
        self.directory_vector
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
pub struct Counter {
    pub packets: u64,
    pub bytes: u64,
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

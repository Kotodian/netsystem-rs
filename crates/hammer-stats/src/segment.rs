use std::alloc::Layout;
use std::collections::HashMap;
use std::ffi::c_void;
use std::marker::PhantomData;
use std::mem::{align_of, replace, size_of};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hammer_infra::page_size;
use hammer_infra::segment::{Segment, SegmentAllocation};
use hammer_runtime::sync::SpinLock;

use crate::protocol::{
    Counter as WireCounter, DirectoryData, DirectoryDataPointer, DirectoryEntry, DirectoryIndex,
    DirectoryType, Gauge as WireGauge, NameBytes, RingBufferHeader, RingConfig, RingMetadata,
    STAT_SEGMENT_INDEX_INVALID, ScalarBits, SharedHeader, StringVectorPointer, SymlinkIndex,
    VEC_MIN_ALIGN, ring_layout, vec_header_bytes, vec_len, vector_element_offset,
};
use crate::{StatsError, StatsResult};

const VECTOR_HEADER_SIZE: usize = 8;
const VECTOR_DATA_ALIGNMENT: usize = 64;
const INITIAL_DIRECTORY_LENGTH: usize = 3;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct Gauge;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct Counter;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub(crate) struct Timestamp;

#[derive(Clone)]
pub(crate) struct Scalar<M> {
    index: DirectoryIndex,
    state: Arc<SpinLock<StatsSegmentState>>,
    marker: PhantomData<fn() -> M>,
}

#[derive(Clone)]
pub(crate) struct Simple<M> {
    index: DirectoryIndex,
    state: Arc<SpinLock<StatsSegmentState>>,
    marker: PhantomData<fn() -> M>,
}

#[derive(Clone)]
pub(crate) struct Combined<M> {
    index: DirectoryIndex,
    state: Arc<SpinLock<StatsSegmentState>>,
    marker: PhantomData<fn() -> M>,
}

#[derive(Clone)]
pub(crate) struct NameVector {
    index: DirectoryIndex,
    state: Arc<SpinLock<StatsSegmentState>>,
}

#[derive(Clone)]
pub(crate) struct Symlink {
    index: DirectoryIndex,
    state: Arc<SpinLock<StatsSegmentState>>,
}

#[derive(Clone)]
pub(crate) struct Histogram<M> {
    index: DirectoryIndex,
    state: Arc<SpinLock<StatsSegmentState>>,
    marker: PhantomData<fn() -> M>,
}

#[derive(Clone)]
pub(crate) struct RingShape {
    config: RingConfig,
    schema: Box<[u8]>,
}

impl RingShape {
    pub(crate) fn new(config: RingConfig, schema: &[u8]) -> StatsResult<Self> {
        let expected =
            usize::try_from(config.schema_size()).map_err(|_| StatsError::PublicationFailed)?;
        if expected != schema.len() {
            return Err(StatsError::InvalidRingSchema {
                expected,
                actual: schema.len(),
            });
        }
        if config.n_threads() == 0 {
            return Err(StatsError::InvalidShape);
        }
        Ok(Self {
            config,
            schema: schema.into(),
        })
    }
}

#[derive(Clone)]
pub(crate) struct Ring<T> {
    index: DirectoryIndex,
    state: Arc<SpinLock<StatsSegmentState>>,
    marker: PhantomData<fn() -> T>,
}

trait RecordKind: Sized {
    type Shape;
    type Storage: IntoIterator<Item = SegmentAllocation>;
    type Handle;

    fn prepare(
        state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        shape: Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)>;
}

impl RecordKind for Scalar<Gauge> {
    type Shape = ();
    type Storage = [SegmentAllocation; 0];
    type Handle = Self;

    fn prepare(
        _state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        (): Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        Ok((
            DirectoryEntry::new(
                DirectoryType::Gauge.into(),
                name,
                DirectoryData::from(WireGauge::from(0)),
            ),
            [],
            Self {
                index,
                state: Arc::clone(capability),
                marker: PhantomData,
            },
        ))
    }
}

impl RecordKind for Scalar<Timestamp> {
    type Shape = ();
    type Storage = [SegmentAllocation; 0];
    type Handle = Self;

    fn prepare(
        _state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        (): Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        Ok((
            DirectoryEntry::new(
                DirectoryType::ScalarIndex.into(),
                name,
                DirectoryData::from(ScalarBits::from(0_u64)),
            ),
            [],
            Self {
                index,
                state: Arc::clone(capability),
                marker: PhantomData,
            },
        ))
    }
}

impl RecordKind for Simple<Counter> {
    type Shape = (u32, u32);
    type Storage = Vec<SegmentAllocation>;
    type Handle = Self;

    fn prepare(
        state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        shape: Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let rows = usize::try_from(shape.0).map_err(|_| StatsError::PublicationFailed)?;
        let length = usize::try_from(shape.1).map_err(|_| StatsError::PublicationFailed)?;
        if rows == 0 || length == 0 {
            return Err(StatsError::InvalidShape);
        }
        let (outer, outer_data) = state.allocate_vector::<*mut u8>(rows, None, ptr::null_mut())?;
        let mut storage = Vec::new();
        storage
            .try_reserve(rows.checked_add(1).ok_or(StatsError::CollectionCapacity)?)
            .map_err(|_| StatsError::CollectionCapacity)?;
        for row in 0..rows {
            let (inner, inner_data) = state.allocate_vector::<u64>(length, None, 0_u64)?;
            unsafe {
                ptr::write(outer_data.add(row), inner_data.cast::<u8>());
            }
            storage.push(inner);
        }
        let entry = DirectoryEntry::new(
            DirectoryType::CounterVectorSimple.into(),
            name,
            DirectoryData::from(DirectoryDataPointer::from(outer_data.cast::<c_void>())),
        );
        storage.push(outer);
        Ok((
            entry,
            storage,
            Self {
                index,
                state: Arc::clone(capability),
                marker: PhantomData,
            },
        ))
    }
}

impl RecordKind for Combined<Counter> {
    type Shape = (u32, u32);
    type Storage = Vec<SegmentAllocation>;
    type Handle = Self;

    fn prepare(
        state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        shape: Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let rows = usize::try_from(shape.0).map_err(|_| StatsError::PublicationFailed)?;
        let length = usize::try_from(shape.1).map_err(|_| StatsError::PublicationFailed)?;
        if rows == 0 || length == 0 {
            return Err(StatsError::InvalidShape);
        }
        let (outer, outer_data) = state.allocate_vector::<*mut u8>(rows, None, ptr::null_mut())?;
        let mut storage = Vec::new();
        storage
            .try_reserve(rows.checked_add(1).ok_or(StatsError::CollectionCapacity)?)
            .map_err(|_| StatsError::CollectionCapacity)?;
        for row in 0..rows {
            let (inner, inner_data) =
                state.allocate_vector::<WireCounter>(length, None, WireCounter::default())?;
            unsafe {
                ptr::write(outer_data.add(row), inner_data.cast::<u8>());
            }
            storage.push(inner);
        }
        let entry = DirectoryEntry::new(
            DirectoryType::CounterVectorCombined.into(),
            name,
            DirectoryData::from(DirectoryDataPointer::from(outer_data.cast::<c_void>())),
        );
        storage.push(outer);
        Ok((
            entry,
            storage,
            Self {
                index,
                state: Arc::clone(capability),
                marker: PhantomData,
            },
        ))
    }
}

impl RecordKind for NameVector {
    type Shape = u32;
    type Storage = [SegmentAllocation; 1];
    type Handle = Self;

    fn prepare(
        state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        length: Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let length = usize::try_from(length).map_err(|_| StatsError::PublicationFailed)?;
        if length == 0 {
            return Err(StatsError::InvalidShape);
        }
        let (allocation, pointer) =
            state.allocate_vector::<*mut u8>(length, Some(index), ptr::null_mut())?;
        let entry = DirectoryEntry::new(
            DirectoryType::NameVector.into(),
            name,
            DirectoryData::from(StringVectorPointer::from(pointer)),
        );
        Ok((
            entry,
            [allocation],
            Self {
                index,
                state: Arc::clone(capability),
            },
        ))
    }
}

impl RecordKind for Symlink {
    type Shape = SymlinkIndex;
    type Storage = [SegmentAllocation; 0];
    type Handle = Self;

    fn prepare(
        state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        target: Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let target_index =
            usize::try_from(target.entry_index).map_err(|_| StatsError::PublicationFailed)?;
        let Some(entry) = state.directory_vector.get(target_index) else {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: target.entry_index,
                length: state.directory_vector.len(),
            });
        };
        let kind = DirectoryType::try_from(entry.kind())?;
        if matches!(kind, DirectoryType::Empty | DirectoryType::Illegal) {
            return Err(StatsError::DirectoryEntryUnavailable {
                index: target.entry_index,
            });
        }
        Ok((
            DirectoryEntry::new(
                DirectoryType::Symlink.into(),
                name,
                DirectoryData::from(target),
            ),
            [],
            Self {
                index,
                state: Arc::clone(capability),
            },
        ))
    }
}

impl RecordKind for Histogram<Counter> {
    type Shape = (u32, u32);
    type Storage = Vec<SegmentAllocation>;
    type Handle = Self;

    fn prepare(
        state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        shape: Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let rows = usize::try_from(shape.0).map_err(|_| StatsError::PublicationFailed)?;
        let length = usize::try_from(shape.1).map_err(|_| StatsError::PublicationFailed)?;
        if rows == 0 || length == 0 {
            return Err(StatsError::InvalidShape);
        }
        let (outer, outer_data) = state.allocate_vector::<*mut u8>(rows, None, ptr::null_mut())?;
        let mut storage = Vec::new();
        storage
            .try_reserve(rows.checked_add(1).ok_or(StatsError::CollectionCapacity)?)
            .map_err(|_| StatsError::CollectionCapacity)?;
        for row in 0..rows {
            let (inner, inner_data) = state.allocate_vector::<u64>(length, None, 0_u64)?;
            unsafe {
                ptr::write(outer_data.add(row), inner_data.cast::<u8>());
            }
            storage.push(inner);
        }
        let entry = DirectoryEntry::new(
            DirectoryType::HistogramLog2.into(),
            name,
            DirectoryData::from(DirectoryDataPointer::from(outer_data.cast::<c_void>())),
        );
        storage.push(outer);
        Ok((
            entry,
            storage,
            Self {
                index,
                state: Arc::clone(capability),
                marker: PhantomData,
            },
        ))
    }
}

impl<T> RecordKind for Ring<T> {
    type Shape = RingShape;
    type Storage = [SegmentAllocation; 1];
    type Handle = Self;

    fn prepare(
        state: &StatsSegmentState,
        capability: &Arc<SpinLock<StatsSegmentState>>,
        index: DirectoryIndex,
        name: NameBytes,
        shape: Self::Shape,
    ) -> StatsResult<(DirectoryEntry, Self::Storage, Self::Handle)> {
        let expected = usize::try_from(shape.config.schema_size())
            .map_err(|_| StatsError::PublicationFailed)?;
        if expected != shape.schema.len() {
            return Err(StatsError::InvalidRingSchema {
                expected,
                actual: shape.schema.len(),
            });
        }
        let (header, total) =
            ring_layout(shape.config, VECTOR_DATA_ALIGNMENT, state.mapping.size())?;
        let layout = Layout::from_size_align(total, VECTOR_DATA_ALIGNMENT)
            .map_err(|_| StatsError::InvalidLayout)?;
        let allocation = state.mapping.allocate(layout)?;
        let base_address = (state.mapping.base() as usize)
            .checked_add(
                usize::try_from(allocation.offset()).map_err(|_| StatsError::PublicationFailed)?,
            )
            .ok_or(StatsError::PublicationFailed)?;
        let allocation_end = base_address
            .checked_add(allocation.len())
            .ok_or(StatsError::PublicationFailed)?;
        let data_end = base_address
            .checked_add(total)
            .ok_or(StatsError::PublicationFailed)?;
        if data_end > allocation_end || !base_address.is_multiple_of(VECTOR_DATA_ALIGNMENT) {
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
        for thread_index in 0..n_threads {
            let offset = metadata_offset
                .checked_add(
                    thread_index
                        .checked_mul(size_of::<RingMetadata>())
                        .ok_or(StatsError::PublicationFailed)?,
                )
                .ok_or(StatsError::PublicationFailed)?;
            if expected != 0 {
                let metadata =
                    RingMetadata::new(config.schema_version(), schema_offset, config.schema_size());
                unsafe {
                    ptr::write(base.add(offset).cast::<RingMetadata>(), metadata);
                }
            }
        }
        if expected != 0 {
            unsafe {
                ptr::copy_nonoverlapping(
                    shape.schema.as_ptr(),
                    base.add(
                        usize::try_from(schema_offset)
                            .map_err(|_| StatsError::PublicationFailed)?,
                    ),
                    shape.schema.len(),
                );
            }
        }
        let entry = DirectoryEntry::new(
            DirectoryType::RingBuffer.into(),
            name,
            DirectoryData::from(DirectoryDataPointer::from(base.cast::<c_void>())),
        );
        Ok((
            entry,
            [allocation],
            Self {
                index,
                state: Arc::clone(capability),
                marker: PhantomData,
            },
        ))
    }
}

pub(crate) struct StatsSegmentState {
    mapping: Segment,
    header: SharedHeader,
    directory_vector: Vec<DirectoryEntry>,
    directory_block: SegmentAllocation,
    payloads: Vec<Vec<SegmentAllocation>>,
    retired: Vec<SegmentAllocation>,
    names: HashMap<NameBytes, DirectoryIndex>,
    first_free: Option<DirectoryIndex>,
    tearing_down: bool,
}

impl StatsSegmentState {
    fn allocate_vector<T>(
        &self,
        count: usize,
        entry_index: Option<DirectoryIndex>,
        value: T,
    ) -> StatsResult<(SegmentAllocation, *mut T)>
    where
        T: Copy,
    {
        if count == 0 || size_of::<T>() == 0 {
            return Err(StatsError::InvalidShape);
        }
        let data_align = vector_data_offset::<T>();
        let header_offset = data_align
            .checked_sub(VECTOR_HEADER_SIZE)
            .ok_or(StatsError::PublicationFailed)?;
        let element_bytes = count
            .checked_mul(size_of::<T>())
            .ok_or(StatsError::PublicationFailed)?;
        let bytes = data_align
            .checked_add(element_bytes)
            .ok_or(StatsError::PublicationFailed)?;
        let layout =
            Layout::from_size_align(bytes, data_align).map_err(|_| StatsError::InvalidLayout)?;
        let allocation = self.mapping.allocate(layout)?;
        let private_header_offset: usize = 0;
        let header = vec_header_bytes(
            u32::try_from(count).map_err(|_| StatsError::PublicationFailed)?,
            u8::try_from(data_align / VEC_MIN_ALIGN).map_err(|_| StatsError::PublicationFailed)?,
            vector_log2_alignment(data_align)?,
            false,
            0,
            0,
        );
        let allocation_base = (self.mapping.base() as usize)
            .checked_add(
                usize::try_from(allocation.offset()).map_err(|_| StatsError::PublicationFailed)?,
            )
            .ok_or(StatsError::PublicationFailed)?;
        let header_end = header_offset
            .checked_add(size_of::<[u8; VECTOR_HEADER_SIZE]>())
            .ok_or(StatsError::PublicationFailed)?;
        if header_end > allocation.len() {
            return Err(StatsError::PublicationFailed);
        }
        let header_address = allocation_base
            .checked_add(header_offset)
            .ok_or(StatsError::PublicationFailed)?;
        if !header_address.is_multiple_of(align_of::<[u8; VECTOR_HEADER_SIZE]>()) {
            return Err(StatsError::PublicationFailed);
        }
        let header_pointer = header_address as *mut [u8; VECTOR_HEADER_SIZE];
        let data_end = data_align
            .checked_add(size_of::<T>())
            .ok_or(StatsError::PublicationFailed)?;
        if data_end > allocation.len() {
            return Err(StatsError::PublicationFailed);
        }
        let data_address = allocation_base
            .checked_add(data_align)
            .ok_or(StatsError::PublicationFailed)?;
        if !data_address.is_multiple_of(align_of::<T>()) {
            return Err(StatsError::PublicationFailed);
        }
        let data_pointer = data_address as *mut T;
        if let Some(index) = entry_index {
            let private_end = private_header_offset
                .checked_add(size_of::<u32>())
                .ok_or(StatsError::PublicationFailed)?;
            if private_end > allocation.len() {
                return Err(StatsError::PublicationFailed);
            }
            let private_address = allocation_base
                .checked_add(private_header_offset)
                .ok_or(StatsError::PublicationFailed)?;
            if !private_address.is_multiple_of(align_of::<u32>()) {
                return Err(StatsError::PublicationFailed);
            }
            let private_header = private_address as *mut u32;
            unsafe {
                ptr::write(private_header, index.raw());
            }
        }
        unsafe {
            ptr::write(header_pointer, header);
            for element in 0..count {
                ptr::write(data_pointer.add(element), value);
            }
        }
        Ok((allocation, data_pointer))
    }

    fn vector_len<T>(&self, pointer: *mut u8) -> StatsResult<usize> {
        if size_of::<T>() == 0 {
            return Err(StatsError::InvalidShape);
        }
        if pointer.is_null() {
            return Err(StatsError::PublicationFailed);
        }
        let address = pointer as usize;
        let base = self.mapping.base() as usize;
        let mapping_end = base
            .checked_add(self.mapping.size())
            .ok_or(StatsError::PublicationFailed)?;
        let header_address = address
            .checked_sub(VECTOR_HEADER_SIZE)
            .ok_or(StatsError::PublicationFailed)?;
        let header_end = header_address
            .checked_add(VECTOR_HEADER_SIZE)
            .ok_or(StatsError::PublicationFailed)?;
        if header_address < base
            || header_end > mapping_end
            || address < header_end
            || address >= mapping_end
        {
            return Err(StatsError::PublicationFailed);
        }
        let header = unsafe {
            ptr::read_unaligned((header_address as *const u8).cast::<[u8; VECTOR_HEADER_SIZE]>())
        };
        let header_size = usize::from(header[4])
            .checked_mul(VEC_MIN_ALIGN)
            .ok_or(StatsError::PublicationFailed)?;
        let data_alignment = 1usize
            .checked_shl(u32::from(header[5] & 0x7f))
            .ok_or(StatsError::PublicationFailed)?;
        let vector_offset = address
            .checked_sub(base)
            .ok_or(StatsError::PublicationFailed)?;
        let encoded_header_offset = vector_offset
            .checked_sub(header_size)
            .ok_or(StatsError::PublicationFailed)?;
        let encoded_header_end = encoded_header_offset
            .checked_add(header_size)
            .ok_or(StatsError::PublicationFailed)?;
        if header_size < VECTOR_HEADER_SIZE
            || encoded_header_end != vector_offset
            || data_alignment < VEC_MIN_ALIGN
            || !data_alignment.is_power_of_two()
            || !header_address.is_multiple_of(VEC_MIN_ALIGN)
            || !encoded_header_offset.is_multiple_of(VEC_MIN_ALIGN)
            || !address.is_multiple_of(data_alignment)
            || !address.is_multiple_of(align_of::<T>())
        {
            return Err(StatsError::PublicationFailed);
        }
        let length =
            usize::try_from(vec_len(Some(&header))).map_err(|_| StatsError::PublicationFailed)?;
        let byte_length = length
            .checked_mul(size_of::<T>())
            .ok_or(StatsError::PublicationFailed)?;
        if address
            .checked_add(byte_length)
            .ok_or(StatsError::PublicationFailed)?
            > mapping_end
        {
            return Err(StatsError::PublicationFailed);
        }
        Ok(length)
    }

    fn vector_element<T>(&self, pointer: *mut u8, index: usize) -> StatsResult<*mut T> {
        if size_of::<T>() == 0 {
            return Err(StatsError::InvalidShape);
        }
        let address = pointer as usize;
        let base = self.mapping.base() as usize;
        let mapping_end = base
            .checked_add(self.mapping.size())
            .ok_or(StatsError::PublicationFailed)?;
        let header_address = address
            .checked_sub(VECTOR_HEADER_SIZE)
            .ok_or(StatsError::PublicationFailed)?;
        let header_end = header_address
            .checked_add(VECTOR_HEADER_SIZE)
            .ok_or(StatsError::PublicationFailed)?;
        if header_address < base
            || header_end > mapping_end
            || address < header_end
            || address >= mapping_end
        {
            return Err(StatsError::PublicationFailed);
        }
        let vector_offset = address
            .checked_sub(base)
            .ok_or(StatsError::PublicationFailed)?;
        let header = unsafe {
            ptr::read_unaligned((header_address as *const u8).cast::<[u8; VECTOR_HEADER_SIZE]>())
        };
        let header_size = usize::from(header[4])
            .checked_mul(VEC_MIN_ALIGN)
            .ok_or(StatsError::PublicationFailed)?;
        let header_offset = vector_offset
            .checked_sub(header_size)
            .ok_or(StatsError::PublicationFailed)?;
        let element_offset = vector_element_offset(
            header_offset,
            vector_offset,
            &header,
            index,
            size_of::<T>(),
            self.mapping.size(),
        )?;
        let element_address = base
            .checked_add(element_offset)
            .ok_or(StatsError::PublicationFailed)?;
        if !element_address.is_multiple_of(align_of::<T>()) {
            return Err(StatsError::PublicationFailed);
        }
        Ok(element_address as *mut T)
    }

    fn allocate_directory(&self, entries: &[DirectoryEntry]) -> StatsResult<SegmentAllocation> {
        let length = u32::try_from(entries.len()).map_err(|_| StatsError::PublicationFailed)?;
        let layout = directory_layout(entries.len())?;
        let allocation = self.mapping.allocate(layout)?;
        let header = vec_header_bytes(length, 1, 3, false, 0, 0);
        let allocation_base = (self.mapping.base() as usize)
            .checked_add(
                usize::try_from(allocation.offset()).map_err(|_| StatsError::PublicationFailed)?,
            )
            .ok_or(StatsError::PublicationFailed)?;
        let header_end = VECTOR_HEADER_SIZE;
        if header_end > allocation.len() {
            return Err(StatsError::PublicationFailed);
        }
        let header_address = allocation_base;
        if !header_address.is_multiple_of(align_of::<[u8; VECTOR_HEADER_SIZE]>()) {
            return Err(StatsError::PublicationFailed);
        }
        let header_pointer = header_address as *mut [u8; VECTOR_HEADER_SIZE];
        let entry_end = VECTOR_HEADER_SIZE
            .checked_add(
                entries
                    .len()
                    .checked_mul(size_of::<DirectoryEntry>())
                    .ok_or(StatsError::PublicationFailed)?,
            )
            .ok_or(StatsError::PublicationFailed)?;
        if entry_end > allocation.len() {
            return Err(StatsError::PublicationFailed);
        }
        let entry_address = allocation_base
            .checked_add(VECTOR_HEADER_SIZE)
            .ok_or(StatsError::PublicationFailed)?;
        if !entry_address.is_multiple_of(align_of::<DirectoryEntry>()) {
            return Err(StatsError::PublicationFailed);
        }
        let entry_pointer = entry_address as *mut DirectoryEntry;
        unsafe {
            ptr::write(header_pointer, header);
            for (index, entry) in entries.iter().enumerate() {
                ptr::write(entry_pointer.add(index), *entry);
            }
        }
        Ok(allocation)
    }

    fn publish(
        &mut self,
        candidate: Vec<DirectoryEntry>,
        new_block: SegmentAllocation,
    ) -> StatsResult<()> {
        let pointer_address = (self.mapping.base() as usize)
            .checked_add(
                usize::try_from(new_block.offset()).map_err(|_| StatsError::PublicationFailed)?,
            )
            .and_then(|address| address.checked_add(VECTOR_HEADER_SIZE))
            .ok_or(StatsError::PublicationFailed)?;
        let pointer_end = VECTOR_HEADER_SIZE
            .checked_add(size_of::<DirectoryEntry>())
            .ok_or(StatsError::PublicationFailed)?;
        if pointer_end > new_block.len()
            || !pointer_address.is_multiple_of(align_of::<DirectoryEntry>())
        {
            return Err(StatsError::PublicationFailed);
        }
        let pointer = pointer_address as *mut DirectoryEntry;
        self.retired
            .try_reserve(1)
            .map_err(|_| StatsError::CollectionCapacity)?;
        let old_block = replace(&mut self.directory_block, new_block);
        self.directory_vector = candidate;
        self.header.set_in_progress(true);
        self.write_shared_header(Ordering::Relaxed);
        self.header.set_directory_vector(pointer);
        self.header.set_epoch(self.header.epoch().wrapping_add(1));
        self.write_shared_header(Ordering::Relaxed);
        self.header.set_in_progress(false);
        self.write_shared_header(Ordering::Release);
        self.retired.push(old_block);
        Ok(())
    }

    fn write_shared_header(&self, ordering: Ordering) {
        let destination = self.mapping.base().cast::<SharedHeader>();
        unsafe {
            let progress = ptr::addr_of_mut!((*destination).in_progress);
            let mapped_progress = AtomicU64::from_ptr(progress);
            if self.header.is_write_in_progress() {
                mapped_progress.store(1, Ordering::Relaxed);
            }

            let source = ptr::addr_of!(self.header).cast::<u8>();
            let destination = destination.cast::<u8>();
            let progress_offset = std::mem::offset_of!(SharedHeader, in_progress);
            let progress_end = progress_offset + size_of::<u64>();
            ptr::copy_nonoverlapping(source, destination, progress_offset);
            ptr::copy_nonoverlapping(
                source.add(progress_end),
                destination.add(progress_end),
                size_of::<SharedHeader>() - progress_end,
            );

            if !self.header.is_write_in_progress() {
                mapped_progress.store(0, ordering);
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct StatsSegment {
    state: Arc<SpinLock<StatsSegmentState>>,
}

impl StatsSegment {
    pub(crate) fn create(name: &str, size: usize) -> StatsResult<Self> {
        let page = page_size()?;
        let minimum = page
            .checked_add(directory_layout(INITIAL_DIRECTORY_LENGTH)?.size())
            .ok_or(StatsError::PublicationFailed)?;
        if size < minimum {
            return Err(StatsError::CapacityTooSmall {
                requested: size,
                minimum,
            });
        }

        let mapping = Segment::shared_with_reserved_prefix(name, size, page)?;
        let directory_vector = initial_directory()?;
        let mut names = HashMap::new();
        names
            .try_reserve(INITIAL_DIRECTORY_LENGTH)
            .map_err(|_| StatsError::CollectionCapacity)?;
        for (raw_index, entry) in directory_vector.iter().enumerate() {
            let index = DirectoryIndex::new(
                u32::try_from(raw_index).map_err(|_| StatsError::PublicationFailed)?,
            );
            names.insert(entry.name_bytes()?, index);
        }

        let header = SharedHeader::new(mapping.base().cast::<c_void>());
        let bootstrap_layout =
            Layout::from_size_align(1, 1).map_err(|_| StatsError::InvalidLayout)?;
        let bootstrap_block = mapping.allocate(bootstrap_layout)?;
        let mut state = StatsSegmentState {
            mapping,
            header,
            directory_vector,
            directory_block: bootstrap_block,
            payloads: {
                let mut payloads = Vec::with_capacity(INITIAL_DIRECTORY_LENGTH);
                payloads.resize_with(INITIAL_DIRECTORY_LENGTH, Vec::new);
                payloads
            },
            retired: Vec::new(),
            names,
            first_free: None,
            tearing_down: false,
        };
        let directory_block = state.allocate_directory(&state.directory_vector)?;
        let initial_vector_address = (state.mapping.base() as usize)
            .checked_add(
                usize::try_from(directory_block.offset())
                    .map_err(|_| StatsError::PublicationFailed)?,
            )
            .and_then(|address| address.checked_add(VECTOR_HEADER_SIZE))
            .ok_or(StatsError::PublicationFailed)?;
        let initial_vector_end = VECTOR_HEADER_SIZE
            .checked_add(size_of::<DirectoryEntry>())
            .ok_or(StatsError::PublicationFailed)?;
        if initial_vector_end > directory_block.len()
            || !initial_vector_address.is_multiple_of(align_of::<DirectoryEntry>())
        {
            return Err(StatsError::PublicationFailed);
        }
        let initial_directory_vector = initial_vector_address as *mut DirectoryEntry;
        state.header.set_directory_vector(initial_directory_vector);
        state.directory_block = directory_block;
        state.header.set_in_progress(true);
        state.write_shared_header(Ordering::Relaxed);
        state.header.set_in_progress(false);
        state.write_shared_header(Ordering::Release);
        Ok(Self {
            state: Arc::new(SpinLock::new(state)),
        })
    }

    pub(crate) fn shared_fd(&self) -> Option<std::os::fd::RawFd> {
        let state = self.state.lock();
        (!state.tearing_down)
            .then(|| state.mapping.shared_fd())
            .flatten()
    }

    pub(crate) fn directory_vector_len(&self) -> usize {
        let state = self.state.lock();
        if state.tearing_down {
            0
        } else {
            state.directory_vector.len()
        }
    }

    fn register<K>(&self, name: &str, shape: K::Shape) -> StatsResult<K::Handle>
    where
        K: RecordKind,
    {
        let name = NameBytes::try_from(name)?;
        let mut state = self.state.lock();
        if state.tearing_down {
            return Err(StatsError::Teardown);
        }
        if state.names.contains_key(&name) {
            return Err(StatsError::DuplicateName(name));
        }
        let (index, next_free, is_new_slot) = match state.first_free {
            Some(index) => {
                let raw_index =
                    usize::try_from(index.raw()).map_err(|_| StatsError::PublicationFailed)?;
                let Some(entry) = state.directory_vector.get(raw_index) else {
                    return Err(StatsError::DirectoryIndexOutOfBounds {
                        index: index.raw(),
                        length: state.directory_vector.len(),
                    });
                };
                if DirectoryType::try_from(entry.kind())? != DirectoryType::Empty {
                    return Err(StatsError::PublicationFailed);
                }
                let next = DirectoryIndex::try_from(entry)?;
                let next = (next.raw() != STAT_SEGMENT_INDEX_INVALID).then_some(next);
                (index, next, false)
            }
            None => {
                let index = DirectoryIndex::new(
                    u32::try_from(state.directory_vector.len())
                        .map_err(|_| StatsError::PublicationFailed)?,
                );
                (index, None, true)
            }
        };

        let (entry, storage, handle) = K::prepare(&state, &self.state, index, name, shape)?;
        let mut allocations = Vec::new();
        let storage = storage.into_iter();
        let (lower, upper) = storage.size_hint();
        allocations
            .try_reserve(upper.unwrap_or(lower))
            .map_err(|_| StatsError::CollectionCapacity)?;
        for allocation in storage {
            allocations.push(allocation);
        }

        let target_length = state
            .directory_vector
            .len()
            .checked_add(usize::from(is_new_slot))
            .ok_or(StatsError::PublicationFailed)?;
        let mut candidate = Vec::new();
        candidate
            .try_reserve(target_length)
            .map_err(|_| StatsError::CollectionCapacity)?;
        candidate.extend(state.directory_vector.iter().copied());
        let raw_index = usize::try_from(index.raw()).map_err(|_| StatsError::PublicationFailed)?;
        if is_new_slot {
            candidate.push(entry);
        } else if let Some(slot) = candidate.get_mut(raw_index) {
            *slot = entry;
        } else {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: index.raw(),
                length: candidate.len(),
            });
        }

        state
            .names
            .try_reserve(1)
            .map_err(|_| StatsError::CollectionCapacity)?;
        let raw_index = usize::try_from(index.raw()).map_err(|_| StatsError::PublicationFailed)?;
        if raw_index == state.payloads.len() {
            state
                .payloads
                .try_reserve(1)
                .map_err(|_| StatsError::CollectionCapacity)?;
        }
        if let Some(payloads) = state.payloads.get_mut(raw_index) {
            payloads
                .try_reserve(allocations.len())
                .map_err(|_| StatsError::CollectionCapacity)?;
        }
        let new_block = state.allocate_directory(&candidate)?;
        state.publish(candidate, new_block)?;
        state.first_free = next_free;
        state.names.insert(name, index);
        if raw_index == state.payloads.len() {
            state.payloads.push(allocations);
        } else if let Some(payloads) = state.payloads.get_mut(raw_index) {
            payloads.extend(allocations);
        }
        Ok(handle)
    }

    pub(crate) fn remove(&self, index: DirectoryIndex) -> StatsResult<()> {
        let mut state = self.state.lock();
        if state.tearing_down {
            return Err(StatsError::Teardown);
        }
        let raw_index = usize::try_from(index.raw()).map_err(|_| StatsError::PublicationFailed)?;
        let Some(entry) = state.directory_vector.get(raw_index).copied() else {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: index.raw(),
                length: state.directory_vector.len(),
            });
        };
        let kind = DirectoryType::try_from(entry.kind())?;
        if matches!(kind, DirectoryType::Empty | DirectoryType::Illegal) {
            return Err(StatsError::DirectoryEntryUnavailable { index: index.raw() });
        }
        let name = entry.name_bytes()?;
        let next_free = state
            .first_free
            .map(DirectoryIndex::raw)
            .unwrap_or(STAT_SEGMENT_INDEX_INVALID);
        let mut candidate = Vec::new();
        candidate
            .try_reserve(state.directory_vector.len())
            .map_err(|_| StatsError::CollectionCapacity)?;
        candidate.extend(state.directory_vector.iter().copied());
        let Some(slot) = candidate.get_mut(raw_index) else {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: index.raw(),
                length: candidate.len(),
            });
        };
        *slot = DirectoryEntry::new(
            DirectoryType::Empty.into(),
            NameBytes::try_from(&[] as &[u8])?,
            DirectoryIndex::new(next_free).into(),
        );
        let new_block = state.allocate_directory(&candidate)?;
        state.publish(candidate, new_block)?;
        state.names.remove(&name);
        state.first_free = Some(index);
        Ok(())
    }

    pub(crate) fn validate(&self, index: DirectoryIndex, row: u32, column: u32) -> StatsResult<()> {
        let mut state = self.state.lock();
        if state.tearing_down {
            return Err(StatsError::Teardown);
        }
        let raw_index = usize::try_from(index.raw()).map_err(|_| StatsError::PublicationFailed)?;
        let Some(entry) = state.directory_vector.get(raw_index).copied() else {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: index.raw(),
                length: state.directory_vector.len(),
            });
        };
        let kind = DirectoryType::try_from(entry.kind())?;
        if !matches!(
            kind,
            DirectoryType::CounterVectorSimple
                | DirectoryType::CounterVectorCombined
                | DirectoryType::HistogramLog2
        ) {
            return Err(StatsError::InvalidShape);
        }

        let row_count = usize::try_from(row)
            .map_err(|_| StatsError::PublicationFailed)?
            .checked_add(1)
            .ok_or(StatsError::PublicationFailed)?;
        let column_count = usize::try_from(column)
            .map_err(|_| StatsError::PublicationFailed)?
            .checked_add(1)
            .ok_or(StatsError::PublicationFailed)?;
        let outer_pointer = DirectoryDataPointer::try_from(&entry)?
            .as_ptr()
            .cast::<u8>();
        if outer_pointer.is_null() {
            return Err(StatsError::PublicationFailed);
        }
        let old_outer_length = state.vector_len::<*mut u8>(outer_pointer)?;
        let mut needs_growth = row_count > old_outer_length;
        if !needs_growth {
            for row_index in 0..row_count {
                let inner_pointer = unsafe {
                    ptr::read(state.vector_element::<*mut u8>(outer_pointer, row_index)?)
                };
                let inner_length = if inner_pointer.is_null() {
                    0
                } else {
                    match kind {
                        DirectoryType::CounterVectorSimple | DirectoryType::HistogramLog2 => {
                            state.vector_len::<u64>(inner_pointer)?
                        }
                        DirectoryType::CounterVectorCombined => {
                            state.vector_len::<WireCounter>(inner_pointer)?
                        }
                        _ => return Err(StatsError::InvalidShape),
                    }
                };
                if inner_length < column_count {
                    needs_growth = true;
                    break;
                }
            }
        }
        if !needs_growth {
            return Ok(());
        }

        let outer_length = old_outer_length.max(row_count);
        let owner_count = old_outer_length
            .checked_add(1)
            .ok_or(StatsError::CollectionCapacity)?;
        let Some(payloads) = state.payloads.get(raw_index) else {
            return Err(StatsError::PublicationFailed);
        };
        if payloads.len() < owner_count {
            return Err(StatsError::PublicationFailed);
        }
        let Some(payloads) = state.payloads.get_mut(raw_index) else {
            return Err(StatsError::PublicationFailed);
        };
        payloads
            .try_reserve(
                outer_length
                    .checked_add(1)
                    .ok_or(StatsError::CollectionCapacity)?,
            )
            .map_err(|_| StatsError::CollectionCapacity)?;

        let mut staged = Vec::new();
        staged
            .try_reserve(
                outer_length
                    .checked_add(1)
                    .ok_or(StatsError::CollectionCapacity)?,
            )
            .map_err(|_| StatsError::CollectionCapacity)?;
        let (new_outer, new_outer_data) =
            state.allocate_vector::<*mut u8>(outer_length, None, ptr::null_mut())?;
        unsafe {
            ptr::copy_nonoverlapping(
                outer_pointer.cast::<*mut u8>(),
                new_outer_data,
                old_outer_length,
            );
        }

        for row_index in 0..row_count {
            let old_inner = if row_index < old_outer_length {
                unsafe { ptr::read(state.vector_element::<*mut u8>(outer_pointer, row_index)?) }
            } else {
                ptr::null_mut()
            };
            let old_length = if old_inner.is_null() {
                0
            } else {
                match kind {
                    DirectoryType::CounterVectorSimple | DirectoryType::HistogramLog2 => {
                        state.vector_len::<u64>(old_inner)?
                    }
                    DirectoryType::CounterVectorCombined => {
                        state.vector_len::<WireCounter>(old_inner)?
                    }
                    _ => return Err(StatsError::InvalidShape),
                }
            };
            if old_length >= column_count {
                continue;
            }

            let (inner, inner_data) = match kind {
                DirectoryType::CounterVectorSimple | DirectoryType::HistogramLog2 => {
                    let (inner, inner_data) =
                        state.allocate_vector::<u64>(column_count, None, 0_u64)?;
                    if old_length != 0 {
                        unsafe {
                            ptr::copy_nonoverlapping(
                                old_inner.cast::<u64>(),
                                inner_data,
                                old_length,
                            );
                        }
                    }
                    (inner, inner_data.cast::<u8>())
                }
                DirectoryType::CounterVectorCombined => {
                    let (inner, inner_data) = state.allocate_vector::<WireCounter>(
                        column_count,
                        None,
                        WireCounter::default(),
                    )?;
                    if old_length != 0 {
                        unsafe {
                            ptr::copy_nonoverlapping(
                                old_inner.cast::<WireCounter>(),
                                inner_data,
                                old_length,
                            );
                        }
                    }
                    (inner, inner_data.cast::<u8>())
                }
                _ => return Err(StatsError::InvalidShape),
            };
            unsafe {
                ptr::write(new_outer_data.add(row_index), inner_data);
            }
            staged.push(inner);
        }

        let mut candidate = Vec::new();
        candidate
            .try_reserve(state.directory_vector.len())
            .map_err(|_| StatsError::CollectionCapacity)?;
        candidate.extend(state.directory_vector.iter().copied());
        let Some(slot) = candidate.get_mut(raw_index) else {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: index.raw(),
                length: candidate.len(),
            });
        };
        *slot = DirectoryEntry::new(
            entry.kind(),
            entry.name_bytes()?,
            DirectoryData::from(DirectoryDataPointer::from(new_outer_data.cast::<c_void>())),
        );

        let new_block = state.allocate_directory(&candidate)?;
        state.publish(candidate, new_block)?;

        let Some(payloads) = state.payloads.get_mut(raw_index) else {
            return Err(StatsError::PublicationFailed);
        };
        payloads.extend(staged);
        payloads.push(new_outer);
        Ok(())
    }

    pub(crate) fn teardown(&mut self) -> StatsResult<()> {
        let mut state = self.state.lock();
        if state.tearing_down {
            return Ok(());
        }
        if Arc::strong_count(&self.state) != 1 {
            return Err(StatsError::WorkerNotQuiescent);
        }
        state.tearing_down = true;
        state.header.set_in_progress(true);
        state.write_shared_header(Ordering::Relaxed);
        state.header.set_directory_vector(ptr::null_mut());
        let epoch = state.header.epoch().wrapping_add(1);
        state.header.set_epoch(epoch);
        state.directory_vector.clear();
        state.names.clear();
        state.first_free = None;
        state.payloads.clear();
        state.retired.clear();
        state.header.set_in_progress(false);
        state.write_shared_header(Ordering::Release);
        Ok(())
    }
}

impl Drop for StatsSegment {
    fn drop(&mut self) {
        let _ = self.teardown();
    }
}

fn initial_directory() -> StatsResult<Vec<DirectoryEntry>> {
    let heartbeat = NameBytes::try_from("/sys/heartbeat")?;
    let last_stats_clear = NameBytes::try_from("/sys/last_stats_clear")?;
    let boottime = NameBytes::try_from("/sys/boottime")?;
    Ok(vec![
        DirectoryEntry::new(
            DirectoryType::ScalarIndex.into(),
            heartbeat,
            DirectoryData::from(ScalarBits::from(0_u64)),
        ),
        DirectoryEntry::new(
            DirectoryType::ScalarIndex.into(),
            last_stats_clear,
            DirectoryData::from(ScalarBits::from(0_u64)),
        ),
        DirectoryEntry::new(
            DirectoryType::ScalarIndex.into(),
            boottime,
            DirectoryData::from(ScalarBits::from(0_u64)),
        ),
    ])
}

fn directory_layout(length: usize) -> StatsResult<Layout> {
    let element_bytes = length
        .checked_mul(size_of::<DirectoryEntry>())
        .ok_or(StatsError::PublicationFailed)?;
    let bytes = VECTOR_HEADER_SIZE
        .checked_add(element_bytes)
        .ok_or(StatsError::PublicationFailed)?;
    Layout::from_size_align(bytes, VECTOR_DATA_ALIGNMENT).map_err(|_| StatsError::InvalidLayout)
}

fn vector_data_offset<T>() -> usize {
    VECTOR_DATA_ALIGNMENT.max(align_of::<T>())
}

fn vector_log2_alignment(align: usize) -> StatsResult<u8> {
    if !align.is_power_of_two() || align < VEC_MIN_ALIGN {
        return Err(StatsError::PublicationFailed);
    }
    u8::try_from(align.trailing_zeros()).map_err(|_| StatsError::PublicationFailed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_segment(name: &str) -> StatsSegment {
        match StatsSegment::create(name, 2 * 1024 * 1024) {
            Ok(segment) => segment,
            Err(error) => panic!("stats segment creation failed: {error}"),
        }
    }

    #[test]
    fn create_publishes_fixed_directory_and_shared_mapping() {
        let segment = create_segment("st-fixed");

        assert_eq!(segment.directory_vector_len(), 3);
        assert!(segment.shared_fd().is_some());
    }

    #[test]
    fn created_segment_exposes_shared_mapping_and_fixed_directory_length() -> StatsResult<()> {
        let segment = StatsSegment::create("st-owner", 2 * 1024 * 1024)?;
        assert!(segment.shared_fd().is_some());
        assert_eq!(segment.directory_vector_len(), 3);
        Ok(())
    }

    #[test]
    fn registration_remove_and_reuse_use_fixed_directory_indices() -> StatsResult<()> {
        let segment = create_segment("st-link");
        let _target = segment.register::<Scalar<Gauge>>("/target", ())?;
        segment.register::<Symlink>(
            "/link",
            SymlinkIndex {
                entry_index: 3,
                vector_index: 7,
            },
        )?;
        assert_eq!(segment.directory_vector_len(), 5);
        assert!(matches!(
            segment.register::<Symlink>(
                "/bad-link",
                SymlinkIndex {
                    entry_index: 99,
                    vector_index: 0,
                },
            ),
            Err(StatsError::DirectoryIndexOutOfBounds { index: 99, .. })
        ));

        segment.remove(DirectoryIndex::new(4))?;
        assert_eq!(segment.directory_vector_len(), 5);
        segment.register::<Scalar<Gauge>>("/replacement", ())?;
        segment.remove(DirectoryIndex::new(4))?;
        Ok(())
    }

    #[test]
    fn validation_grows_registered_matrix() -> StatsResult<()> {
        let segment = create_segment("st-retain");
        segment.register::<Simple<Counter>>("/retain/simple", (1, 1))?;

        segment.validate(DirectoryIndex::new(3), 2, 2)?;
        segment.remove(DirectoryIndex::new(3))?;
        assert!(matches!(
            segment.validate(DirectoryIndex::new(3), 0, 0),
            Err(StatsError::InvalidShape | StatsError::DirectoryEntryUnavailable { .. })
        ));
        Ok(())
    }

    #[test]
    fn validation_preserves_retained_rows_during_mixed_growth() -> StatsResult<()> {
        let segment = create_segment("st-mixed-growth");
        segment.register::<Simple<Counter>>("/mixed/simple", (2, 1))?;

        segment.validate(DirectoryIndex::new(3), 0, 2)?;
        segment.validate(DirectoryIndex::new(3), 1, 2)?;
        segment.remove(DirectoryIndex::new(3))?;
        Ok(())
    }

    #[test]
    fn name_vector_private_header_stores_directory_index_at_vec_header() -> StatsResult<()> {
        let segment = create_segment("st-name-header");
        let _handle = segment.register::<NameVector>("/names", 2)?;
        let state = segment.state.lock();
        let entry = state.directory_vector[3];
        let vector = StringVectorPointer::try_from(&entry)?.as_ptr() as usize;
        let base = state.mapping.base() as usize;
        let private_header = vector
            .checked_sub(VECTOR_DATA_ALIGNMENT)
            .ok_or(StatsError::PublicationFailed)?;
        assert_eq!(
            private_header,
            base + state.payloads[3][0].offset() as usize
        );
        let entry_index = unsafe { ptr::read_unaligned(private_header as *const u32) };
        assert_eq!(entry_index, 3);
        Ok(())
    }

    #[test]
    fn retired_allocations_and_handles_survive_publication_until_quiescence() -> StatsResult<()> {
        let mut segment = StatsSegment::create("st-retired", 2 * 1024 * 1024)?;
        let handle = segment.register::<Simple<Counter>>("/retired", (1, 1))?;
        let initial_payloads = segment.state.lock().payloads[3].len();
        segment.validate(DirectoryIndex::new(3), 2, 2)?;
        {
            let state = segment.state.lock();
            assert!(state.payloads[3].len() > initial_payloads);
            assert!(!state.retired.is_empty());
        }
        segment.remove(DirectoryIndex::new(3))?;
        assert!(matches!(
            segment.teardown(),
            Err(StatsError::WorkerNotQuiescent)
        ));
        drop(handle);
        segment.teardown()?;
        Ok(())
    }

    #[test]
    fn teardown_requires_owner_quiescence() -> StatsResult<()> {
        let mut segment = StatsSegment::create("st-quiesce", 2 * 1024 * 1024)?;
        let worker = segment.clone();

        assert!(matches!(
            segment.teardown(),
            Err(StatsError::WorkerNotQuiescent)
        ));

        drop(worker);
        segment.teardown()?;
        Ok(())
    }

    #[test]
    fn remove_rejects_repeated_removal_and_reuses_slot() -> StatsResult<()> {
        let segment = create_segment("st-remove");
        segment.register::<Scalar<Gauge>>("/remove/me", ())?;
        segment.remove(DirectoryIndex::new(3))?;
        assert!(matches!(
            segment.remove(DirectoryIndex::new(3)),
            Err(StatsError::DirectoryEntryUnavailable { index: 3 })
        ));
        segment.register::<Scalar<Gauge>>("/remove/replacement", ())?;
        assert_eq!(segment.directory_vector_len(), 4);
        Ok(())
    }

    #[test]
    fn teardown_withdraws_directory_and_shared_descriptor() -> StatsResult<()> {
        let mut segment = create_segment("st-teardown");
        segment.teardown()?;
        assert_eq!(segment.directory_vector_len(), 0);
        assert!(segment.shared_fd().is_none());
        assert!(matches!(
            segment.remove(DirectoryIndex::new(2)),
            Err(StatsError::Teardown)
        ));
        segment.teardown()?;
        Ok(())
    }
}

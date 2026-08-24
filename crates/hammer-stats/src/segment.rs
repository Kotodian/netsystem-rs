use std::alloc::Layout;
use std::collections::HashMap;
use std::ffi::c_void;
use std::mem::{align_of, replace, size_of};
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use hammer_infra::page_size;
use hammer_infra::segment::{Segment, SegmentAllocation};
use hammer_infra::sync::SpinLock;

use crate::metric::RecordKind;
use crate::protocol::{
    Counter, DirectoryData, DirectoryDataPointer, DirectoryEntry, DirectoryIndex, DirectoryType,
    NameBytes, STAT_SEGMENT_INDEX_INVALID, SharedHeader, VEC_MIN_ALIGN, vec_header_bytes, vec_len,
    vector_element_offset,
};
use crate::{StatsError, StatsResult};

const VECTOR_HEADER_SIZE: usize = 8;
const VECTOR_DATA_ALIGNMENT: usize = 64;

pub(super) struct StatsSegmentState {
    mapping: Segment,
    header: SharedHeader,
    directory_vector: Vec<DirectoryEntry>,
    directory_block: SegmentAllocation,
    payloads: Vec<Vec<SegmentAllocation>>,
    names: HashMap<NameBytes, DirectoryIndex>,
    first_free: Option<DirectoryIndex>,
    tearing_down: bool,
}

impl StatsSegmentState {
    pub(super) fn allocate_vector<T>(
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
        let header = vec_header_bytes(
            u32::try_from(count).map_err(|_| StatsError::PublicationFailed)?,
            u8::try_from(data_align / VEC_MIN_ALIGN).map_err(|_| StatsError::PublicationFailed)?,
            vector_log2_alignment(data_align)?,
            false,
            0,
            0,
        );
        let allocation_base = self.allocation_address(&allocation)?;
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
            let private_end = size_of::<u32>();
            if private_end > allocation.len() {
                return Err(StatsError::PublicationFailed);
            }
            let private_address = allocation_base;
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

    pub(super) fn mapping_size(&self) -> usize {
        self.mapping.size()
    }

    pub(super) fn allocation_address(&self, allocation: &SegmentAllocation) -> StatsResult<usize> {
        (self.mapping.base() as usize)
            .checked_add(
                usize::try_from(allocation.offset()).map_err(|_| StatsError::PublicationFailed)?,
            )
            .ok_or(StatsError::PublicationFailed)
    }

    pub(super) fn allocate_block(&self, layout: Layout) -> StatsResult<SegmentAllocation> {
        Ok(self.mapping.allocate(layout)?)
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
        let allocation_base = self.allocation_address(&allocation)?;
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
            .checked_add(
                candidate
                    .len()
                    .checked_mul(size_of::<DirectoryEntry>())
                    .ok_or(StatsError::PublicationFailed)?,
            )
            .ok_or(StatsError::PublicationFailed)?;
        if pointer_end > new_block.len()
            || !pointer_address.is_multiple_of(align_of::<DirectoryEntry>())
        {
            return Err(StatsError::PublicationFailed);
        }
        let pointer = pointer_address as *mut DirectoryEntry;
        let old_block = replace(&mut self.directory_block, new_block);
        self.directory_vector = candidate;
        self.header.set_in_progress(true);
        self.write_shared_header(Ordering::Relaxed);
        self.header.set_directory_vector(pointer);
        self.header.set_epoch(self.header.epoch().wrapping_add(1));
        self.write_shared_header(Ordering::Relaxed);
        self.header.set_in_progress(false);
        self.write_shared_header(Ordering::Release);
        drop(old_block);
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

fn directory_entries_for_write<'a>(
    state: &'a mut StatsSegmentState,
    index: DirectoryIndex,
    expected: DirectoryType,
) -> StatsResult<(&'a mut DirectoryEntry, &'a mut DirectoryEntry)> {
    if state.tearing_down {
        return Err(StatsError::Teardown);
    }
    let raw_index = usize::try_from(index.raw()).map_err(|_| StatsError::PublicationFailed)?;
    let published_pointer = state.header.directory_vector();
    if published_pointer.is_null() {
        return Err(StatsError::Teardown);
    }
    let length = state.directory_vector.len();
    let Some(private_entry) = state.directory_vector.get_mut(raw_index) else {
        return Err(StatsError::DirectoryIndexOutOfBounds {
            index: index.raw(),
            length,
        });
    };
    let actual = DirectoryType::try_from(private_entry.kind())?;
    if actual != expected {
        return Err(StatsError::MetricTypeMismatch {
            expected: expected.into(),
            actual: actual.into(),
        });
    }
    // SAFETY: publish and create set this pointer to an allocation containing
    // exactly `state.directory_vector.len()` entries. The state lock excludes
    // replacement while the temporary reference is used.
    let published_entry = unsafe { &mut *published_pointer.add(raw_index) };
    let actual = DirectoryType::try_from(published_entry.kind())?;
    if actual != expected {
        return Err(StatsError::MetricTypeMismatch {
            expected: expected.into(),
            actual: actual.into(),
        });
    }
    Ok((private_entry, published_entry))
}

fn counter_cell<T>(
    state: &StatsSegmentState,
    index: DirectoryIndex,
    row: u32,
    column: u32,
    expected: DirectoryType,
) -> StatsResult<*mut T> {
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
    let actual = DirectoryType::try_from(entry.kind())?;
    if actual != expected {
        return Err(StatsError::MetricTypeMismatch {
            expected: expected.into(),
            actual: actual.into(),
        });
    }
    let outer = DirectoryDataPointer::try_from(&entry)?.as_ptr().cast::<u8>();
    if outer.is_null() {
        return Err(StatsError::InvalidShape);
    }
    let row = usize::try_from(row).map_err(|_| StatsError::PublicationFailed)?;
    let outer_length = state.vector_len::<*mut u8>(outer)?;
    if row >= outer_length {
        return Err(StatsError::InvalidShape);
    }
    // SAFETY: the outer vector and row were validated while the state lock is held.
    let inner = unsafe { ptr::read(state.vector_element::<*mut u8>(outer, row)?) };
    if inner.is_null() {
        return Err(StatsError::InvalidShape);
    }
    let column = usize::try_from(column).map_err(|_| StatsError::PublicationFailed)?;
    let inner_length = state.vector_len::<T>(inner)?;
    if column >= inner_length {
        return Err(StatsError::InvalidShape);
    }
    state.vector_element::<T>(inner, column)
}

impl StatsSegment {
    pub(crate) fn create(name: &str, size: usize) -> StatsResult<Self> {
        let page = page_size()?;
        let minimum = page
            .checked_add(directory_layout(0)?.size())
            .ok_or(StatsError::PublicationFailed)?;
        if size < minimum {
            return Err(StatsError::CapacityTooSmall {
                requested: size,
                minimum,
            });
        }

        let mapping = Segment::shared_with_reserved_prefix(name, size, page)?;
        let directory_vector = Vec::new();
        let names = HashMap::new();

        let header = SharedHeader::new(mapping.base().cast::<c_void>());
        let bootstrap_layout =
            Layout::from_size_align(1, 1).map_err(|_| StatsError::InvalidLayout)?;
        let bootstrap_block = mapping.allocate(bootstrap_layout)?;
        let mut state = StatsSegmentState {
            mapping,
            header,
            directory_vector,
            directory_block: bootstrap_block,
            payloads: Vec::new(),
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
            .checked_add(
                state
                    .directory_vector
                    .len()
                    .checked_mul(size_of::<DirectoryEntry>())
                    .ok_or(StatsError::PublicationFailed)?,
            )
            .ok_or(StatsError::PublicationFailed)?;
        if initial_vector_end > directory_block.len()
            || !initial_vector_address.is_multiple_of(align_of::<DirectoryEntry>())
        {
            return Err(StatsError::PublicationFailed);
        }
        let directory_vector_pointer = initial_vector_address as *mut DirectoryEntry;
        state.header.set_directory_vector(directory_vector_pointer);
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

    pub(super) fn find(
        &self,
        name: NameBytes,
        path: &str,
        expected: DirectoryType,
    ) -> StatsResult<DirectoryIndex> {
        let state = self.state.lock();
        if state.tearing_down {
            return Err(StatsError::Teardown);
        }
        let Some(index) = state.names.get(&name).copied() else {
            return Err(StatsError::MetricNotFound {
                name: path.to_owned(),
            });
        };
        let raw_index = usize::try_from(index.raw()).map_err(|_| StatsError::PublicationFailed)?;
        let Some(entry) = state.directory_vector.get(raw_index) else {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: index.raw(),
                length: state.directory_vector.len(),
            });
        };
        let actual = DirectoryType::try_from(entry.kind())?;
        if actual != expected {
            return Err(StatsError::MetricTypeMismatch {
                expected: expected.into(),
                actual: actual.into(),
            });
        }
        Ok(index)
    }

    pub(super) fn store_timestamp(
        &self,
        index: DirectoryIndex,
        value: u64,
    ) -> StatsResult<()> {
        let mut state = self.state.lock();
        let (private_entry, published_entry) =
            directory_entries_for_write(&mut state, index, DirectoryType::ScalarIndex)?;
        private_entry.set_scalar_value(value);
        published_entry.set_scalar_value(value);
        Ok(())
    }

    pub(super) fn increment_timestamp(&self, index: DirectoryIndex) -> StatsResult<()> {
        let mut state = self.state.lock();
        let (private_entry, published_entry) =
            directory_entries_for_write(&mut state, index, DirectoryType::ScalarIndex)?;
        let value = private_entry.scalar_value().wrapping_add(1);
        private_entry.set_scalar_value(value);
        published_entry.set_scalar_value(value);
        Ok(())
    }

    pub(super) fn store_gauge(&self, index: DirectoryIndex, value: f64) -> StatsResult<()> {
        let mut state = self.state.lock();
        let (private_entry, published_entry) =
            directory_entries_for_write(&mut state, index, DirectoryType::Gauge)?;
        let value = value.to_bits();
        private_entry.set_scalar_value(value);
        published_entry.set_scalar_value(value);
        Ok(())
    }

    pub(super) fn add_simple_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        value: u64,
    ) -> StatsResult<()> {
        let state = self.state.lock();
        let cell = counter_cell::<u64>(
            &state,
            index,
            row,
            column,
            DirectoryType::CounterVectorSimple,
        )?;
        // SAFETY: counter_cell validated the family, row, and column, and the
        // state lock prevents payload publication or growth while this pointer is used.
        unsafe {
            let current = ptr::read(cell);
            ptr::write(cell, current.wrapping_add(value));
        }
        Ok(())
    }

    pub(super) fn add_combined_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        value: Counter,
    ) -> StatsResult<()> {
        let state = self.state.lock();
        let cell = counter_cell::<Counter>(
            &state,
            index,
            row,
            column,
            DirectoryType::CounterVectorCombined,
        )?;
        // SAFETY: counter_cell validated the family, row, and column, and the
        // state lock prevents payload publication or growth while this pointer is used.
        unsafe {
            let current = ptr::read(cell);
            ptr::write(cell, current.wrapping_add(value));
        }
        Ok(())
    }

    pub(super) fn add_histogram(
        &self,
        index: DirectoryIndex,
        row: u32,
        bucket: u32,
        value: u64,
    ) -> StatsResult<()> {
        let state = self.state.lock();
        let cell = counter_cell::<u64>(
            &state,
            index,
            row,
            bucket,
            DirectoryType::HistogramLog2,
        )?;
        // SAFETY: counter_cell validated the family, row, and column, and the
        // state lock prevents payload publication or growth while this pointer is used.
        unsafe {
            let current = ptr::read(cell);
            ptr::write(cell, current.wrapping_add(value));
        }
        Ok(())
    }

    pub(super) fn register<K>(&self, layout: K) -> StatsResult<K::Handle>
    where
        K: RecordKind,
    {
        let name = K::name(&layout);
        let mut state = self.state.lock();
        if state.tearing_down {
            return Err(StatsError::Teardown);
        }
        if state.names.contains_key(&name) {
            return Err(StatsError::DuplicateName);
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

        let (entry, storage, handle) = K::prepare(&state, index, layout)?;
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
        if raw_index == state.payloads.len() {
            state
                .payloads
                .try_reserve(1)
                .map_err(|_| StatsError::CollectionCapacity)?;
        }
        let new_block = state.allocate_directory(&candidate)?;
        state.publish(candidate, new_block)?;
        state.first_free = next_free;
        state.names.insert(name, index);
        if raw_index == state.payloads.len() {
            state.payloads.push(allocations);
        } else if let Some(payloads) = state.payloads.get_mut(raw_index) {
            *payloads = allocations;
        } else {
            return Err(StatsError::PublicationFailed);
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
        if raw_index >= state.payloads.len() {
            return Err(StatsError::PublicationFailed);
        }
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
        let old_payloads = replace(&mut state.payloads[raw_index], Vec::new());
        drop(old_payloads);
        state.names.remove(&name);
        state.first_free = Some(index);
        Ok(())
    }

    pub(crate) fn validate(&self, index: DirectoryIndex, row: u32, column: u32) -> StatsResult<()> {
        let needs_growth = {
            let state = self.state.lock();
            if state.tearing_down {
                return Err(StatsError::Teardown);
            }
            let raw_index =
                usize::try_from(index.raw()).map_err(|_| StatsError::PublicationFailed)?;
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
            let old_outer_length = if outer_pointer.is_null() {
                0
            } else {
                state.vector_len::<*mut u8>(outer_pointer)?
            };
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
                                state.vector_len::<Counter>(inner_pointer)?
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
            needs_growth
        };
        if !needs_growth {
            return Ok(());
        }

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
        let old_outer_length = if outer_pointer.is_null() {
            0
        } else {
            state.vector_len::<*mut u8>(outer_pointer)?
        };
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
                            state.vector_len::<Counter>(inner_pointer)?
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
            .checked_add(usize::from(!outer_pointer.is_null()))
            .ok_or(StatsError::CollectionCapacity)?;
        let Some(payloads) = state.payloads.get(raw_index) else {
            return Err(StatsError::PublicationFailed);
        };
        if payloads.len() < owner_count {
            return Err(StatsError::PublicationFailed);
        }

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
        if old_outer_length != 0 {
            unsafe {
                ptr::copy_nonoverlapping(
                    outer_pointer.cast::<*mut u8>(),
                    new_outer_data,
                    old_outer_length,
                );
            }
        }

        for row_index in 0..outer_length {
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
                        state.vector_len::<Counter>(old_inner)?
                    }
                    _ => return Err(StatsError::InvalidShape),
                }
            };
            if row_index >= row_count || old_length >= column_count {
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
                    let (inner, inner_data) =
                        state.allocate_vector::<Counter>(column_count, None, Counter::default())?;
                    if old_length != 0 {
                        unsafe {
                            ptr::copy_nonoverlapping(
                                old_inner.cast::<Counter>(),
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
        let mut replacement_payloads = Vec::new();
        replacement_payloads
            .try_reserve(
                outer_length
                    .checked_add(1)
                    .ok_or(StatsError::CollectionCapacity)?,
            )
            .map_err(|_| StatsError::CollectionCapacity)?;
        state.publish(candidate, new_block)?;

        let old_payloads = replace(&mut state.payloads[raw_index], Vec::new());
        let mut old_payloads = old_payloads.into_iter();
        let mut staged = staged.into_iter();
        // The published outer vector identifies which row pointers still own old allocations.
        for row_index in 0..outer_length {
            let old_pointer = if row_index < old_outer_length {
                unsafe { ptr::read(outer_pointer.cast::<*mut u8>().add(row_index)) }
            } else {
                ptr::null_mut()
            };
            let new_pointer = unsafe { ptr::read(new_outer_data.add(row_index)) };
            let old_inner = (row_index < old_outer_length)
                .then(|| old_payloads.next())
                .flatten();
            let retain_old =
                new_pointer == old_pointer && (row_index >= row_count || !old_pointer.is_null());
            if retain_old {
                if let Some(old_inner) = old_inner {
                    replacement_payloads.push(old_inner);
                }
            } else {
                drop(old_inner);
                if let Some(inner) = staged.next() {
                    replacement_payloads.push(inner);
                }
            }
        }
        drop(old_payloads.next());
        drop(old_payloads);
        drop(staged);
        replacement_payloads.push(new_outer);
        state.payloads[raw_index] = replacement_payloads;
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
    use crate::metric::{
        Gauge as MetricGauge, NameVector as MetricNameVector, SimpleCounter, layout,
    };
    use crate::protocol::{Gauge as ProtocolGauge, ScalarBits, StringVectorPointer};

    fn create_segment(name: &str) -> StatsSegment {
        match StatsSegment::create(name, 2 * 1024 * 1024) {
            Ok(segment) => segment,
            Err(error) => panic!("stats segment creation failed: {error}"),
        }
    }

    #[test]
    fn create_publishes_empty_directory_and_shared_mapping() {
        let segment = create_segment("st-fixed");

        assert_eq!(segment.directory_vector_len(), 0);
        assert!(segment.shared_fd().is_some());
    }

    #[test]
    fn bound_timestamp_stores_and_increments_by_directory_index() -> StatsResult<()> {
        let stats = crate::StatsMain::create("st-timestamp-owner", 2 * 1024 * 1024)?;
        stats.add_timestamp(crate::Timestamp::new("/sys/heartbeat"))?;

        let heartbeat = crate::Timestamp::bind(&stats, "/sys/heartbeat")?;
        heartbeat.store(&stats, 41)?;
        heartbeat.increment(&stats)?;

        let state = stats.segment.state.lock();
        let mapped_header = unsafe { &*state.mapping.base().cast::<SharedHeader>() };
        let published_directory = mapped_header.directory_vector();
        assert!(!published_directory.is_null());
        // SAFETY: the mapped header points to the current published directory
        // allocation, which contains the registered timestamp at index zero.
        let value = unsafe { ScalarBits::try_from(&*published_directory)? };
        assert_eq!(u64::from(value), 42);
        drop(state);

        stats.add_timestamp(crate::Timestamp::new("/sys/after-heartbeat"))?;
        let state = stats.segment.state.lock();
        let mapped_header = unsafe { &*state.mapping.base().cast::<SharedHeader>() };
        let published_directory = mapped_header.directory_vector();
        let value = unsafe { ScalarBits::try_from(&*published_directory)? };
        assert_eq!(u64::from(value), 42);
        Ok(())
    }

    #[test]
    fn sys_scalar_metrics_keep_vpp_positions_zero_one_two() -> StatsResult<()> {
        let stats = crate::StatsMain::create("st-sys-order", 2 * 1024 * 1024)?;
        for path in [
            "/sys/heartbeat",
            "/sys/last_stats_clear",
            "/sys/boottime",
        ] {
            stats.add_timestamp(crate::Timestamp::new(path))?;
        }

        let heartbeat = crate::Timestamp::bind(&stats, "/sys/heartbeat")?;
        let last_stats_clear = crate::Timestamp::bind(&stats, "/sys/last_stats_clear")?;
        let boottime = crate::Timestamp::bind(&stats, "/sys/boottime")?;
        heartbeat.store(&stats, 11)?;
        last_stats_clear.store(&stats, 22)?;
        boottime.store(&stats, 33)?;

        let state = stats.segment.state.lock();
        for (index, (path, value)) in [
            ("/sys/heartbeat", 11),
            ("/sys/last_stats_clear", 22),
            ("/sys/boottime", 33),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                state.directory_vector[index].name_bytes()?.as_c_str()?.to_bytes(),
                path.as_bytes()
            );
            assert_eq!(u64::from(ScalarBits::try_from(&state.directory_vector[index])?), value);
        }
        Ok(())
    }

    #[test]
    fn bound_gauge_stores_f64_bits_by_directory_index() -> StatsResult<()> {
        let stats = crate::StatsMain::create("st-gauge-owner", 2 * 1024 * 1024)?;
        stats.add_gauge(crate::Gauge::new("/sys/load"))?;

        let load = crate::Gauge::bind(&stats, "/sys/load")?;
        load.store(&stats, 3.5)?;

        let state = stats.segment.state.lock();
        assert_eq!(state.directory_vector[0].scalar_value(), 3.5_f64.to_bits());
        Ok(())
    }

    #[test]
    fn simple_counter_validates_then_adds_at_saved_directory_index() -> StatsResult<()> {
        let stats = crate::StatsMain::create("st-simple-owner", 2 * 1024 * 1024)?;
        stats.add_simple_counter(crate::SimpleCounter::new("/workers/requests"))?;

        let requests = crate::SimpleCounter::bind(&stats, "/workers/requests")?;
        requests.validate(&stats, 1, 2)?;
        requests.add(&stats, 1, 2, 7)?;

        let state = stats.segment.state.lock();
        let outer = DirectoryDataPointer::try_from(&state.directory_vector[0])?.as_ptr();
        let inner = unsafe { ptr::read(state.vector_element::<*mut u8>(outer.cast(), 1)?) };
        let value = state.vector_element::<u64>(inner, 2)?;
        assert_eq!(unsafe { ptr::read(value) }, 7);
        Ok(())
    }

    #[test]
    fn combined_counter_validates_then_adds_counter_pair_at_saved_index() -> StatsResult<()> {
        let stats = crate::StatsMain::create("st-combined-owner", 2 * 1024 * 1024)?;
        stats.add_combined_counter(crate::CombinedCounter::new("/workers/traffic"))?;

        let traffic = crate::CombinedCounter::bind(&stats, "/workers/traffic")?;
        traffic.validate(&stats, 0, 1)?;
        traffic.add(&stats, 0, 1, Counter { packets: 3, bytes: 5 })?;

        let state = stats.segment.state.lock();
        let outer = DirectoryDataPointer::try_from(&state.directory_vector[0])?.as_ptr();
        let inner = unsafe { ptr::read(state.vector_element::<*mut u8>(outer.cast(), 0)?) };
        let value = state.vector_element::<Counter>(inner, 1)?;
        assert_eq!(unsafe { ptr::read(value) }, Counter { packets: 3, bytes: 5 });
        Ok(())
    }

    #[test]
    fn histogram_validates_then_adds_at_saved_directory_index() -> StatsResult<()> {
        let stats = crate::StatsMain::create("st-histogram-owner", 2 * 1024 * 1024)?;
        stats.add_histogram(crate::Histogram::new("/workers/latency"))?;

        let latency = crate::Histogram::bind(&stats, "/workers/latency")?;
        latency.validate(&stats, 0, 4)?;
        latency.add(&stats, 0, 4, 9)?;

        let state = stats.segment.state.lock();
        let outer = DirectoryDataPointer::try_from(&state.directory_vector[0])?.as_ptr();
        let inner = unsafe { ptr::read(state.vector_element::<*mut u8>(outer.cast(), 0)?) };
        let value = state.vector_element::<u64>(inner, 4)?;
        assert_eq!(unsafe { ptr::read(value) }, 9);
        Ok(())
    }

    #[test]
    fn created_segment_exposes_shared_mapping_and_empty_directory() -> StatsResult<()> {
        let segment = StatsSegment::create("st-owner", 2 * 1024 * 1024)?;
        assert!(segment.shared_fd().is_some());
        assert_eq!(segment.directory_vector_len(), 0);
        Ok(())
    }

    #[test]
    fn registration_remove_and_reuse_start_at_zero() -> StatsResult<()> {
        let segment = create_segment("st-registration");
        let _target = segment.register::<layout::Scalar<ProtocolGauge>>(
            MetricGauge::new("/target").try_into()?,
        )?;
        segment.register::<layout::Scalar<ProtocolGauge>>(
            MetricGauge::new("/second").try_into()?,
        )?;
        assert_eq!(segment.directory_vector_len(), 2);

        segment.remove(DirectoryIndex::new(1))?;
        assert_eq!(segment.directory_vector_len(), 2);
        segment.register::<layout::Scalar<ProtocolGauge>>(
            MetricGauge::new("/replacement").try_into()?,
        )?;
        segment.remove(DirectoryIndex::new(1))?;
        Ok(())
    }

    #[test]
    fn validation_grows_registered_matrix() -> StatsResult<()> {
        let segment = create_segment("st-retain");
        segment.register::<layout::Simple<Counter>>(
            SimpleCounter::new("/retain/simple").try_into()?,
        )?;

        segment.validate(DirectoryIndex::new(0), 2, 2)?;
        segment.remove(DirectoryIndex::new(0))?;
        assert!(matches!(
            segment.validate(DirectoryIndex::new(0), 0, 0),
            Err(StatsError::InvalidShape | StatsError::DirectoryEntryUnavailable { .. })
        ));
        Ok(())
    }

    #[test]
    fn validation_preserves_retained_rows_during_mixed_growth() -> StatsResult<()> {
        let segment = create_segment("st-mixed-growth");
        segment.register::<layout::Simple<Counter>>(
            SimpleCounter::new("/mixed/simple").try_into()?,
        )?;

        segment.validate(DirectoryIndex::new(0), 0, 2)?;
        segment.validate(DirectoryIndex::new(0), 1, 2)?;
        segment.remove(DirectoryIndex::new(0))?;
        Ok(())
    }

    #[test]
    fn name_vector_private_header_stores_directory_index_at_vec_header() -> StatsResult<()> {
        let segment = create_segment("st-name-header");
        let _handle = segment.register::<layout::NameVector>(
            MetricNameVector {
                name: "/names".to_owned(),
                length: 2,
            }
            .try_into()?,
        )?;
        let state = segment.state.lock();
        let entry = state.directory_vector[0];
        let vector = StringVectorPointer::try_from(&entry)?.as_ptr() as usize;
        let base = state.mapping.base() as usize;
        let private_header = vector
            .checked_sub(VECTOR_DATA_ALIGNMENT)
            .ok_or(StatsError::PublicationFailed)?;
        assert_eq!(
            private_header,
            base + state.payloads[0][0].offset() as usize
        );
        let entry_index = unsafe { ptr::read_unaligned(private_header as *const u32) };
        assert_eq!(entry_index, 0);
        Ok(())
    }

    #[test]
    fn growth_reclaims_superseded_payloads_after_publication() -> StatsResult<()> {
        let mut segment = StatsSegment::create("st-growth-reclaim", 2 * 1024 * 1024)?;
        segment.register::<layout::Simple<Counter>>(
            SimpleCounter::new("/growth/reclaim").try_into()?,
        )?;
        let initial_payloads = segment.state.lock().payloads[0].len();
        segment.validate(DirectoryIndex::new(0), 2, 2)?;
        {
            let state = segment.state.lock();
            assert!(state.payloads[0].len() > initial_payloads);
        }
        segment.remove(DirectoryIndex::new(0))?;
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
        segment.register::<layout::Scalar<ProtocolGauge>>(
            MetricGauge::new("/remove/me").try_into()?,
        )?;
        segment.remove(DirectoryIndex::new(0))?;
        assert!(matches!(
            segment.remove(DirectoryIndex::new(0)),
            Err(StatsError::DirectoryEntryUnavailable { index: 0 })
        ));
        segment.register::<layout::Scalar<ProtocolGauge>>(
            MetricGauge::new("/remove/replacement").try_into()?,
        )?;
        assert_eq!(segment.directory_vector_len(), 1);
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

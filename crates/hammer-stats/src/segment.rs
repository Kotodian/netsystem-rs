//! VPP-shaped stats segment storage.
//!
//! A segment owns one `MemMain` VM mapping, the fixed-capacity `MemHeap`
//! created inside it, and every directory, counter, name and ring allocation
//! made from that heap. Structural changes publish through the shared header's
//! `in_progress`/`epoch` protocol, mirroring `vlib_stats_segment_lock` and
//! `vlib_stats_segment_unlock`.

use std::alloc::Layout;
use std::collections::HashMap;
use std::ffi::c_void;
use std::io;
use std::mem::size_of;
use std::os::fd::{AsFd, AsRawFd, OwnedFd, RawFd};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::time::Duration;

use hammer_infra::align::{CACHE_LINE, VEC_MIN_ALIGN};
use hammer_infra::mem::{MemError, MemHeap, MemMain, PageSize};
use hammer_infra::sync::SpinLock;

use crate::metric::{
    CombinedCounter, Gauge, Histogram, NameVector, Ring, RingSchema, SimpleCounter, Timestamp,
};
use crate::protocol::{
    Counter, DirectoryData, DirectoryEntry, DirectoryIndex, DirectoryType, NameBytes,
    RingBufferHeader, RingMetadata, STAT_COUNTER_HEARTBEAT, STAT_COUNTERS,
    STAT_SEGMENT_INDEX_INVALID, SharedHeader, SymlinkIndex, TypeCode, ring_layout,
    vec_header_bytes, vec_len,
};
use crate::{StatsError, StatsResult};

/// `sizeof(vec_header_t)` in `vppinfra/vec_bootstrap.h`.
const VECTOR_HEADER_BYTES: usize = 8;
/// `sizeof(vlib_stats_header_t)`: the entry index in front of a name vector.
const NAME_VECTOR_USER_HEADER: usize = 4;

pub struct StatsSegment {
    /// VPP `stat_segment_lockp` protects the directory fields (`stats.h:65-72`).
    stat_segment_lock: SpinLock<DirectoryState>,
    update_interval: Duration,
    memory_size: usize,
    node_counters_enabled: bool,
    heap: NonNull<MemHeap>,
    mapping: NonNull<u8>,
    memfd: OwnedFd,
}

struct DirectoryState {
    directory_vector_by_name: HashMap<NameBytes, DirectoryIndex>,
    dir_vector_first_free_elt: Option<DirectoryIndex>,
}

// SAFETY: the mapped addresses owned by a segment outlive every borrower (the
// segment is owned by the process-level `StatsMain` and has no destruction
// path), directory state is guarded by `stat_segment_lock`, each counter row
// has one writer, and cross-thread counter readers stop workers first.
unsafe impl Send for StatsSegment {}
unsafe impl Sync for StatsSegment {}

impl StatsSegment {
    /// Holds the stats directory lock and its reader-visible transaction
    /// across a graph refork, as `vlib_worker_thread_barrier_release` does.
    /// The caller must not invoke another directory mutation while held.
    pub fn lock_refork(&self) -> impl Sized + '_ {
        let stat_segment_lock = self.stat_segment_lock.lock();
        let transaction = DirectoryWrite::begin(self);
        (transaction, stat_segment_lock)
    }

    pub(crate) fn create(
        name: &str,
        size: usize,
        page_size: PageSize,
        update_interval: Duration,
        node_counters_enabled: bool,
    ) -> StatsResult<Self> {
        let page_bytes = page_size.bytes().map_err(|_| StatsError::Memory {
            source: MemError::PageSizeUnavailable {
                requested: page_size,
            },
        })?;
        let memory_size = size
            .checked_add(page_bytes - 1)
            .map(|value| value & !(page_bytes - 1))
            .ok_or(StatsError::CapacityTooSmall {
                requested: size,
                minimum: page_bytes,
            })?;
        let system_page_size = MemMain::system_page_size();
        let directory_bytes =
            STAT_COUNTERS as usize * size_of::<DirectoryEntry>() + 2 * VEC_MIN_ALIGN;
        let minimum = system_page_size + directory_bytes;
        if memory_size < minimum {
            return Err(StatsError::CapacityTooSmall {
                requested: size,
                minimum,
            });
        }
        let heap_bytes = memory_size - system_page_size;

        let memfd = MemMain::vm_create_backing(page_size, name)
            .map_err(|source| StatsError::Memory { source })?;
        if unsafe { libc::ftruncate(memfd.as_raw_fd(), memory_size as libc::off_t) } != 0 {
            return Err(StatsError::BackingResize {
                size: memory_size,
                source: io::Error::last_os_error(),
            });
        }
        let mapping = MemMain::vm_map(
            None,
            memory_size,
            page_size,
            Some(memfd.as_fd()),
            0,
            page_bytes,
            false,
            name,
        )
        .map_err(|source| StatsError::Memory { source })?;

        // The first system page carries the shared header; the heap covers the
        // remainder of the mapping, exactly like `vlib_stats_init`.
        let heap_base = unsafe { NonNull::new_unchecked(mapping.as_ptr().add(system_page_size)) };
        let heap = match unsafe { MemHeap::create_at(heap_base, heap_bytes, true, name) } {
            Ok(heap) => heap,
            Err(source) => {
                // A heap that was never created has nothing to destroy, so the
                // unpublished candidate rolls back through the mapping alone,
                // like `MainHeapConfig::initialize`.
                if unsafe { MemMain::vm_unmap(mapping) }.is_err() {
                    std::process::abort();
                }
                return Err(StatsError::Memory { source });
            }
        };
        let mut segment = Self {
            stat_segment_lock: SpinLock::new(DirectoryState {
                directory_vector_by_name: HashMap::new(),
                dir_vector_first_free_elt: None,
            }),
            update_interval,
            memory_size,
            node_counters_enabled,
            heap,
            mapping,
            memfd,
        };
        segment.initialize_shared_header();
        Ok(segment)
    }

    /// Publishes the shared header of an empty segment.
    ///
    /// The segment is not published yet, so this writes plain values. The
    /// fixed system slots are declared by the owning `derive(Stats)`
    /// aggregate and created through its bootstrap step.
    fn initialize_shared_header(&mut self) {
        // SAFETY: the mapping is live and no other thread can observe the
        // header before the segment is published.
        unsafe {
            ptr::write(
                self.shared_header(),
                SharedHeader::new(self.mapping.as_ptr().cast::<c_void>()),
            );
        }
    }

    /// Advances the fixed heartbeat slot by one round, the last step of
    /// `do_stat_segment_updates`.
    ///
    /// `Sys::bootstrap` creates the slot before the first round runs, so a
    /// missing or mistyped slot is this module's bug and asserts.
    pub(crate) fn advance_heartbeat(&self) {
        let index = DirectoryIndex::new(STAT_COUNTER_HEARTBEAT);
        let value = {
            let entry = self
                .entry(index)
                .expect("the heartbeat slot is created before the first round");
            entry
                .scalar_value()
                .expect("the heartbeat slot is a scalar timestamp")
        };
        self.set_timestamp(index, value.wrapping_add(1));
    }

    /// Returns the shared backing descriptor owned for the segment lifetime.
    pub fn segment_fd(&self) -> RawFd {
        self.memfd.as_raw_fd()
    }

    pub fn update_interval(&self) -> Duration {
        self.update_interval
    }

    pub fn node_counters_enabled(&self) -> bool {
        self.node_counters_enabled
    }

    pub fn find(&self, name: &str, expected: DirectoryType) -> StatsResult<DirectoryIndex> {
        let name_bytes = NameBytes::try_from(name)?;
        let directory = self.stat_segment_lock.lock();
        let index = directory
            .directory_vector_by_name
            .get(&name_bytes)
            .copied()
            .ok_or_else(|| StatsError::MetricNotFound {
                name: name.to_owned(),
            })?;
        drop(directory);
        let actual = self.entry(index)?.directory_type()?;
        if actual != expected {
            return Err(StatsError::MetricTypeMismatch { expected, actual });
        }
        Ok(index)
    }

    pub fn add_gauge(&self, name: &str) -> StatsResult<Gauge> {
        let index = self.create_entry(name, DirectoryType::Gauge, DirectoryData::value(0))?;
        Ok(Gauge { index })
    }

    pub fn add_timestamp(&self, name: &str) -> StatsResult<Timestamp> {
        let index = self.create_entry(name, DirectoryType::ScalarIndex, DirectoryData::value(0))?;
        Ok(Timestamp { index })
    }

    /// Writes one gauge value into its shared directory slot.
    ///
    /// The caller owns a declared slot, so a wrong index or type is this
    /// module's bug: the write asserts instead of returning a `Result`, like
    /// `vlib_stats_set_gauge`.
    pub fn set_gauge(&self, index: DirectoryIndex, value: u64) {
        self.entry_of_type(index, DirectoryType::Gauge)
            .expect("set_gauge writes a declared gauge slot")
            .set_scalar(value);
    }

    /// Writes one scalar timestamp into its shared directory slot, like
    /// `vlib_stats_set_timestamp`.
    pub fn set_timestamp(&self, index: DirectoryIndex, value: u64) {
        self.entry_of_type(index, DirectoryType::ScalarIndex)
            .expect("set_timestamp writes a declared scalar slot")
            .set_scalar(value);
    }

    /// Writes one cell of a simple counter vector, like the cell writes
    /// `vlib_stats_set_simple_counter` performs through the published vector.
    ///
    /// The shape must already be published by `validate`: this operation never
    /// expands rows or columns and never allocates. Writing outside the
    /// published shape is a bug in the owning collector, so it asserts with the
    /// index, row and column instead of returning a recoverable `Result`.
    pub fn set_simple_counter(&self, index: DirectoryIndex, row: u32, column: u32, value: u64) {
        self.entry_of_type(index, DirectoryType::CounterVectorSimple)
            .unwrap_or_else(|error| {
                panic!(
                    "set_simple_counter: directory index {} row {row} column {column} is not a simple counter vector: {error}",
                    index.raw()
                )
            })
            .set_simple_counter_cell(row, column, value);
    }

    /// Reads one published simple-counter cell, as VPP's `show errors` reads
    /// one per-thread error column. The caller stops worker writers first;
    /// the directory shape is fixed before use.
    pub fn simple_counter(&self, index: DirectoryIndex, row: u32, column: u32) -> u64 {
        self.entry_of_type(index, DirectoryType::CounterVectorSimple)
            .expect("simple_counter reads a declared simple counter vector")
            .simple_counter_cell(row, column)
    }

    /// Adds `increment` to one cell of a simple counter vector, the mechanism
    /// behind VPP's `em->counters[counter] += increment`
    /// (`third_party/vpp/src/vlib/error_funcs.h:35`).
    ///
    /// The shape must already be published by `validate`: this operation never
    /// expands rows or columns and never allocates, exactly like
    /// [`Self::set_simple_counter`]. A wrong index, type, row or column is a
    /// bug in the owning collector, so it asserts with those facts instead of
    /// returning a recoverable `Result`.
    pub fn increment_simple_counter(
        &self,
        index: DirectoryIndex,
        row: u32,
        column: u32,
        increment: u64,
    ) {
        self.entry_of_type(index, DirectoryType::CounterVectorSimple)
            .unwrap_or_else(|error| {
                panic!(
                    "increment_simple_counter: directory index {} row {row} column {column} is not a simple counter vector: {error}",
                    index.raw()
                )
            })
            .add_simple_counter_cell(row, column, increment);
    }

    pub fn add_simple_counter(&self, name: &str) -> StatsResult<SimpleCounter> {
        let index = self.create_entry(
            name,
            DirectoryType::CounterVectorSimple,
            DirectoryData::data(ptr::null_mut()),
        )?;
        Ok(SimpleCounter { index })
    }

    pub fn add_combined_counter(&self, name: &str) -> StatsResult<CombinedCounter> {
        let index = self.create_entry(
            name,
            DirectoryType::CounterVectorCombined,
            DirectoryData::data(ptr::null_mut()),
        )?;
        Ok(CombinedCounter { index })
    }

    pub fn add_histogram(&self, name: &str) -> StatsResult<Histogram> {
        let index = self.create_entry(
            name,
            DirectoryType::HistogramLog2,
            DirectoryData::data(ptr::null_mut()),
        )?;
        Ok(Histogram { index })
    }

    pub fn add_name_vector(&self, name: &str, length: u32) -> StatsResult<NameVector> {
        let data = allocate_vector(
            self.heap(),
            size_of::<*mut u8>(),
            length as usize,
            VEC_MIN_ALIGN,
            NAME_VECTOR_USER_HEADER,
            true,
        )?;
        let index = match self.create_entry(
            name,
            DirectoryType::NameVector,
            DirectoryData::string_vector(data.as_ptr().cast::<*mut u8>()),
        ) {
            Ok(index) => index,
            Err(error) => {
                // SAFETY: the name vector was never published.
                unsafe { free_vector(self.heap(), data, size_of::<*mut u8>()) };
                return Err(error);
            }
        };
        // SAFETY: the user header of a name vector holds its entry index.
        unsafe {
            let user_header =
                data.as_ptr()
                    .sub(vector_prefix(NAME_VECTOR_USER_HEADER, VEC_MIN_ALIGN, true));
            ptr::write(user_header.cast::<u32>(), index.raw());
        }
        Ok(NameVector { index })
    }

    pub fn add_ring<T: RingSchema>(&self, descriptor: Ring<T>) -> StatsResult<DirectoryIndex> {
        let config = descriptor.config();
        let schema = descriptor.schema();
        if config.entry_size() != T::ENTRY_SIZE {
            return Err(StatsError::InvalidRingSchema {
                expected: T::ENTRY_SIZE as usize,
                actual: config.entry_size() as usize,
            });
        }
        if config.schema_version() != T::SCHEMA_VERSION {
            return Err(StatsError::InvalidRingSchema {
                expected: T::SCHEMA_VERSION as usize,
                actual: config.schema_version() as usize,
            });
        }
        if config.schema_size() as usize != schema.len() || schema != T::schema() {
            return Err(StatsError::InvalidRingSchema {
                expected: T::schema().len(),
                actual: schema.len(),
            });
        }
        let (header, total) = ring_layout(config, self.memory_size)?;
        let layout = Layout::from_size_align(total, CACHE_LINE).expect("ring layout is valid");
        let heap = self.heap;
        // SAFETY: the segment owns this heap for the mapping lifetime.
        let segment_heap = unsafe { heap.as_ref() };
        // The ring payload is a shared-heap allocation, which VPP makes with the
        // segment heap active (`clib_mem_set_heap (sm->heap)` around
        // `clib_mem_alloc_aligned` in the ring buffer creation). A fixed-capacity
        // heap that cannot serve it ends the process through the ordinary
        // allocator's failure path instead of returning a control-plane error.
        let active_heap = segment_heap.activate();
        // SAFETY: the ring layout has a non-zero size.
        let allocation = unsafe { std::alloc::alloc_zeroed(layout) };
        let Some(allocation) = NonNull::new(allocation) else {
            // The segment is published and its capacity cannot grow: exhaustion
            // ends the process through the allocator's failure path.
            std::alloc::handle_alloc_error(layout)
        };
        drop(active_heap);
        // SAFETY: the allocation belongs to this segment and `total` bytes stay
        // writable for its lifetime.
        unsafe {
            ptr::write(allocation.as_ptr().cast::<RingBufferHeader>(), header);
            if !schema.is_empty() {
                let metadata_offset = usize::try_from(header.metadata_offset())
                    .expect("ring metadata offset fits usize");
                let schema_offset =
                    metadata_offset + config.n_threads() as usize * size_of::<RingMetadata>();
                ptr::copy_nonoverlapping(
                    schema.as_ptr(),
                    allocation.as_ptr().add(schema_offset),
                    schema.len(),
                );
                for thread in 0..config.n_threads() as usize {
                    ptr::write(
                        allocation
                            .as_ptr()
                            .add(metadata_offset + thread * size_of::<RingMetadata>())
                            .cast::<RingMetadata>(),
                        RingMetadata::new(
                            config.schema_version(),
                            schema_offset as u32,
                            config.schema_size(),
                        ),
                    );
                }
            }
        }
        match self.create_entry(
            descriptor.name(),
            DirectoryType::RingBuffer,
            DirectoryData::data(allocation.as_ptr().cast::<c_void>()),
        ) {
            Ok(index) => Ok(index),
            Err(error) => {
                // SAFETY: the allocation was made with this layout.
                unsafe { segment_heap.deallocate(allocation, layout) };
                Err(error)
            }
        }
    }

    pub fn add_symlink(
        &self,
        target: DirectoryIndex,
        column: u32,
        name: &str,
    ) -> StatsResult<DirectoryIndex> {
        let length = self.directory().len();
        if target.raw() as usize >= length {
            return Err(StatsError::DirectoryIndexOutOfBounds {
                index: target.raw(),
                length,
            });
        }
        self.create_entry(
            name,
            DirectoryType::Symlink,
            DirectoryData::symlink_index(SymlinkIndex {
                entry_index: target.raw(),
                vector_index: column,
            }),
        )
    }

    /// Publishes one name-vector element and its symlinks in a single
    /// reader-visible directory transaction.
    ///
    /// VPP's `statseg_sw_interface_add_del` holds the stats segment lock
    /// across the name and every family symlink. This operation stages both
    /// replacement vectors before changing the published directory pointer.
    pub fn add_name_symlinks(
        &self,
        name_vector: DirectoryIndex,
        element: u32,
        value: &str,
        links: &[(&str, DirectoryIndex)],
    ) -> StatsResult<Vec<DirectoryIndex>> {
        let link_names = links
            .iter()
            .map(|(path, _)| NameBytes::try_from(*path))
            .collect::<Result<Vec<_>, _>>()?;
        let mut directory = self.stat_segment_lock.lock();
        let published = self.directory_pointer();
        let published_length = self.directory().len();
        let name_outer = self
            .entry_of_type(name_vector, DirectoryType::NameVector)?
            .string_vector_pointer()?;
        let name_length = if name_outer.is_null() {
            0
        } else {
            unsafe { vector_length(name_outer.cast()) as usize }
        };
        let position = element as usize;
        // Reused interface columns contain VPP's "deleted" name. The new
        // publication replaces that string after the old symlinks are gone.
        for (offset, name) in link_names.iter().enumerate() {
            if directory.directory_vector_by_name.contains_key(name)
                || link_names[..offset].contains(name)
            {
                return Err(StatsError::DuplicateName {
                    name: links[offset].0.to_owned(),
                });
            }
            let target = links[offset].1;
            if target.raw() as usize >= published_length {
                return Err(StatsError::DirectoryIndexOutOfBounds {
                    index: target.raw(),
                    length: published_length,
                });
            }
        }

        let mut free = directory.dir_vector_first_free_elt;
        let mut appended = 0usize;
        let mut indices = Vec::with_capacity(links.len());
        for _ in links {
            if let Some(index) = free {
                free = {
                    let next = self.entry(index)?.directory_index()?;
                    (next.raw() != STAT_SEGMENT_INDEX_INVALID).then_some(next)
                };
                indices.push(index);
            } else {
                let index = u32::try_from(published_length + appended)
                    .map_err(|_| StatsError::InvalidShape)?;
                indices.push(DirectoryIndex::new(index));
                appended += 1;
            }
        }
        let name = allocate_string(self.heap(), value)?;
        let replacement_names = match allocate_vector(
            self.heap(),
            size_of::<*mut u8>(),
            name_length.max(position + 1),
            VEC_MIN_ALIGN,
            NAME_VECTOR_USER_HEADER,
            true,
        ) {
            Ok(vector) => vector,
            Err(error) => {
                unsafe { free_vector(self.heap(), name, 1) };
                return Err(error);
            }
        };
        let replacement_directory = match allocate_vector(
            self.heap(),
            size_of::<DirectoryEntry>(),
            published_length + appended,
            VEC_MIN_ALIGN,
            0,
            true,
        ) {
            Ok(vector) => vector,
            Err(error) => {
                unsafe {
                    free_vector(self.heap(), replacement_names, size_of::<*mut u8>());
                    free_vector(self.heap(), name, 1);
                }
                return Err(error);
            }
        };
        let next_names = replacement_names.as_ptr().cast::<*mut u8>();
        let next_directory = replacement_directory.as_ptr().cast::<DirectoryEntry>();
        let old_name = if position < name_length {
            unsafe { *name_outer.add(position) }
        } else {
            ptr::null_mut()
        };
        // SAFETY: all destination allocations are private until the pointer
        // publication below, and every copied prefix is in bounds.
        unsafe {
            if name_length != 0 {
                ptr::copy_nonoverlapping(name_outer, next_names, name_length);
            }
            ptr::write(next_names.add(position), name.as_ptr());
            ptr::write(
                replacement_names
                    .as_ptr()
                    .sub(vector_prefix(NAME_VECTOR_USER_HEADER, VEC_MIN_ALIGN, true))
                    .cast::<u32>(),
                name_vector.raw(),
            );
            if published_length != 0 {
                ptr::copy_nonoverlapping(published, next_directory, published_length);
            }
            (*next_directory.add(name_vector.raw() as usize))
                .set_data(DirectoryData::string_vector(next_names));
            for (offset, &index) in indices.iter().enumerate() {
                ptr::write(
                    next_directory.add(index.raw() as usize),
                    DirectoryEntry::new(
                        TypeCode::from(DirectoryType::Symlink),
                        link_names[offset],
                        DirectoryData::symlink_index(SymlinkIndex {
                            entry_index: links[offset].1.raw(),
                            vector_index: element,
                        }),
                    ),
                );
            }
        }
        let transaction = DirectoryWrite::begin(self);
        unsafe {
            AtomicPtr::from_ptr(ptr::addr_of_mut!((*self.shared_header()).directory_vector))
                .store(next_directory, Ordering::Release);
        }
        drop(transaction);
        directory.dir_vector_first_free_elt = free;
        for (name, &index) in link_names.into_iter().zip(&indices) {
            directory.directory_vector_by_name.insert(name, index);
        }
        drop(directory);
        if !old_name.is_null() {
            unsafe { free_vector(self.heap(), NonNull::new_unchecked(old_name), 1) };
        }
        if !name_outer.is_null() {
            unsafe {
                free_vector(
                    self.heap(),
                    NonNull::new_unchecked(name_outer.cast()),
                    size_of::<*mut u8>(),
                )
            };
        }
        if !published.is_null() {
            unsafe {
                free_vector(
                    self.heap(),
                    NonNull::new_unchecked(published.cast()),
                    size_of::<DirectoryEntry>(),
                )
            };
        }
        Ok(indices)
    }

    /// Deletes a name's symlinks and marks that name deleted in one epoch.
    pub fn remove_name_symlinks(
        &self,
        name_vector: DirectoryIndex,
        element: u32,
        links: &[DirectoryIndex],
    ) -> StatsResult<()> {
        let empty_name = NameBytes::try_from(&[] as &[u8])?;
        let mut directory = self.stat_segment_lock.lock();
        let published = self.directory_pointer();
        let published_length = self.directory().len();
        let name_outer = self
            .entry_of_type(name_vector, DirectoryType::NameVector)?
            .string_vector_pointer()?;
        let name_length = unsafe { vector_length(name_outer.cast()) as usize };
        let position = element as usize;
        if position >= name_length {
            return Err(StatsError::InvalidShape);
        }
        let mut link_names = Vec::with_capacity(links.len());
        for &index in links {
            let entry = self.entry_of_type(index, DirectoryType::Symlink)?;
            link_names.push(entry.name_bytes()?);
        }
        let deleted = allocate_string(self.heap(), "deleted")?;
        let replacement_names = match allocate_vector(
            self.heap(),
            size_of::<*mut u8>(),
            name_length,
            VEC_MIN_ALIGN,
            NAME_VECTOR_USER_HEADER,
            true,
        ) {
            Ok(vector) => vector,
            Err(error) => {
                unsafe { free_vector(self.heap(), deleted, 1) };
                return Err(error);
            }
        };
        let replacement_directory = match allocate_vector(
            self.heap(),
            size_of::<DirectoryEntry>(),
            published_length,
            VEC_MIN_ALIGN,
            0,
            true,
        ) {
            Ok(vector) => vector,
            Err(error) => {
                unsafe {
                    free_vector(self.heap(), replacement_names, size_of::<*mut u8>());
                    free_vector(self.heap(), deleted, 1);
                }
                return Err(error);
            }
        };
        let next_names = replacement_names.as_ptr().cast::<*mut u8>();
        let next_directory = replacement_directory.as_ptr().cast::<DirectoryEntry>();
        let old_name = unsafe { *name_outer.add(position) };
        let mut free = directory.dir_vector_first_free_elt;
        unsafe {
            ptr::copy_nonoverlapping(name_outer, next_names, name_length);
            ptr::write(next_names.add(position), deleted.as_ptr());
            ptr::write(
                replacement_names
                    .as_ptr()
                    .sub(vector_prefix(NAME_VECTOR_USER_HEADER, VEC_MIN_ALIGN, true))
                    .cast::<u32>(),
                name_vector.raw(),
            );
            ptr::copy_nonoverlapping(published, next_directory, published_length);
            (*next_directory.add(name_vector.raw() as usize))
                .set_data(DirectoryData::string_vector(next_names));
            for &index in links {
                ptr::write(
                    next_directory.add(index.raw() as usize),
                    DirectoryEntry::new(
                        TypeCode::from(DirectoryType::Empty),
                        empty_name,
                        DirectoryData::index(u64::from(
                            free.map_or(STAT_SEGMENT_INDEX_INVALID, DirectoryIndex::raw),
                        )),
                    ),
                );
                free = Some(index);
            }
        }
        let transaction = DirectoryWrite::begin(self);
        unsafe {
            AtomicPtr::from_ptr(ptr::addr_of_mut!((*self.shared_header()).directory_vector))
                .store(next_directory, Ordering::Release);
        }
        drop(transaction);
        directory.dir_vector_first_free_elt = free;
        for name in link_names {
            directory.directory_vector_by_name.remove(&name);
        }
        drop(directory);
        if !old_name.is_null() {
            unsafe { free_vector(self.heap(), NonNull::new_unchecked(old_name), 1) };
        }
        unsafe {
            free_vector(
                self.heap(),
                NonNull::new_unchecked(name_outer.cast()),
                size_of::<*mut u8>(),
            );
            free_vector(
                self.heap(),
                NonNull::new_unchecked(published.cast()),
                size_of::<DirectoryEntry>(),
            );
        }
        Ok(())
    }

    pub fn rename_symlink(&self, index: DirectoryIndex, name: &str) -> StatsResult<()> {
        let previous_name = {
            let entry = self.entry_of_type(index, DirectoryType::Symlink)?;
            entry.name_bytes()?
        };
        let name_bytes = NameBytes::try_from(name)?;
        let mut directory = self.stat_segment_lock.lock();
        if directory.directory_vector_by_name.contains_key(&name_bytes) {
            return Err(StatsError::DuplicateName {
                name: name.to_owned(),
            });
        }
        let transaction = DirectoryWrite::begin(self);
        // SAFETY: the caller holds the segment lock and `index` is a published
        // symlink slot.
        unsafe { (*self.entry_pointer(index)).set_name(name_bytes) };
        drop(transaction);
        directory.directory_vector_by_name.remove(&previous_name);
        directory.directory_vector_by_name.insert(name_bytes, index);
        drop(directory);
        Ok(())
    }

    /// Sets or clears one element of a name vector, like
    /// `vlib_stats_set_string_vector`.
    pub fn set_name(&self, index: DirectoryIndex, element: u32, value: &str) -> StatsResult<()> {
        let outer = {
            let entry = self.entry_of_type(index, DirectoryType::NameVector)?;
            entry.string_vector_pointer()?
        };
        // SAFETY: a published name vector owns its outer vector.
        let outer_length = if outer.is_null() {
            0
        } else {
            unsafe { vector_length(outer.cast::<u8>()) as usize }
        };
        let position = element as usize;
        if value.is_empty() {
            if position >= outer_length {
                return Ok(());
            }
            // SAFETY: `position` is inside the outer vector.
            let previous = unsafe { ptr::read(outer.add(position)) };
            if previous.is_null() {
                return Ok(());
            }
            let transaction = DirectoryWrite::begin(self);
            // SAFETY: the element belongs to the outer vector of this entry.
            unsafe { ptr::write(outer.add(position), ptr::null_mut()) };
            drop(transaction);
            // SAFETY: the element was allocated by `allocate_string`.
            unsafe { free_vector(self.heap(), NonNull::new_unchecked(previous), 1) };
            return Ok(());
        }

        let string = allocate_string(self.heap(), value)?;
        let mut replaced_outer = None;
        let mut target = outer;
        if position >= outer_length {
            let replacement = allocate_vector(
                self.heap(),
                size_of::<*mut u8>(),
                position + 1,
                VEC_MIN_ALIGN,
                NAME_VECTOR_USER_HEADER,
                true,
            )?;
            // SAFETY: both vectors belong to this segment and the previous
            // `outer_length` elements are initialized.
            unsafe {
                if outer_length != 0 {
                    ptr::copy_nonoverlapping(
                        outer,
                        replacement.as_ptr().cast::<*mut u8>(),
                        outer_length,
                    );
                }
                ptr::write(
                    replacement
                        .as_ptr()
                        .sub(vector_prefix(NAME_VECTOR_USER_HEADER, VEC_MIN_ALIGN, true))
                        .cast::<u32>(),
                    index.raw(),
                );
            }
            replaced_outer = NonNull::new(outer.cast::<u8>());
            target = replacement.as_ptr().cast::<*mut u8>();
        }
        // SAFETY: `position` is inside `target`, which is either the published
        // outer vector or its unpublished replacement.
        let previous = unsafe { ptr::read(target.add(position)) };
        let transaction = DirectoryWrite::begin(self);
        // SAFETY: the target element belongs to this entry.
        unsafe { ptr::write(target.add(position), string.as_ptr()) };
        if replaced_outer.is_some() {
            // SAFETY: the caller holds the segment lock and `index` is the
            // published name vector slot.
            unsafe {
                (*self.entry_pointer(index)).set_data(DirectoryData::string_vector(target));
            }
        }
        drop(transaction);
        if !previous.is_null() {
            // SAFETY: the replaced element was allocated by `allocate_string`.
            unsafe { free_vector(self.heap(), NonNull::new_unchecked(previous), 1) };
        }
        if let Some(replaced) = replaced_outer {
            // SAFETY: the replaced outer vector was swapped out above.
            unsafe { free_vector(self.heap(), replaced, size_of::<*mut u8>()) };
        }
        Ok(())
    }

    /// Grows the shared counter vectors so that `row`/`column` are addressable,
    /// like `vlib_stats_validate`.
    ///
    /// The replacement payload is created with the segment heap active, the
    /// window VPP opens with `clib_mem_set_heap (sm->heap)` in
    /// `vlib_stats_validate`: a counter row and its outer vector are
    /// default-heap vectors, so VPP releases them through the active heap.
    pub fn validate(&self, index: DirectoryIndex, row: u32, column: u32) -> StatsResult<()> {
        let (element_size, published) = {
            let entry = self.entry(index)?;
            let kind = entry.directory_type()?;
            let element_size = match kind {
                DirectoryType::CounterVectorSimple | DirectoryType::HistogramLog2 => {
                    size_of::<u64>()
                }
                DirectoryType::CounterVectorCombined => size_of::<Counter>(),
                _ => return Err(StatsError::InvalidShape),
            };
            (element_size, entry.data_pointer()?.cast::<*mut u8>())
        };
        // SAFETY: a published counter entry owns its outer vector.
        let published_length = if published.is_null() {
            0
        } else {
            unsafe { vector_length(published.cast::<u8>()) as usize }
        };
        let row_length = row as usize + 1;
        let column_length = column as usize + 1;
        let outer_length = published_length.max(row_length);
        let mut grows = row_length > published_length;
        for position in 0..published_length.min(row_length) {
            // SAFETY: `position` is inside the published outer vector.
            let row_pointer = unsafe { ptr::read(published.add(position)) };
            let row_length_here = if row_pointer.is_null() {
                0
            } else {
                // SAFETY: the published outer vector owns this row.
                unsafe { vector_length(row_pointer) as usize }
            };
            if row_length_here < column_length {
                grows = true;
                break;
            }
        }
        // Like `vlib_stats_validate`, an already addressable shape publishes
        // nothing and therefore allocates nothing.
        if !grows {
            return Ok(());
        }

        let heap = self.heap;
        // SAFETY: the segment owns this heap for the mapping lifetime.
        let segment_heap = unsafe { heap.as_ref() };

        // A replacement outer vector keeps every fresh row unreachable from the
        // published entry until the complete shape is addressable.
        let replacement = allocate_vector(
            segment_heap,
            size_of::<*mut u8>(),
            outer_length,
            CACHE_LINE,
            0,
            false,
        )?;
        let slots = replacement.as_ptr().cast::<*mut u8>();
        let mut installed = 0;
        while installed < row_length {
            let row_pointer = if installed < published_length {
                // SAFETY: `installed` is inside the published outer vector.
                unsafe { ptr::read(published.add(installed)) }
            } else {
                ptr::null_mut()
            };
            let row_length_here = if row_pointer.is_null() {
                0
            } else {
                // SAFETY: the published outer vector owns this row.
                unsafe { vector_length(row_pointer) as usize }
            };
            if row_length_here >= column_length {
                // SAFETY: `installed` is inside the replacement outer vector.
                unsafe { ptr::write(slots.add(installed), row_pointer) };
                installed += 1;
                continue;
            }
            let fresh = match allocate_vector(
                segment_heap,
                element_size,
                column_length,
                CACHE_LINE,
                0,
                false,
            ) {
                Ok(fresh) => fresh,
                Err(error) => {
                    for position in 0..installed {
                        // SAFETY: `position` is inside the replacement outer
                        // vector, which holds only this function's rows.
                        let written = unsafe { ptr::read(slots.add(position)) };
                        let previous = if position < published_length {
                            // SAFETY: `position` is inside the published outer vector.
                            unsafe { ptr::read(published.add(position)) }
                        } else {
                            ptr::null_mut()
                        };
                        if written != previous {
                            // SAFETY: `written` was allocated by this function.
                            unsafe {
                                free_vector(
                                    segment_heap,
                                    NonNull::new_unchecked(written),
                                    element_size,
                                )
                            };
                        }
                    }
                    // SAFETY: the replacement outer vector was allocated above.
                    unsafe { free_vector(segment_heap, replacement, size_of::<*mut u8>()) };
                    return Err(error);
                }
            };
            // SAFETY: `fresh` is a fresh zeroed vector and the published row
            // holds `row_length_here` initialized elements.
            unsafe {
                if row_length_here != 0 {
                    ptr::copy_nonoverlapping(
                        row_pointer,
                        fresh.as_ptr(),
                        row_length_here * element_size,
                    );
                }
                ptr::write(slots.add(installed), fresh.as_ptr());
            }
            installed += 1;
        }

        let transaction = DirectoryWrite::begin(self);
        // SAFETY: the caller holds the segment lock and `index` is the
        // published counter slot.
        unsafe {
            (*self.entry_pointer(index))
                .set_data(DirectoryData::data(replacement.as_ptr().cast::<c_void>()));
        }
        drop(transaction);

        for position in 0..published_length {
            // SAFETY: `position` is inside both outer vectors.
            let previous = unsafe { ptr::read(published.add(position)) };
            let current = unsafe { ptr::read(slots.add(position)) };
            if !previous.is_null() && previous != current {
                // SAFETY: the replaced row belonged to the published entry.
                unsafe {
                    free_vector(segment_heap, NonNull::new_unchecked(previous), element_size)
                };
            }
        }
        if !published.is_null() {
            // SAFETY: the replaced outer vector belonged to the published entry.
            unsafe {
                free_vector(
                    segment_heap,
                    NonNull::new_unchecked(published.cast::<u8>()),
                    size_of::<*mut u8>(),
                )
            };
        }
        Ok(())
    }

    /// Releases one directory entry and its payload back to the segment.
    ///
    /// Releasing an entry that is already free is a no-op: the slot is on the
    /// free list and no payload is reachable from it.
    pub fn remove_entry(&self, index: DirectoryIndex) -> StatsResult<()> {
        let (kind, name, payload) = {
            let entry = self.entry(index)?;
            let kind = entry.directory_type()?;
            if kind == DirectoryType::Empty {
                return Ok(());
            }
            let payload = match kind {
                DirectoryType::NameVector => entry.string_vector_pointer()?.cast::<c_void>(),
                DirectoryType::CounterVectorSimple
                | DirectoryType::HistogramLog2
                | DirectoryType::CounterVectorCombined
                | DirectoryType::RingBuffer => entry.data_pointer()?,
                _ => ptr::null_mut(),
            };
            (kind, entry.name_bytes()?, payload)
        };
        let mut directory = self.stat_segment_lock.lock();
        let next_free = directory.dir_vector_first_free_elt
            .map_or(STAT_SEGMENT_INDEX_INVALID, DirectoryIndex::raw);
        let transaction = DirectoryWrite::begin(self);
        // The slot becomes empty before its payload is released: a reader that
        // re-reads the directory never follows a payload that is already gone.
        self.store_entry(
            index,
            DirectoryEntry::new(
                TypeCode::from(DirectoryType::Empty),
                NameBytes::try_from(&[] as &[u8])?,
                DirectoryData::index(u64::from(next_free)),
            ),
        );
        drop(transaction);
        directory.directory_vector_by_name.remove(&name);
        directory.dir_vector_first_free_elt = Some(index);
        drop(directory);
        self.release_payload(kind, payload)
    }

    /// Releases one entry payload with the segment heap active, the window VPP
    /// opens with `clib_mem_set_heap (sm->heap)` around the `vec_free` loops in
    /// `vlib_stats_remove_entry` and around the ring buffer free.
    fn release_payload(&self, kind: DirectoryType, payload: *mut c_void) -> StatsResult<()> {
        let heap = self.heap;
        // SAFETY: the segment owns this heap for the mapping lifetime.
        let segment_heap = unsafe { heap.as_ref() };
        match kind {
            DirectoryType::NameVector => {
                let outer = payload.cast::<*mut u8>();
                if outer.is_null() {
                    return Ok(());
                }
                // SAFETY: the entry owns this outer vector.
                let length = unsafe { vector_length(outer.cast::<u8>()) as usize };
                for position in 0..length {
                    // SAFETY: `position` is inside the outer vector.
                    let string = unsafe { ptr::read(outer.add(position)) };
                    if !string.is_null() {
                        // SAFETY: each element is a string vector of this segment.
                        unsafe { free_vector(segment_heap, NonNull::new_unchecked(string), 1) };
                    }
                }
                // SAFETY: the outer vector belongs to this segment.
                unsafe {
                    free_vector(
                        segment_heap,
                        NonNull::new_unchecked(outer.cast::<u8>()),
                        size_of::<*mut u8>(),
                    )
                };
            }
            DirectoryType::CounterVectorSimple
            | DirectoryType::HistogramLog2
            | DirectoryType::CounterVectorCombined => {
                let element_size = if kind == DirectoryType::CounterVectorCombined {
                    size_of::<Counter>()
                } else {
                    size_of::<u64>()
                };
                let outer = payload.cast::<*mut u8>();
                if outer.is_null() {
                    return Ok(());
                }
                // SAFETY: the entry owns this outer vector.
                let length = unsafe { vector_length(outer.cast::<u8>()) as usize };
                for position in 0..length {
                    // SAFETY: `position` is inside the outer vector.
                    let row = unsafe { ptr::read(outer.add(position)) };
                    if !row.is_null() {
                        // SAFETY: each element is a row vector of this segment.
                        unsafe {
                            free_vector(segment_heap, NonNull::new_unchecked(row), element_size)
                        };
                    }
                }
                // SAFETY: the outer vector belongs to this segment.
                unsafe {
                    free_vector(
                        segment_heap,
                        NonNull::new_unchecked(outer.cast::<u8>()),
                        size_of::<*mut u8>(),
                    )
                };
            }
            DirectoryType::RingBuffer => {
                let ring = payload.cast::<u8>();
                if ring.is_null() {
                    return Ok(());
                }
                // SAFETY: the entry owns the ring allocation created by
                // `add_ring`, whose first bytes are the ring buffer header.
                let header = unsafe { ptr::read(ring.cast::<RingBufferHeader>()) };
                let (_, total) = ring_layout(header.config(), self.memory_size)?;
                let layout =
                    Layout::from_size_align(total, CACHE_LINE).expect("ring layout is valid");
                // SAFETY: the allocation was made with this layout.
                unsafe { segment_heap.deallocate(NonNull::new_unchecked(ring), layout) };
            }
            DirectoryType::Illegal
            | DirectoryType::ScalarIndex
            | DirectoryType::Gauge
            | DirectoryType::Empty
            | DirectoryType::Symlink => {}
        }
        Ok(())
    }

    /// Creates one directory entry, like `vlib_stats_new_entry_internal`:
    /// one transaction covers slot reuse or directory growth, the entry store
    /// and the directory pointer publication.
    fn create_entry(
        &self,
        name: &str,
        directory_type: DirectoryType,
        data: DirectoryData,
    ) -> StatsResult<DirectoryIndex> {
        let name_bytes = NameBytes::try_from(name)?;
        let mut directory = self.stat_segment_lock.lock();
        if directory.directory_vector_by_name.contains_key(&name_bytes) {
            return Err(StatsError::DuplicateName {
                name: name.to_owned(),
            });
        }
        let entry = DirectoryEntry::new(TypeCode::from(directory_type), name_bytes, data);
        let reused = match directory.dir_vector_first_free_elt {
            Some(index) => Some((index, self.entry(index)?.directory_index()?)),
            None => None,
        };
        let transaction = DirectoryWrite::begin(self);
        let index = match reused {
            Some((index, next)) => {
                directory.dir_vector_first_free_elt =
                    (next.raw() != STAT_SEGMENT_INDEX_INVALID).then_some(next);
                index
            }
            None => {
                let index = DirectoryIndex::new(self.directory().len() as u32);
                self.grow_directory(index.raw() as usize + 1)?;
                index
            }
        };
        self.store_entry(index, entry);
        drop(transaction);
        directory.directory_vector_by_name.insert(name_bytes, index);
        drop(directory);
        Ok(index)
    }

    /// Grows the shared directory vector to `length` entries, like the
    /// `vec_validate` in `vlib_stats_create_counter`.
    ///
    /// The caller holds the directory transaction open: the replacement vector
    /// is published here and the replaced one is released afterwards.
    fn grow_directory(&self, length: usize) -> StatsResult<()> {
        let previous = self.directory_pointer();
        let previous_length = self.directory().len();
        let data = allocate_vector(
            self.heap(),
            size_of::<DirectoryEntry>(),
            length,
            VEC_MIN_ALIGN,
            0,
            true,
        )?;
        // SAFETY: `data` names a fresh vector of `length` zeroed entries and
        // `previous` holds `previous_length` initialized entries.
        unsafe {
            if previous_length != 0 {
                ptr::copy_nonoverlapping(
                    previous,
                    data.as_ptr().cast::<DirectoryEntry>(),
                    previous_length,
                );
            }
            AtomicPtr::from_ptr(ptr::addr_of_mut!((*self.shared_header()).directory_vector))
                .store(data.as_ptr().cast::<DirectoryEntry>(), Ordering::Release);
        }
        if !previous.is_null() {
            // SAFETY: the replaced directory vector belongs to this segment.
            unsafe {
                free_vector(
                    self.heap(),
                    NonNull::new_unchecked(previous.cast::<u8>()),
                    size_of::<DirectoryEntry>(),
                )
            };
        }
        Ok(())
    }

    pub(crate) fn entry(&self, index: DirectoryIndex) -> StatsResult<&DirectoryEntry> {
        let directory = self.directory();
        directory
            .get(index.raw() as usize)
            .ok_or(StatsError::DirectoryIndexOutOfBounds {
                index: index.raw(),
                length: directory.len(),
            })
    }

    pub(crate) fn entry_of_type(
        &self,
        index: DirectoryIndex,
        expected: DirectoryType,
    ) -> StatsResult<&DirectoryEntry> {
        let entry = self.entry(index)?;
        let actual = entry.directory_type()?;
        if actual == expected {
            Ok(entry)
        } else {
            Err(StatsError::MetricTypeMismatch { expected, actual })
        }
    }

    /// The published slot of one directory entry, for structural writes.
    ///
    /// Directory writers hold [`StatsSegment::stat_segment_lock`] and have
    /// checked `index` against the published directory.
    fn entry_pointer(&self, index: DirectoryIndex) -> *mut DirectoryEntry {
        // SAFETY: the caller checked the index against the published directory.
        unsafe { self.directory_pointer().add(index.raw() as usize) }
    }

    /// Replaces one whole directory slot, like VPP's
    /// `sm->directory_vector[index] = entry`.
    fn store_entry(&self, index: DirectoryIndex, entry: DirectoryEntry) {
        // SAFETY: the caller holds the segment lock and `index` is inside the
        // published directory.
        unsafe { ptr::write(self.entry_pointer(index), entry) };
    }

    /// The fixed-capacity heap this segment owns.
    ///
    /// The control block lives inside the segment mapping, the segment is owned
    /// by the process-level `StatsMain` and has no runtime destruction path, so
    /// the borrow is valid for the caller's lifetime. Whether it is published as
    /// a `/mem` entry is the owning subsystem's decision; the segment only lends
    /// the heap.
    pub fn heap(&self) -> &'static MemHeap {
        // SAFETY: the heap control block lives inside the segment mapping, and
        // the segment is owned by the process-level `StatsMain` with no runtime
        // destruction path (VPP's stats segment has the same lifetime model), so
        // the borrow outlives every collector that stores it.
        unsafe { &*self.heap.as_ptr() }
    }

    fn shared_header(&self) -> *mut SharedHeader {
        self.mapping.as_ptr().cast::<SharedHeader>()
    }

    fn directory_pointer(&self) -> *mut DirectoryEntry {
        // SAFETY: the shared header is live for the mapping lifetime.
        unsafe { ptr::read(ptr::addr_of!((*self.shared_header()).directory_vector)) }
    }

    fn directory(&self) -> &[DirectoryEntry] {
        let pointer = self.directory_pointer();
        if pointer.is_null() {
            return &[];
        }
        // SAFETY: the published pointer names the directory vector owned by
        // this segment, whose element count precedes the first entry.
        let length = unsafe { vector_length(pointer.cast::<u8>()) as usize };
        unsafe { std::slice::from_raw_parts(pointer, length) }
    }
}

/// One directory transaction while it is being written.
///
/// This is the Rust form of the `vlib_stats_segment_lock` /
/// `vlib_stats_segment_unlock` pair (`third_party/vpp/src/vlib/stats/stats.c:11,32`):
/// the writer marks the shared directory as changing until the value drops,
/// then publishes the completed epoch. The existing `stat_segment_lock`
/// serializes directory writers; this value adds no mutual exclusion.
struct DirectoryWrite {
    header: NonNull<SharedHeader>,
}

impl DirectoryWrite {
    /// Marks one transaction open, like `vlib_stats_segment_lock`.
    fn begin(segment: &StatsSegment) -> Self {
        let header = NonNull::new(segment.shared_header()).expect("the segment mapping is live");
        // SAFETY: `header` names the shared header of the live mapping.
        unsafe {
            AtomicU64::from_ptr(ptr::addr_of_mut!((*header.as_ptr()).in_progress))
                .store(1, Ordering::Relaxed);
        }
        Self { header }
    }
}

impl Drop for DirectoryWrite {
    /// Publishes the completed epoch, like `vlib_stats_segment_unlock`.
    fn drop(&mut self) {
        // SAFETY: `header` names the shared header of the live mapping.
        unsafe {
            let header = self.header.as_ptr();
            AtomicU64::from_ptr(ptr::addr_of_mut!((*header).epoch)).fetch_add(1, Ordering::Relaxed);
            AtomicU64::from_ptr(ptr::addr_of_mut!((*header).in_progress))
                .store(0, Ordering::Release);
        }
    }
}

/// Alignment of the vector data prefix, per `vppinfra/vec.c`.
fn vector_prefix(user_header: usize, data_alignment: usize, explicit_heap: bool) -> usize {
    let base = user_header
        + VECTOR_HEADER_BYTES
        + if explicit_heap {
            size_of::<*mut c_void>()
        } else {
            0
        };
    base.next_multiple_of(data_alignment)
}

/// Allocates one stats segment vector and returns its data pointer.
///
/// `explicit_heap` writes the owning heap before the vector header, which is
/// what the `default_heap` header bit promises to a reader.
fn allocate_vector(
    heap: &MemHeap,
    element_size: usize,
    count: usize,
    data_alignment: usize,
    user_header: usize,
    explicit_heap: bool,
) -> StatsResult<NonNull<u8>> {
    let alignment = data_alignment.max(VEC_MIN_ALIGN);
    let prefix = vector_prefix(user_header, alignment, explicit_heap);
    let requested = element_size
        .checked_mul(count)
        .and_then(|bytes| prefix.checked_add(bytes))
        .ok_or(StatsError::InvalidShape)?;
    let Ok(length) = u32::try_from(count) else {
        return Err(StatsError::InvalidShape);
    };
    let layout = Layout::from_size_align(requested, alignment).expect("vector layout is valid");
    // The window makes the segment heap the active heap, so the ordinary
    // allocator serves the request; exhaustion is the allocator's failure path.
    let active_heap = heap.activate();
    // SAFETY: `requested` is non-zero for every caller.
    let pointer = unsafe { std::alloc::alloc_zeroed(layout) };
    let Some(pointer) = NonNull::new(pointer) else {
        std::alloc::handle_alloc_error(layout)
    };
    drop(active_heap);
    let header_size = u8::try_from(prefix / VEC_MIN_ALIGN).expect("vector prefix fits the header");
    // SAFETY: the allocation is owned by the segment and writable in full.
    unsafe {
        let data = pointer.as_ptr().add(prefix);
        ptr::write(
            data.sub(VECTOR_HEADER_BYTES).cast::<[u8; 8]>(),
            vec_header_bytes(
                length,
                header_size,
                alignment.trailing_zeros() as u8,
                !explicit_heap,
                0,
                0,
            ),
        );
        if explicit_heap {
            ptr::write(
                data.sub(2 * VECTOR_HEADER_BYTES).cast::<*mut c_void>(),
                (heap as *const MemHeap).cast_mut().cast::<c_void>(),
            );
        }
        Ok(NonNull::new_unchecked(data))
    }
}

/// Releases one vector allocated by [`allocate_vector`].
///
/// # Safety
///
/// `data` must be a vector data pointer produced by [`allocate_vector`] on
/// `heap`, and no borrow of it may survive.
unsafe fn free_vector(heap: &MemHeap, data: NonNull<u8>, element_size: usize) {
    let header =
        unsafe { ptr::read_unaligned(data.as_ptr().sub(VECTOR_HEADER_BYTES).cast::<[u8; 8]>()) };
    let prefix = usize::from(header[4]) * VEC_MIN_ALIGN;
    let length = vec_len(Some(&header)) as usize;
    let alignment = VEC_MIN_ALIGN << (header[5] & 0x7f);
    let layout = Layout::from_size_align(prefix + length * element_size, alignment)
        .expect("freed vector layout is valid");
    // SAFETY: the prefix belongs to the allocation made by `allocate_vector`.
    unsafe { heap.deallocate(NonNull::new_unchecked(data.as_ptr().sub(prefix)), layout) };
}

/// Reads the element count stored in front of a vector data pointer.
///
/// # Safety
///
/// `data` must be a vector data pointer owned by the caller.
pub(crate) unsafe fn vector_length(data: *const u8) -> u32 {
    let header = unsafe { ptr::read_unaligned(data.sub(VECTOR_HEADER_BYTES).cast::<[u8; 8]>()) };
    vec_len(Some(&header))
}

/// Allocates one NUL-terminated string vector in the stats heap.
fn allocate_string(heap: &MemHeap, value: &str) -> StatsResult<NonNull<u8>> {
    let data = allocate_vector(heap, 1, value.len() + 1, VEC_MIN_ALIGN, 0, true)?;
    // SAFETY: `data` holds `value.len() + 1` writable bytes.
    unsafe {
        ptr::copy_nonoverlapping(value.as_ptr(), data.as_ptr(), value.len());
        ptr::write(data.as_ptr().add(value.len()), 0);
    }
    Ok(data)
}

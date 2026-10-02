//! Per-object, per-thread counters backed by one stats segment entry.

use std::cell::UnsafeCell;
use std::ptr::NonNull;

use hammer_infra::align::CACHE_LINE;
use hammer_infra::prefetch::prefetch_write_l1;

use crate::metric::{CombinedCounter, SimpleCounter};
use crate::protocol::{Counter, DirectoryType};
use crate::segment::vector_length;
use crate::{StatsMain, StatsResult, StatsSegment};

/// One VPP-style simple counter family. The segment owns both vector levels.
pub struct SimpleCounterMain {
    entry: SimpleCounter,
    thread_count: u32,
    counters: UnsafeCell<NonNull<*mut u64>>,
}

/// One VPP-style packet/byte counter family. The segment owns both vector levels.
pub struct CombinedCounterMain {
    entry: CombinedCounter,
    thread_count: u32,
    counters: UnsafeCell<NonNull<*mut Counter>>,
}

// SAFETY: main-thread structural writes require stopped workers; each worker
// writes only its own preallocated row. Readers of all rows also stop writers.
unsafe impl Send for SimpleCounterMain {}
unsafe impl Sync for SimpleCounterMain {}
unsafe impl Send for CombinedCounterMain {}
unsafe impl Sync for CombinedCounterMain {}

impl SimpleCounterMain {
    /// Registers the family and validates every thread's first object column.
    pub fn new(
        segment: &StatsSegment,
        name: &str,
        thread_count: u32,
        first_index: u32,
    ) -> StatsResult<Self> {
        assert!(thread_count > 0, "simple counter main includes thread 0");
        let entry = segment.add_simple_counter(name)?;
        if let Err(error) = segment.validate(entry.index, thread_count - 1, first_index) {
            segment
                .remove_entry(entry.index)
                .expect("unpublished simple counter entry is removable");
            return Err(error);
        }
        let pointer = segment
            .entry_of_type(entry.index, DirectoryType::CounterVectorSimple)?
            .data_pointer()?;
        let counters = NonNull::new(pointer.cast::<*mut u64>())
            .expect("validated simple counter has an outer vector");
        assert_eq!(counters.as_ptr().addr() % CACHE_LINE, 0);
        for thread in 0..thread_count as usize {
            let row = unsafe { *counters.as_ptr().add(thread) };
            assert!(!row.is_null(), "validated simple counter row exists");
            assert_eq!(row.addr() % CACHE_LINE, 0);
        }
        Ok(Self {
            entry,
            thread_count,
            counters: UnsafeCell::new(counters),
        })
    }

    /// The control owner stops workers before a growth that moves either vector.
    pub fn validate(&self, index: u32) -> StatsResult<()> {
        let segment = &StatsMain::global()?.segment;
        segment.validate(self.entry.index, self.thread_count - 1, index)?;
        let pointer = segment
            .entry_of_type(self.entry.index, DirectoryType::CounterVectorSimple)?
            .data_pointer()?;
        let counters = NonNull::new(pointer.cast::<*mut u64>())
            .expect("validated simple counter has an outer vector");
        assert_eq!(counters.as_ptr().addr() % CACHE_LINE, 0);
        for thread in 0..self.thread_count as usize {
            let row = unsafe { *counters.as_ptr().add(thread) };
            assert!(!row.is_null(), "validated simple counter row exists");
            assert_eq!(row.addr() % CACHE_LINE, 0);
        }
        // SAFETY: the caller stopped every worker before structural growth.
        unsafe { *self.counters.get() = counters };
        Ok(())
    }

    pub fn n_counters(&self) -> u32 {
        let outer = unsafe { *self.counters.get() };
        let first_thread = unsafe { *outer.as_ptr() };
        assert!(!first_thread.is_null(), "simple counter first row exists");
        unsafe { vector_length(first_thread.cast()) }
    }

    #[inline(always)]
    pub fn prefetch(&self, thread_index: u32, index: u32) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let row = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        prefetch_write_l1(unsafe { row.add(index as usize) });
    }

    #[inline(always)]
    pub fn increment(&self, thread_index: u32, index: u32, increment: u64) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let row = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        let cell = unsafe { row.add(index as usize) };
        // SAFETY: only the executing thread writes this row.
        unsafe { *cell = (*cell).wrapping_add(increment) };
    }

    #[inline(always)]
    pub fn decrement(&self, thread_index: u32, index: u32, decrement: u64) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let row = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        let cell = unsafe { row.add(index as usize) };
        let value = unsafe { *cell };
        assert!(value >= decrement, "simple counter underflow");
        unsafe { *cell = value - decrement };
    }

    #[inline(always)]
    pub fn set(&self, thread_index: u32, index: u32, value: u64) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let row = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        unsafe { *row.add(index as usize) = value };
    }

    /// The caller stops all worker writers before cross-thread aggregation.
    #[inline(always)]
    pub fn get(&self, index: u32) -> u64 {
        assert!(index < self.n_counters());
        let mut total = 0u64;
        for thread in 0..self.thread_count {
            let row = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            total = total.wrapping_add(unsafe { *row.add(index as usize) });
        }
        total
    }

    /// The caller stops worker writers before resetting an object column.
    #[inline(always)]
    pub fn zero(&self, index: u32) {
        assert!(index < self.n_counters());
        for thread in 0..self.thread_count {
            let row = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            unsafe { *row.add(index as usize) = 0 };
        }
    }

    /// The caller stops worker writers before clearing the family.
    pub fn clear(&self) {
        let columns = self.n_counters();
        for thread in 0..self.thread_count {
            let row = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            for index in 0..columns {
                unsafe { *row.add(index as usize) = 0 };
            }
        }
    }

    /// The caller stops users before removing the segment-owned entry.
    pub fn remove(self) -> StatsResult<()> {
        StatsMain::global()?.segment.remove_entry(self.entry.index)
    }
}

impl CombinedCounterMain {
    pub fn new(
        segment: &StatsSegment,
        name: &str,
        thread_count: u32,
        first_index: u32,
    ) -> StatsResult<Self> {
        assert!(thread_count > 0, "combined counter main includes thread 0");
        let entry = segment.add_combined_counter(name)?;
        if let Err(error) = segment.validate(entry.index, thread_count - 1, first_index) {
            segment
                .remove_entry(entry.index)
                .expect("unpublished combined counter entry is removable");
            return Err(error);
        }
        let pointer = segment
            .entry_of_type(entry.index, DirectoryType::CounterVectorCombined)?
            .data_pointer()?;
        let counters = NonNull::new(pointer.cast::<*mut Counter>())
            .expect("validated combined counter has an outer vector");
        assert_eq!(counters.as_ptr().addr() % CACHE_LINE, 0);
        for thread in 0..thread_count as usize {
            let row = unsafe { *counters.as_ptr().add(thread) };
            assert!(!row.is_null(), "validated combined counter row exists");
            assert_eq!(row.addr() % CACHE_LINE, 0);
        }
        Ok(Self {
            entry,
            thread_count,
            counters: UnsafeCell::new(counters),
        })
    }

    /// VPP checks row capacity with the stats heap active. Current Hammer
    /// validation replaces a row for any length growth, so length is the
    /// relocation condition until StatsSegment gains in-place growth.
    pub fn will_expand(&self, index: u32) -> bool {
        let segment = &StatsMain::global()
            .expect("combined counter owns a stats entry")
            .segment;
        let active_heap = segment.heap().activate();
        let outer = unsafe { *self.counters.get() };
        let expands = (0..self.thread_count).any(|thread| {
            let row = unsafe { *outer.as_ptr().add(thread as usize) };
            assert!(!row.is_null(), "combined counter row exists");
            index >= unsafe { vector_length(row.cast()) }
        });
        drop(active_heap);
        expands
    }

    /// The control owner stops workers when will_expand reports relocation.
    pub fn validate(&self, index: u32) -> StatsResult<()> {
        let segment = &StatsMain::global()?.segment;
        segment.validate(self.entry.index, self.thread_count - 1, index)?;
        let pointer = segment
            .entry_of_type(self.entry.index, DirectoryType::CounterVectorCombined)?
            .data_pointer()?;
        let counters = NonNull::new(pointer.cast::<*mut Counter>())
            .expect("validated combined counter has an outer vector");
        assert_eq!(counters.as_ptr().addr() % CACHE_LINE, 0);
        for thread in 0..self.thread_count as usize {
            let row = unsafe { *counters.as_ptr().add(thread) };
            assert!(!row.is_null(), "validated combined counter row exists");
            assert_eq!(row.addr() % CACHE_LINE, 0);
        }
        unsafe { *self.counters.get() = counters };
        Ok(())
    }

    pub fn n_counters(&self) -> u32 {
        let outer = unsafe { *self.counters.get() };
        let first_thread = unsafe { *outer.as_ptr() };
        assert!(!first_thread.is_null(), "combined counter first row exists");
        unsafe { vector_length(first_thread.cast()) }
    }

    #[inline(always)]
    pub fn prefetch(&self, thread_index: u32, index: u32) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let row = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        prefetch_write_l1(unsafe { row.add(index as usize) });
    }

    #[inline(always)]
    pub fn increment(&self, thread_index: u32, index: u32, packets: u64, bytes: u64) {
        assert!(thread_index < self.thread_count && index < self.n_counters());
        let row = unsafe { *(*self.counters.get()).as_ptr().add(thread_index as usize) };
        let cell = unsafe { &mut *row.add(index as usize) };
        cell.packets = cell.packets.wrapping_add(packets);
        cell.bytes = cell.bytes.wrapping_add(bytes);
    }

    /// The caller stops all worker writers before cross-thread aggregation.
    #[inline]
    pub fn get(&self, index: u32) -> Counter {
        assert!(index < self.n_counters());
        let mut total = Counter::default();
        for thread in 0..self.thread_count {
            let row = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            let value = unsafe { *row.add(index as usize) };
            total.add(&value);
        }
        total
    }

    /// The caller stops worker writers before resetting an object column.
    #[inline(always)]
    pub fn zero(&self, index: u32) {
        assert!(index < self.n_counters());
        for thread in 0..self.thread_count {
            let row = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            unsafe { (*row.add(index as usize)).zero() };
        }
    }

    pub fn clear(&self) {
        let columns = self.n_counters();
        for thread in 0..self.thread_count {
            let row = unsafe { *(*self.counters.get()).as_ptr().add(thread as usize) };
            for index in 0..columns {
                unsafe { (*row.add(index as usize)).zero() };
            }
        }
    }

    pub fn remove(self) -> StatsResult<()> {
        StatsMain::global()?.segment.remove_entry(self.entry.index)
    }
}

impl Counter {
    #[inline(always)]
    pub fn add(&mut self, other: &Counter) {
        self.packets = self.packets.wrapping_add(other.packets);
        self.bytes = self.bytes.wrapping_add(other.bytes);
    }

    #[inline(always)]
    pub fn sub(&mut self, other: &Counter) {
        assert!(self.packets >= other.packets, "packet counter underflow");
        assert!(self.bytes >= other.bytes, "byte counter underflow");
        self.packets -= other.packets;
        self.bytes -= other.bytes;
    }

    #[inline(always)]
    pub fn zero(&mut self) {
        self.packets = 0;
        self.bytes = 0;
    }
}

use std::alloc::{GlobalAlloc, Layout, handle_alloc_error};
use std::fmt;
use std::marker::PhantomData;
use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::align::{self, CACHE_LINE};
use crate::bitmap::Bitmap;
use crate::heap::Heap;
use crate::heap_boxed::Slice;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Index {
    slot: u32,
    generation: u32,
}

impl Index {
    #[inline(always)]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self { slot, generation }
    }

    #[inline(always)]
    pub const fn slot(self) -> u32 {
        self.slot
    }

    #[inline(always)]
    pub const fn generation(self) -> u32 {
        self.generation
    }
}

pub struct Pool<T, const ALIGN: usize = CACHE_LINE> {
    ptr: NonNull<u8>,
    capacity: usize,
    len: usize,
    stride: usize,
    layout: Option<Layout>,
    free: Slice<u32>,
    free_len: usize,
    free_bitmap: Bitmap,
    generations: Slice<u32>,
    /// Heap that allocated the backing slab. Retained so `Drop` can route the
    /// slab dealloc through the same `Heap` (e.g. the SVM vtable's `dealloc`
    /// returns the offset to the shared-memory region; the Local vtable's
    /// `dealloc` calls `std::alloc::dealloc`).
    heap: Arc<Heap>,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send, const ALIGN: usize> Send for Pool<T, ALIGN> {}
unsafe impl<T: Sync, const ALIGN: usize> Sync for Pool<T, ALIGN> {}

impl<T, const ALIGN: usize> Pool<T, ALIGN> {
    /// Allocates the backing slab from the global allocator (`Heap::local`).
    /// Equivalent to `with_capacity_in(capacity, Arc::new(Heap::local()))`.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_in(capacity, Arc::new(Heap::local()))
    }

    /// Allocates the backing slab from the provided `heap` and retains the
    /// heap handle so that `Drop` can dealloc through the same allocator
    /// (the SVM vtable's `dealloc` returns the offset to the shared-memory
    /// region; the Local vtable's `dealloc` hands the slab back to the
    /// global allocator).
    #[inline]
    pub(crate) fn with_capacity_in(capacity: usize, heap: Arc<Heap>) -> Self {
        let stride = align::slot_stride::<T, ALIGN>();
        let layout = if capacity == 0 {
            None
        } else {
            let size = stride
                .checked_mul(capacity)
                .expect("aligned pool allocation size overflow");
            Some(
                Layout::from_size_align(size, align::allocation_align::<T, ALIGN>())
                    .expect("valid aligned pool layout"),
            )
        };
        let ptr = match layout {
            Some(layout) => {
                let ptr = heap
                    .alloc(layout)
                    .unwrap_or_else(|| handle_alloc_error(layout));
                // Zero the slab so freelist/bitmap walk assumptions hold and
                // `iter` / `remove` never read uninitialised bytes.
                unsafe { ptr::write_bytes(ptr.as_ptr(), 0, layout.size()) };
                ptr
            }
            None => NonNull::dangling(),
        };
        let mut free = Slice::from_elem_in(capacity, 0u32, heap.clone());
        for (offset, slot) in (0..capacity).rev().enumerate() {
            free[offset] = u32::try_from(slot).expect("pool slot index fits u32");
        }
        let mut free_bitmap = Bitmap::with_capacity(capacity);
        for slot in 0..capacity {
            free_bitmap.set(slot);
        }
        let generations = Slice::from_elem_in(capacity, 0u32, heap.clone());

        Self {
            ptr,
            capacity,
            len: 0,
            stride,
            layout,
            free,
            free_len: capacity,
            free_bitmap,
            generations,
            heap,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn insert(&mut self, value: T) -> Option<Index> {
        let slot = self.pop_free_slot()?;
        let slot_index = slot as usize;
        debug_assert!(self.free_bitmap.is_set(slot_index));
        let generation = self.generations[slot_index].wrapping_add(1).max(1);
        self.generations[slot_index] = generation;
        self.free_bitmap.clear(slot_index);
        self.len += 1;
        unsafe { self.slot_ptr_unchecked(slot_index).write(value) };
        Some(Index { slot, generation })
    }

    #[inline]
    pub fn insert_with(&mut self, f: impl FnOnce(Index) -> T) -> Option<Index> {
        let slot = self.pop_free_slot()?;
        let slot_index = slot as usize;
        debug_assert!(self.free_bitmap.is_set(slot_index));
        let generation = self.generations[slot_index].wrapping_add(1).max(1);
        self.generations[slot_index] = generation;
        self.free_bitmap.clear(slot_index);
        self.len += 1;
        let index = Index { slot, generation };
        unsafe { self.slot_ptr_unchecked(slot_index).write(f(index)) };
        Some(index)
    }

    #[inline]
    pub fn get(&self, index: Index) -> Option<&T> {
        self.validate(index)
            .map(|slot| unsafe { &*self.slot_ptr_unchecked(slot) })
    }

    #[inline]
    pub fn get_mut(&mut self, index: Index) -> Option<&mut T> {
        self.validate(index)
            .map(|slot| unsafe { &mut *self.slot_ptr_unchecked(slot) })
    }

    #[inline]
    pub fn remove(&mut self, index: Index) -> Option<T> {
        let slot = self.validate(index)?;
        self.free_bitmap.set(slot);
        self.push_free_slot(slot as u32);
        self.len -= 1;
        Some(unsafe { self.slot_ptr_unchecked(slot).read() })
    }

    #[inline]
    pub fn contains_key(&self, index: Index) -> bool {
        self.validate(index).is_some()
    }

    /// Returns the current live index at `slot`.
    ///
    /// Index-only event queues intentionally omit generations. Consumers use
    /// this operation to resolve the slot to whichever pool entry is live when
    /// the event is dispatched.
    #[inline]
    pub fn index_at_slot(&self, slot: u32) -> Option<Index> {
        let slot_index = slot as usize;
        (slot_index < self.capacity && !self.free_bitmap.is_set(slot_index)).then(|| Index {
            slot,
            generation: self.generations[slot_index],
        })
    }

    #[inline]
    pub fn iter(&self) -> PoolIter<'_, T, ALIGN> {
        PoolIter {
            pool: self,
            next_slot: 0,
        }
    }

    #[inline]
    pub fn slot_ptr(&self, index: Index) -> Option<*const T> {
        self.validate(index)
            .map(|slot| self.slot_ptr_unchecked(slot).cast_const())
    }

    #[inline]
    pub fn prefetch_slot(&self, index: Index) {
        let slot = index.slot() as usize;
        if slot >= self.capacity {
            return;
        }
        // SAFETY: slot < capacity checked above; pointer arithmetic stays inbounds.
        let ptr = self.slot_ptr_unchecked(slot);
        crate::prefetch::prefetch_read_l1(ptr);
    }

    #[inline]
    fn validate(&self, index: Index) -> Option<usize> {
        let slot = index.slot as usize;
        (slot < self.capacity
            && !self.free_bitmap.is_set(slot)
            && self.generations[slot] == index.generation)
            .then_some(slot)
    }

    #[inline]
    fn pop_free_slot(&mut self) -> Option<u32> {
        if self.free_len == 0 {
            return None;
        }
        self.free_len -= 1;
        Some(self.free[self.free_len])
    }

    #[inline]
    fn push_free_slot(&mut self, slot: u32) {
        debug_assert!(self.free_len < self.free.len());
        self.free[self.free_len] = slot;
        self.free_len += 1;
    }

    #[inline(always)]
    fn slot_ptr_unchecked(&self, slot: usize) -> *mut T {
        debug_assert!(slot < self.capacity);
        unsafe { self.ptr.as_ptr().add(slot * self.stride).cast::<T>() }
    }
}

pub struct PoolIter<'a, T, const ALIGN: usize = CACHE_LINE> {
    pool: &'a Pool<T, ALIGN>,
    next_slot: usize,
}

impl<'a, T, const ALIGN: usize> Iterator for PoolIter<'a, T, ALIGN> {
    type Item = (Index, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        while self.next_slot < self.pool.capacity {
            let slot = self.next_slot;
            self.next_slot += 1;
            if self.pool.free_bitmap.is_set(slot) {
                continue;
            }
            let index = Index::new(slot as u32, self.pool.generations[slot]);
            let value = unsafe { &*self.pool.slot_ptr_unchecked(slot) };
            return Some((index, value));
        }
        None
    }
}

impl<T, const ALIGN: usize> Drop for Pool<T, ALIGN> {
    fn drop(&mut self) {
        for slot in 0..self.capacity {
            if !self.free_bitmap.is_set(slot) {
                unsafe { ptr::drop_in_place(self.slot_ptr_unchecked(slot)) };
            }
        }
        if let Some(layout) = self.layout {
            // SAFETY: `layout` matches the one passed to `self.heap.alloc` in
            // `with_capacity_in`, and `self.ptr` was returned by that call.
            unsafe { GlobalAlloc::dealloc(&*self.heap, self.ptr.as_ptr(), layout) };
        }
    }
}

impl<T: fmt::Debug, const ALIGN: usize> fmt::Debug for Pool<T, ALIGN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("len", &self.len)
            .field("capacity", &self.capacity)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Pool;

    #[test]
    fn remove_and_reinsert_updates_generation_and_preserves_lookup_rules() {
        let mut pool = Pool::<u32>::with_capacity(4);
        let first = pool.insert(7).expect("first slot");
        assert_eq!(pool.remove(first), Some(7));
        let second = pool.insert(9).expect("second slot");
        assert_ne!(first.generation(), second.generation());
        assert_eq!(pool.get(first), None);
        assert_eq!(pool.get(second), Some(&9));
    }

    #[test]
    fn iter_skips_free_slots() {
        let mut pool = Pool::<u32>::with_capacity(4);
        let first = pool.insert(7).expect("first slot");
        let second = pool.insert(9).expect("second slot");
        let third = pool.insert(11).expect("third slot");
        assert_eq!(pool.remove(second), Some(9));

        let values: std::vec::Vec<_> = pool.iter().map(|(_, value)| *value).collect();
        assert_eq!(values, vec![7, 11]);
        assert_eq!(pool.get(first), Some(&7));
        assert_eq!(pool.get(third), Some(&11));
    }

    #[test]
    fn prefetch_slot_does_not_panic_on_invalid() {
        let pool = Pool::<u64>::with_capacity(4);
        let invalid = super::Index::new(999, 0);
        pool.prefetch_slot(invalid);
        let vacant = super::Index::new(2, 0);
        pool.prefetch_slot(vacant);
    }

    #[test]
    fn prefetch_slot_runs_on_occupied_slot() {
        let mut pool = Pool::<u64>::with_capacity(4);
        let idx = pool.insert(42).expect("insert");
        pool.prefetch_slot(idx);
        assert_eq!(pool.get(idx), Some(&42));
    }
}

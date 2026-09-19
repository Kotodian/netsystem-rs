//! VPP `vppinfra/heap` element space whose allocations are identified by the
//! offset of their first element.
//!
//! VPP's heap (`third_party/vpp/src/vppinfra/heap.h`) is a vector of elements
//! with a free list: a caller receives a numeric identity for a contiguous run
//! and may release that run later. Hammer uses the same stable-offset contract
//! for node error columns and Feature configuration words.
//!
//! Not related to [`crate::mem::MemHeap`]: that one is the dlmalloc-style
//! memory heap; this one is an offset identity space.

use std::collections::BTreeMap;
use std::mem::MaybeUninit;

/// An element space (VPP's `T *heap`) allocated in contiguous runs.
///
/// An allocation returns the offset of its first element. Live offsets remain
/// stable when other runs are allocated or released.
pub struct Heap<T> {
    values: Vec<MaybeUninit<T>>,
    allocations: BTreeMap<u32, u32>,
    free_runs: BTreeMap<u32, u32>,
}

impl<T> Default for Heap<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Heap<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            values: Vec::new(),
            allocations: BTreeMap::new(),
            free_runs: BTreeMap::new(),
        }
    }

    /// Allocates `elements` copies of `element` and returns the run offset.
    ///
    /// Released runs are reused before the element space grows. Capacity and
    /// offset overflow are process-memory invariants, matching the Main Heap's
    /// non-recoverable allocation contract.
    ///
    /// # Panics
    ///
    /// Panics if `elements` is zero or the element space cannot be represented
    /// by `u32` offsets.
    pub fn alloc(&mut self, elements: u32, element: T) -> u32
    where
        T: Clone,
    {
        assert!(elements > 0, "a heap allocation has at least one element");
        let initialized = vec![element; elements as usize];
        let reusable = self
            .free_runs
            .iter()
            .find(|(_, length)| **length >= elements)
            .map(|(&offset, &length)| (offset, length));
        let offset = if let Some((offset, length)) = reusable {
            self.free_runs.remove(&offset);
            if length != elements {
                self.free_runs.insert(
                    offset
                        .checked_add(elements)
                        .expect("a reusable heap run fits in u32 offsets"),
                    length - elements,
                );
            }
            offset
        } else {
            let offset = u32::try_from(self.values.len())
                .expect("the heap element space fits in a u32 offset");
            let end = offset
                .checked_add(elements)
                .expect("the heap element space fits in a u32 offset");
            self.values.resize_with(end as usize, MaybeUninit::uninit);
            offset
        };

        let end = offset
            .checked_add(elements)
            .expect("an allocated heap run fits in u32 offsets");
        for (position, value) in (offset..end).zip(initialized) {
            self.values[position as usize].write(value);
        }
        assert!(
            self.allocations.insert(offset, elements).is_none(),
            "a heap allocation cannot overlap a live run"
        );
        offset
    }

    /// Removes the allocation beginning at `offset`.
    ///
    /// # Panics
    ///
    /// Panics for an unknown offset or a repeated removal. The heap owns the
    /// run length; callers identify the allocation only by its stable offset.
    pub fn remove(&mut self, offset: u32) {
        let elements = self
            .allocations
            .remove(&offset)
            .expect("a heap removal names a live allocation");

        let end = offset
            .checked_add(elements)
            .expect("a live heap allocation fits in u32 offsets");
        for position in offset..end {
            // SAFETY: the allocation map proves every element in this exact
            // run is initialized and this release drops it exactly once.
            unsafe { self.values[position as usize].assume_init_drop() };
        }

        let mut start = offset;
        let mut length = elements;
        if let Some((&before, &before_length)) = self.free_runs.range(..offset).next_back()
            && before
                .checked_add(before_length)
                .expect("a free heap run fits in u32 offsets")
                == offset
        {
            self.free_runs.remove(&before);
            start = before;
            length = length
                .checked_add(before_length)
                .expect("coalesced heap runs fit in u32 offsets");
        }
        if let Some((&after, &after_length)) = self.free_runs.range(start..).next()
            && start
                .checked_add(length)
                .expect("a free heap run fits in u32 offsets")
                == after
        {
            self.free_runs.remove(&after);
            length = length
                .checked_add(after_length)
                .expect("coalesced heap runs fit in u32 offsets");
        }
        assert!(
            self.free_runs.insert(start, length).is_none(),
            "a released heap run cannot overlap another free run"
        );
    }

    /// The high-water length of the element space.
    #[inline]
    pub fn len(&self) -> u32 {
        u32::try_from(self.values.len()).expect("the heap element space fits in a u32 offset")
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.allocations.is_empty()
    }

    /// Reads one live element at `offset`.
    #[inline]
    pub fn get(&self, offset: u32) -> Option<&T> {
        self.get_slice(offset, 1).map(|values| &values[0])
    }

    /// Borrows a contiguous sub-run contained in one live allocation.
    pub fn get_slice(&self, offset: u32, elements: u32) -> Option<&[T]> {
        let allocation_end = self.allocation_end(offset)?;
        let end = offset.checked_add(elements)?;
        if elements == 0 || end > allocation_end {
            return None;
        }
        // SAFETY: allocation_end proves the requested MaybeUninit elements
        // are initialized and the shared borrow prevents mutation.
        Some(unsafe {
            std::slice::from_raw_parts(
                self.values.as_ptr().add(offset as usize).cast::<T>(),
                elements as usize,
            )
        })
    }

    /// Mutably borrows a contiguous sub-run contained in one live allocation.
    pub fn get_slice_mut(&mut self, offset: u32, elements: u32) -> Option<&mut [T]> {
        let allocation_end = self.allocation_end(offset)?;
        let end = offset.checked_add(elements)?;
        if elements == 0 || end > allocation_end {
            return None;
        }
        // SAFETY: allocation_end proves the requested MaybeUninit elements
        // are initialized and &mut self makes the returned run exclusive.
        Some(unsafe {
            std::slice::from_raw_parts_mut(
                self.values.as_mut_ptr().add(offset as usize).cast::<T>(),
                elements as usize,
            )
        })
    }

    fn allocation_end(&self, offset: u32) -> Option<u32> {
        let (&start, &length) = self.allocations.range(..=offset).next_back()?;
        let end = start
            .checked_add(length)
            .expect("a live heap allocation fits in u32 offsets");
        (offset < end).then_some(end)
    }
}

impl<T: Clone> Clone for Heap<T> {
    fn clone(&self) -> Self {
        let mut cloned = Self {
            values: Vec::with_capacity(self.values.len()),
            allocations: BTreeMap::new(),
            free_runs: self.free_runs.clone(),
        };
        cloned
            .values
            .resize_with(self.values.len(), MaybeUninit::uninit);
        for (&offset, &elements) in &self.allocations {
            let end = offset + elements;
            let initialized = self
                .get_slice(offset, elements)
                .expect("a cloned heap allocation is initialized")
                .to_vec();
            for (position, value) in (offset..end).zip(initialized) {
                cloned.values[position as usize].write(value);
            }
            cloned.allocations.insert(offset, elements);
        }
        cloned
    }
}

impl<T> Drop for Heap<T> {
    fn drop(&mut self) {
        for (&offset, &elements) in &self.allocations {
            let end = offset + elements;
            for position in offset..end {
                // SAFETY: every allocation-map run is initialized, disjoint,
                // and visited exactly once while the heap is dropped.
                unsafe { self.values[position as usize].assume_init_drop() };
            }
        }
    }
}

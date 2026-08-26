use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Range;

use crate::align::CacheLine;
use crate::bitmap::Bitmap;

/// A VPP-style index-addressed pool with cache-line-aligned storage.
///
/// Dynamic pools grow their vector as values are inserted. Fixed pools
/// preallocate their vector and reuse released indexes without growing.
/// Index zero has no special meaning to this container; consumers such as
/// the RB-tree may reserve it for their own sentinel.
pub struct Pool<T> {
    vector: Vec<CacheLine<MaybeUninit<T>>>,
    free_bitmap: Bitmap,
    free_indices: Vec<u32>,
    max_elts: Option<u32>,
    opaque: u32,
}

impl<T> Pool<T> {
    /// Creates a dynamic pool with an initial backing allocation capacity.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            vector: Vec::with_capacity(capacity),
            free_bitmap: Bitmap::with_capacity(capacity),
            free_indices: Vec::with_capacity(capacity),
            max_elts: None,
            opaque: 0,
        }
    }

    /// Preallocates storage for `max_elts` values and reuses released indexes.
    #[inline]
    pub fn with_fixed_capacity(max_elts: u32) -> Self {
        let capacity = max_elts as usize;
        let mut vector = Vec::with_capacity(capacity);
        vector.resize_with(capacity, || CacheLine::new(MaybeUninit::uninit()));

        let mut free_bitmap = Bitmap::with_capacity(capacity);
        for position in 0..capacity {
            free_bitmap.set(position);
        }

        let mut free_indices = Vec::with_capacity(capacity);
        for index in (0..max_elts).rev() {
            free_indices.push(index);
        }

        Self {
            vector,
            free_bitmap,
            free_indices,
            max_elts: Some(max_elts),
            opaque: 0,
        }
    }

    /// Returns the number of initialized values in the pool.
    #[inline]
    pub fn len(&self) -> usize {
        self.vector.len().saturating_sub(self.free_indices.len())
    }

    /// Returns whether the pool has no initialized values.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the backing allocation capacity in elements.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.vector.capacity()
    }

    /// Inserts a value and returns its numeric VPP pool index.
    #[inline]
    pub fn insert(&mut self, value: T) -> u32 {
        let index = if self.free_indices.is_empty() {
            if let Some(max_elts) = self.max_elts {
                panic!("fixed Pool exhausted at {max_elts} elements");
            }
            let index = self.vector.len() as u32;
            self.vector.push(CacheLine::new(MaybeUninit::uninit()));
            self.free_bitmap.clear(index as usize);
            index
        } else {
            let free_len = self.free_indices.len();
            let index = self.free_indices[free_len - 1];
            self.free_indices.truncate(free_len - 1);
            self.free_bitmap.clear(index as usize);
            index
        };

        self.vector[index as usize].write(value);
        index
    }

    /// Returns the initialized value at `index`.
    #[inline]
    pub fn get(&self, index: u32) -> Option<&T> {
        self.contains_key(index)
            .then(|| unsafe { self.vector[index as usize].assume_init_ref() })
    }

    /// Returns mutable access to the initialized value at `index`.
    #[inline]
    pub fn get_mut(&mut self, index: u32) -> Option<&mut T> {
        self.contains_key(index)
            .then(|| unsafe { self.vector[index as usize].assume_init_mut() })
    }

    /// Removes and returns the initialized value at `index`.
    #[inline]
    pub fn remove(&mut self, index: u32) -> Option<T> {
        let position = index as usize;
        if !self.contains_key(index) {
            return None;
        }
        // SAFETY: callers provide an occupied VPP pool index, so this position
        // contains an initialized value that is read exactly once here.
        let value = unsafe { self.vector[position].assume_init_read() };
        self.release_index(index, position);
        Some(value)
    }

    /// Returns whether `index` currently refers to an initialized value.
    #[inline]
    pub fn contains_key(&self, index: u32) -> bool {
        let position = index as usize;
        position < self.vector.len() && !self.free_bitmap.is_set(position)
    }

    /// Iterates over occupied indexes in numeric order.
    #[inline]
    pub fn iter(&self) -> PoolIter<'_, T> {
        PoolIter {
            pool: self,
            next_index: self.first_index(),
        }
    }

    #[inline]
    pub(crate) fn opaque(&self) -> u32 {
        self.opaque
    }

    #[inline]
    pub(crate) fn set_opaque(&mut self, opaque: u32) {
        self.opaque = opaque;
    }

    /// Returns the bytes used by the pool metadata vectors.
    #[inline]
    pub(crate) fn header_bytes(&self) -> usize {
        self.free_bitmap
            .word_len()
            .saturating_mul(std::mem::size_of::<u64>())
            .saturating_add(
                self.free_indices
                    .len()
                    .saturating_mul(std::mem::size_of::<u32>()),
            )
    }

    /// Returns the bytes represented by the logical pool and metadata vectors.
    #[inline]
    pub(crate) fn bytes(&self) -> usize {
        self.vector
            .len()
            .saturating_mul(std::mem::size_of::<CacheLine<MaybeUninit<T>>>())
            .saturating_add(self.header_bytes())
    }

    /// Returns the number of values available without growing the vector.
    #[inline]
    pub(crate) fn free_capacity(&self) -> usize {
        let spare = self.vector.capacity().saturating_sub(self.vector.len());
        self.free_indices.len().saturating_add(spare)
    }

    /// Reports whether inserting without a free index will grow the vector.
    #[inline]
    pub(crate) fn will_get_grow(&self) -> bool {
        self.free_indices.is_empty() && self.vector.len() == self.vector.capacity()
    }

    /// Reports whether releasing `index` will grow the free-index metadata.
    #[inline]
    pub(crate) fn will_put_grow(&self, index: u32) -> bool {
        self.contains_key(index) && self.free_indices.len() == self.free_indices.capacity()
    }

    /// Returns whether `index` is free or outside the logical vector.
    #[inline]
    pub(crate) fn is_free_index(&self, index: u32) -> bool {
        !self.contains_key(index)
    }

    /// Drops the initialized value at `index` and returns its position to the pool.
    #[inline]
    pub(crate) fn put_index(&mut self, index: u32) {
        let position = index as usize;
        // SAFETY: callers provide an occupied VPP pool index, so this position
        // contains an initialized value that is dropped exactly once here.
        unsafe { self.vector[position].assume_init_drop() };
        self.release_index(index, position);
    }

    /// Returns the first occupied numeric index.
    #[inline]
    pub(crate) fn first_index(&self) -> Option<u32> {
        self.free_bitmap
            .first_clear_from(0, self.vector.len())
            .map(|position| position as u32)
    }

    /// Returns the next occupied numeric index after `index`.
    #[inline]
    pub(crate) fn next_index(&self, index: u32) -> Option<u32> {
        self.free_bitmap
            .next_clear_after(index as usize, self.vector.len())
            .map(|position| position as u32)
    }

    /// Returns the next occupied index using the VPP pool traversal semantics.
    #[inline]
    pub(crate) fn next_occupied_index(&self, index: u32) -> Option<u32> {
        self.next_index(index)
    }

    /// Iterates over occupied numeric indexes.
    #[inline]
    pub(crate) fn indices(&self) -> impl Iterator<Item = u32> + '_ {
        std::iter::successors(self.first_index(), |&index| self.next_index(index))
    }

    /// Iterates over occupied numeric indexes in `[start, end)`.
    pub(crate) fn indices_range(&self, start: u32, end: u32) -> impl Iterator<Item = u32> + '_ {
        let first = if start >= end {
            None
        } else {
            self.free_bitmap
                .first_clear_from(start as usize, self.vector.len())
                .map(|position| position as u32)
        };
        std::iter::successors(first, |&index| self.next_occupied_index(index))
            .take_while(move |&index| index < end)
    }

    /// Iterates over contiguous occupied index regions.
    pub(crate) fn regions(&self) -> impl Iterator<Item = Range<u32>> + '_ {
        let mut indices = self.indices().peekable();
        std::iter::from_fn(move || {
            let start = indices.next()?;
            let mut end = start.saturating_add(1);
            while indices.peek().is_some_and(|&index| index == end) {
                indices.next();
                end = end.saturating_add(1);
            }
            Some(start..end)
        })
    }

    /// Drops every initialized value after applying `operation` to it.
    pub(crate) fn clear_with<F>(&mut self, mut operation: F)
    where
        F: FnMut(&mut T),
    {
        for position in 0..self.vector.len() {
            if self.free_bitmap.is_set(position) {
                continue;
            }
            let index = position as u32;
            // SAFETY: a clear free bit marks an initialized value.
            let value = unsafe { self.vector[position].assume_init_mut() };
            operation(value);
            // SAFETY: the callback only receives a valid initialized value and
            // it remains initialized until this drop.
            unsafe { self.vector[position].assume_init_drop() };
            self.release_index(index, position);
        }
    }

    /// Checks the Pool metadata and occupancy invariants.
    pub(crate) fn validate(&self) -> bool {
        if self.free_bitmap.count_set() != self.free_indices.len() {
            return false;
        }
        if self.len().saturating_add(self.free_indices.len()) != self.vector.len() {
            return false;
        }
        for (position, &index) in self.free_indices.iter().enumerate() {
            let index_usize = index as usize;
            if index_usize >= self.vector.len() || !self.free_bitmap.is_set(index_usize) {
                return false;
            }
            if self.free_indices[..position].contains(&index) {
                return false;
            }
        }
        true
    }

    #[inline]
    fn release_index(&mut self, index: u32, position: usize) {
        self.free_bitmap.set(position);
        self.free_indices.push(index);
    }
}

impl<T: Clone> Clone for Pool<T> {
    fn clone(&self) -> Self {
        let mut vector = Vec::with_capacity(self.vector.capacity());
        for position in 0..self.vector.len() {
            let value = if self.free_bitmap.is_set(position) {
                MaybeUninit::uninit()
            } else {
                // SAFETY: a clear free bit marks an initialized value.
                let value = unsafe { self.vector[position].assume_init_ref() };
                MaybeUninit::new(value.clone())
            };
            vector.push(CacheLine::new(value));
        }

        Self {
            vector,
            free_bitmap: self.free_bitmap.clone(),
            free_indices: self.free_indices.clone(),
            max_elts: self.max_elts,
            opaque: self.opaque,
        }
    }
}

/// Iterator over occupied pool indexes and values.
pub struct PoolIter<'a, T> {
    pool: &'a Pool<T>,
    next_index: Option<u32>,
}

impl<'a, T> Iterator for PoolIter<'a, T> {
    type Item = (u32, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next_index?;
        self.next_index = self.pool.next_occupied_index(index);
        let value = self.pool.get(index)?;
        Some((index, value))
    }
}

impl<T> Drop for Pool<T> {
    fn drop(&mut self) {
        for position in 0..self.vector.len() {
            if self.free_bitmap.is_set(position) {
                continue;
            }
            // SAFETY: a clear free bit marks an initialized value.
            unsafe { self.vector[position].assume_init_drop() };
        }
    }
}

impl<T> fmt::Debug for Pool<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("free", &self.free_indices.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::Pool;
    use crate::align::{CACHE_LINE, is_aligned};

    #[test]
    fn dynamic_zero_capacity_pool_grows_on_insert() {
        let mut pool = Pool::<u32>::with_capacity(0);

        assert_eq!(pool.insert(7), 0);
        assert_eq!(pool.get(0), Some(&7));
    }

    #[test]
    fn missing_indexes_are_ordinary_option_results() {
        let mut pool = Pool::<u32>::with_capacity(1);

        assert_eq!(pool.get(0), None);
        assert_eq!(pool.get_mut(0), None);
        assert_eq!(pool.remove(0), None);
    }

    #[test]
    fn preallocated_pool_grows_when_exhausted() {
        let mut pool = Pool::<u32>::with_fixed_capacity(2);
        assert_eq!(pool.insert(1), 0);
        assert_eq!(pool.insert(2), 1);

        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pool.insert(3);
            }))
            .is_err()
        );
        assert!(pool.validate());
    }

    #[test]
    fn dynamic_pool_free_capacity_includes_reserved_vector_space() {
        let pool = Pool::<u32>::with_capacity(4);

        assert_eq!(pool.free_capacity(), 4);
    }

    #[test]
    fn pool_reuses_released_indexes_in_lifo_order() {
        let mut pool = Pool::<u32>::with_capacity(3);
        let first = pool.insert(1);
        let second = pool.insert(2);
        let third = pool.insert(3);

        assert_eq!(pool.remove(first), Some(1));
        assert_eq!(pool.remove(second), Some(2));

        assert_eq!(pool.insert(4), second);
        assert_eq!(pool.insert(5), first);
        assert_eq!(pool.get(third), Some(&3));
    }

    #[test]
    fn pool_grows_without_changing_existing_numeric_indexes() {
        let mut pool = Pool::<u32>::with_capacity(1);
        let first = pool.insert(10);
        let second = pool.insert(20);

        assert_eq!((first, second), (0, 1));
        assert_eq!(pool.get(first), Some(&10));
        assert_eq!(pool.get(second), Some(&20));
    }

    #[test]
    fn pool_iteration_returns_sparse_indexes_in_numeric_order() {
        let mut pool = Pool::<u32>::with_capacity(5);
        let first = pool.insert(10);
        let second = pool.insert(20);
        let third = pool.insert(30);
        assert_eq!(pool.remove(second), Some(20));

        let indexes: Vec<_> = pool.indices().collect();
        let values: Vec<_> = pool.iter().map(|(_, value)| *value).collect();

        assert_eq!(indexes, vec![first, third]);
        assert_eq!(values, vec![10, 30]);
    }

    #[test]
    fn pool_regions_and_range_iteration_follow_occupied_indexes() {
        let mut pool = Pool::<u32>::with_capacity(6);
        assert_eq!(pool.insert(0), 0);
        assert_eq!(pool.insert(1), 1);
        let removed = pool.insert(2);
        assert_eq!(pool.insert(3), 3);
        assert_eq!(pool.insert(4), 4);
        assert_eq!(pool.remove(removed), Some(2));

        assert_eq!(pool.indices_range(1, 5).collect::<Vec<_>>(), vec![1, 3, 4]);
        assert_eq!(pool.regions().collect::<Vec<_>>(), vec![0..2, 3..5]);
    }

    #[test]
    fn pool_occupancy_queries_track_released_indexes() {
        let mut pool = Pool::<u32>::with_capacity(2);
        let index = pool.insert(7);
        let indexes: Vec<_> = pool.indices().collect();

        assert!(!pool.is_free_index(index));
        assert_eq!(pool.remove(index), Some(7));
        let freed_capacity = pool.free_capacity();
        let freed_indexes: Vec<_> = pool.indices().collect();
        assert!(pool.is_free_index(index));
        assert_eq!(pool.free_capacity(), freed_capacity);
        assert_eq!(pool.indices().collect::<Vec<_>>(), freed_indexes);
        assert_eq!(indexes, vec![index]);
    }

    #[test]
    fn pool_preflight_reports_vector_and_free_metadata_growth() {
        let mut pool = Pool::<u32>::with_capacity(1);
        assert!(!pool.will_get_grow());
        let index = pool.insert(1);
        assert!(pool.will_get_grow());
        assert!(!pool.will_put_grow(index));
        pool.put_index(index);
        assert!(pool.validate());
    }

    #[test]
    fn pool_clone_preserves_indexes_values_capacity_and_opaque_storage() {
        let mut pool = Pool::<u32>::with_capacity(4);
        let first = pool.insert(7);
        let removed = pool.insert(9);
        assert_eq!(pool.remove(removed), Some(9));
        pool.set_opaque(42);

        let clone = pool.clone();

        assert_eq!(clone.capacity(), pool.capacity());
        assert_eq!(clone.opaque(), 42);
        assert_eq!(clone.get(first), Some(&7));
        assert!(!clone.contains_key(removed));
        assert!(clone.validate());
    }

    #[test]
    fn pool_clear_with_and_drop_release_each_value_once() {
        struct DropCounter(Rc<Cell<usize>>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let drops = Rc::new(Cell::new(0));
        let mut pool = Pool::with_capacity(3);
        pool.insert(DropCounter(Rc::clone(&drops)));
        pool.insert(DropCounter(Rc::clone(&drops)));
        pool.clear_with(|_| {});
        assert_eq!(drops.get(), 2);
        drop(pool);
        assert_eq!(drops.get(), 2);
    }

    #[test]
    fn pool_remove_drops_returned_value_once() {
        struct DropCounter(Rc<Cell<usize>>);

        impl Drop for DropCounter {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }

        let drops = Rc::new(Cell::new(0));
        let mut pool = Pool::with_capacity(1);
        let index = pool.insert(DropCounter(Rc::clone(&drops)));
        let value = pool.remove(index);
        assert_eq!(drops.get(), 0);
        drop(value);
        assert_eq!(drops.get(), 1);
    }

    #[test]
    fn pool_validation_and_alignment_hold_for_fixed_storage() {
        let pool = Pool::<u64>::with_fixed_capacity(4);

        assert!(pool.validate());
        assert!(is_aligned(pool.vector.as_ptr(), CACHE_LINE));
        assert_eq!(
            pool.bytes(),
            pool.vector.len() * std::mem::size_of_val(&pool.vector[0]) + pool.header_bytes()
        );
    }
}

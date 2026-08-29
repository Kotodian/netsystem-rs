use std::fmt;
use std::mem::MaybeUninit;
use std::ops::Range;

use crate::bitmap::Bitmap;

/// A VPP-style index-addressed pool with natural `T` element layout.
///
/// Dynamic pools grow their vector as values are inserted. Fixed pools
/// preallocate their vector and reuse released indexes without growing.
/// Index zero has no special meaning to this container; consumers such as
/// the RB-tree may reserve it for their own sentinel.
pub struct Pool<T> {
    vector: Vec<MaybeUninit<T>>,
    free_bitmap: Bitmap,
    free_indices: Vec<u32>,
    max_elts: Option<u32>,
    opaque: u32,
}

impl<T> Default for Pool<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Pool<T> {
    /// Creates a VPP-style dynamic pool with no initial reservation.
    #[inline]
    pub const fn new() -> Self {
        Self {
            vector: Vec::new(),
            free_bitmap: Bitmap::new(),
            free_indices: Vec::new(),
            max_elts: None,
            opaque: 0,
        }
    }

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
        vector.resize_with(capacity, MaybeUninit::uninit);

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
            self.vector.push(MaybeUninit::uninit());
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
            .saturating_mul(std::mem::size_of::<MaybeUninit<T>>())
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
            vector.push(value);
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

use std::alloc::{alloc, dealloc, handle_alloc_error};
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::ptr::{self, NonNull};

use crate::align;

pub struct AlignedVec<T, const ALIGN: usize> {
    ptr: NonNull<T>,
    len: usize,
    capacity: usize,
    marker: PhantomData<T>,
}

unsafe impl<T: Send, const ALIGN: usize> Send for AlignedVec<T, ALIGN> {}
unsafe impl<T: Sync, const ALIGN: usize> Sync for AlignedVec<T, ALIGN> {}

impl<T, const ALIGN: usize> AlignedVec<T, ALIGN> {
    #[inline]
    pub const fn new() -> Self {
        assert!(ALIGN != 0 && ALIGN.is_power_of_two());
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            capacity: 0,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(ALIGN != 0 && ALIGN.is_power_of_two());
        let ptr = if capacity == 0 {
            NonNull::dangling()
        } else {
            Self::allocate(capacity)
        };
        Self {
            ptr,
            len: 0,
            capacity,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: only the initialized prefix is exposed.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: only the initialized prefix is exposed through unique access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        let required = self
            .len
            .checked_add(additional)
            .expect("aligned vector length overflow");
        if required <= self.capacity {
            return;
        }
        let doubled = self.capacity.saturating_mul(2).max(4);
        self.grow(required.max(doubled));
    }

    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        let required = self
            .len
            .checked_add(additional)
            .expect("aligned vector length overflow");
        if required > self.capacity {
            self.grow(required);
        }
    }

    #[inline]
    pub fn push(&mut self, value: T) {
        self.reserve(1);
        // SAFETY: reserve established one initialized-write slot.
        unsafe { self.ptr.as_ptr().add(self.len).write(value) };
        self.len += 1;
    }

    #[inline]
    pub fn resize(&mut self, new_len: usize, value: T)
    where
        T: Clone,
    {
        if new_len < self.len {
            // SAFETY: the range is initialized and is removed from the vector.
            unsafe {
                ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                    self.ptr.as_ptr().add(new_len),
                    self.len - new_len,
                ));
            }
            self.len = new_len;
            return;
        }
        self.reserve_exact(new_len - self.len);
        while self.len < new_len {
            // SAFETY: reserve established one initialized-write slot.
            unsafe { self.ptr.as_ptr().add(self.len).write(value.clone()) };
            self.len += 1;
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        // SAFETY: the range is the initialized prefix.
        unsafe { ptr::drop_in_place(self.as_mut_slice()) };
        self.len = 0;
    }

    fn allocate(capacity: usize) -> NonNull<T> {
        let layout = align::array_layout::<T, ALIGN>(capacity);
        // SAFETY: `layout` is valid and the global allocator owns the result.
        let pointer = unsafe { alloc(layout) };
        NonNull::new(pointer.cast()).unwrap_or_else(|| handle_alloc_error(layout))
    }

    fn grow(&mut self, new_capacity: usize) {
        let new_ptr = Self::allocate(new_capacity);
        for index in 0..self.len {
            // SAFETY: both allocations are valid; each old value is moved once.
            unsafe {
                new_ptr
                    .as_ptr()
                    .add(index)
                    .write(self.ptr.as_ptr().add(index).read());
            }
        }
        if self.capacity != 0 {
            let layout = align::array_layout::<T, ALIGN>(self.capacity);
            // SAFETY: this is the allocation and layout owned by `self`.
            unsafe { dealloc(self.ptr.as_ptr().cast(), layout) };
        }
        self.ptr = new_ptr;
        self.capacity = new_capacity;
    }
}

impl<T, const ALIGN: usize> Default for AlignedVec<T, ALIGN> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone, const ALIGN: usize> Clone for AlignedVec<T, ALIGN> {
    fn clone(&self) -> Self {
        let mut result = Self::with_capacity(self.len);
        for value in self.as_slice() {
            result.push(value.clone());
        }
        result
    }
}

impl<T: fmt::Debug, const ALIGN: usize> fmt::Debug for AlignedVec<T, ALIGN> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(formatter)
    }
}

impl<T, const ALIGN: usize> Drop for AlignedVec<T, ALIGN> {
    fn drop(&mut self) {
        // SAFETY: only the initialized prefix contains values.
        unsafe { ptr::drop_in_place(self.as_mut_slice()) };
        if self.capacity != 0 {
            let layout = align::array_layout::<T, ALIGN>(self.capacity);
            // SAFETY: this is the allocation and layout owned by `self`.
            unsafe { dealloc(self.ptr.as_ptr().cast(), layout) };
        }
    }
}

impl<T, const ALIGN: usize> Deref for AlignedVec<T, ALIGN> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const ALIGN: usize> DerefMut for AlignedVec<T, ALIGN> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T, const ALIGN: usize> Index<usize> for AlignedVec<T, ALIGN> {
    type Output = T;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<T, const ALIGN: usize> IndexMut<usize> for AlignedVec<T, ALIGN> {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.as_mut_slice()[index]
    }
}

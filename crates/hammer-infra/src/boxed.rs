use std::alloc::{alloc, dealloc, handle_alloc_error};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};

use crate::align::{self, CACHE_LINE};

pub struct Slice<T, const ALIGN: usize = CACHE_LINE> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send, const ALIGN: usize> Send for Slice<T, ALIGN> {}
unsafe impl<T: Sync, const ALIGN: usize> Sync for Slice<T, ALIGN> {}

impl<T, const ALIGN: usize> Slice<T, ALIGN> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            cap: 0,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_elem(len: usize, value: T) -> Self
    where
        T: Clone,
    {
        if len == 0 {
            return Self::new();
        }

        let ptr = allocate::<T, ALIGN>(len);
        let mut guard = InitGuard::<T, ALIGN> {
            ptr,
            initialized: 0,
            cap: len,
        };
        for index in 0..len {
            // SAFETY: `ptr` points to `len` writable slots and `index < len`.
            unsafe { ptr.as_ptr().add(index).write(value.clone()) };
            guard.initialized += 1;
        }
        mem::forget(guard);

        Self {
            ptr,
            len,
            cap: len,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_fn(mut len: usize, mut f: impl FnMut(usize) -> T) -> Self {
        if len == 0 {
            return Self::new();
        }

        let cap = len;
        let ptr = allocate::<T, ALIGN>(cap);
        let mut guard = InitGuard::<T, ALIGN> {
            ptr,
            initialized: 0,
            cap,
        };
        for index in 0..len {
            // SAFETY: `ptr` points to `cap` writable slots and `index < cap`.
            unsafe { ptr.as_ptr().add(index).write(f(index)) };
            guard.initialized += 1;
        }
        len = guard.initialized;
        mem::forget(guard);

        Self {
            ptr,
            len,
            cap,
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
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[T] {
        // SAFETY: `ptr` points to `len` initialized contiguous elements, or is
        // a valid dangling pointer when `len == 0`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: Same invariant as `as_slice`, with unique access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    #[inline(always)]
    pub fn alignment(&self) -> usize {
        align::allocation_align::<T, ALIGN>()
    }

    #[inline]
    pub(crate) unsafe fn from_raw_parts(ptr: NonNull<T>, len: usize, cap: usize) -> Self {
        Self {
            ptr,
            len,
            cap,
            _marker: PhantomData,
        }
    }
}

impl<T, const ALIGN: usize> Default for Slice<T, ALIGN> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone, const ALIGN: usize> Clone for Slice<T, ALIGN> {
    #[inline]
    fn clone(&self) -> Self {
        self.as_slice().iter().cloned().collect()
    }
}

impl<T: fmt::Debug, const ALIGN: usize> fmt::Debug for Slice<T, ALIGN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl<T, const ALIGN: usize> Drop for Slice<T, ALIGN> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len));
            deallocate::<T, ALIGN>(self.ptr, self.cap);
        }
    }
}

impl<T, const ALIGN: usize> Deref for Slice<T, ALIGN> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const ALIGN: usize> DerefMut for Slice<T, ALIGN> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T, const ALIGN: usize> FromIterator<T> for Slice<T, ALIGN> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut values = crate::vec::RawVec::<T, ALIGN>::new();
        values.extend(iter);
        values.into_boxed_slice()
    }
}

struct InitGuard<T, const ALIGN: usize> {
    ptr: NonNull<T>,
    initialized: usize,
    cap: usize,
}

impl<T, const ALIGN: usize> Drop for InitGuard<T, ALIGN> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(std::slice::from_raw_parts_mut(
                self.ptr.as_ptr(),
                self.initialized,
            ));
            deallocate::<T, ALIGN>(self.ptr, self.cap);
        }
    }
}

#[inline]
pub(crate) fn allocate<T, const ALIGN: usize>(capacity: usize) -> NonNull<T> {
    if capacity == 0 {
        return NonNull::dangling();
    }
    let layout = align::array_layout::<T, ALIGN>(capacity);
    // SAFETY: `layout` is valid and non-zero-sized by construction.
    let ptr = unsafe { alloc(layout) };
    match NonNull::new(ptr.cast::<T>()) {
        Some(ptr) => ptr,
        None => handle_alloc_error(layout),
    }
}

#[inline]
pub(crate) unsafe fn deallocate<T, const ALIGN: usize>(ptr: NonNull<T>, capacity: usize) {
    if capacity == 0 {
        return;
    }
    let layout = align::array_layout::<T, ALIGN>(capacity);
    // SAFETY: Callers pass the same pointer/capacity pair returned by
    // `allocate`.
    unsafe { dealloc(ptr.as_ptr().cast::<u8>(), layout) };
}

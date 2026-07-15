use std::alloc::{GlobalAlloc, handle_alloc_error};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::align::{self, CACHE_LINE};
use crate::heap::Heap;

pub(crate) struct Slice<T, const ALIGN: usize = CACHE_LINE> {
    ptr: NonNull<T>,
    len: usize,
    /// `None` selects the main heap. `Some` retains an explicit non-main heap.
    heap: Option<Arc<Heap>>,
    marker: PhantomData<T>,
}

// SAFETY: Slice owns its allocation and only exposes shared access through
// `&self`; moving it between threads is sound when its elements are Send.
unsafe impl<T: Send, const ALIGN: usize> Send for Slice<T, ALIGN> {}
// SAFETY: shared access is sound when its elements are Sync.
unsafe impl<T: Sync, const ALIGN: usize> Sync for Slice<T, ALIGN> {}

impl<T, const ALIGN: usize> Slice<T, ALIGN> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
            heap: None,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_elem(len: usize, value: T) -> Self
    where
        T: Clone,
    {
        Self::from_fn(len, |_| value.clone())
    }

    /// Allocates `len` slots of `T` from `heap` and initializes every slot to
    /// `value.clone()`. Explicit non-main heaps are retained as provenance;
    /// the main heap is normalized to the default representation.
    #[inline]
    pub fn from_elem_in(len: usize, value: T, heap: Arc<Heap>) -> Self
    where
        T: Clone,
    {
        Self::from_fn_in(len, |_| value.clone(), heap)
    }

    #[inline]
    pub fn from_fn(len: usize, f: impl FnMut(usize) -> T) -> Self {
        Self::from_fn_with_heap(len, f, None)
    }

    #[inline]
    pub(crate) fn from_fn_in(len: usize, f: impl FnMut(usize) -> T, heap: Arc<Heap>) -> Self {
        let heap = (!heap.is_main_heap()).then_some(heap);
        Self::from_fn_with_heap(len, f, heap)
    }

    fn from_fn_with_heap(
        len: usize,
        mut f: impl FnMut(usize) -> T,
        heap: Option<Arc<Heap>>,
    ) -> Self {
        if len == 0 {
            return Self {
                ptr: NonNull::dangling(),
                len: 0,
                heap,
                marker: PhantomData,
            };
        }

        let main_heap = Heap::main();
        let allocator = heap.as_deref().unwrap_or(&main_heap);
        let ptr = allocate_in::<T, ALIGN>(len, allocator);
        let mut guard = SliceInitGuard::<T, ALIGN> {
            ptr,
            initialized: 0,
            capacity: len,
            heap: allocator,
        };
        for index in 0..len {
            // SAFETY: `index < len`; each slot is written exactly once.
            unsafe { ptr.as_ptr().add(index).write(f(index)) };
            guard.initialized += 1;
        }
        mem::forget(guard);
        Self {
            ptr,
            len,
            heap,
            marker: PhantomData,
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
        // SAFETY: `ptr` owns `len` initialized contiguous elements, or is a
        // valid dangling pointer when `len == 0`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        // SAFETY: the same invariant as `as_slice`, with unique access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
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
        let heap = self.heap.clone();
        Self::from_fn_with_heap(self.len, |index| self[index].clone(), heap)
    }
}

impl<T: fmt::Debug, const ALIGN: usize> fmt::Debug for Slice<T, ALIGN> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(formatter)
    }
}

impl<T, const ALIGN: usize> Drop for Slice<T, ALIGN> {
    fn drop(&mut self) {
        if self.len == 0 {
            return;
        }
        let main_heap = Heap::main();
        let heap = self.heap.as_deref().unwrap_or(&main_heap);
        let len = mem::take(&mut self.len);
        // SAFETY: Slice owns all `len` initialized elements and the allocation
        // was created by this same heap with the matching layout.
        unsafe {
            ptr::drop_in_place(std::slice::from_raw_parts_mut(self.ptr.as_ptr(), len));
            deallocate_in::<T, ALIGN>(self.ptr, len, heap);
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

struct SliceInitGuard<'a, T, const ALIGN: usize> {
    ptr: NonNull<T>,
    initialized: usize,
    capacity: usize,
    heap: &'a Heap,
}

impl<T, const ALIGN: usize> Drop for SliceInitGuard<'_, T, ALIGN> {
    fn drop(&mut self) {
        // SAFETY: only the initialized prefix contains valid elements, and the
        // entire allocation was created from `heap` for `capacity` elements.
        unsafe {
            ptr::drop_in_place(std::slice::from_raw_parts_mut(
                self.ptr.as_ptr(),
                self.initialized,
            ));
            deallocate_in::<T, ALIGN>(self.ptr, self.capacity, self.heap);
        }
    }
}

#[inline]
pub(crate) fn allocate_in<T, const ALIGN: usize>(capacity: usize, heap: &Heap) -> NonNull<T> {
    if capacity == 0 {
        return NonNull::dangling();
    }
    let layout = align::array_layout::<T, ALIGN>(capacity);
    heap.alloc(layout)
        .unwrap_or_else(|| handle_alloc_error(layout))
        .cast::<T>()
}

#[inline]
pub(crate) unsafe fn deallocate_in<T, const ALIGN: usize>(
    ptr: NonNull<T>,
    capacity: usize,
    heap: &Heap,
) {
    if capacity == 0 {
        return;
    }
    let layout = align::array_layout::<T, ALIGN>(capacity);
    // SAFETY: callers pass the same heap, capacity, element type, and alignment
    // used by `allocate_in`.
    unsafe { GlobalAlloc::dealloc(heap, ptr.as_ptr().cast::<u8>(), layout) };
}

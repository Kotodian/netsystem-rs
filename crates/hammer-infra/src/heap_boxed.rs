use std::alloc::handle_alloc_error;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};

use crate::align;
use crate::mem::{MemHeap, MemMain};

pub(crate) struct Slice<T, const ALIGN: usize = 0> {
    ptr: NonNull<T>,
    len: usize,
    heap: *mut MemHeap,
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
            heap: ptr::null_mut(),
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_fn(len: usize, f: impl FnMut(usize) -> T) -> Self {
        Self::from_fn_with_heap(len, f, ptr::null_mut())
    }

    #[inline]
    pub(crate) fn from_elem_in(len: usize, value: T, heap: &MemHeap) -> Self
    where
        T: Clone,
    {
        Self::from_fn_in(len, |_| value.clone(), heap)
    }

    #[inline]
    pub(crate) fn from_fn_in(len: usize, f: impl FnMut(usize) -> T, heap: &MemHeap) -> Self {
        let heap = if ptr::eq(heap, MemMain::main_heap()) {
            ptr::null_mut()
        } else {
            (heap as *const MemHeap).cast_mut()
        };
        Self::from_fn_with_heap(len, f, heap)
    }

    fn from_fn_with_heap(len: usize, f: impl FnMut(usize) -> T, heap: *mut MemHeap) -> Self {
        let mut f = f;
        if len == 0 {
            return Self {
                ptr: NonNull::dangling(),
                len: 0,
                heap,
                marker: PhantomData,
            };
        }

        let heap_reference = if heap.is_null() {
            MemMain::main_heap()
        } else {
            unsafe { &*heap }
        };
        let ptr = allocate_in::<T, ALIGN>(len, heap_reference);
        let mut guard = SliceInitGuard::<T, ALIGN> {
            ptr,
            initialized: 0,
            capacity: len,
            heap: heap_reference,
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
        Self::from_fn_with_heap(self.len, |index| self[index].clone(), self.heap)
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
        let heap = if self.heap.is_null() {
            MemMain::main_heap()
        } else {
            unsafe { &*self.heap }
        };
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
    heap: &'a MemHeap,
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

/// Allocates an uninitialized contiguous array from the process Main Heap.
///
/// `ALIGN` controls the allocation base, not the element stride; zero uses
/// the infrastructure vector alignment. The returned pointer owns no Rust
/// values until the caller initializes them. A zero capacity does not allocate.
/// Allocation failure uses the process allocation-error policy.
#[inline]
pub fn allocate<T, const ALIGN: usize>(capacity: usize) -> NonNull<T> {
    allocate_in::<T, ALIGN>(capacity, MemMain::main_heap())
}

#[inline]
pub(crate) fn allocate_in<T, const ALIGN: usize>(capacity: usize, heap: &MemHeap) -> NonNull<T> {
    if capacity == 0 {
        return NonNull::dangling();
    }
    let layout = align::array_layout::<T, ALIGN>(capacity);
    heap.allocate(layout)
        .unwrap_or_else(|| handle_alloc_error(layout))
        .cast::<T>()
}

/// Frees array storage without dropping its elements.
///
/// # Safety
/// For nonzero `capacity`, `ptr` must be a live allocation returned by
/// `allocate::<T, ALIGN>` with the same capacity. All element
/// references must have ended, and the caller must drop any initialized
/// elements that require destruction before releasing their storage.
#[inline]
pub unsafe fn deallocate<T, const ALIGN: usize>(ptr: NonNull<T>, capacity: usize) {
    // SAFETY: the public contract fixes provenance to the process Main Heap.
    unsafe { deallocate_in::<T, ALIGN>(ptr, capacity, MemMain::main_heap()) };
}

#[inline]
pub(crate) unsafe fn deallocate_in<T, const ALIGN: usize>(
    ptr: NonNull<T>,
    capacity: usize,
    heap: &MemHeap,
) {
    if capacity == 0 {
        return;
    }
    let layout = align::array_layout::<T, ALIGN>(capacity);
    let pointer = unsafe { NonNull::new_unchecked(ptr.as_ptr().cast::<u8>()) };
    // SAFETY: callers pass the same heap, capacity, element type, and alignment
    // used by `allocate_in`.
    unsafe { heap.deallocate(pointer, layout) };
}

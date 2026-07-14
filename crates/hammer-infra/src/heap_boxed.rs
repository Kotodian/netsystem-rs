use std::alloc::{GlobalAlloc, handle_alloc_error};
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::align::{self, CACHE_LINE};
use crate::heap::Heap;
use crate::heap_vec::RawVec;

pub(crate) struct Slice<T, const ALIGN: usize = CACHE_LINE> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
    /// `None` selects the main heap. `Some` retains an explicit non-main heap.
    heap: Option<Arc<Heap>>,
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
            heap: None,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_elem(len: usize, value: T) -> Self
    where
        T: Clone,
    {
        let mut values = RawVec::<T, ALIGN>::with_capacity(len);
        for _ in 0..len {
            values.push(value.clone());
        }
        values.into_boxed_slice()
    }

    /// Allocates `len` slots of `T` from `heap` and initializes every slot to
    /// `value.clone()`. Explicit non-main heaps are retained as provenance;
    /// the main heap is normalized to the default representation.
    #[inline]
    pub fn from_elem_in(len: usize, value: T, heap: Arc<Heap>) -> Self
    where
        T: Clone,
    {
        let mut values = RawVec::<T, ALIGN>::with_capacity_in(len, heap);
        for _ in 0..len {
            values.push(value.clone());
        }
        values.into_boxed_slice()
    }

    #[inline]
    pub fn from_fn(len: usize, f: impl FnMut(usize) -> T) -> Self {
        let mut values = RawVec::<T, ALIGN>::with_capacity(len);
        values.extend((0..len).map(f));
        values.into_boxed_slice()
    }

    #[inline]
    pub(crate) fn from_fn_in(len: usize, f: impl FnMut(usize) -> T, heap: Arc<Heap>) -> Self {
        let mut values = RawVec::<T, ALIGN>::with_capacity_in(len, heap);
        values.extend((0..len).map(f));
        values.into_boxed_slice()
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

    /// # Safety
    /// `ptr` must have been allocated for `cap` slots of `T` by the main heap
    /// when `heap` is `None`, or by the retained non-main heap when it is
    /// `Some`.
    #[inline]
    pub(crate) unsafe fn from_raw_parts(
        ptr: NonNull<T>,
        len: usize,
        cap: usize,
        heap: Option<Arc<Heap>>,
    ) -> Self {
        Self {
            ptr,
            len,
            cap,
            heap,
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
        let mut values = match self.heap.as_ref() {
            Some(heap) => RawVec::<T, ALIGN>::with_capacity_in(self.cap, heap.clone()),
            None => RawVec::<T, ALIGN>::with_capacity(self.cap),
        };
        values.extend_from_cloned_slice(self.as_slice());
        values.into_boxed_slice()
    }
}

impl<T: fmt::Debug, const ALIGN: usize> fmt::Debug for Slice<T, ALIGN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl<T, const ALIGN: usize> Drop for Slice<T, ALIGN> {
    fn drop(&mut self) {
        if self.cap == 0 {
            return;
        }
        let main_heap = Heap::main();
        let heap = self.heap.as_deref().unwrap_or(&main_heap);
        let len = self.len;
        self.len = 0;
        unsafe {
            ptr::drop_in_place(std::slice::from_raw_parts_mut(self.ptr.as_ptr(), len));
            deallocate_in::<T, ALIGN>(self.ptr, self.cap, heap);
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
        let mut values = crate::heap_vec::RawVec::<T, ALIGN>::new();
        values.extend(iter);
        values.into_boxed_slice()
    }
}

#[inline]
pub(crate) fn allocate_in<T, const ALIGN: usize>(capacity: usize, heap: &Heap) -> NonNull<T> {
    if capacity == 0 {
        return NonNull::dangling();
    }
    let layout = align::array_layout::<T, ALIGN>(capacity);
    let ptr = heap
        .alloc(layout)
        .unwrap_or_else(|| handle_alloc_error(layout));
    NonNull::new(ptr.as_ptr().cast::<T>()).expect("Heap::alloc returned null")
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
    let raw = ptr.as_ptr().cast::<u8>();
    unsafe { GlobalAlloc::dealloc(heap, raw, layout) };
}

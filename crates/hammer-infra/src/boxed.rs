use std::alloc::{GlobalAlloc, handle_alloc_error};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::align::{self, CACHE_LINE};
use crate::heap::Heap;

pub struct Slice<T, const ALIGN: usize = CACHE_LINE> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
    /// Heap that allocated the backing storage. Retained so `Drop` can
    /// route the dealloc through the same `Heap`. `None` is only valid
    /// when `cap == 0`; the allocating constructors always populate it.
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
        Self::from_elem_in(len, value, Arc::new(Heap::local()))
    }

    /// Allocates `len` slots of `T` from the provided `heap` and
    /// initialises every slot to `value.clone()`. The `heap` is retained
    /// so `Drop` deallocs through the same `Heap` (SVM region or local).
    #[inline]
    pub fn from_elem_in(len: usize, value: T, heap: Arc<Heap>) -> Self
    where
        T: Clone,
    {
        if len == 0 {
            return Self::new();
        }

        let ptr = allocate_in::<T, ALIGN>(len, &heap);
        let mut guard = InitGuard::<T, ALIGN> {
            ptr,
            initialized: 0,
            cap: len,
            heap: heap.clone(),
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
            heap: Some(heap),
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_fn(len: usize, f: impl FnMut(usize) -> T) -> Self {
        Self::from_fn_in(len, f, Arc::new(Heap::local()))
    }

    #[inline]
    pub(crate) fn from_fn_in(
        mut len: usize,
        mut f: impl FnMut(usize) -> T,
        heap: Arc<Heap>,
    ) -> Self {
        if len == 0 {
            return Self::new();
        }

        let cap = len;
        let ptr = allocate_in::<T, ALIGN>(cap, &heap);
        let mut guard = InitGuard::<T, ALIGN> {
            ptr,
            initialized: 0,
            cap,
            heap: heap.clone(),
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
            heap: Some(heap),
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

    /// # Safety
    /// `ptr` must have been returned by `heap.alloc` (or `allocate_in`)
    /// with the layout matching `cap` slots of `T`; `heap` must be the
    /// same `Heap` that produced `ptr`.
    #[inline]
    pub(crate) unsafe fn from_raw_parts(
        ptr: NonNull<T>,
        len: usize,
        cap: usize,
        heap: Arc<Heap>,
    ) -> Self {
        Self {
            ptr,
            len,
            cap,
            heap: Some(heap),
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
        if self.len == 0 {
            return Self::new();
        }

        let heap = self
            .heap
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::new(Heap::local()));
        let ptr = allocate_in::<T, ALIGN>(self.cap, &heap);
        let mut guard = InitGuard::<T, ALIGN> {
            ptr,
            initialized: 0,
            cap: self.cap,
            heap: heap.clone(),
        };

        for (index, value) in self.as_slice().iter().enumerate() {
            unsafe { ptr.as_ptr().add(index).write(value.clone()) };
            guard.initialized += 1;
        }
        mem::forget(guard);

        Self {
            ptr,
            len: self.len,
            cap: self.cap,
            heap: Some(heap),
            _marker: PhantomData,
        }
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
        let Some(heap) = self.heap.as_ref() else {
            return;
        };
        unsafe {
            ptr::drop_in_place(std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len));
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
        let mut values = crate::vec::RawVec::<T, ALIGN>::new();
        values.extend(iter);
        values.into_boxed_slice()
    }
}

struct InitGuard<T, const ALIGN: usize> {
    ptr: NonNull<T>,
    initialized: usize,
    cap: usize,
    heap: Arc<Heap>,
}

impl<T, const ALIGN: usize> Drop for InitGuard<T, ALIGN> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(std::slice::from_raw_parts_mut(
                self.ptr.as_ptr(),
                self.initialized,
            ));
            deallocate_in::<T, ALIGN>(self.ptr, self.cap, &self.heap);
        }
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

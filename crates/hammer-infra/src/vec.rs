use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::ptr::{self, NonNull};

use crate::align::{self, CACHE_LINE};
use crate::boxed::{self, Slice};
use crate::prefetch::prefetch_read_l1;

pub type Vec<T> = RawVec<T, CACHE_LINE>;
pub type Drain<'a, T> = RawDrain<'a, T, CACHE_LINE>;
pub type IntoIter<T> = RawIntoIter<T, CACHE_LINE>;

const COPY_PREFETCH_LINES: usize = 4;

pub struct RawVec<T, const ALIGN: usize = CACHE_LINE> {
    ptr: NonNull<T>,
    len: usize,
    cap: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send, const ALIGN: usize> Send for RawVec<T, ALIGN> {}
unsafe impl<T: Sync, const ALIGN: usize> Sync for RawVec<T, ALIGN> {}

impl<T, const ALIGN: usize> RawVec<T, ALIGN> {
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
    pub fn with_capacity(capacity: usize) -> Self {
        if capacity == 0 {
            return Self::new();
        }
        Self {
            ptr: boxed::allocate::<T, ALIGN>(capacity),
            len: 0,
            cap: capacity,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn from_elem_copy(len: usize, value: T) -> Self
    where
        T: Copy,
    {
        let mut out = Self::with_capacity(len);
        for _ in 0..len {
            out.push(value);
        }
        out
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
        self.cap
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
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    #[inline(always)]
    pub fn alignment(&self) -> usize {
        align::allocation_align::<T, ALIGN>()
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        let required = self
            .len
            .checked_add(additional)
            .expect("aligned vec capacity overflow");
        if required <= self.cap {
            return;
        }
        let doubled = self.cap.max(1).saturating_mul(2);
        self.grow_to(required.max(doubled));
    }

    #[inline(always)]
    pub fn push(&mut self, value: T) {
        if self.len == self.cap {
            self.grow_for_push();
        }
        unsafe { self.ptr.as_ptr().add(self.len).write(value) };
        self.len += 1;
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }
        self.len -= 1;
        Some(unsafe { self.ptr.as_ptr().add(self.len).read() })
    }

    #[inline]
    pub fn remove(&mut self, index: usize) -> T {
        assert!(index < self.len, "remove index out of bounds");
        unsafe {
            let ptr = self.ptr.as_ptr().add(index);
            let value = ptr.read();
            ptr::copy(ptr.add(1), ptr, self.len - index - 1);
            self.len -= 1;
            value
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        unsafe {
            ptr::drop_in_place(std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len));
        }
        self.len = 0;
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        if len >= self.len {
            return;
        }
        unsafe {
            let tail = self.ptr.as_ptr().add(len);
            ptr::drop_in_place(std::slice::from_raw_parts_mut(tail, self.len - len));
        }
        self.len = len;
    }

    #[inline]
    pub fn extend_from_slice(&mut self, other: &[T])
    where
        T: Copy,
    {
        self.extend_from_copy_slice(other);
    }

    #[inline]
    pub fn extend_from_cloned_slice(&mut self, other: &[T])
    where
        T: Clone,
    {
        self.reserve(other.len());
        let start = self.len;
        for (offset, value) in other.iter().enumerate() {
            unsafe { self.ptr.as_ptr().add(start + offset).write(value.clone()) };
            self.len += 1;
        }
    }

    #[inline]
    pub fn extend_from_copy_slice(&mut self, other: &[T])
    where
        T: Copy,
    {
        let count = other.len();
        if count == 0 {
            return;
        }
        self.reserve(count);
        prefetch_slice_prefix(other);
        unsafe {
            ptr::copy_nonoverlapping(other.as_ptr(), self.ptr.as_ptr().add(self.len), count);
        }
        self.len += count;
    }

    #[inline]
    pub fn drain<R>(&mut self, range: R) -> RawDrain<'_, T, ALIGN>
    where
        R: RangeBounds<usize>,
    {
        let len = self.len;
        let start = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start.checked_add(1).expect("drain range start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&end) => end.checked_add(1).expect("drain range end overflow"),
            Bound::Excluded(&end) => end,
            Bound::Unbounded => len,
        };
        assert!(start <= end, "drain range start exceeds end");
        assert!(end <= len, "drain range end exceeds length");

        self.len = start;
        RawDrain {
            vec: self as *mut RawVec<T, ALIGN>,
            current: start,
            end,
            tail_start: end,
            tail_len: len - end,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub fn into_boxed_slice(mut self) -> Slice<T, ALIGN> {
        let ptr = self.ptr;
        let len = self.len;
        let cap = self.cap;
        self.ptr = NonNull::dangling();
        self.len = 0;
        self.cap = 0;
        mem::forget(self);
        unsafe { Slice::from_raw_parts(ptr, len, cap) }
    }

    #[inline]
    fn grow_to(&mut self, next_capacity: usize) {
        let next_ptr = boxed::allocate::<T, ALIGN>(next_capacity);
        for index in 0..self.len {
            unsafe {
                next_ptr
                    .as_ptr()
                    .add(index)
                    .write(self.ptr.as_ptr().add(index).read());
            }
        }
        unsafe { boxed::deallocate::<T, ALIGN>(self.ptr, self.cap) };
        self.ptr = next_ptr;
        self.cap = next_capacity;
    }

    #[cold]
    #[inline(never)]
    fn grow_for_push(&mut self) {
        self.reserve(1);
    }
}

#[inline]
fn prefetch_slice_prefix<T>(slice: &[T]) {
    let byte_len = mem::size_of_val(slice);
    if byte_len == 0 {
        return;
    }
    let lines = byte_len.div_ceil(CACHE_LINE).min(COPY_PREFETCH_LINES);
    let ptr = slice.as_ptr().cast::<u8>();
    for line in 0..lines {
        unsafe { prefetch_read_l1(ptr.add(line * CACHE_LINE)) };
    }
}

pub struct RawDrain<'a, T, const ALIGN: usize = CACHE_LINE> {
    vec: *mut RawVec<T, ALIGN>,
    current: usize,
    end: usize,
    tail_start: usize,
    tail_len: usize,
    _marker: PhantomData<&'a mut RawVec<T, ALIGN>>,
}

impl<T, const ALIGN: usize> Iterator for RawDrain<'_, T, ALIGN> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            return None;
        }
        let index = self.current;
        self.current += 1;
        Some(unsafe { (*self.vec).ptr.as_ptr().add(index).read() })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.end - self.current;
        (len, Some(len))
    }
}

impl<T, const ALIGN: usize> ExactSizeIterator for RawDrain<'_, T, ALIGN> {}

impl<T, const ALIGN: usize> Drop for RawDrain<'_, T, ALIGN> {
    fn drop(&mut self) {
        unsafe {
            let vec = &mut *self.vec;
            while self.current < self.end {
                ptr::drop_in_place(vec.ptr.as_ptr().add(self.current));
                self.current += 1;
            }
            if self.tail_len != 0 {
                ptr::copy(
                    vec.ptr.as_ptr().add(self.tail_start),
                    vec.ptr.as_ptr().add(vec.len),
                    self.tail_len,
                );
            }
            vec.len += self.tail_len;
        }
    }
}

pub struct RawIntoIter<T, const ALIGN: usize = CACHE_LINE> {
    ptr: NonNull<T>,
    cap: usize,
    current: usize,
    end: usize,
}

impl<T, const ALIGN: usize> Iterator for RawIntoIter<T, ALIGN> {
    type Item = T;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.end {
            return None;
        }
        let index = self.current;
        self.current += 1;
        Some(unsafe { self.ptr.as_ptr().add(index).read() })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.end - self.current;
        (len, Some(len))
    }
}

impl<T, const ALIGN: usize> ExactSizeIterator for RawIntoIter<T, ALIGN> {}

impl<T, const ALIGN: usize> Drop for RawIntoIter<T, ALIGN> {
    fn drop(&mut self) {
        unsafe {
            while self.current < self.end {
                ptr::drop_in_place(self.ptr.as_ptr().add(self.current));
                self.current += 1;
            }
            boxed::deallocate::<T, ALIGN>(self.ptr, self.cap);
        }
    }
}

impl<T, const ALIGN: usize> Default for RawVec<T, ALIGN> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone, const ALIGN: usize> Clone for RawVec<T, ALIGN> {
    #[inline]
    fn clone(&self) -> Self {
        self.as_slice().iter().cloned().collect()
    }
}

impl<T: fmt::Debug, const ALIGN: usize> fmt::Debug for RawVec<T, ALIGN> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl<T: PartialEq, const ALIGN: usize> PartialEq for RawVec<T, ALIGN> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq, const ALIGN: usize> Eq for RawVec<T, ALIGN> {}

impl<T, U, const ALIGN: usize> PartialEq<&[U]> for RawVec<T, ALIGN>
where
    T: PartialEq<U>,
{
    #[inline]
    fn eq(&self, other: &&[U]) -> bool {
        self.as_slice() == *other
    }
}

impl<T, U, const ALIGN: usize> PartialEq<[U]> for RawVec<T, ALIGN>
where
    T: PartialEq<U>,
{
    #[inline]
    fn eq(&self, other: &[U]) -> bool {
        self.as_slice() == other
    }
}

impl<T, U, const ALIGN: usize> PartialEq<std::vec::Vec<U>> for RawVec<T, ALIGN>
where
    T: PartialEq<U>,
{
    #[inline]
    fn eq(&self, other: &std::vec::Vec<U>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T, U, const ALIGN: usize, const N: usize> PartialEq<&[U; N]> for RawVec<T, ALIGN>
where
    T: PartialEq<U>,
{
    #[inline]
    fn eq(&self, other: &&[U; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T, U, const ALIGN: usize, const N: usize> PartialEq<[U; N]> for RawVec<T, ALIGN>
where
    T: PartialEq<U>,
{
    #[inline]
    fn eq(&self, other: &[U; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T, const ALIGN: usize> Drop for RawVec<T, ALIGN> {
    fn drop(&mut self) {
        self.clear();
        unsafe { boxed::deallocate::<T, ALIGN>(self.ptr, self.cap) };
    }
}

impl<T, const ALIGN: usize> Deref for RawVec<T, ALIGN> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<T, const ALIGN: usize> DerefMut for RawVec<T, ALIGN> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<T, const ALIGN: usize> Extend<T> for RawVec<T, ALIGN> {
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        self.reserve(lower);
        for value in iter {
            self.push(value);
        }
    }
}

impl<T, const ALIGN: usize> FromIterator<T> for RawVec<T, ALIGN> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut values = Self::new();
        values.extend(iter);
        values
    }
}

impl<T, const ALIGN: usize> From<std::vec::Vec<T>> for RawVec<T, ALIGN> {
    #[inline]
    fn from(values: std::vec::Vec<T>) -> Self {
        values.into_iter().collect()
    }
}

impl<T, const ALIGN: usize> IntoIterator for RawVec<T, ALIGN> {
    type Item = T;
    type IntoIter = RawIntoIter<T, ALIGN>;

    #[inline]
    fn into_iter(mut self) -> Self::IntoIter {
        let iter = RawIntoIter {
            ptr: self.ptr,
            cap: self.cap,
            current: 0,
            end: self.len,
        };
        self.ptr = NonNull::dangling();
        self.len = 0;
        self.cap = 0;
        mem::forget(self);
        iter
    }
}

impl<'a, T, const ALIGN: usize> IntoIterator for &'a RawVec<T, ALIGN> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl<'a, T, const ALIGN: usize> IntoIterator for &'a mut RawVec<T, ALIGN> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_mut_slice().iter_mut()
    }
}

//! Public Rust-shaped `Vec<T>` backed by Hammer's main mimalloc heap.

use std::fmt;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut, RangeBounds};

use allocator_api2::vec::{self as api_vec, Vec as ApiVec};

use crate::boxed::Box;
use crate::main_alloc::MainAllocator;

pub type Drain<'a, T> = api_vec::Drain<'a, T, MainAllocator>;
pub type IntoIter<T> = api_vec::IntoIter<T, MainAllocator>;
pub type Splice<'a, I> = api_vec::Splice<'a, I, MainAllocator>;

/// Three-word concrete vector facade (`ptr`, `len`, `cap`).
#[repr(transparent)]
pub struct Vec<T> {
    inner: ApiVec<T, MainAllocator>,
}

impl<T> Vec<T> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            inner: ApiVec::new_in(MainAllocator),
        }
    }

    #[inline]
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: ApiVec::with_capacity_in(capacity, MainAllocator),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional);
    }

    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        self.inner.reserve_exact(additional);
    }

    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.inner.shrink_to_fit();
    }

    #[inline]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.inner.shrink_to(min_capacity);
    }

    #[inline]
    pub fn truncate(&mut self, len: usize) {
        self.inner.truncate(len);
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        self.inner.as_slice()
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.inner.as_mut_slice()
    }

    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.inner.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.inner.as_mut_ptr()
    }

    #[inline]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        unsafe { self.inner.set_len(new_len) };
    }

    #[inline]
    pub fn swap_remove(&mut self, index: usize) -> T {
        self.inner.swap_remove(index)
    }

    #[inline]
    pub fn insert(&mut self, index: usize, element: T) {
        self.inner.insert(index, element);
    }

    #[inline]
    pub fn remove(&mut self, index: usize) -> T {
        self.inner.remove(index)
    }

    #[inline]
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&T) -> bool,
    {
        self.inner.retain(f);
    }

    #[inline]
    pub fn retain_mut<F>(&mut self, f: F)
    where
        F: FnMut(&mut T) -> bool,
    {
        self.inner.retain_mut(f);
    }

    #[inline]
    pub fn dedup_by_key<F, K>(&mut self, key: F)
    where
        F: FnMut(&mut T) -> K,
        K: PartialEq,
    {
        self.inner.dedup_by_key(key);
    }

    #[inline]
    pub fn dedup_by<F>(&mut self, same_bucket: F)
    where
        F: FnMut(&mut T, &mut T) -> bool,
    {
        self.inner.dedup_by(same_bucket);
    }

    #[inline]
    pub fn push(&mut self, value: T) {
        self.inner.push(value);
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop()
    }

    #[inline]
    pub fn append(&mut self, other: &mut Self) {
        self.inner.append(&mut other.inner);
    }

    #[inline]
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn split_off(&mut self, at: usize) -> Self {
        Self {
            inner: self.inner.split_off(at),
        }
    }

    #[inline]
    pub fn resize_with<F>(&mut self, new_len: usize, f: F)
    where
        F: FnMut() -> T,
    {
        self.inner.resize_with(new_len, f);
    }

    #[inline]
    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<T>] {
        self.inner.spare_capacity_mut()
    }

    #[inline]
    pub fn into_boxed_slice(self) -> Box<[T]> {
        Box::from_api(self.inner.into_boxed_slice())
    }

    #[inline]
    pub fn drain<R>(&mut self, range: R) -> Drain<'_, T>
    where
        R: RangeBounds<usize>,
    {
        self.inner.drain(range)
    }

    #[inline]
    pub fn into_iter(self) -> IntoIter<T> {
        self.inner.into_iter()
    }
}

impl<T: Clone> Vec<T> {
    #[inline]
    pub fn resize(&mut self, new_len: usize, value: T) {
        self.inner.resize(new_len, value);
    }

    #[inline]
    pub fn extend_from_slice(&mut self, other: &[T]) {
        self.inner.extend_from_slice(other);
    }

    #[inline]
    pub fn extend_from_copy_slice(&mut self, other: &[T])
    where
        T: Copy,
    {
        self.inner.extend_from_slice(other);
    }
}

impl<T: Copy> Vec<T> {
    #[inline]
    pub fn from_elem_copy(len: usize, value: T) -> Self {
        let mut values = Self::with_capacity(len);
        values.resize(len, value);
        values
    }
}

impl<T: PartialEq> Vec<T> {
    #[inline]
    pub fn dedup(&mut self) {
        self.inner.dedup();
    }
}

impl<T> Default for Vec<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> Clone for Vec<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Vec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T> Deref for Vec<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &[T] {
        self.inner.as_slice()
    }
}

impl<T> DerefMut for Vec<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [T] {
        self.inner.as_mut_slice()
    }
}

impl<T> Extend<T> for Vec<T> {
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.inner.extend(iter);
    }
}

impl<T> FromIterator<T> for Vec<T> {
    #[inline]
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut out = Self::new();
        out.extend(iter);
        out
    }
}

impl<T, const N: usize> From<[T; N]> for Vec<T> {
    #[inline]
    fn from(array: [T; N]) -> Self {
        let mut out = Self::with_capacity(N);
        for value in array {
            out.push(value);
        }
        out
    }
}

impl<T> From<std::vec::Vec<T>> for Vec<T> {
    #[inline]
    fn from(values: std::vec::Vec<T>) -> Self {
        let mut out = Self::with_capacity(values.len());
        out.extend(values);
        out
    }
}

impl<T> From<Box<[T]>> for Vec<T> {
    #[inline]
    fn from(boxed: Box<[T]>) -> Self {
        Self {
            inner: ApiVec::from(boxed.into_api()),
        }
    }
}

impl<T: PartialEq> PartialEq for Vec<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: Eq> Eq for Vec<T> {}

impl<T: PartialEq> PartialEq<[T]> for Vec<T> {
    #[inline]
    fn eq(&self, other: &[T]) -> bool {
        self.as_slice() == other
    }
}

impl<T: PartialEq> PartialEq<&[T]> for Vec<T> {
    #[inline]
    fn eq(&self, other: &&[T]) -> bool {
        self.as_slice() == *other
    }
}

impl<T: PartialEq, const N: usize> PartialEq<[T; N]> for Vec<T> {
    #[inline]
    fn eq(&self, other: &[T; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: PartialEq, const N: usize> PartialEq<&[T; N]> for Vec<T> {
    #[inline]
    fn eq(&self, other: &&[T; N]) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T: PartialEq> PartialEq<std::vec::Vec<T>> for Vec<T> {
    #[inline]
    fn eq(&self, other: &std::vec::Vec<T>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<T> IntoIterator for Vec<T> {
    type Item = T;
    type IntoIter = IntoIter<T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a, T> IntoIterator for &'a Vec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}

impl<'a, T> IntoIterator for &'a mut Vec<T> {
    type Item = &'a mut T;
    type IntoIter = std::slice::IterMut<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.as_mut_slice().iter_mut()
    }
}

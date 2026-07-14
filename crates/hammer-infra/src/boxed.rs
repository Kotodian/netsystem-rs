//! Public Rust-shaped `Box<T>` / `Box<[T]>` backed by Hammer's main mimalloc heap.

use std::fmt;
use std::ops::{Deref, DerefMut};

use allocator_api2::boxed::Box as ApiBox;

use crate::main_alloc::MainAllocator;
use crate::vec::Vec;

/// Owned heap allocation on the main mimalloc heap.
#[repr(transparent)]
pub struct Box<T: ?Sized> {
    inner: ApiBox<T, MainAllocator>,
}

impl<T> Box<T> {
    #[inline]
    pub fn new(value: T) -> Self {
        Self {
            inner: ApiBox::new_in(value, MainAllocator),
        }
    }

    #[inline]
    pub fn into_inner(boxed: Self) -> T {
        ApiBox::into_inner(boxed.inner)
    }
}

impl<T: Clone> Box<[T]> {
    #[inline]
    pub fn from_elem(len: usize, value: T) -> Self {
        let mut values = Vec::with_capacity(len);
        values.resize(len, value);
        values.into_boxed_slice()
    }
}

impl<T> Box<[T]> {
    #[inline]
    pub fn from_fn(len: usize, mut f: impl FnMut(usize) -> T) -> Self {
        let mut values = Vec::with_capacity(len);
        for index in 0..len {
            values.push(f(index));
        }
        values.into_boxed_slice()
    }

    #[inline]
    pub(crate) fn from_api(inner: ApiBox<[T], MainAllocator>) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) fn into_api(self) -> ApiBox<[T], MainAllocator> {
        self.inner
    }

    #[inline]
    pub fn into_vec(self) -> Vec<T> {
        Vec::from(self)
    }
}

impl<T: ?Sized> Box<T> {
    #[inline]
    pub fn as_ptr(b: &Self) -> *const T {
        ApiBox::as_ptr(&b.inner)
    }

    #[inline]
    pub fn as_mut_ptr(b: &mut Self) -> *mut T {
        ApiBox::as_mut_ptr(&mut b.inner)
    }
}

impl<T: ?Sized> Deref for Box<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T: ?Sized> DerefMut for Box<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Box<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        T::fmt(&*self.inner, f)
    }
}

impl<T: Clone> Clone for Box<[T]> {
    fn clone(&self) -> Self {
        let mut values = Vec::with_capacity(self.len());
        values.extend_from_slice(self);
        values.into_boxed_slice()
    }
}

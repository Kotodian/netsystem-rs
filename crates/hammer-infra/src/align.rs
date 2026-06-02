use std::alloc::Layout;
use std::fmt;
use std::mem;
use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};

pub const CACHE_LINE: usize = 64;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Alignment(NonZeroUsize);

impl Alignment {
    #[inline]
    pub const fn new(value: usize) -> Option<Self> {
        if value != 0 && value.is_power_of_two() {
            // SAFETY: `value` was checked to be non-zero above.
            Some(Self(unsafe { NonZeroUsize::new_unchecked(value) }))
        } else {
            None
        }
    }

    #[inline(always)]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl fmt::Debug for Alignment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Alignment").field(&self.get()).finish()
    }
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CacheLine<T>(T);

impl<T> CacheLine<T> {
    #[inline(always)]
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    #[inline(always)]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> Deref for CacheLine<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CacheLine<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for CacheLine<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CacheLine").field(&self.0).finish()
    }
}

#[inline(always)]
pub const fn align_up(value: usize, alignment: usize) -> usize {
    assert!(alignment != 0, "alignment must be non-zero");
    assert!(
        alignment.is_power_of_two(),
        "alignment must be a power of two"
    );
    (value + alignment - 1) & !(alignment - 1)
}

#[inline(always)]
pub fn is_aligned<T>(ptr: *const T, alignment: usize) -> bool {
    assert!(alignment != 0, "alignment must be non-zero");
    assert!(
        alignment.is_power_of_two(),
        "alignment must be a power of two"
    );
    ptr.addr() & (alignment - 1) == 0
}

#[inline(always)]
pub(crate) fn allocation_align<T, const ALIGN: usize>() -> usize {
    assert!(ALIGN != 0, "alignment must be non-zero");
    assert!(ALIGN.is_power_of_two(), "alignment must be a power of two");
    ALIGN.max(mem::align_of::<T>())
}

#[inline]
pub(crate) fn array_layout<T, const ALIGN: usize>(capacity: usize) -> Layout {
    let alignment = allocation_align::<T, ALIGN>();
    let size = if mem::size_of::<T>() == 0 {
        1
    } else {
        mem::size_of::<T>()
            .checked_mul(capacity)
            .expect("aligned array allocation size overflow")
            .max(1)
    };
    Layout::from_size_align(size, alignment).expect("valid aligned array layout")
}

#[inline(always)]
pub(crate) fn slot_stride<T, const ALIGN: usize>() -> usize {
    align_up(mem::size_of::<T>().max(1), allocation_align::<T, ALIGN>())
}

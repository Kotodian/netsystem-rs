use std::alloc::Layout;
use std::mem;

pub const CACHE_LINE: usize = 64;

/// Zero-sized field marker corresponding to VPP's
/// `CLIB_CACHE_LINE_ALIGN_MARK(mark)`.
///
/// The marker owns no data. In a `repr(C)` record it aligns the record itself
/// or the field group that follows it to a cache-line boundary.
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct CacheLineAlignMark;

#[inline(always)]
pub const fn align_up(value: usize, alignment: usize) -> usize {
    assert!(alignment != 0, "alignment must be non-zero");
    assert!(
        alignment.is_power_of_two(),
        "alignment must be a power of two"
    );
    (value + alignment - 1) & !(alignment - 1)
}

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
    let requested = if ALIGN == 0 { 1 } else { ALIGN };
    assert!(
        requested.is_power_of_two(),
        "alignment must be a power of two"
    );
    requested.max(mem::align_of::<T>())
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

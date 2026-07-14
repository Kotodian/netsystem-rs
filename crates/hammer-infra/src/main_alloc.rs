//! Local main-heap allocator (mimalloc). ZST so public `Vec<T>` stays three words.

use std::alloc::{GlobalAlloc, Layout};
use std::ptr::NonNull;

use allocator_api2::alloc::{AllocError, Allocator};

/// VPP-shaped ordinary vector floor (`VEC_MIN_ALIGN` / clib `sizeof(void*)` class).
pub(crate) const VEC_MIN_ALIGN: usize = 8;

static MIMALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Hammer's process-local main heap backend.
///
/// Public collections default to this allocator. It must remain a ZST so
/// `Vec<T>` / `Box<T>` stay the same size as the Rust std shapes.
///
/// Ordinary allocations raise alignment to at least [`VEC_MIN_ALIGN`], matching
/// VPP's default vector rule. Call sites that need cache-line alignment must use
/// [`crate::vec::AlignedVec`] / [`crate::vec::CacheLineVec`] explicitly.
#[derive(Copy, Clone, Debug, Default)]
pub struct MainAllocator;

fn with_min_align(layout: Layout, min_align: usize) -> Result<Layout, AllocError> {
    debug_assert!(min_align.is_power_of_two() && min_align != 0);
    let align = layout.align().max(min_align);
    Layout::from_size_align(layout.size(), align).map_err(|_| AllocError)
}

fn allocate(layout: Layout, min_align: usize, zeroed: bool) -> Result<NonNull<[u8]>, AllocError> {
    let layout = with_min_align(layout, min_align)?;
    match layout.size() {
        0 => Ok(NonNull::slice_from_raw_parts(NonNull::dangling(), 0)),
        size => {
            // SAFETY: `layout` is valid and allocation failure is represented
            // by the null pointer handled below.
            let ptr = unsafe {
                if zeroed {
                    GlobalAlloc::alloc_zeroed(&MIMALLOC, layout)
                } else {
                    GlobalAlloc::alloc(&MIMALLOC, layout)
                }
            };
            let ptr = NonNull::new(ptr).ok_or(AllocError)?;
            Ok(NonNull::slice_from_raw_parts(ptr, size))
        }
    }
}

#[inline(never)]
pub(crate) fn main_allocate(layout: Layout, min_align: usize) -> Result<NonNull<[u8]>, AllocError> {
    allocate(layout, min_align, false)
}

#[inline(never)]
pub(crate) fn main_allocate_zeroed(
    layout: Layout,
    min_align: usize,
) -> Result<NonNull<[u8]>, AllocError> {
    allocate(layout, min_align, true)
}

#[inline(never)]
pub(crate) unsafe fn main_deallocate(ptr: NonNull<u8>, layout: Layout, min_align: usize) {
    let Ok(layout) = with_min_align(layout, min_align) else {
        return;
    };
    if layout.size() != 0 {
        // SAFETY: the allocator contract requires `ptr` to have been returned
        // by `main_allocate` for the same effective layout.
        unsafe { GlobalAlloc::dealloc(&MIMALLOC, ptr.as_ptr(), layout) };
    }
}

unsafe impl Allocator for MainAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        main_allocate(layout, VEC_MIN_ALIGN)
    }

    fn allocate_zeroed(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        main_allocate_zeroed(layout, VEC_MIN_ALIGN)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: forwarded from `Allocator::deallocate` with its allocation
        // provenance and layout requirements unchanged.
        unsafe { main_deallocate(ptr, layout, VEC_MIN_ALIGN) };
    }
}

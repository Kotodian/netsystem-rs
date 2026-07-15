//! Native malloc-family routing for Rust images that use the system shim.

use std::ffi::{c_int, c_void};
use std::mem::MaybeUninit;
use std::ptr;

use libmimalloc_sys::{
    mi_calloc, mi_free, mi_malloc, mi_malloc_aligned, mi_realloc, mi_usable_size,
};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;

#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

#[inline]
unsafe fn route_malloc(size: usize) -> *mut c_void {
    if super::main_heap_is_allocation_authority() {
        // SAFETY: READY publishes the fixed arena before native callers can
        // route here, and mimalloc accepts every `usize` allocation size.
        unsafe { mi_malloc(size) }
    } else {
        // SAFETY: the platform path calls the original allocator directly and
        // does not re-enter the interposed malloc symbol.
        unsafe { platform::system_malloc(size) }
    }
}

#[inline]
unsafe fn route_calloc(count: usize, size: usize) -> *mut c_void {
    if super::main_heap_is_allocation_authority() {
        // SAFETY: mimalloc checks multiplication overflow and returns either a
        // valid zeroed allocation or null.
        unsafe { mi_calloc(count, size) }
    } else {
        // SAFETY: the platform path calls the original allocator directly.
        unsafe { platform::system_calloc(count, size) }
    }
}

#[inline]
unsafe fn route_free(pointer: *mut c_void) {
    if pointer.is_null() {
        return;
    }
    if super::contains_main_heap(pointer.cast::<u8>()) {
        // SAFETY: the arena range proves this is a live mimalloc-family
        // pointer under the caller's malloc/free contract.
        unsafe { mi_free(pointer) };
    } else {
        // SAFETY: every non-arena pointer accepted by this malloc-family seam
        // retains bootstrap system-allocator provenance.
        unsafe { platform::system_free(pointer) };
    }
}

#[inline]
unsafe fn route_realloc(pointer: *mut c_void, new_size: usize) -> *mut c_void {
    if pointer.is_null() {
        // SAFETY: realloc(NULL, size) has malloc semantics.
        return unsafe { route_malloc(new_size) };
    }
    if new_size == 0 {
        // SAFETY: the pointer is live under realloc's caller contract.
        unsafe { route_free(pointer) };
        return ptr::null_mut();
    }
    if !super::main_heap_is_allocation_authority() {
        // SAFETY: before READY the pointer and resize stay in the original
        // system allocation domain.
        return unsafe { platform::system_realloc(pointer, new_size) };
    }
    if super::contains_main_heap(pointer.cast::<u8>()) {
        // SAFETY: the range check proves mimalloc provenance; failure leaves
        // the original allocation live.
        return unsafe { mi_realloc(pointer, new_size) };
    }

    // SAFETY: `pointer` is a live bootstrap allocation, so the original
    // allocator can report its readable extent.
    let old_size = unsafe { platform::system_usable_size(pointer) };
    // SAFETY: READY routes this allocation only to the fixed mimalloc arena.
    let replacement = unsafe { mi_malloc(new_size) };
    if replacement.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: both allocations are live, non-overlapping, and valid for the
    // copied prefix. The original pointer is released exactly once afterward.
    unsafe {
        ptr::copy_nonoverlapping(
            pointer.cast::<MaybeUninit<u8>>(),
            replacement.cast::<MaybeUninit<u8>>(),
            old_size.min(new_size),
        );
        platform::system_free(pointer);
    }
    replacement
}

#[inline]
#[cfg(target_os = "linux")]
unsafe fn route_reallocarray(pointer: *mut c_void, count: usize, size: usize) -> *mut c_void {
    let Some(total) = count.checked_mul(size) else {
        // SAFETY: errno storage is thread-local and writable for this thread.
        unsafe { platform::set_errno(libc::ENOMEM) };
        return ptr::null_mut();
    };
    // SAFETY: this preserves realloc's pointer contract for the checked size.
    unsafe { route_realloc(pointer, total) }
}

#[inline]
unsafe fn route_aligned_alloc(alignment: usize, size: usize) -> *mut c_void {
    if !valid_alignment(alignment) || !size.is_multiple_of(alignment) {
        // SAFETY: errno storage is thread-local and writable for this thread.
        unsafe { platform::set_errno(libc::EINVAL) };
        return ptr::null_mut();
    }
    // SAFETY: alignment is a power of two accepted by both allocation domains.
    unsafe { route_memalign(alignment, size) }
}

#[inline]
unsafe fn route_memalign(alignment: usize, size: usize) -> *mut c_void {
    if !valid_alignment(alignment) {
        // SAFETY: errno storage is thread-local and writable for this thread.
        unsafe { platform::set_errno(libc::EINVAL) };
        return ptr::null_mut();
    }
    let pointer = if super::main_heap_is_allocation_authority() {
        // SAFETY: READY publishes the fixed arena and alignment was validated.
        unsafe { mi_malloc_aligned(size, alignment) }
    } else {
        // SAFETY: the platform path bypasses the interposed aligned symbols.
        unsafe { platform::system_memalign(alignment, size) }
    };
    if pointer.is_null() {
        // SAFETY: errno storage is thread-local and writable for this thread.
        unsafe { platform::set_errno(libc::ENOMEM) };
    }
    pointer
}

#[inline]
unsafe fn route_posix_memalign(output: *mut *mut c_void, alignment: usize, size: usize) -> c_int {
    if output.is_null() || !valid_alignment(alignment) {
        return libc::EINVAL;
    }
    let pointer = if super::main_heap_is_allocation_authority() {
        // SAFETY: READY publishes the fixed arena and alignment was validated.
        unsafe { mi_malloc_aligned(size, alignment) }
    } else {
        // SAFETY: the platform path bypasses the interposed aligned symbols.
        unsafe { platform::system_memalign(alignment, size) }
    };
    if pointer.is_null() {
        return libc::ENOMEM;
    }
    // SAFETY: POSIX requires the caller to provide writable pointer storage;
    // the null case was rejected above.
    unsafe { output.write(pointer) };
    0
}

#[inline]
unsafe fn route_valloc(size: usize) -> *mut c_void {
    let alignment = platform::page_size();
    // SAFETY: the platform page size is a valid power-of-two alignment.
    unsafe { route_memalign(alignment, size) }
}

#[inline]
#[cfg(target_os = "linux")]
unsafe fn route_pvalloc(size: usize) -> *mut c_void {
    let alignment = platform::page_size();
    let Some(rounded) = size
        .checked_add(alignment - 1)
        .map(|value| value / alignment * alignment)
    else {
        // SAFETY: errno storage is thread-local and writable for this thread.
        unsafe { platform::set_errno(libc::ENOMEM) };
        return ptr::null_mut();
    };
    // SAFETY: the rounded size and page alignment satisfy memalign.
    unsafe { route_memalign(alignment, rounded) }
}

#[inline]
unsafe fn route_usable_size(pointer: *mut c_void) -> usize {
    if pointer.is_null() {
        return 0;
    }
    if super::contains_main_heap(pointer.cast::<u8>()) {
        // SAFETY: the range check proves mimalloc provenance.
        unsafe { mi_usable_size(pointer) }
    } else {
        // SAFETY: the pointer retains bootstrap system provenance.
        unsafe { platform::system_usable_size(pointer) }
    }
}

#[inline]
fn valid_alignment(alignment: usize) -> bool {
    alignment >= std::mem::size_of::<*mut c_void>() && alignment.is_power_of_two()
}

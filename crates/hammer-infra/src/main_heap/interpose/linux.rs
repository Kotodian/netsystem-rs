use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicPtr, Ordering};

unsafe extern "C" {
    fn __libc_malloc(size: usize) -> *mut c_void;
    fn __libc_calloc(count: usize, size: usize) -> *mut c_void;
    fn __libc_free(pointer: *mut c_void);
    fn __libc_realloc(pointer: *mut c_void, size: usize) -> *mut c_void;
    fn __libc_memalign(alignment: usize, size: usize) -> *mut c_void;
}

type MallocUsableSize = unsafe extern "C" fn(*mut c_void) -> usize;

static MALLOC_USABLE_SIZE: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());

#[inline]
pub(super) unsafe fn system_malloc(size: usize) -> *mut c_void {
    // SAFETY: __libc_malloc bypasses the public interposed malloc symbol.
    unsafe { __libc_malloc(size) }
}

#[inline]
pub(super) unsafe fn system_calloc(count: usize, size: usize) -> *mut c_void {
    // SAFETY: __libc_calloc bypasses the public interposed calloc symbol.
    unsafe { __libc_calloc(count, size) }
}

#[inline]
pub(super) unsafe fn system_free(pointer: *mut c_void) {
    // SAFETY: callers provide a live bootstrap glibc allocation.
    unsafe { __libc_free(pointer) };
}

#[inline]
pub(super) unsafe fn system_realloc(pointer: *mut c_void, size: usize) -> *mut c_void {
    // SAFETY: callers provide a live bootstrap glibc allocation.
    unsafe { __libc_realloc(pointer, size) }
}

#[inline]
pub(super) unsafe fn system_memalign(alignment: usize, size: usize) -> *mut c_void {
    // SAFETY: callers validate alignment before reaching glibc.
    unsafe { __libc_memalign(alignment, size) }
}

#[inline]
pub(super) unsafe fn system_usable_size(pointer: *mut c_void) -> usize {
    let mut symbol = MALLOC_USABLE_SIZE.load(Ordering::Acquire);
    if symbol.is_null() {
        // SAFETY: RTLD_NEXT resolves glibc's implementation after this
        // interposing DSO; the nul-terminated name has static lifetime.
        symbol = unsafe { libc::dlsym(libc::RTLD_NEXT, c"malloc_usable_size".as_ptr()) };
        if symbol.is_null() {
            // SAFETY: without the original size query bootstrap realloc cannot
            // preserve bytes without crossing allocator provenance.
            unsafe { libc::abort() };
        }
        MALLOC_USABLE_SIZE.store(symbol, Ordering::Release);
    }
    // SAFETY: POSIX dlsym returns a callable address for the requested symbol;
    // data and function pointers have the same representation on glibc Linux.
    let usable_size: MallocUsableSize = unsafe { std::mem::transmute(symbol) };
    // SAFETY: callers provide a live bootstrap glibc allocation.
    unsafe { usable_size(pointer) }
}

#[inline]
pub(super) fn page_size() -> usize {
    // SAFETY: sysconf has no pointer preconditions and does not allocate.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if size <= 0 || !(size as usize).is_power_of_two() {
        // SAFETY: allocation alignment cannot be implemented soundly without
        // the platform page size, and there is no fallback allocation domain.
        unsafe { libc::abort() };
    }
    size as usize
}

#[inline]
pub(super) unsafe fn set_errno(value: c_int) {
    // SAFETY: __errno_location returns this thread's writable errno slot.
    unsafe { libc::__errno_location().write(value) };
}

/// # Safety
/// Preserves the C `malloc` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn malloc(size: usize) -> *mut c_void {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_malloc(size) }
}

/// # Safety
/// Preserves the C `calloc` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn calloc(count: usize, size: usize) -> *mut c_void {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_calloc(count, size) }
}

/// # Safety
/// `pointer` must be null or a live allocation from this malloc family.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free(pointer: *mut c_void) {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_free(pointer) };
}

/// # Safety
/// `pointer` must satisfy the C `realloc` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn realloc(pointer: *mut c_void, size: usize) -> *mut c_void {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_realloc(pointer, size) }
}

/// # Safety
/// `pointer` must satisfy the C `reallocarray` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn reallocarray(
    pointer: *mut c_void,
    count: usize,
    size: usize,
) -> *mut c_void {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_reallocarray(pointer, count, size) }
}

/// # Safety
/// Preserves the C `aligned_alloc` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn aligned_alloc(alignment: usize, size: usize) -> *mut c_void {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_aligned_alloc(alignment, size) }
}

/// # Safety
/// `output` must point to writable pointer storage.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn posix_memalign(
    output: *mut *mut c_void,
    alignment: usize,
    size: usize,
) -> c_int {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_posix_memalign(output, alignment, size) }
}

/// # Safety
/// Preserves the GNU `memalign` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn memalign(alignment: usize, size: usize) -> *mut c_void {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_memalign(alignment, size) }
}

/// # Safety
/// Preserves the GNU `valloc` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn valloc(size: usize) -> *mut c_void {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_valloc(size) }
}

/// # Safety
/// Preserves the GNU `pvalloc` contract.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pvalloc(size: usize) -> *mut c_void {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_pvalloc(size) }
}

/// # Safety
/// `pointer` must be null or a live allocation from this malloc family.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn malloc_usable_size(pointer: *mut c_void) -> usize {
    // SAFETY: delegated to the process allocation seam.
    unsafe { super::route_usable_size(pointer) }
}

/// # Safety
/// `pointer` must be null or a live allocation from this malloc family.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn cfree(pointer: *mut c_void) {
    // SAFETY: cfree has the same ownership contract as free.
    unsafe { super::route_free(pointer) };
}

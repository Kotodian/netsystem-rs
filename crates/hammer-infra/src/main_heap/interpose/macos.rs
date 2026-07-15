use std::ffi::{c_int, c_void};

#[repr(C)]
struct MallocZone {
    reserved1: *mut c_void,
    reserved2: *mut c_void,
    size: Option<unsafe extern "C" fn(*mut MallocZone, *const c_void) -> usize>,
}

unsafe extern "C" {
    fn malloc_default_zone() -> *mut MallocZone;
    fn malloc_zone_malloc(zone: *mut MallocZone, size: usize) -> *mut c_void;
    fn malloc_zone_calloc(zone: *mut MallocZone, count: usize, size: usize) -> *mut c_void;
    fn malloc_zone_free(zone: *mut MallocZone, pointer: *mut c_void);
    fn malloc_zone_realloc(zone: *mut MallocZone, pointer: *mut c_void, size: usize)
    -> *mut c_void;
    fn malloc_zone_from_ptr(pointer: *const c_void) -> *mut MallocZone;
    fn malloc_zone_memalign(zone: *mut MallocZone, alignment: usize, size: usize) -> *mut c_void;

    #[link_name = "malloc"]
    fn malloc_symbol(size: usize) -> *mut c_void;
    #[link_name = "calloc"]
    fn calloc_symbol(count: usize, size: usize) -> *mut c_void;
    #[link_name = "free"]
    fn free_symbol(pointer: *mut c_void);
    #[link_name = "realloc"]
    fn realloc_symbol(pointer: *mut c_void, size: usize) -> *mut c_void;
    #[link_name = "reallocf"]
    fn reallocf_symbol(pointer: *mut c_void, size: usize) -> *mut c_void;
    #[link_name = "aligned_alloc"]
    fn aligned_alloc_symbol(alignment: usize, size: usize) -> *mut c_void;
    #[link_name = "posix_memalign"]
    fn posix_memalign_symbol(output: *mut *mut c_void, alignment: usize, size: usize) -> c_int;
    #[link_name = "valloc"]
    fn valloc_symbol(size: usize) -> *mut c_void;
    #[link_name = "malloc_size"]
    fn malloc_size_symbol(pointer: *const c_void) -> usize;

    #[link_name = "malloc_type_malloc"]
    fn malloc_type_malloc_symbol(size: usize, type_id: u64) -> *mut c_void;
    #[link_name = "malloc_type_calloc"]
    fn malloc_type_calloc_symbol(count: usize, size: usize, type_id: u64) -> *mut c_void;
    #[link_name = "malloc_type_free"]
    fn malloc_type_free_symbol(pointer: *mut c_void, type_id: u64);
    #[link_name = "malloc_type_realloc"]
    fn malloc_type_realloc_symbol(pointer: *mut c_void, size: usize, type_id: u64) -> *mut c_void;
    #[link_name = "malloc_type_valloc"]
    fn malloc_type_valloc_symbol(size: usize, type_id: u64) -> *mut c_void;
    #[link_name = "malloc_type_aligned_alloc"]
    fn malloc_type_aligned_alloc_symbol(alignment: usize, size: usize, type_id: u64)
    -> *mut c_void;
    #[link_name = "malloc_type_posix_memalign"]
    fn malloc_type_posix_memalign_symbol(
        output: *mut *mut c_void,
        alignment: usize,
        size: usize,
        type_id: u64,
    ) -> c_int;
}

#[inline]
pub(super) unsafe fn system_malloc(size: usize) -> *mut c_void {
    // SAFETY: the default zone is process-global and malloc_zone_malloc calls
    // its callback directly rather than the interposed malloc symbol.
    unsafe { malloc_zone_malloc(malloc_default_zone(), size) }
}

#[inline]
pub(super) unsafe fn system_calloc(count: usize, size: usize) -> *mut c_void {
    // SAFETY: the default zone is process-global and the zone API checks the
    // multiplication just like calloc.
    unsafe { malloc_zone_calloc(malloc_default_zone(), count, size) }
}

#[inline]
pub(super) unsafe fn system_free(pointer: *mut c_void) {
    // SAFETY: callers pass a live bootstrap pointer allocated by a malloc zone.
    let zone = unsafe { malloc_zone_from_ptr(pointer) };
    if zone.is_null() {
        // SAFETY: an unowned pointer violates the malloc/free contract and
        // continuing could cross allocator domains.
        unsafe { libc::abort() };
    }
    // SAFETY: malloc_zone_from_ptr identified the exact owning zone.
    unsafe { malloc_zone_free(zone, pointer) };
}

#[inline]
pub(super) unsafe fn system_realloc(pointer: *mut c_void, size: usize) -> *mut c_void {
    // SAFETY: callers pass a live bootstrap pointer allocated by a malloc zone.
    let zone = unsafe { malloc_zone_from_ptr(pointer) };
    if zone.is_null() {
        // SAFETY: an unowned pointer violates realloc's contract.
        unsafe { libc::abort() };
    }
    // SAFETY: malloc_zone_from_ptr identified the exact owning zone.
    unsafe { malloc_zone_realloc(zone, pointer, size) }
}

#[inline]
pub(super) unsafe fn system_memalign(alignment: usize, size: usize) -> *mut c_void {
    // SAFETY: the caller validated alignment and the zone API bypasses the
    // interposed aligned allocation symbols.
    unsafe { malloc_zone_memalign(malloc_default_zone(), alignment, size) }
}

#[inline]
pub(super) unsafe fn system_usable_size(pointer: *mut c_void) -> usize {
    // SAFETY: callers pass a live bootstrap pointer allocated by a malloc zone.
    let zone = unsafe { malloc_zone_from_ptr(pointer) };
    if zone.is_null() {
        // SAFETY: an unowned pointer violates the size-query contract.
        unsafe { libc::abort() };
    }
    // SAFETY: malloc_zone_t's first three fields are stable public ABI and the
    // owning zone must provide its size callback.
    let Some(size) = (unsafe { &*zone }).size else {
        // SAFETY: a registered zone without its mandatory size callback cannot
        // satisfy the allocator ABI.
        unsafe { libc::abort() };
    };
    // SAFETY: the callback belongs to the zone that owns `pointer`.
    unsafe { size(zone, pointer) }
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
    // SAFETY: __error returns this thread's writable errno slot.
    unsafe { libc::__error().write(value) };
}

unsafe extern "C" fn hammer_malloc(size: usize) -> *mut c_void {
    // SAFETY: this preserves the C malloc contract.
    unsafe { super::route_malloc(size) }
}

unsafe extern "C" fn hammer_calloc(count: usize, size: usize) -> *mut c_void {
    // SAFETY: this preserves the C calloc contract.
    unsafe { super::route_calloc(count, size) }
}

unsafe extern "C" fn hammer_free(pointer: *mut c_void) {
    // SAFETY: C callers must pass null or a live malloc-family pointer.
    unsafe { super::route_free(pointer) };
}

unsafe extern "C" fn hammer_realloc(pointer: *mut c_void, size: usize) -> *mut c_void {
    // SAFETY: C callers must satisfy realloc's pointer contract.
    unsafe { super::route_realloc(pointer, size) }
}

unsafe extern "C" fn hammer_reallocf(pointer: *mut c_void, size: usize) -> *mut c_void {
    // SAFETY: C callers must satisfy reallocf's pointer contract.
    let replacement = unsafe { super::route_realloc(pointer, size) };
    if replacement.is_null() && !pointer.is_null() && size != 0 {
        // SAFETY: reallocf frees its original pointer on allocation failure.
        unsafe { super::route_free(pointer) };
    }
    replacement
}

unsafe extern "C" fn hammer_aligned_alloc(alignment: usize, size: usize) -> *mut c_void {
    // SAFETY: this preserves the C aligned_alloc contract.
    unsafe { super::route_aligned_alloc(alignment, size) }
}

unsafe extern "C" fn hammer_posix_memalign(
    output: *mut *mut c_void,
    alignment: usize,
    size: usize,
) -> c_int {
    // SAFETY: C callers provide writable output storage.
    unsafe { super::route_posix_memalign(output, alignment, size) }
}

unsafe extern "C" fn hammer_valloc(size: usize) -> *mut c_void {
    // SAFETY: this preserves the C valloc contract.
    unsafe { super::route_valloc(size) }
}

unsafe extern "C" fn hammer_malloc_size(pointer: *const c_void) -> usize {
    // SAFETY: C callers pass null or a live malloc-family pointer.
    unsafe { super::route_usable_size(pointer.cast_mut()) }
}

unsafe extern "C" fn hammer_malloc_type_malloc(size: usize, _: u64) -> *mut c_void {
    // SAFETY: the type id does not change allocation ownership semantics.
    unsafe { super::route_malloc(size) }
}

unsafe extern "C" fn hammer_malloc_type_calloc(count: usize, size: usize, _: u64) -> *mut c_void {
    // SAFETY: the type id does not change allocation ownership semantics.
    unsafe { super::route_calloc(count, size) }
}

unsafe extern "C" fn hammer_malloc_type_free(pointer: *mut c_void, _: u64) {
    // SAFETY: the type id does not change allocation ownership semantics.
    unsafe { super::route_free(pointer) };
}

unsafe extern "C" fn hammer_malloc_type_realloc(
    pointer: *mut c_void,
    size: usize,
    _: u64,
) -> *mut c_void {
    // SAFETY: the type id does not change allocation ownership semantics.
    unsafe { super::route_realloc(pointer, size) }
}

unsafe extern "C" fn hammer_malloc_type_valloc(size: usize, _: u64) -> *mut c_void {
    // SAFETY: the type id does not change allocation ownership semantics.
    unsafe { super::route_valloc(size) }
}

unsafe extern "C" fn hammer_malloc_type_aligned_alloc(
    alignment: usize,
    size: usize,
    _: u64,
) -> *mut c_void {
    // SAFETY: the type id does not change allocation ownership semantics.
    unsafe { super::route_aligned_alloc(alignment, size) }
}

unsafe extern "C" fn hammer_malloc_type_posix_memalign(
    output: *mut *mut c_void,
    alignment: usize,
    size: usize,
    _: u64,
) -> c_int {
    // SAFETY: the type id does not change allocation ownership semantics.
    unsafe { super::route_posix_memalign(output, alignment, size) }
}

#[repr(C)]
struct Interpose {
    replacement: *const (),
    replacee: *const (),
}

// SAFETY: each record is immutable loader metadata containing function
// addresses; it is never dereferenced or mutated by Rust.
unsafe impl Sync for Interpose {}

macro_rules! interpose {
    ($name:ident, $replacement:ident, $replacee:ident) => {
        #[used]
        #[unsafe(link_section = "__DATA,__interpose")]
        static $name: Interpose = Interpose {
            replacement: $replacement as *const (),
            replacee: $replacee as *const (),
        };
    };
}

interpose!(INTERPOSE_MALLOC, hammer_malloc, malloc_symbol);
interpose!(INTERPOSE_CALLOC, hammer_calloc, calloc_symbol);
interpose!(INTERPOSE_FREE, hammer_free, free_symbol);
interpose!(INTERPOSE_REALLOC, hammer_realloc, realloc_symbol);
interpose!(INTERPOSE_REALLOCF, hammer_reallocf, reallocf_symbol);
interpose!(
    INTERPOSE_ALIGNED_ALLOC,
    hammer_aligned_alloc,
    aligned_alloc_symbol
);
interpose!(
    INTERPOSE_POSIX_MEMALIGN,
    hammer_posix_memalign,
    posix_memalign_symbol
);
interpose!(INTERPOSE_VALLOC, hammer_valloc, valloc_symbol);
interpose!(
    INTERPOSE_MALLOC_SIZE,
    hammer_malloc_size,
    malloc_size_symbol
);
interpose!(
    INTERPOSE_MALLOC_TYPE_MALLOC,
    hammer_malloc_type_malloc,
    malloc_type_malloc_symbol
);
interpose!(
    INTERPOSE_MALLOC_TYPE_CALLOC,
    hammer_malloc_type_calloc,
    malloc_type_calloc_symbol
);
interpose!(
    INTERPOSE_MALLOC_TYPE_FREE,
    hammer_malloc_type_free,
    malloc_type_free_symbol
);
interpose!(
    INTERPOSE_MALLOC_TYPE_REALLOC,
    hammer_malloc_type_realloc,
    malloc_type_realloc_symbol
);
interpose!(
    INTERPOSE_MALLOC_TYPE_VALLOC,
    hammer_malloc_type_valloc,
    malloc_type_valloc_symbol
);
interpose!(
    INTERPOSE_MALLOC_TYPE_ALIGNED_ALLOC,
    hammer_malloc_type_aligned_alloc,
    malloc_type_aligned_alloc_symbol
);
interpose!(
    INTERPOSE_MALLOC_TYPE_POSIX_MEMALIGN,
    hammer_malloc_type_posix_memalign,
    malloc_type_posix_memalign_symbol
);

//! Process-global fixed-capacity Hammer main heap.
//!
//! Before explicit initialization, Rust startup and configuration parsing use
//! the system allocator as bootstrap storage. Once initialized, every ordinary
//! allocation is served only from one reserved mimalloc arena shared by all
//! Hammer link images. After reservation, mimalloc OS allocation is disabled,
//! so exhaustion cannot expand the arena or fall back to another mapping.

use std::alloc::{GlobalAlloc, Layout, System};
use std::ffi::c_void;
use std::fmt;
use std::hint::spin_loop;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};

use libmimalloc_sys::{
    mi_arena_id_t, mi_arena_min_size, mi_free, mi_malloc_aligned, mi_option_limit_os_alloc,
    mi_option_set_enabled, mi_realloc_aligned, mi_reserve_os_memory_ex, mi_zalloc_aligned,
};

mod interpose;

unsafe extern "C" {
    fn mi_arena_area(arena_id: mi_arena_id_t, size: *mut usize) -> *mut c_void;
    fn mi_arena_min_alignment() -> usize;
}

pub const DEFAULT_MAIN_HEAP_SIZE: usize = 1 << 30;

const UNINITIALIZED: u8 = 0;
const INITIALIZING: u8 = 1;
const READY: u8 = 2;

static STATE: AtomicU8 = AtomicU8::new(UNINITIALIZED);
static ARENA_ID: AtomicPtr<c_void> = AtomicPtr::new(ptr::null_mut());
static ARENA_BASE: AtomicPtr<u8> = AtomicPtr::new(ptr::null_mut());
static CAPACITY: AtomicUsize = AtomicUsize::new(0);

struct HammerMainHeap;

#[global_allocator]
static GLOBAL_ALLOCATOR: HammerMainHeap = HammerMainHeap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MainHeapError {
    InvalidSize { requested: usize },
    SizeOverflow { requested: usize },
    AlreadyInitialized { current: usize, requested: usize },
    ReserveFailed { size: usize, code: i32 },
}

impl fmt::Display for MainHeapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSize { requested } => write!(
                formatter,
                "main heap size {requested} is smaller than the {}-byte arena minimum",
                minimum_capacity()
            ),
            Self::SizeOverflow { requested } => {
                write!(
                    formatter,
                    "main heap size {requested} overflows arena alignment"
                )
            }
            Self::AlreadyInitialized { current, requested } => write!(
                formatter,
                "main heap is already initialized with {current} bytes, requested {requested}"
            ),
            Self::ReserveFailed { size, code } => {
                write!(
                    formatter,
                    "failed to reserve {size}-byte main heap arena: {code}"
                )
            }
        }
    }
}

impl std::error::Error for MainHeapError {}

/// Reserve and publish the process Main Heap.
///
/// Process entry points must call this before spawning threads or loading
/// plugins. Repeating the same effective capacity is a no-op; requesting a
/// different capacity after publication is an error.
pub fn init(requested: usize) -> Result<usize, MainHeapError> {
    let size = aligned_capacity(requested)?;
    loop {
        match STATE.load(Ordering::Acquire) {
            READY => {
                let current = CAPACITY.load(Ordering::Acquire);
                return if current == size {
                    Ok(current)
                } else {
                    Err(MainHeapError::AlreadyInitialized {
                        current,
                        requested: size,
                    })
                };
            }
            INITIALIZING => spin_loop(),
            UNINITIALIZED => {
                if STATE
                    .compare_exchange(
                        UNINITIALIZED,
                        INITIALIZING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return initialize_arena(size);
                }
            }
            _ => unreachable!("invalid main heap state"),
        }
    }
}

// These public functions are the dynamic-link boundary for the process-wide
// allocator authority. Do not inline their state or mimalloc references into
// downstream Hammer libraries or plugin images.
#[inline(never)]
pub fn init_default() -> Result<usize, MainHeapError> {
    init(DEFAULT_MAIN_HEAP_SIZE)
}

#[inline(never)]
pub fn minimum_capacity() -> usize {
    // SAFETY: this query has no preconditions and does not allocate.
    unsafe { mi_arena_min_size() }
}

#[inline]
fn contains_main_heap(pointer: *const u8) -> bool {
    if pointer.is_null() || STATE.load(Ordering::Acquire) != READY {
        return false;
    }
    let base = ARENA_BASE.load(Ordering::Acquire) as usize;
    let capacity = CAPACITY.load(Ordering::Acquire);
    (pointer as usize).wrapping_sub(base) < capacity
}

fn aligned_capacity(requested: usize) -> Result<usize, MainHeapError> {
    let minimum = minimum_capacity();
    if requested < minimum {
        return Err(MainHeapError::InvalidSize { requested });
    }
    // SAFETY: this query has no preconditions and does not allocate.
    let alignment = unsafe { mi_arena_min_alignment() };
    if alignment == 0 {
        return Err(MainHeapError::SizeOverflow { requested });
    }
    requested
        .checked_add(alignment - 1)
        .map(|size| size / alignment * alignment)
        .ok_or(MainHeapError::SizeOverflow { requested })
}

fn initialize_arena(size: usize) -> Result<usize, MainHeapError> {
    let mut arena_id: mi_arena_id_t = ptr::null_mut();
    // SAFETY: mimalloc owns the reservation it creates. The arena is visible
    // to its default thread heaps; OS allocation is disabled immediately after
    // this succeeds, making this reservation their only growth source.
    let code =
        unsafe { mi_reserve_os_memory_ex(size, true, false, false, ptr::from_mut(&mut arena_id)) };
    if code != 0 {
        STATE.store(UNINITIALIZED, Ordering::Release);
        return Err(MainHeapError::ReserveFailed { size, code });
    }

    let mut actual_size = 0usize;
    // SAFETY: a zero return code publishes a valid arena id owned by this
    // mimalloc authority. `actual_size` is writable for the duration of the
    // call and the returned area remains reserved for the process lifetime.
    let arena_base = unsafe { mi_arena_area(arena_id, ptr::from_mut(&mut actual_size)) };
    if arena_id.is_null() || arena_base.is_null() || actual_size < size {
        // A successful reservation with no usable authority violates the C
        // interface contract. Do not retry: the first arena may already be
        // registered and a second reservation would break fixed capacity.
        std::process::abort();
    }

    // SAFETY: initialization is serialized by STATE and no mimalloc-backed
    // Rust allocation is published until READY. This option is process-global
    // to the single mimalloc authority linked into hammer-infra.
    unsafe { mi_option_set_enabled(mi_option_limit_os_alloc, true) };
    ARENA_ID.store(arena_id, Ordering::Release);
    ARENA_BASE.store(arena_base.cast::<u8>(), Ordering::Release);
    CAPACITY.store(actual_size, Ordering::Release);
    STATE.store(READY, Ordering::Release);
    Ok(actual_size)
}

#[inline]
fn main_heap_is_allocation_authority() -> bool {
    STATE.load(Ordering::Acquire) == READY
}

#[inline]
pub(crate) unsafe fn allocate(layout: Layout) -> *mut u8 {
    if !main_heap_is_allocation_authority() {
        // SAFETY: bootstrap allocation is paired with provenance-aware
        // deallocation below.
        return unsafe { System.alloc(layout) };
    }
    // SAFETY: READY means the fixed arena is published and mimalloc OS growth
    // is disabled. `layout` supplies a valid size/alignment pair.
    unsafe { mi_malloc_aligned(layout.size(), layout.align()).cast::<u8>() }
}

#[inline]
pub(crate) unsafe fn allocate_zeroed(layout: Layout) -> *mut u8 {
    if !main_heap_is_allocation_authority() {
        // SAFETY: bootstrap allocation is paired with provenance-aware
        // deallocation below.
        return unsafe { System.alloc_zeroed(layout) };
    }
    // SAFETY: READY means the fixed arena is published and mimalloc OS growth
    // is disabled. `layout` supplies a valid size/alignment pair.
    unsafe { mi_zalloc_aligned(layout.size(), layout.align()).cast::<u8>() }
}

#[inline]
pub(crate) unsafe fn deallocate(pointer: *mut u8, layout: Layout) {
    if pointer.is_null() {
        return;
    }
    if contains_main_heap(pointer) {
        // SAFETY: the GlobalAlloc contract says `pointer` came from this
        // allocator, and the range check identifies its mimalloc provenance.
        unsafe { mi_free(pointer.cast::<c_void>()) };
    } else {
        // SAFETY: pointers outside mimalloc originate from the bootstrap
        // System allocator and retain their original layout.
        unsafe { System.dealloc(pointer, layout) };
    }
}

#[inline]
unsafe fn reallocate(pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
    if !main_heap_is_allocation_authority() {
        // SAFETY: before main-heap initialization, ordinary allocations use
        // System with the supplied original layout.
        return unsafe { System.realloc(pointer, layout, new_size) };
    }

    let Ok(new_layout) = Layout::from_size_align(new_size, layout.align()) else {
        return ptr::null_mut();
    };

    if contains_main_heap(pointer) {
        // SAFETY: the GlobalAlloc contract plus the range check prove
        // `pointer` is a mimalloc block. OS growth is disabled, so a
        // successful resize remains in the fixed arena and failure leaves the
        // original allocation intact.
        return unsafe {
            mi_realloc_aligned(pointer.cast::<c_void>(), new_size, layout.align()).cast::<u8>()
        };
    }

    // SAFETY: allocation uses the published fixed arena.
    let replacement = unsafe { allocate(new_layout) };
    if replacement.is_null() {
        return ptr::null_mut();
    }
    // SAFETY: both allocations are valid for at least the copied prefix and
    // cannot overlap because `replacement` is newly allocated.
    unsafe {
        ptr::copy_nonoverlapping(pointer, replacement, layout.size().min(new_size));
        System.dealloc(pointer, layout);
    }
    replacement
}

// SAFETY: every operation preserves the GlobalAlloc layout contract and routes
// deallocation/reallocation according to the allocation's bootstrap or arena
// provenance.
unsafe impl GlobalAlloc for HammerMainHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: preserves the GlobalAlloc layout contract.
        unsafe { allocate(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: preserves the GlobalAlloc layout contract.
        unsafe { allocate_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: preserves the GlobalAlloc pointer/layout contract.
        unsafe { deallocate(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: preserves the GlobalAlloc pointer/layout contract.
        unsafe { reallocate(pointer, layout, new_size) }
    }
}

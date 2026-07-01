//! VPP "main heap" abstraction: a replaceable, per-NUMA allocation backend.
//! Pool/Vec/Slice draw from a Heap instead of calling std::alloc directly,
//! matching VPP's "active heap" model (vppinfra/mem.h) without porting the
//! slab/bin internals — that belongs to the global allocator, not the framework.
//!
//! Dispatch goes through a hand-written vtable (`HeapVTable`) and a raw data
//! pointer. `Heap` is a concrete struct (NOT a trait; no `dyn`, no autotrait,
//! no `Sized` bound): a `&'static HeapVTable` plus a `*const ()` that points
//! into an `Arc<HeapData>` allocation. The `Arc` keeps the underlying
//! `SvmRegion` alive across `Heap::clone` calls, so an `Arc<Heap>` shared
//! between Pool instances shares the same backing region.
//!
//! Per-NUMA selection happens at `HeapRegistry::for_numa`, not on the hot
//! path. The per-packet fast path (future Tasks 6/7) keeps a lockless
//! per-thread cache and never touches a vtable; vtable calls fire only at
//! construction, cache-refill (batch=32), and cache-overflow.
use std::alloc::{Layout, alloc, dealloc};
use std::ptr::NonNull;
use std::sync::Arc;

use crate::svm_region::SvmRegion;

/// Hand-written vtable. Function pointers are inherently `Send + Sync`.
/// `unsafe fn` keeps the same safety contract as the previous `Heap` trait.
#[repr(C)]
pub struct HeapVTable {
    /// Allocates `layout.size()` bytes aligned to `layout.align()`. Returns
    /// `None` on OOM. The returned pointer is suitable for any
    /// well-aligned read/write of `layout.size()` bytes.
    pub alloc: unsafe fn(*const (), Layout) -> Option<NonNull<u8>>,
    /// # Safety
    /// `ptr` must have been returned by
    /// `(self.vt.alloc)(self.data, layout)` and not previously
    /// deallocated; `layout` must match the alloc call exactly.
    pub dealloc: unsafe fn(*const (), NonNull<u8>, Layout),
    /// Returns the NUMA node this heap is bound to. `0` for a generic
    /// `std::alloc`-backed heap.
    pub numa_node: unsafe fn(*const ()) -> u32,
}

/// Concrete heap handle: vtable + data. NOT a trait; no `dyn`.
pub struct Heap {
    vt: &'static HeapVTable,
    data: *const (),
}

/// Per-Heap payload. The pointer is owned by the `Heap` via `Arc`; `Drop`
/// runs the `SvmRegion::drop` chain (munmap + close) when the last
/// `Arc<Heap>` is dropped.
enum HeapData {
    Local { numa: u32 },
    Svm { region: SvmRegion, numa: u32 },
}

// SAFETY: `HeapData` is `Send + Sync`.
// - `HeapData::Local { numa: u32 }`: `u32` is `Send + Sync`.
// - `HeapData::Svm { region: SvmRegion, numa: u32 }`: `SvmRegion` is
//   `Send + Sync` (it carries `Arc<SvmRegionInner>`, and `SvmRegionInner`
//   has `unsafe impl Send + Sync` in `svm_region.rs`); `u32` is
//   `Send + Sync`. Therefore the variant is `Send + Sync`.
// - The whole enum is `Send + Sync`.
unsafe impl Send for HeapData {}
unsafe impl Sync for HeapData {}

impl Heap {
    /// Local heap — delegates to `std::alloc::{alloc, dealloc}`. The
    /// `numa` argument is recorded for `HeapRegistry` indexing but does
    /// not influence allocation strategy (the global allocator is not
    /// NUMA-aware at this layer).
    pub fn local(numa: u32) -> Heap {
        let raw = Arc::into_raw(Arc::new(HeapData::Local { numa }));
        Heap {
            vt: &LOCAL_VT,
            data: raw as *const (),
        }
    }

    /// SVM-backed heap. `region` is shared via its internal
    /// `Arc<SvmRegionInner>`, so the same fd + mmap survives every
    /// `Heap::clone` of this handle.
    pub fn svm(region: SvmRegion, numa: u32) -> Heap {
        let raw = Arc::into_raw(Arc::new(HeapData::Svm { region, numa }));
        Heap {
            vt: &SVM_VT,
            data: raw as *const (),
        }
    }

    #[inline]
    pub fn alloc(&self, layout: Layout) -> Option<NonNull<u8>> {
        unsafe { (self.vt.alloc)(self.data, layout) }
    }

    /// # Safety
    /// `ptr` must have been returned by `self.alloc(layout)` and not
    /// previously deallocated; `layout` must match the alloc call exactly.
    #[inline]
    pub unsafe fn dealloc(&self, ptr: NonNull<u8>, layout: Layout) {
        unsafe { (self.vt.dealloc)(self.data, ptr, layout) }
    }

    #[inline]
    pub fn numa_node(&self) -> u32 {
        unsafe { (self.vt.numa_node)(self.data) }
    }

    /// Type-erased region accessor. Returns `Some(&SvmRegion)` only when
    /// the vtable pointer matches the SVM vtable (vtable-address identity).
    /// `None` for the Local vtable.
    pub fn region(&self) -> Option<&SvmRegion> {
        if std::ptr::eq(self.vt as *const _, &SVM_VT as *const _) {
            // SAFETY: SVM vtable implies `data` is a valid
            // `*const HeapData` pointing to a `HeapData::Svm` variant
            // (constructed by `Heap::svm`). The `Arc<HeapData>` keeps
            // the pointee alive for the duration of `&self`.
            let data_ref = unsafe { &*(self.data as *const HeapData) };
            match data_ref {
                HeapData::Svm { region, .. } => Some(region),
                // Defensive: the SVM vtable is only paired with
                // `HeapData::Svm`. Match the variant anyway so a
                // future vtable-pairing bug cannot produce a dangling
                // `&SvmRegion`.
                HeapData::Local { .. } => None,
            }
        } else {
            None
        }
    }
}

impl Clone for Heap {
    fn clone(&self) -> Heap {
        // SAFETY: `data` is a valid `*const HeapData` produced by
        // `Arc::into_raw` in `Heap::local` or `Heap::svm`. Bumping the
        // strong count keeps the allocation alive for the new `Heap`,
        // mirroring `Arc::clone` without materialising a temporary
        // `Arc` (which would auto-decrement).
        unsafe { Arc::increment_strong_count(self.data as *const HeapData) };
        Heap {
            vt: self.vt,
            data: self.data,
        }
    }
}

impl Drop for Heap {
    fn drop(&mut self) {
        // SAFETY: `data` is a valid `*const HeapData` produced by
        // `Arc::into_raw` in `Heap::local` or `Heap::svm`. Reconstructing
        // the `Arc` decrements the strong count; when the last
        // `Arc<Heap>` is dropped, the inner `SvmRegion` is dropped
        // (closing fd + munmap via `SvmRegionInner::drop`).
        unsafe { Arc::from_raw(self.data as *const HeapData) };
    }
}

// SAFETY: `Heap` is `Send + Sync`.
// - `vt: &'static HeapVTable` is `&'static`, so it is `Sync`; the
//   function pointers inside are inherently `Send + Sync` (a `fn`
//   pointer can be called from any thread).
// - `data: *const ()` is `!Send + !Sync` by default. It points into an
//   `Arc<HeapData>` allocation whose pointee is `Send + Sync`. The
//   `Arc` reference count provides thread-safe shared ownership, and
//   the vtable functions only read the pointee through the typed
//   vtable (`alloc`/`dealloc` mutate the SvmRegion's *internal* state
//   via the SvmRegion's own synchronization; no aliasing mutation of
//   `HeapData` itself happens through `&self`). `Drop` requires
//   `&mut self`, so it cannot race with shared access.
// Therefore sharing `Heap` across threads is sound.
unsafe impl Send for Heap {}
unsafe impl Sync for Heap {}

// --- Local vtable functions -------------------------------------------------
// `data` is a valid `*const HeapData` to a `HeapData::Local` variant
// (enforced by the static VT pairing with `Heap::local`). `local_alloc` /
// `local_dealloc` ignore the data payload; only `local_numa` reads it.

unsafe fn local_alloc(_data: *const (), layout: Layout) -> Option<NonNull<u8>> {
    unsafe { NonNull::new(alloc(layout)) }
}

unsafe fn local_dealloc(_data: *const (), ptr: NonNull<u8>, layout: Layout) {
    unsafe { dealloc(ptr.as_ptr(), layout) }
}

unsafe fn local_numa(data: *const ()) -> u32 {
    // SAFETY: see module-level comment on the Local vtable.
    let data = unsafe { &*(data as *const HeapData) };
    match data {
        HeapData::Local { numa } => *numa,
        // Unreachable: the Local vtable is only paired with
        // `Heap::local`, which always produces `HeapData::Local`.
        HeapData::Svm { .. } => 0,
    }
}

static LOCAL_VT: HeapVTable = HeapVTable {
    alloc: local_alloc,
    dealloc: local_dealloc,
    numa_node: local_numa,
};

// --- SVM vtable functions ---------------------------------------------------
// `data` is a valid `*const HeapData` to a `HeapData::Svm` variant
// (enforced by the static VT pairing with `Heap::svm`). All three vtable
// functions read the SvmRegion + numa from the payload.

unsafe fn svm_alloc(data: *const (), layout: Layout) -> Option<NonNull<u8>> {
    // SAFETY: see module-level comment on the SVM vtable.
    let data = unsafe { &*(data as *const HeapData) };
    let HeapData::Svm { region, .. } = data else {
        // Unreachable: the SVM vtable is only paired with `Heap::svm`,
        // which always produces `HeapData::Svm`. Treat as OOM rather
        // than UB.
        return None;
    };
    let off = region.alloc(layout.size(), layout.align());
    if off == u64::MAX {
        return None;
    }
    let ptr = unsafe { region.base().add(off as usize) };
    NonNull::new(ptr)
}

unsafe fn svm_dealloc(data: *const (), ptr: NonNull<u8>, layout: Layout) {
    // SAFETY: see module-level comment on the SVM vtable.
    let data = unsafe { &*(data as *const HeapData) };
    let HeapData::Svm { region, .. } = data else {
        // Unreachable: see `svm_alloc`.
        return;
    };
    let base = region.base();
    let off = (ptr.as_ptr() as usize - base as usize) as u64;
    region.free(off, layout.size());
}

unsafe fn svm_numa(data: *const ()) -> u32 {
    // SAFETY: see module-level comment on the SVM vtable.
    let data = unsafe { &*(data as *const HeapData) };
    match data {
        HeapData::Svm { numa, .. } => *numa,
        // Unreachable: see `svm_alloc`.
        HeapData::Local { .. } => 0,
    }
}

static SVM_VT: HeapVTable = HeapVTable {
    alloc: svm_alloc,
    dealloc: svm_dealloc,
    numa_node: svm_numa,
};

pub struct HeapRegistry {
    heaps: Vec<Arc<Heap>>,
}

impl HeapRegistry {
    pub fn new() -> HeapRegistry {
        HeapRegistry { heaps: Vec::new() }
    }

    pub fn register(&mut self, heap: Arc<Heap>) -> &mut Self {
        let n = heap.numa_node() as usize;
        if self.heaps.len() <= n {
            self.heaps.resize(n + 1, Arc::new(Heap::local(0)));
        }
        self.heaps[n] = heap;
        self
    }

    pub fn for_numa(&self, numa: u32) -> Option<Arc<Heap>> {
        self.heaps.get(numa as usize).cloned()
    }
}

impl Default for HeapRegistry {
    fn default() -> Self {
        HeapRegistry::new()
    }
}

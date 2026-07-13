//! Page allocation, publication, and reclamation for bihash values.
//!
//! Published pages are immutable. Writers build a private replacement and
//! publish its allocator offset through the bucket word. Retired offsets are
//! reused only after no lookup hazard protects them.

use std::alloc::{GlobalAlloc, Layout, handle_alloc_error};
use std::cell::{RefCell, UnsafeCell};
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

use crate::align::CACHE_LINE;
use crate::bihash::value::ValuePage;
use crate::boxed::{Slice, allocate_in, deallocate_in};
use crate::heap::Heap;
use crate::vec::Vec;

const MAX_LOG2_PAGES: usize = 8;
const NO_OFFSET: u64 = 0;

static NEXT_HAZARD_DOMAIN: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
struct HazardBinding {
    domain: u64,
    slot: NonNull<CachePadded<AtomicU64>>,
}

thread_local! {
    static BIHASH_HAZARDS: RefCell<Vec<HazardBinding>> = const { RefCell::new(Vec::new()) };
}

struct PageBlock<K, const KVP: usize> {
    pages: NonNull<ValuePage<K, KVP>>,
    log2_pages: u8,
}

#[derive(Clone, Copy)]
struct RetiredOffset {
    offset: u64,
    log2_pages: u8,
}

struct PageAllocState<K, const KVP: usize> {
    blocks: Vec<PageBlock<K, KVP>>,
    directories: Vec<Slice<usize>>,
    hazards: Vec<NonNull<CachePadded<AtomicU64>>>,
    freelists: [Vec<u64>; MAX_LOG2_PAGES + 1],
    retired: Vec<RetiredOffset>,
    heap: Arc<Heap>,
}

impl<K: Copy + Default, const KVP: usize> PageAllocState<K, KVP> {
    fn new(heap: Arc<Heap>) -> Self {
        Self {
            blocks: Vec::with_capacity_in(0, heap.clone()),
            directories: Vec::with_capacity_in(0, heap.clone()),
            hazards: Vec::with_capacity_in(0, heap.clone()),
            freelists: core::array::from_fn(|_| Vec::with_capacity_in(0, heap.clone())),
            retired: Vec::with_capacity_in(0, heap.clone()),
            heap,
        }
    }

    fn allocate(&mut self, log2_pages: u8) -> (u64, Option<*mut usize>) {
        let class = usize::from(log2_pages);
        assert!(
            class <= MAX_LOG2_PAGES,
            "bihash log2_pages exceeds allocator limit"
        );
        self.collect_retired();

        if let Some(offset) = self.freelists[class].pop() {
            let pages = self.pages_mut(offset, log2_pages);
            for page in pages {
                *page = ValuePage::new();
            }
            return (offset, None);
        }

        let page_count = 1usize << log2_pages;
        let pages = allocate_in::<ValuePage<K, KVP>, CACHE_LINE>(page_count, &self.heap);
        for index in 0..page_count {
            // SAFETY: `pages` names `page_count` uninitialized writable slots.
            unsafe { pages.as_ptr().add(index).write(ValuePage::new()) };
        }
        self.blocks.push(PageBlock {
            pages,
            log2_pages,
        });
        let offset = u64::try_from(self.blocks.len()).expect("bihash allocator offset fits u64");
        let directory = self.grow_directory();
        (offset, Some(directory))
    }

    fn grow_directory(&mut self) -> *mut usize {
        let capacity = self.blocks.len().next_power_of_two();
        let mut directory = Slice::from_elem_in(capacity, 0usize, self.heap.clone());
        for (index, block) in self.blocks.iter().enumerate() {
            directory[index] = block.pages.as_ptr().expose_provenance();
        }
        let published = directory.as_mut_ptr();
        self.directories.push(directory);
        published
    }

    fn allocate_hazard(&mut self) -> NonNull<CachePadded<AtomicU64>> {
        let layout = Layout::new::<CachePadded<AtomicU64>>();
        let raw = self
            .heap
            .alloc(layout)
            .unwrap_or_else(|| handle_alloc_error(layout));
        let slot = raw.cast::<CachePadded<AtomicU64>>();
        // SAFETY: `slot` points to one suitably aligned writable allocation.
        unsafe { slot.as_ptr().write(CachePadded::new(AtomicU64::new(NO_OFFSET))) };
        self.hazards.push(slot);
        slot
    }

    fn retire(&mut self, offset: u64, log2_pages: u8) {
        self.retired.push(RetiredOffset {
            offset,
            log2_pages,
        });
        self.collect_retired();
    }

    fn collect_retired(&mut self) {
        let mut index = 0;
        while index < self.retired.len() {
            let retired = self.retired[index];
            let protected = self.hazards.iter().any(|slot| {
                // SAFETY: hazard slots remain allocated until PageAlloc is dropped.
                unsafe { slot.as_ref() }.load(Ordering::SeqCst) == retired.offset
            });
            if protected {
                index += 1;
                continue;
            }
            self.retired.remove(index);
            self.freelists[usize::from(retired.log2_pages)].push(retired.offset);
        }
    }

    fn pages(&self, offset: u64, log2_pages: u8) -> &[ValuePage<K, KVP>] {
        let block = self.block(offset, log2_pages);
        // SAFETY: PageBlock owns `2^log2_pages` initialized contiguous pages.
        unsafe { std::slice::from_raw_parts(block.pages.as_ptr(), 1usize << log2_pages) }
    }

    fn pages_mut(&mut self, offset: u64, log2_pages: u8) -> &mut [ValuePage<K, KVP>] {
        let block = self.block(offset, log2_pages);
        // SAFETY: allocator serialization and unpublished/freelist state provide
        // exclusive access to this page block.
        unsafe { std::slice::from_raw_parts_mut(block.pages.as_ptr(), 1usize << log2_pages) }
    }

    fn block(&self, offset: u64, log2_pages: u8) -> &PageBlock<K, KVP> {
        let index = usize::try_from(offset - 1).expect("bihash allocator offset fits usize");
        let block = &self.blocks[index];
        debug_assert_eq!(block.log2_pages, log2_pages);
        block
    }
}

impl<K, const KVP: usize> Drop for PageAllocState<K, KVP> {
    fn drop(&mut self) {
        for block in &self.blocks {
            let page_count = 1usize << block.log2_pages;
            // SAFETY: every block was initialized by `allocate` and is still owned here.
            unsafe {
                std::ptr::drop_in_place(std::slice::from_raw_parts_mut(
                    block.pages.as_ptr(),
                    page_count,
                ));
                deallocate_in::<ValuePage<K, KVP>, CACHE_LINE>(
                    block.pages,
                    page_count,
                    &self.heap,
                );
            }
        }
        for slot in &self.hazards {
            let layout = Layout::new::<CachePadded<AtomicU64>>();
            // SAFETY: every slot was allocated with this layout from `self.heap`.
            unsafe {
                std::ptr::drop_in_place(slot.as_ptr());
                GlobalAlloc::dealloc(&*self.heap, slot.as_ptr().cast::<u8>(), layout);
            }
        }
    }
}

/// Owns every page address and all reclamation state for one bihash.
pub(crate) struct PageAlloc<K, const KVP: usize> {
    state: UnsafeCell<PageAllocState<K, KVP>>,
    busy: AtomicBool,
    directory: AtomicPtr<usize>,
    hazard_domain: u64,
}

// SAFETY: all PageAllocState mutation is serialized by `busy`; readers only
// dereference immutable published pages protected by a per-thread hazard slot.
unsafe impl<K: Copy + Default + Send, const KVP: usize> Send for PageAlloc<K, KVP> {}
// SAFETY: the same serialization and publication rules permit shared access.
unsafe impl<K: Copy + Default + Send, const KVP: usize> Sync for PageAlloc<K, KVP> {}

impl<K: Copy + Default, const KVP: usize> PageAlloc<K, KVP> {
    pub(crate) fn new_in(heap: Arc<Heap>) -> Self {
        Self {
            state: UnsafeCell::new(PageAllocState::new(heap)),
            busy: AtomicBool::new(false),
            directory: AtomicPtr::new(std::ptr::null_mut()),
            hazard_domain: NEXT_HAZARD_DOMAIN.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub(crate) fn allocate<R>(
        &self,
        log2_pages: u8,
        initialize: impl FnOnce(&mut [ValuePage<K, KVP>]) -> R,
    ) -> (u64, R) {
        self.with_state(|state| {
            let (offset, directory) = state.allocate(log2_pages);
            if let Some(directory) = directory {
                self.directory.store(directory, Ordering::Release);
            }
            let result = initialize(state.pages_mut(offset, log2_pages));
            (offset, result)
        })
    }

    pub(crate) fn replace<R>(
        &self,
        old_offset: u64,
        old_log2_pages: u8,
        new_log2_pages: u8,
        update: impl FnOnce(&[ValuePage<K, KVP>], &mut [ValuePage<K, KVP>]) -> R,
    ) -> (u64, R) {
        self.with_state(|state| {
            let (new_offset, directory) = state.allocate(new_log2_pages);
            if let Some(directory) = directory {
                self.directory.store(directory, Ordering::Release);
            }
            let old_ptr = state.block(old_offset, old_log2_pages).pages;
            let new_ptr = state.block(new_offset, new_log2_pages).pages;
            debug_assert_ne!(old_ptr, new_ptr);
            // SAFETY: the old block remains immutable and live; the new block is
            // unpublished and exclusively owned by this allocator operation.
            let old_pages = unsafe {
                std::slice::from_raw_parts(old_ptr.as_ptr(), 1usize << old_log2_pages)
            };
            let new_pages = unsafe {
                std::slice::from_raw_parts_mut(new_ptr.as_ptr(), 1usize << new_log2_pages)
            };
            let result = update(old_pages, new_pages);
            (new_offset, result)
        })
    }

    pub(crate) fn inspect<R>(
        &self,
        offset: u64,
        log2_pages: u8,
        inspect: impl FnOnce(&[ValuePage<K, KVP>]) -> R,
    ) -> R {
        self.with_state(|state| inspect(state.pages(offset, log2_pages)))
    }

    pub(crate) fn read<R>(
        &self,
        offset: u64,
        log2_pages: u8,
        mut still_published: impl FnMut() -> bool,
        read: impl FnOnce(&[ValuePage<K, KVP>]) -> R,
    ) -> Option<R> {
        let slot = self.hazard_slot();
        // SeqCst supplies the store-load ordering required by hazard publication.
        unsafe { slot.as_ref() }.store(offset, Ordering::SeqCst);
        let hazard = HazardGuard { slot };
        if !still_published() {
            return None;
        }

        let directory = self.directory.load(Ordering::Acquire);
        debug_assert!(!directory.is_null());
        let index = usize::try_from(offset - 1).expect("bihash allocator offset fits usize");
        // SAFETY: bucket publication follows directory publication, the entry is
        // immutable, and the hazard prevents this block from being reset.
        let address = unsafe { directory.add(index).read() };
        let pages = unsafe {
            std::slice::from_raw_parts(
                std::ptr::with_exposed_provenance::<ValuePage<K, KVP>>(address),
                1usize << log2_pages,
            )
        };
        let result = read(pages);
        let published = still_published();
        drop(hazard);
        published.then_some(result)
    }

    pub(crate) fn retire(&self, offset: u64, log2_pages: u8) {
        self.with_state(|state| state.retire(offset, log2_pages));
    }

    fn hazard_slot(&self) -> NonNull<CachePadded<AtomicU64>> {
        let existing = BIHASH_HAZARDS.with(|bindings| {
            bindings
                .borrow()
                .iter()
                .find(|binding| {
                    binding.domain == self.hazard_domain
                        // SAFETY: this thread alone publishes through its slots.
                        && unsafe { binding.slot.as_ref() }.load(Ordering::Relaxed) == NO_OFFSET
                })
                .map(|binding| binding.slot)
        });
        if let Some(slot) = existing {
            return slot;
        }

        let slot = self.with_state(PageAllocState::allocate_hazard);
        BIHASH_HAZARDS.with(|bindings| {
            bindings.borrow_mut().push(HazardBinding {
                domain: self.hazard_domain,
                slot,
            });
        });
        slot
    }

    fn with_state<R>(&self, operation: impl FnOnce(&mut PageAllocState<K, KVP>) -> R) -> R {
        while self
            .busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            core::hint::spin_loop();
        }
        let guard = AllocLock { busy: &self.busy };
        // SAFETY: `guard` holds the allocator's exclusive spin lock.
        let result = operation(unsafe { &mut *self.state.get() });
        drop(guard);
        result
    }
}

struct AllocLock<'a> {
    busy: &'a AtomicBool,
}

impl Drop for AllocLock<'_> {
    fn drop(&mut self) {
        self.busy.store(false, Ordering::Release);
    }
}

struct HazardGuard {
    slot: NonNull<CachePadded<AtomicU64>>,
}

impl Drop for HazardGuard {
    fn drop(&mut self) {
        // SAFETY: the PageAlloc owns this stable slot for the duration of lookup.
        unsafe { self.slot.as_ref() }.store(NO_OFFSET, Ordering::SeqCst);
    }
}

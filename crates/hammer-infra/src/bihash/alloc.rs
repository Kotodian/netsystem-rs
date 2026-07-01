//! Heap-backed page allocator with LIFO freelists for VPP-style bihash.
//!
//! Phase 1 (this task) only supports `log2_pages == 0` (single page
//! allocations).  Multi-page allocations will be added when bucket
//! expansion is implemented.

use crate::bihash::value::ValuePage;

/// Identifies a page (or a contiguous run of 2^log2_pages pages starting at
/// this id). Page IDs are 1-indexed; id 0 is reserved for "no page" state.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct PageId(pub u32);

impl PageId {
    pub const NONE: Self = PageId(0);

    #[inline(always)]
    pub const fn is_none(self) -> bool {
        self.0 == 0
    }

    /// Convert 1-indexed id → 0-indexed offset into the `pages` Vec.
    #[inline(always)]
    pub const fn index(self) -> usize {
        (self.0 - 1) as usize
    }

    /// Convert 0-indexed offset → 1-indexed id.
    #[inline(always)]
    pub const fn from_index(i: usize) -> Self {
        PageId((i + 1) as u32)
    }
}

/// Heap-backed page allocator with per-size-class LIFO freelists.
pub struct PageAlloc<K, V: Copy, const KVP: usize> {
    pages: Vec<ValuePage<K, V, KVP>>,
    /// `freelists[log2_pages]` is a LIFO stack of PageIds.
    freelists: [Vec<PageId>; 8],
    live: usize,
}

impl<K: Copy + Default, V: Copy + Default, const KVP: usize> PageAlloc<K, V, KVP> {
    pub fn new() -> Self {
        Self {
            pages: Vec::new(),
            freelists: Default::default(),
            live: 0,
        }
    }

    /// Allocate a single free page (log2_pages = 0). If the freelist is
    /// empty, pushes a new page onto `pages` and returns its id.
    pub fn alloc_single(&mut self, log2_pages: u8) -> PageId {
        debug_assert_eq!(
            log2_pages, 0,
            "Phase 1 supports only log2_pages == 0"
        );
        if let Some(id) = self.freelists[0].pop() {
            self.pages[id.index()] = ValuePage::new();
            self.live += 1;
            id
        } else {
            self.pages.push(ValuePage::new());
            self.live += 1;
            PageId::from_index(self.pages.len() - 1)
        }
    }

    /// Return a single free page (log2_pages = 0) to the freelist.
    pub fn free(&mut self, id: PageId, log2_pages: u8) {
        debug_assert_eq!(log2_pages, 0);
        debug_assert!(!id.is_none());
        self.freelists[0].push(id);
        self.live -= 1;
    }

    /// Number of live (allocated, not freed) pages.
    #[inline]
    pub fn live_pages(&self) -> usize {
        self.live
    }

    /// Get a shared reference to a page by its id.
    #[inline]
    pub fn get(&self, id: PageId) -> &ValuePage<K, V, KVP> {
        &self.pages[id.index()]
    }

    /// Get an exclusive reference to a page by its id.
    #[inline]
    pub fn get_mut(&mut self, id: PageId) -> &mut ValuePage<K, V, KVP> {
        &mut self.pages[id.index()]
    }
}

impl<K: Copy + Default, V: Copy + Default, const KVP: usize> Default for PageAlloc<K, V, KVP> {
    fn default() -> Self {
        Self::new()
    }
}

//! VPP `vppinfra/heap` allocation side: a grow-only element space whose
//! allocations are identified by the offset of their first element.
//!
//! VPP's heap (`third_party/vpp/src/vppinfra/heap.h`) is a vector of elements
//! with a free list: a caller receives a numeric identity for a run of
//! elements and can release it later. Hammer's node error counters are the
//! only consumer — `vlib_register_errors` allocates one run of counter columns
//! per node and the returned offset *is* the column identity
//! (`third_party/vpp/src/vlib/error.c:138-141`). Nothing in Hammer releases a
//! run: a node unregister does not exist, and a graph rebuild drops the whole
//! owning state. This module therefore carries the allocation side only: no
//! free list, no size bins, no deallocation, no handle.
//!
//! Not related to [`crate::mem::MemHeap`]: that one is the dlmalloc-style
//! memory heap; this one is an offset identity space.

/// A grow-only element space (VPP's `T *heap`) allocated in runs.
///
/// An allocation returns the offset of its first element. Offsets are stable
/// for the heap's lifetime, so they are the identity a caller publishes; the
/// node error column number is one such identity.
#[derive(Clone)]
pub struct Heap<T> {
    values: Vec<T>,
}

impl<T> Default for Heap<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Heap<T> {
    #[inline]
    pub const fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Appends `elements` copies of `element` and returns the offset of the
    /// first appended element.
    ///
    /// Mirrors the tail-append path of `heap_alloc (v, size, handle)`
    /// (`_heap_alloc`, `third_party/vpp/src/vppinfra/heap.c:353-466`).
    /// `elements == 0` is a caller bug, the way VPP's `_heap_alloc` treats
    /// `size == 0` as a no-op no caller relies on (`heap.c:361-362`).
    ///
    /// # Panics
    ///
    /// Panics if `elements` is zero or if the element space would not fit in
    /// the `u32` offset type.
    #[inline]
    pub fn alloc(&mut self, elements: u32, element: T) -> u32
    where
        T: Clone,
    {
        assert!(elements > 0, "a heap allocation has at least one element");
        let offset =
            u32::try_from(self.values.len()).expect("the heap element space fits in a u32 offset");
        let end = self
            .values
            .len()
            .checked_add(elements as usize)
            .expect("the heap element space fits in a u32 offset");
        u32::try_from(end).expect("the heap element space fits in a u32 offset");
        self.values.resize(end, element);
        offset
    }

    /// The length of the element space, `vec_len (em->counters_heap)`
    /// (`third_party/vpp/src/vlib/error.c:140`).
    #[inline]
    pub fn len(&self) -> u32 {
        u32::try_from(self.values.len()).expect("the heap element space fits in a u32 offset")
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Reads the element at `offset`, VPP's `heap[offset]`
    /// (`elt_data`, `third_party/vpp/src/vppinfra/heap.c:180-185`);
    /// `None` when the offset is outside the element space.
    #[inline]
    pub fn get(&self, offset: u32) -> Option<&T> {
        self.values.get(offset as usize)
    }
}

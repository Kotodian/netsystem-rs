//! Offset-based block allocator for SVM region heaps.
//!
//! Rust counterpart of `clib_mem_create_heap` / `clib_mem_heap_alloc` /
//! `clib_mem_heap_free` (ADR-0011 section 12.4). The descriptor holds only
//! offsets and counters, so it can live in shared memory; the bytes are
//! reached through the caller's `arena` slice, which is the process-local view
//! of that region payload.
//!
//! Failure is not recoverable here: exhaustion, double free, misaligned
//! offsets, and block corruption build a [`SvmRegionHeapViolation`] and
//! terminate the process with the structured facts. A shared heap that is
//! already corrupt cannot be unwound without leaving the region in an
//! undecidable state for the other processes mapping it.

use std::alloc::Layout;
use std::fmt;

/// Fixed free-list bins. Bin `i` holds blocks of `[2^(i+5), 2^(i+6))` bytes.
pub const SVM_REGION_HEAP_BINS: usize = 64;

const FLAG_PREVIOUS_IN_USE: u64 = 1 << 0;
const FLAG_CURRENT_IN_USE: u64 = 1 << 1;
const FLAG_MASK: u64 = FLAG_PREVIOUS_IN_USE | FLAG_CURRENT_IN_USE;

/// `previous_size` plus `size_and_flags` at the start of every block.
const BLOCK_HEADER_SIZE: u64 = 16;
/// Bytes between the user pointer and its block header: the back-pointer word.
const USER_PREFIX_SIZE: u64 = 8;
const MIN_ALIGNMENT: u64 = 8;
/// Smallest block: header, back-pointer, and the two free-list link words. The
/// links live at the block tail, so the back-pointer at `block + 16` survives a
/// free; that is what lets `deallocate` report a repeat free as `DoubleFree`
/// instead of reading a chain word as a block offset.
const MIN_BLOCK_SIZE: u64 = BLOCK_HEADER_SIZE + USER_PREFIX_SIZE + 2 * MIN_ALIGNMENT;
const MIN_BIN_EXPONENT: u32 = 5;

/// Offset of the `next` link of the free block at `block`, which is `size` long.
#[inline]
fn chain_next_word(block: u64, size: u64) -> u64 {
    block + size - 2 * MIN_ALIGNMENT
}

/// Offset of the `previous` link of the free block at `block`.
#[inline]
fn chain_previous_word(block: u64, size: u64) -> u64 {
    block + size - MIN_ALIGNMENT
}

/// Facts describing why a region heap cannot continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SvmRegionHeapViolation {
    /// The heap descriptor was never initialized, or no longer describes this arena.
    NotInitialized {
        heap_start: u64,
        heap_end: u64,
        arena_len: u64,
    },
    /// `initialize` was given a range that cannot hold one block.
    InvalidRange {
        heap_start: u64,
        heap_end: u64,
        arena_len: u64,
    },
    /// No free block can satisfy the request.
    Exhausted {
        requested: u64,
        alignment: u64,
        free_bytes: u64,
    },
    /// The request cannot be represented as block sizes.
    LayoutOverflow { size: u64, alignment: u64 },
    /// `reallocate` was asked for a zero-sized result.
    ZeroSizeReallocation { offset: u64 },
    /// The offset does not satisfy the requested alignment.
    Misaligned { offset: u64, alignment: u64 },
    /// A word access would fall outside the heap range or the arena.
    OutOfRange {
        offset: u64,
        length: u64,
        heap_end: u64,
        arena_len: u64,
    },
    /// The offset is not the start of a live allocation.
    NotAllocated { offset: u64 },
    /// The block at the offset is already free.
    DoubleFree { offset: u64 },
    /// Block headers disagree about size or use flags.
    BlockCorruption {
        offset: u64,
        declared_size: u64,
        previous_size: u64,
        heap_end: u64,
    },
    /// A free list is inconsistent with the block it should contain.
    BinCorruption { bin: u8, offset: u64 },
}

impl SvmRegionHeapViolation {
    /// Reports the facts and terminates the process.
    pub fn terminate(self) -> ! {
        eprintln!("svm region heap violation: {self}");
        std::process::abort()
    }
}

impl fmt::Display for SvmRegionHeapViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::NotInitialized {
                heap_start,
                heap_end,
                arena_len,
            } => write!(
                formatter,
                "region heap not initialized: heap [{heap_start}, {heap_end}), arena {arena_len} bytes"
            ),
            Self::InvalidRange {
                heap_start,
                heap_end,
                arena_len,
            } => write!(
                formatter,
                "region heap range [{heap_start}, {heap_end}) is not a valid block range for {arena_len} bytes"
            ),
            Self::Exhausted {
                requested,
                alignment,
                free_bytes,
            } => write!(
                formatter,
                "region heap exhausted: requested {requested} bytes at alignment {alignment}, {free_bytes} free"
            ),
            Self::LayoutOverflow { size, alignment } => {
                write!(
                    formatter,
                    "region heap layout overflow: size {size}, alignment {alignment}"
                )
            }
            Self::ZeroSizeReallocation { offset } => {
                write!(
                    formatter,
                    "region heap reallocate to zero size at offset {offset}"
                )
            }
            Self::Misaligned { offset, alignment } => {
                write!(
                    formatter,
                    "region heap offset {offset} is not aligned to {alignment}"
                )
            }
            Self::OutOfRange {
                offset,
                length,
                heap_end,
                arena_len,
            } => write!(
                formatter,
                "region heap access [{offset}, {}) exceeds heap end {heap_end} or arena {arena_len} bytes",
                offset.saturating_add(length)
            ),
            Self::NotAllocated { offset } => {
                write!(
                    formatter,
                    "region heap offset {offset} is not a live allocation"
                )
            }
            Self::DoubleFree { offset } => {
                write!(
                    formatter,
                    "region heap block of offset {offset} is already free"
                )
            }
            Self::BlockCorruption {
                offset,
                declared_size,
                previous_size,
                heap_end,
            } => write!(
                formatter,
                "region heap block at {offset} is corrupt: size {declared_size}, previous {previous_size}, heap end {heap_end}"
            ),
            Self::BinCorruption { bin, offset } => {
                write!(
                    formatter,
                    "region heap bin {bin} does not contain free block {offset}"
                )
            }
        }
    }
}

/// Shared block allocator for one region heap.
///
/// The descriptor is a field of the region header; it is never one of its own
/// blocks and it has no destructor. Only region creation initializes it, only
/// the owning region serializes it, and the whole value disappears with the
/// segment mapping.
#[repr(C)]
#[derive(Debug)]
pub struct SvmRegionHeap {
    free_bins: [u64; SVM_REGION_HEAP_BINS],
    heap_start: u64,
    heap_end: u64,
    free_bytes: u64,
    used_bytes: u64,
    peak_used_bytes: u64,
}

impl SvmRegionHeap {
    /// Creates an uninitialized descriptor; all methods terminate until
    /// [`Self::initialize`] has run.
    pub const fn new() -> Self {
        Self {
            free_bins: [0; SVM_REGION_HEAP_BINS],
            heap_start: 0,
            heap_end: 0,
            free_bytes: 0,
            used_bytes: 0,
            peak_used_bytes: 0,
        }
    }

    /// Turns `[heap_start, heap_end)` of `arena` into one free block.
    pub fn initialize(&mut self, arena: &mut [u8], heap_start: u64, heap_end: u64) {
        self.initialize_checked(arena, heap_start, heap_end)
            .unwrap_or_else(|violation| violation.terminate());
    }

    /// Allocates `layout` and returns its offset; the returned range is zeroed.
    pub fn allocate(&mut self, arena: &mut [u8], layout: Layout) -> u64 {
        self.allocate_checked(arena, layout)
            .unwrap_or_else(|violation| violation.terminate())
    }

    /// Resizes the live allocation at `offset`; the offset changes only when
    /// the block must grow.
    pub fn reallocate(
        &mut self,
        arena: &mut [u8],
        offset: u64,
        layout: Layout,
        new_size: usize,
    ) -> u64 {
        self.reallocate_checked(arena, offset, layout, new_size)
            .unwrap_or_else(|violation| violation.terminate())
    }

    /// Releases the live allocation at `offset`.
    pub fn deallocate(&mut self, arena: &mut [u8], offset: u64, layout: Layout) {
        self.deallocate_checked(arena, offset, layout)
            .unwrap_or_else(|violation| violation.terminate());
    }

    /// Reports whether `offset` is the start of a live allocation that
    /// satisfies `layout`. As in [`Self::deallocate`], `layout` must be the
    /// layout the block was allocated with: the user offset is re-derived from
    /// the block header and compared, so an interior offset is rejected even
    /// when its own range would fit.
    pub fn holds(&self, arena: &[u8], offset: u64, layout: Layout) -> bool {
        let alignment = (layout.align() as u64).max(MIN_ALIGNMENT);
        let size = layout.size() as u64;
        if !offset.is_multiple_of(alignment) {
            return false;
        }
        match self.locate_block(arena, offset) {
            Ok((block, block_size)) => {
                let user = match align_up(block + BLOCK_HEADER_SIZE + USER_PREFIX_SIZE, alignment) {
                    Ok(user) => user,
                    Err(_) => return false,
                };
                user == offset
                    && offset
                        .checked_add(size)
                        .is_some_and(|end| end <= block + block_size)
            }
            Err(_) => false,
        }
    }

    /// Free bytes tracked in block units; `used_bytes + free_bytes` always
    /// equals `heap_end - heap_start`.
    pub fn free_bytes(&self) -> u64 {
        self.free_bytes
    }

    /// Bytes currently held by live blocks, including block headers.
    pub fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    /// High-water mark of [`Self::used_bytes`].
    pub fn peak_used_bytes(&self) -> u64 {
        self.peak_used_bytes
    }

    /// Borrows `length` initialized bytes of the live allocation at `offset`.
    pub fn bytes_at<'a>(&self, arena: &'a [u8], offset: u64, length: u64) -> &'a [u8] {
        self.bytes_checked(arena, offset, length)
            .unwrap_or_else(|violation| violation.terminate())
    }

    /// Mutable form of [`Self::bytes_at`].
    pub fn bytes_at_mut<'a>(
        &mut self,
        arena: &'a mut [u8],
        offset: u64,
        length: u64,
    ) -> &'a mut [u8] {
        if let Err(violation) = self.bytes_checked(arena, offset, length) {
            violation.terminate();
        }
        let end = offset as usize + length as usize;
        &mut arena[offset as usize..end]
    }

    pub(crate) fn initialize_checked(
        &mut self,
        arena: &mut [u8],
        heap_start: u64,
        heap_end: u64,
    ) -> Result<(), SvmRegionHeapViolation> {
        let arena_len = arena.len() as u64;
        let usable = heap_start.is_multiple_of(MIN_ALIGNMENT)
            && heap_end.is_multiple_of(MIN_ALIGNMENT)
            && heap_start
                .checked_add(MIN_BLOCK_SIZE)
                .is_some_and(|minimum| minimum <= heap_end)
            && heap_end <= arena_len;
        if !usable {
            return Err(SvmRegionHeapViolation::InvalidRange {
                heap_start,
                heap_end,
                arena_len,
            });
        }

        self.free_bins = [0; SVM_REGION_HEAP_BINS];
        self.heap_start = heap_start;
        self.heap_end = heap_end;
        self.free_bytes = heap_end - heap_start;
        self.used_bytes = 0;
        self.peak_used_bytes = 0;

        let size = heap_end - heap_start;
        self.store(arena, heap_start, 0)?;
        self.store(arena, heap_start + 8, size | FLAG_PREVIOUS_IN_USE)?;
        self.push_free(arena, heap_start)
    }

    pub(crate) fn allocate_checked(
        &mut self,
        arena: &mut [u8],
        layout: Layout,
    ) -> Result<u64, SvmRegionHeapViolation> {
        self.require_initialized(arena)?;
        let alignment = (layout.align() as u64).max(MIN_ALIGNMENT);
        let size = layout.size() as u64;
        let overhead = BLOCK_HEADER_SIZE + USER_PREFIX_SIZE;
        let minimum = overhead
            .checked_add(size)
            .ok_or(SvmRegionHeapViolation::LayoutOverflow { size, alignment })?;

        for bin in bin_for(minimum)..SVM_REGION_HEAP_BINS {
            let mut cursor = self.free_bins[bin];
            while cursor != 0 {
                let block_size = self.block_size(arena, cursor)?;
                let next = self.load(arena, chain_next_word(cursor, block_size))?;
                if let Some(total) = self.block_total(cursor, block_size, alignment, size)? {
                    self.unlink_free(arena, cursor)?;
                    let user = self.carve(arena, cursor, block_size, total, alignment)?;
                    self.store(arena, user - USER_PREFIX_SIZE, cursor)?;
                    let end = (cursor + total) as usize;
                    arena[user as usize..end].fill(0);
                    self.used_bytes += total;
                    self.free_bytes -= total;
                    self.peak_used_bytes = self.peak_used_bytes.max(self.used_bytes);
                    return Ok(user);
                }
                cursor = next;
            }
        }

        Err(SvmRegionHeapViolation::Exhausted {
            requested: size,
            alignment,
            free_bytes: self.free_bytes,
        })
    }

    pub(crate) fn reallocate_checked(
        &mut self,
        arena: &mut [u8],
        offset: u64,
        layout: Layout,
        new_size: usize,
    ) -> Result<u64, SvmRegionHeapViolation> {
        self.require_initialized(arena)?;
        if new_size == 0 {
            return Err(SvmRegionHeapViolation::ZeroSizeReallocation { offset });
        }
        let alignment = (layout.align() as u64).max(MIN_ALIGNMENT);
        let new_layout = Layout::from_size_align(new_size, layout.align()).map_err(|_| {
            SvmRegionHeapViolation::LayoutOverflow {
                size: new_size as u64,
                alignment,
            }
        })?;

        let (block, block_size) = self.locate_block(arena, offset)?;
        let usable = block + block_size - offset;
        if new_size as u64 <= usable {
            let user_end = offset.checked_add(new_size as u64).ok_or(
                SvmRegionHeapViolation::LayoutOverflow {
                    size: new_size as u64,
                    alignment,
                },
            )?;
            let keep = align_up(user_end, MIN_ALIGNMENT)?;
            let tail = block + block_size - keep;
            // Both halves must stay block-sized: a live block below
            // `MIN_BLOCK_SIZE` could not hold the free-list links once it is
            // released, and `block_size` rejects such a header as corrupt.
            if tail >= MIN_BLOCK_SIZE && keep - block >= MIN_BLOCK_SIZE {
                self.shrink_block(arena, block, block_size, keep, tail)?;
            }
            return Ok(offset);
        }

        let replacement = self.allocate_checked(arena, new_layout)?;
        let copy = usable.min(new_size as u64) as usize;
        let source = offset as usize;
        let destination = replacement as usize;
        arena.copy_within(source..source + copy, destination);
        self.deallocate_checked(arena, offset, layout)?;
        Ok(replacement)
    }

    pub(crate) fn deallocate_checked(
        &mut self,
        arena: &mut [u8],
        offset: u64,
        layout: Layout,
    ) -> Result<(), SvmRegionHeapViolation> {
        self.require_initialized(arena)?;
        let alignment = (layout.align() as u64).max(MIN_ALIGNMENT);
        if !offset.is_multiple_of(alignment) {
            return Err(SvmRegionHeapViolation::Misaligned { offset, alignment });
        }
        let block = self.block_of_user(arena, offset)?;
        let flags = self.header(arena, block)?;
        if flags & FLAG_CURRENT_IN_USE == 0 {
            return Err(SvmRegionHeapViolation::DoubleFree { offset });
        }
        if align_up(block + BLOCK_HEADER_SIZE + USER_PREFIX_SIZE, alignment)? != offset {
            return Err(SvmRegionHeapViolation::NotAllocated { offset });
        }

        let freed = self.block_size(arena, block)?;
        self.store(arena, block + 8, freed | (flags & FLAG_PREVIOUS_IN_USE))?;

        let mut merged = freed;
        let next = block + freed;
        if next < self.heap_end {
            let next_flags = self.header(arena, next)?;
            if next_flags & FLAG_CURRENT_IN_USE == 0 {
                merged += self.block_size(arena, next)?;
                self.unlink_free(arena, next)?;
            }
        }

        let mut start = block;
        let previous_in_use = if flags & FLAG_PREVIOUS_IN_USE == 0 {
            let previous_size = self.previous_size(arena, block)?;
            if previous_size < MIN_BLOCK_SIZE
                || !previous_size.is_multiple_of(MIN_ALIGNMENT)
                || previous_size > block - self.heap_start
            {
                return Err(SvmRegionHeapViolation::BlockCorruption {
                    offset: block,
                    declared_size: previous_size,
                    previous_size,
                    heap_end: self.heap_end,
                });
            }
            let previous = block - previous_size;
            let previous_flags = self.header(arena, previous)?;
            if previous_flags & FLAG_CURRENT_IN_USE != 0
                || (previous_flags & !FLAG_MASK) != previous_size
            {
                return Err(SvmRegionHeapViolation::BlockCorruption {
                    offset: previous,
                    declared_size: previous_flags & !FLAG_MASK,
                    previous_size,
                    heap_end: self.heap_end,
                });
            }
            self.unlink_free(arena, previous)?;
            start = previous;
            merged += previous_size;
            previous_flags & FLAG_PREVIOUS_IN_USE
        } else {
            FLAG_PREVIOUS_IN_USE
        };

        self.store(arena, start + 8, merged | previous_in_use)?;
        self.record_next_block(arena, start, merged)?;
        self.push_free(arena, start)?;
        self.free_bytes += freed;
        self.used_bytes -= freed;
        Ok(())
    }

    fn bytes_checked<'a>(
        &self,
        arena: &'a [u8],
        offset: u64,
        length: u64,
    ) -> Result<&'a [u8], SvmRegionHeapViolation> {
        let (block, block_size) = self.locate_block(arena, offset)?;
        let end = offset
            .checked_add(length)
            .ok_or(SvmRegionHeapViolation::OutOfRange {
                offset,
                length,
                heap_end: self.heap_end,
                arena_len: arena.len() as u64,
            })?;
        if end > block + block_size {
            return Err(SvmRegionHeapViolation::OutOfRange {
                offset,
                length,
                heap_end: block + block_size,
                arena_len: arena.len() as u64,
            });
        }
        Ok(&arena[offset as usize..end as usize])
    }

    /// Splits the free tail of a shrunk block back into the free list.
    fn shrink_block(
        &mut self,
        arena: &mut [u8],
        block: u64,
        block_size: u64,
        keep: u64,
        tail: u64,
    ) -> Result<(), SvmRegionHeapViolation> {
        let flags = self.header(arena, block)?;
        self.store(
            arena,
            block + 8,
            (keep - block) | FLAG_CURRENT_IN_USE | (flags & FLAG_PREVIOUS_IN_USE),
        )?;

        let mut tail_size = tail;
        let after = block + block_size;
        if after < self.heap_end {
            let after_flags = self.header(arena, after)?;
            if after_flags & FLAG_CURRENT_IN_USE == 0 {
                tail_size += self.block_size(arena, after)?;
                self.unlink_free(arena, after)?;
            }
        }

        self.store(arena, keep, keep - block)?;
        self.store(arena, keep + 8, tail_size | FLAG_PREVIOUS_IN_USE)?;
        self.record_next_block(arena, keep, tail_size)?;
        self.push_free(arena, keep)?;
        self.free_bytes += tail;
        self.used_bytes -= tail;
        Ok(())
    }

    /// Rewrites the physically following block's header for a `block` of
    /// `size` bytes that just changed state: its `previous_size` becomes
    /// `size`, and its `FLAG_PREVIOUS_IN_USE` becomes the inverse of `block`'s
    /// `FLAG_CURRENT_IN_USE`, exactly as dlmalloc's `PREV_INUSE` is maintained.
    fn record_next_block(
        &mut self,
        arena: &mut [u8],
        block: u64,
        size: u64,
    ) -> Result<(), SvmRegionHeapViolation> {
        let next = block + size;
        if next >= self.heap_end {
            return Ok(());
        }
        let flags = self.header(arena, next)?;
        self.store(arena, next, size)?;
        let flags = if self.header(arena, block)? & FLAG_CURRENT_IN_USE != 0 {
            flags | FLAG_PREVIOUS_IN_USE
        } else {
            flags & !FLAG_PREVIOUS_IN_USE
        };
        self.store(arena, next + 8, flags)
    }

    /// Total block bytes needed to place `size` bytes at `alignment` in `block`.
    fn block_total(
        &self,
        block: u64,
        block_size: u64,
        alignment: u64,
        size: u64,
    ) -> Result<Option<u64>, SvmRegionHeapViolation> {
        let user = align_up(block + BLOCK_HEADER_SIZE + USER_PREFIX_SIZE, alignment)?;
        let needed = align_up(user - block + size, MIN_ALIGNMENT)?.max(MIN_BLOCK_SIZE);
        if needed > block_size {
            return Ok(None);
        }
        if block_size - needed < MIN_BLOCK_SIZE {
            return Ok(Some(block_size));
        }
        Ok(Some(needed))
    }

    /// Marks `block..block+total` in use, returning the user offset.
    fn carve(
        &mut self,
        arena: &mut [u8],
        block: u64,
        block_size: u64,
        total: u64,
        alignment: u64,
    ) -> Result<u64, SvmRegionHeapViolation> {
        let user = align_up(block + BLOCK_HEADER_SIZE + USER_PREFIX_SIZE, alignment)?;
        let flags = self.header(arena, block)?;
        let previous_in_use = flags & FLAG_PREVIOUS_IN_USE;
        let remainder = block_size - total;
        if remainder >= MIN_BLOCK_SIZE {
            self.store(
                arena,
                block + 8,
                total | FLAG_CURRENT_IN_USE | previous_in_use,
            )?;
            let next = block + total;
            self.store(arena, next, total)?;
            self.store(arena, next + 8, remainder | FLAG_PREVIOUS_IN_USE)?;
            self.push_free(arena, next)?;
            self.record_next_block(arena, next, remainder)?;
        } else {
            self.store(
                arena,
                block + 8,
                block_size | FLAG_CURRENT_IN_USE | previous_in_use,
            )?;
            self.record_next_block(arena, block, block_size)?;
        }
        Ok(user)
    }

    fn push_free(&mut self, arena: &mut [u8], block: u64) -> Result<(), SvmRegionHeapViolation> {
        let size = self.block_size(arena, block)?;
        let bin = bin_for(size);
        let head = self.free_bins[bin];
        self.store(arena, chain_next_word(block, size), head)?;
        self.store(arena, chain_previous_word(block, size), 0)?;
        if head != 0 {
            let head_size = self.block_size(arena, head)?;
            self.store(arena, chain_previous_word(head, head_size), block)?;
        }
        self.free_bins[bin] = block;
        Ok(())
    }

    fn unlink_free(&mut self, arena: &mut [u8], block: u64) -> Result<(), SvmRegionHeapViolation> {
        let size = self.block_size(arena, block)?;
        let bin = bin_for(size);
        let next = self.load(arena, chain_next_word(block, size))?;
        let previous = self.load(arena, chain_previous_word(block, size))?;
        if previous == 0 {
            if self.free_bins[bin] != block {
                return Err(SvmRegionHeapViolation::BinCorruption {
                    bin: bin as u8,
                    offset: block,
                });
            }
            self.free_bins[bin] = next;
        } else {
            if self.header(arena, previous)? & FLAG_CURRENT_IN_USE != 0 {
                return Err(SvmRegionHeapViolation::BinCorruption {
                    bin: bin as u8,
                    offset: block,
                });
            }
            let previous_size = self.block_size(arena, previous)?;
            self.store(arena, chain_next_word(previous, previous_size), next)?;
        }
        if next != 0 {
            let next_size = self.block_size(arena, next)?;
            self.store(arena, chain_previous_word(next, next_size), previous)?;
        }
        self.store(arena, chain_next_word(block, size), 0)?;
        self.store(arena, chain_previous_word(block, size), 0)
    }

    fn require_initialized(&self, arena: &[u8]) -> Result<(), SvmRegionHeapViolation> {
        let usable = self.heap_end != 0
            && self.heap_start.is_multiple_of(MIN_ALIGNMENT)
            && self.heap_start + MIN_BLOCK_SIZE <= self.heap_end
            && self.heap_end <= arena.len() as u64;
        if usable {
            Ok(())
        } else {
            Err(SvmRegionHeapViolation::NotInitialized {
                heap_start: self.heap_start,
                heap_end: self.heap_end,
                arena_len: arena.len() as u64,
            })
        }
    }

    /// Recovers the block that owns `offset` from the back-pointer word.
    fn locate_block(
        &self,
        arena: &[u8],
        offset: u64,
    ) -> Result<(u64, u64), SvmRegionHeapViolation> {
        self.require_initialized(arena)?;
        let block = self.block_of_user(arena, offset)?;
        let flags = self.header(arena, block)?;
        if flags & FLAG_CURRENT_IN_USE == 0 {
            return Err(SvmRegionHeapViolation::NotAllocated { offset });
        }
        let block_size = self.block_size(arena, block)?;
        if offset >= block + block_size {
            return Err(SvmRegionHeapViolation::NotAllocated { offset });
        }
        Ok((block, block_size))
    }

    fn block_of_user(&self, arena: &[u8], offset: u64) -> Result<u64, SvmRegionHeapViolation> {
        let first = self.heap_start + BLOCK_HEADER_SIZE + USER_PREFIX_SIZE;
        let in_range = offset
            .checked_add(USER_PREFIX_SIZE)
            .is_some_and(|end| offset >= first && end <= self.heap_end);
        if !in_range {
            return Err(SvmRegionHeapViolation::NotAllocated { offset });
        }
        let block = self.load(arena, offset - USER_PREFIX_SIZE)?;
        if block < self.heap_start || !block.is_multiple_of(MIN_ALIGNMENT) {
            return Err(SvmRegionHeapViolation::NotAllocated { offset });
        }
        Ok(block)
    }

    #[inline]
    fn header(&self, arena: &[u8], block: u64) -> Result<u64, SvmRegionHeapViolation> {
        self.load(arena, block + 8)
    }

    #[inline]
    fn previous_size(&self, arena: &[u8], block: u64) -> Result<u64, SvmRegionHeapViolation> {
        self.load(arena, block)
    }

    fn block_size(&self, arena: &[u8], block: u64) -> Result<u64, SvmRegionHeapViolation> {
        let flags = self.header(arena, block)?;
        let size = flags & !FLAG_MASK;
        if size < MIN_BLOCK_SIZE
            || !size.is_multiple_of(MIN_ALIGNMENT)
            || block
                .checked_add(size)
                .is_none_or(|end| end > self.heap_end)
        {
            return Err(SvmRegionHeapViolation::BlockCorruption {
                offset: block,
                declared_size: size,
                previous_size: self.previous_size(arena, block).unwrap_or(0),
                heap_end: self.heap_end,
            });
        }
        Ok(size)
    }

    #[inline]
    fn load(&self, arena: &[u8], offset: u64) -> Result<u64, SvmRegionHeapViolation> {
        let Some(end) = offset.checked_add(USER_PREFIX_SIZE) else {
            return Err(SvmRegionHeapViolation::OutOfRange {
                offset,
                length: USER_PREFIX_SIZE,
                heap_end: self.heap_end,
                arena_len: arena.len() as u64,
            });
        };
        if end > self.heap_end || end > arena.len() as u64 {
            return Err(SvmRegionHeapViolation::OutOfRange {
                offset,
                length: USER_PREFIX_SIZE,
                heap_end: self.heap_end,
                arena_len: arena.len() as u64,
            });
        }
        let start = offset as usize;
        let mut word = [0u8; 8];
        word.copy_from_slice(&arena[start..start + 8]);
        Ok(u64::from_ne_bytes(word))
    }

    #[inline]
    fn store(
        &self,
        arena: &mut [u8],
        offset: u64,
        value: u64,
    ) -> Result<(), SvmRegionHeapViolation> {
        let Some(end) = offset.checked_add(USER_PREFIX_SIZE) else {
            return Err(SvmRegionHeapViolation::OutOfRange {
                offset,
                length: USER_PREFIX_SIZE,
                heap_end: self.heap_end,
                arena_len: arena.len() as u64,
            });
        };
        if end > self.heap_end || end > arena.len() as u64 {
            return Err(SvmRegionHeapViolation::OutOfRange {
                offset,
                length: USER_PREFIX_SIZE,
                heap_end: self.heap_end,
                arena_len: arena.len() as u64,
            });
        }
        let start = offset as usize;
        arena[start..start + 8].copy_from_slice(&value.to_ne_bytes());
        Ok(())
    }
}

impl Default for SvmRegionHeap {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn bin_for(size: u64) -> usize {
    let exponent = u64::BITS - 1 - size.leading_zeros();
    let index = exponent.saturating_sub(MIN_BIN_EXPONENT) as usize;
    index.min(SVM_REGION_HEAP_BINS - 1)
}

#[inline]
fn align_up(value: u64, alignment: u64) -> Result<u64, SvmRegionHeapViolation> {
    let Some(aligned) = value
        .checked_add(alignment - 1)
        .map(|sum| sum & !(alignment - 1))
    else {
        return Err(SvmRegionHeapViolation::LayoutOverflow {
            size: value,
            alignment,
        });
    };
    Ok(aligned)
}

use std::cell::UnsafeCell;
use std::fmt;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::pool::{Index as PoolIndex, Pool};
use crate::rbtree::RbTree;
use crate::segment::Segment;

#[repr(C)]
struct Chunk {
    start_byte: u32,
    length: AtomicU32,
    next: AtomicU64,
}

const CHUNK_HEADER_SIZE: usize = std::mem::size_of::<Chunk>();

#[repr(C, align(64))]
pub struct FifoHeader {
    free_chunk: AtomicU64,
    size: u32,
    min_alloc: u32,
    has_event: AtomicU32,
    want_ntf: AtomicU32,
    want_deq_ntf: AtomicU32,
    has_deq_ntf: AtomicU32,
    deq_thresh: AtomicU32,
    _pad0: [u8; 64 - (8 + 4 + 4 + 4 + 4 + 4 + 4 + 4)],
    head_chunk: AtomicU64,
    head: AtomicU32,
    _pad1: [u8; 64 - (8 + 4)],
    tail_chunk: AtomicU64,
    tail: AtomicU32,
    _pad2: [u8; 64 - (8 + 4)],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FifoError {
    InvalidCapacity,
    CapacityOutOfRange { capacity: usize },
    SegmentExhausted,
    OutOfOrderDisabled,
    OutOfOrderLengthOutOfRange { length: usize },
    OutOfOrderOffsetOverflow { offset: u32, length: u32 },
    OutOfOrderCapacityExceeded { end_offset: u32, available: usize },
}

impl fmt::Display for FifoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => {
                f.write_str("FIFO capacity must be a power of two and at least 2")
            }
            Self::CapacityOutOfRange { capacity } => {
                write!(
                    f,
                    "FIFO capacity {capacity} exceeds the shared layout range"
                )
            }
            Self::SegmentExhausted => {
                f.write_str("segment has insufficient space for FIFO storage")
            }
            Self::OutOfOrderDisabled => f.write_str("out-of-order FIFO delivery is disabled"),
            Self::OutOfOrderLengthOutOfRange { length } => {
                write!(f, "out-of-order FIFO length {length} exceeds u32")
            }
            Self::OutOfOrderOffsetOverflow { offset, length } => {
                write!(
                    f,
                    "out-of-order FIFO offset {offset} plus length {length} overflows u32"
                )
            }
            Self::OutOfOrderCapacityExceeded {
                end_offset,
                available,
            } => {
                write!(
                    f,
                    "out-of-order FIFO end offset {end_offset} exceeds available capacity {available}"
                )
            }
        }
    }
}

impl std::error::Error for FifoError {}

pub struct OooResult {
    pub accepted: u32,
    pub delivered: u32,
    pub start: Option<u32>,
    pub len: u32,
}

struct OooSegment {
    offset: u32,
    len: u32,
}

struct OooBookkeeping {
    base: u32,
    entries: Pool<OooSegment>,
    index: RbTree<u32, PoolIndex>,
}

impl OooBookkeeping {
    fn remove_ooo_entry(&mut self, offset: u32) -> Option<OooSegment> {
        let idx = self.index.remove(&offset)?;
        self.entries.remove(idx)
    }
}

pub struct Fifo {
    seg: Segment,
    base: *mut u8,
    hdr: *mut FifoHeader,
    hdr_off: u64,
    ooo: UnsafeCell<Option<Box<OooBookkeeping>>>,
}

unsafe impl Send for Fifo {}
unsafe impl Sync for Fifo {}

impl Fifo {
    const fn chunk_data_size(capacity: usize) -> usize {
        if capacity < 4096 { capacity } else { 4096 }
    }

    pub const fn layout_bytes(capacity: usize) -> Result<usize, FifoError> {
        if capacity < 2 || !capacity.is_power_of_two() {
            return Err(FifoError::InvalidCapacity);
        }
        if capacity > u32::MAX as usize {
            return Err(FifoError::CapacityOutOfRange { capacity });
        }
        let chunk_data_size = Self::chunk_data_size(capacity);
        let chunk_count = capacity / chunk_data_size;
        let Some(chunks_bytes) = chunk_count.checked_mul(CHUNK_HEADER_SIZE + chunk_data_size)
        else {
            return Err(FifoError::CapacityOutOfRange { capacity });
        };
        let Some(bytes) = std::mem::size_of::<FifoHeader>().checked_add(chunks_bytes) else {
            return Err(FifoError::CapacityOutOfRange { capacity });
        };
        Ok(bytes)
    }

    pub fn new(seg: Segment, capacity: usize) -> Result<Self, FifoError> {
        let bytes = Self::layout_bytes(capacity)?;
        let hdr_off = seg.alloc(bytes, 64).ok_or(FifoError::SegmentExhausted)?;
        unsafe { Self::init_at(seg, hdr_off, capacity) }
    }

    /// Initialise a [`Fifo`] header at a pre-allocated offset in `seg`.
    /// The caller must guarantee that `seg` has [`Self::layout_bytes`] bytes
    /// available at `hdr_offset` and that no other [`Fifo`] uses the same
    /// region.
    pub unsafe fn init_at(
        seg: Segment,
        hdr_offset: u64,
        capacity: usize,
    ) -> Result<Self, FifoError> {
        let layout = Self::layout_bytes(capacity)?;
        let offset = usize::try_from(hdr_offset).expect("FIFO offset exceeds usize");
        let end = offset
            .checked_add(layout)
            .expect("FIFO layout end overflows usize");
        assert!(end <= seg.size(), "FIFO layout exceeds segment bounds");
        let base = seg.base();
        let hdr = unsafe { base.add(offset) as *mut FifoHeader };
        let chunk_size = Self::chunk_data_size(capacity);
        let chunk_count = capacity / chunk_size;
        let first_chunk_off = hdr_offset + std::mem::size_of::<FifoHeader>() as u64;
        let chunk_stride = (CHUNK_HEADER_SIZE + chunk_size) as u64;
        let free_chunk = if chunk_count > 1 {
            first_chunk_off + chunk_stride
        } else {
            0
        };
        unsafe {
            std::ptr::write(
                hdr,
                FifoHeader {
                    free_chunk: AtomicU64::new(free_chunk),
                    size: capacity as u32,
                    min_alloc: chunk_size as u32,
                    has_event: AtomicU32::new(0),
                    want_ntf: AtomicU32::new(0),
                    want_deq_ntf: AtomicU32::new(0),
                    has_deq_ntf: AtomicU32::new(0),
                    deq_thresh: AtomicU32::new(0),
                    _pad0: [0; 64 - (8 + 4 + 4 + 4 + 4 + 4 + 4 + 4)],
                    head_chunk: AtomicU64::new(first_chunk_off),
                    head: AtomicU32::new(0),
                    _pad1: [0; 64 - (8 + 4)],
                    tail_chunk: AtomicU64::new(first_chunk_off),
                    tail: AtomicU32::new(0),
                    _pad2: [0; 64 - (8 + 4)],
                },
            );
            for index in 0..chunk_count {
                let chunk_off = first_chunk_off + index as u64 * chunk_stride;
                let next = if index + 1 < chunk_count {
                    chunk_off + chunk_stride
                } else {
                    0
                };
                std::ptr::write(
                    base.add(chunk_off as usize) as *mut Chunk,
                    Chunk {
                        start_byte: 0,
                        length: AtomicU32::new(0),
                        next: AtomicU64::new(if index == 0 { 0 } else { next }),
                    },
                );
            }
        }
        Ok(Self {
            seg,
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo: UnsafeCell::new(None),
        })
    }

    /// Offset of the [`FifoHeader`] within the backing [`Segment`].
    /// Used by `from_shared` to reconstruct the same FIFO in another
    /// process that shares the segment.
    #[inline]
    pub fn hdr_offset(&self) -> u64 {
        self.hdr_off
    }

    unsafe fn acquire_chunk(&self, start_byte: u32) -> Option<u64> {
        let free_chunk = unsafe { &(*self.hdr).free_chunk };
        let mut chunk_off = free_chunk.load(Ordering::Acquire);
        while chunk_off != 0 {
            let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
            let next = chunk.next.load(Ordering::Relaxed);
            match free_chunk.compare_exchange_weak(
                chunk_off,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    unsafe {
                        std::ptr::write(
                            self.base.add(chunk_off as usize).cast::<Chunk>(),
                            Chunk {
                                start_byte,
                                length: AtomicU32::new(0),
                                next: AtomicU64::new(0),
                            },
                        );
                    }
                    return Some(chunk_off);
                }
                Err(current) => chunk_off = current,
            }
        }
        None
    }

    unsafe fn release_chunk(&self, chunk_off: u64) {
        let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
        let free_chunk = unsafe { &(*self.hdr).free_chunk };
        let mut head = free_chunk.load(Ordering::Acquire);
        loop {
            chunk.next.store(head, Ordering::Relaxed);
            match free_chunk.compare_exchange_weak(
                head,
                chunk_off,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(current) => head = current,
            }
        }
    }

    #[inline]
    pub fn enqueue(&self, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Acquire);
            let tail = (*hdr).tail.load(Ordering::Relaxed);
            if head == tail {
                self.prepare_empty_tail_chunk(tail);
            }
            let used = tail.wrapping_sub(head);
            let free = ((*hdr).size - used) as usize;
            let to_write = src.len().min(free);
            if to_write == 0 {
                return 0;
            }
            let written = self.append_at_tail_without_tail_store(tail, &src[..to_write]);
            if written == 0 {
                return 0;
            }
            let new_tail = tail.wrapping_add(written as u32);
            (*hdr).tail.store(new_tail, Ordering::Release);
            let collected = self.promote_contiguous_from(new_tail);
            (*hdr)
                .tail
                .store(new_tail.wrapping_add(collected), Ordering::Release);
            written + collected as usize
        }
    }

    #[inline]
    pub fn peek(&self, offset: usize, len: usize, dst: &mut [u8]) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let available = tail.wrapping_sub(head) as usize;
            if offset >= available {
                return 0;
            }
            let to_copy = len.min(dst.len()).min(available - offset);
            if to_copy == 0 {
                return 0;
            }
            let logical_pos = head + offset as u32;
            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            let mut copied = 0usize;
            let mut pos = logical_pos;
            while copied < to_copy && chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte + chunk.length.load(Ordering::Relaxed);
                if pos >= chunk.start_byte && pos < chunk_end {
                    let data_off = (pos - chunk.start_byte) as usize;
                    let chunk_avail = (chunk_end - pos) as usize;
                    let to_read = (to_copy - copied).min(chunk_avail);
                    let chunk_data = self.base.add(chunk_off as usize + CHUNK_HEADER_SIZE);
                    std::ptr::copy_nonoverlapping(
                        chunk_data.add(data_off),
                        dst.as_mut_ptr().add(copied),
                        to_read,
                    );
                    copied += to_read;
                    pos += to_read as u32;
                }
                chunk_off = chunk.next.load(Ordering::Acquire);
            }
            copied
        }
    }

    #[inline]
    pub fn peek_segments<R>(
        &self,
        offset: usize,
        len: usize,
        f: impl FnOnce(&[u8], &[u8]) -> R,
    ) -> Option<R> {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let available = tail.wrapping_sub(head) as usize;
            if offset >= available {
                return None;
            }
            let to_read = len.min(available - offset);
            if to_read == 0 {
                return Some(f(&[], &[]));
            }
            let logical_pos = head + offset as u32;
            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte + chunk.length.load(Ordering::Relaxed);
                if logical_pos >= chunk.start_byte && logical_pos < chunk_end {
                    let data_off = (logical_pos - chunk.start_byte) as usize;
                    let chunk_avail = (chunk_end - logical_pos) as usize;
                    let first_len = to_read.min(chunk_avail);
                    let first_slice = std::slice::from_raw_parts(
                        self.base
                            .add(chunk_off as usize + CHUNK_HEADER_SIZE + data_off),
                        first_len,
                    );
                    if first_len == to_read {
                        return Some(f(first_slice, &[]));
                    }
                    let second_len = to_read - first_len;
                    let next_off = chunk.next.load(Ordering::Acquire);
                    if next_off != 0 {
                        let next_chunk = &*(self.base.add(next_off as usize) as *mut Chunk);
                        let second_avail = next_chunk.length.load(Ordering::Relaxed) as usize;
                        let second_actual = second_len.min(second_avail);
                        let second_slice = std::slice::from_raw_parts(
                            self.base.add(next_off as usize + CHUNK_HEADER_SIZE),
                            second_actual,
                        );
                        return Some(f(first_slice, second_slice));
                    }
                    return Some(f(first_slice, &[]));
                }
                chunk_off = chunk.next.load(Ordering::Acquire);
            }
            None
        }
    }

    #[inline]
    pub fn dequeue_drop(&self, len: usize) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let available = tail.wrapping_sub(head) as usize;
            let to_drop = len.min(available);
            if to_drop == 0 {
                return 0;
            }
            let new_head = head + to_drop as u32;
            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                if new_head >= chunk.start_byte + chunk.length.load(Ordering::Relaxed) {
                    if chunk_off == (*hdr).tail_chunk.load(Ordering::Acquire) {
                        break;
                    }
                    let next_off = chunk.next.load(Ordering::Acquire);
                    if next_off == 0 {
                        break;
                    }
                    (*hdr).head_chunk.store(next_off, Ordering::Release);
                    self.release_chunk(chunk_off);
                    chunk_off = next_off;
                } else {
                    break;
                }
            }
            (*hdr).head.store(new_head, Ordering::Release);
            to_drop
        }
    }

    // Rebase after observing the released head without discarding future OOO bytes.
    unsafe fn prepare_empty_tail_chunk(&self, tail: u32) {
        let chunk_off = unsafe { (*self.hdr).tail_chunk.load(Ordering::Relaxed) };
        if chunk_off == 0 {
            return;
        }
        let chunk = unsafe { &mut *self.base.add(chunk_off as usize).cast::<Chunk>() };
        let visible_len = tail.wrapping_sub(chunk.start_byte);
        if chunk.start_byte != tail && chunk.length.load(Ordering::Relaxed) <= visible_len {
            chunk.start_byte = tail;
            chunk.length.store(0, Ordering::Relaxed);
        }
    }

    fn append_at_tail_without_tail_store(&self, offset: u32, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let chunk_data_size = (*hdr).min_alloc as usize;
            let mut written = 0usize;
            let mut remaining_offset = offset;
            let mut remaining_src = src;
            let mut chunk_off = (*hdr).tail_chunk.load(Ordering::Relaxed);

            while !remaining_src.is_empty() {
                if chunk_off == 0 {
                    let Some(new_off) = self.acquire_chunk(remaining_offset) else {
                        return written;
                    };
                    (*hdr).head_chunk.store(new_off, Ordering::Release);
                    (*hdr).tail_chunk.store(new_off, Ordering::Release);
                    chunk_off = new_off;
                }

                let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte + chunk_data_size as u32;
                if remaining_offset >= chunk_end {
                    let next_off = chunk.next.load(Ordering::Acquire);
                    if next_off != 0 {
                        let next = &*(self.base.add(next_off as usize) as *mut Chunk);
                        if remaining_offset >= next.start_byte {
                            (*hdr).tail_chunk.store(next_off, Ordering::Release);
                            chunk_off = next_off;
                            continue;
                        }
                    }
                    if remaining_offset > chunk_end {
                        return written;
                    }

                    let Some(new_off) = self.acquire_chunk(remaining_offset) else {
                        return written;
                    };
                    let new_chunk = &*self.base.add(new_off as usize).cast::<Chunk>();
                    new_chunk.next.store(next_off, Ordering::Relaxed);
                    chunk.next.store(new_off, Ordering::Release);
                    (*hdr).tail_chunk.store(new_off, Ordering::Release);
                    chunk_off = new_off;
                    continue;
                }

                if remaining_offset < chunk.start_byte || remaining_offset > chunk_end {
                    return written;
                }

                let data_ptr = self.base.add(chunk_off as usize + CHUNK_HEADER_SIZE);
                let data_off = (remaining_offset - chunk.start_byte) as usize;
                let chunk_avail = chunk_data_size - data_off;
                let to_write = remaining_src.len().min(chunk_avail);
                std::ptr::copy_nonoverlapping(
                    remaining_src.as_ptr(),
                    data_ptr.add(data_off),
                    to_write,
                );
                let new_used = data_off + to_write;
                if new_used > chunk.length.load(Ordering::Relaxed) as usize {
                    chunk.length.store(new_used as u32, Ordering::Relaxed);
                }
                written += to_write;
                remaining_offset += to_write as u32;
                remaining_src = &remaining_src[to_write..];
            }
            written
        }
    }

    fn write_at_without_tail_store(&self, offset: u32, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let chunk_data_size = (*hdr).min_alloc as usize;
            let mut written = 0usize;
            let mut remaining_offset = offset;
            let mut remaining_src = src;

            let mut chunk_off = (*hdr).tail_chunk.load(Ordering::Acquire);
            let mut prev_off = 0u64;

            // Seek to the chunk covering remaining_offset
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte + chunk_data_size as u32;
                if remaining_offset >= chunk.start_byte && remaining_offset < chunk_end {
                    break;
                }
                prev_off = chunk_off;
                chunk_off = chunk.next.load(Ordering::Acquire);
            }

            // Write across chunks, taking preallocated chunks at the end.
            while !remaining_src.is_empty() {
                if chunk_off == 0 {
                    let Some(new_off) = self.acquire_chunk(remaining_offset) else {
                        return written;
                    };
                    if prev_off != 0 {
                        let prev = &mut *(self.base.add(prev_off as usize) as *mut Chunk);
                        if prev.next.load(Ordering::Acquire) == 0 {
                            prev.next.store(new_off, Ordering::Release);
                        }
                    } else {
                        (*hdr).head_chunk.store(new_off, Ordering::Release);
                        (*hdr).tail_chunk.store(new_off, Ordering::Release);
                    }
                    chunk_off = new_off;
                }

                let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte + chunk_data_size as u32;
                if remaining_offset >= chunk.start_byte && remaining_offset < chunk_end {
                    let data_ptr = self.base.add(chunk_off as usize + CHUNK_HEADER_SIZE);
                    let data_off = (remaining_offset - chunk.start_byte) as usize;
                    let chunk_avail = chunk_data_size - data_off;
                    let to_write = remaining_src.len().min(chunk_avail);
                    std::ptr::copy_nonoverlapping(
                        remaining_src.as_ptr(),
                        data_ptr.add(data_off),
                        to_write,
                    );
                    let new_used = data_off + to_write;
                    if new_used > chunk.length.load(Ordering::Relaxed) as usize {
                        chunk.length.store(new_used as u32, Ordering::Relaxed);
                    }
                    written += to_write;
                    remaining_offset += to_write as u32;
                    remaining_src = &remaining_src[to_write..];
                }

                prev_off = chunk_off;
                chunk_off = chunk.next.load(Ordering::Acquire);
            }
            written
        }
    }

    pub fn max_dequeue(&self) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            tail.wrapping_sub(head) as usize
        }
    }

    pub fn max_enqueue(&self) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Acquire);
            let tail = (*hdr).tail.load(Ordering::Relaxed);
            let used = tail.wrapping_sub(head);
            ((*hdr).size - used) as usize
        }
    }

    #[inline]
    pub fn should_signal(&self, wrote: usize) -> bool {
        if wrote == 0 {
            return false;
        }
        let hdr = self.hdr;
        unsafe {
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let tail_before = tail.wrapping_sub(wrote as u32);
            let head = (*hdr).head.load(Ordering::Acquire);
            if head != tail_before {
                return false;
            }
            loop {
                if (*hdr)
                    .want_ntf
                    .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
                if (*hdr).want_ntf.load(Ordering::Acquire) == 0 {
                    return false;
                }
            }
        }
    }

    #[inline]
    pub fn needs_deq_notification(&self, dropped: usize) -> bool {
        if dropped == 0 {
            return false;
        }
        let hdr = self.hdr;
        unsafe {
            (*hdr)
                .want_deq_ntf
                .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        }
    }

    #[inline]
    pub fn has_event(&self) -> bool {
        unsafe { (*self.hdr).has_event.load(Ordering::Acquire) != 0 }
    }

    #[inline]
    pub fn set_event(&self) -> bool {
        unsafe { (*self.hdr).has_event.swap(1, Ordering::Release) == 0 }
    }

    #[inline]
    pub fn unset_event(&self) {
        unsafe {
            (*self.hdr).has_event.store(0, Ordering::Release);
        }
    }

    #[inline]
    pub fn want_notification(&self) {
        unsafe {
            (*self.hdr).want_ntf.store(1, Ordering::Release);
        }
    }

    #[inline]
    pub fn clear_notification(&self) {
        unsafe {
            (*self.hdr).want_ntf.store(0, Ordering::Release);
        }
    }

    #[inline]
    pub fn want_deq_notification(&self) {
        unsafe {
            (*self.hdr).want_deq_ntf.store(1, Ordering::Release);
        }
    }

    #[inline]
    pub fn clear_deq_notification(&self) {
        unsafe {
            (*self.hdr).want_deq_ntf.store(0, Ordering::Release);
        }
    }

    #[inline]
    pub fn deq_threshold(&self) -> u32 {
        unsafe { (*self.hdr).deq_thresh.load(Ordering::Relaxed) }
    }

    #[inline]
    pub fn has_deq_notification(&self) -> bool {
        unsafe { (*self.hdr).has_deq_ntf.load(Ordering::Acquire) != 0 }
    }

    #[inline]
    pub fn clear_deq_notification_flag(&self) {
        unsafe {
            (*self.hdr).has_deq_ntf.store(0, Ordering::Release);
        }
    }

    pub fn clear(&self) {
        let hdr = self.hdr;
        unsafe {
            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let next_off = chunk.next.load(Ordering::Acquire);
                self.release_chunk(chunk_off);
                chunk_off = next_off;
            }
            let first_chunk = self
                .acquire_chunk(0)
                .expect("a valid FIFO always retains at least one chunk");
            (*hdr).head.store(0, Ordering::Relaxed);
            (*hdr).tail.store(0, Ordering::Relaxed);
            (*hdr).head_chunk.store(first_chunk, Ordering::Relaxed);
            (*hdr).tail_chunk.store(first_chunk, Ordering::Relaxed);
            (*hdr).has_event.store(0, Ordering::Relaxed);
            (*hdr).want_ntf.store(0, Ordering::Relaxed);
            (*hdr).want_deq_ntf.store(0, Ordering::Relaxed);
            (*hdr).has_deq_ntf.store(0, Ordering::Relaxed);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.max_dequeue() == 0
    }

    pub fn is_full(&self) -> bool {
        self.max_enqueue() == 0
    }

    pub fn segment_fd(&self) -> Option<RawFd> {
        self.seg.shared_fd()
    }

    /// Reconstruct a [`Fifo`] from a shared-memory segment at the given
    /// header offset. The caller must guarantee that the segment contains a
    /// valid, initialised `FifoHeader` at `hdr_offset`.
    pub unsafe fn from_shared(seg: Segment, hdr_offset: u64) -> Self {
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_offset as usize) as *mut FifoHeader };
        Self {
            seg,
            base,
            hdr,
            hdr_off: hdr_offset,
            ooo: UnsafeCell::new(None),
        }
    }
}

impl Fifo {
    pub fn enable_ooo(&mut self) {
        *self.ooo.get_mut() = Some(Box::new(OooBookkeeping {
            base: 0,
            entries: Pool::with_capacity(8),
            index: RbTree::with_capacity(8),
        }));
    }

    pub fn enqueue_ooo(&self, offset: u32, src: &[u8]) -> Result<OooResult, FifoError> {
        let ooo = unsafe { &mut *self.ooo.get() };
        let bk = ooo.as_mut().ok_or(FifoError::OutOfOrderDisabled)?;

        let total_len = u32::try_from(src.len())
            .map_err(|_| FifoError::OutOfOrderLengthOutOfRange { length: src.len() })?;
        let end_offset =
            offset
                .checked_add(total_len)
                .ok_or(FifoError::OutOfOrderOffsetOverflow {
                    offset,
                    length: total_len,
                })?;
        let available = self.max_enqueue();
        if end_offset as usize > available {
            return Err(FifoError::OutOfOrderCapacityExceeded {
                end_offset,
                available,
            });
        }
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
        let head = unsafe { (*self.hdr).head.load(Ordering::Acquire) };
        if head == tail {
            unsafe { self.prepare_empty_tail_chunk(tail) };
        }
        bk.base = tail;
        let base = bk.base;
        let abs_pos = tail.wrapping_add(offset);
        let written = self.write_at_without_tail_store(abs_pos, src);
        if written == 0 {
            return Ok(OooResult {
                accepted: 0,
                delivered: 0,
                start: None,
                len: 0,
            });
        }

        let seg_end_full = abs_pos.wrapping_add(written as u32);
        let mut seg_start = abs_pos;
        let mut retained_start = seg_start;
        let mut retained_end = seg_end_full;

        // Predecessor check
        let pred_info = bk
            .index
            .predecessor(&seg_start)
            .and_then(|(_, &idx)| bk.entries.get(idx))
            .map(|s| {
                let end = s.offset.wrapping_add(s.len);
                (s.offset, end, end >= seg_end_full)
            });

        if let Some((pred_start, pred_end, skip)) = pred_info {
            if skip {
                return Ok(OooResult {
                    accepted: 0,
                    delivered: 0,
                    start: None,
                    len: 0,
                });
            }
            if pred_end >= seg_start {
                retained_start = pred_start;
            }
            if pred_end > seg_start {
                seg_start = pred_end;
            }
        }

        if seg_start >= seg_end_full {
            return Ok(OooResult {
                accepted: 0,
                delivered: 0,
                start: None,
                len: 0,
            });
        }

        let mut accepted = seg_end_full.wrapping_sub(seg_start);
        let mut overlap_cursor = seg_start.wrapping_sub(1);
        loop {
            let overlap_info = bk.index.successor(&overlap_cursor).and_then(|(key, &idx)| {
                if *key < seg_end_full {
                    bk.entries
                        .get(idx)
                        .map(|segment| (*key, segment.offset.wrapping_add(segment.len)))
                } else {
                    None
                }
            });
            let Some((existing_start, existing_end)) = overlap_info else {
                break;
            };
            let overlap_start = existing_start.max(seg_start);
            let overlap_end = existing_end.min(seg_end_full);
            if overlap_end > overlap_start {
                accepted = accepted.wrapping_sub(overlap_end.wrapping_sub(overlap_start));
            }
            overlap_cursor = existing_start;
        }
        if accepted == 0 {
            return Ok(OooResult {
                accepted: 0,
                delivered: 0,
                start: None,
                len: 0,
            });
        }

        // Successor walk: remove or trim overlapping segments
        loop {
            let succ_info =
                bk.index
                    .successor(&(seg_start.wrapping_sub(1)))
                    .and_then(|(k, &idx)| {
                        if *k < seg_end_full {
                            bk.entries
                                .get(idx)
                                .map(|s| (*k, s.offset.wrapping_add(s.len)))
                        } else {
                            None
                        }
                    });

            let (succ_key, succ_end) = match succ_info {
                Some(info) => info,
                None => break,
            };

            retained_end = retained_end.max(succ_end);
            if succ_end <= seg_end_full {
                bk.remove_ooo_entry(succ_key);
            } else {
                bk.remove_ooo_entry(succ_key);
                let new_key = seg_end_full;
                let new_len = succ_end.wrapping_sub(new_key);
                if new_len > 0 {
                    let new_idx = bk
                        .entries
                        .insert(OooSegment {
                            offset: new_key,
                            len: new_len,
                        })
                        .expect("ooo pool exhausted");
                    bk.index.insert(new_key, new_idx);
                }
                break;
            }
        }

        // Insert our segment
        let seg_len = seg_end_full.wrapping_sub(seg_start);
        let idx = bk
            .entries
            .insert(OooSegment {
                offset: seg_start,
                len: seg_len,
            })
            .expect("ooo pool exhausted");
        bk.index.insert(seg_start, idx);

        // Contiguous check
        let delivered = if seg_start == bk.base {
            Self::promote_contiguous_inner(bk)
        } else {
            0
        };

        Ok(OooResult {
            accepted,
            delivered,
            start: Some(retained_start.wrapping_sub(base)),
            len: retained_end.wrapping_sub(retained_start),
        })
    }

    fn promote_contiguous_inner(bk: &mut OooBookkeeping) -> u32 {
        let mut delivered: u32 = 0;
        loop {
            let first = bk.index.first().map(|(&k, &v)| (k, v));
            match first {
                Some((first_key, first_idx)) if first_key == bk.base => {
                    let seg = bk.entries.remove(first_idx).expect("ooo entry exists");
                    bk.index.remove(&first_key);
                    bk.base = bk.base.wrapping_add(seg.len);
                    delivered = delivered.wrapping_add(seg.len);
                }
                _ => break,
            }
        }
        delivered
    }

    fn promote_contiguous_from(&self, base: u32) -> u32 {
        let ooo = unsafe { &mut *self.ooo.get() };
        match ooo.as_mut() {
            Some(bk) => {
                bk.base = base;
                Self::promote_contiguous_inner(bk)
            }
            None => 0,
        }
    }

    pub fn promote_contiguous(&self) -> u32 {
        let base = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
        self.promote_contiguous_from(base)
    }

    pub fn ooo_head(&self) -> Option<(u32, u32)> {
        let ooo = unsafe { &*self.ooo.get() };
        let bk = ooo.as_ref()?;
        let (&first_key, &first_idx) = bk.index.first()?;
        let seg = bk.entries.get(first_idx)?;
        let relative = first_key.wrapping_sub(bk.base);
        Some((relative, seg.len))
    }

    pub fn ooo_enqueued(&self) -> usize {
        let ooo = unsafe { &*self.ooo.get() };
        match ooo.as_ref() {
            Some(bk) => bk.entries.len(),
            None => 0,
        }
    }
}

impl Fifo {
    /// Convenience constructor backed by a process-local Segment.
    pub fn with_capacity(capacity: usize) -> Result<Self, FifoError> {
        let bytes = Self::layout_bytes(capacity)?;
        let seg = Segment::local(bytes.saturating_add(256));
        Self::new(seg, capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    fn fifo(cap: usize) -> Fifo {
        let seg = Segment::local(cap * 16 + (1 << 20));
        Fifo::new(seg, cap).expect("fifo")
    }

    #[test]
    fn enqueue_peek_dequeue_roundtrip() {
        let f = fifo(4096);
        assert_eq!(f.enqueue(b"hello world"), 11);
        let mut buf = [0u8; 16];
        assert_eq!(f.peek(0, 11, &mut buf), 11);
        assert_eq!(&buf[..11], b"hello world");
        assert_eq!(f.dequeue_drop(11), 11);
        assert_eq!(f.peek(0, 8, &mut buf), 0);
        assert!(f.max_dequeue() == 0);
    }

    #[test]
    fn enqueue_across_chunk_boundary() {
        let f = fifo(1 << 16);
        let big = vec![0xABu8; 5000];
        assert_eq!(f.enqueue(&big), big.len());
        let mut out = vec![0u8; big.len()];
        assert_eq!(f.peek(0, big.len(), &mut out), big.len());
        assert_eq!(out, big);
    }

    #[test]
    fn dequeue_drop_keeps_tail_chunk_before_future_ooo_successor() {
        let segment = Segment::local(1 << 20);
        let mut f = Fifo::new(segment, 1 << 16).expect("fifo");
        f.enable_ooo();
        let first_chunk = vec![0xA5; 4096];
        let gap_chunk = vec![0x5A; 4096];

        assert_eq!(f.enqueue(&first_chunk), first_chunk.len());
        f.enqueue_ooo(4096, b"future").expect("ooo enqueue");
        assert_eq!(f.dequeue_drop(first_chunk.len()), first_chunk.len());
        assert_eq!(f.enqueue(&gap_chunk), gap_chunk.len() + b"future".len());
        assert_eq!(f.enqueue(b"!"), 1);
    }

    #[test]
    fn attached_fifo_reuses_preallocated_chunks_without_segment_allocation() {
        let capacity = 1 << 14;
        let bytes = Fifo::layout_bytes(capacity).expect("FIFO layout");
        let owner_segment =
            Segment::shared("hammer-fifo-attach", bytes + 256).expect("shared segment");
        let owner = Fifo::new(owner_segment.clone(), capacity).expect("owner FIFO");
        let attached_segment = Segment::from_fd(
            owner_segment.shared_fd().expect("shared descriptor"),
            owner_segment.size(),
        )
        .expect("attached segment");
        let attached = unsafe { Fifo::from_shared(attached_segment, owner.hdr_offset()) };

        let first = vec![0xA5; capacity];
        assert_eq!(attached.enqueue(&first), capacity);
        assert_eq!(owner.dequeue_drop(capacity), capacity);

        let second = vec![0x5A; capacity];
        assert_eq!(attached.enqueue(&second), capacity);
        let mut received = vec![0; capacity];
        assert_eq!(owner.peek(0, capacity, &mut received), capacity);
        assert_eq!(received, second);
    }

    #[test]
    fn peek_segments_returns_two_slices() {
        let f = fifo(4096);
        f.enqueue(b"hello");
        let total = f.peek_segments(0, 5, |a, b| a.len() + b.len());
        assert_eq!(total, Some(5));
    }

    #[test]
    fn should_signal_edge_triggered() {
        let f = fifo(4096);
        f.want_notification();
        assert!(f.should_signal(f.enqueue(&[1])));
        assert!(!f.should_signal(f.enqueue(&[2])));
    }

    #[test]
    fn needs_deq_notification_when_requested() {
        let f = fifo(4096);
        f.enqueue(&[1, 2, 3, 4]);
        assert!(!f.needs_deq_notification(f.dequeue_drop(1)));
        f.want_deq_notification();
        assert!(f.needs_deq_notification(f.dequeue_drop(1)));
    }

    #[test]
    fn spsc_concurrent_no_loss() {
        const N: usize = 10_000;
        let f = Arc::new(fifo(1 << 12));
        let pf = Arc::clone(&f);
        let cf = Arc::clone(&f);
        let payload: Vec<u8> = (0..N).map(|i| (i % 256) as u8).collect();
        let expected = payload.clone();
        let producer = thread::spawn(move || {
            let mut sent = 0;
            while sent < N {
                let end = (sent + 64).min(N);
                let mut off = 0;
                while off < end - sent {
                    let wrote = pf.enqueue(&payload[sent + off..end]);
                    if wrote == 0 {
                        thread::yield_now();
                        continue;
                    }
                    off += wrote;
                }
                sent = end;
            }
        });
        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(N);
            let mut buf = [0u8; 256];
            while received.len() < N {
                let n = cf.peek(0, buf.len(), &mut buf);
                if n == 0 {
                    thread::yield_now();
                    continue;
                }
                received.extend_from_slice(&buf[..n]);
                cf.dequeue_drop(n);
            }
            received
        });
        producer.join().unwrap();
        assert_eq!(consumer.join().unwrap(), expected);
    }

    #[test]
    fn fifo_header_is_cacheline_aligned() {
        use std::mem::{align_of, size_of};
        assert_eq!(align_of::<FifoHeader>(), 64);
        assert_eq!(size_of::<FifoHeader>() % 64, 0);
    }
}

use std::cell::UnsafeCell;
use std::io::{self, BufRead, Read, Write};
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
    want_deq_ntf: AtomicU32,
    has_deq_ntf: AtomicU32,
    deq_thresh: AtomicU32,
    _pad0: [u8; 64 - (8 + 4 + 4 + 4 + 4 + 4 + 4)],
    head_chunk: AtomicU64,
    head: AtomicU32,
    _pad1: [u8; 64 - (8 + 4)],
    tail_chunk: AtomicU64,
    tail: AtomicU32,
    _pad2: [u8; 64 - (8 + 4)],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FifoError {
    #[error("FIFO capacity must be a power of two and at least 2")]
    InvalidCapacity,
    #[error("FIFO capacity {capacity} exceeds the shared layout range")]
    CapacityOutOfRange { capacity: usize },
    #[error("segment has insufficient space for FIFO storage")]
    SegmentExhausted,
    #[error("FIFO has {available} writable bytes for {requested} requested bytes")]
    InsufficientCapacity { requested: usize, available: usize },
    #[error("FIFO writable reservation length {requested} exceeds {max_len}")]
    ReservationTooLong { requested: usize, max_len: usize },
    #[error("FIFO commit initialized {initialized} bytes for reservation of {reserved}")]
    CommitExceedsReservation { initialized: usize, reserved: usize },
    #[error("out-of-order FIFO delivery is disabled")]
    OutOfOrderDisabled,
    #[error("out-of-order FIFO length {length} exceeds u32")]
    OutOfOrderLengthOutOfRange { length: usize },
    #[error("out-of-order FIFO offset {offset} plus length {length} overflows u32")]
    OutOfOrderOffsetOverflow { offset: u32, length: u32 },
    #[error("out-of-order FIFO end offset {end_offset} exceeds available capacity {available}")]
    OutOfOrderCapacityExceeded { end_offset: u32, available: usize },
    #[error("out-of-order FIFO segment storage exhausted at {entries} entries")]
    OutOfOrderStorageExhausted { entries: usize },
}

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

#[derive(Clone, Copy)]
struct ReservedChunk {
    off: u64,
    data_offset: usize,
    original_len: u32,
    reserved_len: usize,
}

pub struct FifoWriteReservation<'a> {
    fifo: &'a Fifo,
    start_tail: u32,
    original_tail_chunk: u64,
    first: Option<ReservedChunk>,
    second: Option<ReservedChunk>,
    reserved_len: usize,
    complete: bool,
}

impl OooBookkeeping {
    fn remove_ooo_entry(&mut self, offset: u32) -> Option<OooSegment> {
        let idx = self.index.remove(&offset)?;
        self.entries.remove(idx)
    }

    /// Grows segment storage before an enqueue can exhaust it, mirroring VPP
    /// `svm_fifo` whose ooo segment pool grows on demand (`pool_get`). Both
    /// containers are rebuilt, so callers must not hold pool indices across
    /// this call.
    fn ensure_capacity(&mut self, needed: usize) -> Result<(), FifoError> {
        if self.entries.capacity() >= needed && self.index.capacity() >= needed {
            return Ok(());
        }
        let new_capacity = self
            .entries
            .capacity()
            .saturating_mul(2)
            .max(needed)
            .next_power_of_two();
        let mut entries = Pool::with_capacity(new_capacity);
        let mut index = RbTree::with_capacity(new_capacity);
        while let Some((&offset, &idx)) = self.index.first() {
            self.index.remove(&offset);
            let Some(segment) = self.entries.remove(idx) else {
                continue;
            };
            let Some(new_idx) = entries.insert(segment) else {
                return Err(FifoError::OutOfOrderStorageExhausted {
                    entries: entries.capacity(),
                });
            };
            index.insert(offset, new_idx);
        }
        self.entries = entries;
        self.index = index;
        Ok(())
    }
}

impl<'a> FifoWriteReservation<'a> {
    pub fn reserved_len(&self) -> usize {
        self.reserved_len
    }

    pub fn segments_mut(&mut self) -> (&mut [u8], &mut [u8]) {
        unsafe {
            let first = match self.first {
                Some(chunk) => {
                    let ptr = self
                        .fifo
                        .base
                        .add(chunk.off as usize + CHUNK_HEADER_SIZE + chunk.data_offset);
                    std::slice::from_raw_parts_mut(ptr, chunk.reserved_len)
                }
                None => &mut [],
            };
            let second = match self.second {
                Some(chunk) => {
                    let ptr = self
                        .fifo
                        .base
                        .add(chunk.off as usize + CHUNK_HEADER_SIZE + chunk.data_offset);
                    std::slice::from_raw_parts_mut(ptr, chunk.reserved_len)
                }
                None => &mut [],
            };
            (first, second)
        }
    }

    pub fn commit(&mut self, initialized: usize) -> Result<usize, FifoError> {
        if initialized > self.reserved_len {
            return Err(FifoError::CommitExceedsReservation {
                initialized,
                reserved: self.reserved_len,
            });
        }

        self.publish_initialized(initialized);
        self.complete = true;
        Ok(initialized)
    }

    pub fn cancel(&mut self) {
        self.rollback();
        self.complete = true;
    }

    fn publish_initialized(&mut self, initialized: usize) {
        unsafe {
            self.set_visible_len(initialized);
            let tail = self.start_tail.wrapping_add(initialized as u32);
            (*self.fifo.hdr)
                .tail_chunk
                .store(self.published_tail_chunk(initialized), Ordering::Release);
            (*self.fifo.hdr).tail.store(tail, Ordering::Release);
            let collected = self.fifo.promote_contiguous_from(tail);
            (*self.fifo.hdr)
                .tail
                .store(tail.wrapping_add(collected), Ordering::Release);
        }
    }

    fn published_tail_chunk(&self, initialized: usize) -> u64 {
        match (self.first, self.second) {
            (_, Some(second)) if initialized > self.first.map_or(0, |first| first.reserved_len) => {
                second.off
            }
            (Some(first), _) => first.off,
            (None, _) => self.original_tail_chunk,
        }
    }

    unsafe fn set_visible_len(&self, initialized: usize) {
        let mut remaining = initialized;
        if let Some(first) = self.first {
            let visible = remaining.min(first.reserved_len);
            unsafe { self.set_chunk_len(first, visible) };
            remaining -= visible;
        }
        if let Some(second) = self.second {
            let visible = remaining.min(second.reserved_len);
            unsafe { self.set_chunk_len(second, visible) };
        }
    }

    unsafe fn set_chunk_len(&self, chunk: ReservedChunk, visible: usize) {
        let chunk_ref = unsafe { &*self.fifo.base.add(chunk.off as usize).cast::<Chunk>() };
        let len = chunk.data_offset + visible;
        let restored = (len as u32).max(chunk.original_len.min(chunk.data_offset as u32));
        chunk_ref.length.store(restored, Ordering::Relaxed);
    }

    fn rollback(&mut self) {
        unsafe {
            if let Some(first) = self.first {
                let chunk = &*self.fifo.base.add(first.off as usize).cast::<Chunk>();
                chunk.length.store(first.original_len, Ordering::Relaxed);
            }
            if let Some(second) = self.second {
                let chunk = &*self.fifo.base.add(second.off as usize).cast::<Chunk>();
                chunk.length.store(second.original_len, Ordering::Relaxed);
            }
            (*self.fifo.hdr)
                .tail_chunk
                .store(self.original_tail_chunk, Ordering::Release);
        }
    }
}

impl Drop for FifoWriteReservation<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.rollback();
        }
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

impl Read for &Fifo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.max_dequeue() == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }

        let read = self.peek(0, buf.len(), buf);
        assert_eq!(
            self.dequeue_drop(read),
            read,
            "FIFO readable bytes changed while held by its consumer"
        );
        Ok(read)
    }
}

impl BufRead for &Fifo {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.readable_segment()
            .ok_or_else(|| io::ErrorKind::WouldBlock.into())
    }

    fn consume(&mut self, amount: usize) {
        let readable = self.readable_segment().map_or(0, <[u8]>::len);
        assert!(
            amount <= readable,
            "cannot consume {amount} bytes from a FIFO segment containing {readable} bytes"
        );
        assert_eq!(
            self.dequeue_drop(amount),
            amount,
            "FIFO readable bytes changed while held by its consumer"
        );
    }
}

impl Write for &Fifo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let available = self.max_enqueue();
        if available == 0 {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        let requested = buf.len().min(available);
        let mut reservation = match self.reserve_write(requested) {
            Ok(reservation) => reservation,
            Err(FifoError::ReservationTooLong { max_len, .. }) => {
                self.reserve_write(max_len).map_err(io::Error::other)?
            }
            Err(error) => return Err(io::Error::other(error)),
        };
        let written = reservation.reserved_len();
        let (first, second) = reservation.segments_mut();
        let first_len = first.len();
        first.copy_from_slice(&buf[..first_len]);
        second.copy_from_slice(&buf[first_len..written]);
        reservation.commit(written).map_err(io::Error::other)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

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
                    want_deq_ntf: AtomicU32::new(0),
                    has_deq_ntf: AtomicU32::new(0),
                    deq_thresh: AtomicU32::new(0),
                    _pad0: [0; 64 - (8 + 4 + 4 + 4 + 4 + 4 + 4)],
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

    fn readable_segment(&self) -> Option<&[u8]> {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            if head == tail {
                return None;
            }

            let mut chunk_off = (*hdr).head_chunk.load(Ordering::Relaxed);
            while chunk_off != 0 {
                let chunk = &*self.base.add(chunk_off as usize).cast::<Chunk>();
                let chunk_end = chunk.start_byte + chunk.length.load(Ordering::Relaxed);
                if head >= chunk.start_byte && head < chunk_end {
                    let data_offset = (head - chunk.start_byte) as usize;
                    let available = (chunk_end - head).min(tail.wrapping_sub(head)) as usize;
                    return Some(std::slice::from_raw_parts(
                        self.base
                            .add(chunk_off as usize + CHUNK_HEADER_SIZE + data_offset),
                        available,
                    ));
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

    pub fn reserve_write(&self, len: usize) -> Result<FifoWriteReservation<'_>, FifoError> {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Acquire);
            let tail = (*hdr).tail.load(Ordering::Relaxed);
            if head == tail {
                self.prepare_empty_tail_chunk(tail);
            }
            let used = tail.wrapping_sub(head);
            let available = ((*hdr).size - used) as usize;
            if len > available {
                return Err(FifoError::InsufficientCapacity {
                    requested: len,
                    available,
                });
            }

            let original_tail_chunk = (*hdr).tail_chunk.load(Ordering::Relaxed);
            if len == 0 {
                return Ok(FifoWriteReservation {
                    fifo: self,
                    start_tail: tail,
                    original_tail_chunk,
                    first: None,
                    second: None,
                    reserved_len: 0,
                    complete: false,
                });
            }

            self.reserve_write_at_tail(tail, len, original_tail_chunk)
        }
    }

    unsafe fn reserve_write_at_tail(
        &self,
        tail: u32,
        len: usize,
        original_tail_chunk: u64,
    ) -> Result<FifoWriteReservation<'_>, FifoError> {
        let chunk_data_size = unsafe { (*self.hdr).min_alloc as usize };
        let first_off = unsafe { self.tail_chunk_for_write(tail, original_tail_chunk)? };
        let (first, remaining) = unsafe { self.reserve_chunk_prefix(first_off, tail, len) };

        if remaining > chunk_data_size {
            unsafe { self.abort_reservation(original_tail_chunk, first, None) };
            return Err(FifoError::ReservationTooLong {
                requested: len,
                max_len: first.reserved_len + chunk_data_size,
            });
        }

        let second = if remaining == 0 {
            None
        } else {
            match unsafe { self.reserve_following_chunk(first.off, remaining) } {
                Ok(second) => Some(second),
                Err(err) => {
                    unsafe { self.abort_reservation(original_tail_chunk, first, None) };
                    return Err(err);
                }
            }
        };

        Ok(FifoWriteReservation {
            fifo: self,
            start_tail: tail,
            original_tail_chunk,
            first: Some(first),
            second,
            reserved_len: len,
            complete: false,
        })
    }

    unsafe fn tail_chunk_for_write(
        &self,
        tail: u32,
        original_tail_chunk: u64,
    ) -> Result<u64, FifoError> {
        let hdr = self.hdr;
        let chunk_data_size = unsafe { (*hdr).min_alloc };
        let mut chunk_off = unsafe { (*hdr).tail_chunk.load(Ordering::Relaxed) };

        loop {
            if chunk_off == 0 {
                let Some(new_off) = (unsafe { self.acquire_chunk(tail) }) else {
                    return Err(FifoError::SegmentExhausted);
                };
                unsafe {
                    (*hdr).head_chunk.store(new_off, Ordering::Release);
                    (*hdr).tail_chunk.store(new_off, Ordering::Release);
                }
                return Ok(new_off);
            }

            let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
            if tail < chunk.start_byte {
                unsafe { self.restore_tail_chunk(original_tail_chunk) };
                return Err(FifoError::SegmentExhausted);
            }

            let chunk_end = chunk.start_byte + chunk_data_size;
            if tail < chunk_end {
                unsafe { self.restore_tail_chunk(chunk_off) };
                return Ok(chunk_off);
            }

            let next_off = chunk.next.load(Ordering::Acquire);
            if next_off != 0 {
                let next = unsafe { &*self.base.add(next_off as usize).cast::<Chunk>() };
                if tail >= next.start_byte {
                    unsafe { self.restore_tail_chunk(next_off) };
                    chunk_off = next_off;
                    continue;
                }
            }

            if tail > chunk_end {
                unsafe { self.restore_tail_chunk(original_tail_chunk) };
                return Err(FifoError::SegmentExhausted);
            }

            let Some(new_off) = (unsafe { self.acquire_chunk(tail) }) else {
                unsafe { self.restore_tail_chunk(original_tail_chunk) };
                return Err(FifoError::SegmentExhausted);
            };
            let new_chunk = unsafe { &*self.base.add(new_off as usize).cast::<Chunk>() };
            new_chunk.next.store(next_off, Ordering::Relaxed);
            chunk.next.store(new_off, Ordering::Release);
            unsafe { self.restore_tail_chunk(new_off) };
            return Ok(new_off);
        }
    }

    unsafe fn reserve_chunk_prefix(
        &self,
        chunk_off: u64,
        logical_pos: u32,
        len: usize,
    ) -> (ReservedChunk, usize) {
        let chunk_data_size = unsafe { (*self.hdr).min_alloc as usize };
        let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
        let data_offset = logical_pos.wrapping_sub(chunk.start_byte) as usize;
        let reserved_len = len.min(chunk_data_size - data_offset);
        let original_len = chunk.length.load(Ordering::Relaxed);
        let visible_len = data_offset + reserved_len;
        if visible_len > original_len as usize {
            chunk.length.store(visible_len as u32, Ordering::Relaxed);
        }
        (
            ReservedChunk {
                off: chunk_off,
                data_offset,
                original_len,
                reserved_len,
            },
            len - reserved_len,
        )
    }

    unsafe fn reserve_following_chunk(
        &self,
        prev_off: u64,
        len: usize,
    ) -> Result<ReservedChunk, FifoError> {
        let prev = unsafe { &*self.base.add(prev_off as usize).cast::<Chunk>() };
        let next_start = prev.start_byte + unsafe { (*self.hdr).min_alloc };
        let next_off = prev.next.load(Ordering::Acquire);
        let chunk_off = unsafe { self.following_chunk_for_write(prev_off, next_start, next_off)? };
        unsafe { self.restore_tail_chunk(chunk_off) };

        let chunk = unsafe { &*self.base.add(chunk_off as usize).cast::<Chunk>() };
        let original_len = chunk.length.load(Ordering::Relaxed);
        if len > original_len as usize {
            chunk.length.store(len as u32, Ordering::Relaxed);
        }
        Ok(ReservedChunk {
            off: chunk_off,
            data_offset: 0,
            original_len,
            reserved_len: len,
        })
    }

    unsafe fn following_chunk_for_write(
        &self,
        prev_off: u64,
        start_byte: u32,
        next_off: u64,
    ) -> Result<u64, FifoError> {
        let prev = unsafe { &*self.base.add(prev_off as usize).cast::<Chunk>() };
        if next_off != 0 {
            let next = unsafe { &*self.base.add(next_off as usize).cast::<Chunk>() };
            if next.start_byte == start_byte {
                return Ok(next_off);
            }
            if next.start_byte < start_byte {
                return Err(FifoError::SegmentExhausted);
            }
        }

        let Some(new_off) = (unsafe { self.acquire_chunk(start_byte) }) else {
            return Err(FifoError::SegmentExhausted);
        };
        let new_chunk = unsafe { &*self.base.add(new_off as usize).cast::<Chunk>() };
        new_chunk.next.store(next_off, Ordering::Relaxed);
        prev.next.store(new_off, Ordering::Release);
        Ok(new_off)
    }

    unsafe fn abort_reservation(
        &self,
        tail_chunk: u64,
        first: ReservedChunk,
        second: Option<ReservedChunk>,
    ) {
        unsafe {
            let first_chunk = &*self.base.add(first.off as usize).cast::<Chunk>();
            first_chunk
                .length
                .store(first.original_len, Ordering::Relaxed);
            if let Some(second) = second {
                let second_chunk = &*self.base.add(second.off as usize).cast::<Chunk>();
                second_chunk
                    .length
                    .store(second.original_len, Ordering::Relaxed);
            }
            self.restore_tail_chunk(tail_chunk);
        }
    }

    unsafe fn restore_tail_chunk(&self, chunk_off: u64) {
        unsafe {
            (*self.hdr).tail_chunk.store(chunk_off, Ordering::Release);
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
        // VPP's `svm_fifo` starts with no configured OOO segment limit and
        // grows `ooo_segments` through `pool_get` as segments arrive. Hammer's
        // fixed-capacity Pool is rebuilt on demand by `ensure_capacity`, so
        // this is only the first slab size, not a session policy.
        const INITIAL_OOO_SEGMENTS: usize = 4;
        *self.ooo.get_mut() = Some(Box::new(OooBookkeeping {
            base: 0,
            entries: Pool::with_capacity(INITIAL_OOO_SEGMENTS),
            index: RbTree::with_capacity(INITIAL_OOO_SEGMENTS),
        }));
    }

    pub fn enqueue_ooo(&self, offset: u32, src: &[u8]) -> Result<OooResult, FifoError> {
        let ooo = unsafe { &mut *self.ooo.get() };
        let bk = ooo.as_mut().ok_or(FifoError::OutOfOrderDisabled)?;
        // One call nets at most one new segment (the split site removes before
        // inserting); +2 keeps a slot of headroom in both containers.
        bk.ensure_capacity(bk.entries.len().saturating_add(2))?;

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
                    let Some(new_idx) = bk.entries.insert(OooSegment {
                        offset: new_key,
                        len: new_len,
                    }) else {
                        return Err(FifoError::OutOfOrderStorageExhausted {
                            entries: bk.entries.capacity(),
                        });
                    };
                    bk.index.insert(new_key, new_idx);
                }
                break;
            }
        }

        // Insert our segment
        let seg_len = seg_end_full.wrapping_sub(seg_start);
        let Some(idx) = bk.entries.insert(OooSegment {
            offset: seg_start,
            len: seg_len,
        }) else {
            return Err(FifoError::OutOfOrderStorageExhausted {
                entries: bk.entries.capacity(),
            });
        };
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
                    bk.index.remove(&first_key);
                    let Some(seg) = bk.entries.remove(first_idx) else {
                        break;
                    };
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
    fn writable_reservation_commits_contiguous_bytes() {
        let f = fifo(4096);
        let mut reservation = f.reserve_write(5).expect("reservation");
        assert_eq!(reservation.reserved_len(), 5);
        {
            let (first, second) = reservation.segments_mut();
            assert_eq!(first.len(), 5);
            assert!(second.is_empty());
            first.copy_from_slice(b"hello");
        }
        assert_eq!(reservation.commit(5), Ok(5));

        let mut out = [0; 8];
        assert_eq!(f.peek(0, 5, &mut out), 5);
        assert_eq!(&out[..5], b"hello");
    }

    #[test]
    fn writable_reservation_commits_split_bytes() {
        let f = fifo(1 << 13);
        let prefix = vec![0xA5; 4090];
        assert_eq!(f.enqueue(&prefix), prefix.len());

        let mut reservation = f.reserve_write(12).expect("split reservation");
        {
            let (first, second) = reservation.segments_mut();
            assert_eq!(first.len(), 6);
            assert_eq!(second.len(), 6);
            first.copy_from_slice(b"abcdef");
            second.copy_from_slice(b"ghijkl");
        }
        assert_eq!(reservation.commit(12), Ok(12));

        let mut out = vec![0; prefix.len() + 12];
        assert_eq!(f.peek(0, out.len(), &mut out), out.len());
        assert_eq!(&out[..prefix.len()], prefix.as_slice());
        assert_eq!(&out[prefix.len()..], b"abcdefghijkl");
    }

    #[test]
    fn writable_reservation_partial_commit_publishes_initialized_prefix_only() {
        let f = fifo(1 << 13);
        let prefix = vec![0xA5; 4090];
        assert_eq!(f.enqueue(&prefix), prefix.len());

        let mut reservation = f.reserve_write(12).expect("split reservation");
        {
            let (first, second) = reservation.segments_mut();
            first.copy_from_slice(b"abcdef");
            second.copy_from_slice(b"ghijkl");
        }
        assert_eq!(reservation.commit(4), Ok(4));

        assert_eq!(f.max_dequeue(), prefix.len() + 4);
        let mut out = vec![0; prefix.len() + 4];
        assert_eq!(f.peek(0, out.len(), &mut out), out.len());
        assert_eq!(&out[prefix.len()..], b"abcd");
        assert_eq!(f.enqueue(b"EFGH"), 4);
        let mut tail = [0; 8];
        assert_eq!(f.peek(prefix.len(), 8, &mut tail), 8);
        assert_eq!(&tail, b"abcdEFGH");
    }

    #[test]
    fn writable_reservation_cancel_leaves_fifo_unchanged() {
        let f = fifo(4096);
        assert_eq!(f.enqueue(b"seed"), 4);
        let mut reservation = f.reserve_write(4).expect("reservation");
        {
            let (first, _) = reservation.segments_mut();
            first.copy_from_slice(b"junk");
        }
        reservation.cancel();

        assert_eq!(f.max_dequeue(), 4);
        let mut out = [0; 8];
        assert_eq!(f.peek(0, 8, &mut out), 4);
        assert_eq!(&out[..4], b"seed");
        assert_eq!(f.enqueue(b"next"), 4);
        assert_eq!(f.peek(0, 8, &mut out), 8);
        assert_eq!(&out, b"seednext");
    }

    #[test]
    fn writable_reservation_drop_leaves_split_fifo_unchanged_and_retriable() {
        let f = fifo(1 << 13);
        let prefix = vec![0xA5; 4090];
        assert_eq!(f.enqueue(&prefix), prefix.len());
        {
            let mut reservation = f.reserve_write(12).expect("split reservation");
            let (first, second) = reservation.segments_mut();
            first.fill(0x11);
            second.fill(0x22);
        }

        assert_eq!(f.max_dequeue(), prefix.len());
        assert_eq!(f.enqueue(b"abcdef"), 6);
        let mut out = vec![0; prefix.len() + 6];
        assert_eq!(f.peek(0, out.len(), &mut out), out.len());
        assert_eq!(&out[prefix.len()..], b"abcdef");
    }

    #[test]
    fn invalid_writable_commit_is_typed_and_drop_rolls_back() {
        let f = fifo(4096);
        let err = {
            let mut reservation = f.reserve_write(4).expect("reservation");
            let (first, _) = reservation.segments_mut();
            first.copy_from_slice(b"junk");
            reservation.commit(5).expect_err("invalid commit length")
        };
        assert_eq!(
            err,
            FifoError::CommitExceedsReservation {
                initialized: 5,
                reserved: 4
            }
        );
        assert_eq!(f.max_dequeue(), 0);
        assert_eq!(f.enqueue(b"next"), 4);
        let mut out = [0; 4];
        assert_eq!(f.peek(0, 4, &mut out), 4);
        assert_eq!(&out, b"next");
    }

    #[test]
    fn writable_reservation_rejects_full_fifo_without_mutation() {
        let f = fifo(4096);
        let full = vec![0xA5; 4096];
        assert_eq!(f.enqueue(&full), full.len());
        let err = match f.reserve_write(1) {
            Ok(_) => panic!("full FIFO accepted writable reservation"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            FifoError::InsufficientCapacity {
                requested: 1,
                available: 0
            }
        );
        assert_eq!(f.max_dequeue(), full.len());
        assert_eq!(f.dequeue_drop(full.len()), full.len());
        assert_eq!(f.enqueue(b"ok"), 2);
    }

    #[test]
    fn writable_reservation_rejects_more_than_two_segments_without_mutation() {
        let f = fifo(1 << 14);
        let first_chunk = vec![0xA5; 4096];
        assert_eq!(f.enqueue(&first_chunk), first_chunk.len());

        let err = match f.reserve_write(8193) {
            Ok(_) => panic!("oversized writable reservation succeeded"),
            Err(err) => err,
        };
        assert_eq!(
            err,
            FifoError::ReservationTooLong {
                requested: 8193,
                max_len: 8192
            }
        );
        assert_eq!(f.max_dequeue(), first_chunk.len());
        assert_eq!(f.enqueue(b"next"), 4);
        let mut out = vec![0; first_chunk.len() + 4];
        assert_eq!(f.peek(0, out.len(), &mut out), out.len());
        assert_eq!(&out[..first_chunk.len()], first_chunk.as_slice());
        assert_eq!(&out[first_chunk.len()..], b"next");
    }

    #[test]
    fn writable_reservation_inserts_gap_before_future_ooo_chunk() {
        let segment = Segment::local(1 << 20);
        let mut f = Fifo::new(segment, 1 << 16).expect("fifo");
        f.enable_ooo();
        let first_chunk = vec![0xA5; 4096];
        let gap_chunk = vec![0x5A; 4096];

        assert_eq!(f.enqueue(&first_chunk), first_chunk.len());
        f.enqueue_ooo(4096, b"future").expect("ooo enqueue");

        let mut reservation = f.reserve_write(gap_chunk.len()).expect("gap reservation");
        let (first, second) = reservation.segments_mut();
        assert_eq!(first.len(), gap_chunk.len());
        assert!(second.is_empty());
        first.copy_from_slice(&gap_chunk);
        assert_eq!(reservation.commit(gap_chunk.len()), Ok(gap_chunk.len()));

        let expected_len = first_chunk.len() + gap_chunk.len() + b"future".len();
        assert_eq!(f.max_dequeue(), expected_len);
        let mut out = vec![0; expected_len];
        assert_eq!(f.peek(0, out.len(), &mut out), out.len());
        assert_eq!(&out[..first_chunk.len()], first_chunk.as_slice());
        assert_eq!(
            &out[first_chunk.len()..first_chunk.len() + gap_chunk.len()],
            gap_chunk.as_slice()
        );
        assert_eq!(&out[first_chunk.len() + gap_chunk.len()..], b"future");
    }

    #[test]
    fn writable_reservation_reuses_released_chunk_after_wraparound() {
        let f = fifo(1 << 13);
        let first = vec![0xA5; 4096];
        let second = vec![0x5A; 512];
        assert_eq!(f.enqueue(&first), first.len());
        assert_eq!(f.enqueue(&second), second.len());
        assert_eq!(f.dequeue_drop(first.len()), first.len());

        let mut reservation = f.reserve_write(4096).expect("wrap reservation");
        let (first_segment, second_segment) = reservation.segments_mut();
        assert_eq!(first_segment.len(), 3584);
        assert_eq!(second_segment.len(), 512);
        first_segment.fill(0x11);
        second_segment.fill(0x22);
        assert_eq!(reservation.commit(4096), Ok(4096));

        let mut out = vec![0; second.len() + 4096];
        assert_eq!(f.peek(0, out.len(), &mut out), out.len());
        assert_eq!(&out[..second.len()], second.as_slice());
        assert_eq!(&out[second.len()..second.len() + 3584], &[0x11; 3584]);
        assert_eq!(&out[second.len() + 3584..], &[0x22; 512]);
    }

    #[test]
    fn set_event_reports_transition_once() {
        let f = fifo(4096);
        assert!(f.set_event());
        assert!(!f.set_event());
        f.unset_event();
        assert!(f.set_event());
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

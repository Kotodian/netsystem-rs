use std::cell::UnsafeCell;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::pool::{Index as PoolIndex, Pool};
use crate::rbtree::RbTree;
use crate::segment::{Local, Segment};

#[repr(C)]
struct Chunk {
    start_byte: u32,
    length: u32,
    next: AtomicU64,
    refcount: AtomicU32,
}

const CHUNK_HEADER_SIZE: usize = std::mem::size_of::<Chunk>();

#[repr(C, align(64))]
pub struct FifoHeader {
    start_chunk: u64,
    end_chunk: u64,
    size: u32,
    min_alloc: u32,
    has_event: AtomicU32,
    want_ntf: AtomicU32,
    want_deq_ntf: AtomicU32,
    has_deq_ntf: AtomicU32,
    deq_thresh: AtomicU32,
    _pad0: [u8; 64 - (8 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 4)],
    head_chunk: u64,
    head: AtomicU32,
    _pad1: [u8; 64 - (8 + 4)],
    tail_chunk: u64,
    tail: AtomicU32,
    _pad2: [u8; 64 - (8 + 4)],
}

#[derive(Debug)]
pub enum FifoError {
    InvalidCapacity,
}

pub struct OooResult {
    pub delivered: u32,
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

pub struct Fifo<S: Segment> {
    seg: S,
    base: *mut u8,
    hdr: *mut FifoHeader,
    hdr_off: u64,
    ooo: UnsafeCell<Option<Box<OooBookkeeping>>>,
}

unsafe impl<S: Segment> Send for Fifo<S> {}
unsafe impl<S: Segment> Sync for Fifo<S> {}

impl<S: Segment> Fifo<S> {
    pub fn new(seg: S, capacity: usize) -> Result<Self, FifoError> {
        if capacity < 2 || !capacity.is_power_of_two() {
            return Err(FifoError::InvalidCapacity);
        }
        let hdr_off = seg.alloc(std::mem::size_of::<FifoHeader>(), 64);
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_off as usize) as *mut FifoHeader };
        let chunk_size = capacity.min(4096);
        let chunk_off = seg.alloc(CHUNK_HEADER_SIZE + chunk_size, 64);
        unsafe {
            std::ptr::write(
                hdr,
                FifoHeader {
                    start_chunk: chunk_off,
                    end_chunk: chunk_off,
                    size: capacity as u32,
                    min_alloc: chunk_size as u32,
                    has_event: AtomicU32::new(0),
                    want_ntf: AtomicU32::new(0),
                    want_deq_ntf: AtomicU32::new(0),
                    has_deq_ntf: AtomicU32::new(0),
                    deq_thresh: AtomicU32::new(0),
                    _pad0: [0; 64 - (8 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 4)],
                    head_chunk: chunk_off,
                    head: AtomicU32::new(0),
                    _pad1: [0; 64 - (8 + 4)],
                    tail_chunk: chunk_off,
                    tail: AtomicU32::new(0),
                    _pad2: [0; 64 - (8 + 4)],
                },
            );
            let chunk = base.add(chunk_off as usize) as *mut Chunk;
            std::ptr::write(
                chunk,
                Chunk {
                    start_byte: 0,
                    length: 0,
                    next: AtomicU64::new(0),
                    refcount: AtomicU32::new(1),
                },
            );
        }
        Ok(Self {
            seg,
            base,
            hdr,
            hdr_off,
            ooo: UnsafeCell::new(None),
        })
    }

    /// Initialise a [`Fifo`] header at a pre-allocated offset in `seg`.
    /// The caller must guarantee that `seg` has `sizeof(FifoHeader) +
    /// CHUNK_HEADER_SIZE + capacity` bytes available at `hdr_offset` and
    /// that no other [`Fifo`] uses the same region.
    pub unsafe fn init_at(seg: S, hdr_offset: u64, capacity: usize) -> Result<Self, FifoError> {
        if capacity < 2 || !capacity.is_power_of_two() {
            return Err(FifoError::InvalidCapacity);
        }
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_offset as usize) as *mut FifoHeader };
        let chunk_size = capacity.min(4096) as u32;
        let chunk_off = hdr_offset + std::mem::size_of::<FifoHeader>() as u64;
        unsafe {
            std::ptr::write(
                hdr,
                FifoHeader {
                    start_chunk: chunk_off,
                    end_chunk: chunk_off,
                    size: capacity as u32,
                    min_alloc: chunk_size,
                    has_event: AtomicU32::new(0),
                    want_ntf: AtomicU32::new(0),
                    want_deq_ntf: AtomicU32::new(0),
                    has_deq_ntf: AtomicU32::new(0),
                    deq_thresh: AtomicU32::new(0),
                    _pad0: [0; 64 - (8 + 8 + 4 + 4 + 4 + 4 + 4 + 4 + 4)],
                    head_chunk: chunk_off,
                    head: AtomicU32::new(0),
                    _pad1: [0; 64 - (8 + 4)],
                    tail_chunk: chunk_off,
                    tail: AtomicU32::new(0),
                    _pad2: [0; 64 - (8 + 4)],
                },
            );
            let chunk = base.add(chunk_off as usize) as *mut Chunk;
            std::ptr::write(
                chunk,
                Chunk {
                    start_byte: 0,
                    length: 0,
                    next: AtomicU64::new(0),
                    refcount: AtomicU32::new(1),
                },
            );
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

    #[inline]
    pub fn enqueue(&self, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Acquire);
            let tail = (*hdr).tail.load(Ordering::Relaxed);
            let used = tail.wrapping_sub(head);
            let free = ((*hdr).size - used) as usize;
            let to_write = src.len().min(free);
            if to_write == 0 {
                return 0;
            }
            let written = self.write_at_without_tail_store(tail, &src[..to_write]);
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
            let mut chunk_off = (*hdr).head_chunk;
            let mut copied = 0usize;
            let mut pos = logical_pos;
            while copied < to_copy && chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte + chunk.length;
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
            let mut chunk_off = (*hdr).head_chunk;
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let chunk_end = chunk.start_byte + chunk.length;
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
                        let second_avail = next_chunk.length as usize;
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
            let mut chunk_off = (*hdr).head_chunk;
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                if new_head >= chunk.start_byte + chunk.length {
                    let next_off = chunk.next.load(Ordering::Acquire);
                    let prev = chunk.refcount.fetch_sub(1, Ordering::Relaxed);
                    if prev == 1 {
                        self.seg
                            .free(chunk_off, CHUNK_HEADER_SIZE + (*hdr).min_alloc as usize);
                    }
                    (*hdr).head_chunk = next_off;
                    chunk_off = next_off;
                    if next_off == 0 {
                        // All chunks freed — reset tail_chunk too so
                        // the producer allocates a fresh chunk on next enqueue.
                        (*hdr).tail_chunk = 0;
                        (*hdr).end_chunk = 0;
                        break;
                    }
                } else {
                    break;
                }
            }
            (*hdr).head.store(new_head, Ordering::Release);
            to_drop
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

            let mut chunk_off = (*hdr).head_chunk;
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

            // Write across chunks, allocating new ones at the end
            while !remaining_src.is_empty() {
                if chunk_off == 0 {
                    let new_off = self.seg.alloc(CHUNK_HEADER_SIZE + chunk_data_size, 64);
                    std::ptr::write(
                        self.base.add(new_off as usize) as *mut Chunk,
                        Chunk {
                            start_byte: remaining_offset,
                            length: 0,
                            next: AtomicU64::new(0),
                            refcount: AtomicU32::new(1),
                        },
                    );
                    if prev_off != 0 {
                        let prev = &mut *(self.base.add(prev_off as usize) as *mut Chunk);
                        prev.next.store(new_off, Ordering::Release);
                    } else {
                        (*hdr).head_chunk = new_off;
                    }
                    (*hdr).end_chunk = new_off;
                    (*hdr).tail_chunk = new_off;
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
                    if new_used > chunk.length as usize {
                        chunk.length = new_used as u32;
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
            let mut chunk_off = (*hdr).head_chunk;
            while chunk_off != 0 {
                let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                let next_off = chunk.next.load(Ordering::Acquire);
                let prev = chunk.refcount.fetch_sub(1, Ordering::Relaxed);
                if prev == 1 {
                    self.seg
                        .free(chunk_off, CHUNK_HEADER_SIZE + (*hdr).min_alloc as usize);
                }
                chunk_off = next_off;
            }
            (*hdr).head.store(0, Ordering::Relaxed);
            (*hdr).tail.store(0, Ordering::Relaxed);
            (*hdr).start_chunk = 0;
            (*hdr).end_chunk = 0;
            (*hdr).head_chunk = 0;
            (*hdr).tail_chunk = 0;
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
        self.seg.fd()
    }

    /// Reconstruct a [`Fifo`] from a shared-memory segment at the given
    /// header offset. The caller must guarantee that the segment contains a
    /// valid, initialised `FifoHeader` at `hdr_offset`.
    pub unsafe fn from_shared(seg: S, hdr_offset: u64) -> Self {
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

impl<S: Segment> Fifo<S> {
    pub fn enable_ooo(&mut self) {
        *self.ooo.get_mut() = Some(Box::new(OooBookkeeping {
            base: 0,
            entries: Pool::with_capacity(8),
            index: RbTree::with_capacity(8),
        }));
    }

    pub fn enqueue_ooo(&self, offset: u32, src: &[u8]) -> Result<OooResult, ()> {
        let ooo = unsafe { &mut *self.ooo.get() };
        let bk = ooo.as_mut().ok_or(())?;

        let total_len = u32::try_from(src.len()).map_err(|_| ())?;
        let end_offset = offset.checked_add(total_len).ok_or(())?;
        if end_offset as usize > self.max_enqueue() {
            return Err(());
        }
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
        bk.base = tail;
        let abs_pos = tail.wrapping_add(offset);
        let written = self.write_at_without_tail_store(abs_pos, src);
        if written == 0 {
            return Ok(OooResult { delivered: 0 });
        }

        let seg_end_full = abs_pos.wrapping_add(written as u32);
        let mut seg_start = abs_pos;

        // Predecessor check
        let pred_info = bk
            .index
            .predecessor(&seg_start)
            .and_then(|(_, &idx)| bk.entries.get(idx))
            .map(|s| {
                let end = s.offset.wrapping_add(s.len);
                (end, end >= seg_end_full, end > seg_start)
            });

        if let Some((pred_end, skip, overlap)) = pred_info {
            if skip {
                return Ok(OooResult { delivered: 0 });
            }
            if overlap {
                seg_start = pred_end;
            }
        }

        if seg_start >= seg_end_full {
            return Ok(OooResult { delivered: 0 });
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

        Ok(OooResult { delivered })
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

impl Fifo<Local> {
    /// Convenience constructor: creates a heap-backed [`Fifo`] with the given
    /// chunk capacity. The underlying `Local` segment is sized to hold
    /// `capacity` chunks plus the header.
    pub fn with_capacity(capacity: usize) -> Result<Self, FifoError> {
        let chunk_data_size = capacity.min(4096);
        let seg =
            Local::new(size_of::<FifoHeader>() + capacity * (size_of::<Chunk>() + chunk_data_size));
        Self::new(seg, capacity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::Local;
    use std::sync::Arc;
    use std::thread;

    fn fifo(cap: usize) -> Fifo<Local> {
        let seg = Local::new(cap * 16 + (1 << 20));
        Fifo::<Local>::new(seg, cap).expect("fifo")
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
    fn peek_segments_two_part_view() {
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

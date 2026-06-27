use std::os::fd::RawFd;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::segment::Segment;

#[repr(C)]
struct Chunk {
    start_byte: u32,
    length: u32,
    next: AtomicU64,
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

struct OooRange {
    start: u32,
    length: u32,
}

#[derive(Debug)]
pub enum FifoError {
    InvalidCapacity,
}

pub struct Fifo<S: Segment> {
    seg: S,
    base: *mut u8,
    hdr: *mut FifoHeader,
    ooo: Mutex<Vec<OooRange>>,
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
                },
            );
        }
        Ok(Self {
            seg,
            base,
            hdr,
            ooo: Mutex::new(Vec::new()),
        })
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
            let chunk_data_size = (*hdr).min_alloc as usize;
            let mut remaining = to_write;
            let mut written = 0usize;
            let mut chunk_off = (*hdr).tail_chunk;
            if chunk_off == 0 {
                let new_off = self.seg.alloc(CHUNK_HEADER_SIZE + chunk_data_size, 64);
                let new_chunk = &mut *(self.base.add(new_off as usize) as *mut Chunk);
                std::ptr::write(new_chunk, Chunk { start_byte: 0, length: 0, next: AtomicU64::new(0) });
                (*hdr).start_chunk = new_off;
                (*hdr).end_chunk = new_off;
                (*hdr).head_chunk = new_off;
                (*hdr).tail_chunk = new_off;
                chunk_off = new_off;
            }
            loop {
                let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                let used = chunk.length as usize;
                let room = chunk_data_size - used;
                if room > 0 {
                    let to_write = remaining.min(room);
                    let data_ptr = self.base.add(chunk_off as usize + CHUNK_HEADER_SIZE);
                    std::ptr::copy_nonoverlapping(
                        src.as_ptr().add(written),
                        data_ptr.add(used),
                        to_write,
                    );
                    chunk.length += to_write as u32;
                    remaining -= to_write;
                    written += to_write;
                }
                if remaining == 0 {
                    break;
                }
                let new_off = self
                    .seg
                    .alloc(CHUNK_HEADER_SIZE + chunk_data_size, 64);
                let new_start_byte = chunk.start_byte + chunk.length;
                let new_chunk = &mut *(self.base.add(new_off as usize) as *mut Chunk);
                std::ptr::write(
                    new_chunk,
                    Chunk {
                        start_byte: new_start_byte,
                        length: 0,
                        next: AtomicU64::new(0),
                    },
                );
                chunk.next.store(new_off, Ordering::Release);
                (*hdr).tail_chunk = new_off;
                (*hdr).end_chunk = new_off;
                chunk_off = new_off;
            }
            let old_tail = (*hdr).tail.load(Ordering::Relaxed);
            (*hdr)
                .tail
                .store(old_tail + written as u32, Ordering::Release);
            written
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
                        self.base.add(chunk_off as usize + CHUNK_HEADER_SIZE + data_off),
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
                    self.seg
                        .free(chunk_off, CHUNK_HEADER_SIZE + (*hdr).min_alloc as usize);
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

    pub fn enqueue_at(&self, offset: u32, src: &[u8]) -> usize {
        if src.is_empty() {
            return 0;
        }
        let hdr = self.hdr;
        unsafe {
            let chunk_data_size = (*hdr).min_alloc as usize;
            let mut chunk_off = (*hdr).head_chunk;
            let mut prev_off = 0u64;
            let mut written = 0usize;
            let mut remaining_offset = offset;
            let mut remaining_src = src;
            loop {
                let found = if chunk_off != 0 {
                    let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                    remaining_offset >= chunk.start_byte
                        && remaining_offset < chunk.start_byte + chunk_data_size as u32
                } else {
                    false
                };
                if found {
                    let chunk = &mut *(self.base.add(chunk_off as usize) as *mut Chunk);
                    let data_ptr = self.base.add(chunk_off as usize + CHUNK_HEADER_SIZE);
                    let data_off = (remaining_offset - chunk.start_byte) as usize;
                    let chunk_avail = chunk_data_size - data_off;
                    let to_write = remaining_src.len().min(chunk_avail);
                    std::ptr::copy_nonoverlapping(remaining_src.as_ptr(), data_ptr.add(data_off), to_write);
                    let new_used = data_off + to_write;
                    if new_used > chunk.length as usize {
                        chunk.length = new_used as u32;
                    }
                    written += to_write;
                    if to_write == remaining_src.len() {
                        break;
                    }
                    remaining_offset += to_write as u32;
                    remaining_src = &remaining_src[to_write..];
                    prev_off = chunk_off;
                    chunk_off = chunk.next.load(Ordering::Acquire);
                } else if chunk_off == 0 {
                    let new_off = self.seg.alloc(CHUNK_HEADER_SIZE + chunk_data_size, 64);
                    let start_byte = if prev_off != 0 {
                        let prev = &mut *(self.base.add(prev_off as usize) as *mut Chunk);
                        let prev_end = prev.start_byte + prev.length;
                        if prev_off == (*hdr).tail_chunk {
                            (*hdr).tail_chunk = new_off;
                            (*hdr).end_chunk = new_off;
                            prev.next.store(new_off, Ordering::Release);
                        }
                        remaining_offset.max(prev_end)
                    } else {
                        remaining_offset
                    };
                    let new_chunk = &mut *(self.base.add(new_off as usize) as *mut Chunk);
                    std::ptr::write(
                        new_chunk,
                        Chunk {
                            start_byte,
                            length: 0,
                            next: AtomicU64::new(0),
                        },
                    );
                    chunk_off = new_off;
                } else {
                    prev_off = chunk_off;
                    chunk_off = {
                        let chunk = &*(self.base.add(chunk_off as usize) as *mut Chunk);
                        chunk.next.load(Ordering::Acquire)
                    };
                }
            }
            let cur_tail = (*hdr).tail.load(Ordering::Relaxed);
            let new_tail = cur_tail.max(offset + written as u32);
            (*hdr).tail.store(new_tail, Ordering::Release);
            let mut ranges = self.ooo.lock().unwrap();
            insert_ooo_range(&mut ranges, offset, written as u32);
            written
        }
    }

    pub fn max_dequeue(&self) -> usize {
        let hdr = self.hdr;
        unsafe {
            let head = (*hdr).head.load(Ordering::Relaxed);
            let tail = (*hdr).tail.load(Ordering::Acquire);
            let ranges = self.ooo.lock().unwrap();
            if ranges.is_empty() {
                return tail.wrapping_sub(head) as usize;
            }
            let mut pos = head;
            let mut total = 0u32;
            for range in ranges.iter() {
                if range.start == pos {
                    total += range.length;
                    pos = range.start + range.length;
                } else if range.start > pos {
                    break;
                }
            }
            total as usize
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

    pub fn clear(&self) {
        let hdr = self.hdr;
        unsafe {
            (*hdr).head.store(0, Ordering::Relaxed);
            (*hdr).tail.store(0, Ordering::Relaxed);
            (*hdr).want_ntf.store(0, Ordering::Relaxed);
            (*hdr).want_deq_ntf.store(0, Ordering::Relaxed);
            (*hdr).has_event.store(0, Ordering::Relaxed);
        }
        *self.ooo.lock().unwrap() = Vec::new();
    }

    pub fn segment_fd(&self) -> Option<RawFd> {
        self.seg.fd()
    }
}

fn insert_ooo_range(ranges: &mut Vec<OooRange>, start: u32, length: u32) {
    if length == 0 {
        return;
    }
    let end = start + length;
    let mut i = 0usize;
    while i < ranges.len() {
        let r = &ranges[i];
        let r_end = r.start + r.length;
        if end < r.start {
            ranges.insert(i, OooRange { start, length });
            return;
        }
        if start <= r_end && end >= r.start {
            let new_start = start.min(r.start);
            let new_end = end.max(r_end);
            ranges[i] = OooRange {
                start: new_start,
                length: new_end - new_start,
            };
            while i + 1 < ranges.len() {
                let next = &ranges[i + 1];
                let next_end = next.start + next.length;
                if new_end >= next.start {
                    let merged_end = new_end.max(next_end);
                    ranges[i] = OooRange {
                        start: new_start,
                        length: merged_end - new_start,
                    };
                    ranges.remove(i + 1);
                } else {
                    break;
                }
            }
            return;
        }
        i += 1;
    }
    ranges.push(OooRange { start, length });
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
    fn enqueue_at_ooo_then_fill_gap() {
        let f = fifo(1 << 16);
        assert_eq!(f.enqueue_at(4, b"world"), 5);
        assert_eq!(f.max_dequeue(), 0);
        assert_eq!(f.enqueue_at(0, b"hell"), 4);
        assert_eq!(f.max_dequeue(), 9);
        let mut buf = [0u8; 16];
        assert_eq!(f.peek(0, 9, &mut buf), 9);
        assert_eq!(&buf[..9], b"hellworld");
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

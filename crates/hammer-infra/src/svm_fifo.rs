use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crossbeam_utils::CachePadded;

use crate::boxed::Slice;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SvmFifoError {
    InvalidCapacity,
}

impl fmt::Display for SvmFifoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCapacity => {
                write!(
                    f,
                    "capacity must be a power of two between 2 and {}",
                    u32::MAX
                )
            }
        }
    }
}

impl std::error::Error for SvmFifoError {}

#[repr(C)]
pub struct SvmFifoCursors {
    head: AtomicU32,
    tail: AtomicU32,
}

impl SvmFifoCursors {
    const fn new() -> Self {
        Self {
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
        }
    }
}

pub struct SvmFifo {
    mask: u32,
    capacity: u32,
    cursors: CachePadded<SvmFifoCursors>,
    want_notification: AtomicBool,
    data: Slice<u8>,
}

impl SvmFifo {
    pub fn with_capacity(capacity: usize) -> Result<Self, SvmFifoError> {
        if capacity < 2 || capacity > u32::MAX as usize || !capacity.is_power_of_two() {
            return Err(SvmFifoError::InvalidCapacity);
        }
        let capacity = capacity as u32;
        Ok(Self {
            mask: capacity - 1,
            capacity,
            cursors: CachePadded::new(SvmFifoCursors::new()),
            want_notification: AtomicBool::new(false),
            data: Slice::from_elem(capacity as usize, 0u8),
        })
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity as usize
    }

    #[inline]
    pub fn max_dequeue(&self) -> usize {
        let head = self.cursors.head.load(Ordering::Relaxed);
        let tail = self.cursors.tail.load(Ordering::Acquire);
        tail.wrapping_sub(head) as usize
    }

    #[inline]
    pub fn max_enqueue(&self) -> usize {
        let head = self.cursors.head.load(Ordering::Acquire);
        let tail = self.cursors.tail.load(Ordering::Relaxed);
        let used = tail.wrapping_sub(head);
        (self.capacity - used) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.max_dequeue() == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.max_enqueue() == 0
    }

    pub fn enqueue(&self, src: &[u8]) -> usize {
        let head = self.cursors.head.load(Ordering::Acquire);
        let tail = self.cursors.tail.load(Ordering::Relaxed);
        let used = tail.wrapping_sub(head);
        let free = self.capacity - used;
        let to_write = src.len().min(free as usize);
        if to_write == 0 {
            return 0;
        }

        let cap = self.capacity as usize;
        let tail_idx = (tail & self.mask) as usize;
        let data_ptr = self.data.as_ptr();
        let first_chunk = (cap - tail_idx).min(to_write);

        // SAFETY: Producer owns `tail`; unread bytes occupy `[head, tail)` and the
        // write targets `[tail, tail + to_write)`, which lies in the producer-reserved
        // free region `[tail, head + capacity)` modulo wrap. Slot indices stay within
        // `[0, capacity)` via `mask`. `src` is an external slice and does not alias
        // `data` storage.
        unsafe {
            std::ptr::copy_nonoverlapping(
                src.as_ptr(),
                data_ptr.add(tail_idx) as *mut u8,
                first_chunk,
            );
            if first_chunk < to_write {
                std::ptr::copy_nonoverlapping(
                    src.as_ptr().add(first_chunk),
                    data_ptr as *mut u8,
                    to_write - first_chunk,
                );
            }
        }

        self.cursors
            .tail
            .store(tail.wrapping_add(to_write as u32), Ordering::Release);
        to_write
    }

    pub fn peek(&self, offset: usize, len: usize, dst: &mut [u8]) -> usize {
        let head = self.cursors.head.load(Ordering::Relaxed);
        let tail = self.cursors.tail.load(Ordering::Acquire);
        let available = tail.wrapping_sub(head) as usize;
        if offset >= available {
            return 0;
        }

        let avail_from_offset = available - offset;
        let to_copy = len.min(dst.len()).min(avail_from_offset);
        if to_copy == 0 {
            return 0;
        }

        let cap = self.capacity as usize;
        let read_pos = head.wrapping_add(offset as u32);
        let read_idx = (read_pos & self.mask) as usize;
        let data_ptr = self.data.as_ptr();
        let first_chunk = (cap - read_idx).min(to_copy);

        // SAFETY: Consumer owns `head`; readable bytes occupy `[head, tail)` and the
        // peek reads `[head + offset, head + offset + to_copy)`, a subrange still
        // within `[head, tail)`. Indices stay within `[0, capacity)` via `mask`.
        // `dst` is an external slice and does not alias `data` storage.
        unsafe {
            std::ptr::copy_nonoverlapping(data_ptr.add(read_idx), dst.as_mut_ptr(), first_chunk);
            if first_chunk < to_copy {
                std::ptr::copy_nonoverlapping(
                    data_ptr,
                    dst.as_mut_ptr().add(first_chunk),
                    to_copy - first_chunk,
                );
            }
        }
        to_copy
    }

    pub fn dequeue_drop(&self, len: usize) -> usize {
        let head = self.cursors.head.load(Ordering::Relaxed);
        let tail = self.cursors.tail.load(Ordering::Acquire);
        let available = tail.wrapping_sub(head) as usize;
        let to_drop = len.min(available);
        if to_drop == 0 {
            return 0;
        }
        self.cursors
            .head
            .store(head.wrapping_add(to_drop as u32), Ordering::Release);
        to_drop
    }

    #[inline]
    pub fn want_notification(&self) {
        self.want_notification.store(true, Ordering::Release);
    }

    #[inline]
    pub fn clear_notification(&self) {
        self.want_notification.store(false, Ordering::Release);
    }

    pub fn should_signal(&self, wrote: usize) -> bool {
        if wrote == 0 {
            return false;
        }
        if !self.want_notification.load(Ordering::Acquire) {
            return false;
        }
        let tail = self.cursors.tail.load(Ordering::Acquire);
        let tail_before = tail.wrapping_sub(wrote as u32);
        let head = self.cursors.head.load(Ordering::Acquire);
        if head != tail_before {
            return false;
        }
        self.want_notification.store(false, Ordering::Release);
        true
    }

    pub fn clear(&self) {
        self.cursors.head.store(0, Ordering::Relaxed);
        self.cursors.tail.store(0, Ordering::Relaxed);
        self.want_notification.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn enqueue_peek_dequeue_roundtrip() {
        let fifo = SvmFifo::with_capacity(16).unwrap();
        let payload = b"hello";
        assert_eq!(fifo.enqueue(payload), payload.len());

        let mut buf = [0u8; 8];
        assert_eq!(fifo.peek(0, payload.len(), &mut buf), payload.len());
        assert_eq!(&buf[..payload.len()], payload);

        assert_eq!(fifo.dequeue_drop(payload.len()), payload.len());
        assert_eq!(fifo.peek(0, 8, &mut buf), 0);
        assert!(fifo.is_empty());
    }

    #[test]
    fn wrap_around() {
        let fifo = SvmFifo::with_capacity(8).unwrap();
        let first = [0u8, 1, 2, 3, 4, 5];
        assert_eq!(fifo.enqueue(&first), first.len());
        assert_eq!(fifo.dequeue_drop(4), 4);

        let second = [6u8, 7, 8, 9, 10];
        assert_eq!(fifo.enqueue(&second), second.len());

        let expected: Vec<u8> = (4u8..=10).collect();
        let mut buf = vec![0u8; expected.len()];
        assert_eq!(fifo.peek(0, expected.len(), &mut buf), expected.len());
        assert_eq!(buf, expected);
    }

    #[test]
    fn partial_write_on_full() {
        let fifo = SvmFifo::with_capacity(4).unwrap();
        assert_eq!(fifo.enqueue(&[1, 2, 3, 4]), 4);
        assert!(fifo.is_full());
        assert_eq!(fifo.enqueue(&[5]), 0);
    }

    #[test]
    fn peek_offset_beyond_available() {
        let fifo = SvmFifo::with_capacity(8).unwrap();
        assert_eq!(fifo.enqueue(&[1, 2, 3]), 3);
        let mut buf = [0u8; 4];
        assert_eq!(fifo.peek(3, 1, &mut buf), 0);
        assert_eq!(fifo.peek(4, 1, &mut buf), 0);
    }

    #[test]
    fn should_signal_edge_triggered() {
        let fifo = SvmFifo::with_capacity(8).unwrap();
        fifo.want_notification();
        assert!(fifo.should_signal(fifo.enqueue(&[1])));
        assert!(!fifo.want_notification.load(Ordering::Acquire));

        assert!(!fifo.should_signal(fifo.enqueue(&[2])));

        fifo.want_notification();
        assert!(!fifo.should_signal(fifo.enqueue(&[3])));
    }

    #[test]
    fn spsc_concurrent_no_loss() {
        const N: usize = 100_000;
        let fifo = Arc::new(SvmFifo::with_capacity(4096).unwrap());
        let producer_fifo = Arc::clone(&fifo);
        let consumer_fifo = Arc::clone(&fifo);
        let payload: Vec<u8> = (0..N).map(|i| (i % 256) as u8).collect();
        let expected = payload.clone();

        let producer = thread::spawn(move || {
            let mut sent = 0usize;
            while sent < N {
                let chunk_end = (sent + 64).min(N);
                let src = &payload[sent..chunk_end];
                let mut offset = 0usize;
                while offset < src.len() {
                    let wrote = producer_fifo.enqueue(&src[offset..]);
                    if wrote == 0 {
                        thread::yield_now();
                        continue;
                    }
                    offset += wrote;
                }
                sent = chunk_end;
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = Vec::with_capacity(N);
            while received.len() < N {
                let available = consumer_fifo.max_dequeue();
                if available == 0 {
                    thread::yield_now();
                    continue;
                }
                let mut buf = vec![0u8; available.min(256)];
                let peeked = consumer_fifo.peek(0, buf.len(), &mut buf);
                if peeked == 0 {
                    continue;
                }
                received.extend_from_slice(&buf[..peeked]);
                consumer_fifo.dequeue_drop(peeked);
            }
            received
        });

        producer.join().unwrap();
        let received = consumer.join().unwrap();
        assert_eq!(received, expected);
    }
}

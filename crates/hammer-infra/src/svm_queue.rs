//! Fixed-size typed queue backed by an [`SvmSegment`].

use std::alloc::{Layout, LayoutError};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::svm_segment::{SegmentBackend, SvmSegment, SvmSegmentError};

const QUEUE_MAGIC: u64 = 0x4841_4d4d_4552_5155;
const QUEUE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SvmQueueConfig {
    pub capacity: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum SvmQueueError {
    #[error("queue capacity must be positive")]
    ZeroCapacity,
    #[error("queue element type must have non-zero size")]
    ZeroSizedElement,
    #[error("queue layout is too large")]
    LayoutOverflow,
    #[error("queue capacity is full")]
    QueueFull,
    #[error("queue is empty")]
    Empty,
    #[error("queue wait deadline expired")]
    Timeout,
    #[error("queue header is invalid")]
    InvalidHeader,
    #[error("queue header version {version} is unsupported")]
    UnsupportedVersion { version: u32 },
    #[error(
        "queue element layout mismatch: requested {requested} bytes/{requested_alignment} alignment, stored {stored} bytes/{stored_alignment} alignment"
    )]
    ElementLayoutMismatch {
        requested: usize,
        requested_alignment: usize,
        stored: usize,
        stored_alignment: usize,
    },
    #[error("queue offset {offset} is outside the segment")]
    InvalidOffset { offset: u64 },
    #[error("queue segment operation failed: {0}")]
    Segment(#[from] SvmSegmentError),
    #[error("queue layout construction failed")]
    Layout(#[from] LayoutError),
}

#[repr(C, align(64))]
struct QueueHeader {
    magic: u64,
    version: u32,
    capacity: u32,
    element_size: u32,
    element_alignment: u32,
    head: AtomicU32,
    tail: AtomicU32,
    lock: AtomicU32,
}

pub struct SvmQueue<T> {
    segment: Arc<SvmSegment>,
    header_offset: u64,
    header: *mut QueueHeader,
    elements: *mut u8,
    _element: PhantomData<T>,
}

unsafe impl<T: Send> Send for SvmQueue<T> {}
unsafe impl<T: Send> Sync for SvmQueue<T> {}

impl<T> SvmQueue<T>
where
    T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    pub fn layout(config: &SvmQueueConfig) -> Result<Layout, SvmQueueError> {
        validate_config::<T>(config)?;
        let bytes = size_of::<QueueHeader>()
            .checked_add(
                (config.capacity as usize)
                    .checked_mul(size_of::<T>())
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)?;
        Layout::from_size_align(bytes, align_of::<QueueHeader>().max(align_of::<T>()))
            .map_err(SvmQueueError::Layout)
    }

    /// Initializes a queue at a segment-relative offset.
    ///
    /// The caller owns the allocation represented by `offset`; this method
    /// only writes the queue header and never allocates from the segment.
    pub unsafe fn init_at(
        segment: Arc<SvmSegment>,
        offset: u64,
        config: &SvmQueueConfig,
    ) -> Result<Self, SvmQueueError> {
        let layout = Self::layout(config)?;
        let header_ptr = segment.offset_ptr(offset, layout.size(), layout.align())?;
        let header = header_ptr.cast::<QueueHeader>();
        unsafe {
            std::ptr::write(
                header,
                QueueHeader {
                    magic: QUEUE_MAGIC,
                    version: QUEUE_VERSION,
                    capacity: config.capacity,
                    element_size: size_of::<T>() as u32,
                    element_alignment: align_of::<T>() as u32,
                    head: AtomicU32::new(0),
                    tail: AtomicU32::new(0),
                    lock: AtomicU32::new(0),
                },
            );
        }
        let elements = unsafe { header_ptr.add(size_of::<QueueHeader>()) };
        Ok(Self {
            segment,
            header_offset: offset,
            header,
            elements,
            _element: PhantomData,
        })
    }

    /// Attaches to an initialized queue without reconstructing any allocator.
    pub unsafe fn attach(segment: Arc<SvmSegment>, offset: u64) -> Result<Self, SvmQueueError> {
        let header_ptr =
            segment.offset_ptr(offset, size_of::<QueueHeader>(), align_of::<QueueHeader>())?;
        let header = header_ptr.cast::<QueueHeader>();
        let stored = unsafe { &*header };
        if stored.magic != QUEUE_MAGIC {
            return Err(SvmQueueError::InvalidHeader);
        }
        if stored.version != QUEUE_VERSION {
            return Err(SvmQueueError::UnsupportedVersion {
                version: stored.version,
            });
        }
        let bytes = size_of::<QueueHeader>()
            .checked_add(
                (stored.capacity as usize)
                    .checked_mul(stored.element_size as usize)
                    .ok_or(SvmQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmQueueError::LayoutOverflow)?;
        segment.offset_ptr(offset, bytes, align_of::<QueueHeader>())?;
        if stored.element_size as usize != size_of::<T>()
            || stored.element_alignment as usize != align_of::<T>()
        {
            return Err(SvmQueueError::ElementLayoutMismatch {
                requested: size_of::<T>(),
                requested_alignment: align_of::<T>(),
                stored: stored.element_size as usize,
                stored_alignment: stored.element_alignment as usize,
            });
        }
        if stored.capacity == 0 {
            return Err(SvmQueueError::ZeroCapacity);
        }
        let elements = unsafe { header_ptr.add(size_of::<QueueHeader>()) };
        Ok(Self {
            segment,
            header_offset: offset,
            header,
            elements,
            _element: PhantomData,
        })
    }

    pub fn segment(&self) -> &Arc<SvmSegment> {
        &self.segment
    }

    pub fn header_offset(&self) -> u64 {
        self.header_offset
    }

    pub fn capacity(&self) -> usize {
        unsafe { (*self.header).capacity as usize }
    }

    pub fn enqueue(&self, value: T) -> Result<(), SvmQueueError> {
        self.lock();
        let header = unsafe { &*self.header };
        let head = header.head.load(Ordering::Relaxed);
        let tail = header.tail.load(Ordering::Relaxed);
        if tail.wrapping_sub(head) >= header.capacity {
            self.unlock();
            return Err(SvmQueueError::QueueFull);
        }
        let slot = tail % header.capacity;
        unsafe {
            std::ptr::write(self.elements.cast::<T>().add(slot as usize), value);
        }
        header.tail.store(tail.wrapping_add(1), Ordering::Release);
        self.unlock();
        Ok(())
    }

    pub fn enqueue_pair(&self, first: T, second: T) -> Result<(), SvmQueueError> {
        self.lock();
        let header = unsafe { &*self.header };
        let head = header.head.load(Ordering::Relaxed);
        let tail = header.tail.load(Ordering::Relaxed);
        if (tail.wrapping_sub(head) as u64) + 2 > header.capacity as u64 {
            self.unlock();
            return Err(SvmQueueError::QueueFull);
        }
        let capacity = header.capacity;
        let elements = self.elements.cast::<T>();
        unsafe {
            std::ptr::write(elements.add((tail % capacity) as usize), first);
            std::ptr::write(elements.add(((tail + 1) % capacity) as usize), second);
        }
        header.tail.store(tail.wrapping_add(2), Ordering::Release);
        self.unlock();
        Ok(())
    }

    pub fn dequeue(&self) -> Result<Option<T>, SvmQueueError> {
        self.lock();
        let header = unsafe { &*self.header };
        let head = header.head.load(Ordering::Relaxed);
        let tail = header.tail.load(Ordering::Acquire);
        if head == tail {
            self.unlock();
            return Ok(None);
        }
        let value = unsafe {
            std::ptr::read(
                self.elements
                    .cast::<T>()
                    .add((head % header.capacity) as usize),
            )
        };
        header.head.store(head.wrapping_add(1), Ordering::Release);
        self.unlock();
        Ok(Some(value))
    }

    pub fn dequeue_batch(&self, destination: &mut [T]) -> Result<usize, SvmQueueError> {
        if destination.is_empty() {
            return Ok(0);
        }
        self.lock();
        let header = unsafe { &*self.header };
        let mut head = header.head.load(Ordering::Relaxed);
        let tail = header.tail.load(Ordering::Acquire);
        let count = (tail.wrapping_sub(head) as usize).min(destination.len());
        let elements = self.elements.cast::<T>();
        for value in destination.iter_mut().take(count) {
            *value = unsafe { std::ptr::read(elements.add((head % header.capacity) as usize)) };
            head = head.wrapping_add(1);
        }
        if count != 0 {
            header.head.store(head, Ordering::Release);
        }
        self.unlock();
        Ok(count)
    }

    pub fn enqueue_wait(&self, value: T) -> Result<(), SvmQueueError> {
        loop {
            match self.enqueue(value) {
                Ok(()) => return Ok(()),
                Err(SvmQueueError::QueueFull) => thread::yield_now(),
                Err(error) => return Err(error),
            }
        }
    }

    pub fn dequeue_wait(&self) -> Result<T, SvmQueueError> {
        loop {
            match self.dequeue()? {
                Some(value) => return Ok(value),
                None => thread::yield_now(),
            }
        }
    }

    pub fn dequeue_until(&self, deadline: Instant) -> Result<T, SvmQueueError> {
        loop {
            match self.dequeue()? {
                Some(value) => return Ok(value),
                None if Instant::now() >= deadline => return Err(SvmQueueError::Timeout),
                None => thread::sleep(Duration::from_millis(1)),
            }
        }
    }

    pub fn len(&self) -> Result<usize, SvmQueueError> {
        self.lock();
        let header = unsafe { &*self.header };
        let len = header
            .tail
            .load(Ordering::Acquire)
            .wrapping_sub(header.head.load(Ordering::Acquire)) as usize;
        self.unlock();
        Ok(len)
    }

    pub fn is_empty(&self) -> Result<bool, SvmQueueError> {
        Ok(self.len()? == 0)
    }

    fn lock(&self) {
        let lock = unsafe { &(*self.header).lock };
        while lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            thread::yield_now();
        }
    }

    fn unlock(&self) {
        unsafe {
            (*self.header).lock.store(0, Ordering::Release);
        }
    }
}

fn validate_config<T>(config: &SvmQueueConfig) -> Result<(), SvmQueueError>
where
    T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    if config.capacity == 0 {
        return Err(SvmQueueError::ZeroCapacity);
    }
    if size_of::<T>() == 0 {
        return Err(SvmQueueError::ZeroSizedElement);
    }
    if size_of::<T>() > u32::MAX as usize || align_of::<T>() > u32::MAX as usize {
        return Err(SvmQueueError::LayoutOverflow);
    }
    Ok(())
}

#[allow(dead_code)]
fn _backend_is_shared(segment: &SvmSegment) -> bool {
    matches!(
        segment.backend(),
        SegmentBackend::Memfd | SegmentBackend::Shm
    )
}

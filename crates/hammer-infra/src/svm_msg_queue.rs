//! Unified descriptor/data-ring message queue.

use std::alloc::{Layout, LayoutError};
use std::marker::PhantomData;
use std::mem::{align_of, size_of};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

use crate::align::align_up;
use crate::svm_segment::{SvmSegment, SvmSegmentError};

const MSG_QUEUE_MAGIC: u64 = 0x4841_4d4d_4552_4d51;
const MSG_QUEUE_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsgDescriptor {
    ring_index: u32,
    element_index: u32,
}

impl MsgDescriptor {
    pub fn ring_index(self) -> u32 {
        self.ring_index
    }
    pub fn element_index(self) -> u32 {
        self.element_index
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsgRingConfig {
    pub capacity: u32,
    pub element_size: usize,
    pub element_alignment: usize,
}

impl MsgRingConfig {
    pub fn new<T>(capacity: u32) -> Self {
        Self {
            capacity,
            element_size: size_of::<T>(),
            element_alignment: align_of::<T>(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SvmMsgQueueConfig<'a> {
    pub capacity: u32,
    pub rings: &'a [MsgRingConfig],
}

#[derive(Debug, thiserror::Error)]
pub enum SvmMsgQueueError {
    #[error("message queue descriptor capacity must be positive")]
    ZeroCapacity,
    #[error("message queue must contain at least one ring")]
    NoRings,
    #[error("message ring {ring} capacity must be positive")]
    ZeroRingCapacity { ring: u32 },
    #[error("message ring {ring} has zero-sized elements")]
    ZeroSizedElement { ring: u32 },
    #[error("message queue layout is too large")]
    LayoutOverflow,
    #[error("message descriptor queue is full")]
    QueueFull,
    #[error("message ring {ring} is full")]
    RingFull { ring: u32 },
    #[error("message ring {ring} does not exist")]
    InvalidRing { ring: u32 },
    #[error(
        "message element layout mismatch for ring {ring}: requested {requested} bytes/{requested_alignment} alignment, stored {stored} bytes/{stored_alignment} alignment"
    )]
    ElementLayoutMismatch {
        ring: u32,
        requested: usize,
        requested_alignment: usize,
        stored: usize,
        stored_alignment: usize,
    },
    #[error("message queue is empty")]
    Empty,
    #[error("message queue wait deadline expired")]
    Timeout,
    #[error("message queue header is invalid")]
    InvalidHeader,
    #[error("message queue header version {version} is unsupported")]
    UnsupportedVersion { version: u32 },
    #[error("message queue segment operation failed: {0}")]
    Segment(#[from] SvmSegmentError),
    #[error("message queue layout construction failed")]
    Layout(#[from] LayoutError),
}

#[repr(C, align(64))]
struct MessageQueueHeader {
    magic: u64,
    version: u32,
    descriptor_capacity: u32,
    ring_count: u32,
    _reserved: u32,
    descriptor_offset: u64,
    ring_offset: u64,
    head: u32,
    tail: u32,
    len: u32,
    lock: AtomicU32,
}

#[repr(C)]
struct MessageRingHeader {
    capacity: u32,
    element_size: u32,
    element_alignment: u32,
    _reserved: u32,
    elements_offset: u64,
    head: u32,
    tail: u32,
    occupied: u32,
    _padding: u32,
}

pub struct SvmMsgQueue<'segment> {
    segment: &'segment SvmSegment,
    header_offset: u64,
    header: *mut MessageQueueHeader,
    descriptors: *mut MsgDescriptor,
    rings: *mut MessageRingHeader,
}

unsafe impl Send for SvmMsgQueue<'_> {}
unsafe impl Sync for SvmMsgQueue<'_> {}

impl<'segment> SvmMsgQueue<'segment> {
    pub fn layout(config: &SvmMsgQueueConfig<'_>) -> Result<Layout, SvmMsgQueueError> {
        validate_config(config)?;
        let mut bytes = size_of::<MessageQueueHeader>();
        bytes = bytes
            .checked_add(
                (config.capacity as usize)
                    .checked_mul(size_of::<MsgDescriptor>())
                    .ok_or(SvmMsgQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmMsgQueueError::LayoutOverflow)?;
        bytes = bytes
            .checked_add(
                config
                    .rings
                    .len()
                    .checked_mul(size_of::<MessageRingHeader>())
                    .ok_or(SvmMsgQueueError::LayoutOverflow)?,
            )
            .ok_or(SvmMsgQueueError::LayoutOverflow)?;
        for ring in config.rings {
            let aligned = bytes
                .checked_add(ring.element_alignment - 1)
                .ok_or(SvmMsgQueueError::LayoutOverflow)?;
            let aligned = align_up(aligned, ring.element_alignment);
            let ring_bytes = (ring.capacity as usize)
                .checked_mul(ring.element_size)
                .ok_or(SvmMsgQueueError::LayoutOverflow)?;
            bytes = aligned
                .checked_add(ring_bytes)
                .ok_or(SvmMsgQueueError::LayoutOverflow)?;
        }
        Layout::from_size_align(bytes, 64).map_err(SvmMsgQueueError::Layout)
    }

    pub unsafe fn init_at(
        segment: &'segment SvmSegment,
        offset: u64,
        config: &SvmMsgQueueConfig<'_>,
    ) -> Result<Self, SvmMsgQueueError> {
        let layout = Self::layout(config)?;
        let base = segment.offset_ptr(offset, layout.size(), layout.align())?;
        let header = base.cast::<MessageQueueHeader>();
        let descriptor_offset = offset
            .checked_add(size_of::<MessageQueueHeader>() as u64)
            .ok_or(SvmMsgQueueError::LayoutOverflow)?;
        let ring_offset = descriptor_offset
            .checked_add(
                (config.capacity as usize)
                    .checked_mul(size_of::<MsgDescriptor>())
                    .ok_or(SvmMsgQueueError::LayoutOverflow)? as u64,
            )
            .ok_or(SvmMsgQueueError::LayoutOverflow)?;
        unsafe {
            std::ptr::write(
                header,
                MessageQueueHeader {
                    magic: MSG_QUEUE_MAGIC,
                    version: MSG_QUEUE_VERSION,
                    descriptor_capacity: config.capacity,
                    ring_count: config.rings.len() as u32,
                    _reserved: 0,
                    descriptor_offset,
                    ring_offset,
                    head: 0,
                    tail: 0,
                    len: 0,
                    lock: AtomicU32::new(0),
                },
            );
        }
        let descriptors = unsafe {
            base.add(size_of::<MessageQueueHeader>())
                .cast::<MsgDescriptor>()
        };
        let rings = segment
            .offset_ptr(
                ring_offset,
                config.rings.len() * size_of::<MessageRingHeader>(),
                8,
            )?
            .cast::<MessageRingHeader>();
        let mut cursor = ring_offset
            .checked_add((config.rings.len() * size_of::<MessageRingHeader>()) as u64)
            .ok_or(SvmMsgQueueError::LayoutOverflow)?;
        for (index, ring) in config.rings.iter().enumerate() {
            let ring_header = unsafe { &mut *rings.add(index) };
            ring_header.capacity = ring.capacity;
            ring_header.element_size = ring.element_size as u32;
            ring_header.element_alignment = ring.element_alignment as u32;
            ring_header._reserved = 0;
            ring_header.head = 0;
            ring_header.tail = 0;
            ring_header.occupied = 0;
            ring_header._padding = 0;
            let aligned = (cursor as usize)
                .checked_add(ring.element_alignment - 1)
                .ok_or(SvmMsgQueueError::LayoutOverflow)?;
            cursor = align_up(aligned, ring.element_alignment) as u64;
            ring_header.elements_offset = cursor;
            cursor = cursor
                .checked_add(
                    (ring.capacity as usize)
                        .checked_mul(ring.element_size)
                        .ok_or(SvmMsgQueueError::LayoutOverflow)? as u64,
                )
                .ok_or(SvmMsgQueueError::LayoutOverflow)?;
        }
        Ok(Self {
            segment,
            header_offset: offset,
            header,
            descriptors,
            rings,
        })
    }

    pub unsafe fn attach(
        segment: &'segment SvmSegment,
        offset: u64,
    ) -> Result<Self, SvmMsgQueueError> {
        let base = segment.offset_ptr(offset, size_of::<MessageQueueHeader>(), 64)?;
        let header = base.cast::<MessageQueueHeader>();
        let stored = unsafe { &*header };
        if stored.magic != MSG_QUEUE_MAGIC {
            return Err(SvmMsgQueueError::InvalidHeader);
        }
        if stored.version != MSG_QUEUE_VERSION {
            return Err(SvmMsgQueueError::UnsupportedVersion {
                version: stored.version,
            });
        }
        if stored.descriptor_capacity == 0 || stored.ring_count == 0 {
            return Err(SvmMsgQueueError::InvalidHeader);
        }
        let descriptors = segment
            .offset_ptr(
                stored.descriptor_offset,
                stored.descriptor_capacity as usize * size_of::<MsgDescriptor>(),
                8,
            )?
            .cast();
        let rings = segment
            .offset_ptr(
                stored.ring_offset,
                stored.ring_count as usize * size_of::<MessageRingHeader>(),
                8,
            )?
            .cast::<MessageRingHeader>();
        for index in 0..stored.ring_count as usize {
            let ring = unsafe { &*rings.add(index) };
            if ring.capacity == 0 || ring.element_size == 0 {
                return Err(SvmMsgQueueError::InvalidHeader);
            }
            segment.offset_ptr(
                ring.elements_offset,
                ring.capacity as usize * ring.element_size as usize,
                ring.element_alignment as usize,
            )?;
        }
        Ok(Self {
            segment,
            header_offset: offset,
            header,
            descriptors,
            rings,
        })
    }

    pub fn header_offset(&self) -> u64 {
        self.header_offset
    }

    pub fn reserve<T>(
        &self,
        ring_index: u32,
    ) -> Result<MsgReservation<'_, 'segment, T>, SvmMsgQueueError>
    where
        T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
    {
        self.lock();
        let header = unsafe { &mut *self.header };
        if ring_index >= header.ring_count {
            self.unlock();
            return Err(SvmMsgQueueError::InvalidRing { ring: ring_index });
        }
        let ring = unsafe { &mut *self.rings.add(ring_index as usize) };
        check_ring_layout::<T>(ring_index, ring)?;
        if header.len >= header.descriptor_capacity {
            self.unlock();
            return Err(SvmMsgQueueError::QueueFull);
        }
        if ring.occupied >= ring.capacity {
            self.unlock();
            return Err(SvmMsgQueueError::RingFull { ring: ring_index });
        }
        let slot = ring.tail % ring.capacity;
        Ok(MsgReservation {
            queue: self,
            ring_index,
            slot,
            committed: false,
            _element: PhantomData,
        })
    }

    pub fn dequeue<T>(&self) -> Result<Option<Message<'_, 'segment, T>>, SvmMsgQueueError>
    where
        T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
    {
        self.lock();
        let header = unsafe { &mut *self.header };
        if header.len == 0 {
            self.unlock();
            return Ok(None);
        }
        let descriptor = unsafe {
            *self
                .descriptors
                .add((header.head % header.descriptor_capacity) as usize)
        };
        if descriptor.ring_index >= header.ring_count {
            self.unlock();
            return Err(SvmMsgQueueError::InvalidRing {
                ring: descriptor.ring_index,
            });
        }
        let ring = unsafe { &mut *self.rings.add(descriptor.ring_index as usize) };
        if let Err(error) = check_ring_layout::<T>(descriptor.ring_index, ring) {
            self.unlock();
            return Err(error);
        }
        header.head = header.head.wrapping_add(1);
        header.len -= 1;
        Ok(Some(Message {
            queue: self,
            descriptor,
            released: false,
            _element: PhantomData,
        }))
    }

    pub fn wait_nonempty(&self) -> Result<(), SvmMsgQueueError> {
        while self.is_empty()? {
            std::thread::yield_now();
        }
        Ok(())
    }

    pub fn wait_nonempty_until(&self, deadline: Instant) -> Result<(), SvmMsgQueueError> {
        while self.is_empty()? {
            if Instant::now() >= deadline {
                return Err(SvmMsgQueueError::Timeout);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    pub fn wait_space(&self, ring: u32) -> Result<(), SvmMsgQueueError> {
        while self.ring_is_full(ring)? {
            std::thread::yield_now();
        }
        Ok(())
    }

    pub fn len(&self) -> Result<usize, SvmMsgQueueError> {
        self.lock();
        let len = unsafe { (*self.header).len as usize };
        self.unlock();
        Ok(len)
    }

    pub fn is_empty(&self) -> Result<bool, SvmMsgQueueError> {
        Ok(self.len()? == 0)
    }

    fn ring_is_full(&self, ring_index: u32) -> Result<bool, SvmMsgQueueError> {
        self.lock();
        let header = unsafe { &*self.header };
        if ring_index >= header.ring_count {
            self.unlock();
            return Err(SvmMsgQueueError::InvalidRing { ring: ring_index });
        }
        let full = unsafe {
            (*self.rings.add(ring_index as usize)).occupied
                >= (*self.rings.add(ring_index as usize)).capacity
        };
        self.unlock();
        Ok(full)
    }

    fn lock(&self) {
        let lock = unsafe { &(*self.header).lock };
        while lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::thread::yield_now();
        }
    }

    fn unlock(&self) {
        unsafe {
            (*self.header).lock.store(0, Ordering::Release);
        }
    }

    fn element_ptr<T>(&self, ring_index: u32, slot: u32) -> *mut T {
        let ring = unsafe { &*self.rings.add(ring_index as usize) };
        let base = self.segment.base();
        unsafe {
            base.add(ring.elements_offset as usize)
                .cast::<T>()
                .add(slot as usize)
        }
    }
}

pub struct MsgReservation<'queue, 'segment, T> {
    queue: &'queue SvmMsgQueue<'segment>,
    ring_index: u32,
    slot: u32,
    committed: bool,
    _element: PhantomData<T>,
}

impl<T> MsgReservation<'_, '_, T>
where
    T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    pub fn value_mut(&mut self) -> &mut T {
        unsafe { &mut *self.queue.element_ptr::<T>(self.ring_index, self.slot) }
    }

    pub fn commit(mut self) -> Result<(), SvmMsgQueueError> {
        let header = unsafe { &mut *self.queue.header };
        let ring = unsafe { &mut *self.queue.rings.add(self.ring_index as usize) };
        let descriptor_slot = header.tail % header.descriptor_capacity;
        unsafe {
            *self.queue.descriptors.add(descriptor_slot as usize) = MsgDescriptor {
                ring_index: self.ring_index,
                element_index: self.slot,
            };
        }
        ring.tail = ring.tail.wrapping_add(1);
        ring.occupied += 1;
        header.tail = header.tail.wrapping_add(1);
        header.len += 1;
        self.committed = true;
        self.queue.unlock();
        Ok(())
    }
}

impl<T> Drop for MsgReservation<'_, '_, T> {
    fn drop(&mut self) {
        if !self.committed {
            self.queue.unlock();
        }
    }
}

pub struct Message<'queue, 'segment, T> {
    queue: &'queue SvmMsgQueue<'segment>,
    descriptor: MsgDescriptor,
    released: bool,
    _element: PhantomData<T>,
}

impl<T> Message<'_, '_, T> {
    pub fn ring_index(&self) -> u32 {
        self.descriptor.ring_index
    }

    pub fn value(&self) -> &T
    where
        T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
    {
        unsafe {
            &*self
                .queue
                .element_ptr::<T>(self.descriptor.ring_index, self.descriptor.element_index)
        }
    }

    pub fn release(mut self) -> Result<(), SvmMsgQueueError> {
        self.release_inner();
        Ok(())
    }

    fn release_inner(&mut self) {
        if self.released {
            return;
        }
        let ring = unsafe { &mut *self.queue.rings.add(self.descriptor.ring_index as usize) };
        assert!(ring.occupied != 0, "message ring occupancy underflow");
        ring.head = ring.head.wrapping_add(1);
        ring.occupied -= 1;
        self.released = true;
        self.queue.unlock();
    }
}

impl<T> Drop for Message<'_, '_, T> {
    fn drop(&mut self) {
        self.release_inner();
    }
}

fn validate_config(config: &SvmMsgQueueConfig<'_>) -> Result<(), SvmMsgQueueError> {
    if config.capacity == 0 {
        return Err(SvmMsgQueueError::ZeroCapacity);
    }
    if config.rings.is_empty() {
        return Err(SvmMsgQueueError::NoRings);
    }
    for (index, ring) in config.rings.iter().enumerate() {
        if ring.capacity == 0 {
            return Err(SvmMsgQueueError::ZeroRingCapacity { ring: index as u32 });
        }
        if ring.element_size == 0 {
            return Err(SvmMsgQueueError::ZeroSizedElement { ring: index as u32 });
        }
        if ring.element_alignment == 0
            || !ring.element_alignment.is_power_of_two()
            || ring.element_size > u32::MAX as usize
            || ring.element_alignment > u32::MAX as usize
        {
            return Err(SvmMsgQueueError::LayoutOverflow);
        }
    }
    Ok(())
}

fn check_ring_layout<T>(ring_index: u32, ring: &MessageRingHeader) -> Result<(), SvmMsgQueueError>
where
    T: Copy + FromBytes + IntoBytes + KnownLayout + Immutable,
{
    if ring.element_size as usize != size_of::<T>()
        || ring.element_alignment as usize != align_of::<T>()
    {
        return Err(SvmMsgQueueError::ElementLayoutMismatch {
            ring: ring_index,
            requested: size_of::<T>(),
            requested_alignment: align_of::<T>(),
            stored: ring.element_size as usize,
            stored_alignment: ring.element_alignment as usize,
        });
    }
    Ok(())
}

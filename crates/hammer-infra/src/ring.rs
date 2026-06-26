use std::cell::UnsafeCell;
use std::fmt;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU32, Ordering};

use crossbeam_utils::CachePadded;

use crate::boxed::Slice;
use crate::prefetch::prefetch_read_l1;
use crate::vec::Vec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubmissionDescriptor<Opcode, UserData, Object, Payload> {
    opcode: Opcode,
    user_data: UserData,
    object: Object,
    payload: Payload,
}

impl<Opcode, UserData, Object, Payload> SubmissionDescriptor<Opcode, UserData, Object, Payload> {
    #[inline]
    pub const fn new(
        opcode: Opcode,
        user_data: UserData,
        object: Object,
        payload: Payload,
    ) -> Self {
        Self {
            opcode,
            user_data,
            object,
            payload,
        }
    }

    #[inline]
    pub fn opcode(&self) -> Opcode
    where
        Opcode: Copy,
    {
        self.opcode
    }

    #[inline]
    pub fn user_data(&self) -> UserData
    where
        UserData: Copy,
    {
        self.user_data
    }

    #[inline]
    pub fn object(&self) -> Object
    where
        Object: Copy,
    {
        self.object
    }

    #[inline]
    pub fn payload(&self) -> Payload
    where
        Payload: Copy,
    {
        self.payload
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletionDescriptor<UserData, ResultCode, Flags, Object, Payload> {
    user_data: UserData,
    result: ResultCode,
    flags: Flags,
    object: Object,
    payload: Payload,
}

impl<UserData, ResultCode, Flags, Object, Payload>
    CompletionDescriptor<UserData, ResultCode, Flags, Object, Payload>
{
    #[inline]
    pub const fn new(
        user_data: UserData,
        result: ResultCode,
        flags: Flags,
        object: Object,
        payload: Payload,
    ) -> Self {
        Self {
            user_data,
            result,
            flags,
            object,
            payload,
        }
    }

    #[inline]
    pub fn user_data(&self) -> UserData
    where
        UserData: Copy,
    {
        self.user_data
    }

    #[inline]
    pub fn result(&self) -> ResultCode
    where
        ResultCode: Copy,
    {
        self.result
    }

    #[inline]
    pub fn flags(&self) -> Flags
    where
        Flags: Copy,
    {
        self.flags
    }

    #[inline]
    pub fn object(&self) -> Object
    where
        Object: Copy,
    {
        self.object
    }

    #[inline]
    pub fn payload(&self) -> Payload
    where
        Payload: Copy,
    {
        self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RingEntry<Descriptor, Attachment> {
    descriptor: Descriptor,
    attachment: Option<Attachment>,
}

impl<Descriptor, Attachment> RingEntry<Descriptor, Attachment> {
    #[inline]
    pub const fn new(descriptor: Descriptor) -> Self {
        Self {
            descriptor,
            attachment: None,
        }
    }

    #[inline]
    pub fn with_attachment(descriptor: Descriptor, attachment: Attachment) -> Self {
        Self {
            descriptor,
            attachment: Some(attachment),
        }
    }

    #[inline]
    pub fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    #[inline]
    pub fn attachment(&self) -> Option<&Attachment> {
        self.attachment.as_ref()
    }

    #[inline]
    pub fn into_parts(self) -> (Descriptor, Option<Attachment>) {
        (self.descriptor, self.attachment)
    }
}

pub struct LocalRing<T> {
    slots: Vec<Option<T>>,
    head: usize,
    len: usize,
}

impl<T> LocalRing<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            slots: Vec::new(),
            head: 0,
            len: 0,
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(None);
        }
        Self {
            slots,
            head: 0,
            len: 0,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.len == self.capacity()
    }

    #[inline]
    pub fn front(&self) -> Option<&T> {
        if self.len == 0 {
            return None;
        }
        self.slots[self.head].as_ref()
    }

    #[inline]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        if self.len == 0 {
            return None;
        }
        self.slots[self.head].as_mut()
    }

    #[inline]
    pub fn try_push(&mut self, value: T) -> Result<(), T> {
        if self.is_full() {
            return Err(value);
        }
        let slot = if self.capacity() == 0 {
            0
        } else {
            (self.head + self.len) % self.capacity()
        };
        self.slots[slot] = Some(value);
        self.len += 1;
        Ok(())
    }

    #[inline]
    pub fn pop(&mut self) -> Option<T> {
        if self.len == 0 {
            return None;
        }

        let value = self.slots[self.head].take();
        debug_assert!(
            value.is_some(),
            "occupied slot expected while ring is non-empty"
        );

        self.len -= 1;
        if self.len == 0 {
            self.head = 0;
        } else {
            self.head = (self.head + 1) % self.capacity();
        }

        value
    }

    #[inline]
    pub fn clear(&mut self) {
        if self.len == 0 {
            return;
        }

        for offset in 0..self.len {
            let slot = (self.head + offset) % self.capacity();
            let _ = self.slots[slot].take();
        }
        self.head = 0;
        self.len = 0;
    }
}

impl<T> Default for LocalRing<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for LocalRing<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalRing")
            .field("len", &self.len)
            .field("capacity", &self.capacity())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RingSlotId(u32);

impl RingSlotId {
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn value(self) -> u32 {
        self.0
    }

    #[inline]
    const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

pub struct IndexedRing<T> {
    ready: LocalRing<RingSlotId>,
    slots: Vec<Option<T>>,
    free: Vec<RingSlotId>,
}

impl<T> IndexedRing<T> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            ready: LocalRing::new(),
            slots: Vec::new(),
            free: Vec::new(),
        }
    }

    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        assert!(
            u32::try_from(capacity).is_ok(),
            "indexed ring capacity exceeds u32 slots"
        );
        let mut slots = Vec::with_capacity(capacity);
        let mut free = Vec::with_capacity(capacity);
        for index in 0..capacity {
            slots.push(None);
            let slot = RingSlotId::new((capacity - index - 1) as u32);
            free.push(slot);
        }
        Self {
            ready: LocalRing::with_capacity(capacity),
            slots,
            free,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.ready.len()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.free.is_empty()
    }

    #[inline]
    pub fn entry(&self, slot: RingSlotId) -> Option<&T> {
        self.slots.get(slot.as_usize()).and_then(Option::as_ref)
    }

    #[inline]
    pub fn entry_mut(&mut self, slot: RingSlotId) -> Option<&mut T> {
        self.slots.get_mut(slot.as_usize()).and_then(Option::as_mut)
    }

    #[inline]
    pub fn try_push(&mut self, value: T) -> Result<RingSlotId, T> {
        let Some(slot) = self.free.pop() else {
            return Err(value);
        };
        self.slots[slot.as_usize()] = Some(value);
        if self.ready.try_push(slot).is_err() {
            let value = self.slots[slot.as_usize()]
                .take()
                .expect("slot was just populated");
            self.free.push(slot);
            return Err(value);
        }
        Ok(slot)
    }

    #[inline]
    pub fn pop(&mut self) -> Option<(RingSlotId, T)> {
        let slot = self.ready.pop()?;
        let value = self.slots[slot.as_usize()]
            .take()
            .expect("ready slot must have an entry");
        self.free.push(slot);
        Some((slot, value))
    }

    #[inline]
    pub fn clear(&mut self) {
        while self.pop().is_some() {}
    }
}

impl<T> Default for IndexedRing<T> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for IndexedRing<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexedRing")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingError<T = ()> {
    InvalidCapacity,
    Full(T),
}

pub struct LockFreeRingHeadTail {
    head: AtomicU32,
    tail: AtomicU32,
}

impl LockFreeRingHeadTail {
    #[inline]
    pub const fn new() -> Self {
        Self {
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
        }
    }

    #[inline]
    pub fn head(&self) -> u32 {
        self.head.load(Ordering::Acquire)
    }

    #[inline]
    pub fn tail(&self) -> u32 {
        self.tail.load(Ordering::Acquire)
    }
}

impl Default for LockFreeRingHeadTail {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for LockFreeRingHeadTail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockFreeRingHeadTail")
            .field("head", &self.head())
            .field("tail", &self.tail())
            .finish()
    }
}

#[repr(C)]
pub struct LockFreeRingCursors {
    producer: CachePadded<LockFreeRingHeadTail>,
    consumer: CachePadded<LockFreeRingHeadTail>,
}

impl LockFreeRingCursors {
    pub const PRODUCER_CACHELINE_OFFSET: usize = 0;
    pub const CONSUMER_CACHELINE_OFFSET: usize =
        std::mem::size_of::<CachePadded<LockFreeRingHeadTail>>();

    #[inline]
    pub const fn new() -> Self {
        Self {
            producer: CachePadded::new(LockFreeRingHeadTail::new()),
            consumer: CachePadded::new(LockFreeRingHeadTail::new()),
        }
    }
}

impl Default for LockFreeRingCursors {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for LockFreeRingCursors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockFreeRingCursors")
            .field("producer", &self.producer)
            .field("consumer", &self.consumer)
            .finish()
    }
}

#[repr(C, align(64))]
pub struct LockFreeRingSlot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> LockFreeRingSlot<T> {
    #[inline]
    const fn uninit() -> Self {
        Self {
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

pub struct LockFreeRing<T: Copy> {
    size: u32,
    mask: u32,
    capacity: u32,
    cursors: LockFreeRingCursors,
    slots: Slice<LockFreeRingSlot<T>>,
}

unsafe impl<T: Copy + Send> Send for LockFreeRing<T> {}
unsafe impl<T: Copy + Send> Sync for LockFreeRing<T> {}

impl<T: Copy> LockFreeRing<T> {
    pub fn with_capacity(size: usize) -> Result<Self, RingError> {
        if size < 2 || !size.is_power_of_two() || size > u32::MAX as usize {
            return Err(RingError::InvalidCapacity);
        }
        let slots = Slice::from_fn(size, |_| LockFreeRingSlot::uninit());
        Ok(Self {
            size: size as u32,
            mask: size as u32 - 1,
            capacity: size as u32 - 1,
            cursors: LockFreeRingCursors::new(),
            slots,
        })
    }

    #[inline]
    pub fn ring_size(&self) -> usize {
        self.size as usize
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity as usize
    }

    #[inline]
    pub fn available_to_read(&self) -> usize {
        let prod_tail = self.cursors.producer.tail.load(Ordering::Acquire);
        let cons_head = self.cursors.consumer.head.load(Ordering::Acquire);
        prod_tail.wrapping_sub(cons_head) as usize
    }

    #[inline]
    pub fn available_to_write(&self) -> usize {
        let cons_tail = self.cursors.consumer.tail.load(Ordering::Acquire);
        let prod_head = self.cursors.producer.head.load(Ordering::Acquire);
        self.mask.wrapping_add(cons_tail).wrapping_sub(prod_head) as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.available_to_read() == 0
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.available_to_write() == 0
    }

    /// Multi-producer enqueue.
    ///
    /// DPDK MP/MC CAS algorithm: a producer atomically reserves a slot by CASing
    /// `producer.head` forward, writes the value, then waits for its turn
    /// (`producer.tail == reserved_head`) before publishing `producer.tail`
    /// with Release ordering. The Release publish pairs with the Acquire load
    /// of `producer.tail` in `dequeue`, so the slot write is visible to
    /// consumers before they observe the new tail.
    ///
    /// `T: Copy` guarantees slot reads do not leave ownership behind, so the
    /// slot is logically vacant once `consumer.tail` advances past it.
    ///
    /// `head`/`tail` are wrapping `u32` counters; the slot index is `head &
    /// mask`. ABA is bounded by u32 space (capacity - 1): a producer that
    /// stalled through `u32::MAX` wraps would observe a stale `head` and CAS
    /// loop until it reloads the current value. With batch sizes and producer
    /// counts in the app-ring regime this wrap is effectively unreachable;
    /// callers must not enqueue more than `u32::MAX` items over the lifetime of
    /// a single ring without a wrap-aware cursor (documented assumption).
    #[inline]
    pub fn enqueue(&self, value: T) -> Result<(), RingError<T>> {
        let prod_head = loop {
            let head = self.cursors.producer.head.load(Ordering::Acquire);
            let cons_tail = self.cursors.consumer.tail.load(Ordering::Acquire);
            let free = self.mask.wrapping_add(cons_tail).wrapping_sub(head);
            if free == 0 {
                return Err(RingError::Full(value));
            }
            let next = head.wrapping_add(1);
            match self.cursors.producer.head.compare_exchange_weak(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break head,
                Err(_) => std::hint::spin_loop(),
            }
        };

        let slot = (prod_head & self.mask) as usize;
        // SAFETY: `prod_head & mask` selects a slot inside the power-of-two
        // table. The CAS above reserved this slot exclusively for this
        // producer until `producer.tail` is published below. No other producer
        // writes to it and no consumer reads it until the Release store of
        // `producer.tail` pairs with their Acquire load.
        unsafe {
            (*self.slots[slot].value.get()).write(value);
        }

        // Wait for prior producers to publish their tail so we preserve
        // FIFO order: only the producer whose reserved head matches the
        // current tail may publish. Other producers may still be writing
        // their slots; we must not overtake them or a consumer could read
        // an uninitialised slot between our publish and theirs.
        while self.cursors.producer.tail.load(Ordering::Acquire) != prod_head {
            std::hint::spin_loop();
        }
        self.cursors
            .producer
            .tail
            .store(prod_head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Multi-consumer dequeue.
    ///
    /// Symmetric to `enqueue`: a consumer CASes `consumer.head` forward to
    /// reserve a slot, reads the value (Acquire load of `producer.tail` proves
    /// the slot was published), waits for its turn, then publishes
    /// `consumer.tail` with Release ordering so producers observe the freed
    /// slot in order.
    #[inline]
    pub fn dequeue(&self) -> Option<T> {
        let cons_head = loop {
            let head = self.cursors.consumer.head.load(Ordering::Acquire);
            let prod_tail = self.cursors.producer.tail.load(Ordering::Acquire);
            let entries = prod_tail.wrapping_sub(head);
            if entries == 0 {
                return None;
            }
            let next = head.wrapping_add(1);
            match self.cursors.consumer.head.compare_exchange_weak(
                head,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break head,
                Err(_) => std::hint::spin_loop(),
            }
        };

        let slot = (cons_head & self.mask) as usize;
        // Prefetch is unnecessary for a single dequeue; batch loops issue
        // ahead-of-touch prefetches instead.
        // SAFETY: `cons_head & mask` selects a slot inside the table. The
        // Acquire load of `producer.tail` above observed `prod_tail >
        // cons_head`, which pairs with the Release store in `enqueue` that
        // followed the slot write. `T: Copy` means reading does not leave
        // ownership to drop.
        let value = unsafe { (*self.slots[slot].value.get()).assume_init_read() };

        while self.cursors.consumer.tail.load(Ordering::Acquire) != cons_head {
            std::hint::spin_loop();
        }
        self.cursors
            .consumer
            .tail
            .store(cons_head.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    /// Dequeue up to `out.len()` items into `out`, stopping when the ring is
    /// empty. Issues an L1 prefetch for the next slot before each read so the
    /// cacheline is warm by the time the load executes. Returns the number of
    /// items moved into `out`.
    #[inline]
    pub fn dequeue_batch(&self, out: &mut [T]) -> usize {
        let mut count = 0;
        while count < out.len() {
            // Prefetch the slot we are about to read. `available_to_read` is
            // a hint; if it under-reports (consumer head advanced after the
            // load) the prefetch targets a slot that may be re-used, which is
            // benign for a read hint.
            if self.available_to_read() == 0 {
                break;
            }
            let next_head = self.cursors.consumer.head.load(Ordering::Acquire);
            let next_slot = (next_head & self.mask) as usize;
            // Prefetch is a read hint and never dereferences for side effects;
            // targeting a slot that may be re-used (consumer head advanced
            // after the load) is benign for a read hint.
            prefetch_read_l1(self.slots[next_slot].value.get() as *const _);
            let Some(value) = self.dequeue() else {
                break;
            };
            out[count] = value;
            count += 1;
        }
        count
    }

    /// Enqueue up to `values.len()` items from `values`, stopping at the first
    /// `RingError::Full`. Returns the number of items successfully enqueued.
    #[inline]
    pub fn enqueue_batch(&self, values: &[T]) -> usize {
        let mut count = 0;
        while count < values.len() {
            if let Err(RingError::Full(_)) = self.enqueue(values[count]) {
                break;
            }
            count += 1;
        }
        count
    }
}

impl<T: Copy> fmt::Debug for LockFreeRing<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockFreeRing")
            .field("ring_size", &self.ring_size())
            .field("capacity", &self.capacity())
            .field("available_to_read", &self.available_to_read())
            .field("available_to_write", &self.available_to_write())
            .finish()
    }
}

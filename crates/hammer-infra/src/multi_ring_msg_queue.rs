//! Session-neutral VPP-shaped multi-ring message queue.
//!
//! Descriptor queue + per-ring data slots with `elsize`. One shared layout
//! serves both producer modes, selected by the [`ProducerMode`] type
//! parameter:
//!
//! - [`MultiProducer`] (default): producers hold [`ProducerGuard`] across
//!   alloc → write → add; the ring keeps a locked free list. This is the VPP
//!   `svm_msg_q` shared-producer shape with Hammer's spinlock divergence.
//! - [`SingleProducer`]: one capability-claimed producer
//!   ([`Producer::reserve`] → write → [`ProducerReservation::publish`]); the
//!   ring uses VPP cursor `head`/`tail` instead of a free list, so there is
//!   no ABA. Consumers take messages without the producer lock; [`RingMsg`]
//!   returns the ring slot on Drop.
//!
//! No Session Event or session business types live here.

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::PageSize;
use crate::segment::Segment;

/// Configuration for one message data ring.
#[derive(Debug, Clone, Copy)]
pub struct RingCfg {
    pub nitems: u32,
    pub elsize: usize,
}

/// Configuration for a multi-ring queue.
#[derive(Debug, Clone, Copy)]
pub struct MultiRingMsgQueueCfg<'a> {
    pub q_nitems: u32,
    pub rings: &'a [RingCfg],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiRingMsgQueueError {
    InvalidConfig,
    QueueFull,
    RingFull,
    BadRing,
    /// The single producer was already claimed by another mapping.
    ProducerClaimed,
    /// The on-segment queue mode does not match the mapping's mode parameter.
    ModeMismatch {
        expected: u32,
        actual: u32,
    },
}

/// Producer-mode marker. Sealed: only [`MultiProducer`] and
/// [`SingleProducer`] exist, so the on-segment `TAG` has exactly two values.
pub trait ProducerMode: private::Sealed {
    /// On-segment mode tag written at init and validated on every mapping.
    const TAG: u32;
}

mod private {
    pub trait Sealed {}
}

/// Shared-producer mode: the existing locked free-list path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultiProducer;

/// Single-producer mode: the capability-claimed VPP cursor `head`/`tail`
/// path with no free list and no ABA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SingleProducer;

impl private::Sealed for MultiProducer {}
impl private::Sealed for SingleProducer {}

impl ProducerMode for MultiProducer {
    const TAG: u32 = 0;
}

impl ProducerMode for SingleProducer {
    const TAG: u32 = 1;
}

#[repr(C)]
struct QueueHeader {
    lock: AtomicU32,
    q_head: AtomicU32,
    q_tail: AtomicU32,
    q_nitems: u32,
    q_mask: u32,
    n_rings: u32,
    /// [`ProducerMode::TAG`] of the initialising process; read on mapping.
    mode: u32,
    /// Single-producer claim (0 free, 1 claimed); unused in MP mode.
    claim: AtomicU32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct MsgDesc {
    ring: u32,
    slot: u32,
}

#[repr(C)]
struct RingHeader {
    /// Free-list stack top (count of free slots); unused in SP mode.
    free_top: AtomicU32,
    /// SP consumer free cursor (VPP `head`); unused in MP mode.
    head: AtomicU32,
    /// SP producer reserve cursor (VPP `tail`); unused in MP mode.
    tail: AtomicU32,
    nitems: u32,
    elsize: u32,
    _pad: u32,
}

/// VPP-shaped multi-ring message queue over a [`Segment`] backend.
pub struct MultiRingMsgQueue<P = MultiProducer> {
    _seg: Segment,
    hdr: *mut QueueHeader,
    descs: *mut MsgDesc,
    rings: Vec<RingView>,
    _mode: PhantomData<P>,
}

struct RingView {
    hdr: *mut RingHeader,
    /// Free slot indices `[0, nitems)` (MP mode only).
    free_slots: *mut u32,
    data: *mut u8,
}

// Moving a queue object between threads is sound: all shared state lives in
// the segment and is accessed through atomics. Shared references are sound
// for MultiProducer (the locked producer protocol); the SingleProducer
// consumer requires `&mut self`, so the SP queue is not Sync.
unsafe impl<P: ProducerMode> Send for MultiRingMsgQueue<P> {}
unsafe impl Sync for MultiRingMsgQueue<MultiProducer> {}

impl<P: ProducerMode> MultiRingMsgQueue<P> {
    pub fn with_cfg(cfg: MultiRingMsgQueueCfg<'_>) -> Result<Self, MultiRingMsgQueueError> {
        validate_cfg(&cfg)?;
        let bytes = Self::layout_bytes(&cfg);
        let page_size = PageSize::Default
            .bytes()
            .map_err(|_| MultiRingMsgQueueError::InvalidConfig)?;
        let segment_bytes = bytes
            .checked_add(page_size)
            .ok_or(MultiRingMsgQueueError::InvalidConfig)?;
        let seg = Segment::local(segment_bytes);
        let hdr_off = seg
            .alloc(bytes, 8)
            .expect("fresh queue segment has queue layout capacity");
        unsafe { Self::init_at(seg, hdr_off, &cfg) }
    }

    /// Byte size of the on-segment layout for `cfg` (excluding caller padding).
    pub fn layout_bytes(cfg: &MultiRingMsgQueueCfg<'_>) -> usize {
        layout_bytes(cfg)
    }

    /// Initialise a multi-ring queue at a pre-allocated offset in `seg`.
    ///
    /// # Safety
    /// Caller must guarantee `seg` has at least [`Self::layout_bytes`] bytes
    /// available at `hdr_offset`, and that no other queue instance mutates the
    /// same region without the queue's producer/consumer protocol.
    pub unsafe fn init_at(
        seg: Segment,
        hdr_offset: u64,
        cfg: &MultiRingMsgQueueCfg<'_>,
    ) -> Result<Self, MultiRingMsgQueueError> {
        validate_cfg(cfg)?;
        // SP rings use cursor masking (`tail & (nitems - 1)`), so they must
        // be power-of-two; MP free-list rings have no such requirement.
        if P::TAG == SingleProducer::TAG
            && cfg.rings.iter().any(|ring| !ring.nitems.is_power_of_two())
        {
            return Err(MultiRingMsgQueueError::InvalidConfig);
        }
        let layout = layout_bytes(cfg);
        let hdr_offset = usize::try_from(hdr_offset).expect("queue offset exceeds usize");
        let end = hdr_offset
            .checked_add(layout)
            .expect("queue layout end overflows usize");
        assert!(end <= seg.size(), "queue layout exceeds segment bounds");
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_offset) as *mut QueueHeader };
        unsafe {
            std::ptr::write(
                hdr,
                QueueHeader {
                    lock: AtomicU32::new(0),
                    q_head: AtomicU32::new(0),
                    q_tail: AtomicU32::new(0),
                    q_nitems: cfg.q_nitems,
                    q_mask: cfg.q_nitems - 1,
                    n_rings: cfg.rings.len() as u32,
                    mode: P::TAG,
                    claim: AtomicU32::new(0),
                },
            );
        }

        let mut offset = hdr_offset + std::mem::size_of::<QueueHeader>();
        let descs = unsafe { base.add(offset) as *mut MsgDesc };
        offset += cfg.q_nitems as usize * std::mem::size_of::<MsgDesc>();

        let mut rings = Vec::with_capacity(cfg.rings.len());
        for ring_cfg in cfg.rings {
            let ring_hdr = unsafe { base.add(offset) as *mut RingHeader };
            unsafe {
                std::ptr::write(
                    ring_hdr,
                    RingHeader {
                        free_top: AtomicU32::new(ring_cfg.nitems),
                        head: AtomicU32::new(0),
                        tail: AtomicU32::new(0),
                        nitems: ring_cfg.nitems,
                        elsize: ring_cfg.elsize as u32,
                        _pad: 0,
                    },
                );
            }
            offset += std::mem::size_of::<RingHeader>();
            let free_slots = unsafe { base.add(offset) as *mut u32 };
            for i in 0..ring_cfg.nitems {
                unsafe {
                    *free_slots.add(i as usize) = i;
                }
            }
            offset += ring_cfg.nitems as usize * std::mem::size_of::<u32>();
            let data = unsafe { base.add(offset) };
            offset += ring_cfg.nitems as usize * ring_cfg.elsize;
            rings.push(RingView {
                hdr: ring_hdr,
                free_slots,
                data,
            });
        }

        let _ = offset;
        Ok(Self {
            _seg: seg,
            hdr,
            descs,
            rings,
            _mode: PhantomData,
        })
    }

    /// Remap an already-initialised multi-ring queue from shared segment
    /// memory, validating the on-segment producer mode against `P`.
    ///
    /// # Safety
    /// `hdr_offset` must point at a queue previously initialised with
    /// [`Self::init_at`] (or equivalent layout). Ring headers must still be
    /// valid; the caller owns signaling separately.
    pub unsafe fn from_shared(
        seg: Segment,
        hdr_offset: u64,
    ) -> Result<Self, MultiRingMsgQueueError> {
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_offset as usize) as *mut QueueHeader };
        let qhdr = unsafe { &*hdr };
        // The mode space is exactly the sealed [`ProducerMode`] tags {0, 1};
        // any other value is a corrupt header, typed as InvalidConfig rather
        // than a misleading ModeMismatch.
        if qhdr.mode != MultiProducer::TAG && qhdr.mode != SingleProducer::TAG {
            return Err(MultiRingMsgQueueError::InvalidConfig);
        }
        if qhdr.mode != P::TAG {
            return Err(MultiRingMsgQueueError::ModeMismatch {
                expected: P::TAG,
                actual: qhdr.mode,
            });
        }
        let mut offset = hdr_offset as usize + std::mem::size_of::<QueueHeader>();
        let descs = unsafe { base.add(offset) as *mut MsgDesc };
        offset += qhdr.q_nitems as usize * std::mem::size_of::<MsgDesc>();

        let mut rings = Vec::with_capacity(qhdr.n_rings as usize);
        for _ in 0..qhdr.n_rings {
            let ring_hdr = unsafe { base.add(offset) as *mut RingHeader };
            let rhdr = unsafe { &*ring_hdr };
            offset += std::mem::size_of::<RingHeader>();
            let free_slots = unsafe { base.add(offset) as *mut u32 };
            offset += rhdr.nitems as usize * std::mem::size_of::<u32>();
            let data = unsafe { base.add(offset) };
            offset += rhdr.nitems as usize * rhdr.elsize as usize;
            rings.push(RingView {
                hdr: ring_hdr,
                free_slots,
                data,
            });
        }
        let _ = offset;
        Ok(Self {
            _seg: seg,
            hdr,
            descs,
            rings,
            _mode: PhantomData,
        })
    }

    /// Take the next descriptor without the producer lock.
    fn sub_descriptor(&self) -> Option<(u32, u32)> {
        let hdr = unsafe { &*self.hdr };
        let head = hdr.q_head.load(Ordering::Relaxed);
        let tail = hdr.q_tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let idx = head & hdr.q_mask;
        let desc = unsafe { *self.descs.add(idx as usize) };
        hdr.q_head.store(head.wrapping_add(1), Ordering::Release);
        Some((desc.ring, desc.slot))
    }

    /// Ring of the next queued descriptor, without consuming it.
    ///
    /// Inspects the head descriptor before any head advance, so a
    /// ring-specific consumer can reject a message while leaving it queued
    /// for its own consumer path. The SingleProducer consumer holds `&mut
    /// self` across a peek and the following [`Self::sub`], so its head
    /// cannot advance in between; a MultiProducer mapping must not pair
    /// peek with a later sub, since a concurrent consumer may advance the
    /// head in between.
    #[inline]
    pub fn peek_ring(&self) -> Option<u32> {
        let hdr = unsafe { &*self.hdr };
        let head = hdr.q_head.load(Ordering::Relaxed);
        let tail = hdr.q_tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let idx = head & hdr.q_mask;
        let desc = unsafe { *self.descs.add(idx as usize) };
        Some(desc.ring)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        let hdr = unsafe { &*self.hdr };
        hdr.q_head.load(Ordering::Acquire) == hdr.q_tail.load(Ordering::Acquire)
    }

    /// Number of queued messages, matching VPP `svm_msg_q_size`.
    #[inline]
    pub fn len(&self) -> usize {
        let hdr = unsafe { &*self.hdr };
        hdr.q_tail
            .load(Ordering::Acquire)
            .wrapping_sub(hdr.q_head.load(Ordering::Acquire)) as usize
    }

    #[inline]
    pub fn ring_element_size(&self, ring: u32) -> Option<usize> {
        self.rings
            .get(ring as usize)
            .map(|ring| unsafe { (*ring.hdr).elsize as usize })
    }
}

impl MultiRingMsgQueue<MultiProducer> {
    #[inline]
    pub fn lock(&self) -> ProducerGuard<'_> {
        self.lock_producer();
        ProducerGuard { queue: self }
    }

    fn lock_producer(&self) {
        let hdr = unsafe { &*self.hdr };
        while hdr
            .lock
            .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
    }

    fn unlock_producer(&self) {
        unsafe {
            (*self.hdr).lock.store(0, Ordering::Release);
        }
    }

    /// Consumer: take next descriptor without the producer lock.
    pub fn sub(&self) -> Option<RingMsg<'_>> {
        let (ring, slot) = self.sub_descriptor()?;
        let ring_view = &self.rings[ring as usize];
        let rhdr = unsafe { &*ring_view.hdr };
        Some(RingMsg {
            ring,
            slot,
            data: unsafe { ring_view.data.add(slot as usize * rhdr.elsize as usize) },
            elsize: rhdr.elsize as usize,
            free: SlotFree::Freelist {
                queue: self,
                ring,
                slot,
            },
            _lifetime: PhantomData,
        })
    }
}

impl MultiRingMsgQueue<SingleProducer> {
    /// Claim the single-producer capability for this queue.
    ///
    /// The shared header claim is taken once with a compare-exchange; a
    /// second claim (same or another mapping of the segment) is a typed
    /// error, never a panic. The returned [`Producer`] is self-contained —
    /// it owns a [`Segment`] clone plus raw pointers into the queue — so the
    /// mapping object it was claimed from may be dropped. Consumer mappings
    /// never claim.
    pub fn claim_producer(&self) -> Result<Producer, MultiRingMsgQueueError> {
        let hdr = unsafe { &*self.hdr };
        if hdr
            .claim
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(MultiRingMsgQueueError::ProducerClaimed);
        }
        Ok(Producer {
            _seg: self._seg.clone(),
            hdr: self.hdr,
            descs: self.descs,
            rings: self
                .rings
                .iter()
                .map(|ring| ProducerRing {
                    hdr: ring.hdr,
                    data: ring.data,
                })
                .collect(),
            _mode: PhantomData,
            _not_sync: PhantomData,
        })
    }

    /// Consumer: take next descriptor without the producer lock.
    ///
    /// `&mut self` enforces a single outstanding borrowed message, which is
    /// what guarantees in-order slot free (VPP `svm_msg_q_free_msg`).
    pub fn sub(&mut self) -> Option<RingMsg<'_>> {
        let (ring, slot) = self.sub_descriptor()?;
        let ring_view = &self.rings[ring as usize];
        let rhdr = unsafe { &*ring_view.hdr };
        Some(RingMsg {
            ring,
            slot,
            data: unsafe { ring_view.data.add(slot as usize * rhdr.elsize as usize) },
            elsize: rhdr.elsize as usize,
            free: SlotFree::Cursor {
                head: &rhdr.head,
                nitems: rhdr.nitems,
            },
            _lifetime: PhantomData,
        })
    }
}

fn validate_cfg(cfg: &MultiRingMsgQueueCfg<'_>) -> Result<(), MultiRingMsgQueueError> {
    if cfg.q_nitems < 2 || !cfg.q_nitems.is_power_of_two() {
        return Err(MultiRingMsgQueueError::InvalidConfig);
    }
    if cfg.rings.is_empty() {
        return Err(MultiRingMsgQueueError::InvalidConfig);
    }
    for ring in cfg.rings {
        if ring.nitems < 1 || ring.elsize == 0 {
            return Err(MultiRingMsgQueueError::InvalidConfig);
        }
    }
    Ok(())
}

fn layout_bytes(cfg: &MultiRingMsgQueueCfg<'_>) -> usize {
    let mut n = std::mem::size_of::<QueueHeader>();
    n += cfg.q_nitems as usize * std::mem::size_of::<MsgDesc>();
    for ring in cfg.rings {
        n += std::mem::size_of::<RingHeader>();
        n += ring.nitems as usize * std::mem::size_of::<u32>();
        n += ring.nitems as usize * ring.elsize;
    }
    n
}

/// RAII producer lock — unlocks on Drop. MultiProducer mode only.
pub struct ProducerGuard<'a> {
    queue: &'a MultiRingMsgQueue<MultiProducer>,
}

impl<'a> ProducerGuard<'a> {
    /// Allocate a free slot on `ring` (must be held under this guard).
    pub fn alloc(&mut self, ring: u32) -> Result<MsgSlot, MultiRingMsgQueueError> {
        let qhdr = unsafe { &*self.queue.hdr };
        let head = qhdr.q_head.load(Ordering::Acquire);
        let tail = qhdr.q_tail.load(Ordering::Relaxed);
        if tail.wrapping_sub(head) == qhdr.q_nitems {
            return Err(MultiRingMsgQueueError::QueueFull);
        }
        let ring_view = self
            .queue
            .rings
            .get(ring as usize)
            .ok_or(MultiRingMsgQueueError::BadRing)?;
        let rhdr = unsafe { &*ring_view.hdr };
        let top = rhdr.free_top.load(Ordering::Relaxed);
        if top == 0 {
            return Err(MultiRingMsgQueueError::RingFull);
        }
        let new_top = top - 1;
        let slot = unsafe { *ring_view.free_slots.add(new_top as usize) };
        rhdr.free_top.store(new_top, Ordering::Relaxed);
        Ok(MsgSlot {
            queue: self.queue as *const MultiRingMsgQueue<MultiProducer>,
            ring,
            slot,
            elsize: rhdr.elsize as usize,
            data: unsafe { ring_view.data.add(slot as usize * rhdr.elsize as usize) },
            committed: false,
        })
    }

    /// Publish an allocated slot onto the descriptor queue.
    pub fn add(&mut self, mut slot: MsgSlot) {
        let qhdr = unsafe { &*self.queue.hdr };
        let tail = qhdr.q_tail.load(Ordering::Relaxed);
        let idx = tail & qhdr.q_mask;
        unsafe {
            *self.queue.descs.add(idx as usize) = MsgDesc {
                ring: slot.ring,
                slot: slot.slot,
            };
        }
        qhdr.q_tail.store(tail.wrapping_add(1), Ordering::Release);
        slot.committed = true;
    }
}

impl Drop for ProducerGuard<'_> {
    fn drop(&mut self) {
        self.queue.unlock_producer();
    }
}

/// Writable ring slot while the producer guard is held.
pub struct MsgSlot {
    queue: *const MultiRingMsgQueue<MultiProducer>,
    ring: u32,
    slot: u32,
    elsize: usize,
    data: *mut u8,
    committed: bool,
}

unsafe impl Send for MsgSlot {}

impl MsgSlot {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.elsize) }
    }
}

impl Drop for MsgSlot {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // Still under producer lock (caller holds ProducerGuard).
        unsafe {
            free_ring_slot_unlocked(&*self.queue, self.ring, self.slot);
        }
    }
}

/// Single-producer capability claimed once from an SP-mode queue.
///
/// Self-contained: owns a [`Segment`] clone plus raw pointers into the shared
/// queue, so the mapping object it was claimed from may be dropped. Not
/// `Sync` by construction (`PhantomData<Cell<()>>`); one exclusive writer.
pub struct Producer<P = SingleProducer> {
    _seg: Segment,
    hdr: *mut QueueHeader,
    descs: *mut MsgDesc,
    rings: Vec<ProducerRing>,
    _mode: PhantomData<P>,
    _not_sync: PhantomData<Cell<()>>,
}

unsafe impl<P: ProducerMode> Send for Producer<P> {}

struct ProducerRing {
    hdr: *mut RingHeader,
    data: *mut u8,
}

impl Producer {
    /// Reserve one ring slot, advancing the SP cursor tail.
    ///
    /// The full check reads the consumer-owned cursor `head` with Acquire
    /// (VPP `svm_msg_q_ring_t.head`); the slot is free when
    /// `tail - head < nitems`. Dropping the returned reservation without
    /// publishing rolls the cursor back.
    pub fn reserve(
        &mut self,
        ring: u32,
    ) -> Result<ProducerReservation<'_>, MultiRingMsgQueueError> {
        let qhdr = unsafe { &*self.hdr };
        let q_head = qhdr.q_head.load(Ordering::Acquire);
        let q_tail = qhdr.q_tail.load(Ordering::Relaxed);
        if q_tail.wrapping_sub(q_head) == qhdr.q_nitems {
            return Err(MultiRingMsgQueueError::QueueFull);
        }
        let (slot, elsize, data) = {
            let ring_view = self
                .rings
                .get(ring as usize)
                .ok_or(MultiRingMsgQueueError::BadRing)?;
            let rhdr = unsafe { &*ring_view.hdr };
            let tail = rhdr.tail.load(Ordering::Relaxed);
            let head = rhdr.head.load(Ordering::Acquire);
            if tail.wrapping_sub(head) >= rhdr.nitems {
                return Err(MultiRingMsgQueueError::RingFull);
            }
            rhdr.tail.store(tail.wrapping_add(1), Ordering::Relaxed);
            let slot = tail & (rhdr.nitems - 1);
            (slot, rhdr.elsize as usize, unsafe {
                ring_view.data.add(slot as usize * rhdr.elsize as usize)
            })
        };
        Ok(ProducerReservation {
            producer: self,
            ring,
            slot,
            elsize,
            data,
            published: false,
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        let hdr = unsafe { &*self.hdr };
        hdr.q_head.load(Ordering::Acquire) == hdr.q_tail.load(Ordering::Relaxed)
    }

    /// Element size of `ring` (None for an out-of-range ring).
    #[inline]
    pub fn ring_element_size(&self, ring: u32) -> Option<usize> {
        self.rings
            .get(ring as usize)
            .map(|ring| unsafe { (*ring.hdr).elsize as usize })
    }
}

/// One reserved SP ring slot; publish or Drop-cancel.
pub struct ProducerReservation<'a> {
    producer: &'a mut Producer,
    ring: u32,
    slot: u32,
    elsize: usize,
    data: *mut u8,
    published: bool,
}

impl ProducerReservation<'_> {
    pub fn payload_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.elsize) }
    }

    /// Publish the slot onto the descriptor queue (VPP `svm_msg_q_add_raw`):
    /// payload (already written via [`Self::payload_mut`]) → descriptor →
    /// `q_tail` Release. Returns whether the queue was empty before this
    /// publish — the empty → nonempty transition decided before the publish,
    /// so exactly the first element of a burst reports `true`.
    pub fn publish(&mut self) -> bool {
        let qhdr = unsafe { &*self.producer.hdr };
        let was_empty = qhdr.q_head.load(Ordering::Acquire) == qhdr.q_tail.load(Ordering::Relaxed);
        let tail = qhdr.q_tail.load(Ordering::Relaxed);
        let idx = tail & qhdr.q_mask;
        unsafe {
            *self.producer.descs.add(idx as usize) = MsgDesc {
                ring: self.ring,
                slot: self.slot,
            };
        }
        qhdr.q_tail.store(tail.wrapping_add(1), Ordering::Release);
        self.published = true;
        was_empty
    }
}

impl Drop for ProducerReservation<'_> {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        // Cancel the reservation: the producer is the sole writer of the ring
        // tail, so a plain rollback restores the slot.
        let rhdr = unsafe { &*self.producer.rings[self.ring as usize].hdr };
        let tail = rhdr.tail.load(Ordering::Relaxed);
        rhdr.tail.store(tail.wrapping_sub(1), Ordering::Relaxed);
    }
}

/// How a [`RingMsg`] returns its ring data slot on Drop.
enum SlotFree<'a> {
    /// MultiProducer: push the slot onto the locked free list.
    Freelist {
        queue: &'a MultiRingMsgQueue<MultiProducer>,
        ring: u32,
        slot: u32,
    },
    /// SingleProducer: advance the ring `head` cursor in order.
    Cursor { head: &'a AtomicU32, nitems: u32 },
}

/// Consumer-owned message; Drop returns the ring data slot.
pub struct RingMsg<'a> {
    ring: u32,
    slot: u32,
    data: *const u8,
    elsize: usize,
    free: SlotFree<'a>,
    _lifetime: PhantomData<&'a ()>,
}

impl RingMsg<'_> {
    pub fn ring_index(&self) -> u32 {
        self.ring
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.elsize) }
    }
}

impl Drop for RingMsg<'_> {
    fn drop(&mut self) {
        match &self.free {
            SlotFree::Freelist { queue, ring, slot } => {
                // Serialize freelist updates with producers.
                queue.lock_producer();
                free_ring_slot_unlocked(queue, *ring, *slot);
                queue.unlock_producer();
            }
            SlotFree::Cursor { head, nitems } => {
                // In-order free (VPP `svm_msg_q_free_msg`): the consumer
                // releases exactly the ring `head` slot and advances the
                // cursor the producer reads for its full check. One
                // outstanding borrowed message is enforced by the consumer's
                // `&mut self` capability.
                let head_value = head.load(Ordering::Relaxed);
                debug_assert!(self.slot == head_value & (nitems - 1));
                head.store(head_value.wrapping_add(1), Ordering::Release);
            }
        }
    }
}

fn free_ring_slot_unlocked(queue: &MultiRingMsgQueue<MultiProducer>, ring: u32, slot: u32) {
    let ring_view = &queue.rings[ring as usize];
    let rhdr = unsafe { &*ring_view.hdr };
    let top = rhdr.free_top.load(Ordering::Relaxed);
    debug_assert!(top < rhdr.nitems);
    unsafe {
        *ring_view.free_slots.add(top as usize) = slot;
    }
    rhdr.free_top.store(top + 1, Ordering::Release);
}

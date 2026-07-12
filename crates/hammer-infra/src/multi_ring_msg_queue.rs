//! Session-neutral VPP-shaped multi-ring message queue.
//!
//! Descriptor queue + per-ring data slots with `elsize`. Producers hold
//! [`ProducerGuard`] across alloc → write → add. Consumers take messages
//! without the producer lock; [`RingMsg`] returns the ring slot on Drop.
//!
//! No Session Event or session business types live here.

use std::sync::atomic::{AtomicU32, Ordering};

use crate::segment::{Local, Segment};

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
}

#[repr(C)]
struct QueueHeader {
    lock: AtomicU32,
    q_head: AtomicU32,
    q_tail: AtomicU32,
    q_nitems: u32,
    q_mask: u32,
    n_rings: u32,
    _pad: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct MsgDesc {
    ring: u32,
    slot: u32,
}

#[repr(C)]
struct RingHeader {
    /// Free-list stack top (count of free slots).
    free_top: AtomicU32,
    nitems: u32,
    elsize: u32,
    _pad: u32,
}

/// VPP-shaped multi-ring message queue over a [`Segment`] backend.
pub struct MultiRingMsgQueue<S: Segment = Local> {
    _seg: S,
    hdr: *mut QueueHeader,
    descs: *mut MsgDesc,
    rings: Vec<RingView>,
}

struct RingView {
    hdr: *mut RingHeader,
    /// Free slot indices `[0, nitems)`.
    free_slots: *mut u32,
    data: *mut u8,
}

unsafe impl<S: Segment> Send for MultiRingMsgQueue<S> {}
unsafe impl<S: Segment> Sync for MultiRingMsgQueue<S> {}

impl MultiRingMsgQueue<Local> {
    pub fn with_cfg(cfg: MultiRingMsgQueueCfg<'_>) -> Result<Self, MultiRingMsgQueueError> {
        validate_cfg(&cfg)?;
        let bytes = Self::layout_bytes(&cfg);
        let seg = Local::new(bytes + 64);
        let hdr_off = seg.alloc(bytes, 8);
        unsafe { Self::init_at(seg, hdr_off, &cfg) }
    }
}

impl<S: Segment> MultiRingMsgQueue<S> {
    /// Byte size of the on-segment layout for `cfg` (excluding caller padding).
    pub fn layout_bytes(cfg: &MultiRingMsgQueueCfg<'_>) -> usize {
        layout_bytes(cfg)
    }

    /// Initialise a multi-ring queue at a pre-allocated offset in `seg`.
    ///
    /// # Safety
    /// Caller must guarantee `seg` has at least [`Self::layout_bytes`] bytes
    /// available at `hdr_offset`, and that no other live view mutates the same
    /// region without the queue's producer/consumer protocol.
    pub unsafe fn init_at(
        seg: S,
        hdr_offset: u64,
        cfg: &MultiRingMsgQueueCfg<'_>,
    ) -> Result<Self, MultiRingMsgQueueError> {
        validate_cfg(cfg)?;
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_offset as usize) as *mut QueueHeader };
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
                    _pad: 0,
                },
            );
        }

        let mut offset = hdr_offset as usize + std::mem::size_of::<QueueHeader>();
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
        })
    }

    /// Remap an already-initialised multi-ring queue from shared segment memory.
    ///
    /// # Safety
    /// `hdr_offset` must point at a queue previously initialised with
    /// [`Self::init_at`] (or equivalent layout). Ring headers must still be
    /// valid; the caller owns signaling separately.
    pub unsafe fn from_shared(seg: S, hdr_offset: u64) -> Self {
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_offset as usize) as *mut QueueHeader };
        let qhdr = unsafe { &*hdr };
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
        Self {
            _seg: seg,
            hdr,
            descs,
            rings,
        }
    }

    #[inline]
    pub fn lock(&self) -> ProducerGuard<'_, S> {
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
    pub fn sub(&self) -> Option<RingMsg<'_, S>> {
        let hdr = unsafe { &*self.hdr };
        let head = hdr.q_head.load(Ordering::Relaxed);
        let tail = hdr.q_tail.load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let idx = head & hdr.q_mask;
        let desc = unsafe { *self.descs.add(idx as usize) };
        hdr.q_head.store(head.wrapping_add(1), Ordering::Release);
        Some(RingMsg {
            queue: self,
            ring: desc.ring,
            slot: desc.slot,
        })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        let hdr = unsafe { &*self.hdr };
        hdr.q_head.load(Ordering::Acquire) == hdr.q_tail.load(Ordering::Acquire)
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

/// RAII producer lock — unlocks on Drop.
pub struct ProducerGuard<'a, S: Segment = Local> {
    queue: &'a MultiRingMsgQueue<S>,
}

impl<'a, S: Segment> ProducerGuard<'a, S> {
    /// Allocate a free slot on `ring` (must be held under this guard).
    pub fn alloc(&mut self, ring: u32) -> Result<MsgSlot<S>, MultiRingMsgQueueError> {
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
            queue: self.queue as *const MultiRingMsgQueue<S>,
            ring,
            slot,
            elsize: rhdr.elsize as usize,
            data: unsafe { ring_view.data.add(slot as usize * rhdr.elsize as usize) },
            committed: false,
        })
    }

    /// Publish an allocated slot onto the descriptor queue.
    pub fn add(&mut self, mut slot: MsgSlot<S>) {
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

impl<S: Segment> Drop for ProducerGuard<'_, S> {
    fn drop(&mut self) {
        self.queue.unlock_producer();
    }
}

/// Writable ring slot while the producer guard is held.
pub struct MsgSlot<S: Segment = Local> {
    queue: *const MultiRingMsgQueue<S>,
    ring: u32,
    slot: u32,
    elsize: usize,
    data: *mut u8,
    committed: bool,
}

unsafe impl<S: Segment> Send for MsgSlot<S> {}

impl<S: Segment> MsgSlot<S> {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.elsize) }
    }
}

impl<S: Segment> Drop for MsgSlot<S> {
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

/// Consumer-owned message; Drop returns the ring data slot.
pub struct RingMsg<'a, S: Segment = Local> {
    queue: &'a MultiRingMsgQueue<S>,
    ring: u32,
    slot: u32,
}

impl<S: Segment> RingMsg<'_, S> {
    pub fn ring_index(&self) -> u32 {
        self.ring
    }

    pub fn as_slice(&self) -> &[u8] {
        let ring = &self.queue.rings[self.ring as usize];
        let elsize = unsafe { (*ring.hdr).elsize as usize };
        unsafe { std::slice::from_raw_parts(ring.data.add(self.slot as usize * elsize), elsize) }
    }
}

impl<S: Segment> Drop for RingMsg<'_, S> {
    fn drop(&mut self) {
        // Serialize freelist updates with producers.
        self.queue.lock_producer();
        free_ring_slot_unlocked(self.queue, self.ring, self.slot);
        self.queue.unlock_producer();
    }
}

fn free_ring_slot_unlocked<S: Segment>(queue: &MultiRingMsgQueue<S>, ring: u32, slot: u32) {
    let ring_view = &queue.rings[ring as usize];
    let rhdr = unsafe { &*ring_view.hdr };
    let top = rhdr.free_top.load(Ordering::Relaxed);
    debug_assert!(top < rhdr.nitems);
    unsafe {
        *ring_view.free_slots.add(top as usize) = slot;
    }
    rhdr.free_top.store(top + 1, Ordering::Release);
}

//! Session Message Queue: runtime wrapper over the infra multi-ring queue.
//!
//! IO events use [`SessionMqRing::Io`]; Connect/Close use [`SessionMqRing::Ctrl`].
//! Callers choose the ring via [`SessionMsgQueue::enqueue_io`] /
//! [`SessionMsgQueue::enqueue_ctrl`] — not by matching `evt_type` inside enqueue.
//!
//! Session Event identity follows ADR-0010 (VPP `session_event_t` rules).

use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};

use hammer_infra::msg_queue::MsgQueue;
use hammer_infra::multi_ring_msg_queue::{
    MultiRingMsgQueue, MultiRingMsgQueueCfg, MultiRingMsgQueueError, RingCfg,
};
use hammer_infra::segment::{Local, Segment, Svm};

/// VPP session MQ ring roles (`session_mq_rings_e`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum SessionMqRing {
    Io = 0,
    Ctrl = 1,
}

/// App↔session message-queue event aligned with VPP `session_event_t`.
///
/// # Identity rules (VPP / ADR-0010)
///
/// - **IO events** (`RxEnq`, `TxDeq`): construct with [`SessionEvt::io`]. Only
///   the session index is significant; worker bits are zero.
/// - **Control events** (`Connect`, `Close`): construct with [`SessionEvt::ctrl`].
///   Identity is the VPP-shaped Session Handle packing
///   `(session_index as u64) | ((worker_index as u64) << 32)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SessionEvt {
    pub evt_type: SessionEvtType,
    /// VPP `session_event_t.postponed`; unused by Hammer producers today.
    pub postponed: u8,
    _pad: [u8; 2],
    identity: u64,
}

impl SessionEvt {
    #[inline]
    pub const fn io(session_index: u32, evt_type: SessionEvtType) -> Self {
        Self {
            evt_type,
            postponed: 0,
            _pad: [0; 2],
            identity: session_index as u64,
        }
    }

    #[inline]
    pub const fn ctrl(session_index: u32, worker_index: u32, evt_type: SessionEvtType) -> Self {
        Self {
            evt_type,
            postponed: 0,
            _pad: [0; 2],
            identity: (session_index as u64) | ((worker_index as u64) << 32),
        }
    }

    #[inline]
    pub const fn session_index(self) -> u32 {
        self.identity as u32
    }

    #[inline]
    pub const fn worker_index(self) -> u32 {
        (self.identity >> 32) as u32
    }

    #[inline]
    pub const fn session_handle_raw(self) -> u64 {
        self.identity
    }

    fn as_bytes(self) -> [u8; SESSION_EVT_BYTES] {
        // SAFETY: repr(C) SessionEvt is exactly SESSION_EVT_BYTES with no padding gaps
        // beyond the explicit `_pad` field.
        unsafe { std::mem::transmute::<Self, [u8; SESSION_EVT_BYTES]>(self) }
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let arr: [u8; SESSION_EVT_BYTES] = bytes.try_into().expect("SessionEvt size");
        unsafe { std::mem::transmute::<[u8; SESSION_EVT_BYTES], Self>(arr) }
    }
}

const SESSION_EVT_BYTES: usize = size_of::<SessionEvt>();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionEvtType {
    RxEnq,
    TxDeq,
    Connect,
    Close,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMsgQueueError {
    InvalidConfig,
    Full(SessionEvt),
}

/// Shared Session Message Queue operations for Local (multi-ring) and flat adapters.
pub trait SessionEventQueue: Send + Sync {
    fn enqueue_io(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError>;
    fn enqueue_ctrl(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError>;
    fn dequeue(&self) -> Option<SessionEvt>;
    fn dequeue_batch(&self, out: &mut [SessionEvt]) -> usize;
    fn fire(&self);
    fn drain(&self) -> bool;
    fn read_signal(&self) -> bool;
    fn clear(&self);
}

/// Segment type with a statically-known session event queue implementation.
pub trait SessionSegment: Segment {
    type EventQueue: SessionEventQueue;
}

/// Session Message Queue with IO + CTRL rings over the infra multi-ring primitive.
pub struct SessionMsgQueue {
    inner: MultiRingMsgQueue,
    signal: AtomicBool,
}

impl SessionMsgQueue {
    /// Default Local queue: 2048 descriptors, 1024 slots per ring, SessionEvt elsize.
    pub fn with_defaults() -> Result<Self, SessionMsgQueueError> {
        Self::with_cfg(2048, 1024)
    }

    pub fn with_cfg(q_nitems: u32, ring_nitems: u32) -> Result<Self, SessionMsgQueueError> {
        if !q_nitems.is_power_of_two() || q_nitems < 2 || ring_nitems < 1 {
            return Err(SessionMsgQueueError::InvalidConfig);
        }
        let rings = [
            RingCfg {
                nitems: ring_nitems,
                elsize: SESSION_EVT_BYTES,
            },
            RingCfg {
                nitems: ring_nitems,
                elsize: SESSION_EVT_BYTES,
            },
        ];
        let inner = MultiRingMsgQueue::with_cfg(MultiRingMsgQueueCfg {
            q_nitems,
            rings: &rings,
        })
        .map_err(|e| match e {
            MultiRingMsgQueueError::InvalidConfig => SessionMsgQueueError::InvalidConfig,
            other => panic!("unexpected multi-ring config error: {other:?}"),
        })?;
        Ok(Self {
            inner,
            signal: AtomicBool::new(false),
        })
    }

    #[inline]
    pub fn enqueue_io(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        SessionEventQueue::enqueue_io(self, evt)
    }

    #[inline]
    pub fn enqueue_ctrl(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        SessionEventQueue::enqueue_ctrl(self, evt)
    }

    #[inline]
    pub fn dequeue(&self) -> Option<SessionEvt> {
        SessionEventQueue::dequeue(self)
    }

    fn enqueue_on(&self, ring: SessionMqRing, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        let bytes = evt.as_bytes();
        let mut guard = self.inner.lock();
        let mut slot = match guard.alloc(ring as u32) {
            Ok(slot) => slot,
            Err(MultiRingMsgQueueError::QueueFull | MultiRingMsgQueueError::RingFull) => {
                return Err(SessionMsgQueueError::Full(evt));
            }
            Err(MultiRingMsgQueueError::InvalidConfig | MultiRingMsgQueueError::BadRing) => {
                return Err(SessionMsgQueueError::InvalidConfig);
            }
        };
        slot.as_mut_slice().copy_from_slice(&bytes);
        guard.add(slot);
        self.fire();
        Ok(())
    }
}

impl SessionEventQueue for SessionMsgQueue {
    fn enqueue_io(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        self.enqueue_on(SessionMqRing::Io, evt)
    }

    fn enqueue_ctrl(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        self.enqueue_on(SessionMqRing::Ctrl, evt)
    }

    fn dequeue(&self) -> Option<SessionEvt> {
        let msg = self.inner.sub()?;
        let evt = SessionEvt::from_bytes(msg.as_slice());
        drop(msg);
        Some(evt)
    }

    fn dequeue_batch(&self, out: &mut [SessionEvt]) -> usize {
        let mut count = 0;
        for slot in out.iter_mut() {
            if let Some(evt) = SessionEventQueue::dequeue(self) {
                *slot = evt;
                count += 1;
            } else {
                break;
            }
        }
        count
    }

    fn fire(&self) {
        self.signal.store(true, Ordering::Release);
    }

    fn drain(&self) -> bool {
        self.signal.swap(false, Ordering::AcqRel)
    }

    fn read_signal(&self) -> bool {
        self.drain()
    }

    fn clear(&self) {
        while SessionEventQueue::dequeue(self).is_some() {}
        self.signal.store(false, Ordering::Relaxed);
    }
}

/// Flat infra `MsgQueue` adapter (SVM / pre-hard-cut). Both rings map to the single flat ring.
pub struct FlatSessionMsgQueue<S: Segment> {
    inner: MsgQueue<S>,
}

impl<S: Segment> FlatSessionMsgQueue<S> {
    pub fn new(inner: MsgQueue<S>) -> Self {
        Self { inner }
    }

    pub fn into_inner(self) -> MsgQueue<S> {
        self.inner
    }

    pub fn inner(&self) -> &MsgQueue<S> {
        &self.inner
    }
}

impl<S: Segment> SessionEventQueue for FlatSessionMsgQueue<S> {
    fn enqueue_io(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        self.inner.enqueue(to_infra_evt(evt)).map_err(|e| match e {
            hammer_infra::msg_queue::MsgQueueError::Full(evt) => {
                SessionMsgQueueError::Full(from_infra_evt(evt))
            }
            hammer_infra::msg_queue::MsgQueueError::InvalidCapacity => {
                SessionMsgQueueError::InvalidConfig
            }
        })
    }

    fn enqueue_ctrl(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        self.enqueue_io(evt)
    }

    fn dequeue(&self) -> Option<SessionEvt> {
        self.inner.dequeue().map(from_infra_evt)
    }

    fn dequeue_batch(&self, out: &mut [SessionEvt]) -> usize {
        let mut infra = vec![
            hammer_infra::msg_queue::SessionEvt::io(
                0,
                hammer_infra::msg_queue::SessionEvtType::RxEnq
            );
            out.len()
        ];
        let count = self.inner.dequeue_batch(&mut infra);
        for (dst, src) in out.iter_mut().zip(infra[..count].iter().copied()) {
            *dst = from_infra_evt(src);
        }
        count
    }

    fn fire(&self) {
        self.inner.fire();
    }

    fn drain(&self) -> bool {
        self.inner.drain()
    }

    fn read_signal(&self) -> bool {
        self.inner.read_signal()
    }

    fn clear(&self) {
        self.inner.clear();
    }
}

impl SessionSegment for Local {
    type EventQueue = SessionMsgQueue;
}

impl SessionSegment for Svm {
    type EventQueue = FlatSessionMsgQueue<Svm>;
}

fn to_infra_evt(evt: SessionEvt) -> hammer_infra::msg_queue::SessionEvt {
    // SAFETY: identical repr(C) layout / ADR-0010 packing.
    unsafe { std::mem::transmute(evt) }
}

fn from_infra_evt(evt: hammer_infra::msg_queue::SessionEvt) -> SessionEvt {
    unsafe { std::mem::transmute(evt) }
}

const _: () = assert!(SESSION_EVT_BYTES == 16);
const _: () = assert!(size_of::<hammer_infra::msg_queue::SessionEvt>() == SESSION_EVT_BYTES);

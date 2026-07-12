//! Session Message Queue: runtime wrapper over the infra multi-ring queue.
//!
//! IO events use [`SessionMqRing::Io`]; Connect/Close use [`SessionMqRing::Ctrl`].
//! Callers choose the ring via [`SessionMsgQueue::enqueue_io`] /
//! [`SessionMsgQueue::enqueue_ctrl`] — not by matching `evt_type` inside enqueue.
//!
//! Session Event identity follows ADR-0010 (VPP `session_event_t` rules).

use std::mem::size_of;
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};

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

/// Shared Session Message Queue operations for Local and SVM backends.
pub trait SessionEventQueue: Send + Sync {
    fn enqueue_io(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError>;
    fn enqueue_ctrl(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError>;
    fn dequeue(&self) -> Option<SessionEvt>;
    fn dequeue_batch(&self, out: &mut [SessionEvt]) -> usize;
    fn fire(&self);
    fn drain(&self) -> bool;
    fn read_signal(&self) -> bool;
    fn clear(&self);
    fn is_empty(&self) -> bool;
    fn read_fd(&self) -> Option<RawFd>;
}

/// Segment type with a statically-known session event queue implementation.
pub trait SessionSegment: Segment {
    type EventQueue: SessionEventQueue;
}

fn session_ring_cfg(q_nitems: u32, ring_nitems: u32) -> Result<[RingCfg; 2], SessionMsgQueueError> {
    if !q_nitems.is_power_of_two() || q_nitems < 2 || ring_nitems < 1 {
        return Err(SessionMsgQueueError::InvalidConfig);
    }
    Ok([
        RingCfg {
            nitems: ring_nitems,
            elsize: SESSION_EVT_BYTES,
        },
        RingCfg {
            nitems: ring_nitems,
            elsize: SESSION_EVT_BYTES,
        },
    ])
}

/// Session Message Queue with IO + CTRL rings over the infra multi-ring primitive.
pub struct SessionMsgQueue<S: Segment = Local> {
    inner: MultiRingMsgQueue<S>,
    signal_atomic: AtomicBool,
    signal_read: Option<RawFd>,
    signal_write: Option<RawFd>,
}

impl SessionMsgQueue<Local> {
    /// Default Local queue: 2048 descriptors, 1024 slots per ring, SessionEvt elsize.
    pub fn with_defaults() -> Result<Self, SessionMsgQueueError> {
        Self::with_cfg(2048, 1024)
    }

    pub fn with_cfg(q_nitems: u32, ring_nitems: u32) -> Result<Self, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
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
            signal_atomic: AtomicBool::new(false),
            signal_read: None,
            signal_write: None,
        })
    }
}

impl<S: Segment> SessionMsgQueue<S> {
    /// On-segment byte size for a Session Message Queue with the given capacities.
    pub fn layout_bytes(q_nitems: u32, ring_nitems: u32) -> Result<usize, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
        Ok(MultiRingMsgQueue::<S>::layout_bytes(
            &MultiRingMsgQueueCfg {
                q_nitems,
                rings: &rings,
            },
        ))
    }

    /// Initialise a Session Message Queue at a pre-allocated segment offset.
    ///
    /// # Safety
    /// Caller must reserve at least [`Self::layout_bytes`] at `hdr_offset`.
    pub unsafe fn init_at(
        seg: S,
        hdr_offset: u64,
        q_nitems: u32,
        ring_nitems: u32,
    ) -> Result<Self, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
        let inner = unsafe {
            MultiRingMsgQueue::init_at(
                seg,
                hdr_offset,
                &MultiRingMsgQueueCfg {
                    q_nitems,
                    rings: &rings,
                },
            )
        }
        .map_err(|e| match e {
            MultiRingMsgQueueError::InvalidConfig => SessionMsgQueueError::InvalidConfig,
            other => panic!("unexpected multi-ring init error: {other:?}"),
        })?;
        Ok(Self {
            inner,
            signal_atomic: AtomicBool::new(false),
            signal_read: None,
            signal_write: None,
        })
    }

    /// Remap an already-initialised Session Message Queue and attach optional signal fds.
    ///
    /// # Safety
    /// `hdr_offset` must point at a queue previously initialised with [`Self::init_at`].
    pub unsafe fn from_shared(
        seg: S,
        hdr_offset: u64,
        signal_read: Option<RawFd>,
        signal_write: Option<RawFd>,
    ) -> Self {
        Self {
            inner: unsafe { MultiRingMsgQueue::from_shared(seg, hdr_offset) },
            signal_atomic: AtomicBool::new(false),
            signal_read,
            signal_write,
        }
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

impl<S: Segment> SessionEventQueue for SessionMsgQueue<S> {
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
        if let Some(fd) = self.signal_write {
            let val: [u8; 1] = [1];
            let ret = unsafe { libc::write(fd, val.as_ptr() as *const libc::c_void, 1) };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() != std::io::ErrorKind::WouldBlock {
                    panic!("session mq signal write failed: {err}");
                }
            }
        } else {
            self.signal_atomic.store(true, Ordering::Release);
        }
    }

    fn drain(&self) -> bool {
        if let Some(fd) = self.signal_read {
            let mut buf = [0u8; 64];
            let mut woke = false;
            loop {
                let ret =
                    unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
                if ret > 0 {
                    woke = true;
                    continue;
                }
                break;
            }
            woke
        } else {
            self.signal_atomic.swap(false, Ordering::AcqRel)
        }
    }

    fn read_signal(&self) -> bool {
        self.drain()
    }

    fn clear(&self) {
        while SessionEventQueue::dequeue(self).is_some() {}
        if let Some(fd) = self.signal_read {
            let mut buf = [0u8; 64];
            loop {
                let ret =
                    unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
                if ret <= 0 {
                    break;
                }
            }
        }
        self.signal_atomic.store(false, Ordering::Relaxed);
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn read_fd(&self) -> Option<RawFd> {
        self.signal_read
    }
}

impl SessionSegment for Local {
    type EventQueue = SessionMsgQueue<Local>;
}

impl SessionSegment for Svm {
    type EventQueue = SessionMsgQueue<Svm>;
}

const _: () = assert!(SESSION_EVT_BYTES == 16);

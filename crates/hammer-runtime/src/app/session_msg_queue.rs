//! Session Message Queue: runtime wrapper over the infra multi-ring queue.
//!
//! IO events use [`SessionMqRing::Io`]; Connect/Close use [`SessionMqRing::Ctrl`].
//! Callers choose the ring via [`SessionMsgQueue::enqueue_io`] /
//! [`SessionMsgQueue::enqueue_ctrl`] — not by matching `evt_type` inside enqueue.
//!
//! Session Event identity follows ADR-0010 (VPP `session_event_t` rules).

use std::mem::size_of;

use hammer_infra::multi_ring_msg_queue::{
    MultiRingMsgQueue, MultiRingMsgQueueCfg, MultiRingMsgQueueError, RingCfg,
};

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

/// Session Message Queue with IO + CTRL rings over the infra multi-ring primitive.
pub struct SessionMsgQueue {
    inner: MultiRingMsgQueue,
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
        Ok(Self { inner })
    }

    pub fn enqueue_io(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        self.enqueue_on(SessionMqRing::Io, evt)
    }

    pub fn enqueue_ctrl(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        self.enqueue_on(SessionMqRing::Ctrl, evt)
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
        Ok(())
    }

    /// Copy out a Session Event and reclaim the ring slot (Drop on inner message).
    pub fn dequeue(&self) -> Option<SessionEvt> {
        let msg = self.inner.sub()?;
        let evt = SessionEvt::from_bytes(msg.as_slice());
        drop(msg);
        Some(evt)
    }
}

const _: () = assert!(SESSION_EVT_BYTES == 16);

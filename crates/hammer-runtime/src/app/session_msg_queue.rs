//! Session Message Queue: runtime wrapper over the infra multi-ring queue.
//!
//! IO events use [`SessionMqRing::Io`]; Connect/Close use [`SessionMqRing::Ctrl`].
//! Callers choose the ring via [`SessionMsgQueue::enqueue_io`] /
//! [`SessionMsgQueue::enqueue_ctrl`] — not by matching `evt_type` inside enqueue.
//!
//! Session Event identity follows ADR-0010 (VPP `session_event_t` rules).

use std::mem::size_of;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hammer_infra::multi_ring_msg_queue::{
    MultiRingMsgQueue, MultiRingMsgQueueCfg, MultiRingMsgQueueError, RingCfg,
};
use hammer_infra::segment::Segment;

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
/// - **IO events** (`RxEnq`, `RxDeq`, `TxEnq`, `TxDeq`, `ProtocolOutput`): construct with
///   [`SessionEvt::io`]. Only the session index is significant; worker bits are zero.
/// - **Control events** (`Connect`, `Close`, `HalfClose`, `Reset`,
///   `Disconnected`, `TransportClosed`): construct with [`SessionEvt::ctrl`].
///   Identity is the VPP-shaped Session Handle packing
///   `(session_index as u64) | ((worker_index as u64) << 32)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SessionEvt {
    pub evt_type: SessionEvtType,
    /// VPP `session_event_t.postponed`; unused by Hammer producers today.
    pub postponed: u8,
    /// Session/app event flags (e.g. urgent RX). Occupies VPP-aligned pad space.
    flags: SessionEvtFlags,
    _pad: u8,
    identity: u64,
}

/// Flags carried on [`SessionEvt`] (no separate OOB/MSG_OOB channel).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct SessionEvtFlags(u8);

impl SessionEvtFlags {
    /// TCP URG / urgent pointer marked this RX delivery.
    pub const URGENT: Self = Self(0x01);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl SessionEvt {
    #[inline]
    pub const fn io(session_index: u32, evt_type: SessionEvtType) -> Self {
        Self::io_with_flags(session_index, evt_type, SessionEvtFlags::empty())
    }

    #[inline]
    pub const fn io_with_flags(
        session_index: u32,
        evt_type: SessionEvtType,
        flags: SessionEvtFlags,
    ) -> Self {
        Self {
            evt_type,
            postponed: 0,
            flags,
            _pad: 0,
            identity: session_index as u64,
        }
    }

    #[inline]
    pub const fn ctrl(session_index: u32, worker_index: u32, evt_type: SessionEvtType) -> Self {
        Self {
            evt_type,
            postponed: 0,
            flags: SessionEvtFlags::empty(),
            _pad: 0,
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

    #[inline]
    pub const fn flags(self) -> SessionEvtFlags {
        self.flags
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
    RxEnq = 0,
    TxDeq = 1,
    Connect = 2,
    Close = 3,
    RxDeq = 4,
    TxEnq = 5,
    ProtocolOutput = 6,
    HalfClose = 7,
    Reset = 8,
    Disconnected = 9,
    TransportClosed = 10,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionMsgQueueError {
    #[error("Session Message Queue configuration is invalid")]
    InvalidConfig,
    #[error("Session Message Queue is full")]
    Full(SessionEvt),
    #[error("Session Message Queue CTRL ring is full")]
    ControlFull,
    #[error("CTRL payload has {bytes} bytes but the ring permits {capacity}")]
    ControlPayloadTooLarge { bytes: usize, capacity: usize },
    #[error("CTRL payload length {bytes} exceeds its element capacity {capacity}")]
    ControlPayloadCorrupt { bytes: usize, capacity: usize },
    #[error("CTRL payload has {bytes} bytes but the destination permits {capacity}")]
    ControlPayloadBufferTooSmall { bytes: usize, capacity: usize },
    #[error("expected a CTRL ring message but dequeued ring {ring}")]
    UnexpectedRing { ring: u32 },
    #[error("failed to create Session Message Queue signal pipe")]
    SignalPipeCreate,
    #[error("failed to read Session Message Queue signal status flags")]
    SignalStatusFlags,
    #[error("failed to set Session Message Queue signal nonblocking status")]
    SignalNonblocking,
    #[error("failed to read Session Message Queue signal descriptor flags")]
    SignalDescriptorFlags,
    #[error("failed to set Session Message Queue signal close-on-exec")]
    SignalCloseOnExec,
}

/// Shared Session Message Queue operations.
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
    fn write_fd(&self) -> Option<RawFd>;
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

fn control_ring_cfg(
    q_nitems: u32,
    ring_nitems: u32,
    control_element_size: usize,
) -> Result<[RingCfg; 2], SessionMsgQueueError> {
    if control_element_size < size_of::<u32>() {
        return Err(SessionMsgQueueError::InvalidConfig);
    }
    let mut rings = session_ring_cfg(q_nitems, ring_nitems)?;
    rings[SessionMqRing::Ctrl as usize].elsize = control_element_size;
    Ok(rings)
}

/// Session Message Queue with IO + CTRL rings over the infra multi-ring primitive.
pub struct SessionMsgQueue {
    inner: MultiRingMsgQueue,
    signal_atomic: AtomicBool,
    signal_read: Option<Arc<OwnedFd>>,
    signal_write: Option<Arc<OwnedFd>>,
}

impl SessionMsgQueue {
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
        .map_err(|_| SessionMsgQueueError::InvalidConfig)?;
        Ok(Self {
            inner,
            signal_atomic: AtomicBool::new(false),
            signal_read: None,
            signal_write: None,
        })
    }

    /// Number of queued events, matching VPP `svm_msg_q_size`.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl SessionMsgQueue {
    /// On-segment byte size for a Session Message Queue with the given capacities.
    pub fn layout_bytes(q_nitems: u32, ring_nitems: u32) -> Result<usize, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
        Ok(MultiRingMsgQueue::layout_bytes(&MultiRingMsgQueueCfg {
            q_nitems,
            rings: &rings,
        }))
    }

    pub(crate) fn layout_bytes_with_ctrl_element(
        q_nitems: u32,
        ring_nitems: u32,
        control_element_size: usize,
    ) -> Result<usize, SessionMsgQueueError> {
        let rings = control_ring_cfg(q_nitems, ring_nitems, control_element_size)?;
        Ok(MultiRingMsgQueue::layout_bytes(&MultiRingMsgQueueCfg {
            q_nitems,
            rings: &rings,
        }))
    }

    /// Initialise a Session Message Queue at a pre-allocated segment offset.
    ///
    /// # Safety
    /// Caller must reserve at least [`Self::layout_bytes`] at `hdr_offset`.
    pub unsafe fn init_at(
        seg: Segment,
        hdr_offset: u64,
        q_nitems: u32,
        ring_nitems: u32,
    ) -> Result<Self, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
        unsafe { Self::init_at_with_rings(seg, hdr_offset, q_nitems, &rings) }
    }

    unsafe fn init_at_with_rings(
        seg: Segment,
        hdr_offset: u64,
        q_nitems: u32,
        rings: &[RingCfg; 2],
    ) -> Result<Self, SessionMsgQueueError> {
        let inner = unsafe {
            MultiRingMsgQueue::init_at(seg, hdr_offset, &MultiRingMsgQueueCfg { q_nitems, rings })
        }
        .map_err(|_| SessionMsgQueueError::InvalidConfig)?;
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
    /// Every supplied descriptor must be valid and transfer ownership to the
    /// returned queue. Equal read/write values are treated as one shared eventfd.
    pub unsafe fn from_shared(
        seg: Segment,
        hdr_offset: u64,
        signal_read: Option<RawFd>,
        signal_write: Option<RawFd>,
    ) -> Self {
        let (signal_read, signal_write) = match (signal_read, signal_write) {
            (Some(read), Some(write)) if read == write => {
                // SAFETY: the caller transfers one valid descriptor; equal values
                // name the same open file description and must have one owner.
                let signal = Arc::new(unsafe { OwnedFd::from_raw_fd(read) });
                (Some(Arc::clone(&signal)), Some(signal))
            }
            (read, write) => {
                let read = match read {
                    Some(fd) => {
                        // SAFETY: the caller transfers ownership of this descriptor.
                        Some(Arc::new(unsafe { OwnedFd::from_raw_fd(fd) }))
                    }
                    None => None,
                };
                let write = match write {
                    Some(fd) => {
                        // SAFETY: the caller transfers ownership of this descriptor.
                        Some(Arc::new(unsafe { OwnedFd::from_raw_fd(fd) }))
                    }
                    None => None,
                };
                (read, write)
            }
        };
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

    /// Dequeues one event together with the MQ ring that classified it.
    #[inline]
    pub fn dequeue_with_ring(&self) -> Option<(SessionMqRing, SessionEvt)> {
        let Some(message) = self.inner.sub() else {
            return None;
        };
        // A SessionMsgQueue is always laid out as exactly [IO, CTRL]. The
        // descriptor's ring is therefore already the classification fact;
        // there is no third ring to recover from here.
        let ring = if message.ring_index() == SessionMqRing::Ctrl as u32 {
            SessionMqRing::Ctrl
        } else {
            SessionMqRing::Io
        };
        let event = SessionEvt::from_bytes(message.as_slice());
        drop(message);
        Some((ring, event))
    }

    pub(crate) fn enqueue_ctrl_payload(&self, payload: &[u8]) -> Result<(), SessionMsgQueueError> {
        let element_size = self
            .inner
            .ring_element_size(SessionMqRing::Ctrl as u32)
            .ok_or(SessionMsgQueueError::InvalidConfig)?;
        let capacity = element_size - size_of::<u32>();
        if payload.len() > capacity {
            return Err(SessionMsgQueueError::ControlPayloadTooLarge {
                bytes: payload.len(),
                capacity,
            });
        }
        let mut queue = self.inner.lock();
        let mut message = queue
            .alloc(SessionMqRing::Ctrl as u32)
            .map_err(|error| match error {
                MultiRingMsgQueueError::QueueFull | MultiRingMsgQueueError::RingFull => {
                    SessionMsgQueueError::ControlFull
                }
                MultiRingMsgQueueError::InvalidConfig | MultiRingMsgQueueError::BadRing => {
                    SessionMsgQueueError::InvalidConfig
                }
            })?;
        let bytes = message.as_mut_slice();
        bytes.fill(0);
        bytes[..size_of::<u32>()].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes[size_of::<u32>()..size_of::<u32>() + payload.len()].copy_from_slice(payload);
        queue.add(message);
        let became_non_empty = self.inner.len() == 1;
        drop(queue);
        if became_non_empty {
            self.fire();
        }
        Ok(())
    }

    pub(crate) fn dequeue_ctrl_payload(
        &self,
        payload: &mut [u8],
    ) -> Result<Option<usize>, SessionMsgQueueError> {
        let Some(message) = self.inner.sub() else {
            return Ok(None);
        };
        if message.ring_index() != SessionMqRing::Ctrl as u32 {
            return Err(SessionMsgQueueError::UnexpectedRing {
                ring: message.ring_index(),
            });
        }
        let bytes = message.as_slice();
        let payload_len = u32::from_le_bytes(
            bytes[..size_of::<u32>()]
                .try_into()
                .expect("CTRL element stores a complete payload length"),
        ) as usize;
        let capacity = bytes.len() - size_of::<u32>();
        if payload_len > capacity {
            return Err(SessionMsgQueueError::ControlPayloadCorrupt {
                bytes: payload_len,
                capacity,
            });
        }
        if payload_len > payload.len() {
            return Err(SessionMsgQueueError::ControlPayloadBufferTooSmall {
                bytes: payload_len,
                capacity: payload.len(),
            });
        }
        payload[..payload_len]
            .copy_from_slice(&bytes[size_of::<u32>()..size_of::<u32>() + payload_len]);
        Ok(Some(payload_len))
    }

    pub fn wait(&self) -> std::io::Result<()> {
        let descriptor = self.read_fd().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "Session Message Queue has no read signal",
            )
        })?;
        loop {
            let mut poll = libc::pollfd {
                fd: descriptor,
                events: libc::POLLIN,
                revents: 0,
            };
            let ready = unsafe { libc::poll(std::ptr::from_mut(&mut poll), 1, -1) };
            if ready >= 0 {
                self.drain();
                return Ok(());
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
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
        let became_non_empty = self.inner.len() == 1;
        drop(guard);
        if became_non_empty {
            self.fire();
        }
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
        out.iter_mut()
            .zip(std::iter::from_fn(|| SessionEventQueue::dequeue(self)))
            .map(|(slot, event)| *slot = event)
            .count()
    }

    fn fire(&self) {
        if let Some(signal_write) = &self.signal_write {
            let fd = signal_write.as_raw_fd();
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
        if let Some(signal_read) = &self.signal_read {
            let fd = signal_read.as_raw_fd();
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
        std::iter::from_fn(|| SessionEventQueue::dequeue(self)).for_each(std::mem::drop);
        if let Some(signal_read) = &self.signal_read {
            let fd = signal_read.as_raw_fd();
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
        self.signal_read.as_ref().map(|signal| signal.as_raw_fd())
    }

    fn write_fd(&self) -> Option<RawFd> {
        self.signal_write.as_ref().map(|signal| signal.as_raw_fd())
    }
}

impl SessionMsgQueue {
    /// Initialise an SVM queue and attach a nonblocking, close-on-exec
    /// app-write/dataplane-read signal pair owned by the queue.
    ///
    /// # Safety
    /// Caller must reserve at least [`Self::layout_bytes`] at `hdr_offset`.
    pub unsafe fn init_at_with_signal(
        seg: Segment,
        hdr_offset: u64,
        q_nitems: u32,
        ring_nitems: u32,
    ) -> Result<Self, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
        unsafe { Self::init_at_with_signal_and_rings(seg, hdr_offset, q_nitems, &rings) }
    }

    pub(crate) unsafe fn init_at_with_signal_and_ctrl_element(
        seg: Segment,
        hdr_offset: u64,
        q_nitems: u32,
        ring_nitems: u32,
        control_element_size: usize,
    ) -> Result<Self, SessionMsgQueueError> {
        let rings = control_ring_cfg(q_nitems, ring_nitems, control_element_size)?;
        unsafe { Self::init_at_with_signal_and_rings(seg, hdr_offset, q_nitems, &rings) }
    }

    unsafe fn init_at_with_signal_and_rings(
        seg: Segment,
        hdr_offset: u64,
        q_nitems: u32,
        rings: &[RingCfg; 2],
    ) -> Result<Self, SessionMsgQueueError> {
        // SAFETY: forwarded caller layout guarantee initializes the shared
        // queue before it is remapped with the signal endpoint ownership.
        drop(unsafe { Self::init_at_with_rings(seg.clone(), hdr_offset, q_nitems, rings) }?);
        let mut fds = [-1; 2];
        // SAFETY: `fds` is writable for two descriptor values. On success each
        // descriptor is immediately transferred into exactly one OwnedFd.
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            return Err(SessionMsgQueueError::SignalPipeCreate);
        }
        // SAFETY: pipe returned fresh, distinct descriptors and ownership moves
        // into the two OwnedFd values exactly once.
        let signal_read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
        // SAFETY: see the preceding ownership transfer for the write endpoint.
        let signal_write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
        [&signal_read, &signal_write].into_iter().try_for_each(
            |fd| -> Result<(), SessionMsgQueueError> {
                // SAFETY: `fd` remains owned by this function; fcntl only queries
                // descriptor flags and does not take ownership.
                let status_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
                if status_flags < 0 {
                    return Err(SessionMsgQueueError::SignalStatusFlags);
                }
                // SAFETY: as above; queried flags are valid input for F_SETFL.
                if unsafe {
                    libc::fcntl(
                        fd.as_raw_fd(),
                        libc::F_SETFL,
                        status_flags | libc::O_NONBLOCK,
                    )
                } < 0
                {
                    return Err(SessionMsgQueueError::SignalNonblocking);
                }
                // SAFETY: as above; this only queries descriptor flags.
                let descriptor_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
                if descriptor_flags < 0 {
                    return Err(SessionMsgQueueError::SignalDescriptorFlags);
                }
                // SAFETY: as above; queried flags are valid input for F_SETFD.
                if unsafe {
                    libc::fcntl(
                        fd.as_raw_fd(),
                        libc::F_SETFD,
                        descriptor_flags | libc::FD_CLOEXEC,
                    )
                } < 0
                {
                    return Err(SessionMsgQueueError::SignalCloseOnExec);
                }
                Ok(())
            },
        )?;
        // SAFETY: the queue was initialized above and the fresh descriptors
        // transfer sole ownership into the returned queue.
        Ok(unsafe {
            Self::from_shared(
                seg,
                hdr_offset,
                Some(signal_read.into_raw_fd()),
                Some(signal_write.into_raw_fd()),
            )
        })
    }
}

const _: () = assert!(SESSION_EVT_BYTES == 16);

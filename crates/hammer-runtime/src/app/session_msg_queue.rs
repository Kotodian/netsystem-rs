//! Session Message Queue: runtime wrapper over the infra multi-ring queue.
//!
//! IO events use [`SessionMqRing::Io`]; Connect/Close use [`SessionMqRing::Ctrl`].
//! Callers choose the ring via [`SessionMsgQueue::enqueue_io`] /
//! [`SessionMsgQueue::enqueue_ctrl`] — not by matching `evt_type` inside enqueue.
//!
//! Session Event identity follows ADR-0010 (VPP `session_event_t` rules).
//!
//! Application Session control queues use the same locked shared queue as
//! worker event queues. They publish fixed VPP-shaped control slots (the
//! payload contract is [`SESSION_CTRL_MSG_MAX_SIZE`], while the physical
//! control element is the VPP `256`-byte slot) via
//! [`SessionMsgQueue::enqueue_control`]. The consumer reads them with
//! [`SessionMsgQueue::dequeue_control`], which returns a borrowed
//! [`SessionControlItem`] that decodes on request. The slot carries one
//! event-type byte plus the private fixed-layout payload selected by that
//! type; there is no `[event][length][payload]` envelope and no heap
//! allocation in the hot path.

use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use hammer_core::session::{SessionEvt, SessionEvtType};
use hammer_infra::multi_ring_msg_queue::{
    MultiProducer, MultiRingMsgQueue, MultiRingMsgQueueCfg, MultiRingMsgQueueError, RingCfg,
    RingMsg,
};
use hammer_infra::segment::Segment;

/// VPP session MQ ring roles (`session_mq_rings_e`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum SessionMqRing {
    Io = 0,
    Ctrl = 1,
}

/// Fixed Application Session control slot size in bytes.
///
/// Mirrors VPP `SESSION_CTRL_MSG_MAX_SIZE` (third_party/vpp/src/vnet/session/
/// session_types.h:53) and the per-element control payload array
/// `session_evt_ctrl_data_t.data[SESSION_CTRL_MSG_MAX_SIZE]` (session.h:51-54).
/// The slot is `[event_type: u8][payload: 85]`.
pub const SESSION_CTRL_MSG_MAX_SIZE: usize = 86;
const SESSION_CTRL_MSG_BYTES: usize = 256;

/// Fixed shared-memory Session event codec size. VPP keeps event type and
/// postponed state before a target union; Hammer preserves the fixed slot,
/// explicit Session identity fields, and its own event metadata.
const SESSION_EVT_BYTES: usize = 24;

fn encode_session_evt(event: SessionEvt) -> [u8; SESSION_EVT_BYTES] {
    let mut bytes = [0_u8; SESSION_EVT_BYTES];
    bytes[0] = event.evt_type as u8;
    bytes[1] = u8::from(event.postponed);
    bytes[2..6].copy_from_slice(&event.session_index.to_le_bytes());
    bytes[6..10].copy_from_slice(&event.thread_index.to_le_bytes());
    bytes[10..18].copy_from_slice(&event.control_data.to_le_bytes());
    bytes
}

fn decode_session_evt(bytes: &[u8]) -> Result<SessionEvt, SessionEvtDecodeError> {
    if bytes.len() != SESSION_EVT_BYTES {
        return Err(SessionEvtDecodeError::InvalidLength { bytes: bytes.len() });
    }
    let evt_type = SessionEvtType::try_from(bytes[0])
        .map_err(|value| SessionEvtDecodeError::InvalidType { value })?;
    let postponed = match bytes[1] {
        0 => false,
        1 => true,
        value => return Err(SessionEvtDecodeError::InvalidPostponed { value }),
    };
    if bytes[18..].iter().any(|byte| *byte != 0) {
        return Err(SessionEvtDecodeError::ReservedBytes);
    }
    Ok(SessionEvt {
        evt_type,
        postponed,
        session_index: u32::from_le_bytes(bytes[2..6].try_into().expect("four byte index")),
        thread_index: u32::from_le_bytes(bytes[6..10].try_into().expect("four byte index")),
        control_data: u64::from_le_bytes(bytes[10..18].try_into().expect("eight byte data")),
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionEvtDecodeError {
    InvalidLength { bytes: usize },
    InvalidType { value: u8 },
    InvalidPostponed { value: u8 },
    ReservedBytes,
}

#[hammer_component_macros::runtime_error(subsystem = "application session MQ")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SessionMsgQueueError {
    #[error("Session Message Queue configuration is invalid")]
    InvalidConfig,
    #[error("Session Message Queue is full")]
    Full(SessionEvt),
    #[error("Session Message Queue CTRL ring is full")]
    ControlFull,
    #[error("CTRL slot event type {value} is not a known Session control event")]
    InvalidControlEventType { value: u8 },
    #[error("Session Message Queue ring {ring} is unexpected for this dequeue path")]
    UnexpectedRing { ring: u32 },
    #[error("Session Message Queue single-producer capability is already claimed")]
    ProducerClaimed,
    #[error(
        "Session Message Queue producer mode {expected} does not match on-segment mode {actual}"
    )]
    ModeMismatch { expected: u32, actual: u32 },
    #[error("failed to write Session Message Queue signal")]
    SignalWrite,
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
    fn dequeue(&self) -> Result<Option<SessionEvt>, SessionMsgQueueError>;
    fn dequeue_batch(&self, out: &mut [SessionEvt]) -> Result<usize, SessionMsgQueueError>;
    fn fire(&self);
    fn drain(&self) -> bool;
    fn read_signal(&self) -> bool;
    fn clear(&self);
    fn is_empty(&self) -> bool;
    fn read_fd(&self) -> Option<RawFd>;
    fn write_fd(&self) -> Option<RawFd>;
}

fn session_ring_cfg(q_nitems: u32, ring_nitems: u32) -> Result<[RingCfg; 2], SessionMsgQueueError> {
    if q_nitems < 1 || ring_nitems < 1 {
        return Err(SessionMsgQueueError::InvalidConfig);
    }
    let ctrl_nitems = (ring_nitems / 2).max(1);
    Ok([
        RingCfg {
            nitems: ctrl_nitems,
            elsize: SESSION_EVT_BYTES,
        },
        RingCfg {
            nitems: ring_nitems,
            elsize: SESSION_EVT_BYTES,
        },
    ])
}

fn control_ring_cfg(q_nitems: u32, ring_nitems: u32) -> Result<[RingCfg; 2], SessionMsgQueueError> {
    let mut rings = session_ring_cfg(q_nitems, ring_nitems)?;
    rings[SessionMqRing::Ctrl as usize].elsize = SESSION_CTRL_MSG_BYTES;
    Ok(rings)
}

/// One borrowed control-slot item from the CTRL ring.
///
/// The slot's leading event type selects the concrete payload; the payload
/// itself stays opaque here and is decoded on request via
/// [`SessionControlItem::decode`], which checks the requested concrete
/// message type against the slot's event type before decoding.
pub struct SessionControlItem<'a> {
    message: RingMsg<'a>,
    event_type: SessionEvtType,
}

impl SessionControlItem<'_> {
    /// Event type selecting the concrete payload in this slot.
    #[inline]
    pub fn event_type(&self) -> SessionEvtType {
        self.event_type
    }

    /// Decodes the slot as the requested concrete control message.
    ///
    /// Returns `None` when the slot's event type selects a different concrete
    /// message type; the caller may try another type or drop the item.
    #[inline]
    pub fn decode<M: super::control::SessionControlPayload>(
        &self,
    ) -> Option<Result<M, super::control::SessionControlDecodeError>> {
        if !M::is_event_type(self.event_type) {
            return None;
        }
        Some(M::decode_wire(self.payload()))
    }

    #[inline]
    fn payload(&self) -> &[u8] {
        &self.message.as_slice()[1..]
    }
}

/// Session Message Queue: runtime wrapper over the infra multi-ring queue.
///
/// The queue uses one VPP-style shared lock for all producers. Ring selection
/// is explicit at each operation, matching `svm_msg_q_alloc_msg_w_ring`.
pub struct SessionMsgQueue {
    inner: MultiRingMsgQueue<MultiProducer>,
    signal_atomic: Arc<AtomicBool>,
    signal_read: Option<Arc<OwnedFd>>,
    signal_write: Option<Arc<OwnedFd>>,
}

// All queue state is in the shared mapping and producer serialization is
// provided by the VPP-shaped queue lock.
unsafe impl Send for SessionMsgQueue {}
unsafe impl Sync for SessionMsgQueue {}

impl SessionMsgQueue {
    /// Number of queued events, matching VPP `svm_msg_q_size`.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
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

    /// Read endpoint of the queue's signal pair, when owned.
    pub fn read_fd(&self) -> Option<RawFd> {
        self.signal_read.as_ref().map(|signal| signal.as_raw_fd())
    }

    /// Write endpoint of the queue's signal pair, when owned.
    pub fn write_fd(&self) -> Option<RawFd> {
        self.signal_write.as_ref().map(|signal| signal.as_raw_fd())
    }

    unsafe fn init_at_with_rings(
        seg: Segment,
        hdr_offset: u64,
        q_nitems: u32,
        rings: &[RingCfg; 2],
    ) -> Result<Self, SessionMsgQueueError> {
        let inner = unsafe {
            MultiRingMsgQueue::<MultiProducer>::init_at(
                seg,
                hdr_offset,
                &MultiRingMsgQueueCfg { q_nitems, rings },
            )
        }
        .map_err(|_| SessionMsgQueueError::InvalidConfig)?;
        Ok(Self {
            inner,
            signal_atomic: Arc::new(AtomicBool::new(false)),
            signal_read: None,
            signal_write: None,
        })
    }

    /// Initialise a queue at a pre-allocated offset and attach a nonblocking,
    /// close-on-exec signal pair owned by the queue.
    ///
    /// # Safety
    /// Caller must reserve the mode's on-segment layout at `hdr_offset`.
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
        unsafe {
            Self::from_shared(
                seg,
                hdr_offset,
                Some(signal_read.into_raw_fd()),
                Some(signal_write.into_raw_fd()),
            )
        }
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
    ) -> Result<Self, SessionMsgQueueError> {
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
        let inner = unsafe { MultiRingMsgQueue::<MultiProducer>::from_shared(seg, hdr_offset) }
            .map_err(|error| match error {
                MultiRingMsgQueueError::ModeMismatch { expected, actual } => {
                    SessionMsgQueueError::ModeMismatch { expected, actual }
                }
                _ => SessionMsgQueueError::InvalidConfig,
            })?;
        Ok(Self {
            inner,
            signal_atomic: Arc::new(AtomicBool::new(false)),
            signal_read,
            signal_write,
        })
    }
}

impl SessionMsgQueue {
    /// Default Local queue: 2048 descriptors, 1024 slots per ring, SessionEvt elsize.
    pub fn with_defaults() -> Result<Self, SessionMsgQueueError> {
        Self::with_cfg(2048, 1024)
    }

    pub fn with_cfg(q_nitems: u32, ring_nitems: u32) -> Result<Self, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
        let inner = MultiRingMsgQueue::<MultiProducer>::with_cfg(MultiRingMsgQueueCfg {
            q_nitems,
            rings: &rings,
        })
        .map_err(|_| SessionMsgQueueError::InvalidConfig)?;
        Ok(Self {
            inner,
            signal_atomic: Arc::new(AtomicBool::new(false)),
            signal_read: None,
            signal_write: None,
        })
    }

    /// On-segment byte size for a Session Message Queue with the given capacities.
    pub fn layout_bytes(q_nitems: u32, ring_nitems: u32) -> Result<usize, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
        Ok(MultiRingMsgQueue::<MultiProducer>::layout_bytes(
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
        seg: Segment,
        hdr_offset: u64,
        q_nitems: u32,
        ring_nitems: u32,
    ) -> Result<Self, SessionMsgQueueError> {
        let rings = session_ring_cfg(q_nitems, ring_nitems)?;
        unsafe { Self::init_at_with_rings(seg, hdr_offset, q_nitems, &rings) }
    }

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

    #[inline]
    pub fn enqueue_io(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        SessionEventQueue::enqueue_io(self, evt)
    }

    #[inline]
    pub fn enqueue_ctrl(&self, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        SessionEventQueue::enqueue_ctrl(self, evt)
    }

    #[inline]
    pub fn dequeue(&self) -> Result<Option<SessionEvt>, SessionMsgQueueError> {
        SessionEventQueue::dequeue(self)
    }

    /// Dequeues one event with its ring classification.
    #[inline]
    pub fn dequeue_with_ring(
        &self,
    ) -> Result<Option<(SessionMqRing, SessionEvt)>, SessionMsgQueueError> {
        let Some(message) = self.inner.sub() else {
            return Ok(None);
        };
        let event = decode_session_evt(message.as_slice())
            .map_err(|_| SessionMsgQueueError::InvalidConfig)?;
        let ring = if message.ring_index() == SessionMqRing::Io as u32 {
            SessionMqRing::Io
        } else {
            SessionMqRing::Ctrl
        };
        drop(message);
        Ok(Some((ring, event)))
    }

    fn enqueue_on(&self, ring: SessionMqRing, evt: SessionEvt) -> Result<(), SessionMsgQueueError> {
        let bytes = encode_session_evt(evt);
        let mut guard = self.inner.lock();
        let mut slot = match guard.alloc(ring as u32) {
            Ok(slot) => slot,
            Err(MultiRingMsgQueueError::QueueFull | MultiRingMsgQueueError::RingFull) => {
                return Err(SessionMsgQueueError::Full(evt));
            }
            Err(MultiRingMsgQueueError::InvalidConfig | MultiRingMsgQueueError::BadRing) => {
                return Err(SessionMsgQueueError::InvalidConfig);
            }
            Err(
                MultiRingMsgQueueError::ProducerClaimed
                | MultiRingMsgQueueError::ModeMismatch { .. },
            ) => {
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

    fn dequeue(&self) -> Result<Option<SessionEvt>, SessionMsgQueueError> {
        let Some(msg) = self.inner.sub() else {
            return Ok(None);
        };
        let evt =
            decode_session_evt(msg.as_slice()).map_err(|_| SessionMsgQueueError::InvalidConfig)?;
        drop(msg);
        Ok(Some(evt))
    }

    fn dequeue_batch(&self, out: &mut [SessionEvt]) -> Result<usize, SessionMsgQueueError> {
        let mut count = 0;
        for slot in out.iter_mut() {
            match SessionEventQueue::dequeue(self) {
                Ok(Some(event)) => *slot = event,
                Ok(None) => break,
                Err(error) => return Err(error),
            }
            count += 1;
        }
        Ok(count)
    }

    fn fire(&self) {
        if let Some(signal_write) = &self.signal_write {
            let fd = signal_write.as_raw_fd();
            let val: [u8; 1] = [1];
            let ret = unsafe { libc::write(fd, val.as_ptr() as *const libc::c_void, 1) };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() != std::io::ErrorKind::WouldBlock {
                    // The enqueue is already committed (VPP
                    // `svm_msg_q_send_signal` warns and returns): report the
                    // typed IO detail through logging and keep the enqueue
                    // successful — a retryable error could duplicate the
                    // message.
                    tracing::warn!(
                        error = %SessionMsgQueueError::SignalWrite,
                        "failed to signal Session Message Queue consumer"
                    );
                }
            }
        } else {
            self.signal_atomic.store(true, Ordering::Release);
        }
    }

    fn drain(&self) -> bool {
        self.drain()
    }

    fn read_signal(&self) -> bool {
        self.read_signal()
    }

    fn clear(&self) {
        // Drain until empty. A misconfigured element (decode failure) is
        // consumed so the next clear can make progress.
        while let Ok(Some(_)) = SessionEventQueue::dequeue(self) {}
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
        self.is_empty()
    }

    fn read_fd(&self) -> Option<RawFd> {
        self.read_fd()
    }

    fn write_fd(&self) -> Option<RawFd> {
        self.write_fd()
    }
}

impl SessionMsgQueue {
    /// Local Application Session control queue: 2048 descriptors, 1024 slots
    /// per ring, fixed [`SESSION_CTRL_MSG_MAX_SIZE`] control slots on CTRL.
    pub fn with_control_defaults() -> Result<Self, SessionMsgQueueError> {
        Self::with_control_cfg(2048, 1024)
    }

    /// Local Application Session control queue with explicit capacities.
    pub fn with_control_cfg(q_nitems: u32, ring_nitems: u32) -> Result<Self, SessionMsgQueueError> {
        let rings = control_ring_cfg(q_nitems, ring_nitems)?;
        let inner = MultiRingMsgQueue::<MultiProducer>::with_cfg(MultiRingMsgQueueCfg {
            q_nitems,
            rings: &rings,
        })
        .map_err(|_| SessionMsgQueueError::InvalidConfig)?;
        Ok(Self {
            inner,
            signal_atomic: Arc::new(AtomicBool::new(false)),
            signal_read: None,
            signal_write: None,
        })
    }

    /// On-segment byte size for a Session control queue with the given
    /// capacities and fixed [`SESSION_CTRL_MSG_MAX_SIZE`] control slots.
    pub fn layout_bytes_with_control(
        q_nitems: u32,
        ring_nitems: u32,
    ) -> Result<usize, SessionMsgQueueError> {
        let rings = control_ring_cfg(q_nitems, ring_nitems)?;
        Ok(MultiRingMsgQueue::<MultiProducer>::layout_bytes(
            &MultiRingMsgQueueCfg {
                q_nitems,
                rings: &rings,
            },
        ))
    }

    /// Initialise a Session control queue (fixed [`SESSION_CTRL_MSG_MAX_SIZE`]
    /// CTRL slots) and attach a nonblocking, close-on-exec signal pair.
    ///
    /// # Safety
    /// Caller must reserve at least [`Self::layout_bytes_with_control`] at
    /// `hdr_offset`.
    pub unsafe fn init_at_with_signal_and_control(
        seg: Segment,
        hdr_offset: u64,
        q_nitems: u32,
        ring_nitems: u32,
    ) -> Result<Self, SessionMsgQueueError> {
        let rings = control_ring_cfg(q_nitems, ring_nitems)?;
        unsafe { Self::init_at_with_signal_and_rings(seg, hdr_offset, q_nitems, &rings) }
    }

    /// Enqueues one concrete Session control message into the CTRL ring.
    /// Allocation, payload write, publication, and wakeup follow the VPP
    /// message-queue producer order while the queue lock serializes producers.
    pub fn enqueue_control<M: super::control::SessionControlPayload>(
        &self,
        message: &M,
    ) -> Result<(), SessionMsgQueueError> {
        if self.inner.ring_element_size(SessionMqRing::Ctrl as u32) != Some(SESSION_CTRL_MSG_BYTES)
        {
            return Err(SessionMsgQueueError::InvalidConfig);
        }
        let mut guard = self.inner.lock();
        let mut reservation = match guard.alloc(SessionMqRing::Ctrl as u32) {
            Ok(slot) => slot,
            Err(MultiRingMsgQueueError::QueueFull | MultiRingMsgQueueError::RingFull) => {
                return Err(SessionMsgQueueError::ControlFull);
            }
            Err(_) => return Err(SessionMsgQueueError::InvalidConfig),
        };
        let payload = reservation.as_mut_slice();
        payload.fill(0);
        payload[0] = message.event_type() as u8;
        message.encode_wire(&mut payload[1..]);
        guard.add(reservation);
        let became_non_empty = self.inner.len() == 1;
        drop(guard);
        if became_non_empty {
            self.fire();
        }
        Ok(())
    }

    /// Moves a control queue into the owner-side producer handle. VPP uses the
    /// queue lock for producer serialization; there is no producer claim tag.
    pub fn claim_producer(self) -> Result<SessionProducer, SessionMsgQueueError> {
        Ok(SessionProducer { queue: self })
    }

    /// Dequeues one borrowed control slot from the CTRL ring.
    ///
    /// The slot's event type is validated against [`SessionEvtType`] before
    /// the borrowed [`SessionControlItem`] is returned; an unknown event type
    /// is a typed queue error rather than a misdecode. A descriptor whose
    /// next ring is not CTRL is reported as
    /// [`SessionMsgQueueError::UnexpectedRing`] without being consumed: the
    /// message stays queued for its own consumer path.
    pub fn dequeue_control(
        &mut self,
    ) -> Result<Option<SessionControlItem<'_>>, SessionMsgQueueError> {
        // Symmetric with SessionProducer::enqueue_control: only a fixed
        // SESSION_CTRL_MSG_MAX_SIZE CTRL ring is a control queue. A
        // SessionEvt-sized CTRL ring carries worker control events, not
        // control slots, and decoding one as a slot would misread it. Nothing
        // is consumed on this path.
        if self.inner.ring_element_size(SessionMqRing::Ctrl as u32) != Some(SESSION_CTRL_MSG_BYTES)
        {
            return Err(SessionMsgQueueError::InvalidConfig);
        }
        // Validate the next ring before advancing head: the single-producer
        // consumer holds `&mut self`, so no other consumer can advance head
        // between this peek and the sub below.
        if let Some(ring) = self.inner.peek_ring() {
            if ring != SessionMqRing::Ctrl as u32 {
                return Err(SessionMsgQueueError::UnexpectedRing { ring });
            }
        }
        let Some(message) = self.inner.sub() else {
            return Ok(None);
        };
        let bytes = message.as_slice();
        let event_type = SessionEvtType::try_from(bytes[0])
            .map_err(|value| SessionMsgQueueError::InvalidControlEventType { value })?;
        Ok(Some(SessionControlItem {
            message,
            event_type,
        }))
    }
}

pub struct SessionProducer {
    queue: SessionMsgQueue,
}

impl SessionProducer {
    /// Enqueues one concrete Session control message into a fixed
    /// VPP-shaped control slot on the CTRL ring.
    ///
    /// Static dispatch through the sealed [`SessionControlPayload`] trait:
    /// callers pass the concrete message and never touch wire bytes.
    pub fn enqueue_control<M: super::control::SessionControlPayload>(
        &mut self,
        message: &M,
    ) -> Result<(), SessionMsgQueueError> {
        self.queue.enqueue_control(message)
    }

    /// Read endpoint of the queue's signal pair, when owned.
    pub fn read_fd(&self) -> Option<RawFd> {
        self.queue.read_fd()
    }

    /// Write endpoint of the queue's signal pair, when owned.
    pub fn write_fd(&self) -> Option<RawFd> {
        self.queue.write_fd()
    }
}

const _: () = assert!(SESSION_EVT_BYTES == 24);

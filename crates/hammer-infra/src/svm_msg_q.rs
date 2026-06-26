use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::ring::{LockFreeRing, RingError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SessionEvt {
    pub session_index: u32,
    pub evt_type: SessionEvtType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionEvtType {
    /// RX fifo has new data (VPP→app, SESSION_EVT_ENQ).
    RxEnq,
    /// TX fifo has space (SESSION_EVT_DEQ).
    TxDeq,
    /// Connection established (SESSION_EVT_CONNECT).
    Connect,
    /// Connection closed (SESSION_EVT_CLOSE).
    Close,
}

#[derive(Debug)]
pub enum SvmMsgQError {
    InvalidCapacity,
    Full(SessionEvt),
    Eventfd(io::Error),
}

impl Clone for SvmMsgQError {
    fn clone(&self) -> Self {
        match self {
            Self::InvalidCapacity => Self::InvalidCapacity,
            Self::Full(evt) => Self::Full(*evt),
            Self::Eventfd(err) => Self::Eventfd(io::Error::new(err.kind(), err.to_string())),
        }
    }
}

impl PartialEq for SvmMsgQError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InvalidCapacity, Self::InvalidCapacity) => true,
            (Self::Full(a), Self::Full(b)) => a == b,
            (Self::Eventfd(a), Self::Eventfd(b)) => {
                a.kind() == b.kind() && a.raw_os_error() == b.raw_os_error()
            }
            _ => false,
        }
    }
}

impl Eq for SvmMsgQError {}

#[cfg(target_os = "linux")]
struct EventfdSignal {
    fd: i32,
}

#[cfg(target_os = "linux")]
impl EventfdSignal {
    fn new() -> Result<Self, SvmMsgQError> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(SvmMsgQError::Eventfd(io::Error::last_os_error()));
        }
        Ok(Self { fd })
    }

    fn fd(&self) -> i32 {
        self.fd
    }

    fn signal(&self) {
        let value: u64 = 1;
        loop {
            // SAFETY: `self.fd` is owned by this `EventfdSignal` from construction until
            // `Drop` closes it. The write target is a stack `u64`; no aliasing with the
            // kernel eventfd buffer. Non-blocking fd: EAGAIN means the counter is full
            // (u64::MAX); retry after a consumer read drains it.
            let ret = unsafe {
                libc::write(
                    self.fd,
                    &value as *const u64 as *const libc::c_void,
                    std::mem::size_of::<u64>(),
                )
            };
            if ret >= 0 {
                return;
            }
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                continue;
            }
            panic!("eventfd write failed: {err}");
        }
    }

    fn read_signal(&self) -> bool {
        let mut value: u64 = 0;
        // SAFETY: `self.fd` is valid for the lifetime of this signal object. The read
        // buffer is a stack `u64`. Non-blocking fd: EAGAIN means no pending signal.
        let ret = unsafe {
            libc::read(
                self.fd,
                &mut value as *mut u64 as *mut libc::c_void,
                std::mem::size_of::<u64>(),
            )
        };
        if ret >= 0 {
            return true;
        }
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EAGAIN) {
            return false;
        }
        panic!("eventfd read failed: {err}");
    }
}

#[cfg(target_os = "linux")]
impl Drop for EventfdSignal {
    fn drop(&mut self) {
        // SAFETY: `self.fd` is owned exclusively here; closing twice is prevented by Drop.
        let ret = unsafe { libc::close(self.fd) };
        debug_assert_eq!(ret, 0, "eventfd close failed");
    }
}

#[cfg(not(target_os = "linux"))]
struct AtomicBoolSignal {
    flag: AtomicBool,
}

#[cfg(not(target_os = "linux"))]
impl AtomicBoolSignal {
    fn new() -> Result<Self, SvmMsgQError> {
        Ok(Self {
            flag: AtomicBool::new(false),
        })
    }

    fn fd(&self) -> i32 {
        -1
    }

    fn signal(&self) {
        self.flag.store(true, Ordering::Release);
    }

    fn read_signal(&self) -> bool {
        self.flag.swap(false, Ordering::AcqRel)
    }
}

#[cfg(target_os = "linux")]
type SignalInner = EventfdSignal;

#[cfg(not(target_os = "linux"))]
type SignalInner = AtomicBoolSignal;

pub struct SvmMsgQ {
    ring: LockFreeRing<SessionEvt>,
    signal_inner: SignalInner,
}

impl SvmMsgQ {
    pub fn with_capacity(msg_count: usize) -> Result<Self, SvmMsgQError> {
        let ring = LockFreeRing::with_capacity(msg_count).map_err(|err| match err {
            RingError::InvalidCapacity => SvmMsgQError::InvalidCapacity,
            RingError::Full(_) => unreachable!("with_capacity does not enqueue"),
        })?;
        let signal_inner = SignalInner::new()?;
        Ok(Self { ring, signal_inner })
    }

    pub fn enqueue(&self, evt: SessionEvt) -> Result<(), SvmMsgQError> {
        self.ring.enqueue(evt).map_err(|err| match err {
            RingError::InvalidCapacity => SvmMsgQError::InvalidCapacity,
            RingError::Full(evt) => SvmMsgQError::Full(evt),
        })?;
        self.signal();
        Ok(())
    }

    pub fn enqueue_batch(&self, evts: &[SessionEvt]) -> usize {
        let count = self.ring.enqueue_batch(evts);
        if count > 0 {
            self.signal();
        }
        count
    }

    #[inline]
    pub fn dequeue(&self) -> Option<SessionEvt> {
        self.ring.dequeue()
    }

    #[inline]
    pub fn dequeue_batch(&self, out: &mut [SessionEvt]) -> usize {
        self.ring.dequeue_batch(out)
    }

    pub fn signal(&self) {
        self.signal_inner.signal();
    }

    pub fn read_signal(&self) -> bool {
        self.signal_inner.read_signal()
    }

    pub fn eventfd(&self) -> i32 {
        self.signal_inner.fd()
    }

    pub fn clear(&self) {
        while self.dequeue().is_some() {}
        while self.read_signal() {}
    }
}

impl fmt::Debug for SvmMsgQ {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SvmMsgQ")
            .field("ring", &self.ring)
            .field("eventfd", &self.eventfd())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evt(session_index: u32, evt_type: SessionEvtType) -> SessionEvt {
        SessionEvt {
            session_index,
            evt_type,
        }
    }

    #[test]
    fn enqueue_dequeue_roundtrip() {
        let q = SvmMsgQ::with_capacity(8).unwrap();
        let sent = evt(42, SessionEvtType::RxEnq);
        q.enqueue(sent).unwrap();
        assert_eq!(q.dequeue(), Some(sent));
        assert_eq!(q.dequeue(), None);
    }

    #[test]
    fn enqueue_batch_one_signal() {
        let q = SvmMsgQ::with_capacity(8).unwrap();
        let batch = [
            evt(1, SessionEvtType::RxEnq),
            evt(2, SessionEvtType::TxDeq),
            evt(3, SessionEvtType::Connect),
        ];
        assert_eq!(q.enqueue_batch(&batch), 3);
        assert_eq!(q.dequeue(), Some(batch[0]));
        assert_eq!(q.dequeue(), Some(batch[1]));
        assert_eq!(q.dequeue(), Some(batch[2]));
        assert_eq!(q.dequeue(), None);
        assert!(q.read_signal());
        assert!(!q.read_signal());
    }

    #[test]
    fn full_returns_evt() {
        // `LockFreeRing::with_capacity(2)` holds one event (one slot reserved).
        let q = SvmMsgQ::with_capacity(2).unwrap();
        assert!(q.enqueue(evt(1, SessionEvtType::RxEnq)).is_ok());
        match q.enqueue(evt(2, SessionEvtType::RxEnq)) {
            Err(SvmMsgQError::Full(returned)) => {
                assert_eq!(returned, evt(2, SessionEvtType::RxEnq));
            }
            other => panic!("expected Full, got {other:?}"),
        }
        match q.enqueue(evt(3, SessionEvtType::RxEnq)) {
            Err(SvmMsgQError::Full(returned)) => {
                assert_eq!(returned, evt(3, SessionEvtType::RxEnq));
            }
            other => panic!("expected Full, got {other:?}"),
        }
    }

    #[test]
    fn dequeue_batch_fills_out() {
        let q = SvmMsgQ::with_capacity(8).unwrap();
        for i in 0..5 {
            q.enqueue(evt(i, SessionEvtType::Close)).unwrap();
        }
        let mut out = [evt(0, SessionEvtType::RxEnq); 8];
        assert_eq!(q.dequeue_batch(&mut out), 5);
        for (i, got) in out[..5].iter().enumerate() {
            assert_eq!(*got, evt(i as u32, SessionEvtType::Close));
        }
        assert_eq!(q.dequeue(), None);
    }

    #[test]
    fn signal_read_signal() {
        let q = SvmMsgQ::with_capacity(4).unwrap();
        assert!(!q.read_signal());
        q.signal();
        assert!(q.read_signal());
        assert!(!q.read_signal());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn eventfd_fd_is_valid() {
        let q = SvmMsgQ::with_capacity(4).unwrap();
        assert!(q.eventfd() >= 0);
    }
}

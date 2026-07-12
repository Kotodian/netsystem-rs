use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use crate::segment::{Local, Segment};

/// App↔session message-queue event aligned with VPP `session_event_t`.
///
/// # Identity rules (VPP)
///
/// - **IO events** (`RxEnq`, `TxDeq`): construct with [`SessionEvt::io`]. Only
///   the session index is significant; worker bits are zero.
/// - **Control events** (`Connect`, `Close`): construct with [`SessionEvt::ctrl`].
///   Identity is the VPP-shaped Session Handle packing
///   `(session_index as u64) | ((worker_index as u64) << 32)`.
///
/// # ABI / shared memory
///
/// Layout is `repr(C)` and 16 bytes: `evt_type`, `postponed`, 2 pad bytes, then
/// a `u64` identity. Local and SVM backends share this layout. Hammer does
/// **not** embed pool generation in the event; after a free slot is reused, a
/// stale index-only IO event may target the replacement session, matching VPP.
///
/// Consume paths should drop events whose session slot is free/unmapped, and
/// drop Close events whose worker index does not match the draining worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SessionEvt {
    pub evt_type: SessionEvtType,
    /// VPP `session_event_t.postponed`; unused by Hammer producers today.
    pub postponed: u8,
    _pad: [u8; 2],
    /// IO events: low 32 bits = session_index, high 32 bits = 0.
    /// Control Close: VPP-shaped Session Handle packing
    /// `(session_index as u64) | ((worker_index as u64) << 32)`.
    identity: u64,
}

impl SessionEvt {
    /// VPP IO event identity: session index only (`SESSION_IO_EVT_*`).
    #[inline]
    pub const fn io(session_index: u32, evt_type: SessionEvtType) -> Self {
        Self {
            evt_type,
            postponed: 0,
            _pad: [0; 2],
            identity: session_index as u64,
        }
    }

    /// VPP control event identity: full session handle
    /// (`SESSION_CTRL_EVT_CLOSE` / reset).
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SessionEvtType {
    RxEnq,
    TxDeq,
    Connect,
    Close,
}

#[repr(C)]
pub struct MsgQueueHeader {
    head: AtomicU32,
    tail: AtomicU32,
    size: u32,
    mask: u32,
}

#[derive(Debug)]
pub enum MsgQueueError {
    InvalidCapacity,
    Full(SessionEvt),
}

#[allow(dead_code)]
pub struct MsgQueue<S: Segment> {
    seg: S,
    base: *mut u8,
    hdr: *mut MsgQueueHeader,
    hdr_off: u64,
    signal_read: Option<RawFd>,
    signal_write: Option<RawFd>,
    signal_atomic: AtomicBool,
}

unsafe impl<S: Segment> Send for MsgQueue<S> {}
unsafe impl<S: Segment> Sync for MsgQueue<S> {}

impl<S: Segment> MsgQueue<S> {
    pub fn new(seg: S, capacity: usize) -> Result<Self, MsgQueueError> {
        if capacity < 2 || !capacity.is_power_of_two() {
            return Err(MsgQueueError::InvalidCapacity);
        }
        let hdr_size = std::mem::size_of::<MsgQueueHeader>();
        let slot_bytes = capacity * std::mem::size_of::<SessionEvt>();
        let hdr_off = seg.alloc(hdr_size + slot_bytes, 8);
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_off as usize) as *mut MsgQueueHeader };
        unsafe {
            std::ptr::write(
                hdr,
                MsgQueueHeader {
                    head: AtomicU32::new(0),
                    tail: AtomicU32::new(0),
                    size: capacity as u32,
                    mask: (capacity - 1) as u32,
                },
            );
        }
        Ok(Self {
            seg,
            base,
            hdr,
            hdr_off,
            signal_read: None,
            signal_write: None,
            signal_atomic: AtomicBool::new(false),
        })
    }

    /// Initialise a [`MsgQueue`] header at a pre-allocated offset in `seg`.
    /// The caller must guarantee that `seg` has `sizeof(MsgQueueHeader) +
    /// capacity * sizeof(SessionEvt)` bytes available at `hdr_offset`.
    /// Signal fds must be set up separately (e.g. via `from_shared` for the
    /// remote side, or by wrapping the returned queue).
    pub unsafe fn init_at(seg: S, hdr_offset: u64, capacity: usize) -> Result<Self, MsgQueueError> {
        if capacity < 2 || !capacity.is_power_of_two() {
            return Err(MsgQueueError::InvalidCapacity);
        }
        let base = seg.base();
        let hdr = unsafe { base.add(hdr_offset as usize) as *mut MsgQueueHeader };
        unsafe {
            std::ptr::write(
                hdr,
                MsgQueueHeader {
                    head: AtomicU32::new(0),
                    tail: AtomicU32::new(0),
                    size: capacity as u32,
                    mask: (capacity - 1) as u32,
                },
            );
        }
        Ok(Self {
            seg,
            base,
            hdr,
            hdr_off: hdr_offset,
            signal_read: None,
            signal_write: None,
            signal_atomic: AtomicBool::new(false),
        })
    }

    pub unsafe fn from_shared(
        seg: S,
        offset: u64,
        signal_read: Option<RawFd>,
        signal_write: Option<RawFd>,
    ) -> Self {
        let base = seg.base();
        Self {
            seg,
            base,
            hdr: unsafe { base.add(offset as usize) as *mut MsgQueueHeader },
            hdr_off: offset,
            signal_read,
            signal_write,
            signal_atomic: AtomicBool::new(false),
        }
    }

    #[inline]
    unsafe fn slot_ptr(&self, index: u32) -> *mut SessionEvt {
        let slot_off = self.hdr_off as usize
            + std::mem::size_of::<MsgQueueHeader>()
            + (index as usize) * std::mem::size_of::<SessionEvt>();
        unsafe { self.base.add(slot_off) as *mut SessionEvt }
    }

    pub fn enqueue(&self, evt: SessionEvt) -> Result<(), MsgQueueError> {
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Relaxed) };
        let head = unsafe { (*self.hdr).head.load(Ordering::Acquire) };
        let free = unsafe { (*self.hdr).mask.wrapping_add(head).wrapping_sub(tail) };
        if free == 0 {
            return Err(MsgQueueError::Full(evt));
        }
        let slot = tail & unsafe { (*self.hdr).mask };
        unsafe {
            std::ptr::write(self.slot_ptr(slot), evt);
        }
        unsafe {
            (*self.hdr)
                .tail
                .store(tail.wrapping_add(1), Ordering::Release);
        }
        self.fire();
        Ok(())
    }

    pub fn enqueue_batch(&self, evts: &[SessionEvt]) -> usize {
        let mut count = 0;
        for evt in evts {
            if self.enqueue(*evt).is_ok() {
                count += 1;
            } else {
                break;
            }
        }
        count
    }

    pub fn dequeue(&self) -> Option<SessionEvt> {
        let head = unsafe { (*self.hdr).head.load(Ordering::Relaxed) };
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
        if head == tail {
            return None;
        }
        let slot = head & unsafe { (*self.hdr).mask };
        let evt = unsafe { std::ptr::read(self.slot_ptr(slot)) };
        unsafe {
            (*self.hdr)
                .head
                .store(head.wrapping_add(1), Ordering::Release);
        }
        Some(evt)
    }

    pub fn dequeue_batch(&self, out: &mut [SessionEvt]) -> usize {
        let mut count = 0;
        for slot in out.iter_mut() {
            if let Some(evt) = self.dequeue() {
                *slot = evt;
                count += 1;
            } else {
                break;
            }
        }
        count
    }

    pub fn fire(&self) {
        if let Some(fd) = self.signal_write {
            let val: [u8; 1] = [1];
            let ret = unsafe { libc::write(fd, val.as_ptr() as *const libc::c_void, 1) };
            if ret < 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EAGAIN) {
                    panic!("msgq signal write failed: {err}");
                }
            }
        } else {
            self.signal_atomic.store(true, Ordering::Release);
        }
    }

    pub fn drain(&self) -> bool {
        if let Some(fd) = self.signal_read {
            let mut buf = [0u8; 64];
            let ret = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            ret > 0
        } else {
            self.signal_atomic.swap(false, Ordering::AcqRel)
        }
    }

    pub fn is_empty(&self) -> bool {
        // SAFETY: `self.hdr` points to a valid `MsgQueueHeader` in shared
        // memory for the lifetime of `self`, guaranteed by `Segment::map`.
        let head = unsafe { (*self.hdr).head.load(Ordering::Acquire) };
        // SAFETY: Same invariant as above; `head` and `tail` share the same
        // header allocation and are both valid for the lifetime of `self`.
        let tail = unsafe { (*self.hdr).tail.load(Ordering::Acquire) };
        head == tail
    }

    pub fn read_fd(&self) -> Option<RawFd> {
        self.signal_read
    }

    pub fn write_fd(&self) -> Option<RawFd> {
        self.signal_write
    }

    /// Offset of the [`MsgQueueHeader`] within the backing [`Segment`].
    #[inline]
    pub fn hdr_offset(&self) -> u64 {
        self.hdr_off
    }

    pub fn read_signal(&self) -> bool {
        self.drain()
    }

    pub fn clear(&self) {
        while self.dequeue().is_some() {}
        if let Some(fd) = self.signal_read {
            let mut buf = [0u8; 64];
            while unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) } > 0 {}
        }
        self.signal_atomic.store(false, Ordering::Relaxed);
    }
}

impl MsgQueue<Local> {
    pub fn with_capacity(capacity: usize) -> Result<Self, MsgQueueError> {
        let seg = Local::new(size_of::<MsgQueueHeader>() + capacity * size_of::<SessionEvt>() + 64);
        Self::new(seg, capacity)
    }
}

impl<S: Segment> Drop for MsgQueue<S> {
    fn drop(&mut self) {
        if let Some(fd) = self.signal_read {
            unsafe {
                libc::close(fd);
            }
        }
        if let Some(fd) = self.signal_write {
            unsafe {
                libc::close(fd);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::Local;
    use SessionEvtType::{Connect, RxEnq, TxDeq};

    fn evt(i: u32, t: SessionEvtType) -> SessionEvt {
        SessionEvt::io(i, t)
    }

    fn test_signal_pipe() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        for fd in fds {
            let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
            assert!(flags >= 0);
            assert_eq!(
                unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) },
                0
            );
            let fdflags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
            assert!(fdflags >= 0);
            assert_eq!(
                unsafe { libc::fcntl(fd, libc::F_SETFD, fdflags | libc::FD_CLOEXEC) },
                0
            );
        }
        (fds[0], fds[1])
    }

    #[test]
    fn header_layout() {
        use std::mem::{align_of, size_of};
        assert_eq!(align_of::<MsgQueueHeader>(), 4);
        assert_eq!(size_of::<MsgQueueHeader>(), 16);
    }

    #[test]
    fn io_event_carries_session_index_only() {
        let evt = SessionEvt::io(42, TxDeq);
        assert_eq!(evt.evt_type, TxDeq);
        assert_eq!(evt.session_index(), 42);
        assert_eq!(evt.worker_index(), 0);
    }

    #[test]
    fn close_event_carries_full_session_handle() {
        let evt = SessionEvt::ctrl(7, 3, SessionEvtType::Close);
        assert_eq!(evt.evt_type, SessionEvtType::Close);
        assert_eq!(evt.session_index(), 7);
        assert_eq!(evt.worker_index(), 3);
    }

    #[test]
    fn session_evt_layout_matches_vpp_handle_packing() {
        use std::mem::size_of;
        // event_type + postponed + pad + u64 identity (VPP session_event_t shape)
        assert_eq!(size_of::<SessionEvt>(), 16);
        let evt = SessionEvt::ctrl(0x1111_2222, 0x3333_4444, SessionEvtType::Close);
        assert_eq!(
            evt.session_handle_raw(),
            (0x1111_2222u64) | ((0x3333_4444u64) << 32)
        );
    }

    #[test]
    fn enqueue_dequeue_roundtrip_in_process() {
        let seg = Local::new(4096);
        let q = MsgQueue::<Local>::new(seg, 8).expect("msgq");
        let sent = evt(42, SessionEvtType::RxEnq);
        q.enqueue(sent).expect("enqueue");
        assert_eq!(q.dequeue(), Some(sent));
        assert_eq!(q.dequeue(), None);
    }

    #[test]
    fn enqueue_batch_fires_once() {
        let seg = Local::new(4096);
        let q = MsgQueue::<Local>::new(seg, 8).expect("msgq");
        let batch = [evt(1, RxEnq), evt(2, TxDeq), evt(3, Connect)];
        assert_eq!(q.enqueue_batch(&batch), 3);
        assert_eq!(q.dequeue(), Some(batch[0]));
        assert_eq!(q.dequeue(), Some(batch[1]));
        assert_eq!(q.dequeue(), Some(batch[2]));
    }

    #[test]
    fn full_returns_evt() {
        let seg = Local::new(4096);
        let q = MsgQueue::<Local>::new(seg, 2).expect("msgq");
        assert!(q.enqueue(evt(1, RxEnq)).is_ok());
        assert!(q.enqueue(evt(2, RxEnq)).is_err());
    }

    #[test]
    fn in_process_signal_has_no_fd() {
        let seg = Local::new(4096);
        let q = MsgQueue::<Local>::new(seg, 4).expect("msgq");
        assert!(q.read_fd().is_none());
        assert!(!q.drain());
        q.fire();
        assert!(q.drain());
        assert!(!q.drain());
    }

    #[test]
    fn cross_process_signal_has_fd() {
        let seg = Local::new(4096);
        unsafe {
            MsgQueue::<Local>::init_at(seg.clone(), 0, 4).expect("msgq init");
        }
        let (read_fd, write_fd) = test_signal_pipe();
        let q = unsafe { MsgQueue::<Local>::from_shared(seg, 0, Some(read_fd), Some(write_fd)) };
        assert_eq!(q.read_fd(), Some(read_fd));
        assert_eq!(q.write_fd(), Some(write_fd));
        assert!(!q.drain());
        q.fire();
        assert!(q.drain());
        assert!(!q.drain());
    }

    #[test]
    fn cross_process_signal_wakes_thread() {
        let seg = Local::new(4096);
        unsafe {
            MsgQueue::<Local>::init_at(seg.clone(), 0, 4).expect("msgq init");
        }
        let (read_fd, write_fd) = test_signal_pipe();
        let q = std::sync::Arc::new(unsafe {
            MsgQueue::<Local>::from_shared(seg, 0, Some(read_fd), Some(write_fd))
        });
        let wq = std::sync::Arc::clone(&q);
        let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dc = std::sync::Arc::clone(&done);
        let h = std::thread::spawn(move || {
            while !dc.load(Ordering::Acquire) {
                if wq.drain() {
                    dc.store(true, Ordering::Release);
                }
            }
        });
        std::thread::sleep(std::time::Duration::from_millis(10));
        q.fire();
        h.join().unwrap();
        assert!(done.load(Ordering::Acquire));
    }
}

//! Apple-specific utun device that uses the private `recvmsg_x` / `sendmsg_x`
//! syscalls to amortize per-packet syscall overhead.
//!
//! The Go reference implementation (`tun_ios.go::BatchRead`/`BatchWrite`) uses
//! the same pattern via `golang.org/x/sys/unix`. Our `tun_rs::AsyncDevice`
//! based wrapper falls back to one `recv`/`send` syscall per IP packet, which
//! becomes a meaningful CPU bottleneck under sustained upload — every TCP
//! segment writes back through the system stack: client → utun (recv) → NAT
//! rewrite → utun (send) → kernel routes to listener → outbound stream.
//!
//! This module is compiled only on Apple targets; non-Apple builds keep using
//! `AsyncTunDevice` via the `tun_rs` crate.
#![cfg(any(target_os = "macos", target_os = "ios", target_os = "tvos"))]

use std::collections::VecDeque;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use libc::{c_int, c_long, c_void, iovec, size_t, socklen_t};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

use hammer_core::error::HammerError;

use crate::tun::TunDevice;

/// macOS/iOS private syscall numbers (`<sys/syscall.h>`).
const SYS_RECVMSG_X: c_long = 480;
const SYS_SENDMSG_X: c_long = 481;

/// utun prepends a 4-byte protocol family on each IP packet.
const PACKET_HEADER_LEN: usize = 4;

/// Pre-encoded protocol family headers (network byte order).
const PF_INET_HEADER: [u8; 4] = [0, 0, 0, libc::AF_INET as u8];
const PF_INET6_HEADER: [u8; 4] = [0, 0, 0, libc::AF_INET6 as u8];

/// Ask the kernel to coalesce up to this many packets per recvmsg_x — matches
/// the Go reference implementation's `batchSize := (512KiB / mtu) + 1`.
const BATCH_TARGET_BYTES: usize = 512 * 1024;

/// `setsockopt` option to hint the kernel about our batch size. Mirrors the
/// `UTUN_OPT_MAX_PENDING_PACKETS` constant exposed in `<net/if_utun.h>` —
/// libc does not currently re-export it.
const UTUN_OPT_MAX_PENDING_PACKETS: c_int = 13;

/// Equivalent of macOS `struct msghdr_x` from `<sys/socket.h>`.
///
/// Identical to `libc::msghdr` followed by an extra `size_t msg_datalen`
/// field that the kernel populates with the total bytes received per slot.
#[repr(C)]
#[derive(Clone, Copy)]
struct MsgHdrX {
    msg_name: *mut c_void,
    msg_namelen: socklen_t,
    msg_iov: *mut iovec,
    msg_iovlen: c_int,
    msg_control: *mut c_void,
    msg_controllen: socklen_t,
    msg_flags: c_int,
    msg_datalen: size_t,
}

unsafe impl Send for MsgHdrX {}

fn empty_msghdr_x() -> MsgHdrX {
    MsgHdrX {
        msg_name: std::ptr::null_mut(),
        msg_namelen: 0,
        msg_iov: std::ptr::null_mut(),
        msg_iovlen: 0,
        msg_control: std::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
        msg_datalen: 0,
    }
}

fn empty_iovec() -> iovec {
    iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    }
}

pub struct AppleTunDevice {
    fd: AsyncFd<OwnedFd>,
    mtu: usize,
    batch_size: usize,
    closed: AtomicBool,
    rx: Mutex<RxState>,
    tx: Mutex<TxState>,
}

struct RxState {
    /// Each slot owns a 4-byte AF prefix buffer that the kernel writes into.
    headers: Vec<[u8; PACKET_HEADER_LEN]>,
    /// Each slot owns an MTU-sized payload buffer.
    payloads: Vec<Vec<u8>>,
    /// Two iovecs per slot: header + payload.
    iovecs: Vec<[iovec; 2]>,
    msgs: Vec<MsgHdrX>,
    /// Packets ready to hand to `recv()` callers.
    pending: VecDeque<Vec<u8>>,
}

impl RxState {
    fn new(batch_size: usize, mtu: usize) -> Self {
        Self {
            headers: vec![[0u8; PACKET_HEADER_LEN]; batch_size],
            payloads: (0..batch_size).map(|_| vec![0u8; mtu]).collect(),
            iovecs: vec![[empty_iovec(); 2]; batch_size],
            msgs: vec![empty_msghdr_x(); batch_size],
            pending: VecDeque::new(),
        }
    }
}

struct TxState {
    /// Reusable AF prefix scratch (one slot only — sends are still 1:1 here;
    /// batching the write side adds latency without helping throughput when
    /// each packet has already been freshly NAT-rewritten).
    header: [u8; PACKET_HEADER_LEN],
    iov: [iovec; 2],
    msg: MsgHdrX,
}

impl TxState {
    fn new() -> Self {
        Self {
            header: [0u8; PACKET_HEADER_LEN],
            iov: [empty_iovec(); 2],
            msg: empty_msghdr_x(),
        }
    }
}

impl AppleTunDevice {
    /// # Safety
    ///
    /// `fd` must be an exclusively-owned utun file descriptor; this device
    /// closes it on drop.
    pub unsafe fn from_fd(fd: RawFd, mtu: usize) -> Result<Arc<Self>, HammerError> {
        if mtu == 0 {
            return Err(HammerError::internal("apple TUN MTU must be > 0"));
        }
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };

        // The kernel hands us a blocking fd; AsyncFd assumes nonblocking.
        let raw = owned.as_raw_fd();
        let flags = unsafe { libc::fcntl(raw, libc::F_GETFL, 0) };
        if flags == -1 {
            return Err(HammerError::internal(format!(
                "fcntl F_GETFL: {}",
                io::Error::last_os_error()
            )));
        }
        if flags & libc::O_NONBLOCK == 0
            && unsafe { libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
        {
            return Err(HammerError::internal(format!(
                "fcntl F_SETFL O_NONBLOCK: {}",
                io::Error::last_os_error()
            )));
        }

        let batch_size = (BATCH_TARGET_BYTES / mtu).max(1);

        // Best-effort hint to the kernel — older iOS versions do not expose
        // this opt and return EINVAL; that is fine.
        let batch_int = batch_size as c_int;
        unsafe {
            libc::setsockopt(
                raw,
                libc::SYSPROTO_CONTROL,
                UTUN_OPT_MAX_PENDING_PACKETS,
                (&batch_int as *const c_int).cast::<c_void>(),
                std::mem::size_of::<c_int>() as socklen_t,
            );
        }

        let async_fd = AsyncFd::with_interest(owned, Interest::READABLE | Interest::WRITABLE)
            .map_err(|err| HammerError::internal(format!("register apple utun fd: {err}")))?;

        Ok(Arc::new(Self {
            fd: async_fd,
            mtu,
            batch_size,
            closed: AtomicBool::new(false),
            rx: Mutex::new(RxState::new(batch_size, mtu)),
            tx: Mutex::new(TxState::new()),
        }))
    }

    fn refill_rx(&self, state: &mut RxState) -> io::Result<usize> {
        // Re-bind iovec pointers each time — the underlying buffer addresses
        // are stable for the device's lifetime, but the kernel mutates these
        // between calls so we keep the bookkeeping idempotent.
        for i in 0..self.batch_size {
            state.iovecs[i][0] = iovec {
                iov_base: state.headers[i].as_mut_ptr().cast::<c_void>(),
                iov_len: PACKET_HEADER_LEN,
            };
            state.iovecs[i][1] = iovec {
                iov_base: state.payloads[i].as_mut_ptr().cast::<c_void>(),
                iov_len: self.mtu,
            };
            state.msgs[i] = MsgHdrX {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: state.iovecs[i].as_mut_ptr(),
                msg_iovlen: 2,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
                msg_datalen: 0,
            };
        }
        let n = unsafe {
            libc::syscall(
                SYS_RECVMSG_X,
                self.fd.get_ref().as_raw_fd(),
                state.msgs.as_mut_ptr(),
                self.batch_size as c_int,
                libc::MSG_DONTWAIT,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let n = n as usize;
        for i in 0..n {
            let total = state.msgs[i].msg_datalen;
            if total <= PACKET_HEADER_LEN {
                continue;
            }
            let payload_len = total - PACKET_HEADER_LEN;
            if payload_len > self.mtu {
                continue;
            }
            let mut packet = Vec::with_capacity(payload_len);
            packet.extend_from_slice(&state.payloads[i][..payload_len]);
            state.pending.push_back(packet);
        }
        Ok(n)
    }

    fn try_send(&self, state: &mut TxState, packet: &[u8]) -> io::Result<()> {
        let header = match packet.first().map(|b| b >> 4) {
            Some(4) => PF_INET_HEADER,
            Some(6) => PF_INET6_HEADER,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unsupported IP version for utun write",
                ));
            }
        };
        state.header = header;
        state.iov[0] = iovec {
            iov_base: state.header.as_mut_ptr().cast::<c_void>(),
            iov_len: PACKET_HEADER_LEN,
        };
        // sendmsg_x reads from this buffer; the cast through *mut c_void is
        // required by libc::iovec, but the kernel will not mutate it.
        state.iov[1] = iovec {
            iov_base: packet.as_ptr() as *mut c_void,
            iov_len: packet.len(),
        };
        state.msg = MsgHdrX {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: state.iov.as_mut_ptr(),
            msg_iovlen: 2,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
            msg_datalen: 0,
        };
        let n = unsafe {
            libc::syscall(
                SYS_SENDMSG_X,
                self.fd.get_ref().as_raw_fd(),
                (&mut state.msg as *mut MsgHdrX).cast::<c_void>(),
                1_c_int,
                libc::MSG_DONTWAIT,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[async_trait]
impl TunDevice for AppleTunDevice {
    async fn recv(&self) -> Result<Vec<u8>, HammerError> {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(HammerError::internal("apple TUN closed"));
            }
            // Drain the prefetch queue first — keeps recv() amortized.
            {
                let mut state = self.rx.lock().expect("apple TUN rx poisoned");
                if let Some(pkt) = state.pending.pop_front() {
                    return Ok(pkt);
                }
            }
            let mut guard = self
                .fd
                .readable()
                .await
                .map_err(|err| HammerError::internal(format!("apple TUN readable: {err}")))?;
            let result = {
                let mut state = self.rx.lock().expect("apple TUN rx poisoned");
                self.refill_rx(&mut state)
            };
            match result {
                Ok(_) => {
                    // The previous refill drained the kernel queue — clear
                    // readiness so the next loop iteration re-arms the wait.
                    let still_pending = self
                        .rx
                        .lock()
                        .expect("apple TUN rx poisoned")
                        .pending
                        .is_empty();
                    if still_pending {
                        guard.clear_ready();
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    guard.clear_ready();
                }
                Err(err) => {
                    return Err(HammerError::internal(format!("recvmsg_x: {err}")));
                }
            }
        }
    }

    async fn send(&self, packet: Vec<u8>) -> Result<(), HammerError> {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(HammerError::internal("apple TUN closed"));
            }
            let mut guard = self
                .fd
                .writable()
                .await
                .map_err(|err| HammerError::internal(format!("apple TUN writable: {err}")))?;
            let result = {
                let mut state = self.tx.lock().expect("apple TUN tx poisoned");
                self.try_send(&mut state, &packet)
            };
            match result {
                Ok(()) => return Ok(()),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    guard.clear_ready();
                }
                Err(err) => {
                    return Err(HammerError::internal(format!("sendmsg_x: {err}")));
                }
            }
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

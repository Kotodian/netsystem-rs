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
use std::time::Duration;

use async_trait::async_trait;
use libc::{c_int, c_void, iovec, size_t, socklen_t};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;
use tracing::debug;

use hammer_core::error::HammerError;

use crate::protocol::tun::TunDevice;
use crate::protocol::tun::stack::{
    is_transient_tun_send_backpressure, should_clear_tun_send_readiness,
};

/// macOS/iOS private syscall numbers (`<sys/syscall.h>`). On Apple targets
/// `libc::syscall` is variadic with the first arg typed as `c_int`, so these
/// must be `c_int` rather than the `c_long` you see on Linux.
const SYS_RECVMSG_X: c_int = 480;
const SYS_SENDMSG_X: c_int = 481;

/// utun prepends a 4-byte protocol family on each IP packet.
const PACKET_HEADER_LEN: usize = 4;

/// Pre-encoded protocol family headers (network byte order).
const PF_INET_HEADER: [u8; 4] = [0, 0, 0, libc::AF_INET as u8];
const PF_INET6_HEADER: [u8; 4] = [0, 0, 0, libc::AF_INET6 as u8];

/// Ask the kernel to coalesce up to this many packets per recvmsg_x — matches
/// the Go reference implementation's `batchSize := (512KiB / mtu) + 1`.
const BATCH_TARGET_BYTES: usize = 512 * 1024;
const SEND_BACKPRESSURE_RETRY_LIMIT: usize = 64;
const SEND_BACKPRESSURE_RETRY_DELAY: Duration = Duration::from_millis(2);

/// `setsockopt` option to hint the kernel about our batch size. Mirrors the
/// `UTUN_OPT_MAX_PENDING_PACKETS` constant exposed in `<net/if_utun.h>` —
/// libc does not currently re-export it. The Go reference build uses the
/// same value (`tun_ios.go::configure`).
const UTUN_OPT_MAX_PENDING_PACKETS: c_int = 16;

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
    /// Free MTU-sized Vec<u8> buffers shared between recv and send. recvmsg_x
    /// writes packet payload directly into a pool buffer (zero-copy), the
    /// rewritten packet flows through packet_loop and back into send_batch,
    /// and after sendmsg_x the buffer is recycled here for the next refill.
    pool: Mutex<Vec<Vec<u8>>>,
}

struct RxState {
    /// Each slot owns a 4-byte AF prefix buffer that the kernel writes into.
    headers: Vec<[u8; PACKET_HEADER_LEN]>,
    /// Two iovecs per slot: header (stable, owned by this state) + payload
    /// (rewritten per refill_rx call to point at a pool-borrowed buffer).
    iovecs: Vec<[iovec; 2]>,
    msgs: Vec<MsgHdrX>,
    /// Packets ready to hand to `recv()` / `recv_batch()` callers. Each Vec
    /// here is a former pool buffer with its length truncated to the actual
    /// payload size.
    pending: VecDeque<Vec<u8>>,
}

// SAFETY: the iovec / MsgHdrX raw pointers we store reference buffers owned
// by this same struct or by Mutex-protected pool entries. Access is
// serialized through the parent Mutex, so moving the struct between threads
// is sound.
unsafe impl Send for RxState {}
unsafe impl Send for TxState {}

impl RxState {
    fn new(batch_size: usize) -> Self {
        let mut state = Self {
            headers: vec![[0u8; PACKET_HEADER_LEN]; batch_size],
            iovecs: vec![[empty_iovec(); 2]; batch_size],
            msgs: vec![empty_msghdr_x(); batch_size],
            pending: VecDeque::new(),
        };
        // Header iovec, msg_iov, msg_iovlen are stable across calls — set
        // them up once. The payload iovec[1] is rebound per refill_rx to a
        // fresh pool-borrowed buffer.
        for i in 0..batch_size {
            state.iovecs[i][0] = iovec {
                iov_base: state.headers[i].as_mut_ptr().cast::<c_void>(),
                iov_len: PACKET_HEADER_LEN,
            };
            state.msgs[i].msg_iov = state.iovecs[i].as_mut_ptr();
            state.msgs[i].msg_iovlen = 2;
        }
        state
    }
}

struct TxState {
    /// One AF prefix per slot (rewritten per call to match the IP family).
    headers: Vec<[u8; PACKET_HEADER_LEN]>,
    /// Two iovecs per slot: header + caller-supplied packet payload.
    iovecs: Vec<[iovec; 2]>,
    msgs: Vec<MsgHdrX>,
}

impl TxState {
    fn new(batch_size: usize) -> Self {
        let mut state = Self {
            headers: vec![[0u8; PACKET_HEADER_LEN]; batch_size],
            iovecs: vec![[empty_iovec(); 2]; batch_size],
            msgs: vec![empty_msghdr_x(); batch_size],
        };
        // Header iovec slots point at our owned `headers[i]` for the lifetime
        // of TxState; the payload iovec is rewritten per call. msg_iov /
        // msg_iovlen are stable so we set them up once.
        for i in 0..batch_size {
            state.iovecs[i][0] = iovec {
                iov_base: state.headers[i].as_mut_ptr().cast::<c_void>(),
                iov_len: PACKET_HEADER_LEN,
            };
            state.msgs[i].msg_iov = state.iovecs[i].as_mut_ptr();
            state.msgs[i].msg_iovlen = 2;
        }
        state
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

        // Pre-warm the buffer pool with `batch_size` MTU-sized Vecs so the
        // first refill_rx doesn't have to allocate.
        let mut pool: Vec<Vec<u8>> = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            pool.push(Vec::with_capacity(mtu));
        }
        Ok(Arc::new(Self {
            fd: async_fd,
            mtu,
            batch_size,
            closed: AtomicBool::new(false),
            rx: Mutex::new(RxState::new(batch_size)),
            tx: Mutex::new(TxState::new(batch_size)),
            pool: Mutex::new(pool),
        }))
    }

    fn refill_rx(&self, state: &mut RxState) -> io::Result<usize> {
        // Borrow batch_size buffers from the recycle pool (or allocate if
        // empty). recvmsg_x writes packet payload directly into these — no
        // intermediate fixed buffer, no extra copy.
        let mut bufs: Vec<Vec<u8>> = self.take_pool_buffers(self.batch_size);
        for (i, buf) in bufs.iter_mut().enumerate() {
            if buf.capacity() < self.mtu {
                buf.reserve_exact(self.mtu - buf.capacity());
            }
            // SAFETY: capacity is at least mtu; the next syscall will
            // overwrite the bytes we expose here, and on success we truncate
            // to the actual payload length. Until then the slice content is
            // logically uninitialized but readable as u8.
            unsafe {
                buf.set_len(self.mtu);
            }
            state.iovecs[i][1] = iovec {
                iov_base: buf.as_mut_ptr().cast::<c_void>(),
                iov_len: self.mtu,
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
            // Push every borrowed buffer back to the pool unchanged.
            for buf in bufs.iter_mut() {
                unsafe {
                    buf.set_len(0);
                }
            }
            self.return_pool_buffers(bufs);
            return Err(io::Error::last_os_error());
        }
        let n = n as usize;

        // First n slots got data; truncate to actual length and push to
        // pending. Remaining slots — and any with invalid sizes — go back
        // to the pool.
        let mut to_pool: Vec<Vec<u8>> = Vec::with_capacity(self.batch_size);
        for (i, mut buf) in bufs.into_iter().enumerate() {
            if i < n {
                let total = state.msgs[i].msg_datalen;
                if total > PACKET_HEADER_LEN && total - PACKET_HEADER_LEN <= self.mtu {
                    let payload_len = total - PACKET_HEADER_LEN;
                    buf.truncate(payload_len);
                    state.pending.push_back(buf);
                    continue;
                }
            }
            unsafe {
                buf.set_len(0);
            }
            to_pool.push(buf);
        }
        if !to_pool.is_empty() {
            self.return_pool_buffers(to_pool);
        }
        Ok(n)
    }

    fn take_pool_buffers(&self, count: usize) -> Vec<Vec<u8>> {
        let mut bufs = {
            let mut pool = self.pool.lock().expect("apple TUN pool poisoned");
            let take = count.min(pool.len());
            let start = pool.len() - take;
            pool.drain(start..).collect::<Vec<_>>()
        };
        while bufs.len() < count {
            bufs.push(Vec::with_capacity(self.mtu));
        }
        bufs
    }

    fn return_pool_buffers(&self, mut bufs: Vec<Vec<u8>>) {
        let mut pool = self.pool.lock().expect("apple TUN pool poisoned");
        for mut buf in bufs.drain(..) {
            if buf.capacity() >= self.mtu {
                unsafe {
                    buf.set_len(0);
                }
                pool.push(buf);
            }
        }
    }

    /// Issue one `sendmsg_x` for `chunk_len` packets starting at index 0 of
    /// `state`. Returns the number of mmsghdrs the kernel actually consumed
    /// (may be < chunk_len on partial sends).
    fn try_send_chunk(&self, state: &mut TxState, chunk_len: usize) -> io::Result<usize> {
        if chunk_len == 0 {
            return Ok(0);
        }
        let n = unsafe {
            libc::syscall(
                SYS_SENDMSG_X,
                self.fd.get_ref().as_raw_fd(),
                state.msgs.as_mut_ptr(),
                chunk_len as c_int,
                libc::MSG_DONTWAIT,
            )
        };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(n as usize)
    }

    /// Populate `chunk_len` slots in `state` from `packets[start..]`. Skips
    /// packets whose IP version we cannot identify, returning how many were
    /// actually staged.
    fn stage_send_chunk(
        state: &mut TxState,
        packets: &[Vec<u8>],
        start: usize,
        max_packets: usize,
    ) -> usize {
        let mut staged = 0;
        let limit = state.headers.len().min(max_packets.max(1));
        for packet in packets.iter().skip(start) {
            if staged == limit {
                break;
            }
            let header = match packet.first().map(|b| b >> 4) {
                Some(4) => PF_INET_HEADER,
                Some(6) => PF_INET6_HEADER,
                _ => continue,
            };
            state.headers[staged] = header;
            state.iovecs[staged][1] = iovec {
                iov_base: packet.as_ptr() as *mut c_void,
                iov_len: packet.len(),
            };
            staged += 1;
        }
        staged
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
        self.send_batch(vec![packet]).await
    }

    async fn recv_batch(&self, _max: usize) -> Result<Vec<Vec<u8>>, HammerError> {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Err(HammerError::internal("apple TUN closed"));
            }
            // Drain whatever the previous syscall buffered.
            {
                let mut state = self.rx.lock().expect("apple TUN rx poisoned");
                if !state.pending.is_empty() {
                    return Ok(state.pending.drain(..).collect());
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
                    let mut state = self.rx.lock().expect("apple TUN rx poisoned");
                    if state.pending.is_empty() {
                        guard.clear_ready();
                    } else {
                        return Ok(state.pending.drain(..).collect());
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

    async fn send_batch(&self, packets: Vec<Vec<u8>>) -> Result<(), HammerError> {
        if packets.is_empty() {
            return Ok(());
        }
        // Filter out anything we can't tag with an IP family up front so the
        // chunk loop below can skip past completed slots without skipping
        // around invalid entries.
        let valid: Vec<Vec<u8>> = packets
            .into_iter()
            .filter(|p| matches!(p.first().map(|b| b >> 4), Some(4) | Some(6)))
            .collect();
        if valid.is_empty() {
            return Ok(());
        }

        let mut offset = 0;
        let mut max_chunk = usize::MAX;
        let mut backpressure_retries = 0;
        let send_result = loop {
            if offset >= valid.len() {
                break Ok::<(), HammerError>(());
            }
            if self.closed.load(Ordering::SeqCst) {
                break Err(HammerError::internal("apple TUN closed"));
            }
            let mut guard = match self.fd.writable().await {
                Ok(guard) => guard,
                Err(err) => {
                    break Err(HammerError::internal(format!("apple TUN writable: {err}")));
                }
            };
            let result = {
                let mut state = self.tx.lock().expect("apple TUN tx poisoned");
                let staged = Self::stage_send_chunk(&mut state, &valid, offset, max_chunk);
                self.try_send_chunk(&mut state, staged)
                    .map(|n| (n, staged))
                    .map_err(|err| (err, staged))
            };
            match result {
                Ok((0, _)) => {
                    break Err(HammerError::internal("sendmsg_x wrote zero packets"));
                }
                Ok((n_sent, staged)) => {
                    offset += n_sent;
                    if n_sent > 0 {
                        backpressure_retries = 0;
                        if n_sent == staged {
                            max_chunk = usize::MAX;
                        }
                    }
                }
                Err((err, _)) if should_clear_tun_send_readiness(&err) => {
                    guard.clear_ready();
                }
                // utun is a kernel control socket; under burst the kctl pipe
                // queue fills and `sendmsg_x` returns ENOBUFS or ENOSPC.
                // Both are transient backpressure, but they are not readiness
                // loss signals. Clearing AsyncFd readiness here can park this
                // packet loop waiting for an edge that never arrives.
                Err((err, staged)) if is_transient_tun_send_backpressure(&err) => {
                    drop(guard);
                    backpressure_retries += 1;
                    max_chunk = if max_chunk == usize::MAX {
                        (staged / 2).max(1)
                    } else {
                        (max_chunk / 2).max(1)
                    };
                    if backpressure_retries >= SEND_BACKPRESSURE_RETRY_LIMIT {
                        break Err(HammerError::internal(format!(
                            "apple TUN send backpressure exhausted after {backpressure_retries} retries: {err}; dropped {} packet(s)",
                            valid.len() - offset
                        )));
                    }
                    if backpressure_retries == 1 {
                        debug!("apple TUN send backpressure: {err}; backing off");
                    }
                    tokio::time::sleep(SEND_BACKPRESSURE_RETRY_DELAY).await;
                }
                Err((err, _)) => {
                    break Err(HammerError::internal(format!("sendmsg_x: {err}")));
                }
            }
        };
        // Recycle every buffer we accepted into `valid` regardless of send
        // outcome — the data is already gone from the kernel's perspective
        // for the slots that succeeded, and partial-write retries above
        // covered the rest. Returning the allocations to the pool lets the
        // next refill_rx reuse them instead of allocating fresh ones.
        self.return_pool_buffers(valid);
        send_result
    }

    fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

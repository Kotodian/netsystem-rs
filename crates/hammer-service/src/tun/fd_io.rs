use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::task::{Context, Poll};

use hammer_core::error::{CoreError, CoreResult};
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

const APPLE_UTUN_HEADER_LEN: usize = 4;
const MAX_WRITEV_IOVEC: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunFdPacketFormat {
    RawIp,
    AppleUtun,
}

impl TunFdPacketFormat {
    #[inline]
    pub const fn platform_default() -> Self {
        if cfg!(any(
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos"
        )) {
            Self::AppleUtun
        } else {
            Self::RawIp
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunFdSendResult {
    Complete,
    Partial(usize),
    Backpressure,
}

#[derive(Clone)]
pub struct TunFdIo {
    inner: Arc<TunFdIoInner>,
}

struct TunFdIoInner {
    fd: AsyncFd<OwnedFd>,
    mtu: usize,
    format: TunFdPacketFormat,
}

impl TunFdIo {
    /// # Safety
    ///
    /// `fd` must be an exclusively-owned TUN/utun file descriptor. The
    /// returned `TunFdIo` closes it on drop.
    #[inline]
    pub unsafe fn from_fd(fd: RawFd, mtu: usize) -> CoreResult<Self> {
        unsafe { Self::from_fd_with_format(fd, mtu, TunFdPacketFormat::platform_default()) }
    }

    /// # Safety
    ///
    /// `fd` must be an exclusively-owned file descriptor. The returned
    /// `TunFdIo` closes it on drop.
    pub unsafe fn from_fd_with_format(
        fd: RawFd,
        mtu: usize,
        format: TunFdPacketFormat,
    ) -> CoreResult<Self> {
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        Self::from_owned_fd_with_format(owned, mtu, format)
    }

    #[inline]
    pub fn from_owned_fd(fd: OwnedFd, mtu: usize) -> CoreResult<Self> {
        Self::from_owned_fd_with_format(fd, mtu, TunFdPacketFormat::platform_default())
    }

    pub fn from_owned_fd_with_format(
        fd: OwnedFd,
        mtu: usize,
        format: TunFdPacketFormat,
    ) -> CoreResult<Self> {
        if mtu == 0 {
            return Err(CoreError::internal("TUN fd MTU must be > 0"));
        }
        set_nonblocking(fd.as_raw_fd())?;
        let fd = AsyncFd::with_interest(fd, Interest::READABLE | Interest::WRITABLE)
            .map_err(|err| CoreError::internal(format!("register TUN fd: {err}")))?;
        Ok(Self {
            inner: Arc::new(TunFdIoInner { fd, mtu, format }),
        })
    }

    #[inline]
    pub fn mtu(&self) -> usize {
        self.inner.mtu
    }

    #[inline]
    pub fn packet_format(&self) -> TunFdPacketFormat {
        self.inner.format
    }

    pub fn try_recv_buffer(&self, dst: &mut [u8]) -> CoreResult<Option<usize>> {
        if dst.is_empty() {
            return Ok(None);
        }
        let payload_len = dst.len().min(self.inner.mtu);
        let result = match self.inner.format {
            TunFdPacketFormat::RawIp => {
                try_async_fd_io(&self.inner.fd, Interest::READABLE, |raw| {
                    try_read(raw, &mut dst[..payload_len])
                })
            }
            TunFdPacketFormat::AppleUtun => {
                try_async_fd_io(&self.inner.fd, Interest::READABLE, |raw| {
                    let mut header = [0u8; APPLE_UTUN_HEADER_LEN];
                    try_readv_payload(raw, &mut header, dst, self.inner.mtu)
                })
            }
        };
        match result {
            Ok(result) => result,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(err) => Err(CoreError::internal(format!("read TUN fd: {err}"))),
        }
    }

    pub fn try_send_buffer(&self, packet: &[u8], offset: usize) -> CoreResult<TunFdSendResult> {
        if offset >= packet.len() {
            return Ok(TunFdSendResult::Complete);
        }
        let payload = &packet[offset..];
        let result = match self.inner.format {
            TunFdPacketFormat::RawIp => {
                try_async_fd_io(&self.inner.fd, Interest::WRITABLE, |raw| {
                    try_write_payload(raw, payload, offset)
                })
            }
            TunFdPacketFormat::AppleUtun => {
                if offset > 0 {
                    try_async_fd_io(&self.inner.fd, Interest::WRITABLE, |raw| {
                        try_write_payload(raw, payload, offset)
                    })
                } else {
                    let Some(header) = apple_utun_header(packet) else {
                        return Ok(TunFdSendResult::Complete);
                    };
                    try_async_fd_io(&self.inner.fd, Interest::WRITABLE, |raw| {
                        try_writev_payload(raw, &header, payload)
                    })
                }
            }
        };
        match result {
            Ok(result) => result,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                Ok(TunFdSendResult::Backpressure)
            }
            Err(err) => Err(CoreError::internal(format!("write TUN fd: {err}"))),
        }
    }

    pub fn try_send_buffers(
        &self,
        segments: &[&[u8]],
        offset: usize,
        total_len: usize,
    ) -> CoreResult<TunFdSendResult> {
        if offset >= total_len {
            return Ok(TunFdSendResult::Complete);
        }
        if segment_total_len(segments) < total_len {
            return Err(CoreError::internal(
                "writev TUN fd payload segments shorter than total length",
            ));
        }
        let result = match self.inner.format {
            TunFdPacketFormat::RawIp => {
                try_async_fd_io(&self.inner.fd, Interest::WRITABLE, |raw| {
                    try_writev_segments(raw, None, segments, offset, total_len)
                })
            }
            TunFdPacketFormat::AppleUtun => {
                if offset > 0 {
                    try_async_fd_io(&self.inner.fd, Interest::WRITABLE, |raw| {
                        try_writev_segments(raw, None, segments, offset, total_len)
                    })
                } else {
                    let Some(first_byte) = first_payload_byte(segments) else {
                        return Err(CoreError::internal(
                            "writev TUN fd payload segments shorter than total length",
                        ));
                    };
                    let Some(header) = apple_utun_header_from_first_byte(first_byte) else {
                        return Ok(TunFdSendResult::Complete);
                    };
                    try_async_fd_io(&self.inner.fd, Interest::WRITABLE, |raw| {
                        try_writev_segments(raw, Some(&header), segments, offset, total_len)
                    })
                }
            }
        };
        match result {
            Ok(result) => result,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                Ok(TunFdSendResult::Backpressure)
            }
            Err(err) => Err(CoreError::internal(format!("writev TUN fd: {err}"))),
        }
    }

    #[inline]
    pub async fn readable(&self) -> CoreResult<()> {
        let _guard = self
            .inner
            .fd
            .readable()
            .await
            .map_err(|err| CoreError::internal(format!("TUN fd readable: {err}")))?;
        Ok(())
    }

    #[inline]
    pub async fn writable(&self) -> CoreResult<()> {
        let _guard = self
            .inner
            .fd
            .writable()
            .await
            .map_err(|err| CoreError::internal(format!("TUN fd writable: {err}")))?;
        Ok(())
    }

    pub fn poll_read_ready(&self, cx: &mut Context<'_>) -> Poll<CoreResult<()>> {
        match self.inner.fd.poll_read_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_guard)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                Poll::Ready(Err(CoreError::internal(format!("TUN fd readable: {err}"))))
            }
        }
    }

    pub fn poll_write_ready(&self, cx: &mut Context<'_>) -> Poll<CoreResult<()>> {
        match self.inner.fd.poll_write_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(_guard)) => Poll::Ready(Ok(())),
            Poll::Ready(Err(err)) => {
                Poll::Ready(Err(CoreError::internal(format!("TUN fd writable: {err}"))))
            }
        }
    }
}

fn set_nonblocking(raw: RawFd) -> CoreResult<()> {
    let flags = unsafe { libc::fcntl(raw, libc::F_GETFL, 0) };
    if flags == -1 {
        return Err(CoreError::internal(format!(
            "fcntl F_GETFL: {}",
            io::Error::last_os_error()
        )));
    }
    if flags & libc::O_NONBLOCK == 0
        && unsafe { libc::fcntl(raw, libc::F_SETFL, flags | libc::O_NONBLOCK) } == -1
    {
        return Err(CoreError::internal(format!(
            "fcntl F_SETFL O_NONBLOCK: {}",
            io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn try_async_fd_io<T>(
    fd: &AsyncFd<OwnedFd>,
    interest: Interest,
    mut do_io: impl FnMut(RawFd) -> io::Result<CoreResult<T>>,
) -> io::Result<CoreResult<T>> {
    let mut attempted = false;
    match fd.try_io(interest, |fd| {
        attempted = true;
        do_io(fd.as_raw_fd())
    }) {
        Ok(result) => Ok(result),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock && !attempted => {
            do_io(fd.get_ref().as_raw_fd())
        }
        Err(err) => Err(err),
    }
}

fn try_read(raw: RawFd, dst: &mut [u8]) -> io::Result<CoreResult<Option<usize>>> {
    loop {
        let n = unsafe { libc::read(raw, dst.as_mut_ptr().cast(), dst.len()) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if err.kind() == io::ErrorKind::WouldBlock {
                return Err(err);
            }
            return Ok(Err(CoreError::internal(format!("read TUN fd: {err}"))));
        }
        return Ok(Ok((n > 0).then_some(n as usize)));
    }
}

fn try_readv_payload(
    raw: RawFd,
    header: &mut [u8; APPLE_UTUN_HEADER_LEN],
    dst: &mut [u8],
    mtu: usize,
) -> io::Result<CoreResult<Option<usize>>> {
    let payload_len = dst.len().min(mtu);
    loop {
        let mut iov = [
            libc::iovec {
                iov_base: header.as_mut_ptr().cast(),
                iov_len: APPLE_UTUN_HEADER_LEN,
            },
            libc::iovec {
                iov_base: dst.as_mut_ptr().cast(),
                iov_len: payload_len,
            },
        ];
        let n = unsafe { libc::readv(raw, iov.as_mut_ptr(), iov.len() as i32) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if err.kind() == io::ErrorKind::WouldBlock {
                return Err(err);
            }
            return Ok(Err(CoreError::internal(format!("readv TUN fd: {err}"))));
        }
        let n = n as usize;
        if n <= APPLE_UTUN_HEADER_LEN {
            return Ok(Ok(None));
        }
        return Ok(Ok(Some(n - APPLE_UTUN_HEADER_LEN)));
    }
}

fn try_write_payload(
    raw: RawFd,
    payload: &[u8],
    base_offset: usize,
) -> io::Result<CoreResult<TunFdSendResult>> {
    loop {
        let n = unsafe { libc::write(raw, payload.as_ptr().cast(), payload.len()) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if is_tun_fd_backpressure(&err) {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            return Ok(Err(CoreError::internal(format!("write TUN fd: {err}"))));
        }
        if n == 0 {
            return Ok(Err(CoreError::internal("write TUN fd wrote zero bytes")));
        }
        let n = n as usize;
        if n == payload.len() {
            return Ok(Ok(TunFdSendResult::Complete));
        }
        return Ok(Ok(TunFdSendResult::Partial(base_offset + n)));
    }
}

fn try_writev_payload(
    raw: RawFd,
    header: &[u8; APPLE_UTUN_HEADER_LEN],
    payload: &[u8],
) -> io::Result<CoreResult<TunFdSendResult>> {
    loop {
        let iov = [
            libc::iovec {
                iov_base: header.as_ptr() as *mut libc::c_void,
                iov_len: APPLE_UTUN_HEADER_LEN,
            },
            libc::iovec {
                iov_base: payload.as_ptr() as *mut libc::c_void,
                iov_len: payload.len(),
            },
        ];
        let n = unsafe { libc::writev(raw, iov.as_ptr(), iov.len() as i32) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if is_tun_fd_backpressure(&err) {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            return Ok(Err(CoreError::internal(format!("writev TUN fd: {err}"))));
        }
        if n == 0 {
            return Ok(Err(CoreError::internal("writev TUN fd wrote zero bytes")));
        }
        return Ok(writev_payload_result(n as usize, payload.len()));
    }
}

fn try_writev_segments(
    raw: RawFd,
    header: Option<&[u8; APPLE_UTUN_HEADER_LEN]>,
    segments: &[&[u8]],
    offset: usize,
    total_len: usize,
) -> io::Result<CoreResult<TunFdSendResult>> {
    let Some(window) =
        writev_segment_set(header, segments, offset, total_len, writev_iovec_limit())
    else {
        return Ok(Err(CoreError::internal(
            "writev TUN fd packet exceeds single syscall iovec capacity",
        )));
    };
    if window.payload_len == 0 {
        return Ok(Ok(TunFdSendResult::Complete));
    }
    loop {
        let n = unsafe { libc::writev(raw, window.iov.as_ptr(), window.iov_len as i32) };
        if n < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if is_tun_fd_backpressure(&err) {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            return Ok(Err(CoreError::internal(format!("writev TUN fd: {err}"))));
        }
        if n == 0 {
            return Ok(Err(CoreError::internal("writev TUN fd wrote zero bytes")));
        }
        return Ok(writev_segments_result(
            n as usize,
            header.is_some(),
            offset,
            window.payload_len,
            total_len,
        ));
    }
}

struct WritevSegmentSet<'a> {
    iov: [libc::iovec; MAX_WRITEV_IOVEC],
    iov_len: usize,
    payload_len: usize,
    _marker: std::marker::PhantomData<&'a [u8]>,
}

fn writev_segment_set<'a>(
    header: Option<&'a [u8; APPLE_UTUN_HEADER_LEN]>,
    segments: &'a [&'a [u8]],
    offset: usize,
    total_len: usize,
    iovec_limit: usize,
) -> Option<WritevSegmentSet<'a>> {
    let mut iov = [empty_iovec(); MAX_WRITEV_IOVEC];
    let mut iov_len = 0;
    let mut payload_len = 0;
    if let Some(header) = header {
        if iov_len == iovec_limit {
            return None;
        }
        iov[iov_len] = const_iovec(header.as_slice());
        iov_len += 1;
    }
    let mut cursor = 0usize;
    let mut remaining_skip = offset;
    for segment in segments.iter().copied() {
        if cursor >= total_len {
            break;
        }
        let remaining_total = total_len - cursor;
        let segment = &segment[..segment.len().min(remaining_total)];
        cursor += segment.len();
        if remaining_skip >= segment.len() {
            remaining_skip -= segment.len();
            continue;
        }
        let payload = &segment[remaining_skip..];
        remaining_skip = 0;
        if payload.is_empty() {
            continue;
        }
        if iov_len == iovec_limit {
            return None;
        }
        iov[iov_len] = const_iovec(payload);
        iov_len += 1;
        payload_len += payload.len();
    }
    if cursor < total_len || payload_len < total_len.saturating_sub(offset) {
        return None;
    }
    Some(WritevSegmentSet {
        iov,
        iov_len,
        payload_len,
        _marker: std::marker::PhantomData,
    })
}

fn writev_iovec_limit() -> usize {
    let platform_limit = unsafe { libc::sysconf(libc::_SC_IOV_MAX) };
    if platform_limit > 0 {
        MAX_WRITEV_IOVEC.min(platform_limit as usize)
    } else {
        MAX_WRITEV_IOVEC
    }
}

fn segment_total_len(segments: &[&[u8]]) -> usize {
    segments.iter().map(|segment| segment.len()).sum()
}

fn writev_segments_result(
    bytes_written: usize,
    has_header: bool,
    offset: usize,
    payload_len: usize,
    total_len: usize,
) -> CoreResult<TunFdSendResult> {
    let payload_written = if has_header {
        if bytes_written < APPLE_UTUN_HEADER_LEN {
            return Err(CoreError::internal(
                "writev TUN fd wrote partial Apple utun header",
            ));
        }
        if bytes_written == APPLE_UTUN_HEADER_LEN {
            return Err(CoreError::internal(
                "writev TUN fd wrote only Apple utun header",
            ));
        }
        bytes_written - APPLE_UTUN_HEADER_LEN
    } else {
        bytes_written
    };
    let next_offset = offset + payload_written;
    if payload_written >= payload_len && next_offset >= total_len {
        Ok(TunFdSendResult::Complete)
    } else {
        Ok(TunFdSendResult::Partial(next_offset))
    }
}

#[inline]
const fn empty_iovec() -> libc::iovec {
    libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    }
}

#[inline]
fn const_iovec(bytes: &[u8]) -> libc::iovec {
    libc::iovec {
        iov_base: bytes.as_ptr() as *mut libc::c_void,
        iov_len: bytes.len(),
    }
}

fn writev_payload_result(bytes_written: usize, payload_len: usize) -> CoreResult<TunFdSendResult> {
    if bytes_written < APPLE_UTUN_HEADER_LEN {
        return Err(CoreError::internal(
            "writev TUN fd wrote partial Apple utun header",
        ));
    }
    if bytes_written == APPLE_UTUN_HEADER_LEN {
        return Err(CoreError::internal(
            "writev TUN fd wrote only Apple utun header",
        ));
    }
    let payload_written = bytes_written - APPLE_UTUN_HEADER_LEN;
    if payload_written == payload_len {
        return Ok(TunFdSendResult::Complete);
    }
    Ok(TunFdSendResult::Partial(payload_written))
}

fn apple_utun_header(packet: &[u8]) -> Option<[u8; APPLE_UTUN_HEADER_LEN]> {
    packet
        .first()
        .and_then(|byte| apple_utun_header_from_first_byte(*byte))
}

fn apple_utun_header_from_first_byte(byte: u8) -> Option<[u8; APPLE_UTUN_HEADER_LEN]> {
    match byte >> 4 {
        4 => Some([0, 0, 0, libc::AF_INET as u8]),
        6 => Some([0, 0, 0, libc::AF_INET6 as u8]),
        _ => None,
    }
}

fn first_payload_byte(segments: &[&[u8]]) -> Option<u8> {
    segments.iter().find_map(|segment| segment.first().copied())
}

fn is_tun_fd_backpressure(err: &io::Error) -> bool {
    if err.kind() == io::ErrorKind::WouldBlock {
        return true;
    }
    matches!(
        err.raw_os_error(),
        Some(code) if code == libc::ENOBUFS || code == libc::ENOSPC
    )
}

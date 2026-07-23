use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use hammer_infra::segment::Svm;
use hammer_runtime::app::{AppSession, SessionHandle, SessionOffsets};
use thiserror::Error;

/// Failures while an app client receives an attached shared-memory session.
#[derive(Debug, Error)]
pub enum AppClientError {
    #[error("failed to connect to app server at {path}")]
    Connect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to receive attach descriptors")]
    Receive {
        #[source]
        source: std::io::Error,
    },
    #[error("attach metadata length mismatch: expected {expected}, got {actual}")]
    MetadataLength { expected: usize, actual: usize },
    #[error("attach descriptor control data was truncated")]
    ControlTruncated,
    #[error("attach message contained unexpected control data")]
    UnexpectedControl,
    #[error("attach descriptor control header is invalid")]
    InvalidControlHeader,
    #[error("attach descriptor payload is invalid")]
    InvalidDescriptorPayload,
    #[error("attach descriptor count mismatch: expected {expected}, got {actual}")]
    DescriptorCount { expected: usize, actual: usize },
    #[error("failed to read received attach descriptor flags")]
    ReceivedDescriptorFlags {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set received attach descriptor close-on-exec")]
    ReceivedDescriptorCloseOnExec {
        #[source]
        source: std::io::Error,
    },
    #[error("attach offsets exceed the mapped address range")]
    OffsetOverflow,
    #[error("failed to map the attached SVM segment")]
    SegmentMap {
        #[source]
        source: std::io::Error,
    },
}

/// Client side of the application-session protocol.
/// Connects to the dataplane's Unix socket, receives shared-memory fds
/// and layout offsets, and reconstructs the app-side [`AppSession`].
pub struct AppClient;

/// Parse a SCM_RIGHTS message, returning the received fds and raw data.
fn recv_attach_message(
    stream: &UnixStream,
) -> Result<([OwnedFd; 3], SessionOffsets), AppClientError> {
    let mut offsets_bytes = [0u64; 4];
    let mut cmsg_buf = [0u8; 64];
    let mut iov = libc::iovec {
        iov_base: offsets_bytes.as_mut_ptr() as *mut libc::c_void,
        iov_len: std::mem::size_of_val(&offsets_bytes),
    };
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov as *mut _;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut _;
    msg.msg_controllen = cmsg_buf.len() as _;

    // SAFETY: message points to writable data and control buffers for recvmsg.
    let ret = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) };
    if ret < 0 {
        return Err(AppClientError::Receive {
            source: std::io::Error::last_os_error(),
        });
    }

    let mut received = Vec::new();
    let mut rights_headers = 0usize;
    // SAFETY: recvmsg initialized the ancillary buffer and msg_controllen.
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level != libc::SOL_SOCKET || (*cmsg).cmsg_type != libc::SCM_RIGHTS {
                return Err(AppClientError::UnexpectedControl);
            }
            rights_headers += 1;
            let header_len = libc::CMSG_LEN(0) as usize;
            let cmsg_len = (*cmsg).cmsg_len as usize;
            if cmsg_len < header_len {
                return Err(AppClientError::InvalidControlHeader);
            }
            let payload_len = cmsg_len - header_len;
            if payload_len % std::mem::size_of::<RawFd>() != 0 {
                return Err(AppClientError::InvalidDescriptorPayload);
            }
            let fd_count = payload_len / std::mem::size_of::<RawFd>();
            let data = libc::CMSG_DATA(cmsg).cast::<RawFd>();
            for offset in 0..fd_count {
                let raw = data.add(offset).read_unaligned();
                // SAFETY: SCM_RIGHTS installed a fresh descriptor in this
                // process and ownership transfers immediately to OwnedFd.
                received.push(OwnedFd::from_raw_fd(raw));
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    if ret as usize != std::mem::size_of_val(&offsets_bytes) {
        return Err(AppClientError::MetadataLength {
            expected: std::mem::size_of_val(&offsets_bytes),
            actual: ret as usize,
        });
    }
    if msg.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0 {
        return Err(AppClientError::ControlTruncated);
    }
    if rights_headers != 1 || received.len() != 3 {
        return Err(AppClientError::DescriptorCount {
            expected: 3,
            actual: received.len(),
        });
    }
    for fd in &received {
        // SAFETY: fcntl only queries a live received descriptor.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
        if flags < 0 {
            return Err(AppClientError::ReceivedDescriptorFlags {
                source: std::io::Error::last_os_error(),
            });
        }
        // SAFETY: queried descriptor flags are valid for F_SETFD.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            return Err(AppClientError::ReceivedDescriptorCloseOnExec {
                source: std::io::Error::last_os_error(),
            });
        }
    }
    let fds: [OwnedFd; 3] =
        received
            .try_into()
            .map_err(|received: Vec<OwnedFd>| AppClientError::DescriptorCount {
                expected: 3,
                actual: received.len(),
            })?;

    let offsets = SessionOffsets {
        rx_fifo_off: offsets_bytes[0],
        tx_fifo_off: offsets_bytes[1],
        evt_q_off: offsets_bytes[2],
        tx_evt_q_off: offsets_bytes[3],
    };

    Ok((fds, offsets))
}

impl AppClient {
    /// Connect to the dataplane's app socket at `path` and set up
    /// the app-side half of a shared-memory session.
    pub fn connect(path: &str, handle: SessionHandle) -> Result<AppSession<Svm>, AppClientError> {
        let stream = UnixStream::connect(path).map_err(|source| AppClientError::Connect {
            path: path.into(),
            source,
        })?;

        let (fds, offsets) = recv_attach_message(&stream)?;
        let [shm_fd, evt_q_read_fd, tx_evt_q_write_fd] = fds;

        // Determine the segment size from the allocated offsets.
        // The last byte used is tx_evt_q_off + msgq_size; use the
        // offsets as a hint for the size.
        let seg_size = offsets
            .tx_evt_q_off
            .checked_add(4096)
            .ok_or(AppClientError::OffsetOverflow)? as usize;

        let seg = Svm::from_fd(shm_fd.as_raw_fd(), seg_size)
            .map_err(|source| AppClientError::SegmentMap { source })?;

        let session = unsafe {
            AppSession::<Svm>::from_segment(
                handle,
                &seg,
                &offsets,
                Some(evt_q_read_fd.into_raw_fd()),
                None,
                None,
                Some(tx_evt_q_write_fd.into_raw_fd()),
            )
        };

        Ok(session)
    }
}

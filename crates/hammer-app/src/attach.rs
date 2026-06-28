use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::segment::Svm;

use hammer_runtime::app::{AppSession, SessionHandle, SessionOffsets};

/// Client side of the attach protocol.
/// Connects to the dataplane's Unix socket, receives shared-memory fds
/// and layout offsets, and reconstructs the app-side [`AppSession`].
pub struct AttachClient {
    seg: Svm,
    pub session: AppSession<Svm>,
    pub offsets: SessionOffsets,
}

/// Parse a SCM_RIGHTS message, returning the received fds and raw data.
fn recv_attach_message(stream: &UnixStream) -> HammerResult<([RawFd; 3], SessionOffsets)> {
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

    let ret = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msg, 0) };
    if ret < 0 {
        return Err(HammerError::internal(format!(
            "attach recvmsg failed: {}",
            std::io::Error::last_os_error()
        )));
    }

    let mut fds = [0i32; 3];
    let mut found = false;
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let payload_len = (*cmsg).cmsg_len as usize - libc::CMSG_LEN(0) as usize;
                let fd_count = payload_len / std::mem::size_of::<RawFd>();
                let count = fd_count.min(fds.len());
                std::ptr::copy_nonoverlapping(
                    libc::CMSG_DATA(cmsg) as *const RawFd,
                    fds.as_mut_ptr(),
                    count,
                );
                found = true;
                break;
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }
    if !found {
        return Err(HammerError::internal(
            "attach recvmsg did not contain SCM_RIGHTS",
        ));
    }

    let offsets = SessionOffsets {
        rx_fifo_off: offsets_bytes[0],
        tx_fifo_off: offsets_bytes[1],
        evt_q_off: offsets_bytes[2],
        tx_evt_q_off: offsets_bytes[3],
    };

    Ok((fds, offsets))
}

impl AttachClient {
    /// Connect to the dataplane's attach socket at `path` and set up
    /// the app-side half of a shared-memory session.
    pub fn connect(path: &str, handle: SessionHandle) -> HammerResult<Self> {
        let stream = UnixStream::connect(path)
            .map_err(|e| HammerError::internal(format!("attach connect to {path}: {e}")))?;

        let (fds, offsets) = recv_attach_message(&stream)?;
        let [shm_fd, evt_q_read_fd, tx_evt_q_write_fd] = fds;

        // Determine the segment size from the allocated offsets.
        // The last byte used is tx_evt_q_off + msgq_size; use the
        // offsets as a hint for the size.
        let seg_size = offsets
            .tx_evt_q_off
            .checked_add(4096)
            .ok_or_else(|| HammerError::internal("attach offset overflow"))?
            as usize;

        let seg = Svm::from_fd(shm_fd, seg_size)
            .map_err(|e| HammerError::internal(format!("attach mmap shm: {e}")))?;

        let session = unsafe {
            AppSession::<Svm>::from_segment(
                handle,
                &seg,
                &offsets,
                Some(evt_q_read_fd),
                None,
                None,
                Some(tx_evt_q_write_fd),
            )
        };

        Ok(Self {
            seg,
            session,
            offsets,
        })
    }
}

use std::os::fd::RawFd;
use std::os::unix::io::AsRawFd;

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::fifo::Fifo;
use hammer_infra::fifo::FifoError;
use hammer_infra::segment::{Segment, Svm};

use crate::app::{
    AppSession, AppSessionConfig, SessionHandle, SessionMsgQueue, SessionOffsets, SessionSegment,
};

/// Server side of the Unix-domain-socket attach protocol.
/// The dataplane binds a listener, accepts app connections, and sends
/// shared-memory segment fds + offset layout to the app process.
pub struct AttachServer {
    listener: std::os::unix::net::UnixListener,
}

/// Result of a successful attach: the dataplane-side session object and
/// the metadata needed by the app process to reconstruct the session.
pub struct AttachedApp<S: SessionSegment> {
    pub session: AppSession<S>,
    pub offsets: SessionOffsets,
    pub shm_fd: RawFd,
}

fn create_pipe_flags() -> HammerResult<(RawFd, RawFd)> {
    let mut fds = [0i32; 2];
    let ret = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if ret != 0 {
        return Err(HammerError::internal("create attach signal pipe"));
    }
    for fd in &fds {
        let flags = unsafe { libc::fcntl(*fd, libc::F_GETFL) };
        unsafe {
            libc::fcntl(*fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
        let fdflags = unsafe { libc::fcntl(*fd, libc::F_GETFD) };
        unsafe {
            libc::fcntl(*fd, libc::F_SETFD, fdflags | libc::FD_CLOEXEC);
        }
    }
    Ok((fds[0], fds[1]))
}

fn dup_fd(fd: RawFd) -> HammerResult<RawFd> {
    let duped = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if duped < 0 {
        Err(HammerError::internal("dup attach signal fd"))
    } else {
        Ok(duped)
    }
}

/// Pack the fds needed by the app process into a SCM_RIGHTS message
/// and send them together with the offset layout over `stream`.
fn send_attach_message(
    stream: &std::os::unix::net::UnixStream,
    shm_fd: RawFd,
    evt_q_read_fd: RawFd,
    tx_evt_q_write_fd: RawFd,
    offsets: &SessionOffsets,
) -> HammerResult<()> {
    let fds = [shm_fd, evt_q_read_fd, tx_evt_q_write_fd];
    let offsets_bytes: [u64; 4] = [
        offsets.rx_fifo_off,
        offsets.tx_fifo_off,
        offsets.evt_q_off,
        offsets.tx_evt_q_off,
    ];

    let iov = libc::iovec {
        iov_base: offsets_bytes.as_ptr() as *mut libc::c_void,
        iov_len: std::mem::size_of_val(&offsets_bytes),
    };

    let mut cmsg_buf = [0u8; 80];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &iov as *const _ as *mut _;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut _;
    msg.msg_controllen = cmsg_buf.len() as _;

    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        if cmsg.is_null() {
            return Err(HammerError::internal("attach CMSG_FIRSTHDR failed"));
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN((fds.len() * std::mem::size_of::<RawFd>()) as u32) as _;
        std::ptr::copy_nonoverlapping(fds.as_ptr(), libc::CMSG_DATA(cmsg) as *mut RawFd, fds.len());
        msg.msg_controllen = (*cmsg).cmsg_len as _;
    }

    let ret = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
    if ret < 0 {
        Err(HammerError::internal(format!(
            "attach sendmsg failed: {}",
            std::io::Error::last_os_error()
        )))
    } else {
        Ok(())
    }
}

impl AttachServer {
    /// Bind to a Unix domain socket at `path`.
    pub fn bind(path: &str) -> HammerResult<Self> {
        let _ = std::fs::remove_file(path);
        let listener = std::os::unix::net::UnixListener::bind(path).map_err(|e| {
            HammerError::internal(format!("failed to bind attach server at {path}: {e}"))
        })?;
        Ok(Self { listener })
    }

    /// Accept a single app process connection and set up a shared-memory
    /// session. Returns an [`AttachedApp`] with the dataplane-side session
    /// and the metadata the app process needs to reconstruct its side.
    pub fn accept(
        &self,
        config: AppSessionConfig,
        seg: &Svm,
        handle: SessionHandle,
    ) -> HammerResult<AttachedApp<Svm>> {
        let offsets =
            SessionOffsets::allocate(seg, config.fifo_capacity as u32, config.evt_q_capacity);

        unsafe {
            Fifo::<Svm>::init_at(seg.clone(), offsets.rx_fifo_off, config.fifo_capacity)
                .map_err(|e| HammerError::internal(format!("attach init rx fifo: {e:?}")))?;
            Fifo::<Svm>::init_at(seg.clone(), offsets.tx_fifo_off, config.fifo_capacity)
                .map_err(|e| HammerError::internal(format!("attach init tx fifo: {e:?}")))?;
        }

        let ring_nitems = config.evt_q_capacity.max(1) as u32;
        let q_nitems = (config.evt_q_capacity + 1).next_power_of_two().max(2) as u32;
        unsafe {
            SessionMsgQueue::<Svm>::init_at(seg.clone(), offsets.evt_q_off, q_nitems, ring_nitems)
                .map_err(|e| HammerError::internal(format!("attach init evt_q: {e:?}")))?;
            SessionMsgQueue::<Svm>::init_at(seg.clone(), offsets.tx_evt_q_off, 64, 64)
                .map_err(|e| HammerError::internal(format!("attach init tx_evt_q: {e:?}")))?;
        }

        // Create signal pipe pairs for cross-process notification.
        let (evt_q_read, evt_q_write) = create_pipe_flags()?;
        let (tx_evt_q_read, tx_evt_q_write) = create_pipe_flags()?;

        let session = unsafe {
            AppSession::<Svm>::from_segment(
                handle,
                seg,
                &offsets,
                None,
                Some(evt_q_write),
                Some(tx_evt_q_read),
                None,
            )
        };

        let (stream, _) = self
            .listener
            .accept()
            .map_err(|e| HammerError::internal(format!("attach accept connection: {e}")))?;

        let shm_fd = seg
            .fd()
            .ok_or_else(|| HammerError::internal("attach segment has no fd"))?;

        // Dup the fds we need to send to the app so the server-side Session
        // Message Queues (inside the AppSession, via from_segment) retain
        // ownership of their copies.
        let client_evt_q_read = dup_fd(evt_q_read)?;
        let client_tx_evt_q_write = dup_fd(tx_evt_q_write)?;

        send_attach_message(
            &stream,
            shm_fd,
            client_evt_q_read,
            client_tx_evt_q_write,
            &offsets,
        )?;

        Ok(AttachedApp {
            session,
            offsets,
            shm_fd,
        })
    }
}

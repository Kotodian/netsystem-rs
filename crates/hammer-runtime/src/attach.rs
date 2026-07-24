use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};

use crate::{AttachError, RuntimeResult};
use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;

use crate::app::{AppSession, AppSessionConfig, SessionHandle, SessionMsgQueue, SessionOffsets};

/// Server side of the Unix-domain-socket attach protocol.
/// The dataplane binds a listener, accepts app connections, and sends
/// shared-memory segment fds + offset layout to the app process.
pub struct AppServer {
    listener: std::os::unix::net::UnixListener,
}

/// Server-owned resources for one application session.
///
/// The session itself remains [`AppSession`]. The layout and descriptor fields
/// are protocol metadata needed by the application process.
pub struct AppSessionResources {
    pub session: AppSession,
    pub offsets: SessionOffsets,
    pub shm_fd: RawFd,
}

fn create_pipe_flags() -> RuntimeResult<(OwnedFd, OwnedFd)> {
    let mut fds = [0i32; 2];
    // SAFETY: fds is writable for two descriptors. Fresh descriptors are
    // transferred into OwnedFd immediately on success.
    let ret = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if ret != 0 {
        return Err(AttachError::SignalPipeCreate {
            source: std::io::Error::last_os_error(),
        }
        .into());
    }
    // SAFETY: pipe returned two fresh descriptors with unique ownership.
    let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    // SAFETY: ownership of the distinct write descriptor transfers once.
    let write = unsafe { OwnedFd::from_raw_fd(fds[1]) };
    for fd in [&read, &write] {
        // SAFETY: fcntl only queries the live descriptor owned above.
        let flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(AttachError::SignalStatusFlags {
                source: std::io::Error::last_os_error(),
            }
            .into());
        }
        // SAFETY: queried flags are valid for this live descriptor.
        if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(AttachError::SignalNonblocking {
                source: std::io::Error::last_os_error(),
            }
            .into());
        }
        // SAFETY: fcntl only queries descriptor flags.
        let descriptor_flags = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_GETFD) };
        if descriptor_flags < 0 {
            return Err(AttachError::SignalDescriptorFlags {
                source: std::io::Error::last_os_error(),
            }
            .into());
        }
        // SAFETY: queried flags are valid for this live descriptor.
        if unsafe {
            libc::fcntl(
                fd.as_raw_fd(),
                libc::F_SETFD,
                descriptor_flags | libc::FD_CLOEXEC,
            )
        } < 0
        {
            return Err(AttachError::SignalCloseOnExec {
                source: std::io::Error::last_os_error(),
            }
            .into());
        }
    }
    Ok((read, write))
}

/// Pack the fds needed by the app process into a SCM_RIGHTS message
/// and send them together with the offset layout over `stream`.
fn send_attach_message(
    stream: &std::os::unix::net::UnixStream,
    shm_fd: RawFd,
    evt_q_read_fd: RawFd,
    tx_evt_q_write_fd: RawFd,
    offsets: &SessionOffsets,
    segment_size: usize,
) -> RuntimeResult<()> {
    let fds = [shm_fd, evt_q_read_fd, tx_evt_q_write_fd];
    let metadata: [u64; 5] = [
        offsets.rx_fifo_off,
        offsets.tx_fifo_off,
        offsets.evt_q_off,
        offsets.tx_evt_q_off,
        segment_size as u64,
    ];

    let iov = libc::iovec {
        iov_base: metadata.as_ptr() as *mut libc::c_void,
        iov_len: std::mem::size_of_val(&metadata),
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
            return Err(AttachError::ControlHeaderMissing.into());
        }
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type = libc::SCM_RIGHTS;
        (*cmsg).cmsg_len = libc::CMSG_LEN((fds.len() * std::mem::size_of::<RawFd>()) as u32) as _;
        std::ptr::copy_nonoverlapping(fds.as_ptr(), libc::CMSG_DATA(cmsg) as *mut RawFd, fds.len());
        msg.msg_controllen = (*cmsg).cmsg_len as _;
    }

    let ret = unsafe { libc::sendmsg(stream.as_raw_fd(), &msg, 0) };
    if ret < 0 {
        Err(AttachError::Send {
            source: std::io::Error::last_os_error(),
        }
        .into())
    } else {
        Ok(())
    }
}

impl AppServer {
    /// Bind to a Unix domain socket at `path`.
    pub fn bind(path: &str) -> RuntimeResult<Self> {
        let _ = std::fs::remove_file(path);
        let listener =
            std::os::unix::net::UnixListener::bind(path).map_err(|source| AttachError::Bind {
                path: path.into(),
                source,
            })?;
        Ok(Self { listener })
    }

    /// Accept a single app process connection and set up a shared-memory
    /// session. Returns the [`AppSessionResources`] held by the dataplane and
    /// and the metadata the app process needs to reconstruct its side.
    pub fn accept(
        &self,
        config: AppSessionConfig,
        seg: &Segment,
        handle: SessionHandle,
    ) -> RuntimeResult<AppSessionResources> {
        let offsets = SessionOffsets::allocate(seg, config.fifo_capacity, config.evt_q_capacity)
            .map_err(|source| AttachError::SessionLayout { source })?;

        unsafe {
            Fifo::init_at(seg.clone(), offsets.rx_fifo_off, config.fifo_capacity)
                .map_err(|_| AttachError::RxFifoInvalid)?;
            Fifo::init_at(seg.clone(), offsets.tx_fifo_off, config.fifo_capacity)
                .map_err(|_| AttachError::TxFifoInvalid)?;
        }

        let ring_nitems = config.evt_q_capacity.max(1) as u32;
        let q_nitems = (config.evt_q_capacity + 1).next_power_of_two().max(2) as u32;
        unsafe {
            SessionMsgQueue::init_at(seg.clone(), offsets.evt_q_off, q_nitems, ring_nitems)
                .map_err(|_| AttachError::EventQueueInvalid)?;
            SessionMsgQueue::init_at(seg.clone(), offsets.tx_evt_q_off, 64, 64)
                .map_err(|_| AttachError::TxEventQueueInvalid)?;
        }

        // Create signal pipe pairs for cross-process notification.
        let (evt_q_read, evt_q_write) = create_pipe_flags()?;
        let (tx_evt_q_read, tx_evt_q_write) = create_pipe_flags()?;

        let session = unsafe {
            AppSession::from_segment(
                handle,
                seg,
                &offsets,
                None,
                Some(evt_q_write.into_raw_fd()),
                Some(tx_evt_q_read.into_raw_fd()),
                None,
            )
        };

        let (stream, _) = self
            .listener
            .accept()
            .map_err(|source| AttachError::Accept { source })?;

        let shm_fd = seg
            .shared_fd()
            .ok_or(AttachError::SegmentDescriptorMissing)?;

        send_attach_message(
            &stream,
            shm_fd,
            evt_q_read.as_raw_fd(),
            tx_evt_q_write.as_raw_fd(),
            &offsets,
            seg.size(),
        )?;

        Ok(AppSessionResources {
            session,
            offsets,
            shm_fd,
        })
    }
}

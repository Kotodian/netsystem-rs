use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use hammer_infra::segment::Segment;
use hammer_runtime::app::{AppSession, ApplicationId, SessionHandle, SessionOffsets};
use hammer_runtime::attach::{
    ATTACH_DESCRIPTOR_COUNT, ATTACH_METADATA_BYTES, ATTACH_METADATA_WORDS, ATTACH_PROTOCOL_VERSION,
    ATTACH_REPLY_BYTES, ATTACH_REPLY_WORDS, ATTACH_REQUEST_BYTES, ATTACH_STATUS_ACCEPTED,
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppClientError {
    #[error("failed to connect to app server at {path}")]
    Connect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to register Application on the attach connection")]
    Registration {
        #[source]
        source: std::io::Error,
    },
    #[error("attach server rejected the Application attach request")]
    AttachRejected,
    #[error("failed to receive attach descriptors")]
    Receive {
        #[source]
        source: std::io::Error,
    },
    #[error("attach metadata length mismatch: expected {expected}, got {actual}")]
    MetadataLength { expected: usize, actual: usize },
    #[error("unsupported attach protocol version {actual}")]
    ProtocolVersion { actual: u64 },
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
    #[error("failed to map the attached session segment")]
    SessionSegmentMap {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to map the attached worker event segment")]
    WorkerSegmentMap {
        #[source]
        source: std::io::Error,
    },
}

pub struct AppClient {
    stream: UnixStream,
    application: ApplicationId,
}

impl AppClient {
    pub fn connect(path: &str) -> Result<Self, AppClientError> {
        let mut stream = UnixStream::connect(path).map_err(|source| AppClientError::Connect {
            path: path.into(),
            source,
        })?;
        let request = ATTACH_PROTOCOL_VERSION.to_le_bytes();
        debug_assert_eq!(request.len(), ATTACH_REQUEST_BYTES);
        stream
            .write_all(&request)
            .map_err(|source| AppClientError::Registration { source })?;
        let mut reply = [0_u8; ATTACH_REPLY_BYTES];
        stream
            .read_exact(&mut reply)
            .map_err(|source| AppClientError::Registration { source })?;
        let mut words = [0_u64; ATTACH_REPLY_WORDS];
        for (word, chunk) in words.iter_mut().zip(reply.chunks_exact(size_of::<u64>())) {
            *word = u64::from_le_bytes(
                chunk
                    .try_into()
                    .expect("attach reply word occupies one complete u64"),
            );
        }
        if words[0] != ATTACH_PROTOCOL_VERSION {
            return Err(AppClientError::ProtocolVersion { actual: words[0] });
        }
        if words[1] != ATTACH_STATUS_ACCEPTED {
            return Err(AppClientError::AttachRejected);
        }
        Ok(Self {
            stream,
            application: ApplicationId::from_raw(words[2]),
        })
    }

    #[inline]
    pub const fn application(&self) -> ApplicationId {
        self.application
    }

    pub fn accept(&self) -> Result<AppSession, AppClientError> {
        let mut metadata = [0_u8; ATTACH_METADATA_BYTES];
        let mut control = [0_u8; 128];
        let mut iov = libc::iovec {
            iov_base: metadata.as_mut_ptr().cast::<libc::c_void>(),
            iov_len: metadata.len(),
        };
        let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
        message.msg_iov = std::ptr::from_mut(&mut iov);
        message.msg_iovlen = 1;
        message.msg_control = control.as_mut_ptr().cast::<libc::c_void>();
        message.msg_controllen = control.len() as _;

        let received_bytes = unsafe { libc::recvmsg(self.stream.as_raw_fd(), &mut message, 0) };
        if received_bytes < 0 {
            return Err(AppClientError::Receive {
                source: std::io::Error::last_os_error(),
            });
        }

        let mut received = Vec::with_capacity(ATTACH_DESCRIPTOR_COUNT);
        let mut rights_headers = 0_usize;
        unsafe {
            let mut header = libc::CMSG_FIRSTHDR(&message);
            while !header.is_null() {
                if (*header).cmsg_level != libc::SOL_SOCKET
                    || (*header).cmsg_type != libc::SCM_RIGHTS
                {
                    return Err(AppClientError::UnexpectedControl);
                }
                rights_headers += 1;
                let header_len = libc::CMSG_LEN(0) as usize;
                let control_len = (*header).cmsg_len as usize;
                if control_len < header_len {
                    return Err(AppClientError::InvalidControlHeader);
                }
                let payload_len = control_len - header_len;
                if payload_len % size_of::<RawFd>() != 0 {
                    return Err(AppClientError::InvalidDescriptorPayload);
                }
                let descriptor_count = payload_len / size_of::<RawFd>();
                let data = libc::CMSG_DATA(header).cast::<RawFd>();
                for index in 0..descriptor_count {
                    received.push(OwnedFd::from_raw_fd(data.add(index).read_unaligned()));
                }
                header = libc::CMSG_NXTHDR(&message, header);
            }
        }

        if received_bytes as usize != metadata.len() {
            return Err(AppClientError::MetadataLength {
                expected: metadata.len(),
                actual: received_bytes as usize,
            });
        }
        if message.msg_flags & (libc::MSG_CTRUNC | libc::MSG_TRUNC) != 0 {
            return Err(AppClientError::ControlTruncated);
        }
        if rights_headers != 1 || received.len() != ATTACH_DESCRIPTOR_COUNT {
            return Err(AppClientError::DescriptorCount {
                expected: ATTACH_DESCRIPTOR_COUNT,
                actual: received.len(),
            });
        }
        for descriptor in &received {
            let flags = unsafe { libc::fcntl(descriptor.as_raw_fd(), libc::F_GETFD) };
            if flags < 0 {
                return Err(AppClientError::ReceivedDescriptorFlags {
                    source: std::io::Error::last_os_error(),
                });
            }
            if unsafe {
                libc::fcntl(
                    descriptor.as_raw_fd(),
                    libc::F_SETFD,
                    flags | libc::FD_CLOEXEC,
                )
            } < 0
            {
                return Err(AppClientError::ReceivedDescriptorCloseOnExec {
                    source: std::io::Error::last_os_error(),
                });
            }
        }
        let [session_fd, tx_event_fd, event_read_fd, tx_event_write_fd]: [OwnedFd;
            ATTACH_DESCRIPTOR_COUNT] = received.try_into().map_err(|received: Vec<OwnedFd>| {
            AppClientError::DescriptorCount {
                expected: ATTACH_DESCRIPTOR_COUNT,
                actual: received.len(),
            }
        })?;

        let mut words = [0_u64; ATTACH_METADATA_WORDS];
        for (word, chunk) in words
            .iter_mut()
            .zip(metadata.chunks_exact(size_of::<u64>()))
        {
            let Ok(bytes) = <[u8; size_of::<u64>()]>::try_from(chunk) else {
                return Err(AppClientError::MetadataLength {
                    expected: ATTACH_METADATA_WORDS * size_of::<u64>(),
                    actual: metadata.len(),
                });
            };
            *word = u64::from_le_bytes(bytes);
        }
        if words[0] != ATTACH_PROTOCOL_VERSION {
            return Err(AppClientError::ProtocolVersion { actual: words[0] });
        }
        let session_segment_size =
            usize::try_from(words[2]).map_err(|_| AppClientError::OffsetOverflow)?;
        let tx_event_segment_size =
            usize::try_from(words[3]).map_err(|_| AppClientError::OffsetOverflow)?;
        if session_segment_size == 0
            || session_segment_size > isize::MAX as usize
            || tx_event_segment_size == 0
            || tx_event_segment_size > isize::MAX as usize
        {
            return Err(AppClientError::OffsetOverflow);
        }
        let offsets = SessionOffsets {
            rx_fifo_off: words[4],
            tx_fifo_off: words[5],
            evt_q_off: words[6],
            tx_evt_q_off: words[7],
        };
        if [offsets.rx_fifo_off, offsets.tx_fifo_off, offsets.evt_q_off]
            .into_iter()
            .any(|offset| offset >= session_segment_size as u64)
            || offsets.tx_evt_q_off >= tx_event_segment_size as u64
        {
            return Err(AppClientError::OffsetOverflow);
        }

        let session_segment = Segment::from_fd(session_fd.as_raw_fd(), session_segment_size)
            .map_err(|source| AppClientError::SessionSegmentMap { source })?;
        let tx_event_segment = Segment::from_fd(tx_event_fd.as_raw_fd(), tx_event_segment_size)
            .map_err(|source| AppClientError::WorkerSegmentMap { source })?;
        Ok(unsafe {
            AppSession::from_segments(
                SessionHandle::from(words[1]),
                &session_segment,
                &tx_event_segment,
                &offsets,
                Some(event_read_fd.into_raw_fd()),
                None,
                None,
                Some(tx_event_write_fd.into_raw_fd()),
            )
        })
    }
}

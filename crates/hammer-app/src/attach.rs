use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, ApplicationId, ApplicationSessionMqError, ApplicationSessionStatus, SessionHandle,
    SessionMsgQueue, SessionOffsets,
};
use hammer_runtime::attach::{
    APPLICATION_MQ_DESCRIPTOR_COUNT, APPLICATION_MQ_METADATA_BYTES, APPLICATION_MQ_METADATA_WORDS,
    ATTACH_DESCRIPTOR_COUNT, ATTACH_METADATA_BYTES, ATTACH_METADATA_WORDS, ATTACH_PROTOCOL_VERSION,
    ATTACH_REPLY_BYTES, ATTACH_REPLY_WORDS, ATTACH_REQUEST_BYTES, ATTACH_STATUS_ACCEPTED,
};
use thiserror::Error;

mod descriptor;

#[derive(Debug, Error)]
pub enum AppClientError {
    #[error("failed to open Application attach socket at {path}")]
    Attach {
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
    #[error("failed to map Application Session control segment")]
    SessionControlSegmentMap {
        #[source]
        source: std::io::Error,
    },
    #[error("Application Session control metadata contains an invalid size or offset")]
    SessionControlOffset,
    #[error("Application Session MQ operation failed")]
    SessionControl {
        #[source]
        source: ApplicationSessionMqError,
    },
    #[error("failed while waiting for an Application Session reply")]
    SessionReplyWait {
        #[source]
        source: std::io::Error,
    },
    #[error("Application Session reply context mismatch: expected {expected}, got {actual}")]
    SessionReplyContext { expected: u64, actual: u64 },
    #[error("Application Session request was rejected with status {status:?}")]
    SessionRejected { status: ApplicationSessionStatus },
}

pub struct AppClient {
    stream: UnixStream,
    application: ApplicationId,
    pub(crate) session_requests: SessionMsgQueue,
    pub(crate) session_replies: SessionMsgQueue,
    pub(crate) next_session_context: u64,
}

impl AppClient {
    pub fn attach(path: &str) -> Result<Self, AppClientError> {
        let mut stream = UnixStream::connect(path).map_err(|source| AppClientError::Attach {
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
        let application = ApplicationId::from_raw(words[2]);
        let (metadata, [segment_fd, request_write_fd, reply_read_fd]) = descriptor::receive::<
            APPLICATION_MQ_METADATA_BYTES,
            APPLICATION_MQ_DESCRIPTOR_COUNT,
        >(&stream)?;
        let words = descriptor::words::<APPLICATION_MQ_METADATA_WORDS>(&metadata)?;
        if words[0] != ATTACH_PROTOCOL_VERSION {
            return Err(AppClientError::ProtocolVersion { actual: words[0] });
        }
        let segment_size =
            usize::try_from(words[1]).map_err(|_| AppClientError::SessionControlOffset)?;
        if segment_size == 0
            || segment_size > isize::MAX as usize
            || words[2] >= segment_size as u64
            || words[3] >= segment_size as u64
        {
            return Err(AppClientError::SessionControlOffset);
        }
        let segment = Segment::from_fd(segment_fd.as_raw_fd(), segment_size)
            .map_err(|source| AppClientError::SessionControlSegmentMap { source })?;
        let session_requests = unsafe {
            SessionMsgQueue::from_shared(
                segment.clone(),
                words[2],
                None,
                Some(request_write_fd.into_raw_fd()),
            )
        };
        let session_replies = unsafe {
            SessionMsgQueue::from_shared(segment, words[3], Some(reply_read_fd.into_raw_fd()), None)
        };
        Ok(Self {
            stream,
            application,
            session_requests,
            session_replies,
            next_session_context: 1,
        })
    }

    #[inline]
    pub const fn application(&self) -> ApplicationId {
        self.application
    }

    pub fn accept(&self) -> Result<AppSession, AppClientError> {
        let (metadata, [session_fd, tx_event_fd, event_read_fd, tx_event_write_fd]) =
            descriptor::receive::<ATTACH_METADATA_BYTES, ATTACH_DESCRIPTOR_COUNT>(&self.stream)?;
        let words = descriptor::words::<ATTACH_METADATA_WORDS>(&metadata)?;
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

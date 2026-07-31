use std::io::{Read, Write};
use std::os::fd::{AsRawFd, IntoRawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;

use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, ApplicationId, ApplicationSessionMqError, ApplicationSessionStatus, SessionHandle,
    SessionMsgQueue, SessionOffsets,
};
use hammer_runtime::attach::{
    APPLICATION_MQ_BASE_DESCRIPTOR_COUNT, APPLICATION_MQ_METADATA_BYTES,
    APPLICATION_MQ_METADATA_WORDS, ATTACH_DESCRIPTOR_COUNT, ATTACH_METADATA_BYTES,
    ATTACH_METADATA_WORDS, ATTACH_PROTOCOL_VERSION, ATTACH_REPLY_BYTES, ATTACH_REPLY_WORDS,
    ATTACH_REQUEST_BYTES, ATTACH_STATUS_ACCEPTED, MAX_ATTACH_DESCRIPTORS,
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
    #[error("attach descriptor count {actual} exceeds protocol maximum {max}")]
    DescriptorCountTooLarge { actual: usize, max: usize },
    #[error("Application MQ publication requires at least one Data Worker")]
    ApplicationMqWorkerCountZero,
    #[error("Application MQ worker count {count} cannot be represented")]
    ApplicationMqWorkerCountInvalid { count: u64 },
    #[error("Application MQ segment size {size} cannot be represented")]
    ApplicationMqSegmentSize { size: u64 },
    #[error("failed to map the attached Application MQ segment")]
    ApplicationMqSegmentMap {
        #[source]
        source: std::io::Error,
    },
    #[error(
        "Application MQ worker {worker} offset {offset} is outside segment size {segment_size}"
    )]
    ApplicationMqOffset {
        worker: usize,
        offset: u64,
        segment_size: u64,
    },
    #[error("Application MQ worker {worker} is outside the mapped worker range {worker_count}")]
    WorkerQueueMissing { worker: usize, worker_count: usize },
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
    rx_mqs: Box<[Arc<SessionMsgQueue>]>,
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
        let (mut metadata, descriptors) =
            descriptor::receive(&stream, APPLICATION_MQ_METADATA_BYTES)?;
        let words = descriptor::words_prefix::<APPLICATION_MQ_METADATA_WORDS>(&metadata)?;
        if words[0] != ATTACH_PROTOCOL_VERSION {
            return Err(AppClientError::ProtocolVersion { actual: words[0] });
        }
        let control_segment_size =
            usize::try_from(words[1]).map_err(|_| AppClientError::SessionControlOffset)?;
        let rx_mqs_segment_size = usize::try_from(words[4])
            .map_err(|_| AppClientError::ApplicationMqSegmentSize { size: words[4] })?;
        let worker_count = usize::try_from(words[5])
            .map_err(|_| AppClientError::ApplicationMqWorkerCountInvalid { count: words[5] })?;
        if worker_count == 0 {
            return Err(AppClientError::ApplicationMqWorkerCountZero);
        }
        let expected_descriptors = APPLICATION_MQ_BASE_DESCRIPTOR_COUNT
            .checked_add(worker_count)
            .ok_or(AppClientError::InvalidDescriptorPayload)?;
        if expected_descriptors > MAX_ATTACH_DESCRIPTORS {
            return Err(AppClientError::DescriptorCountTooLarge {
                actual: expected_descriptors,
                max: MAX_ATTACH_DESCRIPTORS,
            });
        }
        if descriptors.len() != expected_descriptors {
            return Err(AppClientError::DescriptorCount {
                expected: expected_descriptors,
                actual: descriptors.len(),
            });
        }
        let expected_metadata_len = APPLICATION_MQ_METADATA_BYTES
            .checked_add(
                worker_count
                    .checked_mul(size_of::<u64>())
                    .ok_or(AppClientError::InvalidDescriptorPayload)?,
            )
            .ok_or(AppClientError::InvalidDescriptorPayload)?;
        metadata.resize(expected_metadata_len, 0);
        stream
            .read_exact(&mut metadata[APPLICATION_MQ_METADATA_BYTES..])
            .map_err(|source| AppClientError::Receive { source })?;
        if control_segment_size == 0
            || control_segment_size > isize::MAX as usize
            || words[2] >= control_segment_size as u64
            || words[3] >= control_segment_size as u64
            || rx_mqs_segment_size == 0
            || rx_mqs_segment_size > isize::MAX as usize
        {
            return Err(AppClientError::SessionControlOffset);
        }
        let mut descriptors = descriptors.into_iter();
        let control_segment = Segment::from_fd(
            descriptors
                .next()
                .expect("validated attach descriptor count")
                .as_raw_fd(),
            control_segment_size,
        )
        .map_err(|source| AppClientError::SessionControlSegmentMap { source })?;
        let session_requests = unsafe {
            SessionMsgQueue::from_shared(
                control_segment.clone(),
                words[2],
                None,
                Some(
                    descriptors
                        .next()
                        .expect("validated attach descriptor count")
                        .into_raw_fd(),
                ),
            )
        };
        let session_replies = unsafe {
            SessionMsgQueue::from_shared(
                control_segment,
                words[3],
                Some(
                    descriptors
                        .next()
                        .expect("validated attach descriptor count")
                        .into_raw_fd(),
                ),
                None,
            )
        };
        let rx_mqs_segment = Segment::from_fd(
            descriptors
                .next()
                .expect("validated attach descriptor count")
                .as_raw_fd(),
            rx_mqs_segment_size,
        )
        .map_err(|source| AppClientError::ApplicationMqSegmentMap { source })?;
        let rx_mq_offsets =
            descriptor::words_slice(&metadata, APPLICATION_MQ_METADATA_WORDS, worker_count)?;
        let mut rx_mqs = Vec::with_capacity(worker_count);
        for worker in 0..worker_count {
            let offset = rx_mq_offsets[worker];
            if offset >= rx_mqs_segment.size() as u64 {
                return Err(AppClientError::ApplicationMqOffset {
                    worker,
                    offset,
                    segment_size: rx_mqs_segment.size() as u64,
                });
            }
            rx_mqs.push(Arc::new(unsafe {
                SessionMsgQueue::from_shared(
                    rx_mqs_segment.clone(),
                    offset,
                    None,
                    Some(
                        descriptors
                            .next()
                            .expect("validated attach descriptor count")
                            .into_raw_fd(),
                    ),
                )
            }));
        }
        Ok(Self {
            stream,
            application,
            session_requests,
            session_replies,
            rx_mqs: rx_mqs.into_boxed_slice(),
            next_session_context: 1,
        })
    }

    #[inline]
    pub const fn application(&self) -> ApplicationId {
        self.application
    }

    pub fn accept(&self) -> Result<AppSession, AppClientError> {
        let (metadata, descriptors) = descriptor::receive(&self.stream, ATTACH_METADATA_BYTES)?;
        let words = descriptor::words_prefix::<ATTACH_METADATA_WORDS>(&metadata)?;
        if words[0] != ATTACH_PROTOCOL_VERSION {
            return Err(AppClientError::ProtocolVersion { actual: words[0] });
        }
        if descriptors.len() != ATTACH_DESCRIPTOR_COUNT {
            return Err(AppClientError::DescriptorCount {
                expected: ATTACH_DESCRIPTOR_COUNT,
                actual: descriptors.len(),
            });
        }
        let handle = SessionHandle::from(words[1]);
        let session_segment_size =
            usize::try_from(words[2]).map_err(|_| AppClientError::OffsetOverflow)?;
        if session_segment_size == 0 || session_segment_size > isize::MAX as usize {
            return Err(AppClientError::OffsetOverflow);
        }
        let offsets = SessionOffsets {
            rx_fifo_off: words[3],
            tx_fifo_off: words[4],
            evt_q_off: words[5],
        };
        if [offsets.rx_fifo_off, offsets.tx_fifo_off, offsets.evt_q_off]
            .into_iter()
            .any(|offset| offset >= session_segment_size as u64)
        {
            return Err(AppClientError::OffsetOverflow);
        }

        let mut descriptors = descriptors.into_iter();
        let session_segment = Segment::from_fd(
            descriptors
                .next()
                .expect("validated attach descriptor count")
                .as_raw_fd(),
            session_segment_size,
        )
        .map_err(|source| AppClientError::SessionSegmentMap { source })?;
        let worker = handle.worker_index() as usize;
        let worker_queue =
            self.rx_mqs
                .get(worker)
                .cloned()
                .ok_or(AppClientError::WorkerQueueMissing {
                    worker,
                    worker_count: self.rx_mqs.len(),
                })?;
        Ok(unsafe {
            AppSession::from_segment(
                handle,
                &session_segment,
                &offsets,
                Some(
                    descriptors
                        .next()
                        .expect("validated attach descriptor count")
                        .into_raw_fd(),
                ),
                worker_queue,
            )
        })
    }
}

use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, IntoRawFd};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::sync::Arc;

use hammer_infra::segment::Segment;
use hammer_runtime::app::{
    AppSession, AppSessionError, SessionAcceptedMsg, SessionBoundMsg, SessionConnectError,
    SessionConnectedMsg, SessionControlDecodeError, SessionControlError, SessionEvtType,
    SessionHandle, SessionMsgQueue, SessionMsgQueueError, SessionOffsets, SessionProducer,
    SessionUnlistenReplyMsg, SingleProducer,
};
use hammer_runtime::attach::{
    APPLICATION_MQ_BASE_DESCRIPTOR_COUNT, APPLICATION_MQ_METADATA_BYTES,
    APPLICATION_MQ_METADATA_WORDS, ATTACH_DESCRIPTOR_COUNT, ATTACH_METADATA_BYTES,
    ATTACH_METADATA_WORDS, ATTACH_PROTOCOL_VERSION, ATTACH_REPLY_BYTES, ATTACH_REPLY_WORDS,
    ATTACH_REQUEST_BYTES, ATTACH_STATUS_ACCEPTED, ExtConfigStore, MAX_ATTACH_DESCRIPTORS,
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
    #[error("bounded ext-config storage published by the daemon is invalid")]
    ExtConfig {
        #[source]
        source: hammer_runtime::AttachError,
    },
    #[error("the daemon published no bounded ext-config storage for this Application")]
    ExtConfigStoreMissing,
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
        source: SessionMsgQueueError,
    },
    #[error("Application Session control payload could not be decoded")]
    SessionControlDecode {
        #[source]
        source: SessionControlDecodeError,
    },
    #[error("failed to reconstruct the Application Session")]
    SessionFromSegment {
        #[source]
        source: AppSessionError,
    },
    #[error("failed while waiting for an Application Session reply")]
    SessionReplyWait {
        #[source]
        source: std::io::Error,
    },
    #[error("Application Session reply context mismatch: expected {expected}, got {actual}")]
    SessionReplyContext { expected: u64, actual: u64 },
    #[error("Application Session request was rejected: {error}")]
    SessionRejected { error: SessionControlError },
    #[error("Application connection {connection:?} failed: {error}")]
    SessionConnectFailed {
        connection: u32,
        error: SessionConnectError,
    },
    #[error("Application Session emitted unexpected event {event:?}")]
    UnexpectedSessionEvent { event: SessionEvtType },
    #[error("Application Session handle mismatch: expected {expected:?}, got {actual:?}")]
    SessionHandleMismatch {
        expected: SessionHandle,
        actual: SessionHandle,
    },
}

/// One typed Application Session control message buffered by the client.
///
/// The client's single reply inbox: service messages (BOUND,
/// UNLISTEN_REPLY, CONNECTED, ACCEPTED) are pushed in arrival order and
/// consumed by context or Session handle. This is the in-memory typed view of
/// the wire control slot; it is never serialized itself.
#[derive(Debug)]
pub enum ControlReply {
    Bound(SessionBoundMsg),
    Unlisten(SessionUnlistenReplyMsg),
    Connected(SessionConnectedMsg),
    Accepted(SessionAcceptedMsg),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlReplyKind {
    Bound,
    Unlisten,
    Connected,
    Accepted,
}

impl ControlReply {
    #[inline]
    pub fn kind(&self) -> ControlReplyKind {
        match self {
            Self::Bound(_) => ControlReplyKind::Bound,
            Self::Unlisten(_) => ControlReplyKind::Unlisten,
            Self::Connected(_) => ControlReplyKind::Connected,
            Self::Accepted(_) => ControlReplyKind::Accepted,
        }
    }

    #[inline]
    pub(crate) fn context(&self) -> u64 {
        match self {
            Self::Bound(reply) => reply.context,
            Self::Unlisten(reply) => reply.context,
            Self::Connected(reply) => reply.context,
            Self::Accepted(reply) => reply.context,
        }
    }
}

pub struct AppClient {
    stream: UnixStream,
    application: u32,
    /// The Application-owned single-producer capability for Session control
    /// requests. The daemon maps the same queue as a consumer only; it never
    /// claims the producer.
    pub(crate) session_requests: RefCell<SessionProducer>,
    /// The Application-side consumer of Session control replies. The daemon
    /// owns the reply single-producer capability.
    pub(crate) session_replies: RefCell<SessionMsgQueue<SingleProducer>>,
    rx_mqs: Box<[Arc<SessionMsgQueue>]>,
    pub(crate) ext_config: Option<ExtConfigStore>,
    pub(crate) next_session_context: u64,
    pub(crate) pending_replies: RefCell<VecDeque<ControlReply>>,
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
        let application =
            u32::try_from(words[2]).map_err(|_| AppClientError::InvalidDescriptorPayload)?;
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
        // The Application owns the request single-producer capability: it is
        // claimed once here (a daemon-side claim would be a typed error) and
        // outlives the mapping.
        let session_requests = unsafe {
            SessionMsgQueue::<SingleProducer>::from_shared(
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
        }
        .map_err(|source| AppClientError::SessionControl { source })?
        .claim_producer()
        .map_err(|source| AppClientError::SessionControl { source })?;
        let session_replies = unsafe {
            SessionMsgQueue::<SingleProducer>::from_shared(
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
        }
        .map_err(|source| AppClientError::SessionControl { source })?;
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
        let ext_config = if words[6] == 0 {
            None
        } else if words[6] < rx_mqs_segment_size as u64 {
            ExtConfigStore::from_shared(rx_mqs_segment.clone(), words[6] as usize)
                .map(Some)
                .map_err(|source| AppClientError::ExtConfig { source })?
        } else {
            return Err(AppClientError::ExtConfig {
                source: hammer_runtime::AttachError::ExtConfigOffsetOutOfRange,
            });
        };
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
            rx_mqs.push(Arc::new(
                unsafe {
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
                }
                .map_err(|source| AppClientError::SessionControl { source })?,
            ));
        }
        Ok(Self {
            stream,
            application,
            session_requests: RefCell::new(session_requests),
            session_replies: RefCell::new(session_replies),
            rx_mqs: rx_mqs.into_boxed_slice(),
            ext_config,
            next_session_context: 1,
            pending_replies: RefCell::new(VecDeque::new()),
        })
    }

    /// Control-queue seam for client MQ protocol tests: builds a client
    /// over an existing Session control request/reply queue pair, without the
    /// attach handshake. `stream` is the descriptor stream: established-Session
    /// methods (`accept` / `accept_with_handle`) read the production attach
    /// metadata and SCM_RIGHTS descriptors from it, so the test must deliver
    /// them in the daemon format (or avoid those methods). `rx_mqs` is the
    /// per-worker Application Rx MQ set selected by
    /// `handle.thread_index`; no ext-config store is attached.
    pub fn with_queues(
        stream: UnixStream,
        application: u32,
        requests: SessionProducer,
        replies: SessionMsgQueue<SingleProducer>,
        rx_mqs: Box<[Arc<SessionMsgQueue>]>,
    ) -> Self {
        Self {
            stream,
            application,
            session_requests: RefCell::new(requests),
            session_replies: RefCell::new(replies),
            rx_mqs,
            ext_config: None,
            next_session_context: 1,
            pending_replies: RefCell::new(VecDeque::new()),
        }
    }

    #[inline]
    pub const fn application(&self) -> u32 {
        self.application
    }

    pub fn accept(&self) -> Result<AppSession, AppClientError> {
        self.accept_with_handle(None)
    }

    /// Reconstructs one established App Session from the published attach
    /// descriptors, verifying the received Session handle against
    /// `expected_handle` when given.
    ///
    /// This is the established-session seam consumed by the client Session
    /// layer: the
    /// CONNECTED/ACCEPTED control message carries the Session handle and
    /// flags, and the descriptors arrive on the attach stream; the caller
    /// preserves the flags from the control message itself.
    pub fn accept_with_handle(
        &self,
        expected_handle: Option<SessionHandle>,
    ) -> Result<AppSession, AppClientError> {
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
        let session_index =
            u32::try_from(words[1]).map_err(|_| AppClientError::InvalidDescriptorPayload)?;
        let thread_index =
            u32::try_from(words[2]).map_err(|_| AppClientError::InvalidDescriptorPayload)?;
        let handle = SessionHandle::new(session_index, thread_index);
        if let Some(expected) = expected_handle
            && expected != handle
        {
            return Err(AppClientError::SessionHandleMismatch {
                expected,
                actual: handle,
            });
        }
        let session_segment_size =
            usize::try_from(words[3]).map_err(|_| AppClientError::OffsetOverflow)?;
        if session_segment_size == 0 || session_segment_size > isize::MAX as usize {
            return Err(AppClientError::OffsetOverflow);
        }
        let offsets = SessionOffsets {
            rx_fifo_off: words[4],
            tx_fifo_off: words[5],
            evt_q_off: words[6],
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
        let worker = handle.thread_index as usize;
        let worker_queue =
            self.rx_mqs
                .get(worker)
                .cloned()
                .ok_or(AppClientError::WorkerQueueMissing {
                    worker,
                    worker_count: self.rx_mqs.len(),
                })?;
        unsafe {
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
        }
        .map_err(|source| AppClientError::SessionFromSegment { source })
    }
}

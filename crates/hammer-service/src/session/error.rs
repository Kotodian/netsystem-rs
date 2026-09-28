use hammer_core::data_plane::NodeId;
use hammer_infra::svm::fifo::FifoError;
use hammer_infra::svm::fifo_segment::FifoSegmentError;
use hammer_infra::svm::msg_queue::SvmMsgQError;
use hammer_runtime::RuntimeError;
use thiserror::Error;

use super::app::ApplicationError;

#[hammer_component_macros::runtime_error(subsystem = "session queue")]
#[derive(Debug, Error)]
pub enum SessionQueueError {
    #[error("session queue node is not registered")]
    NodeMissing,
    #[error("runtime thread {thread_index} is not a data worker")]
    WorkerUnavailable { thread_index: u32 },
    #[error("session worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("session queue output node {output_node:?} is not registered for {consumer:?}")]
    OutputMissing {
        consumer: NodeId,
        output_node: NodeId,
    },
    #[error("Application {application:?} has no per-worker MQ registration")]
    ApplicationMqMissing { application: u32 },
    #[error("Session worker message-queue segment creation failed")]
    SegmentCreate {
        #[source]
        source: FifoSegmentError,
    },
}

#[hammer_component_macros::runtime_error(subsystem = "session")]
#[derive(Debug, Error)]
pub enum SessionError {
    // VPP source: session_types.h:519-576, foreach_session_error.
    #[error("unknown Session failure")]
    Unknown,
    #[error("connection refused")]
    Refused,
    #[error("Session operation timed out")]
    TimedOut,
    #[error("Session allocation failed")]
    Allocation,
    #[error("Session object is not owned by the caller")]
    Owner,
    #[error("no route")]
    NoRoute,
    #[error("no resolving interface")]
    NoInterface,
    #[error("local interface has no IP address")]
    NoIp,
    #[error("no local port is available")]
    NoPort,
    #[error("operation is not supported")]
    NotSupported,
    #[error("endpoint is not listening")]
    NotListening,
    #[error("Session does not exist")]
    NoSession,
    #[error("Application is not attached")]
    NoApplication,
    #[error("Application is already attached")]
    ApplicationAttached,
    #[error("Application attach failed")]
    ApplicationAttach {
        #[source]
        source: ApplicationError,
    },
    #[error("Application detach failed")]
    ApplicationDetach {
        #[source]
        source: ApplicationError,
    },
    #[error("local port is already in use")]
    PortInUse,
    #[error("IP address is already in use")]
    IpInUse,
    #[error("IP and port pair is already listening")]
    AlreadyListening,
    #[error("address is not in use")]
    AddressNotInUse,
    #[error("invalid Session value")]
    Invalid,
    #[error("invalid remote IP address")]
    InvalidRemoteIp,
    #[error("invalid Application Worker")]
    InvalidApplicationWorker,
    #[error("invalid Application Namespace")]
    InvalidNamespace,
    #[error("Session segment has no space for a FIFO pair")]
    SegmentNoSpace,
    #[error("new Session segment has no space for a FIFO pair")]
    NewSegmentNoSpace,
    #[error("Session segment creation failed")]
    SegmentCreate {
        #[source]
        source: FifoSegmentError,
    },
    #[error("Session was filtered")]
    Filtered,
    #[error("requested Session scope is not supported")]
    ScopeNotSupported,
    #[error("Binary API connection has no file descriptor")]
    BinaryApiNoFileDescriptor,
    #[error("Binary API file descriptor send failed")]
    BinaryApiSendFileDescriptor,
    #[error("Binary API registration does not exist")]
    BinaryApiRegistrationMissing,
    #[error("Session message allocation failed: {source}")]
    MessageQueueAllocation {
        #[source]
        source: SvmMsgQError,
    },
    #[error("TLS handshake failed")]
    TlsHandshake,
    #[error("eventfd allocation failed")]
    EventFdAllocation,
    #[error("extended transport configuration is missing")]
    ExtendedConfigMissing,
    #[error("crypto engine is missing")]
    CryptoEngineMissing,
    #[error("crypto certificate/key pair is missing")]
    CryptoKeyPairMissing,
    #[error("local-scope connect failed")]
    LocalConnect,
    #[error("Application Namespace secret is incorrect")]
    WrongNamespaceSecret,
    #[error("system call failed")]
    Syscall,
    #[error("transport is not registered")]
    TransportNotRegistered,
    #[error("maximum stream count reached")]
    MaxStreamsReached,
    #[error("session {session_id:?} is not in the session pool")]
    SessionMissing { session_id: u32 },
    #[error("Session App {app:?} is not registered")]
    SessionAppNotRegistered { app: u32 },
    #[error("session {lower:?} already has an upper Session attached")]
    UpperSessionAlreadyAttached { lower: u32 },
    #[error("session {session_id:?} cannot publish its connection in its current state")]
    PublicationRejected { session_id: u32 },
    #[error("session {session_id:?} is active and cannot be rolled back")]
    RollbackRejected { session_id: u32 },
    #[error("transport Session {session_id:?} construction did not complete")]
    TransportSessionCreateIncomplete { session_id: u32 },
    #[error("session {session_id:?} connection is not published")]
    NotPublished { session_id: u32 },
    #[error(
        "session {session_id:?} out-of-order RX offset {offset} plus buffered length {buffered_len} overflows u32"
    )]
    RxOutOfOrderOffsetOverflow {
        session_id: u32,
        offset: u32,
        buffered_len: u32,
    },
    #[error("session {session_id:?} out-of-order RX enqueue failed at offset {offset}")]
    RxOutOfOrderEnqueue {
        session_id: u32,
        offset: u32,
        #[source]
        source: FifoError,
    },
    #[error(
        "session {session_id:?} transport TX offset {tx_offset} exceeds pending length {available}"
    )]
    TxOffsetOutOfRange {
        session_id: u32,
        tx_offset: usize,
        available: usize,
    },
    #[error("session {session_id:?} TX FIFO has no {payload_len} bytes at offset {tx_offset}")]
    TxFifoRangeInvalid {
        session_id: u32,
        tx_offset: usize,
        payload_len: usize,
    },
    #[error(
        "session {session_id:?} datagram payload length {payload_len} does not match header length {header_len}"
    )]
    DatagramLengthMismatch {
        session_id: u32,
        payload_len: usize,
        header_len: u32,
    },
    #[error("session {session_id:?} datagram FIFO reservation failed")]
    DatagramFifo {
        session_id: u32,
        #[source]
        source: FifoError,
    },
    #[error("session {session_id:?} accepted OOO delivery reported no retained span")]
    OooSpanMissing { session_id: u32 },
    #[error("session {session_id:?} accepted OOO delivery reported an invalid span")]
    OooSpanInvalid { session_id: u32 },
    #[error("Session transport operation failed")]
    TransportOpFailed {
        #[source]
        source: RuntimeError,
    },
}

use hammer_core::data_plane::NodeId;
use hammer_infra::svm::fifo::FifoError;
use hammer_infra::svm::msg_queue::SvmMsgQError;
use hammer_runtime::app::{SessionControlError, SessionHandle};
use hammer_runtime::{DataWorkerId, RuntimeError};
use thiserror::Error;

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
}

pub use hammer_runtime::app::SessionConnectError;

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
    SegmentCreate,
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
    #[error("session {session_id:?} RX accounting exceeds u32")]
    RxLengthOverflow { session_id: u32 },
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
    #[error("Session has no data workers configured")]
    NoDataWorkers,
    #[error("Session listener {listener:?} is not registered")]
    ListenerMissing { listener: SessionHandle },
    #[error("Session listener control is owned by another thread")]
    ListenerControlWrongThread,
    #[error("Session transport does not register listener operations")]
    TransportListenUnsupported,
    #[error("Session transport does not register active-open")]
    TransportConnectUnsupported,
    #[error("Session active-open has no Application connection")]
    ApplicationConnectionMissing,
    #[error("Session transport does not register stream active-open")]
    TransportConnectStreamUnsupported,
    #[error("Session transport operation failed")]
    TransportOpFailed {
        #[source]
        source: RuntimeError,
    },
    #[error("CONNECT_STREAM requires a parent Session handle")]
    ConnectStreamParentMissing,
    #[error(
        "CONNECT_STREAM for parent {parent:?} arrived on worker {actual:?}, expected owner worker {expected:?}"
    )]
    ConnectStreamWrongWorker {
        parent: SessionHandle,
        expected: DataWorkerId,
        actual: DataWorkerId,
    },
    #[error("Session {session_id:?} connect publication failed and its cleanup failed")]
    ConnectPublicationCleanup {
        session_id: u32,
        #[source]
        publication: RuntimeError,
        cleanup: RuntimeError,
    },
}

/// Maps a concrete [`SessionError`] to the control-protocol error the
/// Application observes, mirroring `From<ApplicationError>`. VPP notifies the
/// app worker with the specific rv of the failed connect/listen op
/// (`app_worker_connect_notify` with `rv != SESSION_E_NONE`,
/// session_node.c:263-267, session.c:1419-1452); every variant is listed
/// explicitly so an unmapped internal failure cannot silently substitute a
/// misleading wire error.
impl From<SessionError> for SessionControlError {
    fn from(error: SessionError) -> Self {
        match error {
            SessionError::Unknown
            | SessionError::Refused
            | SessionError::TimedOut
            | SessionError::Allocation
            | SessionError::Owner
            | SessionError::NoRoute
            | SessionError::NoInterface
            | SessionError::NoIp
            | SessionError::NoPort
            | SessionError::NotSupported
            | SessionError::NotListening
            | SessionError::NoSession
            | SessionError::NoApplication
            | SessionError::ApplicationAttached
            | SessionError::PortInUse
            | SessionError::IpInUse
            | SessionError::AlreadyListening
            | SessionError::AddressNotInUse
            | SessionError::Invalid
            | SessionError::InvalidRemoteIp
            | SessionError::InvalidApplicationWorker
            | SessionError::InvalidNamespace
            | SessionError::SegmentNoSpace
            | SessionError::NewSegmentNoSpace
            | SessionError::SegmentCreate
            | SessionError::Filtered
            | SessionError::ScopeNotSupported
            | SessionError::BinaryApiNoFileDescriptor
            | SessionError::BinaryApiSendFileDescriptor
            | SessionError::BinaryApiRegistrationMissing
            | SessionError::MessageQueueAllocation { .. }
            | SessionError::TlsHandshake
            | SessionError::EventFdAllocation
            | SessionError::ExtendedConfigMissing
            | SessionError::CryptoEngineMissing
            | SessionError::CryptoKeyPairMissing
            | SessionError::LocalConnect
            | SessionError::WrongNamespaceSecret
            | SessionError::Syscall
            | SessionError::TransportNotRegistered
            | SessionError::MaxStreamsReached => Self::TransportFailed,
            SessionError::ListenerMissing { .. } => Self::ListenerMissing,
            SessionError::ListenerControlWrongThread => Self::ApplicationControlWrongThread,
            SessionError::TransportListenUnsupported => Self::TransportListenUnsupported,
            SessionError::TransportConnectUnsupported => Self::TransportConnectUnsupported,
            SessionError::ApplicationConnectionMissing => Self::ConnectionMissing,
            SessionError::TransportConnectStreamUnsupported { .. } => {
                Self::TransportConnectUnsupported
            }
            SessionError::TransportOpFailed { .. } => Self::TransportFailed,
            SessionError::NoDataWorkers => Self::NoDataWorkers,
            SessionError::ConnectStreamParentMissing => Self::ConnectStreamParentMissing,
            SessionError::ConnectStreamWrongWorker { .. } => Self::ConnectStreamWrongWorker,
            // Session-table internals are never produced by a control op; the
            // concrete error stays in the source chain for diagnostics while
            // the wire reports the generic transport failure.
            SessionError::SessionMissing { .. }
            | SessionError::SessionAppNotRegistered { .. }
            | SessionError::UpperSessionAlreadyAttached { .. }
            | SessionError::PublicationRejected { .. }
            | SessionError::RollbackRejected { .. }
            | SessionError::NotPublished { .. }
            | SessionError::RxOutOfOrderOffsetOverflow { .. }
            | SessionError::RxOutOfOrderEnqueue { .. }
            | SessionError::TxOffsetOutOfRange { .. }
            | SessionError::TxFifoRangeInvalid { .. }
            | SessionError::RxLengthOverflow { .. }
            | SessionError::DatagramLengthMismatch { .. }
            | SessionError::DatagramFifo { .. }
            | SessionError::OooSpanMissing { .. }
            | SessionError::OooSpanInvalid { .. }
            | SessionError::TransportSessionCreateIncomplete { .. }
            | SessionError::ConnectPublicationCleanup { .. } => Self::TransportFailed,
        }
    }
}

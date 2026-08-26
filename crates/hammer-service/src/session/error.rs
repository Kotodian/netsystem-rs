use hammer_core::data_plane::NodeId;
use hammer_infra::fifo::FifoError;
use hammer_runtime::app::{ApplicationId, SessionControlError, SessionHandle};
use hammer_runtime::{DataWorkerId, RuntimeError};
use thiserror::Error;

use super::SessionId;
use super::runtime::SessionTransportId;

#[hammer_component_macros::runtime_error(subsystem = "session queue")]
#[derive(Debug, Error)]
#[repr(u16)]
pub enum SessionQueueError {
    #[error("session queue node is not registered")]
    NodeMissing,
    #[error("runtime thread {thread_index} is not a data worker")]
    WorkerUnavailable { thread_index: u32 },
    #[error("session worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("session worker {worker} is already installed")]
    WorkerAlreadyInstalled { worker: usize },
    #[error("session worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: hammer_infra::thread_owned::ThreadOwnedError,
    },
    #[error("session queue output node {output_node:?} is not registered for {consumer:?}")]
    OutputMissing {
        consumer: NodeId,
        output_node: NodeId,
    },
    #[error("Application {application:?} already has a per-worker MQ registration")]
    ApplicationMqAlreadyRegistered { application: ApplicationId },
    #[error("Application {application:?} has no per-worker MQ registration")]
    ApplicationMqMissing { application: ApplicationId },
    #[error("Session App does not provide registered context construction")]
    SessionAppContextCreateUnsupported,
    #[error("Session App {app:?} is already installed on this worker")]
    SessionAppAlreadyInstalled {
        app: hammer_runtime::app::SessionAppId,
    },
    #[error("Session App {app:?} is not installed on this worker")]
    SessionAppNotInstalled {
        app: hammer_runtime::app::SessionAppId,
    },
}

impl SessionQueueError {
    #[inline(always)]
    pub const fn code(&self) -> u16 {
        match self {
            Self::NodeMissing => 0,
            Self::WorkerUnavailable { .. } => 1,
            Self::WorkerOutOfRange { .. } => 2,
            Self::WorkerAlreadyInstalled { .. } => 3,
            Self::WorkerAccess { .. } => 4,
            Self::OutputMissing { .. } => 5,
            Self::ApplicationMqAlreadyRegistered { .. } => 6,
            Self::ApplicationMqMissing { .. } => 7,
            Self::SessionAppContextCreateUnsupported => 8,
            Self::SessionAppAlreadyInstalled { .. } => 9,
            Self::SessionAppNotInstalled { .. } => 10,
        }
    }
}

/// Worker-local transport action dispatch error, returned to the transport
/// calling through the Session Worker rather than to the Application control
/// plane. The Session entry is the transport authority; dispatch derives the
/// transport from it, so the remaining failures are a missing worker table and
/// an absent or non-transport Session (mirroring VPP's transport VFT dispatch
/// guards, session.c:1658-1712).
#[hammer_component_macros::runtime_error(subsystem = "session transport action")]
#[derive(Debug, Error)]
pub enum SessionTransportActionError {
    #[error("transport {transport:?} has no worker action table installed")]
    MissingRegistration { transport: SessionTransportId },
    #[error("transport {transport:?} already has a worker action table installed")]
    AlreadyRegistered { transport: SessionTransportId },
    #[error("session {session_id:?} is not present on this worker")]
    InvalidSession { session_id: SessionId },
    #[error("transport worker action `{action}` failed")]
    TransportActionFailed {
        action: &'static str,
        #[source]
        source: RuntimeError,
    },
}

pub use hammer_runtime::app::SessionConnectError;

#[hammer_component_macros::runtime_error(subsystem = "session")]
#[derive(Debug, Error)]
pub enum SessionError {
    #[error("session {session_id:?} is not in the session pool")]
    SessionMissing { session_id: SessionId },
    #[error("session {lower:?} already has an upper Session attached")]
    UpperSessionAlreadyAttached { lower: SessionId },
    #[error("session {session_id:?} cannot publish its connection in its current state")]
    PublicationRejected { session_id: SessionId },
    #[error("session {session_id:?} is active and cannot be rolled back")]
    RollbackRejected { session_id: SessionId },
    #[error("transport Session {session_id:?} construction did not complete")]
    TransportSessionCreateIncomplete { session_id: SessionId },
    #[error("session {session_id:?} connection is not published")]
    NotPublished { session_id: SessionId },
    #[error(
        "session {session_id:?} out-of-order RX offset {offset} plus buffered length {buffered_len} overflows u32"
    )]
    RxOutOfOrderOffsetOverflow {
        session_id: SessionId,
        offset: u32,
        buffered_len: u32,
    },
    #[error("session {session_id:?} out-of-order RX enqueue failed at offset {offset}")]
    RxOutOfOrderEnqueue {
        session_id: SessionId,
        offset: u32,
        #[source]
        source: FifoError,
    },
    #[error(
        "session {session_id:?} transport TX offset {tx_offset} exceeds pending length {available}"
    )]
    TxOffsetOutOfRange {
        session_id: SessionId,
        tx_offset: usize,
        available: usize,
    },
    #[error("session {session_id:?} TX FIFO has no {payload_len} bytes at offset {tx_offset}")]
    TxFifoRangeInvalid {
        session_id: SessionId,
        tx_offset: usize,
        payload_len: usize,
    },
    #[error("session {session_id:?} RX accounting exceeds u32")]
    RxLengthOverflow { session_id: SessionId },
    #[error(
        "session {session_id:?} datagram payload length {payload_len} does not match header length {header_len}"
    )]
    DatagramLengthMismatch {
        session_id: SessionId,
        payload_len: usize,
        header_len: u32,
    },
    #[error("session {session_id:?} datagram FIFO reservation failed")]
    DatagramFifo {
        session_id: SessionId,
        #[source]
        source: FifoError,
    },
    #[error("session {session_id:?} accepted OOO delivery reported no retained span")]
    OooSpanMissing { session_id: SessionId },
    #[error("session {session_id:?} accepted OOO delivery reported an invalid span")]
    OooSpanInvalid { session_id: SessionId },
    #[error("Session listener state is unavailable on this Data Worker")]
    ListenerMainMissing,
    #[error("Session has no data workers configured")]
    NoDataWorkers,
    #[error("Session listener {listener:?} is not registered")]
    ListenerMissing { listener: SessionHandle },
    #[error("Session listener control is owned by another thread")]
    ListenerControlWrongThread,
    #[error("Session transport `{transport}` does not register listener operations")]
    TransportListenUnsupported { transport: &'static str },
    #[error("Session transport `{transport}` does not register active-open")]
    TransportConnectUnsupported { transport: &'static str },
    #[error("Session transport `{transport}` does not register stream active-open")]
    TransportConnectStreamUnsupported { transport: &'static str },
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
        session_id: SessionId,
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
            SessionError::ListenerMainMissing => Self::SessionMainUnavailable,
            SessionError::ListenerMissing { .. } => Self::ListenerMissing,
            SessionError::ListenerControlWrongThread => Self::ApplicationControlWrongThread,
            SessionError::TransportListenUnsupported { .. } => Self::TransportListenUnsupported,
            SessionError::TransportConnectUnsupported { .. } => Self::TransportConnectUnsupported,
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

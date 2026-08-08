use hammer_core::data_plane::NodeId;
use hammer_infra::fifo::FifoError;
use hammer_runtime::app::{ApplicationId, ApplicationSessionStatus};
use hammer_runtime::{RuntimeError, SessionListenerId};
use thiserror::Error;

use super::SessionId;

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

/// One final active-connect failure before a Session exists.
///
/// This is the Session-owned error category at the transport-to-Session seam.
/// Quinn and other transport errors are classified by the owning plugin and
/// translated once into this type; the wire status remains
/// [`ApplicationSessionStatus`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SessionConnectError {
    #[error("TLS alert {alert}")]
    TlsAlert { alert: u8 },
    #[error("QUIC version is unsupported")]
    QuicVersionUnsupported,
    #[error("QUIC handshake timed out")]
    TimedOut,
    #[error("the peer refused the connection")]
    ConnectionRefused,
    #[error("the peer reset the connection")]
    ConnectionReset,
    #[error("the peer closed the connection with code {code}")]
    PeerClosed { code: u64 },
    #[error("QUIC transport error {code}")]
    QuicTransportError { code: u64 },
    #[error("local QUIC connection resources are exhausted")]
    LocalResourceExhausted,
    #[error("the local connection closed during the handshake")]
    LocalClosed,
}

impl From<SessionConnectError> for ApplicationSessionStatus {
    fn from(error: SessionConnectError) -> Self {
        match error {
            SessionConnectError::TlsAlert { alert } => Self::TlsAlert { alert },
            SessionConnectError::QuicVersionUnsupported => Self::QuicVersionUnsupported,
            SessionConnectError::TimedOut => Self::HandshakeTimedOut,
            SessionConnectError::ConnectionRefused => Self::ConnectionRefused,
            SessionConnectError::ConnectionReset => Self::ConnectionReset,
            SessionConnectError::PeerClosed { code } => Self::PeerClosed { code },
            SessionConnectError::QuicTransportError { code } => Self::QuicTransportError { code },
            SessionConnectError::LocalResourceExhausted => Self::LocalConnectionResourceExhausted,
            SessionConnectError::LocalClosed => Self::LocalConnectionClosed,
        }
    }
}

#[hammer_component_macros::runtime_error(subsystem = "session")]
#[derive(Debug, Error)]
pub(crate) enum SessionError {
    #[error("session pool capacity {capacity} is exhausted")]
    CapacityExhausted { capacity: usize },
    #[error("session {session_id:?} is not in the session pool")]
    SessionMissing { session_id: SessionId },
    #[error("session {session_id:?} cannot publish its connection in its current state")]
    PublicationRejected { session_id: SessionId },
    #[error("session {session_id:?} is active and cannot be rolled back")]
    RollbackRejected { session_id: SessionId },
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
    #[error("Session listener {listener:?} is not registered")]
    ListenerMissing { listener: SessionListenerId },
    #[error("Session listener capacity {capacity} is exhausted")]
    ListenerCapacityExhausted { capacity: usize },
    #[error("Session listener control is owned by another thread")]
    ListenerControlWrongThread,
    #[error("Session transport `{transport}` does not register listener operations")]
    TransportListenUnsupported { transport: &'static str },
    #[error("Session transport `{transport}` does not register active-open")]
    TransportConnectUnsupported { transport: &'static str },
    #[error("Session {session_id:?} connect publication failed and its cleanup failed")]
    ConnectPublicationCleanup {
        session_id: SessionId,
        #[source]
        publication: RuntimeError,
        cleanup: RuntimeError,
    },
}

#[cfg(test)]
mod tests {
    use hammer_runtime::RuntimeError;

    use super::{SessionError, SessionQueueError};

    #[test]
    fn runtime_conversion_preserves_session_queue_source() {
        let error: RuntimeError = SessionQueueError::NodeMissing.into();
        let RuntimeError::Subsystem { subsystem, source } = error else {
            panic!("session queue conversion must use the runtime subsystem seam");
        };

        assert_eq!(subsystem, "session queue");
        assert!(matches!(
            source.downcast_ref::<SessionQueueError>(),
            Some(SessionQueueError::NodeMissing)
        ));
        assert!(source.source().is_none());
    }

    #[test]
    fn runtime_conversion_preserves_session_source() {
        let error: RuntimeError = SessionError::ListenerMainMissing.into();
        let RuntimeError::Subsystem { subsystem, source } = error else {
            panic!("session conversion must use the runtime subsystem seam");
        };

        assert_eq!(subsystem, "session");
        assert!(matches!(
            source.downcast_ref::<SessionError>(),
            Some(SessionError::ListenerMainMissing)
        ));
        assert!(source.source().is_none());
    }
}

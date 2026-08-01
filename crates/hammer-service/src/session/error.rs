use hammer_core::data_plane::NodeId;
use hammer_infra::fifo::FifoError;
use hammer_runtime::app::ApplicationId;
use hammer_runtime::{SessionConnectionId, SessionListenerId};
use thiserror::Error;

use super::SessionId;

#[hammer_component_macros::runtime_error(subsystem = "session queue")]
#[derive(Debug, Error)]
#[repr(u16)]
pub enum SessionQueueError {
    #[error("dispatch failed")]
    DispatchFailed,
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
}

impl SessionQueueError {
    #[inline(always)]
    pub const fn code(&self) -> u16 {
        match self {
            Self::DispatchFailed => 0,
            Self::NodeMissing => 1,
            Self::WorkerUnavailable { .. } => 2,
            Self::WorkerOutOfRange { .. } => 3,
            Self::WorkerAlreadyInstalled { .. } => 4,
            Self::WorkerAccess { .. } => 5,
            Self::OutputMissing { .. } => 6,
            Self::ApplicationMqAlreadyRegistered { .. } => 7,
            Self::ApplicationMqMissing { .. } => 8,
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
    #[error("session {session_id:?} accepted OOO delivery reported no retained span")]
    OooSpanMissing { session_id: SessionId },
    #[error("session {session_id:?} accepted OOO delivery reported an invalid span")]
    OooSpanInvalid { session_id: SessionId },
    #[error(
        "Application connection {connection:?} requires an Application Main on this Data Worker"
    )]
    ApplicationMainMissingForConnection { connection: SessionConnectionId },
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
}

#[cfg(test)]
mod tests {
    use std::error::Error;

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

use hammer_core::data_plane::NodeId;
use hammer_infra::fifo::FifoError;
use hammer_runtime::RuntimeError;
use hammer_runtime::SessionListenerId;
use hammer_runtime::app::ApplicationConnectionId;
use thiserror::Error;

use super::SessionId;

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
    #[error("session queue attachment slot {slot} is not registered")]
    AttachmentSlotMissing { slot: usize },
    #[error("session queue attachment registry is already borrowed")]
    AttachmentRegistryBorrowed,
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
            Self::AttachmentSlotMissing { .. } => 7,
            Self::AttachmentRegistryBorrowed => 8,
        }
    }
}

impl From<SessionQueueError> for RuntimeError {
    fn from(source: SessionQueueError) -> Self {
        Self::subsystem("session queue", source)
    }
}

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
    ApplicationMainMissingForConnection { connection: ApplicationConnectionId },
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
}

impl From<SessionError> for RuntimeError {
    fn from(source: SessionError) -> Self {
        Self::subsystem("session", source)
    }
}

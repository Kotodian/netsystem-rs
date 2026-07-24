use thiserror::Error;

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
        }
    }
}

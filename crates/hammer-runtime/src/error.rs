use hammer_core::error::DataPlaneError;
use thiserror::Error;

/// Failures owned by graph execution, process lifecycle, and plugin loading.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    DataPlane(#[from] DataPlaneError),
    #[error("parse TOML: {message}")]
    ConfigParse { message: String },
    #[error("invalid runtime configuration: {message}")]
    ConfigValidation { message: String },
    #[error("{stage}: {message}")]
    Lifecycle { stage: String, message: String },
    #[error("service closed")]
    ServiceClosed,
    #[error("memory initialization has not completed")]
    MemoryNotInitialized,
    #[error("File registry is full")]
    FilePoolFull,
    #[error("File index {index:?} is stale or not registered")]
    FileIndexInvalid { index: hammer_infra::pool::Index },
    #[error("read File descriptor")]
    FileRead {
        #[source]
        source: std::io::Error,
    },
    #[error("write File descriptor")]
    FileWrite {
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    MainHeap(#[from] hammer_infra::main_heap::MainHeapError),
    #[error(transparent)]
    Plugin(#[from] crate::plugin::PluginError),
    #[error("worker count {count} does not fit u32")]
    WorkerCountOverflow { count: usize },
    #[error("a worker graph update is already pending")]
    WorkerGraphUpdateAlreadyPending,
    #[error("the pending worker graph is missing")]
    WorkerGraphUpdateMissing,
    #[error("worker graph update state is poisoned")]
    WorkerGraphUpdateStatePoisoned,
    #[error("worker graph update is not additive")]
    WorkerGraphUpdateNotAdditive,
    #[error(transparent)]
    Attach(#[from] AttachError),
    #[error("{subsystem} subsystem failed")]
    Subsystem {
        subsystem: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("runtime invariant violated: {detail}")]
    Invariant { detail: String },
}

#[derive(Debug, Error)]
pub enum AttachError {
    #[error("failed to create attach signal pipe")]
    SignalPipeCreate {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read attach signal status flags")]
    SignalStatusFlags {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set attach signal nonblocking status")]
    SignalNonblocking {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read attach signal descriptor flags")]
    SignalDescriptorFlags {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set attach signal close-on-exec")]
    SignalCloseOnExec {
        #[source]
        source: std::io::Error,
    },
    #[error("attach control buffer has no first header")]
    ControlHeaderMissing,
    #[error("failed to send attach descriptors")]
    Send {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind attach server at {path}")]
    Bind {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("attach RX FIFO configuration is invalid")]
    RxFifoInvalid,
    #[error("attach TX FIFO configuration is invalid")]
    TxFifoInvalid,
    #[error("attach event queue configuration is invalid")]
    EventQueueInvalid,
    #[error("attach TX event queue configuration is invalid")]
    TxEventQueueInvalid,
    #[error("failed to accept attach client")]
    Accept {
        #[source]
        source: std::io::Error,
    },
    #[error("attach segment has no backing descriptor")]
    SegmentDescriptorMissing,
    #[error("failed to duplicate remote app session signal descriptor")]
    SessionSignalDuplicate {
        #[source]
        source: std::io::Error,
    },
}

impl RuntimeError {
    pub fn config_parse(message: impl Into<String>) -> Self {
        Self::ConfigParse {
            message: message.into(),
        }
    }
    pub fn config_validation(message: impl Into<String>) -> Self {
        Self::ConfigValidation {
            message: message.into(),
        }
    }
    pub fn lifecycle(stage: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Lifecycle {
            stage: stage.into(),
            message: message.into(),
        }
    }
    pub const fn service_closed() -> Self {
        Self::ServiceClosed
    }
    pub fn subsystem(
        subsystem: &'static str,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::Subsystem {
            subsystem,
            source: Box::new(source),
        }
    }
    pub fn invariant(detail: impl Into<String>) -> Self {
        Self::Invariant {
            detail: detail.into(),
        }
    }
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

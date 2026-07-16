use std::path::PathBuf;
use thiserror::Error;

/// Error type used by foundational layers (config, log, lifecycle, runtime
/// registry). `HammerError` is a public alias for this type so downstream crates
/// share one workspace error boundary.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("parse TOML: {message}")]
    ConfigParse { message: String },

    #[error("{message}")]
    ConfigValidation { message: String },

    #[error("{stage}: {message}")]
    Lifecycle { stage: String, message: String },

    #[error("service closed")]
    ServiceClosed,

    #[error("memory initialization has not completed")]
    MemoryNotInitialized,

    #[error(transparent)]
    MainHeap(#[from] hammer_infra::main_heap::MainHeapError),

    #[error("duplicate plugin roots at indexes {first} and {duplicate}")]
    PluginDuplicateRoot { first: usize, duplicate: usize },

    #[error("plugin dependency cycle while loading `{path}`")]
    PluginDependencyCycle { path: PathBuf },

    #[error("host plugin version is invalid")]
    PluginHostVersionInvalid,

    #[error("required plugin version is invalid")]
    PluginRequiredVersionInvalid,

    #[error("host plugin version does not satisfy the plugin requirement")]
    PluginSemVerMismatch,

    #[error("failed to open plugin `{path}`")]
    PluginLibraryOpen { path: PathBuf },

    #[error("failed to read the registration symbol from plugin `{path}`")]
    PluginRegistrationSymbol { path: PathBuf },

    #[error("plugin `{path}` returned a null registration")]
    PluginRegistrationNull { path: PathBuf },

    #[error("plugin `{path}` exported a mismatched registration name")]
    PluginNameMismatch { path: PathBuf },

    #[error("failed to resolve the daemon executable path")]
    PluginExecutablePath {
        #[source]
        source: std::io::Error,
    },

    #[error("daemon executable `{executable}` has no parent directory")]
    PluginExecutableParentMissing { executable: PathBuf },

    #[error("worker count {count} does not fit u32")]
    WorkerCountOverflow { count: usize },

    #[error("a worker graph update is already pending")]
    WorkerGraphUpdateAlreadyPending,

    #[error("the pending worker graph is missing")]
    WorkerGraphUpdateMissing,

    #[error("worker graph update state is poisoned")]
    WorkerGraphUpdateStatePoisoned,

    #[error("failed to create attach signal pipe")]
    AttachSignalPipeCreate {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read attach signal status flags")]
    AttachSignalStatusFlags {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set attach signal nonblocking status")]
    AttachSignalNonblocking {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read attach signal descriptor flags")]
    AttachSignalDescriptorFlags {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set attach signal close-on-exec")]
    AttachSignalCloseOnExec {
        #[source]
        source: std::io::Error,
    },
    #[error("attach control buffer has no first header")]
    AttachControlHeaderMissing,
    #[error("failed to send attach descriptors")]
    AttachSend {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind attach server at {path}")]
    AttachBind {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("attach RX FIFO configuration is invalid")]
    AttachRxFifoInvalid,
    #[error("attach TX FIFO configuration is invalid")]
    AttachTxFifoInvalid,
    #[error("attach event queue configuration is invalid")]
    AttachEventQueueInvalid,
    #[error("attach TX event queue configuration is invalid")]
    AttachTxEventQueueInvalid,
    #[error("failed to accept attach client")]
    AttachAccept {
        #[source]
        source: std::io::Error,
    },
    #[error("attach segment has no backing descriptor")]
    AttachSegmentDescriptorMissing,
    #[error("failed to connect to attach server at {path}")]
    AttachConnect {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to receive attach descriptors")]
    AttachReceive {
        #[source]
        source: std::io::Error,
    },
    #[error("attach metadata length mismatch: expected {expected}, got {actual}")]
    AttachMetadataLength { expected: usize, actual: usize },
    #[error("attach descriptor control data was truncated")]
    AttachControlTruncated,
    #[error("attach message contained unexpected control data")]
    AttachUnexpectedControl,
    #[error("attach descriptor control header is invalid")]
    AttachInvalidControlHeader,
    #[error("attach descriptor payload is invalid")]
    AttachInvalidDescriptorPayload,
    #[error("attach descriptor count mismatch: expected {expected}, got {actual}")]
    AttachDescriptorCount { expected: usize, actual: usize },
    #[error("failed to read received attach descriptor flags")]
    AttachReceivedDescriptorFlags {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to set received attach descriptor close-on-exec")]
    AttachReceivedDescriptorCloseOnExec {
        #[source]
        source: std::io::Error,
    },
    #[error("attach offsets exceed the mapped address range")]
    AttachOffsetOverflow,
    #[error("failed to map the attached SVM segment")]
    AttachSegmentMap {
        #[source]
        source: std::io::Error,
    },
    #[error("remote app session requires a signal-read descriptor")]
    AttachSessionSignalMissing,
    #[error("failed to duplicate remote app session signal descriptor")]
    AttachSessionSignalDuplicate {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to register remote app session readiness")]
    AttachSessionReadiness {
        #[source]
        source: std::io::Error,
    },

    #[error(transparent)]
    Tcp(#[from] crate::protocol::tcp::TcpError),

    #[error(transparent)]
    DataPlane(#[from] DataPlaneError),

    #[error("{message}")]
    Internal { message: String },
}

#[derive(Debug, Error)]
pub enum DataPlaneError {
    #[error("buffer frame capacity exceeded")]
    BufferFrameCapacityExceeded,

    #[error("frame pool exhausted")]
    FramePoolExhausted,

    #[error("frame slot is checked out")]
    FrameSlotCheckedOut,

    #[error(
        "index belongs to another pool: expected pool {expected_pool_id}, got pool {actual_pool_id}"
    )]
    ForeignIndex {
        expected_pool_id: u64,
        actual_pool_id: u64,
    },

    #[error(
        "stale index: slot {slot} generation {index_generation} != current {current_generation}"
    )]
    StaleIndex {
        slot: u32,
        index_generation: u32,
        current_generation: u32,
    },

    #[error("index slot {slot} out of bounds for pool {pool_id}")]
    IndexSlotOutOfBounds { pool_id: u64, slot: u32 },

    #[error("index slot {slot} is free in pool {pool_id}")]
    IndexSlotFree { pool_id: u64, slot: u32 },

    #[error("frame slot already has a frame")]
    FrameSlotAlreadyHasFrame,

    #[error("frame pool available-list overflow")]
    FramePoolAvailableOverflow,

    #[error("scheduled frame queue exhausted")]
    ScheduledFrameQueueExhausted,

    #[error("data plane handoff target worker out of bounds")]
    HandoffTargetWorkerOutOfBounds,

    #[error("data plane handoff queue exhausted")]
    HandoffQueueExhausted,

    #[error("data plane handoff is not configured")]
    HandoffNotConfigured,

    #[error("data plane handoff node handle is not configured")]
    HandoffNodeHandleMissing,

    #[error("named next fallback node is not registered")]
    NamedNextFallbackMissing,

    #[error("configured data-plane instruction set is unsupported")]
    UnsupportedDataPlaneInstructionSet,

    #[error("constructor-published graph registration is unnamed")]
    UnnamedGraphRegistration,

    #[error("active NUMA buffer pool is missing")]
    ActiveNumaBufferPoolMissing,

    #[error("NUMA node {numa_node} does not fit usize")]
    NumaNodeDoesNotFitUsize { numa_node: u32 },

    #[error("NUMA node {numa_node} exceeds static memory table capacity {capacity}")]
    NumaNodeExceedsStaticMemoryTable { numa_node: u32, capacity: usize },

    #[error("duplicate NUMA memory entry for node {numa_node}")]
    DuplicateNumaMemoryEntry { numa_node: u32 },

    #[error("no static buffer arena configured for thread {thread_index} on NUMA node {numa_node}")]
    StaticBufferArenaMissing { thread_index: u32, numa_node: u32 },
}

impl CoreError {
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

    pub fn service_closed() -> Self {
        Self::ServiceClosed
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

pub type CoreResult<T> = Result<T, CoreError>;

// Backwards-compatible aliases so M1 source layouts keep building during the
// migration window. New code should use `CoreError` / `CoreResult` directly.
pub type HammerError = CoreError;
pub type HammerResult<T> = CoreResult<T>;

/// Attach a string context to any `Result<T, E>` where `E: Display`,
/// returning a `CoreError::Internal` whose message is `"{ctx}: {source}"`.
/// Replaces the recurring
/// `.map_err(|err| CoreError::internal(format!("xxx: {err}")))` pattern that
/// shows up ~116 times across `hammer-runtime`.
///
/// The context closure is only invoked on the error path, so success-path
/// callers pay no allocation cost.
///
/// # Example
///
/// ```
/// use hammer_core::error::{CoreError, WithContext};
///
/// fn open(path: &str) -> Result<(), CoreError> {
///     std::fs::File::open(path).with_context(|| format!("open {path}"))?;
///     Ok(())
/// }
///
/// let err = open("/no/such/file").unwrap_err();
/// assert!(err.to_string().starts_with("open /no/such/file: "));
/// ```
pub trait WithContext<T> {
    fn with_context<F, S>(self, context: F) -> CoreResult<T>
    where
        F: FnOnce() -> S,
        S: std::fmt::Display;
}

impl<T, E: std::fmt::Display> WithContext<T> for Result<T, E> {
    fn with_context<F, S>(self, context: F) -> CoreResult<T>
    where
        F: FnOnce() -> S,
        S: std::fmt::Display,
    {
        self.map_err(|err| CoreError::internal(format!("{}: {err}", context())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_context_passes_through_ok_without_invoking_closure() {
        let mut called = false;
        let result: Result<i32, std::io::Error> = Ok(42);
        let mapped = result.with_context(|| {
            called = true;
            "should not run"
        });
        assert!(matches!(mapped, Ok(42)));
        assert!(!called, "closure must be lazy on the success path");
    }

    #[test]
    fn with_context_wraps_error_with_message_prefix() {
        let result: Result<(), &str> = Err("inner failed");
        let err = result.with_context(|| "outer step").unwrap_err();
        assert_eq!(err.to_string(), "outer step: inner failed");
        assert!(matches!(err, CoreError::Internal { .. }));
    }

    #[test]
    fn with_context_accepts_any_display_context() {
        // String, &str, and format!()-derived String all satisfy the
        // `Display` bound, so callers can pick whichever fits.
        let r1: Result<(), &str> = Err("boom");
        let r2: Result<(), &str> = Err("boom");
        let r3: Result<(), &str> = Err("boom");
        assert_eq!(
            r1.with_context(|| "static").unwrap_err().to_string(),
            "static: boom"
        );
        assert_eq!(
            r2.with_context(|| String::from("owned"))
                .unwrap_err()
                .to_string(),
            "owned: boom"
        );
        assert_eq!(
            r3.with_context(|| format!("formatted {}", 7))
                .unwrap_err()
                .to_string(),
            "formatted 7: boom"
        );
    }
}

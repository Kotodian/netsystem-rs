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
    #[error("File poller does not support required operation `{operation}`")]
    FilePollerOperationUnsupported { operation: &'static str },
    #[error("File poller completion queue is full while {operation}")]
    FileCompletionQueueFull { operation: &'static str },
    #[error("File poller multishot probe produced no completion")]
    FilePollerProbeCompletionMissing,
    #[error("File poller submission queue is full")]
    FileSubmissionQueueFull,
    #[error("{operation}")]
    FilePollerIo {
        operation: &'static str,
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
    #[error("plugin `{plugin}` state is not initialized")]
    PluginStateNotInitialized { plugin: &'static str },
    #[error("thread {thread_index} is not a data worker")]
    DataWorkerIdUnavailable { thread_index: u32 },
    #[error("worker configuration cannot change after runtime initialization")]
    WorkerConfigurationAlreadyInitialized,
    #[error("worker thread registry is poisoned")]
    WorkerThreadRegistryPoisoned,
    #[error("data workers are already started")]
    DataWorkersAlreadyStarted,
    #[error("Process Nodes can only start on the main Runtime Engine")]
    ProcessNodesRequireMainEngine,
    #[error("Process Nodes must be controlled by their owner thread")]
    ProcessControlWrongThread,
    #[error("control timer interval must be non-zero")]
    ControlTimerIntervalZero,
    #[error("control thread is stopped")]
    ControlThreadStopped,
    #[error("control command panicked")]
    ControlCommandPanicked,
    #[error("control command was canceled")]
    ControlCommandCanceled,
    #[error("control command timed out")]
    ControlCommandTimedOut,
    #[error("control worker barrier is not configured")]
    ControlBarrierUnavailable,
    #[error("duplicate Process Node `{name}`")]
    DuplicateProcessNode { name: &'static str },
    #[error("data worker {worker:?} does not match Handoff owner {handoff_owner:?}")]
    HandoffWorkerMismatch {
        worker: crate::DataWorkerId,
        handoff_owner: crate::DataWorkerId,
    },
    #[error("Graph Node `{node}` initialization failed")]
    GraphNodeInitialization {
        node: &'static str,
        #[source]
        source: Box<RuntimeError>,
    },
    #[error("packet trace serialization failed")]
    PacketTraceSerialization {
        #[source]
        source: bincode::Error,
    },
    #[error("node error recording requires an active Graph Node dispatch")]
    NodeDispatchContextMissing,
    #[error("Handoff continuation requires an active Graph Node dispatch")]
    HandoffDispatchContextMissing,
    #[error("required runtime capability `{type_name}` is not registered")]
    RuntimeCapabilityMissing { type_name: &'static str },
    #[error("data worker exited before reaching the {phase} barrier")]
    WorkerExitedBeforeStartupBarrier { phase: &'static str },
    #[error("data worker requested exit during initialization")]
    WorkerRequestedExitDuringInitialization,
    #[error("node runtime data value {value} does not fit u64")]
    NodeRuntimeDataOverflow { value: usize },
    #[error("node runtime data word {word} value {value} does not fit usize")]
    NodeRuntimeDataWordOutOfRange { word: usize, value: u64 },
    #[error("graph node next count {count} does not fit a u16 slot")]
    NodeNextCountOverflow { count: usize },
    #[error("named-next registration cannot also supply resolved next nodes")]
    NamedNextWithResolvedTargets,
    #[error("named-next registration requires a declared next-node registration")]
    NamedNextRegistrationKindInvalid,
    #[error("named-next count {actual} does not match declared count {declared}")]
    NamedNextCountMismatch { declared: usize, actual: usize },
    #[error("plain node registration cannot declare {count} initial next nodes")]
    PlainNodeHasInitialNexts { count: usize },
    #[error("sibling node registration cannot declare {count} initial next nodes")]
    SiblingNodeHasInitialNexts { count: usize },
    #[error("initial next count {actual} does not match declared count {declared}")]
    InitialNextCountMismatch { declared: usize, actual: usize },
    #[error("graph node name `{name}` is already registered")]
    NodeNameAlreadyRegistered { name: &'static str },
    #[error("graph node `{node}` references unregistered sibling owner `{owner}`")]
    SiblingOwnerNotRegistered {
        node: &'static str,
        owner: &'static str,
    },
    #[error("graph node {node:?} is not registered")]
    NodeNotRegistered {
        node: hammer_core::data_plane::NodeId,
    },
    #[error("graph node {node:?} next slot {slot} is not registered")]
    NodeNextSlotNotRegistered {
        node: hammer_core::data_plane::NodeId,
        slot: usize,
    },
    #[error("graph node {node:?} next slot {slot} is outside next count {next_count}")]
    NodeNextSlotOutOfRange {
        node: hammer_core::data_plane::NodeId,
        slot: usize,
        next_count: usize,
    },
    #[error("data worker cannot mutate graph topology")]
    GraphTopologyMutationFromWorker,
    #[error("graph node handle {handle:?} is already registered")]
    NodeHandleAlreadyRegistered {
        handle: hammer_core::data_plane::NodeHandle,
    },
    #[error("graph node handle {handle:?} is not registered")]
    NodeHandleNotRegistered {
        handle: hammer_core::data_plane::NodeHandle,
    },
    #[error("graph node index {slot} does not fit u32")]
    NodeIdOverflow { slot: usize },
    #[error("graph node {node:?} is not a driver node")]
    NodeNotDriver {
        node: hammer_core::data_plane::NodeId,
    },
    #[error("node error table exhausted its u16 encoding space")]
    NodeErrorSlotOverflow,
    #[error(transparent)]
    Init(#[from] crate::init::InitError),
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

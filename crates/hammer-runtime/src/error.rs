use hammer_core::error::DataPlaneError;
use thiserror::Error;

/// Failures owned by graph execution, process lifecycle, and plugin loading.
#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    DataPlane(#[from] DataPlaneError),
    #[error("parse TOML: {message}")]
    ConfigParse { message: String },
    #[error("config callback `{function}` failed to parse section `{section}`")]
    ConfigFunctionParse {
        function: &'static str,
        section: &'static str,
        #[source]
        source: toml::de::Error,
    },
    #[error("invalid runtime configuration: {message}")]
    ConfigValidation { message: String },
    #[error("{stage}: {message}")]
    Lifecycle { stage: String, message: String },
    #[error("service closed")]
    ServiceClosed,
    #[error("read startup configuration `{path}`")]
    StartupConfigRead {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("wait for Unix signal `{signal}`")]
    UnixSignal {
        signal: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("global FileMain is already initialized")]
    FileMainAlreadyInitialized,
    #[error("global FileMain is not initialized")]
    FileMainNotInitialized,
    #[error("global FileMain has already been taken")]
    FileMainAlreadyTaken,
    #[error("File index {index:?} is stale or not registered")]
    FileIndexInvalid { index: u32 },
    #[error("deadline registry is full")]
    DeadlinePoolFull,
    #[error("deadline index {index:?} is stale or not registered")]
    DeadlineIndexInvalid { index: u32 },
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
    #[error("accept on File descriptor")]
    FileAccept {
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
    #[error(transparent)]
    Stats(#[from] hammer_stats::StatsError),
    #[error("worker count {count} does not fit u32")]
    WorkerCountOverflow { count: usize },
    #[error("worker count must be non-zero")]
    WorkerCountZero,
    #[error("worker thread stack size must be non-zero")]
    WorkerStackSizeZero,
    #[error("worker blocking thread count must be non-zero")]
    WorkerBlockingThreadCountZero,
    #[error("thread count does not fit u32")]
    ThreadCountOverflow,
    #[error("operating-system CPU inventory is unavailable")]
    CpuInventoryUnavailable,
    #[error("CPU index {cpu} does not fit the runtime identity")]
    CpuIndexOverflow { cpu: usize },
    #[error("configured CPU {cpu} is unavailable")]
    CpuUnavailable { cpu: usize },
    #[error("CPU {cpu} configured for data worker {worker} is unavailable")]
    WorkerCpuUnavailable { worker: usize, cpu: usize },
    #[error("no CPU remains for data worker {worker}")]
    WorkerCpuExhausted { worker: usize },
    #[error("failed to bind runtime thread {thread_index} to CPU {cpu}")]
    WorkerCpuAffinity { thread_index: u32, cpu: usize },
    #[cfg(target_os = "linux")]
    #[error("failed to configure data-worker scheduling policy")]
    WorkerScheduler {
        #[source]
        source: Box<thread_priority::Error>,
    },
    #[cfg(target_os = "macos")]
    #[error("failed to configure data-worker QoS")]
    WorkerQos {
        #[source]
        source: std::io::Error,
    },
    #[error("plugin `{plugin}` state is not initialized")]
    PluginStateNotInitialized { plugin: &'static str },
    #[error("thread {thread_index} is not a data worker")]
    DataWorkerIdUnavailable { thread_index: u32 },
    #[error("control operation must run on GlobalMain")]
    ControlRequiresMainThread,
    #[error("control operation requires the worker barrier while Data Workers are running")]
    ControlRequiresWorkerBarrier,
    #[error("worker configuration cannot change after runtime initialization")]
    WorkerConfigurationAlreadyInitialized,
    #[error("worker configuration field `{field}` is specified more than once via `{alias}`")]
    WorkerConfigurationFieldDuplicate {
        field: &'static str,
        alias: &'static str,
    },
    #[error("failed to parse worker configuration field `{field}`")]
    WorkerConfigurationFieldParse {
        field: &'static str,
        #[source]
        source: toml::de::Error,
    },
    #[error("unknown worker configuration field `{field}`")]
    WorkerConfigurationFieldUnknown { field: String },
    #[error("data workers are already started")]
    DataWorkersAlreadyStarted,
    #[error("runtime thread {thread_index} setup failed")]
    ThreadSetup {
        thread_index: u32,
        #[source]
        source: Box<RuntimeError>,
    },
    #[error("failed to spawn runtime thread {thread_index} for registration `{name}`")]
    ThreadSpawn {
        thread_index: u32,
        name: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to build the Tokio runtime for data worker {worker}")]
    DataWorkerRuntime {
        worker: u32,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to build the Tokio runtime for thread-zero Process Nodes")]
    MainRuntime {
        #[source]
        source: std::io::Error,
    },
    #[error("data worker {worker} exited with status {status}")]
    DataWorkerExited { worker: u32, status: i32 },
    #[error("data worker {worker} callback `{function}` initialization failed")]
    WorkerInitialization {
        worker: u32,
        function: &'static str,
        #[source]
        source: Box<RuntimeError>,
    },
    #[error("thread-zero Process runtime is unavailable")]
    MainProcessRuntimeUnavailable,
    #[error(transparent)]
    AppSession(#[from] crate::app::AppSessionError),
    #[error("Application Session control operation failed")]
    SessionControl {
        #[source]
        source: crate::app::SessionMsgQueueError,
    },
    #[error("Application Session control payload decode failed")]
    SessionControlDecode {
        #[source]
        source: crate::app::SessionControlDecodeError,
    },
    #[error("duplicate Process Node `{name}`")]
    DuplicateProcessNode { name: &'static str },
    #[error("Process Node declaration has no registered name")]
    ProcessNodeNameMissing,
    #[error("Process Node declaration `{name}` has graph kind {kind:?}")]
    ProcessNodeKindInvalid {
        name: &'static str,
        kind: hammer_core::data_plane::NodeKind,
    },
    #[error("Process Node declaration `{name}` has no concrete future constructor")]
    ProcessStartMissing { name: &'static str },
    #[error("Process Node declaration `{name}` has no NodeId storage")]
    ProcessNodeIndexStorageMissing { name: &'static str },
    #[error("Process Node `{name}` has not installed its NodeId")]
    ProcessNodeIdentityUnavailable { name: &'static str },
    #[error("Process Node {node:?} is not registered on thread zero")]
    ProcessNodeNotRegistered {
        node: hammer_core::data_plane::NodeId,
    },
    #[error("Process Node {node:?} is already started")]
    ProcessNodeAlreadyStarted {
        node: hammer_core::data_plane::NodeId,
    },
    #[error("Process event receiver requested outside its constructor step")]
    ProcessConstructorInactive,
    #[error("Process Node {node:?} event receiver is already installed")]
    ProcessEventReceiverAlreadyTaken {
        node: hammer_core::data_plane::NodeId,
    },
    #[error("Process Node {node:?} event queue is closed")]
    ProcessEventQueueClosed {
        node: hammer_core::data_plane::NodeId,
    },
    #[error("Process Node {node:?} task join failed")]
    ProcessTaskJoin {
        node: hammer_core::data_plane::NodeId,
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("Process shutdown failed: {primary}; later Process cleanup also failed: {cleanup}")]
    ProcessShutdownCleanup {
        #[source]
        primary: Box<RuntimeError>,
        cleanup: Box<RuntimeError>,
    },
    #[error("data worker {worker:?} does not match Handoff owner {handoff_owner:?}")]
    HandoffWorkerMismatch {
        worker: crate::DataWorkerId,
        handoff_owner: crate::DataWorkerId,
    },
    #[error("Graph Node `{node}` initialization failed")]
    GraphNodeInitialization {
        node: &'static str,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("packet trace serialization failed")]
    PacketTraceSerialization {
        #[source]
        source: bincode::Error,
    },
    #[error("node error recording requires an active Graph Node dispatch")]
    NodeDispatchContextMissing,
    #[error("required runtime capability `{type_name}` is not registered")]
    RuntimeCapabilityMissing { type_name: &'static str },
    #[error("runtime thread {thread_index} exited before reaching the startup barrier")]
    ThreadExitedBeforeStartupBarrier { thread_index: u32 },
    #[error("data worker requested exit during initialization")]
    WorkerRequestedExitDuringInitialization,
    #[error("system clock is before the Unix epoch")]
    SystemClockBeforeUnixEpoch {
        #[source]
        source: std::time::SystemTimeError,
    },
    #[error("node runtime data value {value} does not fit u64")]
    NodeRuntimeValueOverflow { value: usize },
    #[error("node runtime data word {word} value {value} does not fit usize")]
    NodeRuntimeWordOutOfRange { word: usize, value: u64 },
    #[error("graph node next count {count} does not fit a u16 slot")]
    NodeNextCountOverflow { count: usize },
    #[error(
        "graph edge {node:?} -> {next:?} has next slot {actual}, expected shared slot {expected}"
    )]
    NodeNextSlotMismatch {
        node: hammer_core::data_plane::NodeId,
        next: hammer_core::data_plane::NodeId,
        actual: u16,
        expected: u16,
    },
    #[error("named-next registration cannot also supply resolved next nodes")]
    NamedNextWithResolvedTargets,
    #[error("named-next registration requires a declared next-node registration")]
    NamedNextRegistrationKindInvalid,
    #[error("named-next count {actual} does not match declared count {declared}")]
    NamedNextCountMismatch { declared: usize, actual: usize },
    #[error("unregistered node cannot declare {count} initial next nodes")]
    UnregisteredNodeHasInitialNexts { count: usize },
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
    #[error("graph node {node:?} is not a driver or pre-input node")]
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
}

#[derive(Debug, Error)]
pub enum AttachError {
    #[error("failed to create shared Application Session control segment")]
    ControlSegmentCreate {
        #[source]
        source: std::io::Error,
    },
    #[error("Application Session control segment capacity is exhausted")]
    ControlSegmentCapacity,
    #[error("Application Session control queue layout is invalid")]
    ControlQueueLayout {
        #[source]
        source: crate::app::SessionMsgQueueError,
    },
    #[error("failed to initialize Application Session control queue")]
    ControlQueueInit {
        #[source]
        source: crate::app::SessionMsgQueueError,
    },
    #[error("Application Session control operation failed")]
    SessionControl {
        #[source]
        source: crate::app::SessionMsgQueueError,
    },
    #[error("Application Session ExtConfig request exceeds the fixed chunk capacity")]
    ExtConfigOversized { requested: usize, max: usize },
    #[error("Application Session ExtConfig storage is exhausted")]
    ExtConfigExhausted,
    #[error("Application Session ExtConfig offset is out of range")]
    ExtConfigOffsetOutOfRange,
    #[error(
        "Application Session ExtConfig chunk is not allocated (double free or stale reference)"
    )]
    ExtConfigNotAllocated,
    #[error("Application Session ACCEPTED publication is unavailable")]
    AcceptedPublicationUnavailable,
    #[error("Application Session control queue signal is missing")]
    ControlSignalMissing,
    #[error("failed to duplicate Application Session control queue signal")]
    ControlSignalDuplicate {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to register Application Session control queue signal")]
    ControlSignalRegistration {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to read Application Session control queue signal")]
    ControlSignalRead {
        #[source]
        source: std::io::Error,
    },
    #[error("attach descriptor count {actual} exceeds protocol maximum {max}")]
    DescriptorCountTooLarge { actual: usize, max: usize },
    #[error("Application MQ segment has no backing descriptor")]
    ApplicationMqSegmentMissing,
    #[error("Application MQ publication requires at least one Data Worker")]
    ApplicationMqWorkerCountZero,
    #[error("Application MQ publication has {queues} queues but {offsets} offsets")]
    ApplicationMqQueueCountMismatch { queues: usize, offsets: usize },
    #[error("Application MQ descriptor count exceeds addressable range")]
    ApplicationMqDescriptorCountOverflow,
    #[error("Application MQ descriptor count {actual} exceeds protocol maximum {max}")]
    ApplicationMqDescriptorCountTooLarge { actual: usize, max: usize },
    #[error(
        "Application MQ worker {worker} offset {offset} is outside segment size {segment_size}"
    )]
    ApplicationMqOffsetOutOfRange {
        worker: usize,
        offset: u64,
        segment_size: u64,
    },
    #[error("Application MQ worker {worker} has no write signal descriptor")]
    ApplicationMqWriteSignalMissing { worker: usize },
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
    #[error("attach publication capacity must be non-zero")]
    PublicationCapacityInvalid,
    #[error("attach publication queue is full")]
    PublicationQueueFull,
    #[error("attach publication queue is closed")]
    PublicationQueueClosed,
    #[error("attach server is already running")]
    ServerAlreadyRunning,
    #[error("failed to set attach listener nonblocking status")]
    ListenerNonblocking {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to register attach listener with Tokio")]
    ListenerRegistration {
        #[source]
        source: std::io::Error,
    },
    #[error("failed to bind attach server at {path}")]
    Bind {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to allocate attach session layout")]
    SessionLayout {
        #[source]
        source: hammer_infra::svm::fifo::FifoError,
    },
    #[error("attach RX FIFO configuration is invalid")]
    RxFifoInvalid,
    #[error("attach event queue configuration is invalid")]
    EventQueueInvalid,
    #[error("failed to accept attach client")]
    Accept {
        #[source]
        source: std::io::Error,
    },
    #[error("attach segment has no backing descriptor")]
    SegmentDescriptorMissing,
    #[error("attach session event queue has no read signal descriptor")]
    SessionSignalMissing,
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
}

pub type RuntimeResult<T> = Result<T, RuntimeError>;

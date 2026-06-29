pub mod buffer;
pub mod certificate;
pub mod component;
pub mod connection;
pub mod handoff;
pub mod instruction_set;
pub mod network;
pub mod node;
pub mod platform;
pub mod service;
pub mod trace;
pub mod wakeup;

pub use buffer::{
    BUFFER_CACHE_LINE_SIZE, BUFFER_INVALID_INDEX, Buffer, BufferFlags, BufferFrame,
    BufferFrameBatch, BufferFrameBatchCursor, BufferFrameBatchIndices, BufferFramePairBatch,
    BufferFramePairBatchCursor, BufferFrameQuadBatch, BufferFrameQuadBatchCursor,
    BufferHeaderCacheline0, BufferHeaderCacheline1, BufferIndex, BufferNodeError,
    BufferPacketCursor, BufferPool, BufferPoolArena, DataPlaneBuffers, DataPlaneRuntime,
    FrameIndex, FramePool, FrameRef, FrameRefMut, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES,
    PooledBufferFrame, PrimaryOpaque, SecondaryOpaque,
};
pub use hammer_core::protocol::icmp::IcmpErrorMetadata;
pub use hammer_core::{Network, SocksAddr};
pub use hammer_infra::hint::unlikely;
pub use handoff::{DataPlaneHandoff, DataPlaneHandoffWorker, DataWorkerId};
pub use instruction_set::{DataPlaneInstructionSet, FrameBatchWidth};

// Re-exports used by the runtime crate so it doesn't have to know which
// sub-module each trait lives in.
pub use certificate::CertificateProviderService;
pub use component::{
    AsAnyComponent, ComponentMeta, ComponentMetadata, ComponentMetricsMeta, RuntimeComponent,
};
pub use connection::ConnectionHandle;
pub use network::NetworkManager;
pub use node::{
    DriverNode, InternalNode, NextFrame, Node, NodeDescriptor, NodeEntry, NodeErrorCounters,
    NodeHandle, NodeId, NodeKind, NodeNext, NodeNextFrames, NodeNextStorage, NodeProcessFn,
    NodeRegistration, NodeResult, NodeRuntime, NodeRuntimeData, NodeRuntimeReady, NodeState,
    NoopNode, default_prefetch_indices,
};
pub use platform::{
    DefaultInterfaceUpdateListener, NetworkInterface, PlatformInterface, TunOptions, WifiState,
};

pub use trace::{
    PacketTrace, TraceControlHandle, TraceControlPlane, TraceEntry, TraceFormatter,
    TraceInputPolicy, TracePolicy, TraceRecord, TraceRecordSink,
};

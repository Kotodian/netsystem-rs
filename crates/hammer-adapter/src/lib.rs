pub mod buffer;
pub mod certificate;
pub mod component;
pub mod connection;
pub mod dialer;
pub mod endpoint;
pub mod handler;
pub mod handoff;
pub mod inbound;
pub mod instruction_set;
pub mod network;
pub mod node;
pub mod outbound;
pub mod packet_buffer;
pub mod platform;
pub mod probe;
pub mod service;
pub mod trace;

pub use buffer::{
    BUFFER_CACHE_LINE_SIZE, Buffer, BufferBatchMut, BufferFlags, BufferFrame, BufferFrameBatch,
    BufferFrameBatchCursor, BufferFrameBatchIndices, BufferFramePairBatch,
    BufferFramePairBatchCursor, BufferFrameQuadBatch, BufferFrameQuadBatchCursor, BufferIndex,
    BufferNodeError, BufferPacketCursor, BufferPool, BufferPoolArena, BufferRef, BufferRefMut,
    DataPlaneBuffers, DataPlaneRuntime, FrameIndex, FramePool, FrameRef, FrameRefMut,
    PooledBufferFrame,
};
pub use hammer_core::lifecycle::{
    ALL_STAGES, LIFECYCLE_ORDER, Lifecycle, LifecycleService, StartStage,
};
pub use hammer_core::protocol::icmp::IcmpErrorMetadata;
pub use hammer_infra::hint::unlikely;
pub use handoff::{DataPlaneHandoff, DataPlaneHandoffWorker, DataWorkerId};
pub use instruction_set::{DataPlaneInstructionSet, FrameBatchWidth};

// Re-exports used by the runtime crate so it doesn't have to know which
// sub-module each trait lives in.
pub use certificate::{CertificateProviderManager, CertificateProviderService, CertificateStore};
pub use component::{
    AsAnyComponent, ComponentMeta, ComponentMetadata, ComponentMetricsMeta, RuntimeComponent,
};
pub use connection::{ConnectionHandle, ConnectionManager};
pub use dialer::{Dialer, Network};
pub use endpoint::{Endpoint, EndpointComponent, EndpointLocalFlow, EndpointManager};
pub use hammer_core::SocksAddr;
pub use handler::{ConnectionHandler, PacketConnectionHandler};
pub use inbound::{Inbound, InboundComponent, InboundManager};
pub use network::NetworkManager;
pub use node::{
    DriverNode, InternalNode, NextFrame, Node, NodeDescriptor, NodeErrorCounters, NodeHandle,
    NodeId, NodeKind, NodeNext, NodeNextEnqueue, NodeNextFrames, NodeNextStorage,
    NodeNextVectorEnqueue, NodeProcessFn, NodeRegistration, NodeResult, NodeRuntime,
    NodeRuntimeData, NodeRuntimeReady, NodeState, NodeVectorDispatch, NoopNode,
    default_prefetch_indices,
};
pub use outbound::{
    IcmpReply, Outbound, OutboundComponent, OutboundManager, ProxyIcmpConn, ProxyPacketConn,
    ProxyStream,
};
pub use packet_buffer::{
    ForwardingMetadata, IpEcnCodepoint, NetworkOpaque, NetworkPayloadOpaque,
    PACKET_BUFFER_INVALID_INDEX, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES,
    PacketBufferCacheline0, PacketBufferCacheline1, PacketBufferFlags, PrimaryOpaque,
    SecondaryOpaque, TapEthernetMetadata,
};
pub use platform::{
    DefaultInterfaceUpdateListener, NetworkInterface, PlatformInterface, TunOptions, WifiState,
};
pub use probe::{ProbeProtocol, ProbeProtocolComponent, ProbeReport};
pub use service::ServiceManager;
pub use trace::{
    PacketTrace, TraceControlHandle, TraceControlPlane, TraceEntry, TraceFormatter,
    TraceInputPolicy, TracePolicy, TraceRecord, TraceRecordSink,
};

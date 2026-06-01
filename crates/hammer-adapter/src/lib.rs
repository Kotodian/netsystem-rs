pub mod buffer;
pub mod certificate;
pub mod component;
pub mod connection;
pub mod dialer;
pub mod dns;
pub mod endpoint;
pub mod handler;
pub mod handoff;
pub mod inbound;
pub mod instruction_set;
pub mod network;
pub mod node;
pub mod outbound;
pub mod platform;
pub mod probe;
pub mod router;
pub mod rule;
pub mod service;

pub use buffer::{
    BUFFER_CACHE_LINE_SIZE, Buffer, BufferFlags, BufferFrame, BufferFrameBatchIndices,
    BufferFramePairBatch, BufferFramePairBatchCursor, BufferFrameQuadBatch,
    BufferFrameQuadBatchCursor, BufferIndex, BufferNodeError, BufferPacketCursor, BufferPool,
    BufferPoolArena, BufferRef, BufferRefMut, DataPlaneBuffers, DataPlaneRuntime, FrameIndex,
    FramePool, FrameRef, FrameRefMut, PooledBufferFrame,
};
pub use hammer_core::lifecycle::{
    ALL_STAGES, LIFECYCLE_ORDER, Lifecycle, LifecycleService, StartStage,
};
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
pub use dns::{
    DnsQueryOptions, DnsRouter, DnsTransport, DnsTransportComponent, DnsTransportManager,
};
pub use endpoint::{Endpoint, EndpointComponent, EndpointLocalFlow, EndpointManager};
pub use handler::{ConnectionHandler, PacketConnectionHandler};
pub use inbound::{Inbound, InboundComponent, InboundManager};
pub use network::NetworkManager;
pub use node::{
    DriverNode, InternalNode, NextFrame, Node, NodeErrorCounters, NodeHandle, NodeId, NodeNext,
    NodeNextFrames, NodeNextGroups, NodeResult, NodeRuntime, NodeRuntimeReady, NoopNode,
};
pub use outbound::{
    IcmpReply, Outbound, OutboundComponent, OutboundManager, ProxyIcmpConn, ProxyPacketConn,
    ProxyStream,
};
pub use platform::{
    DefaultInterfaceUpdateListener, NetworkInterface, PlatformInterface, TunOptions, WifiState,
};
pub use probe::{ProbeProtocol, ProbeProtocolComponent, ProbeReport};
pub use router::Router;
pub use rule::{
    ForwardingDpoType, ForwardingMetadata, HeadlessRule, RouteDecision, RouteMetadata, RouteTarget,
    Rule, SocksAddr,
};
pub use service::ServiceManager;

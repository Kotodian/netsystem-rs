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
pub mod packet_buffer;
pub mod platform;
pub mod probe;
pub mod router;
pub mod rule;
pub mod service;

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
    NodeNextEnqueue, NodeNextFrames, NodeNextStorage, NodeNextVectorEnqueue, NodeResult,
    NodeRuntime, NodeRuntimeReady, NoopNode, PacketNextResolver, process_cached_rewrite_next,
    process_cached_speculative_next,
};
pub use outbound::{
    IcmpReply, Outbound, OutboundComponent, OutboundManager, ProxyIcmpConn, ProxyPacketConn,
    ProxyStream,
};
pub use packet_buffer::{
    NetworkOpaque, NetworkOpaquePayload, NetworkPayloadOpaque, PACKET_BUFFER_INVALID_INDEX,
    PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES, PacketBufferFlags, PacketBufferHeader,
    PacketBufferHeaderExt, PrimaryOpaque, PrimaryOpaquePayload, SecondaryOpaque,
    SecondaryOpaquePayload,
};
pub use platform::{
    DefaultInterfaceUpdateListener, NetworkInterface, PlatformInterface, TunOptions, WifiState,
};
pub use probe::{ProbeProtocol, ProbeProtocolComponent, ProbeReport};
pub use router::Router;
pub use rule::{
    FeaturePathEntry, ForwardingDpoType, ForwardingMetadata, HeadlessRule, RouteDecision,
    RouteMetadata, RouteTarget, Rule, SocksAddr, TapEthernetMetadata,
};
pub use service::ServiceManager;

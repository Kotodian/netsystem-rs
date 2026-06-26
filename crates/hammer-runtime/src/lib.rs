// Re-export adapter types so the macros don't need fully-qualified paths and
// downstream call sites can `use hammer_runtime::adapter::Lifecycle` if they
// prefer the runtime crate's namespace.
pub mod adapter {
    pub use hammer_adapter::*;
}

pub use hammer_core::error::{HammerError, HammerResult};

pub mod app;
mod component_registry;
mod control_thread;
mod data_plane;
pub mod endpoints;
pub mod graph;
pub mod inbounds;
mod macros;
pub mod outbounds;
pub mod protocol;
mod socket_protector;
pub mod spawn;
#[cfg(feature = "endpoint-wireguard")]
pub mod wireguard {
    pub use crate::protocol::endpoint::wireguard::*;
}

pub use component_registry::{
    EventSubscriberComponentDeclaration, register_event_subscriber_component,
};
pub use control_thread::{
    ControlEvent, ControlEventArgs, ControlEventFilter, ControlEventSubscriptionHandle,
    ControlThread, ControlThreadHandle, ControlTimerHandle, EventSubscriberBuilder, LogEventArgs,
};
pub use endpoints::EndpointManager;
pub use hammer_core::{
    MetricCounter, MetricGauge, MetricKind, MetricLabel, MetricSample, MetricsRegistry,
    MetricsScope,
};
pub use inbounds::InboundManager;
pub use outbounds::OutboundManager;
pub use socket_protector::{RuntimePlatform, SocketProtector};
pub use spawn::{DataPlaneBarrierGuard, DataPlaneBarrierHandle};

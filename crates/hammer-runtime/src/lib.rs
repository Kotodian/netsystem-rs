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
pub mod graph;
mod numa;
mod worker_thread;
mod macros;
pub mod protocol;
mod socket_protector;
pub mod spawn;

pub use component_registry::{
    EventSubscriberComponentDeclaration, register_event_subscriber_component,
};
pub use control_thread::{
    ControlEvent, ControlEventArgs, ControlEventFilter, ControlEventSubscriptionHandle,
    ControlThread, ControlThreadHandle, ControlTimerHandle, EventSubscriberBuilder, LogEventArgs,
};
pub use hammer_core::{
    MetricCounter, MetricGauge, MetricKind, MetricLabel, MetricSample, MetricsRegistry,
    MetricsScope,
};
pub use socket_protector::{RuntimePlatform, SocketProtector};
pub use spawn::{DataPlaneBarrierGuard, DataPlaneBarrierHandle};

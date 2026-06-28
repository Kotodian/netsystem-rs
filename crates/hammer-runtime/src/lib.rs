// Re-export adapter types so the macros don't need fully-qualified paths and
// downstream call sites can `use hammer_runtime::adapter::Lifecycle` if they
// prefer the runtime crate's namespace.
pub mod engine;
pub use engine::{Engine, EnginePool};
pub mod barrier;
pub mod init;
pub mod main_loop;

pub mod adapter {
    pub use hammer_adapter::*;
}

pub use hammer_core::error::{HammerError, HammerResult};

pub mod app;
pub mod attach;
mod component_registry;
mod control_thread;
mod data_plane;
pub use data_plane::new_worker_runtime;
pub mod graph;
mod macros;
mod numa;
pub mod protocol;
mod socket_protector;
pub mod spawn;
pub mod start_workers;
mod worker_thread;

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

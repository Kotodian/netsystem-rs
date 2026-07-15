extern crate self as hammer_runtime;

mod registration;

#[doc(hidden)]
pub mod __private {
    pub use crate::registration::RegistrationImage;
}

crate::__declare_registration_image!();

pub mod engine;
pub use engine::{Engine, EnginePool};
pub mod file;
pub use file::{File, FileFunction, FileFunctions, FileMain};
pub mod barrier;
pub mod init;
pub mod main_loop;
pub mod memory;
pub mod plugin;
pub mod plugin_loader;
mod process;

pub use hammer_core::error::{HammerError, HammerResult};
pub use hammer_core::protocol::icmp::IcmpErrorMetadata;
pub use hammer_core::{Network, SocksAddr};
pub use hammer_infra::hint::unlikely;

pub mod app;
pub mod attach;
mod component;
mod control_thread;
pub mod data_plane;
pub mod handoff;
pub mod instruction_set;
pub mod node;
pub mod trace;
pub use data_plane::{DataPlaneRuntime, DataPlaneRuntimeConfig, new_worker_runtime};
pub use handoff::{DataPlaneHandoff, DataPlaneHandoffWorker, DataWorkerId};
pub use instruction_set::{DataPlaneInstructionSet, FrameBatchWidth};
pub use node::{
    DriverNode, InternalNode, Node, NodeDescriptor, NodeEntry, NodeErrorCounters, NodeProcessFn,
    NodeResult, NodeRuntime, NodeRuntimeData, NodeRuntimeReady, NodeRuntimeStatsRow, NoopNode,
    default_prefetch_indices,
};
pub use plugin::{PluginError, PluginMain, PluginRegistration, host_meets_plugin_requirement};
pub use plugin_loader::{
    built_plugin_cdylib_path, built_plugin_path, configured_plugin_path, plugin_cdylib_filename,
    plugin_cdylib_path, read_plugin_registration, workspace_target_dir,
};
pub use process::{
    ProcessContext, ProcessEntry, ProcessEventBatch, ProcessFuture, ProcessHandle, ProcessWake,
};
pub use trace::{
    PacketTrace, TraceControlHandle, TraceControlPlane, TraceEntry, TraceFormatter,
    TraceInputPolicy, TracePolicy, TraceRecord, TraceRecordSink,
};
pub mod graph;

mod numa;
pub mod protocol;
pub mod spawn;
pub mod start_workers;
mod worker_thread;

pub use component::{ComponentMeta, ComponentMetadata, ComponentMetricsMeta};
pub use control_thread::{ControlThread, ControlThreadHandle, ControlTimerHandle};
pub use hammer_core::{
    MetricCounter, MetricGauge, MetricKind, MetricLabel, MetricSample, MetricsRegistry,
    MetricsScope,
};
pub use spawn::{DataPlaneBarrierGuard, DataPlaneBarrierHandle, with_data_plane_runtime};

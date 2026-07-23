extern crate self as hammer_runtime;

mod registration;

#[doc(hidden)]
pub mod __private {
    pub use abi_stable::export_root_module;
    pub use abi_stable::prefix_type::PrefixTypeTrait;
    pub use abi_stable::RRef;
    pub use abi_stable::std_types::{ROption, RSlice, RStr};
    pub use crate::registration::RegistrationImage;
}

crate::__declare_registration_image!(
    init_functions = [graph::install::__INIT_FN_INSTALL_PACKET_GRAPH];
    config_functions = [trace::__CONFIG_FN_RUNTIME_TRACE_CONFIG];
    early_config_functions = [memory::__CONFIG_FN_RUNTIME_WORKER_CONFIG];
    main_loop_enter_functions = [];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [];
);

pub(crate) fn builtin_registration_image() -> &'static registration::RegistrationImage {
    &__HAMMER_REGISTRATION_IMAGE
}

pub mod engine;
pub mod error;
pub use engine::{Engine, EnginePool};
pub mod config;
pub mod file;
pub use file::{File, FileFunction, FileFunctions, FileMain};
pub mod barrier;
pub mod init;
pub mod log;
pub mod main_loop;
pub mod memory;
pub mod metrics;
pub mod plugin;
pub mod plugin_loader;
mod process;
pub mod registry;

pub use error::{AttachError, RuntimeError, RuntimeResult};
pub use hammer_infra::hint::unlikely;

pub mod app;
pub mod attach;
mod control_thread;
pub mod data_plane;
pub mod handoff;
pub mod instruction_set;
pub mod network;
pub mod node;
pub mod trace;
pub use data_plane::{
    DataPlaneBufferConfig, DataPlaneRuntime, DataPlaneRuntimeConfig, new_worker_runtime,
};
pub use handoff::{DataPlaneHandoff, DataPlaneHandoffWorker, DataWorkerId};
pub use hammer_core::data_plane::FrameBatchWidth;
pub use instruction_set::DataPlaneInstructionSet;
pub use metrics::{
    MetricCounter, MetricGauge, MetricKind, MetricLabel, MetricSample, MetricsRegistry,
    MetricsScope, RegistryRecorder,
};
pub use network::{Network, SocksAddr};
pub use node::{
    DriverNode, InternalNode, Node, NodeDescriptor, NodeEntry, NodeErrorCounters, NodeProcessFn,
    NodeResult, NodeRuntime, NodeRuntimeData, NodeRuntimeReady, NodeRuntimeStatsRow, NoopNode,
    default_prefetch_indices,
};
pub use plugin::{
    IpOutput, IpOutput_CTO, PluginError, PluginMain, PluginMetadata, PluginModule,
    PluginModuleRef, host_meets_plugin_requirement,
};
pub use process::{
    ProcessContext, ProcessEntry, ProcessEventBatch, ProcessFuture, ProcessHandle, ProcessWake,
};
pub use registry::RuntimeRegistry;
pub use trace::{
    PacketTrace, TraceControlHandle, TraceControlPlane, TraceEntry, TraceFormatter,
    TraceInputPolicy, TracePolicy, TraceRecord, TraceRecordSink,
};
pub mod graph;

mod numa;
pub mod spawn;
pub mod start_workers;
mod worker_thread;

pub use control_thread::{ControlThread, ControlThreadHandle, ControlTimerHandle};
pub use spawn::{DataPlaneBarrierGuard, DataPlaneBarrierHandle, with_data_plane_runtime};

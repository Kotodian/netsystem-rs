extern crate self as hammer_runtime;

pub mod registration;

#[doc(hidden)]
pub mod __private {
    pub use crate::binary_api::{BinaryApiMethodEntry, BinaryApiMethodReply};
    pub use crate::registration::{RegistrationImage, StatsRegistration};
    pub use abi_stable::RRef;
    pub use abi_stable::export_root_module;
    pub use abi_stable::prefix_type::PrefixTypeTrait;
    pub use abi_stable::std_types::{ROption, RSlice, RStr};
}

crate::__declare_registration_image!(
    init_functions = [
        graph::install::__INIT_FN_INSTALL_PACKET_GRAPH,
        file::__INIT_FN_FILE_MAIN_INIT,
        config::stats::__INIT_FN_STATS_MAIN_INIT,
    ];
    config_functions = [trace::__CONFIG_FN_RUNTIME_TRACE_CONFIG];
    early_config_functions = [
        memory::__CONFIG_FN_RUNTIME_WORKER_CONFIG,
        config::stats::__CONFIG_FN_RUNTIME_STATS_CONFIG,
    ];
    main_loop_enter_functions = [start_workers::__INIT_FN_START_WORKERS];
    main_loop_exit_functions = [];
    worker_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [global_main::__PROCESS_NODE_STATSEG_COLLECTOR_PROCESS];
    binary_api_methods = [];
    stats_registrations = [global_main::__STATS_REGISTRATION_Sys];
);

pub(crate) fn builtin_registration_image() -> &'static registration::RegistrationImage {
    &__HAMMER_REGISTRATION_IMAGE
}

pub mod error;
pub mod global_main;
pub use global_main::{GlobalMain, ensure_main_thread, ensure_main_thread_with_barrier};
pub mod config;
pub mod file;
pub use file::{
    AsyncFileMain, Deadline, DeadlineFunction, FILE_MAIN, File, FileFunction, FileFunctions,
    FileMain,
};

pub mod barrier;
pub mod binary_api;
pub mod init;
pub mod log;
pub mod main_loop;
pub mod memory;
pub mod metrics;
pub mod plugin;
pub mod plugin_loader;
mod process;
pub mod registry;
pub mod session;
pub mod sync;

pub use error::{AttachError, RuntimeError, RuntimeResult};
pub use hammer_infra::hint::unlikely;
pub use hammer_infra::simd::Simd;

pub mod app;
pub mod attach;
mod control_thread;
pub mod data_plane;
pub mod handoff;
pub mod network;
pub mod node;
mod runtime_simd;
pub mod trace;
pub use data_plane::{DataPlaneBufferConfig, DataPlaneMain};
pub use hammer_core::data_plane::FrameBatchWidth;
pub use handoff::{DataPlaneHandoff, DataPlaneHandoffWorker, DataWorkerId};
pub use init::WorkerInitFunction;
pub use metrics::{
    MetricCounter, MetricGauge, MetricKind, MetricLabel, MetricSample, MetricsRegistry,
    MetricsScope, RegistryRecorder,
};
pub use network::{Network, SocksAddr};
pub use node::{
    DriverNode, InternalNode, Node, NodeDescriptor, NodeEntry, NodeErrorCode, NodeErrorDescriptor,
    NodeErrorSeverity, NodeProcessFn, NodeRuntime, NodeRuntimeData, NodeRuntimeReady,
    default_prefetch_indices,
};
pub use plugin::{
    PluginError, PluginMain, PluginMetadata, PluginModule, PluginModuleRef,
    host_meets_plugin_requirement,
};
pub use process::{
    ProcessContext, ProcessEntry, ProcessEventBatch, ProcessFuture, ProcessHandle, ProcessWake,
};
pub use registry::RuntimeRegistry;
pub use session::{SessionConnectEndpoint, SessionListenEndpoint};
pub use trace::{
    PacketTrace, TraceControlHandle, TraceControlPlane, TraceEntry, TraceFormatter,
    TraceInputPolicy, TracePolicy, TraceRecord, TraceRecordSink,
};
pub mod graph;

mod numa;
pub mod spawn;
pub mod start_workers;
mod worker_thread;

pub use barrier::WorkerBarrier;
pub use control_thread::ControlThread;
pub use spawn::{with_data_plane_main, with_data_plane_main_mut};

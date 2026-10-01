extern crate self as hammer_runtime;

pub mod registration;

#[doc(hidden)]
pub mod __private {
    pub use crate::file::record::{File, FileFunctions};
    pub use crate::registration::{RegistrationImage, StatsRegistration};
    pub use abi_stable::RRef;
    pub use abi_stable::export_root_module;
    pub use abi_stable::prefix_type::PrefixTypeTrait;
    pub use abi_stable::std_types::{ROption, RSlice, RStr};
    pub use tokio::task::{JoinHandle, spawn_local};
}

crate::__declare_registration_image!(
    init_functions = [
        cli::__INIT_FN_CLI_MAIN_INIT,
        unix_cli::__INIT_FN_UNIX_CLI_INIT,
        config::stats::__INIT_FN_STATS_MAIN_INIT,
    ];
    config_functions = [
        config::physmem::__CONFIG_FN_RUNTIME_PHYSMEM_CONFIG,
        trace::__CONFIG_FN_RUNTIME_TRACE_CONFIG,
        config::worker::__CONFIG_FN_RUNTIME_CPU_CONFIG,
        config::worker::__CONFIG_FN_RUNTIME_WORKER_CONFIG,
        config::stats::__CONFIG_FN_RUNTIME_STATS_CONFIG,
    ];
    main_loop_enter_functions = [
        start_workers::__INIT_FN_START_WORKERS,
        node_stats::__INIT_FN_INSTALL_NODE_STATS,
    ];
    main_loop_exit_functions = [
        unix_cli::__INIT_FN_UNIX_CLI_EXIT,
        config::stats::__INIT_FN_EXIT_STATS_MAIN,
    ];
    worker_init_functions = [];
    num_workers_change_functions = [];
    api_init_functions = [];
    graph_nodes = [];
    node_functions = [];
    process_nodes = [config::stats::__PROCESS_NODE_STATSEG_COLLECTOR_PROCESS];
    stats_registrations = [
        config::stats::__STATS_REGISTRATION_Sys,
        thread_main::__STATS_REGISTRATION_WorkerThreadCount,
        config::stats::__STATS_REGISTRATION_MainHeapUsage,
        config::stats::__STATS_REGISTRATION_StatSegmentUsage,
        config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_MAIN_HEAP,
        config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_STAT_SEGMENT_HEAP,
        config::stats::__STATS_COLLECT_REGISTRATION_REGISTER_WORKER_MAIN_LOOP,
        data_plane::buffer_stats::__STATS_COLLECT_REGISTRATION_REGISTER_BUFFER_POOLS,
        node_stats::__STATS_COLLECT_REGISTRATION_REGISTER_NODE_STATS,
    ];
    cli_commands = [cli::__CLI_COMMAND_SHOW_VERSION, cli::__CLI_COMMAND_WAIT];
);

pub(crate) fn builtin_registration_image() -> &'static registration::RegistrationImage {
    &__HAMMER_REGISTRATION_IMAGE
}

pub mod error;
pub mod global_main;
pub use global_main::GlobalMain;
pub mod cli;
pub mod config;
pub mod file;
#[cfg(target_os = "linux")]
pub mod unix_cli;
#[cfg(not(target_os = "linux"))]
mod unix_cli {
    #[hammer_component_macros::init_function(name = "unix_cli_init")]
    fn init_unix_cli(_: &mut crate::DataPlaneMain) -> crate::RuntimeResult<()> {
        Ok(())
    }

    #[hammer_component_macros::main_loop_exit_function(name = "unix_cli_exit")]
    fn exit_unix_cli(_: &mut crate::DataPlaneMain) -> crate::RuntimeResult<()> {
        Ok(())
    }
}
#[cfg(target_os = "linux")]
pub use file::AsyncFileMain;
pub use file::{
    Deadline, DeadlineFunction, FILE_MAIN, File, FileFunction, FileFunctions, FileMain,
    FileReadinessMode, WorkerFilePollMode,
};

pub mod barrier;
pub use barrier::WorkerBarrier;
pub mod init;
pub mod log;
pub mod main_loop;
pub mod plugin;
pub mod plugin_loader;
mod process;

pub use error::{RuntimeError, RuntimeResult};
pub use hammer_infra::hint::unlikely;
pub use hammer_infra::simd::Simd;

pub mod data_plane;
pub mod handoff;
pub mod node;
pub(crate) mod node_stats;
mod runtime_simd;
pub mod thread_main;
pub mod trace;
pub mod unix_main;
pub use data_plane::{DataPlaneBufferConfig, DataPlaneMain};
pub use hammer_core::data_plane::FrameBatchWidth;
pub use handoff::{DataPlaneHandoff, DataPlaneHandoffWorker, DataWorkerId};
pub use main_loop::enqueue_main_thread_future;
pub use node::{
    DriverNode, InternalNode, Node, NodeDescriptor, NodeEntry, NodeErrorCode, NodeErrorDescriptor,
    NodeErrorSeverity, NodeMain, NodeProcessFn, NodeRuntime, NodeRuntimeReady,
    default_prefetch_indices,
};
pub use plugin::{
    PluginError, PluginMain, PluginMetadata, PluginModule, PluginModuleRef,
    host_meets_plugin_requirement,
};
pub use process::Process;
pub use thread_main::ThreadMain;
pub use thread_main::interrupt_worker_node;
pub use thread_main::is_current_worker;
pub use thread_main::{ensure_main_thread, ensure_main_thread_with_barrier};
pub use trace::{
    PacketTrace, TraceControlHandle, TraceControlPlane, TraceEntry, TraceFormatter,
    TraceInputPolicy, TracePolicy, TraceRecord, TraceRecordSink,
};
pub use unix_main::UnixMain;
pub mod graph;

mod numa;
pub mod start_workers;
mod worker_thread;
pub use worker_thread::WorkerThread;

#[macro_export]
macro_rules! worker_thread_barrier_sync {
    ($body:block) => {{
        let __worker_barrier_guard = $crate::barrier::__main_sync_guard();
        let __worker_barrier_result = $body;
        drop(__worker_barrier_guard);
        __worker_barrier_result
    }};
    ($main:expr, $body:block) => {{
        let __worker_barrier_guard = $crate::barrier::__sync_guard($main);
        let __worker_barrier_result = $body;
        drop(__worker_barrier_guard);
        __worker_barrier_result
    }};
}
#[cfg(test)]
static BUFFER_MAIN_INIT: std::sync::Once = std::sync::Once::new();

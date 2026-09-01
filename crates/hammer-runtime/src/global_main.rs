use hammer_infra::align::CacheLineAlignMark;
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{StatsConfig, Worker};
use crate::error::{RuntimeError, RuntimeResult};
use crate::process::ProcessContext;
use crate::spawn::DataRemoteLocalQueue;
use crate::{AsyncFileMain, ControlThread, DataPlaneMain, PluginMain, RuntimeRegistry};
use hammer_component_macros::Stats;
use hammer_stats::{StatsMain, Timestamp};

thread_local! {
    static CURRENT_GLOBAL_MAIN: RefCell<Option<*mut GlobalMain>> = const { RefCell::new(None) };
}

#[derive(Stats)]
pub(crate) struct Sys {
    heartbeat: Timestamp,
    last_stats_clear: Timestamp,
    boottime: Timestamp,
}

#[hammer_component_macros::process_node(name = "statseg-collector-process")]
async fn stat_segment_collector_process(mut context: ProcessContext) -> RuntimeResult<()> {
    let sys = context.require::<Sys>()?;
    let stats_main = StatsMain::global()?;
    let config = context.require::<StatsConfig>()?;

    // VPP `stat_segment_collector_process` writes boottime once before its
    // immediate update pass and interval suspend loop.
    let boottime = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| RuntimeError::Lifecycle {
            stage: "stats collector".to_owned(),
            message: format!("read Unix epoch time: {error}"),
        })?
        .as_secs();
    sys.boottime.store(&stats_main, boottime)?;

    loop {
        sys.heartbeat.increment(&stats_main)?;
        let _ = context
            .wait_for_event_or_clock(config.update_interval)
            .await;
    }
}

#[repr(C)]
pub struct GlobalMain {
    cacheline0: CacheLineAlignMark,
    pub(crate) main: DataPlaneMain,
    control_thread: ControlThread,
    pub registry: Arc<RuntimeRegistry>,
    pub(crate) barrier: crate::barrier::WorkerBarrier,
    pub(crate) main_loop_exit_now: Arc<AtomicBool>,
    pub main_loop_exit_status: Arc<Mutex<i32>>,
    pub(crate) memory_initialized: bool,
    pub(crate) worker_config: Worker,
    pub(crate) called_init_functions: HashSet<&'static str>,
    pub(crate) called_early_config_functions: HashSet<&'static str>,
    pub(crate) called_config_functions: HashSet<&'static str>,
    pub(crate) called_main_loop_enter_functions: HashSet<&'static str>,
    pub(crate) called_main_loop_exit_functions: HashSet<&'static str>,
    pub(crate) main_loop_entered: bool,
    pub(crate) publication: Arc<WorkerPublication>,
    pub(crate) workers_updating_graph: Arc<AtomicU32>,
    // VPP `need_vlib_worker_thread_node_runtime_update`: one coalescing graph
    // refork fact consumed by the outermost barrier release.
    worker_graph_refork_pending: AtomicBool,
    main_loop_exit_functions_called: bool,
    worker_threads: Vec<JoinHandle<RuntimeResult<()>>>,
    worker_control_queues: Arc<[DataRemoteLocalQueue]>,
    control_file_main: Option<AsyncFileMain>,
    ipc_listener: Option<tokio::net::TcpListener>,
    closed: bool,
    // Drop after every owner that may retain DSO code or Drop glue. Plugin
    // images themselves remain mapped for the full process lifetime.
    plugin_main: PluginMain,
}

mod control;
mod lifecycle;
mod metadata;
mod plugins;
mod publication;
mod registrations;
mod workers;

pub(crate) use control::thread_panic_message;
pub use control::{ensure_main_thread, ensure_main_thread_with_barrier};
pub(crate) use publication::WorkerPublication;

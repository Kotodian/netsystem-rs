//! Complete additive plugin-loading example.
//!
//! Run with:
//!
//! ```text
//! cargo run -p hammer --example plugin_additive_load
//! ```
//!
//! Pass a plugin directory as the first argument to override the workspace
//! `target/{debug,release}/deps` default.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use hammer_core::config::Config;
use hammer_core::data_plane::NodeId;
use hammer_core::error::CoreError;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::engine::{Engine, EnginePool};
use hammer_runtime::plugin_loader::built_plugin_path;

// Shared device/interface/transport/session registrations remain host-owned.
use hammer_service as _;

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error(transparent)]
    Hammer(#[from] CoreError),
    #[error("failed to build the Process Node runtime")]
    ProcessRuntime {
        #[source]
        source: std::io::Error,
    },
    #[error("the host graph did not publish the drop node")]
    HostDropNodeMissing,
    #[error("an empty startup root set activated a plugin")]
    StartupPluginSetNotEmpty,
    #[error("loading ip did not publish the expected plugin set")]
    IpPluginSetMismatch,
    #[error("loading ip did not publish the ip-input graph node")]
    IpInputNodeMissing,
    #[error("loading ip changed the existing drop NodeId from {before:?} to {after:?}")]
    DropNodeChanged {
        before: NodeId,
        after: Option<NodeId>,
    },
    #[error("loading ip did not start the ip-reassembly-expire-walk Process Node")]
    IpProcessNodeMissing,
    #[error("loading a missing plugin unexpectedly succeeded")]
    MissingPluginLoadSucceeded,
    #[error("loading a missing plugin returned the wrong typed error")]
    UnexpectedMissingPluginError {
        #[source]
        source: CoreError,
    },
    #[error("a failed plugin transaction changed the active plugin set")]
    FailedTransactionChangedPluginSet,
    #[error("a failed plugin transaction changed the existing drop NodeId")]
    FailedTransactionChangedDropNode,
}

fn main() -> Result<(), ExampleError> {
    let plugin_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(built_plugin_path);

    let process_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| ExampleError::ProcessRuntime { source })?;

    let mut config = Config::default();
    config.worker.count = 1;
    config.worker.buffer.slots_per_numa = 256;
    config.worker.buffer.frame_pool_size = 32;
    let config = Arc::new(config);

    let registry = RuntimeRegistry::new();
    registry.set(Arc::clone(&config));
    let runtime = hammer_runtime::new_worker_runtime(&config)?;
    let mut pool = EnginePool::new(Engine::new(runtime, registry));
    pool.main_engine_mut().install_current();

    let example_result = run_example(pool.main_engine_mut(), &plugin_path);
    let process_shutdown = pool
        .main_engine_mut()
        .shutdown_process_nodes(&process_runtime);
    let close_result = pool.close();
    Engine::uninstall_current();

    example_result?;
    process_shutdown?;
    close_result?;
    Ok(())
}

fn run_example(engine: &mut Engine, plugin_path: &Path) -> Result<(), ExampleError> {
    let config = engine.registry.require::<Config>()?;
    hammer_runtime::memory::memory_init(engine, config)?;

    // Materialize host registrations with no startup plugin roots, then start
    // one live Data Worker and the main-thread Process Node runtime.
    engine.load_plugins(plugin_path, &[])?;
    hammer_runtime::init::run_main_loop_enter(engine)?;
    engine.start_process_nodes()?;

    if !engine.loaded_plugins().is_empty() {
        return Err(ExampleError::StartupPluginSetNotEmpty);
    }
    let drop_before = engine
        .runtime
        .node_by_name("drop")
        .ok_or(ExampleError::HostDropNodeMissing)?;
    let ip_roots = ["ip".into()];

    // This call performs real dlopen, incremental lifecycle/config dispatch,
    // append-only main graph extension, worker graph publication, and worker-init.
    engine.load_plugins(plugin_path, &ip_roots)?;
    if engine.loaded_plugins().as_slice() != ["ip"] {
        return Err(ExampleError::IpPluginSetMismatch);
    }
    if engine.runtime.node_by_name("ip-input").is_none() {
        return Err(ExampleError::IpInputNodeMissing);
    }
    let drop_after_load = engine.runtime.node_by_name("drop");
    if drop_after_load != Some(drop_before) {
        return Err(ExampleError::DropNodeChanged {
            before: drop_before,
            after: drop_after_load,
        });
    }
    if engine.process_handle("ip-reassembly-expire-walk").is_none() {
        return Err(ExampleError::IpProcessNodeMissing);
    }

    // Repeated load is a no-op, while a failed new closure leaves the active
    // set and existing NodeIds unchanged.
    engine.load_plugins(plugin_path, &ip_roots)?;
    let missing_roots = ["missing".into()];
    match engine.load_plugins(plugin_path, &missing_roots) {
        Err(CoreError::PluginLibraryOpen { .. }) => {}
        Err(source) => return Err(ExampleError::UnexpectedMissingPluginError { source }),
        Ok(()) => return Err(ExampleError::MissingPluginLoadSucceeded),
    }
    if engine.loaded_plugins().as_slice() != ["ip"] {
        return Err(ExampleError::FailedTransactionChangedPluginSet);
    }
    if engine.runtime.node_by_name("drop") != Some(drop_before) {
        return Err(ExampleError::FailedTransactionChangedDropNode);
    }

    println!("loaded plugin: ip");
    println!("main graph and live worker update completed");
    Ok(())
}

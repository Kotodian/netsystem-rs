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

use std::ffi::OsStr;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use hammer_core::config::Config;
use hammer_core::data_plane::NodeId;
use hammer_core::error::CoreError;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::engine::{Engine, EnginePool};
use hammer_runtime::plugin_loader::{built_plugin_path, plugin_cdylib_path};

// Shared device/interface/transport/session registrations remain host-owned.
use hammer_service as _;

const EXAMPLE_CONFIG: &str = r#"
plugins = ["tun", "ip", "tcp", "udp"]

[memory]
main_heap_size = "256 MiB"
"#;
const PLUGIN_NAMES: [&str; 4] = ["tun", "ip", "tcp", "udp"];

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error(transparent)]
    MainHeap(#[from] hammer_infra::main_heap::MainHeapError),
    #[error(transparent)]
    Hammer(#[from] CoreError),
    #[error("required image does not exist: {path}")]
    ImageMissing { path: PathBuf },
    #[error("failed to run `{tool}` for {path}")]
    ImageInspectionIo {
        tool: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("`{tool}` failed for {path} with status {status:?}: {stderr}")]
    ImageInspectionFailed {
        tool: &'static str,
        path: PathBuf,
        status: Option<i32>,
        stderr: String,
    },
    #[error("image does not dynamically depend on the shared hammer-infra authority: {path}")]
    SharedInfraDependencyMissing { path: PathBuf },
    #[error("image embeds an independent mimalloc authority: {path}")]
    IndependentAllocatorEmbedded { path: PathBuf },
    #[error("the shared hammer-infra image does not contain the mimalloc authority: {path}")]
    SharedAllocatorMissing { path: PathBuf },
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    #[error("image inspection is unsupported on this platform")]
    ImageInspectionUnsupported,
    #[error("failed to build the Process Node runtime")]
    ProcessRuntime {
        #[source]
        source: std::io::Error,
    },
    #[error("the host graph did not publish the drop node")]
    HostDropNodeMissing,
    #[error("an empty startup root set activated a plugin")]
    StartupPluginSetNotEmpty,
    #[error("loading the real plugin closure did not publish the expected set")]
    PluginSetMismatch,
    #[error("loading the real plugin closure did not publish graph node `{name}`")]
    PluginNodeMissing { name: &'static str },
    #[error("loading plugins changed the existing drop NodeId from {before:?} to {after:?}")]
    DropNodeChanged {
        before: NodeId,
        after: Option<NodeId>,
    },
    #[error("loading plugins did not start the ip-reassembly-expire-walk Process Node")]
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
    let requested_capacity = {
        let bootstrap = hammer_core::config::parse_bootstrap_config(EXAMPLE_CONFIG)?;
        bootstrap.memory.main_heap_size
    };
    let main_heap_capacity = hammer_infra::main_heap::init(requested_capacity)?;
    let mut config = hammer_core::config::parse_config(EXAMPLE_CONFIG)?;
    exercise_post_ready_allocations(&config)?;

    let plugin_path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(built_plugin_path);

    let process_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| ExampleError::ProcessRuntime { source })?;

    config.worker.count = 1;
    config.worker.buffer.slots_per_numa = 256;
    config.worker.buffer.frame_pool_size = 32;
    let config = Arc::new(config);

    let registry = RuntimeRegistry::new();
    registry.set(Arc::clone(&config));
    let runtime = hammer_runtime::new_worker_runtime(&config)?;
    let mut pool = EnginePool::new(Engine::new(runtime, registry));
    pool.main_engine_mut().install_current();

    let example_result = run_example(pool.main_engine_mut(), &plugin_path, main_heap_capacity);
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

fn run_example(
    engine: &mut Engine,
    plugin_path: &Path,
    main_heap_capacity: usize,
) -> Result<(), ExampleError> {
    let config = engine.registry.require::<Config>()?;
    hammer_runtime::memory::memory_init(engine, Arc::clone(&config))?;

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
    let roots = config.requested_plugins().to_vec();

    // This call performs real dlopen, incremental lifecycle/config dispatch,
    // append-only main graph extension, worker graph publication, and worker-init.
    engine.load_plugins(plugin_path, &roots)?;
    if engine.loaded_plugins().as_slice() != PLUGIN_NAMES {
        return Err(ExampleError::PluginSetMismatch);
    }
    for name in ["tun-input", "ip-input", "tcp-input", "udp-input"] {
        if engine.runtime.node_by_name(name).is_none() {
            return Err(ExampleError::PluginNodeMissing { name });
        }
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
    verify_shared_allocator_images(plugin_path)?;

    engine.load_plugins(plugin_path, &roots)?;
    let missing_roots = ["missing".into()];
    match engine.load_plugins(plugin_path, &missing_roots) {
        Err(CoreError::PluginLibraryOpen { .. }) => {}
        Err(source) => return Err(ExampleError::UnexpectedMissingPluginError { source }),
        Ok(()) => return Err(ExampleError::MissingPluginLoadSucceeded),
    }
    if engine.loaded_plugins().as_slice() != PLUGIN_NAMES {
        return Err(ExampleError::FailedTransactionChangedPluginSet);
    }
    if engine.runtime.node_by_name("drop") != Some(drop_before) {
        return Err(ExampleError::FailedTransactionChangedDropNode);
    }

    println!("fixed main heap: {main_heap_capacity} bytes");
    println!("loaded plugins: tun, ip, tcp, udp");
    println!("host and plugin images share libhammer_infra allocator authority");
    println!("main graph and live worker update completed");
    Ok(())
}

fn exercise_post_ready_allocations(config: &Config) -> Result<(), ExampleError> {
    let string = String::from("Hammer fixed-capacity process-global main heap");

    let values = vec![0x5au64; 64];

    let boxed = Box::new([0xa5u8; 128]);

    let shared = Arc::new([0x3cu8; 128]);

    let current_exe =
        std::env::current_exe().map_err(|source| ExampleError::ImageInspectionIo {
            tool: "current_exe",
            path: PathBuf::from("<current executable>"),
            source,
        })?;
    let path = PathBuf::from("hammer/main-heap/example/allocation");

    let parsed_plugins = config.requested_plugins();
    let Some(first_plugin) = parsed_plugins.first() else {
        return Err(ExampleError::PluginSetMismatch);
    };
    black_box((
        &string,
        &values,
        &boxed,
        &shared,
        &current_exe,
        &path,
        parsed_plugins,
        first_plugin,
    ));
    Ok(())
}

fn verify_shared_allocator_images(plugin_path: &Path) -> Result<(), ExampleError> {
    let infra_name = format!(
        "{}hammer_infra{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let infra_path = plugin_path.join(&infra_name);
    require_image(&infra_path)?;

    let infra_symbols = image_symbols(&infra_path)?;
    if !infra_symbols.contains("mi_reserve_os_memory_ex") {
        return Err(ExampleError::SharedAllocatorMissing { path: infra_path });
    }

    let mut consumers =
        vec![
            std::env::current_exe().map_err(|source| ExampleError::ImageInspectionIo {
                tool: "current_exe",
                path: PathBuf::from("<current executable>"),
                source,
            })?,
        ];
    for name in ["hammer_core", "hammer_runtime", "hammer_service"] {
        consumers.push(plugin_path.join(format!(
            "{}{name}{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        )));
    }
    for name in PLUGIN_NAMES {
        consumers.push(plugin_cdylib_path(plugin_path, name));
    }

    for path in consumers {
        require_image(&path)?;
        if !dynamic_dependencies(&path)?.contains(&infra_name) {
            return Err(ExampleError::SharedInfraDependencyMissing { path });
        }
        if image_symbols(&path)?.contains("mi_reserve_os_memory_ex") {
            return Err(ExampleError::IndependentAllocatorEmbedded { path });
        }
    }
    Ok(())
}

fn require_image(path: &Path) -> Result<(), ExampleError> {
    if path.is_file() {
        Ok(())
    } else {
        Err(ExampleError::ImageMissing {
            path: path.to_path_buf(),
        })
    }
}

#[cfg(target_os = "macos")]
fn dynamic_dependencies(path: &Path) -> Result<String, ExampleError> {
    run_image_tool("otool", &[OsStr::new("-L")], path)
}

#[cfg(target_os = "linux")]
fn dynamic_dependencies(path: &Path) -> Result<String, ExampleError> {
    run_image_tool("readelf", &[OsStr::new("-d")], path)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn dynamic_dependencies(_: &Path) -> Result<String, ExampleError> {
    Err(ExampleError::ImageInspectionUnsupported)
}

fn image_symbols(path: &Path) -> Result<String, ExampleError> {
    run_image_tool("nm", &[OsStr::new("-a")], path)
}

fn run_image_tool(
    tool: &'static str,
    arguments: &[&OsStr],
    path: &Path,
) -> Result<String, ExampleError> {
    let output = Command::new(tool)
        .args(arguments)
        .arg(path)
        .output()
        .map_err(|source| ExampleError::ImageInspectionIo {
            tool,
            path: path.to_path_buf(),
            source,
        })?;
    if !output.status.success() {
        return Err(ExampleError::ImageInspectionFailed {
            tool,
            path: path.to_path_buf(),
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

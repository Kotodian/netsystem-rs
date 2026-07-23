//! Complete additive plugin-loading example.
//!
//! Run with:
//!
//! ```text
//! HAMMER_PLUGIN_DIR=target/debug cargo run -p hammer --example plugin_additive_load
//! ```
//!

use std::ffi::OsStr;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use hammer_core::data_plane::NodeId;
use hammer_runtime::RuntimeRegistry;
use hammer_runtime::config::Memory;
use hammer_runtime::engine::{Engine, EnginePool};
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig};
use hammer_runtime::{PluginError, RuntimeError};

// Shared device/interface/transport/session registrations remain host-owned.
use hammer_service as _;

const EXAMPLE_CONFIG: &str = r#"
plugins = ["tun", "ip", "tcp", "udp"]

[memory]
main_heap_size = "256 MiB"

[worker]
count = 1

[worker.buffer]
slots_per_numa = 256
frame_pool_size = 32
"#;
const PLUGIN_NAMES: [&str; 4] = ["ip", "tun", "tcp", "udp"];

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct ExampleEarlyConfig {
    memory: Memory,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(default)]
struct ExampleStartupConfig {
    plugins: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
enum ExampleError {
    #[error(transparent)]
    MainHeap(#[from] hammer_infra::main_heap::MainHeapError),
    #[error(transparent)]
    Hammer(#[from] RuntimeError),
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
    #[error("the example configuration did not declare any plugin roots")]
    StartupPluginRootsMissing,
    #[error("loading the real plugin closure published {actual:?}, expected {expected:?}")]
    PluginSetMismatch {
        expected: Vec<&'static str>,
        actual: Vec<String>,
    },
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
        source: RuntimeError,
    },
    #[error("a failed plugin transaction changed the active plugin set")]
    FailedTransactionChangedPluginSet,
    #[error("a failed plugin transaction changed the existing drop NodeId")]
    FailedTransactionChangedDropNode,
}

fn main() -> Result<(), ExampleError> {
    let memory = parse_early_memory(EXAMPLE_CONFIG)?;
    let main_heap_capacity = hammer_infra::main_heap::init(memory.main_heap_size)?;
    let roots = parse_startup_roots(EXAMPLE_CONFIG)?;
    exercise_post_ready_allocations(&roots)?;

    let process_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|source| ExampleError::ProcessRuntime { source })?;

    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig::default());
    let mut pool = EnginePool::new(Engine::new(runtime, RuntimeRegistry::new()));
    pool.main_engine_mut()
        .plugin_main_mut()
        .register_builtin_image(hammer_service::registration_image());
    pool.main_engine_mut().install_current();

    let example_result = run_example(
        pool.main_engine_mut(),
        main_heap_capacity,
        &roots,
        EXAMPLE_CONFIG,
    );
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
    main_heap_capacity: usize,
    roots: &[String],
    config_document: &str,
) -> Result<(), ExampleError> {
    engine.configure_early(config_document)?;
    let plugin_path = engine
        .plugin_main()
        .directory()
        .map_err(RuntimeError::from)?;

    // Materialize host registrations with no startup plugin roots, then start
    // one live Data Worker and the main-thread Process Node runtime.
    engine.load_plugins(&[], config_document)?;
    hammer_runtime::init::run_main_loop_enter(engine)?;
    engine.start_process_nodes()?;

    if !engine.loaded_plugins().is_empty() {
        return Err(ExampleError::StartupPluginSetNotEmpty);
    }
    let drop_before = engine
        .runtime
        .node_by_name("drop")
        .ok_or(ExampleError::HostDropNodeMissing)?;
    // This call performs real dlopen, incremental lifecycle/config dispatch,
    // append-only main graph extension, worker graph publication, and worker-init.
    engine.load_plugins(roots, config_document)?;
    let loaded_plugins = engine.loaded_plugins();
    if loaded_plugins.as_slice() != PLUGIN_NAMES {
        return Err(ExampleError::PluginSetMismatch {
            expected: PLUGIN_NAMES.to_vec(),
            actual: loaded_plugins,
        });
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
    verify_shared_allocator_images(&plugin_path)?;

    engine.load_plugins(roots, config_document)?;
    let missing_roots = ["missing".into()];
    match engine.load_plugins(&missing_roots, config_document) {
        Err(RuntimeError::Plugin(PluginError::ManifestRead { .. })) => {}
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
    println!("loaded plugins: ip, tun, tcp, udp");
    println!("host and plugin images share libhammer_infra allocator authority");
    println!("main graph and live worker update completed");
    Ok(())
}

fn parse_early_memory(document: &str) -> Result<Memory, ExampleError> {
    let config: ExampleEarlyConfig = toml::from_str(document)
        .map_err(|error| RuntimeError::config_parse(format!("parse example TOML: {error}")))?;
    config.memory.validate()?;
    Ok(config.memory)
}

fn parse_startup_roots(document: &str) -> Result<Vec<String>, ExampleError> {
    let config: ExampleStartupConfig = toml::from_str(document)
        .map_err(|error| RuntimeError::config_parse(format!("parse example TOML: {error}")))?;
    Ok(config.plugins)
}

fn exercise_post_ready_allocations(roots: &[String]) -> Result<(), ExampleError> {
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

    let Some(first_plugin) = roots.first() else {
        return Err(ExampleError::StartupPluginRootsMissing);
    };
    black_box((
        &string,
        &values,
        &boxed,
        &shared,
        &current_exe,
        &path,
        roots,
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
        consumers.push(plugin_path.join(libloading::library_filename(format!(
            "hammer_plugin_{name}"
        ))));
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

use std::fs;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hammer_core::config::parse_config;
use hammer_core::data_plane::DataPlaneBufferConfig;
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::graph::install_packet_graph;
use hammer_runtime::plugin_loader::{
    built_plugin_cdylib_path, built_plugin_path, plugin_cdylib_filename,
};
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, Engine, PluginError, PluginMain,
    host_meets_plugin_requirement,
};

fn test_engine() -> Engine {
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 256,
            buffer_slots: 32,
            frame_slots: 32,
            ..DataPlaneBufferConfig::default()
        },
    });
    Engine::new(runtime, RuntimeRegistry::new())
}

#[test]
fn platform_library_name_uses_hammer_plugin_prefix() {
    let name = plugin_cdylib_filename("tun");
    #[cfg(target_os = "macos")]
    assert_eq!(name, "libhammer_plugin_tun.dylib");
    #[cfg(target_os = "linux")]
    assert_eq!(name, "libhammer_plugin_tun.so");
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    assert!(name.contains("hammer_plugin_tun"));
}

#[test]
fn empty_root_set_loads_no_plugin_instances() {
    let main = PluginMain::load(
        env!("CARGO_PKG_VERSION"),
        "/directory/does/not/need/to/exist",
        &[],
    )
    .expect("empty plugin set");
    assert!(main.loaded_plugins().is_empty());
}

#[test]
fn duplicate_roots_are_rejected_before_opening_libraries() {
    let error = PluginMain::load(
        env!("CARGO_PKG_VERSION"),
        "/directory/does/not/need/to/exist",
        &["udp".into(), "udp".into()],
    )
    .expect_err("duplicate roots must fail");
    assert_eq!(error, PluginError::Duplicate("udp".into()));
}

#[test]
fn semver_checks_host_compatibility() {
    assert!(host_meets_plugin_requirement("1.2.3", "1.0.0").is_ok());
    assert!(host_meets_plugin_requirement("0.1.0", "1.0.0").is_err());
}

#[test]
fn missing_plugin_reports_name_and_resolved_path() {
    let directory = std::env::temp_dir().join("hammer-missing-plugin-directory");
    let error = PluginMain::load(env!("CARGO_PKG_VERSION"), &directory, &["missing".into()])
        .expect_err("missing plugin must fail");
    match error {
        PluginError::LibraryOpen { name, path, .. } => {
            assert_eq!(name, "missing");
            assert_eq!(path, directory.join(plugin_cdylib_filename("missing")));
        }
        other => panic!("unexpected error: {other}"),
    }
}

#[test]
fn dso_constructors_publish_and_failed_load_unlinks_before_successful_activation() {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    let mismatch_dir = std::env::temp_dir().join(format!(
        "hammer-registration-rollback-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir_all(&mismatch_dir).expect("create mismatch plugin directory");
    let mismatch_path = mismatch_dir.join(plugin_cdylib_filename("mismatch"));
    fs::copy(built_plugin_cdylib_path("udp"), &mismatch_path)
        .expect("copy UDP DSO under a mismatched name");

    let error = PluginMain::load(
        env!("CARGO_PKG_VERSION"),
        &mismatch_dir,
        &["mismatch".into()],
    )
    .expect_err("mismatched registration must fail");
    assert_eq!(
        error,
        PluginError::NameMismatch {
            requested: "mismatch".into(),
            exported: "udp".into(),
        }
    );
    fs::remove_dir_all(&mismatch_dir).expect("remove mismatch plugin directory");

    let mut rollback_probe = test_engine();
    // Shared infrastructure may remain mapped independently and references IP
    // nexts. Node creation precedes named-next resolution, so the failed UDP
    // image can still be checked without requiring a complete plugin graph.
    _ = install_packet_graph(&mut rollback_probe, Arc::new(Default::default()));
    assert!(rollback_probe.runtime.node_by_name("udp-input").is_none());

    let config = Arc::new(parse_config("plugins = [\"udp\"]").expect("UDP plugin config"));
    let mut engine = test_engine();
    engine.registry.set(Arc::clone(&config));
    engine
        .load_plugins_from_config(&built_plugin_path())
        .expect("load UDP dependency closure");
    assert_eq!(engine.loaded_plugins(), ["ip", "udp"]);

    for name in ["ip", "udp"] {
        let registration = engine
            .plugin_main()
            .registration(name)
            .expect("plugin metadata");
        assert_eq!(registration.name, name);
        assert_eq!(registration.version, env!("CARGO_PKG_VERSION"));
    }

    engine.start_process_nodes().expect("start Process Nodes");
    assert!(engine.process_handle("ip-reassembly-expire-walk").is_some());
}

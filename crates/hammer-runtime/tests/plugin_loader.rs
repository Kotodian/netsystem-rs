use hammer_core::data_plane::DataPlaneBufferConfig;
use hammer_runtime::plugin_loader::{built_plugin_path, plugin_cdylib_filename};
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, PluginError, PluginMain,
    host_meets_plugin_requirement,
};
use std::sync::Mutex;

static DYNAMIC_LIBRARY_TEST: Mutex<()> = Mutex::new(());

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
fn plugin_main_loads_all_network_dsos_and_owns_their_contributions() {
    let library_guard = DYNAMIC_LIBRARY_TEST.lock().expect("dynamic library test");
    let roots = ["tun".into(), "ip".into(), "tcp".into(), "udp".into()];
    let main = PluginMain::load(env!("CARGO_PKG_VERSION"), built_plugin_path(), &roots)
        .expect("load network plugins");

    assert_eq!(main.loaded_plugins(), roots);
    for name in ["tun", "ip", "tcp", "udp"] {
        let registration = main.registration(name).expect("registration");
        assert_eq!(registration.name, name);
        assert_eq!(registration.version, env!("CARGO_PKG_VERSION"));
        assert!(
            registration
                .graph_nodes
                .iter()
                .any(|entry| entry.plugin == Some(name)),
            "{name} must export at least one owned Graph Node"
        );
    }

    let graph_nodes = main.graph_nodes();
    for name in ["tun", "ip", "tcp", "udp"] {
        assert!(graph_nodes.iter().any(|entry| entry.plugin == Some(name)));
    }
    drop(graph_nodes);
    drop(main);
    drop(library_guard);
}

#[test]
fn tcp_root_loads_ip_dependency_first() {
    let library_guard = DYNAMIC_LIBRARY_TEST.lock().expect("dynamic library test");
    let main = PluginMain::load(
        env!("CARGO_PKG_VERSION"),
        built_plugin_path(),
        &["tcp".into()],
    )
    .expect("load tcp dependency closure");
    assert_eq!(main.loaded_plugins(), ["ip", "tcp"]);
    drop(main);
    drop(library_guard);
}

#[test]
fn dynamically_imported_node_entry_installs_callable_graph_state() {
    let library_guard = DYNAMIC_LIBRARY_TEST.lock().expect("dynamic library test");
    let main = PluginMain::load(
        env!("CARGO_PKG_VERSION"),
        built_plugin_path(),
        &["udp".into()],
    )
    .expect("load udp plugin");
    let entry = main
        .graph_nodes()
        .into_iter()
        .find(|entry| entry.registration.name() == Some("udp-input"))
        .expect("udp-input contribution");
    let runtime = DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity: 128,
            buffer_slots: 8,
            frame_slots: 8,
            ..DataPlaneBufferConfig::default()
        },
    });

    let installed = (entry.init)(&runtime, 0).expect("call dynamic node init");
    assert_eq!(runtime.node_by_name("udp-input"), Some(installed));
    drop(runtime);
    drop(main);
    drop(library_guard);
}

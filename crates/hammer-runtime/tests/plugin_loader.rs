use hammer_runtime::plugin_loader::{
    LoadTransaction, collect_plugin_inventory, plugin_cdylib_filename,
};
use hammer_runtime::{PluginRegistration, select_and_expand_plugins};

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
fn load_transaction_rolls_back_libraries_when_activate_fails() {
    let catalog = [
        PluginRegistration {
            name: "session",
            version: "0.1.0",
            version_required: "0.1.0",
            load_after: &[],
        },
        PluginRegistration {
            name: "tcp",
            version: "0.1.0",
            version_required: "0.1.0",
            load_after: &["session"],
        },
    ];
    let order = select_and_expand_plugins(&["tcp".into()], &catalog).expect("plan");
    assert_eq!(order, ["session", "tcp"]);

    let mut tx = LoadTransaction::new("0.1.0");
    let err = tx
        .activate_in_order(&order, |name| {
            if name == "tcp" {
                Err("tcp activate failed".into())
            } else {
                Ok(())
            }
        })
        .expect_err("activate must fail");
    assert!(err.contains("tcp"));
    assert!(
        tx.activated().is_empty(),
        "rollback must clear activated set, got {:?}",
        tx.activated()
    );
}

#[test]
fn shared_dependency_stays_referenced_across_two_roots() {
    let mut tx = LoadTransaction::new("0.1.0");
    tx.activate_in_order(&["session", "tcp"], |_| Ok(()))
        .expect("tcp");
    tx.activate_in_order(&["session", "udp"], |_| Ok(()))
        .expect("udp");
    assert_eq!(tx.refcount("session"), 2);
    tx.release_plan(&["session", "tcp"]);
    assert_eq!(tx.refcount("session"), 1);
    assert!(tx.is_held("session"));
    tx.release_plan(&["session", "udp"]);
    assert_eq!(tx.refcount("session"), 0);
    assert!(!tx.is_held("session"));
}

#[test]
fn collect_plugin_inventory_merges_private_slices() {
    #[derive(Debug, PartialEq, Eq)]
    struct Entry {
        name: &'static str,
    }
    static IP: [Entry; 1] = [Entry { name: "ip-init" }];
    static TUN: [Entry; 1] = [Entry { name: "tun-init" }];

    let merged = collect_plugin_inventory(&["ip", "tun"], |plugin| match plugin {
        "ip" => Ok(&IP[..]),
        "tun" => Ok(&TUN[..]),
        other => Err(format!("missing inventory for {other}")),
    })
    .expect("collect");
    let names: Vec<&str> = merged.iter().map(|entry| entry.name).collect();
    assert_eq!(names, ["ip-init", "tun-init"]);
}

#[test]
fn open_tun_cdylib_and_read_registration_via_dlsym() {
    use hammer_core::plugin::host_meets_plugin_requirement;
    use hammer_runtime::plugin_loader::{LoadTransaction, built_plugin_cdylib_path};

    let path = built_plugin_cdylib_path("tun");
    assert!(
        path.is_file(),
        "expected built plugin at {} (build hammer-plugin-tun first)",
        path.display()
    );

    let mut tx = LoadTransaction::new(env!("CARGO_PKG_VERSION"));
    tx.open_library("tun", &path).expect("dlopen tun");
    assert!(tx.has_library("tun"));

    let registration = tx.registration("tun").expect("dlsym registration");
    assert_eq!(registration.name, "tun");
    assert_eq!(registration.load_after, &[] as &[&str]);
    host_meets_plugin_requirement(env!("CARGO_PKG_VERSION"), registration.version_required)
        .expect("semver");
    assert_eq!(registration.version, env!("CARGO_PKG_VERSION"));
}

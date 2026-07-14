use hammer_runtime::plugin_loader::{
    collect_plugin_inventory, plugin_cdylib_filename, LoadTransaction,
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
            name: "device",
            version: "0.1.0",
            version_required: "0.1.0",
            load_after: &[],
        },
        PluginRegistration {
            name: "interface",
            version: "0.1.0",
            version_required: "0.1.0",
            load_after: &["device"],
        },
    ];
    let order = select_and_expand_plugins(&["interface".into()], &catalog).expect("plan");
    assert_eq!(order, ["device", "interface"]);

    let mut tx = LoadTransaction::new("0.1.0");
    let err = tx
        .activate_in_order(&order, |name| {
            if name == "interface" {
                Err("interface activate failed".into())
            } else {
                Ok(())
            }
        })
        .expect_err("activate must fail");
    assert!(err.contains("interface"));
    assert!(
        tx.activated().is_empty(),
        "rollback must clear activated set, got {:?}",
        tx.activated()
    );
}

#[test]
fn shared_dependency_stays_referenced_across_two_roots() {
    let mut tx = LoadTransaction::new("0.1.0");
    tx.activate_in_order(&["device", "tun"], |_| Ok(()))
        .expect("tun");
    tx.activate_in_order(&["device", "ip"], |_| Ok(()))
        .expect("ip");
    assert_eq!(tx.refcount("device"), 2);
    tx.release_plan(&["device", "tun"]);
    assert_eq!(tx.refcount("device"), 1);
    assert!(tx.is_held("device"));
    tx.release_plan(&["device", "ip"]);
    assert_eq!(tx.refcount("device"), 0);
    assert!(!tx.is_held("device"));
}

#[test]
fn collect_plugin_inventory_merges_private_slices() {
    #[derive(Debug, PartialEq, Eq)]
    struct Entry {
        name: &'static str,
    }
    static DEVICE: [Entry; 1] = [Entry { name: "device-init" }];
    static TUN: [Entry; 1] = [Entry { name: "tun-init" }];

    let merged = collect_plugin_inventory(&["device", "tun"], |plugin| match plugin {
        "device" => Ok(&DEVICE[..]),
        "tun" => Ok(&TUN[..]),
        other => Err(format!("missing inventory for {other}")),
    })
    .expect("collect");
    let names: Vec<&str> = merged.iter().map(|entry| entry.name).collect();
    assert_eq!(names, ["device-init", "tun-init"]);
}

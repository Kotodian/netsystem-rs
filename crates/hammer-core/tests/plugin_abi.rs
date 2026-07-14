use hammer_core::plugin::{
    PluginRegistration, host_meets_plugin_requirement, select_and_expand_plugins,
};

#[test]
fn semver_rejects_incompatible_host() {
    let plugin = PluginRegistration {
        name: "tcp",
        version: "0.1.0",
        version_required: "1.0.0",
        load_after: &[],
    };
    assert!(
        host_meets_plugin_requirement("0.1.0", plugin.version_required).is_err(),
        "host 0.1.0 must not satisfy plugin requiring 1.0.0"
    );
}

#[test]
fn semver_accepts_compatible_host() {
    assert!(host_meets_plugin_requirement("1.2.3", "1.0.0").is_ok());
}

#[test]
fn configured_roots_expand_hard_load_after_dependencies() {
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
        PluginRegistration {
            name: "tun",
            version: "0.1.0",
            version_required: "0.1.0",
            load_after: &["device", "interface"],
        },
    ];
    let ordered = select_and_expand_plugins(&["tun".into()], &catalog).expect("expand");
    assert_eq!(ordered, ["device", "interface", "tun"]);
}

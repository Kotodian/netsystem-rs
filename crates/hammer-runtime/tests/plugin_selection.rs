//! Plugin selection and contribution filtering.

use hammer_infra::vec::Vec;
use hammer_runtime::{
    PluginError, PluginRegistration, filter_by_plugin, select_loaded_plugins_from,
};

fn catalog(entries: &[(&'static str, &'static [&'static str])]) -> Vec<PluginRegistration> {
    entries
        .iter()
        .map(|(name, load_after)| PluginRegistration { name, load_after })
        .collect()
}

#[test]
fn unknown_plugin_name_is_rejected() {
    let catalog = catalog(&[("tun", &[])]);
    let err = select_loaded_plugins_from(&["ghost".into()], &catalog).unwrap_err();
    assert_eq!(err, PluginError::Unknown("ghost".into()));
}

#[test]
fn duplicate_plugin_name_is_rejected() {
    let catalog = catalog(&[("tun", &[])]);
    let err = select_loaded_plugins_from(&["tun".into(), "tun".into()], &catalog).unwrap_err();
    assert_eq!(err, PluginError::Duplicate("tun".into()));
}

#[test]
fn load_after_cycle_is_rejected() {
    let catalog = catalog(&[("a", &["b"]), ("b", &["a"])]);
    let err = select_loaded_plugins_from(&["a".into(), "b".into()], &catalog).unwrap_err();
    assert!(matches!(err, PluginError::Cycle(_)));
}

#[test]
fn load_after_to_unloaded_plugin_is_rejected() {
    let catalog = catalog(&[("tcp", &["session"]), ("session", &[])]);
    let err = select_loaded_plugins_from(&["tcp".into()], &catalog).unwrap_err();
    assert_eq!(
        err,
        PluginError::LoadAfterUnloaded {
            name: "tcp".into(),
            dep: "session".into(),
        }
    );
}

#[test]
fn filter_excludes_unloaded_plugin_contributions_and_keeps_builtins() {
    #[derive(Debug, PartialEq)]
    struct Contribution {
        plugin: Option<&'static str>,
        name: &'static str,
    }

    let items = [
        Contribution {
            plugin: None,
            name: "memory_init",
        },
        Contribution {
            plugin: Some("tun"),
            name: "tun_config",
        },
        Contribution {
            plugin: Some("tcp"),
            name: "tcp_init",
        },
    ];

    let filtered = filter_by_plugin(&items, &["tun"], |item| item.plugin);
    let names: Vec<&str> = filtered.iter().map(|item| item.name).collect();
    assert_eq!(names, hammer_infra::vec!["memory_init", "tun_config"]);
}

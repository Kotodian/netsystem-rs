//! Statically linked plugin catalog wiring on top of `hammer_core::plugin`.
//!
//! Cargo features decide which `PluginRegistration` entries are linked into
//! `PLUGIN_REGISTRATIONS`. Startup `Config.plugins` selects a loaded subset;
//! lifecycle and graph contributions are then filtered by that set.

use hammer_core::error::HammerResult;
use hammer_infra::vec::Vec;

pub use hammer_core::plugin::{
    PluginError, PluginRegistration, filter_by_plugin, host_meets_plugin_requirement,
    select_and_expand_plugins, select_loaded_plugins_from, validate_catalog_semver,
};

#[linkme::distributed_slice]
pub static PLUGIN_REGISTRATIONS: [PluginRegistration] = [..];

/// Names of every plugin linked into this binary.
pub fn compiled_plugin_names() -> Vec<&'static str> {
    PLUGIN_REGISTRATIONS
        .iter()
        .map(|registration| registration.name)
        .collect()
}

/// Validate and order the requested plugin list against the compiled catalog.
///
/// Rejects unknown names, duplicates, `load_after` edges to unloaded plugins,
/// and cycles among the selected set. Does not auto-load missing dependencies.
pub fn select_loaded_plugins(requested: &[String]) -> HammerResult<Vec<&'static str>> {
    select_loaded_plugins_from(requested, &PLUGIN_REGISTRATIONS).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(entries: &[(&'static str, &'static [&'static str])]) -> Vec<PluginRegistration> {
        entries
            .iter()
            .map(|(name, load_after)| PluginRegistration {
                name,
                version: "0.1.0",
                version_required: "0.1.0",
                load_after,
            })
            .collect()
    }

    #[test]
    fn rejects_unknown_plugin() {
        let catalog = catalog(&[("tun", &[])]);
        let err = select_loaded_plugins_from(&["tcp".into()], &catalog).unwrap_err();
        assert_eq!(err, PluginError::Unknown("tcp".into()));
    }

    #[test]
    fn rejects_duplicate() {
        let catalog = catalog(&[("tun", &[])]);
        let err = select_loaded_plugins_from(&["tun".into(), "tun".into()], &catalog).unwrap_err();
        assert_eq!(err, PluginError::Duplicate("tun".into()));
    }

    #[test]
    fn rejects_load_after_cycle() {
        let catalog = catalog(&[("a", &["b"]), ("b", &["a"])]);
        let err = select_loaded_plugins_from(&["a".into(), "b".into()], &catalog).unwrap_err();
        assert!(matches!(err, PluginError::Cycle(_)));
    }

    #[test]
    fn rejects_load_after_unloaded() {
        let catalog = catalog(&[("tcp", &["session"]), ("session", &[])]);
        let err = select_loaded_plugins_from(&["tcp".into()], &catalog).unwrap_err();
        assert_eq!(
            err,
            PluginError::LoadAfterMissing {
                name: "tcp".into(),
                dep: "session".into(),
            }
        );
    }

    #[test]
    fn orders_by_load_after() {
        let catalog = catalog(&[("tcp", &["session"]), ("session", &[]), ("ip", &[])]);
        let loaded =
            select_loaded_plugins_from(&["tcp".into(), "session".into(), "ip".into()], &catalog)
                .expect("select");
        let session = loaded.iter().position(|n| *n == "session").unwrap();
        let tcp = loaded.iter().position(|n| *n == "tcp").unwrap();
        assert!(session < tcp);
    }

    #[test]
    fn filter_keeps_builtins_and_loaded_only() {
        #[derive(Debug, PartialEq)]
        struct Item {
            plugin: Option<&'static str>,
            name: &'static str,
        }
        let items = [
            Item {
                plugin: None,
                name: "builtin",
            },
            Item {
                plugin: Some("tun"),
                name: "tun-node",
            },
            Item {
                plugin: Some("tcp"),
                name: "tcp-node",
            },
        ];
        let filtered = filter_by_plugin(&items, &["tun"], |item| item.plugin);
        let names: Vec<&str> = filtered.iter().map(|item| item.name).collect();
        assert_eq!(names, hammer_infra::vec!["builtin", "tun-node"]);
    }
}

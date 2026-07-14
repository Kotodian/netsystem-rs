//! Statically linked plugin catalog and startup selection.
//!
//! Cargo features decide which `PluginRegistration` entries are linked into
//! `PLUGIN_REGISTRATIONS`. Startup `Config.plugins` selects a loaded subset;
//! lifecycle and graph contributions are then filtered by that set.

use hammer_core::error::{HammerError, HammerResult};
use hammer_infra::vec::Vec;
use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;

/// Static metadata for one compiled plugin (VPP `VLIB_PLUGIN_REGISTER` shape).
#[derive(Debug, Clone, Copy)]
pub struct PluginRegistration {
    pub name: &'static str,
    pub load_after: &'static [&'static str],
}

#[linkme::distributed_slice]
pub static PLUGIN_REGISTRATIONS: [PluginRegistration] = [..];

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PluginError {
    #[error("unknown plugin `{0}` (not in compiled catalog)")]
    Unknown(String),
    #[error("duplicate plugin `{0}` in requested list")]
    Duplicate(String),
    #[error("plugin `{name}` load_after references unloaded plugin `{dep}`")]
    LoadAfterUnloaded { name: String, dep: String },
    #[error("plugin load_after cycle involving `{0}`")]
    Cycle(String),
}

impl From<PluginError> for HammerError {
    fn from(err: PluginError) -> Self {
        HammerError::config_validation(err.to_string())
    }
}

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

/// Testable selection against an explicit catalog (avoids linkme in unit tests).
pub fn select_loaded_plugins_from(
    requested: &[String],
    catalog: &[PluginRegistration],
) -> Result<Vec<&'static str>, PluginError> {
    let mut selected: Vec<&'static str> = Vec::with_capacity(requested.len());
    for name in requested {
        let Some(registration) = catalog.iter().find(|entry| entry.name == name.as_str()) else {
            return Err(PluginError::Unknown(name.clone()));
        };
        if selected.contains(&registration.name) {
            return Err(PluginError::Duplicate(name.clone()));
        }
        selected.push(registration.name);
    }

    let selected_set = &selected;
    let mut graph = DiGraphMap::<&str, ()>::new();
    for name in selected_set.iter().copied() {
        graph.add_node(name);
    }
    for name in selected_set.iter().copied() {
        let registration = catalog
            .iter()
            .find(|entry| entry.name == name)
            .expect("selected name must be in catalog");
        for dep in registration.load_after {
            if !selected_set.iter().any(|loaded| *loaded == *dep) {
                return Err(PluginError::LoadAfterUnloaded {
                    name: name.to_owned(),
                    dep: (*dep).to_owned(),
                });
            }
            graph.add_edge(*dep, name, ());
        }
    }

    let ordered =
        toposort(&graph, None).map_err(|cycle| PluginError::Cycle(cycle.node_id().to_string()))?;
    Ok(ordered.into_iter().collect())
}

/// Keep builtins (`plugin: None`) and contributions owned by a loaded plugin.
pub fn filter_by_plugin<'a, T>(
    items: &'a [T],
    loaded: &[&str],
    plugin_of: impl Fn(&T) -> Option<&'static str>,
) -> Vec<&'a T> {
    items
        .iter()
        .filter(|item| match plugin_of(item) {
            None => true,
            Some(name) => loaded.iter().any(|loaded| *loaded == name),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalog(entries: &[(&'static str, &'static [&'static str])]) -> Vec<PluginRegistration> {
        entries
            .iter()
            .map(|(name, load_after)| PluginRegistration { name, load_after })
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
            PluginError::LoadAfterUnloaded {
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

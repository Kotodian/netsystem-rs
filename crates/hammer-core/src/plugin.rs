//! Plugin registration and load-order selection (VPP `VLIB_PLUGIN_REGISTER` shape).
//!
//! Dynamic loading owns `libloading::Library` handles elsewhere; this module is
//! the catalog / SemVer / dependency-expansion surface approved in #95.

use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;
use semver::{Version, VersionReq};

use crate::error::{HammerError, HammerResult};
use hammer_infra::vec::Vec;

/// Metadata for one plugin (static catalog entry or `dlsym` export).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PluginRegistration {
    pub name: &'static str,
    pub version: &'static str,
    pub version_required: &'static str,
    pub load_after: &'static [&'static str],
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PluginError {
    #[error("unknown plugin `{0}`")]
    Unknown(String),
    #[error("duplicate plugin `{0}` in requested list")]
    Duplicate(String),
    #[error("plugin `{name}` load_after references missing plugin `{dep}`")]
    LoadAfterMissing { name: String, dep: String },
    #[error("plugin load_after cycle involving `{0}`")]
    Cycle(String),
    #[error("host version `{host}` does not satisfy plugin requirement `{required}`")]
    SemVerMismatch { host: String, required: String },
    #[error("invalid semver: {0}")]
    InvalidSemVer(String),
}

impl From<PluginError> for HammerError {
    fn from(err: PluginError) -> Self {
        HammerError::config_validation(err.to_string())
    }
}

/// Returns Ok when `host_version` satisfies `version_required`.
///
/// Empty `version_required` means no constraint (VPP empty `version_required`).
pub fn host_meets_plugin_requirement(
    host_version: &str,
    version_required: &str,
) -> Result<(), PluginError> {
    if version_required.is_empty() {
        return Ok(());
    }
    let host = Version::parse(host_version)
        .map_err(|err| PluginError::InvalidSemVer(format!("host `{host_version}`: {err}")))?;
    let required = Version::parse(version_required).map_err(|err| {
        PluginError::InvalidSemVer(format!("required `{version_required}`: {err}"))
    })?;
    // Plugin states the minimum host version it needs.
    let req = VersionReq::parse(&format!(">={required}"))
        .map_err(|err| PluginError::InvalidSemVer(err.to_string()))?;
    if req.matches(&host) {
        Ok(())
    } else {
        Err(PluginError::SemVerMismatch {
            host: host_version.to_owned(),
            required: version_required.to_owned(),
        })
    }
}

/// Expand configured root plugins with transitive `load_after` dependencies,
/// reject duplicates/unknown/cycles, and return load order (dependencies first).
pub fn select_and_expand_plugins(
    roots: &[String],
    catalog: &[PluginRegistration],
) -> Result<Vec<&'static str>, PluginError> {
    let mut selected: Vec<&'static str> = Vec::new();
    let mut stack: Vec<&'static str> = Vec::new();

    for root in roots {
        let Some(registration) = catalog.iter().find(|entry| entry.name == root.as_str()) else {
            return Err(PluginError::Unknown(root.clone()));
        };
        if stack.contains(&registration.name) || selected.contains(&registration.name) {
            return Err(PluginError::Duplicate(root.clone()));
        }
        stack.push(registration.name);
    }

    while let Some(name) = stack.pop() {
        if selected.contains(&name) {
            continue;
        }
        let registration = catalog
            .iter()
            .find(|entry| entry.name == name)
            .ok_or_else(|| PluginError::Unknown(name.to_owned()))?;
        for dep in registration.load_after {
            let Some(dep_reg) = catalog.iter().find(|entry| entry.name == *dep) else {
                return Err(PluginError::LoadAfterMissing {
                    name: name.to_owned(),
                    dep: (*dep).to_owned(),
                });
            };
            if !selected.contains(&dep_reg.name) && !stack.contains(&dep_reg.name) {
                stack.push(dep_reg.name);
            }
        }
        selected.push(name);
    }

    let mut graph = DiGraphMap::<&str, ()>::new();
    for name in selected.iter().copied() {
        graph.add_node(name);
    }
    for name in selected.iter().copied() {
        let registration = catalog
            .iter()
            .find(|entry| entry.name == name)
            .expect("selected name must be in catalog");
        for dep in registration.load_after {
            graph.add_edge(*dep, name, ());
        }
    }

    let ordered =
        toposort(&graph, None).map_err(|cycle| PluginError::Cycle(cycle.node_id().to_string()))?;
    Ok(ordered.into_iter().collect())
}

/// Validate roots against a catalog without expanding missing deps (static mode).
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
                return Err(PluginError::LoadAfterMissing {
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

pub fn validate_catalog_semver(
    host_version: &str,
    selected: &[&str],
    catalog: &[PluginRegistration],
) -> HammerResult<()> {
    for name in selected {
        let registration = catalog
            .iter()
            .find(|entry| entry.name == *name)
            .ok_or_else(|| PluginError::Unknown((*name).to_owned()))?;
        host_meets_plugin_requirement(host_version, registration.version_required)?;
    }
    Ok(())
}

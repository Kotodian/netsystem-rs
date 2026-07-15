//! Dynamic plugin registration and dependency selection.
//!
//! `PluginMain` corresponds to VPP's `plugin_main_t`: it owns metadata,
//! dependency order, and DSO handles. Executable registrations are published
//! independently by load constructors into the runtime registration authority.

use semver::{Version, VersionReq};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use hammer_core::error::HammerError;
use hammer_infra::vec::Vec;
use libloading::Library;

use crate::plugin_loader::{plugin_cdylib_path, read_plugin_registration};

/// Metadata exported by one plugin DSO.
#[derive(Clone, Copy)]
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
    #[error("plugin load_after cycle involving `{0}`")]
    Cycle(String),
    #[error("host version `{host}` does not satisfy plugin requirement `{required}`")]
    SemVerMismatch { host: String, required: String },
    #[error("invalid semver: {0}")]
    InvalidSemVer(String),
    #[error("failed to open plugin `{name}` at `{path}`: {error}")]
    LibraryOpen {
        name: String,
        path: PathBuf,
        error: String,
    },
    #[error("failed to read registration from plugin `{name}` at `{path}`: {error}")]
    Registration {
        name: String,
        path: PathBuf,
        error: String,
    },
    #[error("plugin file for `{requested}` exported registration for `{exported}`")]
    NameMismatch { requested: String, exported: String },
}

impl From<PluginError> for HammerError {
    fn from(error: PluginError) -> Self {
        HammerError::config_validation(error.to_string())
    }
}

/// Main-thread plugin authority, corresponding to VPP's `plugin_main_t`.
///
/// The library table owns every DSO handle. Engines and workers share this
/// object so imported function pointers and static registration data cannot
/// outlive their provider library.
pub struct PluginMain {
    host_version: String,
    plugin_path: PathBuf,
    libraries: HashMap<String, Library>,
    load_order: Vec<String>,
}

impl std::fmt::Debug for PluginMain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginMain")
            .field("host_version", &self.host_version)
            .field("plugin_path", &self.plugin_path)
            .field("load_order", &self.load_order)
            .finish_non_exhaustive()
    }
}

impl PluginMain {
    pub fn empty(host_version: impl Into<String>) -> Self {
        Self {
            host_version: host_version.into(),
            plugin_path: PathBuf::new(),
            libraries: HashMap::new(),
            load_order: Vec::new(),
        }
    }

    /// Load configured roots and their transitive `load_after` dependencies.
    ///
    /// A failed load drops the partially built `PluginMain`, closing every DSO
    /// before any imported contribution can be installed into the runtime.
    pub fn load(
        host_version: impl Into<String>,
        plugin_path: impl Into<PathBuf>,
        roots: &[String],
    ) -> Result<Self, PluginError> {
        let mut main = Self {
            host_version: host_version.into(),
            plugin_path: plugin_path.into(),
            libraries: HashMap::new(),
            load_order: Vec::new(),
        };
        let mut requested = Vec::with_capacity(roots.len());
        for root in roots {
            if requested.contains(root) {
                return Err(PluginError::Duplicate(root.clone()));
            }
            requested.push(root.clone());
        }
        let mut visiting = Vec::new();
        for root in roots {
            main.load_one(root, &mut visiting)?;
        }
        Ok(main)
    }

    fn load_one(&mut self, name: &str, visiting: &mut Vec<String>) -> Result<(), PluginError> {
        if visiting.iter().any(|candidate| candidate == name) {
            return Err(PluginError::Cycle(name.to_owned()));
        }
        if self.libraries.contains_key(name) {
            return Ok(());
        }

        let path = plugin_cdylib_path(&self.plugin_path, name);
        // SAFETY: `PluginMain` retains the returned handle until every Engine
        // sharing it has stopped using imported registration data.
        let library = unsafe { Library::new(&path) }.map_err(|error| PluginError::LibraryOpen {
            name: name.to_owned(),
            path: path.clone(),
            error: error.to_string(),
        })?;
        let (exported_name, version_required, dependencies) = {
            let registration =
                read_plugin_registration(&library).map_err(|error| PluginError::Registration {
                    name: name.to_owned(),
                    path: path.clone(),
                    error,
                })?;
            (
                registration.name.to_owned(),
                registration.version_required.to_owned(),
                registration
                    .load_after
                    .iter()
                    .map(|dependency| (*dependency).to_owned())
                    .collect::<Vec<_>>(),
            )
        };
        if exported_name != name {
            return Err(PluginError::NameMismatch {
                requested: name.to_owned(),
                exported: exported_name,
            });
        }
        host_meets_plugin_requirement(&self.host_version, &version_required)?;

        self.libraries.insert(name.to_owned(), library);
        visiting.push(name.to_owned());
        for dependency in dependencies {
            self.load_one(&dependency, visiting)?;
        }
        visiting.pop();
        self.load_order.push(name.to_owned());
        Ok(())
    }

    #[inline]
    pub fn plugin_path(&self) -> &Path {
        &self.plugin_path
    }

    #[inline]
    pub fn loaded_plugins(&self) -> &[String] {
        &self.load_order
    }

    pub fn registration(&self, name: &str) -> Result<&PluginRegistration, PluginError> {
        let library = self
            .libraries
            .get(name)
            .ok_or_else(|| PluginError::Unknown(name.to_owned()))?;
        read_plugin_registration(library).map_err(|error| PluginError::Registration {
            name: name.to_owned(),
            path: plugin_cdylib_path(&self.plugin_path, name),
            error,
        })
    }
}

pub fn host_meets_plugin_requirement(
    host_version: &str,
    version_required: &str,
) -> Result<(), PluginError> {
    if version_required.is_empty() {
        return Ok(());
    }
    let host = Version::parse(host_version)
        .map_err(|error| PluginError::InvalidSemVer(format!("host `{host_version}`: {error}")))?;
    let required = Version::parse(version_required).map_err(|error| {
        PluginError::InvalidSemVer(format!("required `{version_required}`: {error}"))
    })?;
    let requirement = VersionReq::parse(&format!(">={required}"))
        .map_err(|error| PluginError::InvalidSemVer(error.to_string()))?;
    if requirement.matches(&host) {
        Ok(())
    } else {
        Err(PluginError::SemVerMismatch {
            host: host_version.to_owned(),
            required: version_required.to_owned(),
        })
    }
}

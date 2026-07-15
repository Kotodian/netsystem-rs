//! Dynamic plugin registration and dependency selection.
//!
//! `PluginMain` corresponds to VPP's `plugin_main_t`: it owns metadata,
//! dependency order, and DSO handles. Executable registrations are published
//! independently by load constructors into the runtime registration authority.

use semver::{Version, VersionReq};
use std::path::{Path, PathBuf};

use hammer_core::error::HammerError;
use hammer_infra::map::FlatHashTable;
use hammer_infra::spinlock::Spinlock;
use hammer_infra::vec::Vec;
use libloading::Library;

use crate::plugin_loader::{plugin_cdylib_path, read_plugin_registration};

static PLUGIN_MAIN: Spinlock<Option<PluginMain>> = Spinlock::new(None);

/// Metadata exported by one plugin DSO.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PluginRegistration {
    pub name: &'static str,
    pub version: &'static str,
    pub version_required: &'static str,
    pub load_after: &'static [&'static str],
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PluginError {
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
    #[error("plugins are already activated for this process")]
    AlreadyActivated,
}

impl From<PluginError> for HammerError {
    fn from(error: PluginError) -> Self {
        HammerError::config_validation(error.to_string())
    }
}

/// Main-thread plugin authority, corresponding to VPP's `plugin_main_t`.
///
/// The process-global library table owns every activated DSO handle, matching
/// VPP's `vlib_plugin_main`. Failed load transactions never enter this table and
/// unload before returning their error.
pub struct PluginMain {
    library_index_by_name: FlatHashTable<&'static str, usize>,
    load_order: Vec<&'static str>,
    // Drop last so every DSO-backed name remains valid while indexes drop.
    libraries: Vec<Library>,
}

impl std::fmt::Debug for PluginMain {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PluginMain")
            .field("load_order", &self.load_order)
            .finish_non_exhaustive()
    }
}

impl PluginMain {
    /// Load configured roots and their transitive `load_after` dependencies.
    ///
    /// A failed load drops the partially built `PluginMain`; DSO destructors
    /// unlink constructor-published contributions before the handles close. A
    /// successful dependency closure remains mapped until process exit.
    pub fn load(
        host_version: &str,
        plugin_path: &Path,
        roots: &[String],
    ) -> Result<(), PluginError> {
        if roots.is_empty() {
            return Ok(());
        }

        let mut requested = FlatHashTable::with_capacity(roots.len());
        for root in roots {
            let name = root.as_str();
            if requested.get(&name).is_some() {
                return Err(PluginError::Duplicate(root.clone()));
            }
            requested.insert(name, ());
        }

        let mut main = Self {
            library_index_by_name: FlatHashTable::with_capacity(roots.len()),
            load_order: Vec::with_capacity(roots.len()),
            libraries: Vec::with_capacity(roots.len()),
        };
        let mut visiting = FlatHashTable::with_capacity(roots.len());
        let mut loaded = FlatHashTable::with_capacity(roots.len());
        for root in roots {
            main.load_one(host_version, plugin_path, root, &mut visiting, &mut loaded)?;
        }

        let mut process_main = PLUGIN_MAIN.lock();
        if process_main.is_some() {
            return Err(PluginError::AlreadyActivated);
        }
        *process_main = Some(main);
        Ok(())
    }

    fn load_one<'a>(
        &mut self,
        host_version: &str,
        plugin_path: &Path,
        name: &'a str,
        visiting: &mut FlatHashTable<&'a str, ()>,
        loaded: &mut FlatHashTable<&'a str, ()>,
    ) -> Result<(), PluginError> {
        if visiting.get(&name).is_some() {
            return Err(PluginError::Cycle(name.to_owned()));
        }
        if loaded.get(&name).is_some() {
            return Ok(());
        }
        visiting.insert(name, ());

        let path = plugin_cdylib_path(plugin_path, name);
        // SAFETY: a successful transaction moves the handle into the
        // process-global PluginMain. A failed transaction drops it after the
        // DSO destructor unlinks its constructor-published registration image.
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
                registration.name,
                registration.version_required,
                registration.load_after,
            )
        };
        if exported_name != name {
            return Err(PluginError::NameMismatch {
                requested: name.to_owned(),
                exported: exported_name.to_owned(),
            });
        }
        host_meets_plugin_requirement(host_version, version_required)?;

        let library_index = self.libraries.len();
        self.libraries.push(library);
        self.library_index_by_name
            .insert(exported_name, library_index);
        for dependency in dependencies {
            self.load_one(host_version, plugin_path, dependency, visiting, loaded)?;
        }
        visiting.remove(&name);
        loaded.insert(exported_name, ());
        self.load_order.push(exported_name);
        Ok(())
    }

    pub fn loaded_plugins() -> Vec<&'static str> {
        PLUGIN_MAIN
            .lock()
            .as_ref()
            .map_or_else(Vec::new, |main| main.load_order.clone())
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

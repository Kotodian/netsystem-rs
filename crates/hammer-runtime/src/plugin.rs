//! Dynamic plugin registration and dependency selection.
//!
//! `PluginMain` corresponds to VPP's `plugin_main_t`: it owns metadata,
//! dependency order, and DSO handles. Executable registrations are published
//! independently by load constructors into the runtime registration authority.

use semver::Version;
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

#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("duplicate plugin roots at indexes {first} and {duplicate}")]
    DuplicateRoot { first: usize, duplicate: usize },
    #[error("plugin load_after cycle while loading `{path}`")]
    Cycle { path: PathBuf },
    #[error("host version `{host}` does not satisfy plugin requirement `{required}`")]
    SemVerMismatch { host: Version, required: Version },
    #[error("invalid host semver")]
    InvalidHostSemVer {
        #[source]
        source: semver::Error,
    },
    #[error("invalid required semver")]
    InvalidRequiredSemVer {
        #[source]
        source: semver::Error,
    },
    #[error("failed to open plugin at `{path}`")]
    LibraryOpen {
        path: PathBuf,
        #[source]
        source: libloading::Error,
    },
    #[error("failed to read the registration symbol from plugin at `{path}`")]
    RegistrationSymbol {
        path: PathBuf,
        #[source]
        source: libloading::Error,
    },
    #[error("plugin at `{path}` returned a null registration")]
    RegistrationNull { path: PathBuf },
    #[error("plugin at `{path}` exported a mismatched registration name")]
    NameMismatch { path: PathBuf },
    #[error("failed to resolve the daemon executable path")]
    ExecutablePath {
        #[source]
        source: std::io::Error,
    },
    #[error("daemon executable `{executable}` has no parent directory")]
    ExecutableParentMissing { executable: PathBuf },
}

impl From<PluginError> for HammerError {
    fn from(error: PluginError) -> Self {
        match error {
            PluginError::DuplicateRoot { first, duplicate } => {
                Self::PluginDuplicateRoot { first, duplicate }
            }
            PluginError::Cycle { path } => Self::PluginDependencyCycle { path },
            PluginError::SemVerMismatch { .. } => Self::PluginSemVerMismatch,
            PluginError::InvalidHostSemVer { .. } => Self::PluginHostVersionInvalid,
            PluginError::InvalidRequiredSemVer { .. } => Self::PluginRequiredVersionInvalid,
            PluginError::LibraryOpen { path, .. } => Self::PluginLibraryOpen { path },
            PluginError::RegistrationSymbol { path, .. } => Self::PluginRegistrationSymbol { path },
            PluginError::RegistrationNull { path } => Self::PluginRegistrationNull { path },
            PluginError::NameMismatch { path } => Self::PluginNameMismatch { path },
            PluginError::ExecutablePath { source } => Self::PluginExecutablePath { source },
            PluginError::ExecutableParentMissing { executable } => {
                Self::PluginExecutableParentMissing { executable }
            }
        }
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
        for (index, root) in roots.iter().enumerate() {
            let name = root.as_str();
            if let Some(first) = requested.get(&name).copied() {
                return Err(PluginError::DuplicateRoot {
                    first,
                    duplicate: index,
                });
            }
            requested.insert(name, index);
        }

        let mut process_main = PLUGIN_MAIN.lock();
        let mut transaction = Self {
            library_index_by_name: FlatHashTable::with_capacity(roots.len()),
            load_order: Vec::with_capacity(roots.len()),
            libraries: Vec::with_capacity(roots.len()),
        };
        let mut visiting = FlatHashTable::with_capacity(roots.len());
        let mut loaded = FlatHashTable::with_capacity(roots.len());
        for root in roots {
            transaction.load_one(
                host_version,
                plugin_path,
                root,
                process_main.as_ref(),
                &mut visiting,
                &mut loaded,
            )?;
        }

        if transaction.load_order.is_empty() {
            return Ok(());
        }

        let main = process_main.get_or_insert_with(|| Self {
            library_index_by_name: FlatHashTable::with_capacity(transaction.libraries.len()),
            load_order: Vec::with_capacity(transaction.load_order.len()),
            libraries: Vec::with_capacity(transaction.libraries.len()),
        });
        let library_base = main.libraries.len();
        for (name, index) in transaction.library_index_by_name.iter() {
            main.library_index_by_name
                .insert(name, library_base + index);
        }
        main.load_order.append(&mut transaction.load_order);
        main.libraries.append(&mut transaction.libraries);
        Ok(())
    }

    fn load_one<'a>(
        &mut self,
        host_version: &str,
        plugin_path: &Path,
        name: &'a str,
        active: Option<&PluginMain>,
        visiting: &mut FlatHashTable<&'a str, ()>,
        loaded: &mut FlatHashTable<&'a str, ()>,
    ) -> Result<(), PluginError> {
        if active
            .and_then(|main| main.library_index_by_name.get(&name))
            .is_some()
        {
            return Ok(());
        }
        if visiting.get(&name).is_some() {
            return Err(PluginError::Cycle {
                path: plugin_cdylib_path(plugin_path, name),
            });
        }
        if loaded.get(&name).is_some() {
            return Ok(());
        }
        visiting.insert(name, ());

        let path = plugin_cdylib_path(plugin_path, name);
        // SAFETY: a successful transaction moves the handle into the
        // process-global PluginMain. A failed transaction drops it after the
        // DSO destructor unlinks its constructor-published registration image.
        let library =
            unsafe { Library::new(&path) }.map_err(|source| PluginError::LibraryOpen {
                path: path.clone(),
                source,
            })?;
        let (exported_name, version_required, dependencies) = {
            let registration = read_plugin_registration(&library)
                .map_err(|source| PluginError::RegistrationSymbol {
                    path: path.clone(),
                    source,
                })?
                .ok_or_else(|| PluginError::RegistrationNull { path: path.clone() })?;
            (
                registration.name,
                registration.version_required,
                registration.load_after,
            )
        };
        if exported_name != name {
            return Err(PluginError::NameMismatch { path });
        }
        host_meets_plugin_requirement(host_version, version_required)?;

        let library_index = self.libraries.len();
        self.libraries.push(library);
        self.library_index_by_name
            .insert(exported_name, library_index);
        for dependency in dependencies {
            self.load_one(
                host_version,
                plugin_path,
                dependency,
                active,
                visiting,
                loaded,
            )?;
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
    let host =
        Version::parse(host_version).map_err(|source| PluginError::InvalidHostSemVer { source })?;
    let required = Version::parse(version_required)
        .map_err(|source| PluginError::InvalidRequiredSemVer { source })?;
    if host >= required {
        Ok(())
    } else {
        Err(PluginError::SemVerMismatch { host, required })
    }
}

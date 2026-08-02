//! Dynamic plugin registration and dependency selection.
//!
//! `PluginMain` corresponds to VPP's `plugin_main_t`: it owns metadata,
//! dependency order, DSO handles, and executable registration images.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use abi_stable::{
    RRef, StableAbi,
    library::RootModule,
    std_types::{RSlice, RStr},
};
use object::{Object, ObjectSection};
use semver::Version;
use serde::Deserialize;

use crate::app::SessionAppRegistration;
use crate::binary_api::BinaryApiMethodEntry;
use crate::init::{ConfigFunction, InitFunction};
use crate::node::{NodeEntry, NodeFunctionRegistration};
use crate::plugin_loader::{PluginLibrary, read_plugin_module};
use crate::process::ProcessEntry;
use crate::registration::RegistrationImage;
use crate::session::SessionTransportRegistration;

/// Metadata owned by one dynamically loaded plugin module.
#[repr(C)]
#[derive(Clone, Copy, StableAbi)]
pub struct PluginMetadata {
    name: RStr<'static>,
    version: RStr<'static>,
    version_required: RStr<'static>,
    load_after: RSlice<'static, RStr<'static>>,
}

impl PluginMetadata {
    #[doc(hidden)]
    pub const fn new(
        name: RStr<'static>,
        version: RStr<'static>,
        version_required: RStr<'static>,
        load_after: RSlice<'static, RStr<'static>>,
    ) -> Self {
        Self {
            name,
            version,
            version_required,
            load_after,
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn version_required(&self) -> &str {
        self.version_required.as_str()
    }

    #[inline]
    pub fn version(&self) -> &str {
        self.version.as_str()
    }

    #[inline]
    pub fn load_after(&self) -> impl Iterator<Item = &str> {
        self.load_after
            .as_slice()
            .iter()
            .map(|dependency| dependency.as_str())
    }
}

/// The sole `abi_stable` root module exported by every Hammer plugin DSO.
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = PluginModuleRef)))]
pub struct PluginModule {
    pub metadata: PluginMetadata,
    #[sabi(last_prefix_field)]
    pub registration_image: RRef<'static, RegistrationImage>,
}

/// Declarative DSO metadata read before loading a plugin.
///
/// The macro writes this TOML document into a fixed object-file section so
/// `PluginMain` can discover the complete dependency closure before `dlopen`.
#[derive(Deserialize)]
struct PluginManifest {
    name: String,
    version: String,
    version_required: String,
    #[serde(default)]
    load_after: Vec<String>,
}

impl PluginModule {
    #[doc(hidden)]
    pub const fn new(
        metadata: PluginMetadata,
        registration_image: RRef<'static, RegistrationImage>,
    ) -> Self {
        Self {
            metadata,
            registration_image,
        }
    }
}

impl RootModule for PluginModuleRef {
    abi_stable::declare_root_module_statics! {PluginModuleRef}

    const BASE_NAME: &'static str = "hammer_plugin";
    const NAME: &'static str = "hammer_plugin";
    const VERSION_STRINGS: abi_stable::sabi_types::VersionStrings =
        abi_stable::package_version_strings!();
}

#[derive(Debug, thiserror::Error)]
pub enum PluginError {
    #[error("plugin `{name}` is not loaded")]
    NotLoaded { name: String },
    #[error("Session App `{name}` is not registered")]
    SessionAppMissing { name: String },
    #[error("Session App `{name}` is registered more than once")]
    SessionAppDuplicate { name: String },
    #[error("Session Transport `{name}` is not registered")]
    SessionTransportMissing { name: String },
    #[error("Session Transport `{name}` is registered more than once")]
    SessionTransportDuplicate { name: String },
    #[error("Binary API method `{name}` is not registered")]
    BinaryApiMethodMissing { name: String },
    #[error("Binary API method `{name}` is registered more than once")]
    BinaryApiMethodDuplicate { name: String },
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
    #[error("failed to initialize the abi_stable root module from plugin `{path}`")]
    RootModule {
        path: PathBuf,
        #[source]
        source: crate::plugin_loader::PluginModuleLoadError,
    },
    #[error("plugin at `{path}` exported a mismatched module name")]
    NameMismatch { path: PathBuf },
    #[error("failed to read plugin manifest from `{path}`")]
    ManifestRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to inspect plugin manifest in `{path}`: {message}")]
    ManifestObject { path: PathBuf, message: String },
    #[error("plugin manifest section is missing from `{path}`")]
    ManifestMissing { path: PathBuf },
    #[error("failed to parse plugin manifest in `{path}`")]
    ManifestParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("plugin root metadata does not match the manifest in `{path}`")]
    ManifestMetadataMismatch { path: PathBuf },
    #[error("failed to resolve the daemon executable path")]
    ExecutablePath {
        #[source]
        source: std::io::Error,
    },
    #[error("daemon executable `{executable}` has no parent directory")]
    ExecutableParentMissing { executable: PathBuf },
}

/// Main-thread plugin authority, corresponding to VPP's `plugin_main_t`.
///
/// The library table indexes every activated DSO, matching VPP's
/// `vlib_plugin_main`. Opened Rust plugin images and loader handles remain live
/// until process exit; failed transactions never publish their modules into
/// this active table.
pub struct PluginMain {
    modules_by_plugin: HashMap<String, PluginModuleRef>,
    library_index_by_name: HashMap<String, usize>,
    load_order: Vec<String>,
    builtin_registration_images: Vec<&'static RegistrationImage>,
    // Drop last so every DSO-backed root module remains valid while indexes drop.
    libraries: Vec<PluginLibrary>,
}

impl Default for PluginMain {
    fn default() -> Self {
        Self {
            modules_by_plugin: HashMap::new(),
            library_index_by_name: HashMap::new(),
            load_order: Vec::new(),
            builtin_registration_images: vec![crate::builtin_registration_image()],
            libraries: Vec::new(),
        }
    }
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
    /// Adds one host-owned registration image before plugin lifecycle starts.
    pub fn register_builtin_image(&mut self, image: &'static RegistrationImage) {
        self.builtin_registration_images.push(image);
    }

    /// Resolve the configured plugin directory.
    ///
    /// VPP owns plugin-path selection in `plugin_main`; Hammer accepts
    /// `HAMMER_PLUGIN_DIR` and otherwise loads libraries beside the daemon.
    pub fn directory(&self) -> Result<PathBuf, PluginError> {
        if let Some(path) = std::env::var_os("HAMMER_PLUGIN_DIR") {
            return Ok(PathBuf::from(path));
        }
        let executable =
            std::env::current_exe().map_err(|source| PluginError::ExecutablePath { source })?;
        executable
            .parent()
            .map(Path::to_path_buf)
            .ok_or(PluginError::ExecutableParentMissing { executable })
    }

    /// Load configured roots and their transitive `load_after` dependencies.
    ///
    /// Failed transactions publish no runtime state. Every image opened during
    /// either a failed or successful transaction remains mapped until process
    /// exit because active Rust DSO unload has no proven teardown protocol.
    pub fn load(&mut self, host_version: &str, roots: &[String]) -> Result<(), PluginError> {
        let plugin_path = self.directory()?;
        if roots.is_empty() {
            return Ok(());
        }

        let mut requested = HashMap::with_capacity(roots.len());
        for (index, root) in roots.iter().enumerate() {
            let name = root.as_str();
            if let Some(first) = requested.get(name).copied() {
                return Err(PluginError::DuplicateRoot {
                    first,
                    duplicate: index,
                });
            }
            requested.insert(name, index);
        }

        let mut manifests = HashMap::with_capacity(roots.len());
        let mut visiting = HashSet::with_capacity(roots.len());
        let mut resolved = HashSet::with_capacity(roots.len());
        let mut load_order = Vec::with_capacity(roots.len());
        for root in roots {
            Self::resolve_load_order(
                host_version,
                &plugin_path,
                root,
                self,
                &mut manifests,
                &mut visiting,
                &mut resolved,
                &mut load_order,
            )?;
        }

        let mut transaction = Self {
            modules_by_plugin: HashMap::with_capacity(roots.len()),
            library_index_by_name: HashMap::with_capacity(roots.len()),
            load_order: Vec::with_capacity(roots.len()),
            builtin_registration_images: self.builtin_registration_images.clone(),
            libraries: Vec::with_capacity(roots.len()),
        };
        for name in load_order {
            let manifest = manifests
                .get(&name)
                .expect("resolved plugin must retain its manifest");
            transaction.load_one(&plugin_path, manifest)?;
        }

        if transaction.load_order.is_empty() {
            return Ok(());
        }

        let library_base = self.libraries.len();
        for (name, &index) in &transaction.library_index_by_name {
            self.library_index_by_name
                .insert(name.clone(), library_base + index);
        }
        self.modules_by_plugin.extend(transaction.modules_by_plugin);
        self.load_order.append(&mut transaction.load_order);
        self.libraries.append(&mut transaction.libraries);
        Ok(())
    }

    fn resolve_load_order(
        host_version: &str,
        plugin_path: &Path,
        name: &str,
        active: &PluginMain,
        manifests: &mut HashMap<String, PluginManifest>,
        visiting: &mut HashSet<String>,
        resolved: &mut HashSet<String>,
        load_order: &mut Vec<String>,
    ) -> Result<(), PluginError> {
        if active.library_index_by_name.contains_key(name) || resolved.contains(name) {
            return Ok(());
        }
        let path = plugin_path.join(libloading::library_filename(format!(
            "hammer_plugin_{name}"
        )));
        if !visiting.insert(name.to_owned()) {
            return Err(PluginError::Cycle { path });
        }

        let bytes = std::fs::read(&path).map_err(|source| PluginError::ManifestRead {
            path: path.clone(),
            source,
        })?;
        let file = object::File::parse(&*bytes).map_err(|source| PluginError::ManifestObject {
            path: path.clone(),
            message: source.to_string(),
        })?;
        let section = file
            .sections()
            .find(|section| {
                section
                    .name()
                    .is_ok_and(|name| matches!(name, "__hammer_plugin" | ".hammer_plugin"))
            })
            .ok_or_else(|| PluginError::ManifestMissing { path: path.clone() })?;
        let data = section
            .data()
            .map_err(|source| PluginError::ManifestObject {
                path: path.clone(),
                message: source.to_string(),
            })?;
        let data = std::str::from_utf8(data).map_err(|source| PluginError::ManifestObject {
            path: path.clone(),
            message: source.to_string(),
        })?;
        let manifest: PluginManifest =
            toml::from_str(data).map_err(|source| PluginError::ManifestParse {
                path: path.clone(),
                source,
            })?;
        if manifest.name != name {
            return Err(PluginError::NameMismatch { path });
        }
        host_meets_plugin_requirement(host_version, &manifest.version_required)?;
        for dependency in &manifest.load_after {
            Self::resolve_load_order(
                host_version,
                plugin_path,
                dependency,
                active,
                manifests,
                visiting,
                resolved,
                load_order,
            )?;
        }
        visiting.remove(name);
        resolved.insert(name.to_owned());
        manifests.insert(name.to_owned(), manifest);
        load_order.push(name.to_owned());
        Ok(())
    }

    fn load_one(
        &mut self,
        plugin_path: &Path,
        manifest: &PluginManifest,
    ) -> Result<(), PluginError> {
        let path = plugin_path.join(libloading::library_filename(format!(
            "hammer_plugin_{}",
            manifest.name
        )));
        let library = PluginLibrary::open(&path).map_err(|source| PluginError::LibraryOpen {
            path: path.clone(),
            source,
        })?;
        let module = read_plugin_module(&library).map_err(|source| PluginError::RootModule {
            path: path.clone(),
            source,
        })?;
        let metadata = module.metadata();
        if metadata.name() != manifest.name
            || metadata.version() != manifest.version
            || metadata.version_required() != manifest.version_required
            || metadata
                .load_after()
                .ne(manifest.load_after.iter().map(String::as_str))
        {
            return Err(PluginError::ManifestMetadataMismatch { path });
        }
        let library_index = self.libraries.len();
        self.libraries.push(library);
        self.modules_by_plugin.insert(manifest.name.clone(), module);
        self.library_index_by_name
            .insert(manifest.name.clone(), library_index);
        self.load_order.push(manifest.name.clone());
        Ok(())
    }

    pub fn loaded_plugins(&self) -> Vec<String> {
        self.load_order.clone()
    }

    #[inline]
    pub(crate) fn registration_generation(&self) -> u64 {
        (self.load_order.len() + 1) as u64
    }

    fn collect_registrations<T: Copy + 'static>(
        &self,
        inventory: impl Fn(&RegistrationImage) -> &'static [T],
    ) -> Vec<T> {
        let mut registrations = Vec::new();
        for image in &self.builtin_registration_images {
            registrations.extend_from_slice(inventory(image));
        }
        for name in &self.load_order {
            let Some(module) = self.modules_by_plugin.get(name) else {
                continue;
            };
            registrations.extend_from_slice(inventory(module.registration_image().get()));
        }
        registrations
    }

    pub(crate) fn init_functions(&self) -> Vec<InitFunction> {
        self.collect_registrations(RegistrationImage::init_functions)
    }

    pub(crate) fn config_functions(&self, early: bool) -> Vec<ConfigFunction> {
        self.collect_registrations(|image| image.config_functions(early))
    }

    pub(crate) fn worker_init_functions(&self) -> Vec<InitFunction> {
        self.collect_registrations(RegistrationImage::worker_init_functions)
    }

    pub(crate) fn main_loop_enter_functions(&self) -> Vec<InitFunction> {
        self.collect_registrations(RegistrationImage::main_loop_enter_functions)
    }

    pub(crate) fn main_loop_exit_functions(&self) -> Vec<InitFunction> {
        self.collect_registrations(RegistrationImage::main_loop_exit_functions)
    }

    pub(crate) fn graph_nodes(&self) -> Vec<NodeEntry> {
        self.collect_registrations(RegistrationImage::graph_nodes)
    }

    pub(crate) fn node_functions(&self) -> Vec<NodeFunctionRegistration> {
        self.collect_registrations(RegistrationImage::node_functions)
    }

    pub(crate) fn process_nodes(&self) -> Vec<ProcessEntry> {
        self.collect_registrations(RegistrationImage::process_nodes)
    }

    pub fn session_transports(&self) -> Vec<SessionTransportRegistration> {
        self.collect_registrations(RegistrationImage::session_transports)
    }

    pub fn session_transport(
        &self,
        name: &str,
    ) -> Result<SessionTransportRegistration, PluginError> {
        let mut found = None;
        for transport in self.session_transports() {
            if transport.name() != name {
                continue;
            }
            if found.is_some() {
                return Err(PluginError::SessionTransportDuplicate {
                    name: name.to_owned(),
                });
            }
            found = Some(transport);
        }
        found.ok_or_else(|| PluginError::SessionTransportMissing {
            name: name.to_owned(),
        })
    }

    pub fn session_apps(&self) -> Vec<SessionAppRegistration> {
        self.collect_registrations(RegistrationImage::session_apps)
    }

    pub fn session_app(&self, name: &str) -> Result<SessionAppRegistration, PluginError> {
        let mut found = None;
        for entry in self.session_apps() {
            if entry.name() != name {
                continue;
            }
            if found.is_some() {
                return Err(PluginError::SessionAppDuplicate {
                    name: name.to_owned(),
                });
            }
            found = Some(entry);
        }
        found.ok_or_else(|| PluginError::SessionAppMissing {
            name: name.to_owned(),
        })
    }

    pub fn binary_api_methods(&self) -> Vec<BinaryApiMethodEntry> {
        self.collect_registrations(RegistrationImage::binary_api_methods)
    }

    pub fn binary_api_method(&self, name: &str) -> Result<BinaryApiMethodEntry, PluginError> {
        let mut found = None;
        for entry in self.binary_api_methods() {
            if entry.name() != name {
                continue;
            }
            if found.is_some() {
                return Err(PluginError::BinaryApiMethodDuplicate {
                    name: name.to_owned(),
                });
            }
            found = Some(entry);
        }
        found.ok_or_else(|| PluginError::BinaryApiMethodMissing {
            name: name.to_owned(),
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

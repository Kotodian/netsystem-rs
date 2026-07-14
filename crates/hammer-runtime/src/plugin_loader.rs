//! Dynamic plugin load transaction (VPP-shaped `dlopen` ownership).
//!
//! Owns activation refcounts for shared dependencies. Activated plugins are
//! not `dlclose`d while still referenced (#95). Rollback only drops handles
//! acquired in a failed activate call.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use hammer_core::plugin::PluginRegistration;
use hammer_infra::vec::Vec;
use libloading::Library;

/// Platform cdylib file name for a plugin root, e.g. `libhammer_plugin_tun.dylib`.
pub fn plugin_cdylib_filename(plugin_name: &str) -> String {
    libloading::library_filename(format!("hammer_plugin_{plugin_name}"))
        .to_string_lossy()
        .into_owned()
}

/// Resolve `dir` / platform filename for `plugin_name`.
pub fn plugin_cdylib_path(dir: &Path, plugin_name: &str) -> PathBuf {
    dir.join(plugin_cdylib_filename(plugin_name))
}

#[derive(Debug, Default)]
struct HeldPlugin {
    refcount: usize,
    library: Option<Library>,
}

/// Load-versus-instance bookkeeping for dependency transactions.
#[derive(Debug)]
pub struct LoadTransaction {
    host_version: String,
    held: HashMap<String, HeldPlugin>,
    activated: Vec<String>,
}

impl LoadTransaction {
    pub fn new(host_version: impl Into<String>) -> Self {
        Self {
            host_version: host_version.into(),
            held: HashMap::new(),
            activated: Vec::new(),
        }
    }

    pub fn host_version(&self) -> &str {
        &self.host_version
    }

    pub fn activated(&self) -> &[String] {
        &self.activated
    }

    pub fn refcount(&self, name: &str) -> usize {
        self.held.get(name).map(|held| held.refcount).unwrap_or(0)
    }

    pub fn is_held(&self, name: &str) -> bool {
        self.refcount(name) > 0
    }

    pub fn has_library(&self, name: &str) -> bool {
        self.held
            .get(name)
            .and_then(|held| held.library.as_ref())
            .is_some()
    }

    /// `dlsym` registration from a held library.
    pub fn registration(&self, name: &str) -> Result<&'static PluginRegistration, String> {
        let library = self
            .held
            .get(name)
            .and_then(|held| held.library.as_ref())
            .ok_or_else(|| format!("plugin `{name}` is not held"))?;
        read_plugin_registration(library)
    }

    /// Open a cdylib and retain it under `name` (refcount +1).
    pub fn open_library(&mut self, name: &str, path: &Path) -> Result<(), String> {
        if let Some(held) = self.held.get_mut(name) {
            held.refcount += 1;
            return Ok(());
        }
        let library = unsafe { Library::new(path) }.map_err(|err| err.to_string())?;
        self.held.insert(
            name.to_owned(),
            HeldPlugin {
                refcount: 1,
                library: Some(library),
            },
        );
        Ok(())
    }

    /// Acquire every plugin in `order` (dependencies first). On failure, release
    /// only the plugins acquired in this call (rollback).
    pub fn activate_in_order(
        &mut self,
        order: &[&str],
        mut activate: impl FnMut(&str) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut acquired: Vec<String> = Vec::new();
        for name in order {
            match activate(name) {
                Ok(()) => {
                    let entry = self.held.entry((*name).to_owned()).or_default();
                    entry.refcount += 1;
                    if !self.activated.iter().any(|activated| activated == name) {
                        self.activated.push((*name).to_owned());
                    }
                    acquired.push((*name).to_owned());
                }
                Err(err) => {
                    self.release_plan_names(&acquired);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    /// Drop one reference for each name in the plan (shared deps stay if others hold them).
    pub fn release_plan(&mut self, order: &[&str]) {
        let names: Vec<String> = order.iter().map(|name| (*name).to_owned()).collect();
        self.release_plan_names(&names);
    }

    pub fn release(&mut self, name: &str) {
        self.release_plan(&[name]);
    }

    fn release_plan_names(&mut self, names: &[String]) {
        for name in names.iter().rev() {
            let Some(entry) = self.held.get_mut(name) else {
                continue;
            };
            if entry.refcount == 0 {
                continue;
            }
            entry.refcount -= 1;
            if entry.refcount == 0 {
                self.held.remove(name);
                self.activated.retain(|activated| activated != name);
            }
        }
    }
}

/// Collect plugin-private inventory slices after libraries are held.
///
/// `collect` is typically a `dlsym` export returning records isomorphic with
/// existing init/graph entries. No Registrar type (#95).
pub fn collect_plugin_inventory<T>(
    plugin_names: &[&str],
    mut collect: impl FnMut(&str) -> Result<&'static [T], String>,
) -> Result<Vec<&'static T>, String> {
    let mut out = Vec::new();
    for name in plugin_names {
        let slice = collect(name)?;
        out.extend(slice.iter());
    }
    Ok(out)
}

/// `dlsym` the plugin registration export (`hammer_plugin_registration`).
///
/// The symbol returns a pointer to a static `PluginRegistration` owned by the
/// cdylib (Rust layout; not a C registration block).
pub fn read_plugin_registration(library: &Library) -> Result<&'static PluginRegistration, String> {
    type RegistrationFn = unsafe extern "C" fn() -> *const PluginRegistration;
    let symbol = unsafe { library.get::<RegistrationFn>(b"hammer_plugin_registration\0") }
        .map_err(|err| err.to_string())?;
    let ptr = unsafe { symbol() };
    if ptr.is_null() {
        return Err("hammer_plugin_registration returned null".into());
    }
    Ok(unsafe { &*ptr })
}

/// Workspace `target/{debug,release}` directory for integration tests.
pub fn workspace_target_dir() -> PathBuf {
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    if let Ok(dir) = std::env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(dir).join(profile);
    }
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.pop(); // crates/
    dir.pop(); // workspace root
    dir.join("target").join(profile)
}

/// Resolve a built `libhammer_plugin_<name>.{dylib,so}` under the workspace target dir.
pub fn built_plugin_cdylib_path(plugin_name: &str) -> PathBuf {
    let target = workspace_target_dir();
    let primary = plugin_cdylib_path(&target, plugin_name);
    if primary.is_file() {
        return primary;
    }
    // Path-dep builds place the cdylib under `deps/`.
    plugin_cdylib_path(&target.join("deps"), plugin_name)
}

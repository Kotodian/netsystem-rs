//! Host helpers for the VPP-shaped plugin load prototype.
//!
//! Library handle = `libloading::Library` (VPP `plugin_info_t.handle` role).

use std::path::{Path, PathBuf};

use libloading::Library;

pub fn plugin_cdylib_path(cargo_package: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.pop();
    path.push("target");
    path.push(if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    });
    let lib_stem = cargo_package.replace('-', "_");
    path.push(libloading::library_filename(&lib_stem));
    path
}

pub fn load_plugin(path: &Path) -> Result<Library, libloading::Error> {
    unsafe { Library::new(path) }
}

/// Node-shaped `dlsym` process: same roles as `NodeProcessFn`
/// (`DataPlaneRuntime`, `NodeRuntimeData` words, host-owned `BufferFrame`).
pub type PluginNodeProcess = unsafe extern "C" fn(
    *const hammer_runtime::DataPlaneRuntime,
    *const u64,
    *mut hammer_core::data_plane::BufferFrame,
) -> usize;

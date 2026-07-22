//! The `abi_stable` root-module boundary used by [`crate::plugin::PluginMain`].

use abi_stable::library::{AbiHeaderRef, ROOT_MODULE_LOADER_NAME_WITH_NUL};
use libloading::Library;

use crate::PluginModuleRef;

#[doc(hidden)]
#[derive(Debug, thiserror::Error)]
pub enum PluginModuleLoadError {
    #[error("the abi_stable root-module header is missing")]
    Header {
        #[source]
        source: libloading::Error,
    },
    #[error("the abi_stable root-module header is invalid")]
    HeaderUpgrade {
        #[source]
        source: abi_stable::library::LibraryError,
    },
    #[error("the plugin root module is incompatible")]
    Module {
        #[source]
        source: abi_stable::library::LibraryError,
    },
}

/// Loads the sole `abi_stable` root module while `library` remains retained by
/// `PluginMain`.
pub(crate) fn read_plugin_module(
    library: &Library,
) -> Result<PluginModuleRef, PluginModuleLoadError> {
    // SAFETY: `abi_stable::export_root_module` emits this header in every
    // plugin DSO. The copied header is valid while PluginMain retains library.
    let header = unsafe {
        *library
            .get::<AbiHeaderRef>(ROOT_MODULE_LOADER_NAME_WITH_NUL.as_bytes())
            .map_err(|source| PluginModuleLoadError::Header { source })?
    };
    let header = header
        .upgrade()
        .map_err(|source| PluginModuleLoadError::HeaderUpgrade { source })?;
    header
        .init_root_module::<PluginModuleRef>()
        .map_err(|source| PluginModuleLoadError::Module { source })
}

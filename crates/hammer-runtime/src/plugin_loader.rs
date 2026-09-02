//! The `abi_stable` root-module boundary used by [`crate::plugin::PluginMain`].

use std::path::Path;
#[cfg(unix)]
use std::ptr::NonNull;

use abi_stable::library::{AbiHeaderRef, ROOT_MODULE_LOADER_NAME_WITH_NUL};
#[cfg(not(unix))]
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

/// Process-lifetime plugin image.
///
/// Active Rust DSOs contain TLS, static registration data, and Drop glue whose
/// teardown cannot be ordered safely around every runtime owner. The loader
/// handle is intentionally retained until the operating system reclaims the
/// process; runtime plugin unload requires a separate drain protocol.
pub(crate) struct PluginLibrary {
    #[cfg(unix)]
    handle: NonNull<libc::c_void>,
    #[cfg(not(unix))]
    library: &'static Library,
}

// SAFETY: POSIX permits a successful dlopen handle to be used by dlsym from
// multiple threads. Hammer never closes this process-lifetime handle.
#[cfg(unix)]
unsafe impl Send for PluginLibrary {}

// SAFETY: see the Send implementation. Symbol lookup does not mutate the
// handle, and PluginMain publishes the library only after validation.
#[cfg(unix)]
unsafe impl Sync for PluginLibrary {}

impl PluginLibrary {
    pub(crate) fn open(path: &Path) -> Result<Self, libloading::Error> {
        #[cfg(unix)]
        {
            use libloading::os::unix::{Library as UnixLibrary, RTLD_LOCAL, RTLD_NOW};

            // SAFETY: loading executes plugin initializers. PluginMain validates
            // the root module before publishing it, and RTLD_NODELETE plus the
            // retained handle prevent termination routines from running during
            // ordinary runtime teardown.
            let library = unsafe {
                UnixLibrary::open(Some(path), RTLD_NOW | RTLD_LOCAL | libc::RTLD_NODELETE)
            }?;
            let handle = NonNull::new(library.into_raw())
                .expect("a successfully opened plugin library has a non-null handle");
            Ok(Self { handle })
        }

        #[cfg(not(unix))]
        {
            // SAFETY: non-Unix targets currently have no supported Hammer
            // plugin deployment. Retaining the handle preserves the same
            // process-lifetime contract until a platform policy is defined.
            let library = unsafe { Library::new(path) }?;
            Ok(Self {
                library: Box::leak(Box::new(library)),
            })
        }
    }

    pub(crate) fn symbol<T>(&self, name: &[u8]) -> Result<*const T, libloading::Error> {
        #[cfg(unix)]
        {
            use libloading::os::unix::Library as UnixLibrary;
            // SAFETY: `self.handle` is a live RTLD_NODELETE handle retained for
            // the process lifetime; the temporary owner is converted back below.
            let temporary = unsafe { UnixLibrary::from_raw(self.handle.as_ptr()) };
            // SAFETY: the caller supplies the ABI type for the named symbol;
            // the returned address remains valid while this library is mapped.
            let pointer = unsafe { temporary.get::<*const T>(name).map(|symbol| *symbol) };
            let handle = temporary.into_raw();
            debug_assert_eq!(handle, self.handle.as_ptr());
            pointer
        }
        #[cfg(not(unix))]
        {
            // SAFETY: the retained process-lifetime library owns the symbol.
            unsafe { self.library.get::<*const T>(name).map(|symbol| *symbol) }
        }
    }
}

/// Loads the sole `abi_stable` root module while its process-lifetime image
/// remains retained.
pub(crate) fn read_plugin_module(
    library: &PluginLibrary,
) -> Result<PluginModuleRef, PluginModuleLoadError> {
    // SAFETY: `abi_stable::export_root_module` emits this header in every
    // plugin DSO. The copied header remains valid because PluginLibrary never
    // unloads its image during the process lifetime.
    #[cfg(unix)]
    let header = {
        use libloading::os::unix::Library as UnixLibrary;

        // SAFETY: handle came from UnixLibrary::into_raw and remains owned by
        // this PluginLibrary. The temporary owner is converted back to the
        // same raw handle before it can run Drop.
        let temporary = unsafe { UnixLibrary::from_raw(library.handle.as_ptr()) };
        let result = {
            // SAFETY: the abi_stable export defines this symbol and type. The
            // copied header remains valid while the process-lifetime image is
            // mapped.
            unsafe {
                temporary
                    .get::<AbiHeaderRef>(ROOT_MODULE_LOADER_NAME_WITH_NUL.as_bytes())
                    .map(|header| *header)
                    .map_err(|source| PluginModuleLoadError::Header { source })
            }
        };
        let handle = temporary.into_raw();
        debug_assert_eq!(handle, library.handle.as_ptr());
        result?
    };

    #[cfg(not(unix))]
    // SAFETY: the abi_stable export defines this symbol and type, and the
    // process-lifetime library remains mapped while the header is used.
    let header = unsafe {
        *library
            .library
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

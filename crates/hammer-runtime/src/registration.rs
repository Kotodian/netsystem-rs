//! Process-wide executable registration authority.
//!
//! Every registration-bearing link image owns one [`RegistrationImage`]. Its
//! load constructor links the image here, matching VPP's constructor-populated
//! `vlib_global_main`. Plugin metadata and DSO ownership remain in
//! `PluginMain`; this module only catalogs executable registrations.

use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};

use hammer_infra::vec::Vec;

use crate::init::InitFunction;
use crate::node::{NodeEntry, NodeFunctionRegistration};
use crate::process::ProcessEntry;

static REGISTRATION_HEAD: AtomicPtr<RegistrationImage> = AtomicPtr::new(ptr::null_mut());
static REGISTRATION_LOCK: AtomicBool = AtomicBool::new(false);
static REGISTRATION_GENERATION: AtomicU64 = AtomicU64::new(0);

/// One link image's immutable executable inventories and intrusive list link.
///
/// This type is public only so macro expansions in dependent DSOs can create a
/// static value. It is not a supported Hammer interface.
#[doc(hidden)]
pub struct RegistrationImage {
    inventories: UnsafeCell<MaybeUninit<RegistrationInventories>>,
    next: AtomicPtr<RegistrationImage>,
    linked: AtomicBool,
}

struct RegistrationInventories {
    init_functions: &'static [InitFunction],
    config_functions: &'static [InitFunction],
    early_config_functions: &'static [InitFunction],
    main_loop_enter_functions: &'static [InitFunction],
    main_loop_exit_functions: &'static [InitFunction],
    worker_init_functions: &'static [InitFunction],
    graph_nodes: &'static [NodeEntry],
    node_functions: &'static [NodeFunctionRegistration],
    process_nodes: &'static [ProcessEntry],
}

// SAFETY: inventories are initialized before publication and thereafter read
// only while REGISTRATION_LOCK is held. The same lock serializes unlinking.
unsafe impl Sync for RegistrationImage {}

impl RegistrationImage {
    #[doc(hidden)]
    pub const fn new() -> Self {
        Self {
            inventories: UnsafeCell::new(MaybeUninit::uninit()),
            next: AtomicPtr::new(ptr::null_mut()),
            linked: AtomicBool::new(false),
        }
    }

    /// Initializes and links this image into the process-wide authority.
    ///
    /// # Safety
    /// All inventories must belong to the caller's static link image and stay
    /// mapped until the matching call to [`Self::unlink`].
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn link(
        &'static self,
        init_functions: &'static [InitFunction],
        config_functions: &'static [InitFunction],
        early_config_functions: &'static [InitFunction],
        main_loop_enter_functions: &'static [InitFunction],
        main_loop_exit_functions: &'static [InitFunction],
        worker_init_functions: &'static [InitFunction],
        graph_nodes: &'static [NodeEntry],
        node_functions: &'static [NodeFunctionRegistration],
        process_nodes: &'static [ProcessEntry],
    ) {
        with_registration_lock(|| {
            if self.linked.load(Ordering::Relaxed) {
                return;
            }
            let inventories = RegistrationInventories {
                init_functions,
                config_functions,
                early_config_functions,
                main_loop_enter_functions,
                main_loop_exit_functions,
                worker_init_functions,
                graph_nodes,
                node_functions,
                process_nodes,
            };
            // SAFETY: the lock gives exclusive access, and the image is not
            // published until this initialization is complete.
            unsafe { (*self.inventories.get()).write(inventories) };
            let head = REGISTRATION_HEAD.load(Ordering::Relaxed);
            self.next.store(head, Ordering::Relaxed);
            self.linked.store(true, Ordering::Relaxed);
            REGISTRATION_HEAD.store(ptr::from_ref(self).cast_mut(), Ordering::Release);
            REGISTRATION_GENERATION.fetch_add(1, Ordering::Release);
        });
    }

    /// Removes this image before its code and static data are unmapped.
    ///
    /// # Safety
    /// The caller must be the unload destructor paired with [`Self::link`], and
    /// runtime code must not retain copied registrations after the provider DSO
    /// handle is released.
    #[doc(hidden)]
    pub unsafe fn unlink(&'static self) {
        with_registration_lock(|| {
            if !self.linked.swap(false, Ordering::Relaxed) {
                return;
            }

            let target = ptr::from_ref(self).cast_mut();
            let mut previous: *mut RegistrationImage = ptr::null_mut();
            let mut current = REGISTRATION_HEAD.load(Ordering::Acquire);
            while !current.is_null() {
                // SAFETY: all nodes reachable from REGISTRATION_HEAD are
                // linked static RegistrationImages. The lock prevents an
                // unload destructor from unmapping a node during traversal.
                let next = unsafe { (*current).next.load(Ordering::Relaxed) };
                if current == target {
                    if previous.is_null() {
                        REGISTRATION_HEAD.store(next, Ordering::Release);
                    } else {
                        // SAFETY: previous was reached from the same protected
                        // list and remains mapped while the lock is held.
                        unsafe { (*previous).next.store(next, Ordering::Relaxed) };
                    }
                    self.next.store(ptr::null_mut(), Ordering::Relaxed);
                    REGISTRATION_GENERATION.fetch_add(1, Ordering::Release);
                    return;
                }
                previous = current;
                current = next;
            }
        });
    }
}

#[inline]
pub(crate) fn generation() -> u64 {
    REGISTRATION_GENERATION.load(Ordering::Acquire)
}

struct RegistrationLockGuard;

impl Drop for RegistrationLockGuard {
    fn drop(&mut self) {
        REGISTRATION_LOCK.store(false, Ordering::Release);
    }
}

fn with_registration_lock<R>(operation: impl FnOnce() -> R) -> R {
    while REGISTRATION_LOCK
        .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        spin_loop();
    }
    let guard = RegistrationLockGuard;
    let result = operation();
    drop(guard);
    result
}

fn collect<T: Copy>(inventory: impl Fn(&RegistrationInventories) -> &[T]) -> Vec<T> {
    with_registration_lock(|| {
        let mut result = Vec::new();
        let mut current = REGISTRATION_HEAD.load(Ordering::Acquire);
        while !current.is_null() {
            // SAFETY: the protected list contains only linked static images;
            // copied entries stay valid because the process-global PluginMain
            // owns every activated provider DSO until process exit. Failed
            // transactions unlink before returning their error.
            let image = unsafe { &*current };
            // SAFETY: a linked image initializes inventories before
            // publication, and the lock prevents unlink during this read.
            let inventories = unsafe { (&*image.inventories.get()).assume_init_ref() };
            result.extend_from_slice(inventory(inventories));
            current = image.next.load(Ordering::Relaxed);
        }
        result
    })
}

pub(crate) fn init_functions() -> Vec<InitFunction> {
    collect(|image| image.init_functions)
}

pub(crate) fn config_functions(early: bool) -> Vec<InitFunction> {
    if early {
        collect(|image| image.early_config_functions)
    } else {
        collect(|image| image.config_functions)
    }
}

pub(crate) fn worker_init_functions() -> Vec<InitFunction> {
    collect(|image| image.worker_init_functions)
}

pub(crate) fn main_loop_enter_functions() -> Vec<InitFunction> {
    collect(|image| image.main_loop_enter_functions)
}

pub(crate) fn main_loop_exit_functions() -> Vec<InitFunction> {
    collect(|image| image.main_loop_exit_functions)
}

pub(crate) fn graph_nodes() -> Vec<NodeEntry> {
    collect(|image| image.graph_nodes)
}

pub(crate) fn node_functions() -> Vec<NodeFunctionRegistration> {
    collect(|image| image.node_functions)
}

pub(crate) fn process_nodes() -> Vec<ProcessEntry> {
    collect(|image| image.process_nodes)
}

/// Declares one link image's private inventories and load/unload hooks.
#[doc(hidden)]
#[macro_export]
macro_rules! __declare_registration_image {
    () => {
        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_INIT_FUNCTIONS: [$crate::init::InitFunction] = [..];

        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_CONFIG_FUNCTIONS: [$crate::init::InitFunction] = [..];

        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_EARLY_CONFIG_FUNCTIONS: [$crate::init::InitFunction] = [..];

        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_MAIN_LOOP_ENTER_FUNCTIONS: [$crate::init::InitFunction] = [..];

        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_MAIN_LOOP_EXIT_FUNCTIONS: [$crate::init::InitFunction] = [..];

        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_WORKER_INIT_FUNCTIONS: [$crate::init::InitFunction] = [..];

        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_GRAPH_NODES: [$crate::NodeEntry] = [..];

        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_NODE_FUNCTIONS: [$crate::node::NodeFunctionRegistration] = [..];

        #[::linkme::distributed_slice]
        static __HAMMER_IMAGE_PROCESS_NODES: [$crate::ProcessEntry] = [..];

        static __HAMMER_REGISTRATION_IMAGE: $crate::__private::RegistrationImage =
            $crate::__private::RegistrationImage::new();

        #[cfg(target_vendor = "apple")]
        unsafe extern "C" {
            static __dso_handle: u8;

            fn __cxa_atexit(
                callback: extern "C" fn(*mut ::core::ffi::c_void),
                argument: *mut ::core::ffi::c_void,
                dso_handle: *const ::core::ffi::c_void,
            ) -> ::core::ffi::c_int;
        }

        extern "C" fn __hammer_link_registration_image() {
            // SAFETY: this image is static and its platform destructor unlinks
            // it before the containing image is unmapped.
            unsafe {
                __HAMMER_REGISTRATION_IMAGE.link(
                    &__HAMMER_IMAGE_INIT_FUNCTIONS,
                    &__HAMMER_IMAGE_CONFIG_FUNCTIONS,
                    &__HAMMER_IMAGE_EARLY_CONFIG_FUNCTIONS,
                    &__HAMMER_IMAGE_MAIN_LOOP_ENTER_FUNCTIONS,
                    &__HAMMER_IMAGE_MAIN_LOOP_EXIT_FUNCTIONS,
                    &__HAMMER_IMAGE_WORKER_INIT_FUNCTIONS,
                    &__HAMMER_IMAGE_GRAPH_NODES,
                    &__HAMMER_IMAGE_NODE_FUNCTIONS,
                    &__HAMMER_IMAGE_PROCESS_NODES,
                )
            }

            #[cfg(target_vendor = "apple")]
            {
                // Clang lowers __attribute__((destructor)) this way on Apple.
                // __dso_handle is a linker symbol; __cxa_atexit requires the
                // symbol's address, not the pointer-sized contents there.
                let status = unsafe {
                    __cxa_atexit(
                        __hammer_unlink_registration_image,
                        ::core::ptr::null_mut(),
                        (&raw const __dso_handle).cast(),
                    )
                };
                if status != 0 {
                    // SAFETY: link completed above and this image is still
                    // mapped while its constructor is running.
                    unsafe { __HAMMER_REGISTRATION_IMAGE.unlink() }
                    ::std::process::abort();
                }
            }
        }

        #[cfg(target_vendor = "apple")]
        extern "C" fn __hammer_unlink_registration_image(_: *mut ::core::ffi::c_void) {
            // SAFETY: dyld invokes the DSO-scoped callback before unmapping the
            // containing image and its private inventories.
            unsafe { __HAMMER_REGISTRATION_IMAGE.unlink() }
        }

        #[cfg(not(target_vendor = "apple"))]
        extern "C" fn __hammer_unlink_registration_image() {
            // SAFETY: the platform invokes this destructor before unmapping
            // the containing image and its private inventories.
            unsafe { __HAMMER_REGISTRATION_IMAGE.unlink() }
        }

        #[used]
        #[cfg_attr(
            target_vendor = "apple",
            unsafe(link_section = "__DATA,__mod_init_func,mod_init_funcs")
        )]
        #[cfg_attr(not(target_vendor = "apple"), unsafe(link_section = ".init_array"))]
        static __HAMMER_REGISTRATION_CONSTRUCTOR: extern "C" fn() =
            __hammer_link_registration_image;

        #[cfg(not(target_vendor = "apple"))]
        #[used]
        #[unsafe(link_section = ".fini_array")]
        static __HAMMER_REGISTRATION_DESTRUCTOR: extern "C" fn() =
            __hammer_unlink_registration_image;
    };
}

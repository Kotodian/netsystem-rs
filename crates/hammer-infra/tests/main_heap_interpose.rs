use std::ffi::c_void;
use std::ptr;

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn malloc_size(pointer: *const c_void) -> usize;
    fn malloc_type_free(pointer: *mut c_void, type_id: u64);
    fn malloc_type_malloc(size: usize, type_id: u64) -> *mut c_void;
}

#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn malloc_usable_size(pointer: *mut c_void) -> usize;
}

#[test]
fn native_malloc_family_switches_at_main_heap_ready() {
    // SAFETY: every pointer is checked for null, used only within its allocated
    // size, and released exactly once through the matching C allocation API.
    unsafe {
        let bootstrap_to_free = libc::malloc(64);
        assert!(!bootstrap_to_free.is_null());

        let bootstrap_to_migrate = libc::malloc(64).cast::<u8>();
        assert!(!bootstrap_to_migrate.is_null());
        for offset in 0..64 {
            bootstrap_to_migrate.add(offset).write(offset as u8);
        }

        hammer_infra::main_heap::init(256 << 20).expect("initialize fixed main heap");

        libc::free(bootstrap_to_free);

        let migrated = libc::realloc(bootstrap_to_migrate.cast::<c_void>(), 128).cast::<u8>();
        assert!(!migrated.is_null());
        assert_ne!(migrated, bootstrap_to_migrate);
        for offset in 0..64 {
            assert_eq!(migrated.add(offset).read(), offset as u8);
        }
        assert!(native_usable_size(migrated.cast::<c_void>()) >= 128);
        libc::free(migrated.cast::<c_void>());

        let allocated = libc::malloc(96).cast::<u8>();
        assert!(!allocated.is_null());
        libc::free(allocated.cast::<c_void>());

        let current_exe = std::env::current_exe().expect("resolve executable through libstd");
        assert!(!current_exe.as_os_str().as_encoded_bytes().is_empty());

        let zeroed = libc::calloc(32, 4).cast::<u8>();
        assert!(!zeroed.is_null());
        assert!(
            std::slice::from_raw_parts(zeroed, 128)
                .iter()
                .all(|byte| *byte == 0)
        );
        libc::free(zeroed.cast::<c_void>());

        let mut aligned = ptr::null_mut();
        assert_eq!(libc::posix_memalign(&mut aligned, 4096, 257), 0);
        assert!(!aligned.is_null());
        assert_eq!(aligned as usize % 4096, 0);
        libc::free(aligned);

        #[cfg(target_os = "macos")]
        {
            let typed = malloc_type_malloc(144, 0x48414d4d4552).cast::<u8>();
            assert!(!typed.is_null());
            malloc_type_free(typed.cast::<c_void>(), 0x48414d4d4552);
        }
    }
}

unsafe fn native_usable_size(pointer: *mut c_void) -> usize {
    #[cfg(target_os = "macos")]
    {
        // SAFETY: the caller supplies a live malloc-family pointer.
        unsafe { malloc_size(pointer) }
    }

    #[cfg(target_os = "linux")]
    {
        // SAFETY: the caller supplies a live malloc-family pointer.
        unsafe { malloc_usable_size(pointer) }
    }
}

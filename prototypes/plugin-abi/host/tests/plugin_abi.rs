use std::alloc::{GlobalAlloc, Layout, System};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use hammer_core::data_plane::{
    BufferFrame, BufferHeaderCacheline0, BufferHeaderCacheline1, BufferPool, Index,
};
use hammer_plugin_abi_host::{load_plugin, plugin_cdylib_path, PluginNodeProcess};
use hammer_runtime::node::NodeRuntimeData;
use libloading::Symbol;

#[global_allocator]
static COUNTED: CountedAlloc = CountedAlloc;

struct CountedAlloc;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountedAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

fn build_cdylib(package: &str) {
    let status = Command::new("cargo")
        .args(["build", "-p", package])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .status()
        .expect("spawn cargo build");
    assert!(status.success(), "cargo build -p {package} failed");
}

#[test]
fn plugin_layout_matches_host_index_and_buffer_headers() {
    build_cdylib("hammer-plugin-abi-plugin");
    let lib = load_plugin(&plugin_cdylib_path("hammer-plugin-abi-plugin")).expect("dlopen");

    type LayoutFn = unsafe extern "C" fn() -> usize;
    unsafe {
        let index_size: Symbol<LayoutFn> = lib.get(b"hammer_plugin_index_size\0").unwrap();
        let index_align: Symbol<LayoutFn> = lib.get(b"hammer_plugin_index_align\0").unwrap();
        let cl0_size: Symbol<LayoutFn> = lib
            .get(b"hammer_plugin_buffer_header_cl0_size\0")
            .unwrap();
        let cl1_size: Symbol<LayoutFn> = lib
            .get(b"hammer_plugin_buffer_header_cl1_size\0")
            .unwrap();

        assert_eq!(index_size(), std::mem::size_of::<Index>());
        assert_eq!(index_align(), std::mem::align_of::<Index>());
        assert_eq!(cl0_size(), std::mem::size_of::<BufferHeaderCacheline0>());
        assert_eq!(cl1_size(), std::mem::size_of::<BufferHeaderCacheline1>());
    }
}

#[test]
fn plugin_node_process_uses_host_buffer_frame_without_hot_path_alloc() {
    build_cdylib("hammer-plugin-abi-plugin");
    let lib = load_plugin(&plugin_cdylib_path("hammer-plugin-abi-plugin")).expect("dlopen");

    let process: Symbol<PluginNodeProcess> = unsafe {
        lib.get(b"hammer_plugin_node_process\0")
            .expect("dlsym node process")
    };

    let mut frame = BufferFrame::with_capacity(256);
    let pool = BufferPool::with_capacity(64, 4);
    let index = pool.alloc_index_with_bytes(b"x").expect("alloc");
    frame.push_index(index).expect("push");
    assert_eq!(frame.len(), 1);

    let runtime_data = NodeRuntimeData::empty();
    let words = [
        runtime_data.word(0),
        runtime_data.word(1),
        runtime_data.word(2),
        runtime_data.word(3),
    ];
    let before = ALLOC_COUNT.load(Ordering::SeqCst);
    let observed = unsafe { process(words.as_ptr(), &mut frame) };
    let after = ALLOC_COUNT.load(Ordering::SeqCst);

    assert_eq!(observed, 1);
    assert_eq!(frame.len(), 1, "frame stays host-owned; no shadow copy");
    assert_eq!(after, before, "hot-path node process must not allocate");
}

#[test]
fn plugin_panic_is_contained_at_abi_boundary() {
    build_cdylib("hammer-plugin-abi-plugin");
    let lib = load_plugin(&plugin_cdylib_path("hammer-plugin-abi-plugin")).expect("dlopen");

    type PanicProbe = unsafe extern "C" fn(u8) -> i32;
    let probe: Symbol<PanicProbe> = unsafe { lib.get(b"hammer_plugin_panic_probe\0").unwrap() };

    let contained = unsafe { probe(1) };
    assert_eq!(contained, 1, "plugin must catch panic before returning across ABI");
    let ok = unsafe { probe(0) };
    assert_eq!(ok, 0);
}

#[test]
fn libloading_library_close_releases_handle() {
    build_cdylib("hammer-plugin-abi-plugin");
    let path = plugin_cdylib_path("hammer-plugin-abi-plugin");
    let lib = load_plugin(&path).expect("dlopen");
    drop(lib);
    let lib_again = load_plugin(&path).expect("dlopen after close");
    drop(lib_again);
}

#[test]
fn abi_stable_raw_library_can_be_owned_and_dropped_without_root_module() {
    build_cdylib("hammer-plugin-abi-plugin");
    let path = plugin_cdylib_path("hammer-plugin-abi-plugin");
    let raw = abi_stable::library::RawLibrary::load_at(&path).expect("RawLibrary::load_at");
    drop(raw);
}

#[test]
fn abi_stable_root_module_header_api_is_not_used_for_hammer_plugins() {
    // `lib_header_from_path` is the RootModule load helper. Its docs and
    // implementation (`mem::forget(raw_lib)`) leak the library. Our plugin
    // intentionally exports Node-shaped `dlsym` symbols instead of
    // `#[export_root_module]`, so this API must fail — proving we bypass it.
    build_cdylib("hammer-plugin-abi-plugin");
    let path = plugin_cdylib_path("hammer-plugin-abi-plugin");
    match abi_stable::library::lib_header_from_path(&path) {
        Ok(_) => panic!("hammer plugins must not export abi_stable RootModule"),
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("GetSymbol") || msg.contains("symbol") || msg.contains("root"),
                "expected missing root-module symbol, got: {msg}"
            );
        }
    }
}

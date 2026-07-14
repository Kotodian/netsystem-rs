//! Prototype plugin cdylib.
//!
//! Exports layout probes and a Node-shaped process over canonical
//! `hammer-core` / `hammer-runtime` types. No parallel Frame/Buffer types.

use hammer_core::data_plane::{
    BufferFrame, BufferHeaderCacheline0, BufferHeaderCacheline1, Index,
};
use hammer_runtime::node::NodeRuntimeData;
use hammer_runtime::DataPlaneRuntime;

#[unsafe(no_mangle)]
pub extern "C" fn hammer_plugin_index_size() -> usize {
    core::mem::size_of::<Index>()
}

#[unsafe(no_mangle)]
pub extern "C" fn hammer_plugin_index_align() -> usize {
    core::mem::align_of::<Index>()
}

#[unsafe(no_mangle)]
pub extern "C" fn hammer_plugin_buffer_header_cl0_size() -> usize {
    core::mem::size_of::<BufferHeaderCacheline0>()
}

#[unsafe(no_mangle)]
pub extern "C" fn hammer_plugin_buffer_header_cl1_size() -> usize {
    core::mem::size_of::<BufferHeaderCacheline1>()
}

/// Node-shaped process: observe host `BufferFrame` length without copying
/// payload bytes or allocating. Returns `frame.len()`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn hammer_plugin_node_process(
    _runtime: *const DataPlaneRuntime,
    runtime_data_words: *const u64,
    frame: *mut BufferFrame,
) -> usize {
    if runtime_data_words.is_null() || frame.is_null() {
        return 0;
    }
    let words = unsafe { *(runtime_data_words as *const [u64; 4]) };
    let _runtime_data = NodeRuntimeData::from_words(words);
    unsafe { (*frame).len() }
}

/// Panic is caught inside the plugin so unwind never crosses the ABI.
/// Returns 1 when a panic was contained, 0 otherwise.
#[unsafe(no_mangle)]
pub extern "C" fn hammer_plugin_panic_probe(should_panic: u8) -> i32 {
    let result = std::panic::catch_unwind(|| {
        if should_panic != 0 {
            panic!("prototype plugin panic");
        }
    });
    match result {
        Ok(()) => 0,
        Err(_) => 1,
    }
}

use std::ffi::c_void;
use std::ptr;

const CHUNK_SIZE: usize = 1 << 20;
const MAX_CHUNKS: usize = 1024;

#[test]
fn exhausted_main_heap_never_falls_back_to_system_malloc() {
    let requested = hammer_infra::main_heap::minimum_capacity().max(64 << 20);
    hammer_infra::main_heap::init(requested).expect("initialize fixed main heap");

    let mut allocations = [ptr::null_mut::<c_void>(); MAX_CHUNKS];
    let mut allocated = 0usize;
    let mut exhausted = false;

    // SAFETY: each successful allocation is stored exactly once, used only for
    // provenance inspection, and released exactly once before assertions run.
    unsafe {
        while allocated < allocations.len() {
            let pointer = libc::malloc(CHUNK_SIZE);
            if pointer.is_null() {
                exhausted = true;
                break;
            }
            allocations[allocated] = pointer;
            allocated += 1;
        }

        for pointer in allocations.into_iter().take(allocated) {
            libc::free(pointer);
        }
    }

    assert!(exhausted);
    assert!(allocated < MAX_CHUNKS);
}

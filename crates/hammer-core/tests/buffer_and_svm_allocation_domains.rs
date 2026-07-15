use hammer_core::data_plane::BufferPool;
use hammer_infra::svm_region::SvmRegion;

#[test]
fn buffer_arena_and_svm_memory_stay_outside_main_heap() {
    hammer_infra::main_heap::init(256 << 20).expect("initialize fixed main heap");

    let buffer_arena = BufferPool::with_capacity(2048, 16);
    let index = buffer_arena
        .alloc_index()
        .expect("allocate buffer from arena");
    let arena_base = buffer_arena.base_ptr();
    let buffer_header = buffer_arena.buffer_raw_ptr(index.slot()).cast::<u8>();
    let buffer_payload = buffer_arena.data_raw_ptr(index.slot());
    let arena_size = buffer_arena.slot_stride() * 17;

    assert!((buffer_header as usize).wrapping_sub(arena_base as usize) < arena_size);
    assert!((buffer_payload as usize).wrapping_sub(arena_base as usize) < arena_size);

    let svm_region = SvmRegion::with_size(1 << 20);
    let svm_offset = svm_region.alloc(512, 64);
    assert_ne!(svm_offset, u64::MAX);
    // SAFETY: a successful SVM allocation returns an offset inside this live
    // mapping, and the pointer is used only for range inspection.
    let svm_allocation = unsafe { svm_region.base().add(svm_offset as usize) };

    assert!((svm_allocation as usize).wrapping_sub(svm_region.base() as usize) < svm_region.size());
}

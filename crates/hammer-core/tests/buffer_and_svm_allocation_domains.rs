use hammer_infra::svm_region::SvmRegion;

#[test]
fn svm_memory_stays_outside_main_heap() {
    hammer_infra::main_heap::init(256 << 20).expect("initialize fixed main heap");

    let svm_region = SvmRegion::with_size(1 << 20);
    let svm_offset = svm_region.alloc(512, 64);
    assert_ne!(svm_offset, u64::MAX);
    // SAFETY: a successful SVM allocation returns an offset inside this live
    // mapping, and the pointer is used only for range inspection.
    let svm_allocation = unsafe { svm_region.base().add(svm_offset as usize) };

    assert!((svm_allocation as usize).wrapping_sub(svm_region.base() as usize) < svm_region.size());
}

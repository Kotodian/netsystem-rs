use std::process::Command;

use byte_unit::Byte;
use hammer_infra::align::{CACHE_LINE, is_aligned};
use hammer_infra::aligned_vec::AlignedVec;
use hammer_infra::mem::{MainHeapConfig, MemMain};
use hammer_infra::pmalloc::{PMALLOC_BLOCK_SIZE, PmallocMain};

const CHILD_MODE: &str = "HAMMER_PMALLOC_TEST_CHILD";

#[test]
fn pmalloc_behavior() {
    if std::env::var_os(CHILD_MODE).is_some() {
        run_pmalloc_cases();
        unsafe { libc::_exit(0) }
    }

    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .env(CHILD_MODE, "1")
        .arg("--exact")
        .arg("pmalloc_behavior")
        .arg("--nocapture")
        .output()
        .expect("spawn pmalloc test process");
    assert!(
        output.status.success(),
        "pmalloc child failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run_pmalloc_cases() {
    MainHeapConfig {
        size: Byte::from_u64(128 << 20),
        page_size: hammer_infra::PageSize::Default,
        default_hugepage_size: Some(hammer_infra::PageSize::Default),
    }
    .initialize()
    .expect("initialize process Main Heap");

    aligned_vec_grows_without_changing_element_stride();
    pmalloc_allocates_aligned_chunks_and_merges_on_free();
    pmalloc_shared_arena_uses_fixed_capacity();
    pmalloc_rejects_an_allocation_larger_than_the_arena_page();
    pmalloc_converts_addresses_in_place();
    pmalloc_rejects_non_power_of_two_alignment();
    pmalloc_rejects_foreign_free();
}

fn aligned_vec_grows_without_changing_element_stride() {
    let mut values = AlignedVec::<usize, CACHE_LINE>::new();
    for value in 0..64 {
        values.push(value);
    }

    assert_eq!(values.len(), 64);
    assert!(is_aligned(values.as_ptr(), CACHE_LINE));
    assert_eq!(
        values.as_ptr().wrapping_add(1).addr() - values.as_ptr().addr(),
        std::mem::size_of::<usize>()
    );
    assert_eq!(values[63], 63);

    values.resize(8, usize::MAX);
    assert_eq!(values.as_slice(), &(0..8).collect::<Vec<_>>()[..]);
    values.clear();
    assert!(values.is_empty());
}

fn pmalloc_allocates_aligned_chunks_and_merges_on_free() {
    let page_size = MemMain::system_page_size();
    let mut pmalloc = PmallocMain::new();
    pmalloc
        .initialize(None, page_size * 4)
        .expect("initialize pmalloc");

    let first = pmalloc
        .alloc_aligned(64, CACHE_LINE)
        .expect("allocate first chunk");
    let second = pmalloc
        .alloc_aligned(128, CACHE_LINE * 2)
        .expect("allocate second chunk");
    assert!(!first.is_null());
    assert!(!second.is_null());
    assert!(is_aligned(first, CACHE_LINE));
    assert!(is_aligned(second, CACHE_LINE * 2));
    assert_eq!(pmalloc.chunk_index_by_va.len(), 2);

    pmalloc.free(first);
    pmalloc.free(second);
    assert!(pmalloc.chunk_index_by_va.is_empty());
    assert_eq!(
        pmalloc.pages[0].n_free_blocks,
        u32::try_from(page_size / PMALLOC_BLOCK_SIZE).unwrap()
    );
}

fn pmalloc_shared_arena_uses_fixed_capacity() {
    let page_size = MemMain::system_page_size();
    let mut pmalloc = PmallocMain::new();
    pmalloc
        .initialize(None, page_size * 4)
        .expect("initialize pmalloc");

    let arena = pmalloc
        .create_shared_arena("pmalloc-test", page_size, page_size.trailing_zeros(), 0)
        .expect("create shared arena");
    assert!(!arena.is_null());
    let allocation = pmalloc
        .alloc_from_arena(arena, 64, CACHE_LINE)
        .expect("allocate from shared arena");
    assert!(!allocation.is_null());
    assert!(is_aligned(allocation, CACHE_LINE));
    pmalloc.free(allocation);
}

fn pmalloc_rejects_an_allocation_larger_than_the_arena_page() {
    let page_size = MemMain::system_page_size();
    let mut pmalloc = PmallocMain::new();
    pmalloc
        .initialize(None, page_size * 2)
        .expect("initialize pmalloc");
    let arena = pmalloc
        .create_shared_arena("pmalloc-small", page_size, page_size.trailing_zeros(), 0)
        .expect("create shared arena");
    let arena_count = pmalloc.arenas.len();
    let allocation = pmalloc
        .alloc_from_arena(arena, page_size + PMALLOC_BLOCK_SIZE, CACHE_LINE)
        .expect("large allocation refusal");
    assert!(allocation.is_null());
    assert_eq!(pmalloc.arenas.len(), arena_count);
}

fn pmalloc_converts_addresses_in_place() {
    let page_size = MemMain::system_page_size();
    let mut pmalloc = PmallocMain::new();
    pmalloc
        .initialize(None, page_size * 2)
        .expect("initialize pmalloc");
    let allocation = pmalloc
        .alloc_aligned(64, CACHE_LINE)
        .expect("allocate address");
    let virtual_address = allocation.addr();
    let physical_address = pmalloc.get_pa(virtual_address);
    let mut addresses = [virtual_address, virtual_address + 8];
    pmalloc.convert_to_phys_addrs_with_offset(&mut addresses, 16);
    assert_eq!(addresses[0], physical_address + 16);
    pmalloc.free(allocation);
}

fn pmalloc_rejects_non_power_of_two_alignment() {
    let page_size = MemMain::system_page_size();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut pmalloc = PmallocMain::new();
        pmalloc
            .initialize(None, page_size * 2)
            .expect("initialize pmalloc");
        let _ = pmalloc.alloc_aligned(64, 96);
    }));
    assert!(result.is_err());
}

fn pmalloc_rejects_foreign_free() {
    let page_size = MemMain::system_page_size();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut pmalloc = PmallocMain::new();
        pmalloc
            .initialize(None, page_size * 2)
            .expect("initialize pmalloc");
        pmalloc.free(std::ptr::NonNull::<u8>::dangling().as_ptr());
    }));
    assert!(result.is_err());
}

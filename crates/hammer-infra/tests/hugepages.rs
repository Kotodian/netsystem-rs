#[cfg(target_os = "linux")]
use byte_unit::Byte;
#[cfg(target_os = "linux")]
use hammer_infra::{PageSize, main_heap, physmem::PhysmemMap};

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires privileges and a host with writable 2 MiB HugeTLB pools"]
fn main_heap_and_buffer_arena_use_verified_hugetlb_pages() {
    let page_size = PageSize::Bytes(Byte::from_u64(2 << 20));
    let mut cpu: libc::c_uint = 0;
    let mut numa_node: libc::c_uint = 0;
    // SAFETY: both output pointers are writable and getcpu retains neither.
    assert_eq!(
        unsafe {
            libc::syscall(
                libc::SYS_getcpu,
                &mut cpu,
                &mut numa_node,
                std::ptr::null_mut::<libc::c_void>(),
            )
        },
        0,
        "query current NUMA node"
    );

    let requested = main_heap::minimum_capacity().max(256 << 20);
    let capacity = main_heap::init_with(requested, page_size, Some(numa_node))
        .expect("initialize HugeTLB Main Heap");
    assert!(capacity >= requested);

    let mapping = PhysmemMap::create("verify-hugepages", 8 << 20, page_size, numa_node)
        .expect("create HugeTLB Buffer mapping");
    assert!(mapping.is_hugetlb());
    assert_eq!(mapping.page_size(), 2 << 20);
    assert_eq!(mapping.numa_node(), numa_node);
}

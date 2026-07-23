#![cfg(target_os = "linux")]

use std::ffi::c_void;
use std::fs;
use std::path::PathBuf;
use std::ptr;

use byte_unit::Byte;
use hammer_infra::physmem::{PhysmemError, PhysmemMap};
use hammer_infra::{PageSize, main_heap};
use procfs::process::Process;

const PAGE_2_MIB: usize = 2 << 20;
const PAGE_1_GIB: usize = 1 << 30;
const EXHAUST_CHUNK: usize = 1 << 20;
const MAX_EXHAUST_CHUNKS: usize = 2048;

#[test]
#[ignore = "requires root and writable Linux HugeTLB pools"]
fn automatic_pool_growth_and_buffer_backing_use_2_mib_hugetlb() {
    let numa_node = current_numa_node();
    let pool = hugepage_pool(numa_node, PAGE_2_MIB).expect("2 MiB HugeTLB pool");
    let free_before = read_pool_value(&pool, "free_hugepages");
    let current_before = read_pool_value(&pool, "nr_hugepages");

    let mapping = PhysmemMap::create(
        "verify-2m-buffer",
        PAGE_2_MIB,
        PageSize::Bytes(Byte::from_u64(PAGE_2_MIB as u64)),
        numa_node,
    )
    .expect("create 2 MiB HugeTLB Buffer mapping");

    assert_eq!(mapping.page_size(), PAGE_2_MIB);
    assert!(mapping.is_hugetlb());
    assert_eq!(kernel_page_size(mapping.base()), PAGE_2_MIB);
    assert!(hugetlb_bytes(mapping.base()) >= PAGE_2_MIB);
    if free_before == 0 {
        assert!(read_pool_value(&pool, "nr_hugepages") > current_before);
    }
}

#[test]
#[ignore = "requires root and writable Linux HugeTLB pools"]
fn buffer_backing_uses_1_gib_hugetlb_when_advertised() {
    let numa_node = current_numa_node();
    if hugepage_pool(numa_node, PAGE_1_GIB).is_none() {
        eprintln!("1 GiB HugeTLB is not advertised; skipping");
        return;
    }

    let mapping = PhysmemMap::create(
        "verify-1g-buffer",
        1,
        PageSize::Bytes(Byte::from_u64(PAGE_1_GIB as u64)),
        numa_node,
    )
    .expect("create 1 GiB HugeTLB Buffer mapping");

    assert_eq!(mapping.page_size(), PAGE_1_GIB);
    assert!(mapping.is_hugetlb());
    assert_eq!(kernel_page_size(mapping.base()), PAGE_1_GIB);
    assert!(hugetlb_bytes(mapping.base()) >= PAGE_1_GIB);
}

#[test]
#[ignore = "requires root and writable Linux HugeTLB pools"]
fn main_heap_uses_2_mib_hugetlb_and_never_falls_back() {
    let bootstrap = unsafe { libc::malloc(64).cast::<u8>() };
    assert!(!bootstrap.is_null());
    for offset in 0..64 {
        unsafe { bootstrap.add(offset).write(offset as u8) };
    }

    let numa_node = current_numa_node();
    let page_size = PageSize::Bytes(Byte::from_u64(PAGE_2_MIB as u64));
    let requested = main_heap::minimum_capacity().max(64 << 20);
    let capacity = main_heap::init_with(requested, page_size, Some(numa_node))
        .expect("initialize 2 MiB HugeTLB Main Heap");
    assert_eq!(capacity % PAGE_2_MIB, 0);

    let migrated = unsafe { libc::realloc(bootstrap.cast::<c_void>(), 128).cast::<u8>() };
    assert!(!migrated.is_null());
    assert_ne!(migrated, bootstrap);
    for offset in 0..64 {
        assert_eq!(unsafe { migrated.add(offset).read() }, offset as u8);
    }

    let allocation = vec![0xA5_u8; EXHAUST_CHUNK];
    assert_eq!(kernel_page_size(allocation.as_ptr()), PAGE_2_MIB);
    assert!(hugetlb_bytes(allocation.as_ptr()) >= PAGE_2_MIB);

    let maps_before = fs::read_to_string("/proc/self/maps").expect("read maps before exhaustion");
    let mut allocations = [ptr::null_mut::<c_void>(); MAX_EXHAUST_CHUNKS];
    let mut allocated = 0usize;
    let mut exhausted = false;
    unsafe {
        while allocated < allocations.len() {
            let pointer = libc::malloc(EXHAUST_CHUNK);
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
        libc::free(migrated.cast::<c_void>());
    }
    assert!(exhausted, "fixed HugeTLB Main Heap must exhaust");
    assert!(allocated < MAX_EXHAUST_CHUNKS);
    assert_eq!(
        fs::read_to_string("/proc/self/maps").expect("read maps after exhaustion"),
        maps_before,
        "Main Heap exhaustion created a new mapping"
    );
}

#[test]
#[ignore = "requires root and writable Linux HugeTLB pools"]
fn main_heap_uses_1_gib_hugetlb_when_advertised() {
    let numa_node = current_numa_node();
    if hugepage_pool(numa_node, PAGE_1_GIB).is_none() {
        eprintln!("1 GiB HugeTLB is not advertised; skipping");
        return;
    }

    let page_size = PageSize::Bytes(Byte::from_u64(PAGE_1_GIB as u64));
    let capacity = main_heap::init_with(PAGE_1_GIB, page_size, Some(numa_node))
        .expect("initialize 1 GiB HugeTLB Main Heap");
    assert_eq!(capacity, PAGE_1_GIB);

    let allocation = vec![0x5A_u8; EXHAUST_CHUNK];
    assert_eq!(kernel_page_size(allocation.as_ptr()), PAGE_1_GIB);
    assert!(hugetlb_bytes(allocation.as_ptr()) >= PAGE_1_GIB);
}

#[test]
#[ignore = "Linux HugeTLB strict-failure verification"]
fn unsupported_explicit_hugepage_requests_fail_without_fallback() {
    const UNSUPPORTED_PAGE_SIZE: usize = 16 << 20;

    let numa_node = current_numa_node();
    let page_size = PageSize::Bytes(Byte::from_u64(UNSUPPORTED_PAGE_SIZE as u64));
    assert!(matches!(
        PhysmemMap::create("unsupported", 1, page_size, numa_node),
        Err(PhysmemError::HugePageUnsupported { .. })
    ));
    assert!(matches!(
        main_heap::init_with(64 << 20, page_size, Some(numa_node)),
        Err(main_heap::MainHeapError::Mapping {
            source: PhysmemError::HugePageUnsupported { .. }
        })
    ));
}

#[test]
#[ignore = "Linux THP rejection verification"]
fn transparent_hugepage_hint_is_not_reported_as_hugetlb() {
    let mapping = PhysmemMap::create("thp", 4 << 20, PageSize::Default, current_numa_node())
        .expect("create ordinary Buffer mapping");
    unsafe {
        libc::madvise(mapping.base().cast(), mapping.size(), libc::MADV_HUGEPAGE);
        for offset in (0..mapping.size()).step_by(4096) {
            mapping.base().add(offset).write_volatile(0);
        }
    }

    assert!(!mapping.is_hugetlb());
    assert_ne!(kernel_page_size(mapping.base()), PAGE_2_MIB);
}

fn current_numa_node() -> u32 {
    let mut cpu: libc::c_uint = 0;
    let mut node: libc::c_uint = 0;
    let result = unsafe {
        libc::syscall(
            libc::SYS_getcpu,
            &mut cpu,
            &mut node,
            ptr::null_mut::<libc::c_void>(),
        )
    };
    assert_eq!(
        result,
        0,
        "getcpu failed: {}",
        std::io::Error::last_os_error()
    );
    node
}

fn hugepage_pool(numa_node: u32, page_size: usize) -> Option<PathBuf> {
    let node_pool = PathBuf::from(format!(
        "/sys/devices/system/node/node{numa_node}/hugepages/hugepages-{}kB",
        page_size / 1024
    ));
    if node_pool.is_dir() {
        return Some(node_pool);
    }
    let global_pool = PathBuf::from(format!(
        "/sys/kernel/mm/hugepages/hugepages-{}kB",
        page_size / 1024
    ));
    global_pool.is_dir().then_some(global_pool)
}

fn read_pool_value(pool: &std::path::Path, name: &str) -> usize {
    fs::read_to_string(pool.join(name))
        .unwrap_or_else(|error| panic!("read {}: {error}", pool.join(name).display()))
        .trim()
        .parse()
        .unwrap_or_else(|error| panic!("parse {}: {error}", pool.join(name).display()))
}

fn kernel_page_size(pointer: *const u8) -> usize {
    smaps_value(pointer, "KernelPageSize")
}

fn hugetlb_bytes(pointer: *const u8) -> usize {
    smaps_value(pointer, "Private_Hugetlb") + smaps_value(pointer, "Shared_Hugetlb")
}

fn smaps_value(pointer: *const u8, name: &str) -> usize {
    let address = pointer as u64;
    Process::myself()
        .and_then(|process| process.smaps())
        .expect("read /proc/self/smaps")
        .0
        .into_iter()
        .find(|mapping| mapping.address.0 <= address && mapping.address.1 > address)
        .and_then(|mapping| mapping.extension.map.get(name).copied())
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0)
}

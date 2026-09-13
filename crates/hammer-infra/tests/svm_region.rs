use std::ffi::c_void;
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::process::{Command, Stdio};
use std::ptr::NonNull;
use std::sync::atomic::AtomicU64;

use byte_unit::Byte;
use hammer_infra::mem::{MainHeapConfig, MemMain};
use hammer_infra::svm::region::{
    SVM_REGION_VERSION, SvmRegion, SvmRegionConfig, SvmRegionError, SvmRegionFlags,
};

const REGION_PROCESS_MODE: &str = "HAMMER_REGION_PROCESS_MODE";
const REGION_PROCESS_FD: &str = "HAMMER_REGION_PROCESS_FD";
const REGION_PROCESS_BASE: &str = "HAMMER_REGION_PROCESS_BASE";
const REGION_PROCESS_SIZE: &str = "HAMMER_REGION_PROCESS_SIZE";
const REGION_PROCESS_NOTIFY_FD: &str = "HAMMER_REGION_PROCESS_NOTIFY_FD";

const LOCAL_ROOT_BASE: usize = 0x4000_0000_0000;
const SHARED_REGION_BASE: usize = 0x4100_0000_0000;
const OWNER_DEATH_BASE: usize = 0x4200_0000_0000;

#[test]
fn fixed_va_regions_use_pvt_and_data_mem_heaps() {
    if let Ok(mode) = std::env::var(REGION_PROCESS_MODE) {
        run_region_process(&mode);
    }

    for mode in ["local-root", "shared-region", "old-layout", "owner-death"] {
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .env(REGION_PROCESS_MODE, mode)
            .arg("--exact")
            .arg("fixed_va_regions_use_pvt_and_data_mem_heaps")
            .arg("--nocapture")
            .output()
            .expect("spawn region process");
        assert!(
            output.status.success(),
            "region process {mode} failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn run_region_process(mode: &str) -> ! {
    initialize_main_heap();
    match mode {
        "local-root" => local_root_region(),
        "shared-region" => shared_region_server(),
        "shared-client" => shared_region_client(),
        "occupied-client" => occupied_region_client(),
        "exited-client" => exited_region_client(),
        "old-layout" => old_layout_is_rejected(),
        "owner-death" => owner_death_server(),
        "owner-death-client" => owner_death_client(),
        _ => panic!("unknown region process mode {mode}"),
    }
    unsafe { libc::_exit(0) }
}

fn initialize_main_heap() {
    MainHeapConfig {
        size: Byte::from_u64(256 << 20),
        page_size: hammer_infra::PageSize::Default,
        default_hugepage_size: None,
    }
    .initialize()
    .expect("initialize process Main Heap");
}

fn local_root_region() {
    let page_size = MemMain::system_page_size();
    let root_config = SvmRegionConfig {
        name: "phase-two-root".to_owned(),
        size: 32 << 20,
        pvt_heap_size: 0,
        flags: SvmRegionFlags::NODATA,
    };
    let mut root = SvmRegion::create(
        NonZeroUsize::new(LOCAL_ROOT_BASE).expect("fixed root base"),
        &root_config,
        memfd("phase-two-root"),
    )
    .expect("create root region");
    assert_eq!(root.base().as_ptr().addr(), LOCAL_ROOT_BASE);
    assert_eq!(root.size(), root_config.size);
    assert_eq!(root.flags(), SvmRegionFlags::NODATA);
    assert_eq!(root.client_count().expect("root client count"), 1);

    {
        let lock = root.lock().expect("lock root region");
        assert!(lock.data_heap().is_none());
        assert_eq!(
            lock.pvt_heap().base().as_ptr().addr(),
            LOCAL_ROOT_BASE + page_size
        );
        let main = lock.main_region().expect("root main region");
        assert_eq!(main.subregion_count(), 0);
        assert!(lock.pvt_heap().is_heap_object(NonNull::from(main).cast()));
    }

    let data_config = SvmRegionConfig {
        name: "phase-two-data".to_owned(),
        size: 2 << 20,
        pvt_heap_size: 0,
        flags: SvmRegionFlags::DATA_HEAP,
    };
    let data_region = root
        .find_or_create_subregion(&data_config, memfd("phase-two-data"))
        .expect("create Data Heap subregion");
    let data_base = data_region.base();
    {
        let lock = data_region.lock().expect("lock Data Heap region");
        let data_heap = lock.data_heap().expect("Data Heap exists");
        let active_heap = data_heap.activate();
        let mut bytes = Vec::with_capacity(1);
        bytes.extend(0..4096u32);
        let text = String::from("allocated from the active Data Heap");
        assert!(data_heap.is_heap_object(NonNull::from(bytes.as_slice()).cast()));
        assert!(data_heap.is_heap_object(NonNull::from(text.as_bytes()).cast()));
        drop(text);
        drop(bytes);
        drop(active_heap);
    }

    let plain_config = SvmRegionConfig {
        name: "phase-two-plain".to_owned(),
        size: 64 << 10,
        pvt_heap_size: 0,
        flags: SvmRegionFlags::NONE,
    };
    let plain_region = root
        .find_or_create_subregion(&plain_config, memfd("phase-two-plain"))
        .expect("create subregion without Data Heap");
    {
        let lock = plain_region.lock().expect("lock plain region");
        assert!(lock.data_heap().is_none());
        assert!(lock.main_region().is_none());
    }

    {
        let lock = root.lock().expect("lock populated root");
        let main = lock.main_region().expect("root main region");
        let data_index = main
            .subregion_index(&data_config.name)
            .expect("Data Heap region is indexed by name");
        assert!(main.subregion(data_index).is_some());
        assert!(main.subregion_index(&plain_config.name).is_some());
        assert_eq!(main.subregion_count(), 2);
    }

    plain_region.unmap().expect("unmap plain subregion");
    data_region.unmap().expect("unmap Data Heap subregion");
    {
        let lock = root.lock().expect("lock cleaned root");
        let main = lock.main_region().expect("root main region");
        assert_eq!(main.subregion_count(), 0);
        assert!(main.subregion_index(&data_config.name).is_none());
        assert!(main.subregion_index(&plain_config.name).is_none());
    }

    let replacement = root
        .find_or_create_subregion(&data_config, memfd("phase-two-data-replacement"))
        .expect("reuse released root bitmap range");
    assert_eq!(replacement.base(), data_base);
    replacement.unmap().expect("unmap replacement region");
    root.unmap().expect("unmap root region");
}

fn shared_region_server() {
    let config = SvmRegionConfig {
        name: "phase-two-shared".to_owned(),
        size: 4 << 20,
        pvt_heap_size: 0,
        flags: SvmRegionFlags::DATA_HEAP,
    };
    let backing = memfd("phase-two-shared");
    let shared_descriptor = inheritable_duplicate(backing.as_raw_fd());
    let region = SvmRegion::create(
        NonZeroUsize::new(SHARED_REGION_BASE).expect("fixed shared base"),
        &config,
        backing,
    )
    .expect("create shared region");

    let shared = spawn_region_client(
        "shared-client",
        shared_descriptor.as_raw_fd(),
        region.base().as_ptr().addr(),
        region.size(),
    );
    assert!(shared.success(), "same-VA client failed: {shared}");
    assert_eq!(region.client_count().expect("post-client count"), 1);

    let occupied = spawn_region_client(
        "occupied-client",
        shared_descriptor.as_raw_fd(),
        region.base().as_ptr().addr(),
        region.size(),
    );
    assert!(occupied.success(), "occupied-VA client failed: {occupied}");
    assert_eq!(region.client_count().expect("occupied attach count"), 1);

    let exited = spawn_region_client(
        "exited-client",
        shared_descriptor.as_raw_fd(),
        region.base().as_ptr().addr(),
        region.size(),
    );
    assert!(exited.success(), "exited client failed: {exited}");
    assert_eq!(region.client_count().expect("stale client count"), 2);
    assert_eq!(
        region
            .remove_exited_clients()
            .expect("remove exited client"),
        1
    );
    assert_eq!(region.client_count().expect("clean client count"), 1);
    region.unmap().expect("unmap shared region");
}

fn shared_region_client() {
    let expected_base = environment_usize(REGION_PROCESS_BASE);
    let region = SvmRegion::attach(inherited_descriptor()).expect("attach shared region");
    assert_eq!(region.base().as_ptr().addr(), expected_base);
    assert_eq!(region.client_count().expect("client sees both pids"), 2);
    let lock = region.lock().expect("lock attached region");
    let pvt_heap = lock.pvt_heap();
    assert_eq!(
        pvt_heap.base().as_ptr().addr(),
        expected_base + MemMain::system_page_size()
    );
    let data_heap = lock.data_heap().expect("attached Data Heap");
    let active_heap = data_heap.activate();
    let mut values = Vec::with_capacity(1);
    values.extend(0..2048u64);
    assert!(data_heap.is_heap_object(NonNull::from(values.as_slice()).cast()));
    drop(values);
    drop(active_heap);
    drop(lock);
    region.unmap().expect("client unmaps and unregisters pid");
}

fn occupied_region_client() {
    let base = environment_usize(REGION_PROCESS_BASE);
    let size = environment_usize(REGION_PROCESS_SIZE);
    let reservation = unsafe {
        libc::mmap(
            base as *mut c_void,
            size,
            libc::PROT_NONE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_FIXED_NOREPLACE,
            -1,
            0,
        )
    };
    assert_ne!(reservation, libc::MAP_FAILED);
    assert_eq!(reservation.addr(), base);
    let error = SvmRegion::attach(inherited_descriptor()).expect_err("occupied VA rejects attach");
    assert!(matches!(
        error,
        SvmRegionError::AddressRangeOccupied {
            base: found,
            size: found_size,
            ..
        } if found == base && found_size == size
    ));
    assert_eq!(
        unsafe { libc::mprotect(reservation, size, libc::PROT_READ | libc::PROT_WRITE) },
        0
    );
    unsafe { reservation.cast::<u8>().write(0xA5) };
    assert_eq!(unsafe { reservation.cast::<u8>().read() }, 0xA5);
    assert_eq!(unsafe { libc::munmap(reservation, size) }, 0);
}

fn exited_region_client() {
    let region = SvmRegion::attach(inherited_descriptor()).expect("attach before process exit");
    assert_eq!(
        region.base().as_ptr().addr(),
        environment_usize(REGION_PROCESS_BASE)
    );
    std::mem::forget(region);
}

fn old_layout_is_rejected() {
    let page_size = MemMain::system_page_size();
    let backing = memfd("phase-two-old-layout");
    assert_eq!(
        unsafe { libc::ftruncate(backing.as_raw_fd(), page_size as libc::off_t) },
        0
    );
    let mapping = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            page_size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            backing.as_raw_fd(),
            0,
        )
    };
    assert_ne!(mapping, libc::MAP_FAILED);
    unsafe {
        mapping
            .cast::<AtomicU64>()
            .write(AtomicU64::new((1 << 16) | 1));
    }
    assert_eq!(unsafe { libc::munmap(mapping, page_size) }, 0);
    let error = SvmRegion::attach(backing).expect_err("offset layout version is rejected");
    assert!(matches!(
        error,
        SvmRegionError::UnsupportedVersion {
            found,
            expected: SVM_REGION_VERSION,
        } if found == (1 << 16) | 1
    ));
}

fn owner_death_server() {
    let config = SvmRegionConfig {
        name: "phase-two-owner-death".to_owned(),
        size: 2 << 20,
        pvt_heap_size: 0,
        flags: SvmRegionFlags::NONE,
    };
    let backing = memfd("phase-two-owner-death");
    let shared_descriptor = inheritable_duplicate(backing.as_raw_fd());
    let region = SvmRegion::create(
        NonZeroUsize::new(OWNER_DEATH_BASE).expect("owner-death base"),
        &config,
        backing,
    )
    .expect("create owner-death region");
    let mut pipe_descriptors = [0; 2];
    assert_eq!(unsafe { libc::pipe(pipe_descriptors.as_mut_ptr()) }, 0);
    let read_descriptor = unsafe { OwnedFd::from_raw_fd(pipe_descriptors[0]) };
    let write_descriptor = unsafe { OwnedFd::from_raw_fd(pipe_descriptors[1]) };
    clear_close_on_exec(write_descriptor.as_raw_fd());

    let mut child = region_command("owner-death-client")
        .env(REGION_PROCESS_FD, shared_descriptor.as_raw_fd().to_string())
        .env(REGION_PROCESS_BASE, OWNER_DEATH_BASE.to_string())
        .env(
            REGION_PROCESS_NOTIFY_FD,
            write_descriptor.as_raw_fd().to_string(),
        )
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn owner-death client");
    drop(write_descriptor);
    let mut notification = 0u8;
    assert_eq!(
        unsafe {
            libc::read(
                read_descriptor.as_raw_fd(),
                std::ptr::from_mut(&mut notification).cast(),
                1,
            )
        },
        1
    );
    assert_eq!(notification, 1);
    let child_pid = child.id() as i32;
    assert!(child.wait().expect("wait owner-death child").success());

    for _ in 0..2 {
        let error = match region.lock() {
            Err(error) => error,
            Ok(_) => panic!("dead mutex owner must stop region access"),
        };
        assert!(matches!(
            error,
            SvmRegionError::OwnerDied {
                owner_pid,
                owner_tag: 7,
            } if owner_pid == child_pid
        ));
    }
    drop(region);
}

fn owner_death_client() {
    let region = SvmRegion::attach(inherited_descriptor()).expect("attach owner-death client");
    assert_eq!(
        region.base().as_ptr().addr(),
        environment_usize(REGION_PROCESS_BASE)
    );
    let lock = region.lock().expect("hold region mutex");
    let notify_descriptor = environment_i32(REGION_PROCESS_NOTIFY_FD);
    let notification = [1u8];
    assert_eq!(
        unsafe {
            libc::write(
                notify_descriptor,
                notification.as_ptr().cast(),
                notification.len(),
            )
        },
        1
    );
    std::mem::forget(lock);
    std::mem::forget(region);
    unsafe { libc::_exit(0) }
}

fn spawn_region_client(
    mode: &str,
    descriptor: RawFd,
    base: usize,
    size: usize,
) -> std::process::ExitStatus {
    region_command(mode)
        .env(REGION_PROCESS_FD, descriptor.to_string())
        .env(REGION_PROCESS_BASE, base.to_string())
        .env(REGION_PROCESS_SIZE, size.to_string())
        .status()
        .expect("spawn region client")
}

fn region_command(mode: &str) -> Command {
    let mut command = Command::new(std::env::current_exe().expect("current test executable"));
    command
        .env(REGION_PROCESS_MODE, mode)
        .arg("--exact")
        .arg("fixed_va_regions_use_pvt_and_data_mem_heaps")
        .arg("--nocapture");
    command
}

fn memfd(name: &str) -> OwnedFd {
    let name = std::ffi::CString::new(name).expect("memfd name");
    let descriptor = unsafe { libc::syscall(libc::SYS_memfd_create, name.as_ptr(), 0) };
    assert!(
        descriptor >= 0,
        "memfd_create failed: {}",
        std::io::Error::last_os_error()
    );
    unsafe { OwnedFd::from_raw_fd(descriptor as RawFd) }
}

fn inheritable_duplicate(descriptor: RawFd) -> OwnedFd {
    let duplicate = unsafe { libc::dup(descriptor) };
    assert!(
        duplicate >= 0,
        "dup failed: {}",
        std::io::Error::last_os_error()
    );
    clear_close_on_exec(duplicate);
    unsafe { OwnedFd::from_raw_fd(duplicate) }
}

fn clear_close_on_exec(descriptor: RawFd) {
    assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_SETFD, 0) }, 0);
}

fn inherited_descriptor() -> OwnedFd {
    let descriptor = environment_i32(REGION_PROCESS_FD);
    unsafe { OwnedFd::from_raw_fd(descriptor) }
}

fn environment_usize(name: &str) -> usize {
    std::env::var(name)
        .expect("region process environment")
        .parse()
        .expect("numeric region process environment")
}

fn environment_i32(name: &str) -> i32 {
    std::env::var(name)
        .expect("region process environment")
        .parse()
        .expect("numeric region process environment")
}

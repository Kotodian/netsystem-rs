//! Behavior tests for the shared-VM mapping owner (ADR-0011 section 13).
//!
//! The mapping owns the OS resource and the shared header. Payload layout
//! belongs to the region or FIFO segment, so these cases only exercise the
//! mapping lifecycle: create/attach/delete, header-derived size and identity,
//! and the `ready` publication boundary.

#![cfg(target_os = "linux")]

use std::error::Error;
use std::num::NonZeroUsize;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;

use byte_unit::Byte;
use hammer_infra::mem::{MainHeapConfig, MemError, MemMain, PageSize};
use hammer_infra::svm::ssvm::{
    SSVM_NAME_MAX, SSVM_PAYLOAD_OFFSET, SsvmConfig, SsvmError, SsvmPrivate, SsvmSegmentBackend,
};

const PAYLOAD_BYTES: usize = 1 << 16;
const SHORT_TIMEOUT: Duration = Duration::from_millis(50);
const PROCESS_MODE: &str = "HAMMER_SSVM_PROCESS_MODE";
const ATTACH_MODE: &str = "HAMMER_SSVM_ATTACH_MODE";
const ATTACH_FD: &str = "HAMMER_SSVM_ATTACH_FD";
const ATTACH_NAME: &str = "HAMMER_SSVM_ATTACH_NAME";
const ATTACH_BASE: &str = "HAMMER_SSVM_ATTACH_BASE";
const REQUESTED_BASE: u64 = 0x5000_0000_0000;

fn initialize_main_heap() {
    static MAIN_HEAP: OnceLock<()> = OnceLock::new();
    MAIN_HEAP.get_or_init(|| {
        MainHeapConfig {
            size: Byte::from_u64(256 << 20),
            page_size: hammer_infra::PageSize::Default,
            default_hugepage_size: None,
        }
        .initialize()
        .expect("initialize process Main Heap");
    });
}

fn memfd_config(name: &str, size: usize) -> SsvmConfig {
    SsvmConfig {
        backend: SsvmSegmentBackend::Memfd,
        name: name.to_string(),
        size,
        requested_va: 0,
        huge_page: false,
        attach_timeout: SHORT_TIMEOUT,
    }
}

fn run_attached_process(mode: &str, test_name: &str, descriptor: Option<i32>, name: Option<&str>) {
    if let Some(descriptor) = descriptor {
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert!(flags >= 0, "read memfd descriptor flags");
        assert_eq!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0,
            "clear memfd close-on-exec"
        );
    }
    let mut command = Command::new(std::env::current_exe().expect("current test executable"));
    command
        .env(ATTACH_MODE, mode)
        .env_remove(PROCESS_MODE)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture");
    if let Some(descriptor) = descriptor {
        command.env(ATTACH_FD, descriptor.to_string());
    }
    if let Some(name) = name {
        command.env(ATTACH_NAME, name);
    }
    let output = command.output().expect("spawn SSVM attached process");
    assert!(
        output.status.success(),
        "SSVM attached process {mode} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run_attached_child(mode: &str) -> ! {
    initialize_main_heap();
    match mode {
        "memfd-attach" => {
            let count = MemMain::mapping_count();
            let descriptor = std::env::var(ATTACH_FD)
                .expect("memfd descriptor")
                .parse()
                .expect("memfd descriptor number");
            let client = SsvmPrivate::client_init_memfd(descriptor).expect("client mapping");
            assert!(!client.is_server());
            assert_eq!(client.segment_type(), SsvmSegmentBackend::Memfd);
            assert_eq!(client.name(), "hammer-ssvm-header");
            assert_eq!(client.client_pid(), std::process::id());
            assert!(client.is_ready());
            assert_eq!(MemMain::mapping_count(), count + 1);
            drop(client);
            assert_eq!(MemMain::mapping_count(), count);
        }
        "memfd-not-ready" => {
            let descriptor = std::env::var(ATTACH_FD)
                .expect("memfd descriptor")
                .parse()
                .expect("memfd descriptor number");
            let client = SsvmPrivate::client_init_memfd(descriptor).expect("client mapping");
            assert!(!client.is_ready());
        }
        "shm-attach" => {
            let name = std::env::var(ATTACH_NAME).expect("shm name");
            let client = SsvmPrivate::client_init_shm(&name, SHORT_TIMEOUT).expect("shm attach");
            assert_eq!(client.name(), name);
            assert!(client.is_ready());
        }
        "shm-not-ready" => {
            let name = std::env::var(ATTACH_NAME).expect("shm name");
            match SsvmPrivate::client_init_shm(&name, SHORT_TIMEOUT) {
                Err(SsvmError::ClientTimeout { name: reported, .. }) => assert_eq!(reported, name),
                other => panic!("an unpublished shm segment must time out, got {other:?}"),
            }
        }
        "shm-missing" => {
            let name = std::env::var(ATTACH_NAME).expect("shm name");
            match SsvmPrivate::client_init_shm(&name, SHORT_TIMEOUT) {
                Err(SsvmError::ClientTimeout { .. }) => {}
                other => panic!("a deleted shm segment must not attach, got {other:?}"),
            }
        }
        "occupied-attach" => {
            let descriptor = std::env::var(ATTACH_FD)
                .expect("memfd descriptor")
                .parse()
                .expect("memfd descriptor number");
            let base = std::env::var(ATTACH_BASE)
                .expect("requested address")
                .parse::<usize>()
                .expect("requested address number");
            let count = MemMain::mapping_count();
            let reservation = MemMain::vm_map(
                NonZeroUsize::new(base),
                PAYLOAD_BYTES,
                PageSize::Default,
                None,
                0,
                0,
                false,
                "occupied ssvm address",
            )
            .expect("reserve SSVM address");
            match SsvmPrivate::client_init_memfd(descriptor) {
                Err(SsvmError::Mmap { operation, source }) => {
                    assert_eq!(operation, "client mapping");
                    match &source {
                        MemError::AddressRangeOccupied { base: occupied, .. } => {
                            assert_eq!(*occupied, base);
                        }
                        other => panic!("expected occupied address, got {other:?}"),
                    }
                    assert!(source.source().is_some(), "mapping source is preserved");
                }
                other => panic!("occupied SSVM address must fail without replacement: {other:?}"),
            }
            assert_eq!(MemMain::mapping_count(), count + 1);
            unsafe { MemMain::vm_unmap(reservation) }.expect("release address reservation");
            assert_eq!(MemMain::mapping_count(), count);
        }
        _ => panic!("unknown SSVM attached process mode {mode}"),
    }
    unsafe { libc::_exit(0) }
}

fn run_test_process(mode: &str, test_name: &str) {
    if let Ok(attach_mode) = std::env::var(ATTACH_MODE) {
        run_attached_child(&attach_mode);
    }
    if let Ok(child_mode) = std::env::var(PROCESS_MODE) {
        initialize_main_heap();
        match child_mode.as_str() {
            "identity" => memfd_segment_publishes_identity_and_payload_bounds_case(),
            "attach" => client_mapping_reads_size_and_name_from_the_header_case(),
            "memfd-ready" => attach_does_not_wait_for_a_payload_that_is_not_ready_case(),
            "shm-delete" => shm_segment_attaches_by_name_and_delete_unlinks_it_case(),
            "shm-ready" => shm_attach_times_out_while_the_payload_is_unpublished_case(),
            "private" => private_mapping_is_never_attachable_case(),
            "invalid" => invalid_names_and_sizes_are_typed_failures_case(),
            "bounds" => offset_access_rejects_out_of_bounds_and_misaligned_ranges_case(),
            "mapping" => mappings_are_registered_and_requested_addresses_are_fixed_case(),
            "shm-requested" => shm_requested_address_publishes_actual_mapping_case(),
            _ => panic!("unknown SSVM process mode {child_mode}"),
        }
        unsafe { libc::_exit(0) }
    }

    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .env(PROCESS_MODE, mode)
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .output()
        .expect("spawn SSVM test process");
    assert!(
        output.status.success(),
        "SSVM process {mode} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn memfd_segment_publishes_identity_and_payload_bounds() {
    run_test_process(
        "identity",
        "memfd_segment_publishes_identity_and_payload_bounds",
    );
}

fn memfd_segment_publishes_identity_and_payload_bounds_case() {
    let count = MemMain::mapping_count();
    let server =
        SsvmPrivate::server_init_memfd(&memfd_config("hammer-ssvm-identity", PAYLOAD_BYTES))
            .expect("memfd segment");

    assert!(server.is_server());
    assert_eq!(MemMain::mapping_count(), count + 1);
    assert_eq!(server.segment_type(), SsvmSegmentBackend::Memfd);
    assert_eq!(server.name(), "hammer-ssvm-identity");
    assert_eq!(server.server_pid(), std::process::id());
    assert_eq!(server.client_pid(), 0);
    assert_ne!(
        server.ssvm_va(),
        0,
        "generic SSVM publishes its actual address"
    );
    assert!(server.heap().is_some(), "generic SSVM owns a locked heap");
    assert_eq!(server.payload_offset(), SSVM_PAYLOAD_OFFSET);
    assert!(
        server.ssvm_size().is_multiple_of(4096),
        "the mapping is page rounded"
    );
    assert!(server.ssvm_size() >= PAYLOAD_BYTES);
    assert_eq!(
        server.payload_len(),
        server.ssvm_size() as u64 - SSVM_PAYLOAD_OFFSET
    );
    assert!(!server.is_ready(), "the payload owner publishes ready");

    server.publish_ready();
    assert!(server.is_ready());
    assert!(
        server.fd().is_some(),
        "a memfd mapping keeps its descriptor"
    );
    drop(server);
    assert_eq!(MemMain::mapping_count(), count);
}

#[test]
fn mappings_are_registered_and_requested_addresses_are_fixed() {
    run_test_process(
        "mapping",
        "mappings_are_registered_and_requested_addresses_are_fixed",
    );
}

fn mappings_are_registered_and_requested_addresses_are_fixed_case() {
    let count = MemMain::mapping_count();
    let mut config = memfd_config("hammer-ssvm-fixed", PAYLOAD_BYTES);
    config.requested_va = REQUESTED_BASE;
    let server = SsvmPrivate::server_init_memfd(&config).expect("fixed-address memfd segment");
    assert_eq!(server.ssvm_va(), REQUESTED_BASE);
    assert_eq!(server.base().addr() as u64, REQUESTED_BASE);
    assert_eq!(MemMain::mapping_count(), count + 1);

    let descriptor = server.fd().expect("server descriptor");
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
    assert!(flags >= 0);
    assert_eq!(
        unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
        0
    );
    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .env(ATTACH_MODE, "occupied-attach")
        .env(ATTACH_FD, descriptor.to_string())
        .env(ATTACH_BASE, REQUESTED_BASE.to_string())
        .arg("--exact")
        .arg("mappings_are_registered_and_requested_addresses_are_fixed")
        .arg("--nocapture")
        .output()
        .expect("spawn occupied-address client");
    assert!(
        output.status.success(),
        "occupied-address client failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    drop(server);
    assert_eq!(MemMain::mapping_count(), count);

    let reservation = MemMain::vm_map(
        NonZeroUsize::new(REQUESTED_BASE as usize),
        PAYLOAD_BYTES,
        PageSize::Default,
        None,
        0,
        0,
        false,
        "occupied server address",
    )
    .expect("reserve server address");
    match SsvmPrivate::server_init_memfd(&config) {
        Err(SsvmError::CreateFailure { operation, source }) => {
            assert_eq!(operation, "memfd mapping");
            let memory_error = source
                .get_ref()
                .expect("memory error source")
                .downcast_ref::<MemError>()
                .expect("memory error is retained");
            match memory_error {
                MemError::AddressRangeOccupied { base, .. } => {
                    assert_eq!(*base, REQUESTED_BASE as usize);
                }
                other => panic!("expected occupied address, got {other:?}"),
            }
        }
        other => panic!("occupied server address must return CREATE_FAILURE: {other:?}"),
    }
    assert_eq!(MemMain::mapping_count(), count + 1);
    unsafe { MemMain::vm_unmap(reservation) }.expect("release server address reservation");
    assert_eq!(MemMain::mapping_count(), count);
}

#[test]
fn shm_requested_address_publishes_actual_mapping() {
    run_test_process(
        "shm-requested",
        "shm_requested_address_publishes_actual_mapping",
    );
}

fn shm_requested_address_publishes_actual_mapping_case() {
    let name = format!("/hammer-ssvm-requested-{}", std::process::id());
    let config = SsvmConfig {
        backend: SsvmSegmentBackend::Shm,
        name: name.clone(),
        size: PAYLOAD_BYTES,
        requested_va: REQUESTED_BASE,
        huge_page: false,
        attach_timeout: SHORT_TIMEOUT,
    };
    let count = MemMain::mapping_count();
    let server = SsvmPrivate::server_init_shm(&config).expect("requested-address shm segment");
    let offset = server.ssvm_va() - REQUESTED_BASE;
    assert!(offset <= 15 * MemMain::system_page_size() as u64);
    assert_eq!(offset % MemMain::system_page_size() as u64, 0);
    assert_eq!(server.base().addr() as u64, server.ssvm_va());
    assert_eq!(MemMain::mapping_count(), count + 1);
    server.publish_ready();
    run_attached_process(
        "shm-attach",
        "shm_requested_address_publishes_actual_mapping",
        None,
        Some(&name),
    );
    server.delete();
    assert_eq!(MemMain::mapping_count(), count);
}

#[test]
fn client_mapping_reads_size_and_name_from_the_header() {
    run_test_process(
        "attach",
        "client_mapping_reads_size_and_name_from_the_header",
    );
}

fn client_mapping_reads_size_and_name_from_the_header_case() {
    let server = SsvmPrivate::server_init_memfd(&memfd_config("hammer-ssvm-header", PAYLOAD_BYTES))
        .expect("memfd segment");
    server.publish_ready();
    let descriptor = server.fd().expect("server descriptor");
    run_attached_process(
        "memfd-attach",
        "client_mapping_reads_size_and_name_from_the_header",
        Some(descriptor),
        None,
    );
}

#[test]
fn attach_does_not_wait_for_a_payload_that_is_not_ready() {
    run_test_process(
        "memfd-ready",
        "attach_does_not_wait_for_a_payload_that_is_not_ready",
    );
}

fn attach_does_not_wait_for_a_payload_that_is_not_ready_case() {
    let server = SsvmPrivate::server_init_memfd(&memfd_config("hammer-ssvm-ready", PAYLOAD_BYTES))
        .expect("memfd segment");
    let descriptor = server.fd().expect("server descriptor");
    run_attached_process(
        "memfd-not-ready",
        "attach_does_not_wait_for_a_payload_that_is_not_ready",
        Some(descriptor),
        None,
    );
    match server.wait_ready(SHORT_TIMEOUT) {
        Err(SsvmError::ClientTimeout { name, seconds }) => {
            assert_eq!(name, "hammer-ssvm-ready");
            assert_eq!(seconds, 0);
        }
        other => panic!("an unpublished payload must time out, got {other:?}"),
    }
}

#[test]
fn shm_segment_attaches_by_name_and_delete_unlinks_it() {
    run_test_process(
        "shm-delete",
        "shm_segment_attaches_by_name_and_delete_unlinks_it",
    );
}

fn shm_segment_attaches_by_name_and_delete_unlinks_it_case() {
    let name = format!("/hammer-ssvm-{}", std::process::id());
    let config = SsvmConfig {
        backend: SsvmSegmentBackend::Shm,
        name: name.clone(),
        size: PAYLOAD_BYTES,
        requested_va: 0,
        huge_page: false,
        attach_timeout: SHORT_TIMEOUT,
    };
    let server = SsvmPrivate::server_init_shm(&config).expect("shm segment");
    assert_eq!(server.segment_type(), SsvmSegmentBackend::Shm);
    assert!(
        server.fd().is_none(),
        "the shm descriptor closes after mapping"
    );
    server.publish_ready();

    run_attached_process(
        "shm-attach",
        "shm_segment_attaches_by_name_and_delete_unlinks_it",
        None,
        Some(&name),
    );
    server.delete();
    run_attached_process(
        "shm-missing",
        "shm_segment_attaches_by_name_and_delete_unlinks_it",
        None,
        Some(&name),
    );
}

#[test]
fn shm_attach_times_out_while_the_payload_is_unpublished() {
    run_test_process(
        "shm-ready",
        "shm_attach_times_out_while_the_payload_is_unpublished",
    );
}

fn shm_attach_times_out_while_the_payload_is_unpublished_case() {
    let name = format!("/hammer-ssvm-unpublished-{}", std::process::id());
    let config = SsvmConfig {
        backend: SsvmSegmentBackend::Shm,
        name: name.clone(),
        size: PAYLOAD_BYTES,
        requested_va: 0,
        huge_page: false,
        attach_timeout: SHORT_TIMEOUT,
    };
    let server = SsvmPrivate::server_init_shm(&config).expect("shm segment");

    run_attached_process(
        "shm-not-ready",
        "shm_attach_times_out_while_the_payload_is_unpublished",
        None,
        Some(&name),
    );

    server.publish_ready();
    run_attached_process(
        "shm-attach",
        "shm_attach_times_out_while_the_payload_is_unpublished",
        None,
        Some(&name),
    );
}

#[test]
fn private_mapping_is_never_attachable() {
    run_test_process("private", "private_mapping_is_never_attachable");
}

fn private_mapping_is_never_attachable_case() {
    let config = SsvmConfig {
        backend: SsvmSegmentBackend::Private,
        name: "hammer-ssvm-private".to_string(),
        size: PAYLOAD_BYTES,
        requested_va: 0,
        huge_page: false,
        attach_timeout: SHORT_TIMEOUT,
    };
    let server = SsvmPrivate::server_init_private(&config).expect("private mapping");
    assert_eq!(server.segment_type(), SsvmSegmentBackend::Private);
    assert_eq!(server.name(), "hammer-ssvm-private");
    assert!(server.fd().is_none());
    assert!(server.heap().is_some(), "private SSVM owns a locked heap");
    assert!(
        server.ssvm_size() < config.size,
        "ssvm_size is dlmalloc free space"
    );

    match SsvmPrivate::client_init(&config, None) {
        Err(SsvmError::BackendUnavailable) => {}
        other => panic!("a private mapping must not attach, got {other:?}"),
    }
}

#[test]
fn invalid_names_and_sizes_are_typed_failures() {
    run_test_process("invalid", "invalid_names_and_sizes_are_typed_failures");
}

fn invalid_names_and_sizes_are_typed_failures_case() {
    match SsvmPrivate::server_init_memfd(&memfd_config("", PAYLOAD_BYTES)) {
        Err(SsvmError::NoName { max }) => assert_eq!(max, SSVM_NAME_MAX),
        other => panic!("an empty name must be rejected, got {other:?}"),
    }
    let too_long = "n".repeat(SSVM_NAME_MAX);
    match SsvmPrivate::server_init_memfd(&memfd_config(&too_long, PAYLOAD_BYTES)) {
        Err(SsvmError::NoName { max }) => assert_eq!(max, SSVM_NAME_MAX),
        other => panic!("an over-long name must be rejected, got {other:?}"),
    }
    match SsvmPrivate::server_init_memfd(&memfd_config("hammer-ssvm-size", 0)) {
        Err(SsvmError::NoSize) => {}
        other => panic!("a zero size must be rejected, got {other:?}"),
    }
}

#[test]
fn offset_access_rejects_out_of_bounds_and_misaligned_ranges() {
    run_test_process(
        "bounds",
        "offset_access_rejects_out_of_bounds_and_misaligned_ranges",
    );
}

fn offset_access_rejects_out_of_bounds_and_misaligned_ranges_case() {
    let server = SsvmPrivate::server_init_memfd(&memfd_config("hammer-ssvm-offset", PAYLOAD_BYTES))
        .expect("memfd segment");
    let size = server.ssvm_size();

    match server.offset_ptr(size as u64, 1, 1) {
        Err(SsvmError::OutOfBounds { .. }) => {}
        other => panic!("an out-of-bounds range must be rejected, got {other:?}"),
    }
    match server.offset_ptr(4, 1, 64) {
        Err(SsvmError::Misaligned { offset, alignment }) => {
            assert_eq!(offset, 4);
            assert_eq!(alignment, 64);
        }
        other => panic!("a misaligned offset must be rejected, got {other:?}"),
    }
    assert!(server.offset_ptr(0, 1, 1).is_ok());
}

//! Behavior tests for the shared-VM mapping owner (ADR-0011 section 13).
//!
//! The mapping owns the OS resource and the shared header. Payload layout
//! belongs to the region or FIFO segment, so these cases only exercise the
//! mapping lifecycle: create/attach/delete, header-derived size and identity,
//! and the `ready` publication boundary.

#![cfg(target_os = "linux")]

use std::time::Duration;

use hammer_infra::svm::ssvm::{
    SSVM_NAME_MAX, SSVM_PAYLOAD_OFFSET, SsvmConfig, SsvmError, SsvmPrivate, SsvmSegmentBackend,
};

const PAYLOAD_BYTES: usize = 1 << 16;
const SHORT_TIMEOUT: Duration = Duration::from_millis(50);

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

#[test]
fn memfd_segment_publishes_identity_and_payload_bounds() {
    let server =
        SsvmPrivate::server_init_memfd(&memfd_config("hammer-ssvm-identity", PAYLOAD_BYTES))
            .expect("memfd segment");

    assert!(server.is_server());
    assert_eq!(server.segment_type(), SsvmSegmentBackend::Memfd);
    assert_eq!(server.name(), "hammer-ssvm-identity");
    assert_eq!(server.server_pid(), std::process::id());
    assert_eq!(server.client_pid(), 0);
    assert_eq!(server.ssvm_va(), 0, "addresses are free unless requested");
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
}

#[test]
fn client_mapping_reads_size_and_name_from_the_header() {
    let server = SsvmPrivate::server_init_memfd(&memfd_config("hammer-ssvm-header", PAYLOAD_BYTES))
        .expect("memfd segment");
    server.publish_ready();
    let descriptor = server.fd().expect("server descriptor");

    let client = SsvmPrivate::client_init_memfd(descriptor).expect("client mapping");

    assert!(!client.is_server());
    assert_eq!(client.segment_type(), SsvmSegmentBackend::Memfd);
    assert_eq!(client.name(), "hammer-ssvm-header");
    assert_eq!(client.ssvm_size(), server.ssvm_size());
    assert_eq!(client.payload_len(), server.payload_len());
    assert_eq!(client.client_pid(), std::process::id());
    assert!(
        client.is_ready(),
        "the creator already published the payload"
    );

    let payload_start = SSVM_PAYLOAD_OFFSET + 16;
    let writer = client
        .offset_ptr(payload_start, 4, 1)
        .expect("client payload pointer");
    // SAFETY: the range is inside the shared mapping below the header boundary,
    // and this test is the only writer.
    unsafe { std::slice::from_raw_parts_mut(writer, 4) }.copy_from_slice(&[0xA5; 4]);
    let reader = server
        .offset_ptr(payload_start, 4, 1)
        .expect("server payload pointer");
    // SAFETY: same block written above.
    assert_eq!(unsafe { std::slice::from_raw_parts(reader, 4) }, [0xA5; 4]);
}

#[test]
fn attach_does_not_wait_for_a_payload_that_is_not_ready() {
    let server = SsvmPrivate::server_init_memfd(&memfd_config("hammer-ssvm-ready", PAYLOAD_BYTES))
        .expect("memfd segment");
    let descriptor = server.fd().expect("server descriptor");

    let client = SsvmPrivate::client_init_memfd(descriptor).expect("client mapping");

    assert!(!client.is_ready());
    match client.wait_ready(SHORT_TIMEOUT) {
        Err(SsvmError::ClientTimeout { name, seconds }) => {
            assert_eq!(name, "hammer-ssvm-ready");
            assert_eq!(seconds, 0);
        }
        other => panic!("an unpublished payload must time out, got {other:?}"),
    }
}

#[test]
fn shm_segment_attaches_by_name_and_delete_unlinks_it() {
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

    let client = SsvmPrivate::client_init_shm(&name, SHORT_TIMEOUT).expect("shm attach");
    assert_eq!(client.ssvm_size(), server.ssvm_size());
    assert_eq!(client.name(), name);
    assert!(client.is_ready());

    drop(client);
    server.delete();
    match SsvmPrivate::client_init_shm(&name, SHORT_TIMEOUT) {
        Err(SsvmError::ClientTimeout { .. }) => {}
        other => panic!("a deleted shm segment must not attach, got {other:?}"),
    }
}

#[test]
fn shm_attach_times_out_while_the_payload_is_unpublished() {
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

    match SsvmPrivate::client_init_shm(&name, SHORT_TIMEOUT) {
        Err(SsvmError::ClientTimeout { name: reported, .. }) => assert_eq!(reported, name),
        other => panic!("an unpublished shm segment must time out, got {other:?}"),
    }

    server.publish_ready();
    SsvmPrivate::client_init_shm(&name, SHORT_TIMEOUT).expect("attach after publication");
}

#[test]
fn private_mapping_is_never_attachable() {
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

    match SsvmPrivate::client_init(&config, None) {
        Err(SsvmError::BackendUnavailable) => {}
        other => panic!("a private mapping must not attach, got {other:?}"),
    }
}

#[test]
fn invalid_names_and_sizes_are_typed_failures() {
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

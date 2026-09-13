#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::time::Duration;

use hammer_infra::svm::fifo::{FifoSegmentHeader, SvmFifoShared};
use hammer_infra::svm::fifo_segment::{FifoSegmentFtype, SvmFifoSegment, SvmFifoSegmentConfig};
use hammer_infra::svm::ssvm::{SsvmConfig, SsvmPrivate, SsvmSegmentBackend};

const SEGMENT_SIZE: usize = 1 << 20;

fn memfd_config(name: &str) -> SsvmConfig {
    SsvmConfig {
        backend: SsvmSegmentBackend::Memfd,
        name: name.to_string(),
        size: SEGMENT_SIZE,
        requested_va: 0,
        huge_page: false,
        attach_timeout: Duration::from_millis(100),
    }
}

#[test]
fn fifo_segment_uses_and_reclaims_shared_freelists() {
    let segment = Arc::new(
        SsvmPrivate::server_init_fifo_segment(&SsvmConfig {
            backend: SsvmSegmentBackend::Private,
            name: "hammer-fifo-segment".to_string(),
            size: SEGMENT_SIZE,
            requested_va: 0,
            huge_page: false,
            attach_timeout: Duration::from_millis(100),
        })
        .expect("private segment"),
    );
    let mut fifo_segment = SvmFifoSegment::new(
        Arc::clone(&segment),
        SvmFifoSegmentConfig {
            slices: 2,
            ..SvmFifoSegmentConfig::default()
        },
    )
    .expect("fifo segment");

    fifo_segment.preallocate_chunks(1, 8192, 8).expect("chunks");
    fifo_segment
        .preallocate_fifo_headers(1, 2)
        .expect("headers");
    assert_eq!(fifo_segment.free_chunk_count(8192), 8);
    assert_eq!(fifo_segment.free_fifo_count(), 2);

    let index = fifo_segment
        .allocate_fifo(1, 7000, FifoSegmentFtype::RxFifo)
        .expect("fifo allocation");
    assert_eq!(fifo_segment.free_fifo_count(), 1);
    assert_eq!(fifo_segment.free_chunk_count(8192), 7);
    let fifo = fifo_segment.fifo(1, index).expect("fifo handle");
    let payload = vec![0xA5; 7000];
    assert_eq!(fifo.enqueue(&payload), payload.len());
    let mut received = vec![0; payload.len()];
    assert_eq!(fifo.dequeue(received.len(), &mut received), payload.len());
    assert_eq!(received, payload);

    fifo_segment.free_server_fifo(1, index).expect("fifo free");
    assert_eq!(fifo_segment.free_fifo_count(), 2);
    assert_eq!(fifo_segment.free_chunk_count(8192), 8);
    assert!(fifo_segment.cached_bytes() >= 8 * 4096);
    assert!(fifo_segment.available_bytes() >= fifo_segment.freelist_bytes());
}

#[test]
fn fifo_segment_attaches_shared_fifo_by_offset() {
    let server_segment = Arc::new(
        SsvmPrivate::server_init_fifo_segment(&memfd_config("hammer-fifo-segment-attach"))
            .expect("server segment"),
    );
    let mut server = SvmFifoSegment::new(
        Arc::clone(&server_segment),
        SvmFifoSegmentConfig {
            slices: 1,
            ..SvmFifoSegmentConfig::default()
        },
    )
    .expect("server fifo segment");
    let fifo_index = server
        .allocate_fifo(0, 4096, FifoSegmentFtype::RxFifo)
        .expect("server fifo");
    let fifo_offset = server
        .fifo(0, fifo_index)
        .expect("server fifo handle")
        .hdr_offset();
    let descriptor = server_segment.fd().expect("memfd descriptor");

    let client_segment =
        Arc::new(SsvmPrivate::client_init_memfd(descriptor).expect("client segment attach"));
    let mut client = SvmFifoSegment::attach(
        Arc::clone(&client_segment),
        SvmFifoSegmentConfig {
            slices: 1,
            ..SvmFifoSegmentConfig::default()
        },
    )
    .expect("client fifo segment attach");
    let client_index = client
        .attach_fifo(0, fifo_offset as usize)
        .expect("client fifo attach");

    let payload = b"shared fifo";
    assert_eq!(
        server.fifo(0, fifo_index).unwrap().enqueue(payload),
        payload.len()
    );
    let client_fifo = client.fifo(0, client_index).expect("client fifo handle");
    let mut received = [0; 11];
    assert_eq!(
        client_fifo.dequeue(received.len(), &mut received),
        payload.len()
    );
    assert_eq!(&received, payload);
    client.cleanup().expect("client fifo cleanup");
}

#[test]
fn fifo_segment_shared_layout_is_cache_aligned() {
    assert_eq!(std::mem::align_of::<FifoSegmentHeader>(), 64);
    assert_eq!(std::mem::size_of::<FifoSegmentHeader>() % 64, 0);
    assert_eq!(std::mem::align_of::<SvmFifoShared>(), 64);
    assert_eq!(std::mem::size_of::<SvmFifoShared>() % 64, 0);
}

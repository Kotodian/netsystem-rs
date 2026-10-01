#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::time::Duration;

use hammer_infra::svm::fifo_segment::{FifoSegmentFtype, SvmFifoSegment, SvmFifoSegmentConfig};
use hammer_infra::svm::ssvm::{SsvmConfig, SsvmPrivate, SsvmSegmentBackend};

fn allocated_fifo(capacity: usize) -> (SvmFifoSegment, u32) {
    let mapping = Arc::new(
        SsvmPrivate::server_init_fifo_segment(&SsvmConfig {
            backend: SsvmSegmentBackend::Private,
            name: "hammer-fifo-behavior".to_string(),
            size: 1 << 20,
            requested_va: 0,
            huge_page: false,
            attach_timeout: Duration::from_millis(100),
        })
        .expect("private FIFO segment mapping"),
    );
    let mut segment = SvmFifoSegment::new(
        mapping,
        SvmFifoSegmentConfig {
            first_allocation_percent: 50,
            ..SvmFifoSegmentConfig::default()
        },
    )
    .expect("FIFO segment");
    let index = segment
        .allocate_fifo(0, capacity, FifoSegmentFtype::RxFifo)
        .expect("FIFO allocation");
    (segment, index)
}

#[test]
fn fifo_round_trip_non_power_of_two_capacity() {
    let (mut segment, index) = allocated_fifo(4101);
    let fifo = segment.fifo(0, index).expect("fifo");
    let payload: Vec<u8> = (0..4090).map(|value| value as u8).collect();

    assert_eq!(fifo.enqueue(&payload), payload.len());
    assert_eq!(fifo.max_dequeue(), payload.len());
    let mut peeked = vec![0; payload.len()];
    assert_eq!(fifo.peek(0, peeked.len(), &mut peeked), payload.len());
    assert_eq!(peeked, payload);

    let mut received = vec![0; payload.len()];
    assert_eq!(fifo.dequeue(received.len(), &mut received), payload.len());
    assert_eq!(received, payload);
    assert!(fifo.is_empty());
    assert_eq!(fifo.max_enqueue(), 4101);
}

#[test]
fn fifo_accepts_data_after_full_drain() {
    let (segment, index) = allocated_fifo(4096);
    let fifo = segment.fifo(0, index).expect("fifo");
    let payload = vec![0x5a; 4096];

    for _ in 0..3 {
        assert_eq!(fifo.enqueue_ooo(0, &payload).unwrap().accepted, 4096);
        assert_eq!(fifo.max_enqueue(), 0);
        assert_eq!(fifo.drop_dequeue(payload.len()), payload.len());
        assert_eq!(fifo.max_enqueue(), payload.len());
    }
}

#[test]
fn fifo_extends_chunks_before_consumed_bytes_reach_tail() {
    let (segment, index) = allocated_fifo(4096);
    let fifo = segment.fifo(0, index).expect("fifo");
    let first = vec![0x11; 3072];
    let second = vec![0x22; 2048];

    assert_eq!(fifo.enqueue(&first), first.len());
    assert_eq!(fifo.drop_dequeue(2048), 2048);
    assert_eq!(fifo.enqueue(&second), second.len());
    assert_eq!(fifo.max_dequeue(), 3072);

    let mut received = vec![0; 3072];
    assert_eq!(fifo.dequeue(received.len(), &mut received), received.len());
    assert_eq!(&received[..1024], &first[2048..]);
    assert_eq!(&received[1024..], second.as_slice());
}

#[test]
fn fifo_extends_chunks_for_out_of_order_data() {
    let (segment, index) = allocated_fifo(4096);
    let fifo = segment.fifo(0, index).expect("fifo");

    assert_eq!(fifo.enqueue(&vec![0x11; 3072]), 3072);
    assert_eq!(fifo.drop_dequeue(2048), 2048);
    assert_eq!(fifo.enqueue_ooo(512, &[0x33; 1024]).unwrap().accepted, 1024);
    let contiguous = fifo.enqueue_ooo(0, &[0x22; 512]).unwrap();
    assert_eq!(contiguous.accepted, 512);
    assert_eq!(contiguous.delivered, 1024);

    let mut received = vec![0; 2560];
    assert_eq!(fifo.dequeue(received.len(), &mut received), received.len());
    assert!(received[..1024].iter().all(|&byte| byte == 0x11));
    assert!(received[1024..1536].iter().all(|&byte| byte == 0x22));
    assert!(received[1536..].iter().all(|&byte| byte == 0x33));
}

#[test]
fn fifo_extends_chunks_for_segmented_enqueue() {
    let (segment, index) = allocated_fifo(4096);
    let fifo = segment.fifo(0, index).expect("fifo");

    assert_eq!(fifo.enqueue(&vec![0x11; 3072]), 3072);
    assert_eq!(fifo.drop_dequeue(2048), 2048);
    assert_eq!(
        fifo.enqueue_segments(2048, [&[0x22; 512][..], &[0x33; 1536][..]]),
        Ok(2048)
    );

    let mut received = vec![0; 3072];
    assert_eq!(fifo.dequeue(received.len(), &mut received), received.len());
    assert!(received[..1024].iter().all(|&byte| byte == 0x11));
    assert!(received[1024..1536].iter().all(|&byte| byte == 0x22));
    assert!(received[1536..].iter().all(|&byte| byte == 0x33));
}

#[test]
fn fifo_extends_with_chunk_larger_than_initial_allocation() {
    let (segment, index) = allocated_fifo(8192);
    let fifo = segment.fifo(0, index).expect("fifo");
    let first = vec![0x11; 7168];
    let second = vec![0x22; 6144];

    assert_eq!(fifo.enqueue(&first), first.len());
    assert_eq!(fifo.drop_dequeue(6144), 6144);
    assert_eq!(fifo.enqueue(&second), second.len());

    let mut received = vec![0; 7168];
    assert_eq!(fifo.dequeue(received.len(), &mut received), received.len());
    assert_eq!(&received[..1024], &first[6144..]);
    assert_eq!(&received[1024..], second.as_slice());
}

#[test]
fn fifo_copies_across_chunks_and_preserves_segmented_enqueue() {
    let (mut segment, index) = allocated_fifo(8192);
    let fifo = segment.fifo(0, index).expect("fifo");
    let first = vec![0x11; 4096];
    let second = vec![0x22; 2904];

    assert_eq!(
        fifo.enqueue_segments(7000, [&first[..], &second[..]]),
        Ok(7000)
    );
    let mut received = vec![0; 7000];
    assert_eq!(fifo.dequeue(received.len(), &mut received), 7000);
    assert_eq!(&received[..4096], first.as_slice());
    assert_eq!(&received[4096..], second.as_slice());
}

#[test]
fn fifo_ooo_gap_overlap_and_wrap_are_ordered() {
    let (mut segment, index) = allocated_fifo(4097);
    let fifo = segment.fifo(0, index).expect("fifo");
    fifo.init_pointers(u32::MAX - 16, u32::MAX - 16);

    assert_eq!(
        fifo.enqueue_ooo(4, &[4, 5, 6, 7]),
        Ok(hammer_infra::svm::fifo::OooResult {
            accepted: 4,
            delivered: 0,
            start: Some(4),
            len: 4,
        })
    );
    assert_eq!(fifo.max_dequeue(), 0);
    assert_eq!(fifo.enqueue_ooo(0, &[0, 1, 2, 3]).unwrap().delivered, 4);
    assert_eq!(fifo.max_dequeue(), 8);

    let mut received = [0; 8];
    assert_eq!(fifo.dequeue(8, &mut received), 8);
    assert_eq!(received, [0, 1, 2, 3, 4, 5, 6, 7]);
}

#[test]
fn fifo_ooo_merges_duplicate_and_adjacent_ranges() {
    let (mut segment, index) = allocated_fifo(4097);
    let fifo = segment.fifo(0, index).expect("fifo");

    assert_eq!(fifo.enqueue_ooo(4, &[4, 5, 6, 7]).unwrap().start, Some(4));
    let merged = fifo.enqueue_ooo(6, &[6, 7, 8, 9]).unwrap();
    assert_eq!((merged.start, merged.len), (Some(4), 6));
    assert_eq!(fifo.enqueue_ooo(4, &[4, 5, 6, 7]).unwrap().start, None);
    assert_eq!(fifo.enqueue_ooo(0, &[0, 1, 2, 3]).unwrap().delivered, 6);

    let mut received = [0; 10];
    assert_eq!(fifo.dequeue(received.len(), &mut received), received.len());
    assert_eq!(received, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn fifo_ooo_promotes_across_chunk_after_in_order_fill() {
    let (mut segment, index) = allocated_fifo(8192);
    let fifo = segment.fifo(0, index).expect("fifo");
    let prefix = vec![0x11; 4092];
    let future = [0x22; 16];

    assert_eq!(fifo.enqueue_ooo(4092, &future).unwrap().accepted, 16);
    assert_eq!(fifo.max_dequeue(), 0);
    assert_eq!(fifo.enqueue_ooo(0, &prefix).unwrap().delivered, 16);
    let mut received = vec![0; prefix.len() + future.len()];
    assert_eq!(fifo.dequeue(received.len(), &mut received), received.len());
    assert_eq!(&received[..prefix.len()], prefix.as_slice());
    assert_eq!(&received[prefix.len()..], future.as_slice());
}

#[test]
fn fifo_ooo_capacity_rejection_preserves_published_tail() {
    let (mut segment, index) = allocated_fifo(4096);
    let fifo = segment.fifo(0, index).expect("fifo");
    let future = [0x22; 16];
    let prefix = vec![0x11; 4080];

    assert_eq!(fifo.enqueue_ooo(4080, &future).unwrap().accepted, 16);
    assert!(fifo.enqueue_ooo(4081, &future).is_err());
    assert_eq!(fifo.max_dequeue(), 0);
    assert_eq!(fifo.enqueue_ooo(0, &prefix).unwrap().delivered, 16);
    assert_eq!(fifo.max_dequeue(), 4096);
}

#[test]
fn fifo_segmented_enqueue_failure_does_not_publish_partial_bytes() {
    let (mut segment, index) = allocated_fifo(4096);
    let fifo = segment.fifo(0, index).expect("fifo");
    let first = [1, 2, 3, 4];
    let second = [5];
    let result = fifo.enqueue_segments(4, [&first[..], &second[..]]);

    assert!(result.is_err());
    assert_eq!(fifo.max_dequeue(), 0);
    assert_eq!(fifo.max_enqueue(), 4096);
}

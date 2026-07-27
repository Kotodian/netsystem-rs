use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;

fn fifo(cap: usize) -> Fifo {
    let segment = Segment::local(cap * 16 + (1 << 20));
    Fifo::new(segment, cap).expect("fifo")
}

#[test]
fn ooo_enqueue_tracks_segments() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    let result = f.enqueue_ooo(10, b"hello").unwrap();
    assert_eq!(result.accepted, 5);
    assert_eq!(result.delivered, 0);
    assert_eq!(result.start, Some(10));
    assert_eq!(f.ooo_enqueued(), 1);
}

#[test]
fn ooo_promote_contiguous_gap_not_filled() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    f.enqueue_ooo(10, b"hello").unwrap();

    f.enqueue(b"hello");
    assert_eq!(f.promote_contiguous(), 0);
}

#[test]
fn ooo_promote_contiguous_gap_filled() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    f.enqueue_ooo(10, b"hello").unwrap();

    f.enqueue(b"hello");

    let result = f.enqueue_ooo(0, b"hellohello").unwrap();
    assert_eq!(result.accepted, 5);
    assert_eq!(result.start, Some(0));
    assert_eq!(result.len, 10);
    assert!(result.delivered > 0);
}

#[test]
fn ooo_enqueue_does_not_advance_visible_tail_before_gap_fills() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    let result = f.enqueue_ooo(5, b"world").expect("ooo enqueue");

    assert_eq!(result.accepted, 5);
    assert_eq!(result.delivered, 0);
    assert_eq!(result.start, Some(5));
    assert_eq!(f.max_dequeue(), 0);
    assert_eq!(f.ooo_enqueued(), 1);
    let mut out = [0u8; 16];
    assert_eq!(f.peek(0, out.len(), &mut out), 0);
}

#[test]
fn in_order_enqueue_collects_contiguous_ooo_bytes() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    f.enqueue_ooo(5, b"world").expect("ooo enqueue");
    assert_eq!(f.enqueue(b"hello"), 10);

    let mut out = [0u8; 16];
    assert_eq!(f.peek(0, out.len(), &mut out), 10);
    assert_eq!(&out[..10], b"helloworld");
    assert_eq!(f.ooo_enqueued(), 0);
}

#[test]
fn in_order_enqueue_after_future_ooo_uses_visible_tail_chunk() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    assert_eq!(f.enqueue(b"abcd"), 4);
    f.enqueue_ooo(4, b"ijkl").expect("ooo enqueue");
    assert_eq!(f.enqueue(b"efgh"), 8);

    let mut out = [0u8; 16];
    assert_eq!(f.peek(0, out.len(), &mut out), 12);
    assert_eq!(&out[..12], b"abcdefghijkl");
    assert_eq!(f.ooo_enqueued(), 0);
}

#[test]
fn in_order_enqueue_inserts_gap_chunk_before_future_ooo_chunk() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    let first_chunk = vec![b'a'; 4096];
    let gap_chunk = vec![b'b'; 4096];
    assert_eq!(f.enqueue(&first_chunk), first_chunk.len());
    f.enqueue_ooo(4096, b"future").expect("ooo enqueue");

    assert_eq!(f.enqueue(&gap_chunk), gap_chunk.len() + b"future".len());

    let mut out = vec![0u8; first_chunk.len() + gap_chunk.len() + b"future".len()];
    assert_eq!(f.peek(0, out.len(), &mut out), out.len());
    assert_eq!(&out[..first_chunk.len()], first_chunk.as_slice());
    assert_eq!(
        &out[first_chunk.len()..first_chunk.len() + gap_chunk.len()],
        gap_chunk.as_slice()
    );
    assert_eq!(&out[first_chunk.len() + gap_chunk.len()..], b"future");
    assert_eq!(f.ooo_enqueued(), 0);

    assert_eq!(f.enqueue(b"!"), 1);
    let mut out = vec![0u8; first_chunk.len() + gap_chunk.len() + b"future!".len()];
    assert_eq!(f.peek(0, out.len(), &mut out), out.len());
    assert_eq!(&out[first_chunk.len() + gap_chunk.len()..], b"future!");
}

#[test]
fn ooo_enqueue_rejects_future_write_beyond_fifo_capacity() {
    let mut f = fifo(64);
    f.enable_ooo();

    assert!(f.enqueue_ooo(60, b"12345").is_err());
    assert_eq!(f.max_dequeue(), 0);
    assert_eq!(f.ooo_enqueued(), 0);
}

#[test]
fn duplicate_ooo_enqueue_reports_zero_newly_accepted_bytes() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    let first = f.enqueue_ooo(5, b"world").expect("first ooo enqueue");
    let duplicate = f.enqueue_ooo(5, b"world").expect("duplicate ooo enqueue");

    assert_eq!(first.accepted, 5);
    assert_eq!(duplicate.accepted, 0);
    assert_eq!(duplicate.delivered, 0);
    assert_eq!(duplicate.start, None);
}

#[test]
fn partial_overlap_ooo_enqueue_reports_retained_span_len() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    let first = f.enqueue_ooo(5, b"world").expect("first ooo enqueue");
    let overlap = f
        .enqueue_ooo(7, b"rld!!")
        .expect("partially overlapping ooo enqueue");

    assert_eq!(first.accepted, 5);
    assert_eq!(first.start, Some(5));
    assert_eq!(first.len, 5);

    assert_eq!(overlap.accepted, 2);
    assert_eq!(overlap.delivered, 0);
    assert_eq!(overlap.start, Some(5));
    assert_eq!(overlap.len, 7);
}

#[test]
fn ooo_segment_storage_grows_on_demand() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    for offset in (1..=17).map(|index| index * 2) {
        let result = f.enqueue_ooo(offset, &[offset as u8]).expect("ooo enqueue");
        assert_eq!(result.accepted, 1);
    }

    assert_eq!(f.ooo_enqueued(), 17);
}

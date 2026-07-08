use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Local;

fn fifo(cap: usize) -> Fifo<Local> {
    let seg = Local::new(cap * 16 + (1 << 20));
    Fifo::<Local>::new(seg, cap).expect("fifo")
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
    assert_eq!(result.accepted, 10);
    assert_eq!(result.start, Some(0));
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

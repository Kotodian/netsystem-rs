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
    assert_eq!(result.delivered, 0);
    assert_eq!(f.ooo_enqueued(), 1);
}

#[test]
fn ooo_promote_contiguous_gap_not_filled() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    f.enqueue_ooo(10, b"hello").unwrap();

    f.enqueue_at(0, b"hello");
    assert_eq!(f.promote_contiguous(), 0);
}

#[test]
fn ooo_promote_contiguous_gap_filled() {
    let mut f = fifo(1 << 16);
    f.enable_ooo();

    f.enqueue_ooo(10, b"hello").unwrap();

    f.enqueue_at(0, b"hello");

    let result = f.enqueue_ooo(0, b"hellohello").unwrap();
    assert!(result.delivered > 0);
}

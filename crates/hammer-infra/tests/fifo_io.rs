use std::io::{BufRead, ErrorKind, Read, Write};

use hammer_infra::fifo::Fifo;
use hammer_infra::segment::Segment;

fn local_fifo(capacity: usize) -> Fifo {
    Fifo::new(Segment::local(1 << 20), capacity).expect("local FIFO")
}

#[test]
fn fifo_read_and_write_follow_nonblocking_io_semantics() {
    let fifo = local_fifo(4);
    let mut writer = &fifo;
    let mut reader = &fifo;

    assert_eq!(writer.write(b"data").expect("write FIFO"), 4);
    assert_eq!(
        writer.write(b"x").expect_err("full FIFO").kind(),
        ErrorKind::WouldBlock
    );

    let mut observed = [0; 4];
    assert_eq!(reader.read(&mut observed).expect("read FIFO"), 4);
    assert_eq!(&observed, b"data");
    assert_eq!(
        reader.read(&mut observed).expect_err("empty FIFO").kind(),
        ErrorKind::WouldBlock
    );
}

#[test]
fn reservation_write_remains_invisible_until_commit() {
    let fifo = local_fifo(8);
    let mut reservation = fifo.reserve_write(4).expect("FIFO reservation");

    let (first, second) = reservation.segments_mut();
    let first_len = first.len();
    first.copy_from_slice(&b"data"[..first_len]);
    second.copy_from_slice(&b"data"[first_len..]);
    assert_eq!(fifo.max_dequeue(), 0);

    assert_eq!(reservation.commit(4).expect("commit FIFO reservation"), 4);
    let mut reader = &fifo;
    let mut observed = [0; 4];
    reader
        .read_exact(&mut observed)
        .expect("read committed bytes");
    assert_eq!(&observed, b"data");
}

#[test]
fn fifo_buf_read_consumes_the_current_segment() {
    let fifo = local_fifo(8);
    assert_eq!(fifo.enqueue(b"data"), 4);

    let mut reader = &fifo;
    assert_eq!(reader.fill_buf().expect("readable FIFO segment"), b"data");
    reader.consume(2);
    assert_eq!(reader.fill_buf().expect("remaining FIFO segment"), b"ta");
}

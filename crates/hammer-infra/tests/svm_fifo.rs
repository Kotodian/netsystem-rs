use hammer_infra::svm::fifo::Fifo;

#[test]
fn fifo_round_trip_non_power_of_two_capacity() {
    let fifo = Fifo::with_capacity(101).expect("fifo");
    let payload: Vec<u8> = (0..90).map(|value| value as u8).collect();

    assert_eq!(fifo.enqueue(&payload), payload.len());
    assert_eq!(fifo.max_dequeue(), payload.len());
    let (first, second) = fifo.segments(0, payload.len()).expect("segments");
    assert_eq!([first, second].concat(), payload);

    let mut received = vec![0; payload.len()];
    assert_eq!(fifo.dequeue(received.len(), &mut received), payload.len());
    assert_eq!(received, payload);
    assert!(fifo.is_empty());
    assert_eq!(fifo.max_enqueue(), 101);
}

#[test]
fn fifo_copies_across_chunks_and_preserves_segmented_enqueue() {
    let fifo = Fifo::with_capacity(8192).expect("fifo");
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
    let fifo = Fifo::with_capacity(101).expect("fifo");
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
fn fifo_segmented_enqueue_failure_does_not_publish_partial_bytes() {
    let fifo = Fifo::with_capacity(128).expect("fifo");
    let first = [1, 2, 3, 4];
    let second = [5];
    let result = fifo.enqueue_segments(4, [&first[..], &second[..]]);

    assert!(result.is_err());
    assert_eq!(fifo.max_dequeue(), 0);
    assert_eq!(fifo.max_enqueue(), 128);
}

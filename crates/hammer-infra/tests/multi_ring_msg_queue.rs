//! Behavioral tests for the session-neutral VPP-shaped multi-ring message queue.
//! Assert observable enqueue/dequeue outcomes only — no source greps.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use hammer_infra::multi_ring_msg_queue::{
    MultiProducer, MultiRingMsgQueue, MultiRingMsgQueueCfg, MultiRingMsgQueueError, RingCfg,
};
use hammer_infra::segment::Segment;

fn u64_queue() -> MultiRingMsgQueue {
    MultiRingMsgQueue::with_cfg(MultiRingMsgQueueCfg {
        q_nitems: 64,
        rings: &[
            RingCfg {
                nitems: 32,
                elsize: 8,
            },
            RingCfg {
                nitems: 16,
                elsize: 8,
            },
        ],
    })
    .expect("queue")
}

fn write_u64(q: &MultiRingMsgQueue, ring: u32, value: u64) -> Result<(), MultiRingMsgQueueError> {
    let mut guard = q.lock();
    let mut slot = guard.alloc(ring)?;
    slot.as_mut_slice().copy_from_slice(&value.to_le_bytes());
    guard.add(slot);
    Ok(())
}

fn read_u64(msg: &hammer_infra::multi_ring_msg_queue::RingMsg<'_>) -> u64 {
    let bytes: [u8; 8] = msg.as_slice().try_into().expect("elsize 8");
    u64::from_le_bytes(bytes)
}

#[test]
fn two_rings_roundtrip_preserves_payload_and_ring() {
    let q = u64_queue();

    write_u64(&q, 0, 0x1111_1111_1111_1111).expect("io ring");
    write_u64(&q, 1, 0x2222_2222_2222_2222).expect("ctrl ring");

    let first = q.sub().expect("first");
    assert_eq!(first.ring_index(), 0);
    assert_eq!(read_u64(&first), 0x1111_1111_1111_1111);
    drop(first);

    let second = q.sub().expect("second");
    assert_eq!(second.ring_index(), 1);
    assert_eq!(read_u64(&second), 0x2222_2222_2222_2222);
    drop(second);

    assert!(q.sub().is_none());
}

#[test]
fn drop_reclaims_ring_slot_for_reuse() {
    // One-slot ring: without reclaim the second alloc must fail; with Drop reclaim it succeeds.
    let q = MultiRingMsgQueue::with_cfg(MultiRingMsgQueueCfg {
        q_nitems: 8,
        rings: &[RingCfg {
            nitems: 2, // usable depth 1 for power-of-two empty/full convention if used
            elsize: 8,
        }],
    })
    .expect("queue");

    // Fill every ring data slot once, dequeue+drop each, then refill — proves reclaim.
    let capacity = 2u64;
    for i in 0..capacity {
        write_u64(&q, 0, i).expect("fill");
    }
    // Ring or queue should be full for another write before reclaim.
    let full = write_u64(&q, 0, 99);
    assert!(
        matches!(
            full,
            Err(MultiRingMsgQueueError::RingFull | MultiRingMsgQueueError::QueueFull)
        ),
        "expected full before reclaim, got {full:?}"
    );

    let mut seen = Vec::new();
    while let Some(msg) = q.sub() {
        seen.push(read_u64(&msg));
        // Drop frees ring slot.
    }
    assert_eq!(seen.len(), capacity as usize);

    for i in 100..100 + capacity {
        write_u64(&q, 0, i).expect("reuse after reclaim");
    }
    let mut again = Vec::new();
    while let Some(msg) = q.sub() {
        again.push(read_u64(&msg));
    }
    assert_eq!(again, (100..100 + capacity).collect::<Vec<_>>());
}

#[test]
fn queue_full_is_observable_when_descriptor_queue_saturates() {
    // Tiny descriptor queue, larger ring capacity: saturating descriptors fails with QueueFull.
    let q = MultiRingMsgQueue::with_cfg(MultiRingMsgQueueCfg {
        q_nitems: 2,
        rings: &[RingCfg {
            nitems: 16,
            elsize: 8,
        }],
    })
    .expect("queue");

    write_u64(&q, 0, 1).expect("first");
    let second = write_u64(&q, 0, 2);
    // With q_nitems == 2, usable depth is typically 1 (empty/full share a slot) or 2 —
    // either way a small queue must eventually report QueueFull before ring exhaustion.
    let mut enqueued = 1usize;
    if second.is_ok() {
        enqueued = 2;
    } else {
        assert!(matches!(second, Err(MultiRingMsgQueueError::QueueFull)));
    }
    loop {
        match write_u64(&q, 0, 3) {
            Ok(()) => enqueued += 1,
            Err(MultiRingMsgQueueError::QueueFull) => break,
            Err(other) => panic!("expected QueueFull, got {other:?}"),
        }
        assert!(enqueued < 16, "should hit queue full before ring full");
    }
}

#[test]
fn ring_full_is_observable_when_ring_saturates() {
    // Large descriptor queue, tiny ring: alloc fails with RingFull.
    let q = MultiRingMsgQueue::with_cfg(MultiRingMsgQueueCfg {
        q_nitems: 64,
        rings: &[RingCfg {
            nitems: 2,
            elsize: 8,
        }],
    })
    .expect("queue");

    write_u64(&q, 0, 1).expect("first");
    // Second may succeed depending on empty/full convention; keep going until RingFull.
    loop {
        match write_u64(&q, 0, 2) {
            Ok(()) => {}
            Err(MultiRingMsgQueueError::RingFull) => break,
            Err(other) => panic!("expected RingFull, got {other:?}"),
        }
    }
}

#[test]
fn concurrent_producers_preserve_all_elements() {
    let q = Arc::new(u64_queue());
    let producers = 8usize;
    let per_producer = 100usize;
    let total = producers * per_producer;
    let started = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for p in 0..producers {
        let q = Arc::clone(&q);
        let started = Arc::clone(&started);
        handles.push(thread::spawn(move || {
            started.fetch_add(1, Ordering::SeqCst);
            while started.load(Ordering::SeqCst) < producers {
                thread::yield_now();
            }
            for i in 0..per_producer {
                let value = ((p as u64) << 32) | (i as u64);
                // Spin on full until consumer drains — MP safety under contention.
                loop {
                    match write_u64(&q, (p % 2) as u32, value) {
                        Ok(()) => break,
                        Err(
                            MultiRingMsgQueueError::QueueFull | MultiRingMsgQueueError::RingFull,
                        ) => {
                            thread::yield_now();
                        }
                        Err(other) => panic!("unexpected {other:?}"),
                    }
                }
            }
        }));
    }

    let mut got = HashSet::with_capacity(total);
    while got.len() < total {
        if let Some(msg) = q.sub() {
            got.insert(read_u64(&msg));
        } else {
            thread::yield_now();
        }
    }

    for h in handles {
        h.join().expect("producer");
    }

    assert_eq!(got.len(), total);
    for p in 0..producers {
        for i in 0..per_producer {
            let value = ((p as u64) << 32) | (i as u64);
            assert!(got.contains(&value), "missing {value:#x}");
        }
    }
    assert!(q.sub().is_none());
}

#[test]
fn svm_init_at_and_from_shared_roundtrip_preserves_payload() {
    let cfg = MultiRingMsgQueueCfg {
        q_nitems: 8,
        rings: &[
            RingCfg {
                nitems: 4,
                elsize: 8,
            },
            RingCfg {
                nitems: 4,
                elsize: 8,
            },
        ],
    };
    let bytes = MultiRingMsgQueue::<MultiProducer>::layout_bytes(&cfg);
    let seg = Segment::shared_default();
    let off = seg.alloc(bytes, 64).expect("queue allocation");

    let producer = unsafe { MultiRingMsgQueue::init_at(seg.clone(), off, &cfg) }.expect("init_at");
    write_u64(&producer, 0, 0x1111_2222_3333_4444).expect("io");
    write_u64(&producer, 1, 0xaaaa_bbbb_cccc_dddd).expect("ctrl");

    let consumer = unsafe { MultiRingMsgQueue::from_shared(seg, off) }.expect("from_shared");
    let first = consumer.sub().expect("first");
    assert_eq!(first.ring_index(), 0);
    assert_eq!(read_u64(&first), 0x1111_2222_3333_4444);
    drop(first);
    let second = consumer.sub().expect("second");
    assert_eq!(second.ring_index(), 1);
    assert_eq!(read_u64(&second), 0xaaaa_bbbb_cccc_dddd);
    drop(second);
    assert!(consumer.sub().is_none());
}

#[test]
fn single_producer_mode_roundtrips_without_freelist() {
    use hammer_infra::multi_ring_msg_queue::SingleProducer;

    let mut q = MultiRingMsgQueue::<SingleProducer>::with_cfg(MultiRingMsgQueueCfg {
        q_nitems: 8,
        rings: &[RingCfg {
            nitems: 4,
            elsize: 16,
        }],
    })
    .expect("queue");

    let mut producer = q.claim_producer().expect("claim producer");
    let mut reservation = producer.reserve(0).expect("reserve");
    reservation
        .payload_mut()
        .copy_from_slice(b"abcdefghijklmnop");
    assert!(
        reservation.publish(),
        "first publish on an empty queue must report the empty -> nonempty transition"
    );
    drop(reservation);

    let msg = q.sub().expect("message");
    assert_eq!(msg.as_slice(), b"abcdefghijklmnop");
    drop(msg);
    assert!(q.sub().is_none());

    // The slot was returned in order; the producer can reuse it without a
    // freelist or ABA bookkeeping.
    let mut reservation = producer.reserve(0).expect("reserve after free");
    reservation
        .payload_mut()
        .copy_from_slice(b"0123456789abcdef");
    reservation.publish();
    drop(reservation);
    let msg = q.sub().expect("reused message");
    assert_eq!(msg.as_slice(), b"0123456789abcdef");
    drop(msg);
    assert!(q.sub().is_none());
}

#[test]
fn single_producer_mode_full_and_wrap_are_typed() {
    use hammer_infra::multi_ring_msg_queue::SingleProducer;

    let mut q = MultiRingMsgQueue::<SingleProducer>::with_cfg(MultiRingMsgQueueCfg {
        q_nitems: 64,
        rings: &[RingCfg {
            nitems: 4,
            elsize: 8,
        }],
    })
    .expect("queue");
    let mut producer = q.claim_producer().expect("claim producer");

    for i in 0..4u64 {
        let mut reservation = producer.reserve(0).expect("reserve");
        reservation.payload_mut().copy_from_slice(&i.to_le_bytes());
        reservation.publish();
    }
    // Ring is full: the fifth reserve is a typed error, never a panic.
    assert!(matches!(
        producer.reserve(0),
        Err(MultiRingMsgQueueError::RingFull)
    ));

    // Consume in order; each Drop returns the slot, so the cursors wrap.
    for i in 0..4 {
        let msg = q.sub().expect("message");
        assert_eq!(read_u64(&msg), i);
        drop(msg);
    }
    assert!(q.sub().is_none());

    // Cursor wrapped: slot 0 is reused after the full round.
    let mut reservation = producer.reserve(0).expect("reserve after wrap");
    reservation
        .payload_mut()
        .copy_from_slice(&99u64.to_le_bytes());
    reservation.publish();
    drop(reservation);
    let msg = q.sub().expect("wrapped message");
    assert_eq!(read_u64(&msg), 99);
    drop(msg);
}

#[test]
fn single_producer_mode_descriptor_queue_full_is_typed() {
    use hammer_infra::multi_ring_msg_queue::SingleProducer;

    let mut q = MultiRingMsgQueue::<SingleProducer>::with_cfg(MultiRingMsgQueueCfg {
        q_nitems: 2,
        rings: &[RingCfg {
            nitems: 16,
            elsize: 8,
        }],
    })
    .expect("queue");
    let mut producer = q.claim_producer().expect("claim producer");

    for i in 0..2u64 {
        let mut reservation = producer.reserve(0).expect("reserve");
        reservation.payload_mut().copy_from_slice(&i.to_le_bytes());
        reservation.publish();
    }
    assert!(matches!(
        producer.reserve(0),
        Err(MultiRingMsgQueueError::QueueFull)
    ));

    // Consuming one descriptor frees space for the next reserve.
    drop(q.sub().expect("message"));
    let mut reservation = producer.reserve(0).expect("reserve after consume");
    reservation
        .payload_mut()
        .copy_from_slice(&7u64.to_le_bytes());
    reservation.publish();
    drop(reservation);
    let mut seen = Vec::new();
    while let Some(msg) = q.sub() {
        seen.push(read_u64(&msg));
    }
    assert_eq!(seen, vec![1, 7]);
}

#[test]
fn single_producer_mode_mapping_mode_mismatch_is_typed() {
    use hammer_infra::multi_ring_msg_queue::SingleProducer;

    let cfg = MultiRingMsgQueueCfg {
        q_nitems: 8,
        rings: &[RingCfg {
            nitems: 4,
            elsize: 8,
        }],
    };
    let bytes = MultiRingMsgQueue::<MultiProducer>::layout_bytes(&cfg);
    let seg = Segment::shared_default();
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    unsafe { MultiRingMsgQueue::<SingleProducer>::init_at(seg.clone(), off, &cfg) }
        .expect("SP init");

    // An SP queue mapped as MP must fail with the mode tag mismatch.
    let error = match unsafe { MultiRingMsgQueue::<MultiProducer>::from_shared(seg.clone(), off) } {
        Err(error) => error,
        Ok(_) => panic!("SP queue mapped as MP must be rejected"),
    };
    assert!(matches!(
        error,
        MultiRingMsgQueueError::ModeMismatch {
            expected: 0,
            actual: 1
        }
    ));

    // And an MP queue mapped as SP fails symmetrically.
    let mp_bytes = MultiRingMsgQueue::<MultiProducer>::layout_bytes(&cfg);
    let mp_seg = Segment::shared_default();
    let mp_off = mp_seg.alloc(mp_bytes, 64).expect("queue allocation");
    unsafe { MultiRingMsgQueue::<MultiProducer>::init_at(mp_seg.clone(), mp_off, &cfg) }
        .expect("MP init");
    let error = match unsafe { MultiRingMsgQueue::<SingleProducer>::from_shared(mp_seg, mp_off) } {
        Err(error) => error,
        Ok(_) => panic!("MP queue mapped as SP must be rejected"),
    };
    assert!(matches!(
        error,
        MultiRingMsgQueueError::ModeMismatch {
            expected: 1,
            actual: 0
        }
    ));
}

#[test]
fn single_producer_cross_mapping_double_claim_is_typed() {
    use hammer_infra::multi_ring_msg_queue::SingleProducer;

    let cfg = MultiRingMsgQueueCfg {
        q_nitems: 8,
        rings: &[RingCfg {
            nitems: 4,
            elsize: 8,
        }],
    };
    let bytes = MultiRingMsgQueue::<MultiProducer>::layout_bytes(&cfg);
    let seg = Segment::shared_default();
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    unsafe { MultiRingMsgQueue::<SingleProducer>::init_at(seg.clone(), off, &cfg) }
        .expect("SP init");

    let first = unsafe { MultiRingMsgQueue::<SingleProducer>::from_shared(seg.clone(), off) }
        .expect("first mapping");
    let second = unsafe { MultiRingMsgQueue::<SingleProducer>::from_shared(seg, off) }
        .expect("second mapping");

    first.claim_producer().expect("first claim");
    assert!(matches!(
        second.claim_producer(),
        Err(MultiRingMsgQueueError::ProducerClaimed)
    ));
}

#[test]
fn single_producer_concurrent_fifo_preserves_order_and_reuses_slots() {
    use hammer_infra::multi_ring_msg_queue::SingleProducer;

    const TOTAL: u64 = 100_000;
    let cfg = MultiRingMsgQueueCfg {
        q_nitems: 4096,
        rings: &[RingCfg {
            nitems: 1024,
            elsize: 16,
        }],
    };
    let bytes = MultiRingMsgQueue::<MultiProducer>::layout_bytes(&cfg);
    let seg = Segment::shared_default();
    let off = seg.alloc(bytes, 64).expect("queue allocation");
    unsafe { MultiRingMsgQueue::<SingleProducer>::init_at(seg.clone(), off, &cfg) }
        .expect("SP init");

    let mut queue = unsafe { MultiRingMsgQueue::<SingleProducer>::from_shared(seg.clone(), off) }
        .expect("consumer mapping");
    let producer = queue.claim_producer().expect("claim producer");

    let consumer = thread::spawn(move || {
        let mut expected = 0u64;
        while expected < TOTAL {
            if let Some(msg) = queue.sub() {
                let bytes: [u8; 16] = msg.as_slice().try_into().expect("elsize 16");
                let value = u64::from_le_bytes(bytes[..8].try_into().expect("value"));
                assert_eq!(value, expected, "FIFO order violated");
                expected += 1;
                // Drop returns the slot in order; a freelist/ABA bug would
                // surface here as a duplicate or corrupted sequence value.
            }
        }
        assert!(queue.sub().is_none());
        expected
    });

    let mut producer = producer;
    for i in 0..TOTAL {
        loop {
            match producer.reserve(0) {
                Ok(mut reservation) => {
                    let payload = reservation.payload_mut();
                    payload[..8].copy_from_slice(&i.to_le_bytes());
                    payload[8..].fill(0);
                    reservation.publish();
                    break;
                }
                Err(MultiRingMsgQueueError::RingFull | MultiRingMsgQueueError::QueueFull) => {
                    thread::yield_now();
                }
                Err(other) => panic!("unexpected {other:?}"),
            }
        }
    }

    assert_eq!(consumer.join().expect("consumer"), TOTAL);
}

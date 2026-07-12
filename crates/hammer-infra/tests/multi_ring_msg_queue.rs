//! Behavioral tests for the session-neutral VPP-shaped multi-ring message queue.
//! Assert observable enqueue/dequeue outcomes only — no source greps.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use hammer_infra::multi_ring_msg_queue::{
    MultiRingMsgQueue, MultiRingMsgQueueCfg, MultiRingMsgQueueError, RingCfg,
};

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

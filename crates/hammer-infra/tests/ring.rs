use hammer_infra::align::CACHE_LINE;
use hammer_infra::ring::{
    CompletionDescriptor, IndexedRing, LocalRing, LockFreeRing, LockFreeRingCursors,
    LockFreeRingHeadTail, RingEntry, RingError, SubmissionDescriptor,
};

#[test]
fn local_ring_preserves_fifo_order_across_wraparound() {
    let mut ring = LocalRing::with_capacity(3);

    assert_eq!(ring.pop(), None);

    assert!(ring.try_push(1).is_ok());
    assert!(ring.try_push(2).is_ok());
    assert_eq!(ring.pop(), Some(1));

    assert!(ring.try_push(3).is_ok());
    assert!(ring.try_push(4).is_ok());

    assert_eq!(ring.len(), 3);
    assert_eq!(ring.capacity(), 3);
    assert_eq!(ring.pop(), Some(2));
    assert_eq!(ring.pop(), Some(3));
    assert_eq!(ring.pop(), Some(4));
    assert_eq!(ring.pop(), None);
    assert!(ring.is_empty());
}

#[test]
fn local_ring_rejects_push_when_full_without_dropping_value() {
    let mut ring = LocalRing::with_capacity(2);

    assert!(ring.try_push(10).is_ok());
    assert!(ring.try_push(20).is_ok());

    let value = ring.try_push(30).unwrap_err();
    assert_eq!(value, 30);

    assert_eq!(ring.len(), 2);
    assert_eq!(ring.pop(), Some(10));
    assert_eq!(ring.pop(), Some(20));
    assert_eq!(ring.pop(), None);
}

#[test]
fn generic_submission_and_completion_descriptors_are_transport_agnostic() {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Opcode {
        Send,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Object {
        Flow(u64),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Payload {
        Buffer(u64),
    }

    let sqe = SubmissionDescriptor::new(Opcode::Send, 11_u64, Object::Flow(7), Payload::Buffer(9));
    assert_eq!(sqe.opcode(), Opcode::Send);
    assert_eq!(sqe.user_data(), 11);
    assert_eq!(sqe.object(), Object::Flow(7));
    assert_eq!(sqe.payload(), Payload::Buffer(9));

    let cqe =
        CompletionDescriptor::new(11_u64, 128_i32, 3_u32, Object::Flow(7), Payload::Buffer(9));
    assert_eq!(cqe.user_data(), 11);
    assert_eq!(cqe.result(), 128);
    assert_eq!(cqe.flags(), 3);
    assert_eq!(cqe.object(), Object::Flow(7));
    assert_eq!(cqe.payload(), Payload::Buffer(9));
}

#[test]
fn ring_entry_can_attach_transport_specific_state_to_generic_descriptors() {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Opcode {
        Recv,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Object {
        Flow(u64),
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Payload {
        Buffer(u64),
    }

    let descriptor =
        SubmissionDescriptor::new(Opcode::Recv, 7_u64, Object::Flow(11), Payload::Buffer(19));
    let entry = RingEntry::with_attachment(descriptor, "registered-buffer");

    assert_eq!(*entry.descriptor(), descriptor);
    assert_eq!(entry.attachment(), Some(&"registered-buffer"));

    let (round_trip_descriptor, attachment) = entry.into_parts();
    assert_eq!(round_trip_descriptor, descriptor);
    assert_eq!(attachment, Some("registered-buffer"));
}

#[test]
fn indexed_ring_queues_slot_ids_while_entries_live_in_a_slot_table() {
    let mut ring = IndexedRing::with_capacity(2);

    let first = ring.try_push("first").expect("push first");
    let second = ring.try_push("second").expect("push second");

    assert_eq!(ring.len(), 2);
    assert_eq!(ring.capacity(), 2);
    assert_eq!(ring.entry(first), Some(&"first"));
    assert_eq!(ring.entry(second), Some(&"second"));

    let (first_slot, first_entry) = ring.pop().expect("pop first");
    assert_eq!(first_slot, first);
    assert_eq!(first_entry, "first");
    assert_eq!(ring.entry(first), None);
    assert_eq!(ring.entry(second), Some(&"second"));

    let (second_slot, second_entry) = ring.pop().expect("pop second");
    assert_eq!(second_slot, second);
    assert_eq!(second_entry, "second");
    assert_eq!(ring.entry(second), None);
    assert!(ring.pop().is_none());
}

#[test]
fn indexed_ring_reuses_released_slots_after_pop() {
    let mut ring = IndexedRing::with_capacity(2);

    let first = ring.try_push("first").expect("push first");
    let second = ring.try_push("second").expect("push second");
    assert_ne!(first, second);

    let (released, value) = ring.pop().expect("pop first");
    assert_eq!(released, first);
    assert_eq!(value, "first");

    let recycled = ring.try_push("third").expect("push third");
    assert_eq!(recycled, first);
    assert_eq!(ring.entry(recycled), Some(&"third"));
    assert_eq!(ring.entry(second), Some(&"second"));
}

#[test]
fn lock_free_ring_sp_sc_tracks_capacity_and_wraparound() {
    let ring = LockFreeRing::with_capacity(4).expect("ring");

    assert_eq!(ring.capacity(), 3);
    assert_eq!(ring.available_to_read(), 0);
    assert_eq!(ring.available_to_write(), 3);

    assert_eq!(ring.enqueue_sp(10), Ok(()));
    assert_eq!(ring.enqueue_sp(11), Ok(()));
    assert_eq!(ring.enqueue_sp(12), Ok(()));
    assert_eq!(ring.enqueue_sp(13), Err(RingError::Full(13)));
    assert_eq!(ring.available_to_read(), 3);
    assert_eq!(ring.available_to_write(), 0);

    assert_eq!(ring.dequeue_sc(), Some(10));
    assert_eq!(ring.dequeue_sc(), Some(11));
    assert_eq!(ring.available_to_read(), 1);
    assert_eq!(ring.available_to_write(), 2);

    assert_eq!(ring.enqueue_sp(13), Ok(()));
    assert_eq!(ring.enqueue_sp(14), Ok(()));
    assert_eq!(ring.dequeue_sc(), Some(12));
    assert_eq!(ring.dequeue_sc(), Some(13));
    assert_eq!(ring.dequeue_sc(), Some(14));
    assert_eq!(ring.dequeue_sc(), None);
}

#[test]
fn lock_free_ring_rejects_non_power_of_two_size() {
    assert!(matches!(
        LockFreeRing::<u64>::with_capacity(3),
        Err(RingError::InvalidCapacity)
    ));
}

#[test]
fn lock_free_ring_cursors_are_split_by_cacheline() {
    assert_eq!(std::mem::align_of::<LockFreeRingHeadTail>(), CACHE_LINE);
    assert_eq!(std::mem::size_of::<LockFreeRingHeadTail>(), CACHE_LINE);
    assert_eq!(std::mem::align_of::<LockFreeRingCursors>(), CACHE_LINE);
    assert_eq!(LockFreeRingCursors::PRODUCER_CACHELINE_OFFSET, 0);
    assert_eq!(LockFreeRingCursors::CONSUMER_CACHELINE_OFFSET, CACHE_LINE);
}

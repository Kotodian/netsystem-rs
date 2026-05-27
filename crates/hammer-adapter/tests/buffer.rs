use hammer_adapter::{BufferFrame, BufferPool, RouteMetadata};

#[test]
fn buffer_pool_free_index_releases_slot_for_reuse_with_new_generation() {
    let pool = BufferPool::with_capacity(128, 1);
    let first_index = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"hello")
        .expect("alloc first buffer");
    assert_eq!(pool.in_use(), 1);
    assert_eq!(
        pool.copy_current(first_index).expect("first current"),
        b"hello"
    );
    pool.free_index(first_index);

    assert_eq!(pool.in_use(), 0);

    let second_index = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"world")
        .expect("alloc second buffer");

    assert_eq!(second_index.slot(), first_index.slot());
    assert_ne!(second_index.generation(), first_index.generation());
    assert!(pool.get(first_index).is_err());
    assert_eq!(
        pool.copy_current(second_index).expect("second current"),
        b"world"
    );
    pool.free_index(second_index);
}

#[test]
fn buffer_cursor_headroom_and_append_manage_current_bytes() {
    let pool = BufferPool::with_capacity(16, 2);
    let buffer = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"payload")
        .expect("alloc buffer");

    pool.advance(buffer, 3).expect("advance current");
    assert_eq!(
        pool.copy_current(buffer).expect("advanced current"),
        b"load"
    );

    pool.prepend(buffer, b"pre").expect("prepend into headroom");
    assert_eq!(
        pool.copy_current(buffer).expect("prepended current"),
        b"preload"
    );

    pool.append(buffer, b"-tail").expect("append current");
    assert_eq!(
        pool.copy_packet(buffer).expect("appended packet"),
        b"preload-tail"
    );

    pool.truncate_current(buffer, 7).expect("truncate current");
    assert_eq!(
        pool.copy_current(buffer).expect("truncated current"),
        b"preload"
    );
    pool.free_index(buffer);
}

#[test]
fn append_beyond_one_slot_creates_and_frees_chain() {
    let pool = BufferPool::with_capacity(8, 4);
    let buffer = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"123456")
        .expect("alloc buffer");

    pool.append(buffer, b"7890abcdef").expect("append chain");

    assert!(pool.is_chained(buffer).expect("buffer is chained"));
    assert_eq!(
        pool.copy_packet(buffer).expect("chained packet"),
        b"1234567890abcdef"
    );
    assert!(pool.in_use() > 1);

    pool.free_index(buffer);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn alloc_with_bytes_beyond_one_slot_creates_chain() {
    let pool = BufferPool::with_capacity(4, 4);
    let buffer = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"abcdefghijkl")
        .expect("alloc chained buffer");

    assert!(pool.is_chained(buffer).expect("buffer is chained"));
    assert_eq!(
        pool.copy_packet(buffer).expect("chained packet"),
        b"abcdefghijkl"
    );
    assert_eq!(pool.in_use(), 3);
    pool.free_index(buffer);
}

#[test]
fn buffer_pool_reports_single_segment_and_chained_packets() {
    let pool = BufferPool::with_capacity(4, 8);
    let single = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"abc")
        .expect("alloc single buffer");
    let chained = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"abcdefghij")
        .expect("alloc chained buffer");

    assert!(!pool.is_chained(single).expect("single chain state"));
    assert!(pool.is_chained(chained).expect("chained state"));
    assert_eq!(pool.copy_current(single).expect("single current"), b"abc");
    assert_eq!(pool.copy_packet(single).expect("single packet"), b"abc");
    assert_eq!(
        pool.copy_current(chained).expect("chained current"),
        b"abcd"
    );
    assert_eq!(
        pool.copy_packet(chained).expect("chained packet"),
        b"abcdefghij"
    );

    pool.free_index(single);
    pool.free_index(chained);
}

#[test]
fn buffer_pool_rejects_index_from_another_runtime() {
    let first_pool = BufferPool::with_capacity(8, 2);
    let second_pool = BufferPool::with_capacity(8, 2);
    let first_index = first_pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc first pool buffer");
    let second_index = second_pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"other")
        .expect("alloc second pool buffer");

    assert_eq!(first_index.slot(), second_index.slot());
    assert_eq!(first_index.generation(), second_index.generation());
    assert_ne!(first_index.pool_id(), second_index.pool_id());
    assert!(second_pool.get(first_index).is_err());
    assert!(second_pool.copy_packet(first_index).is_err());
    assert!(second_pool.is_chained(first_index).is_err());

    first_pool.free_index(first_index);
    second_pool.free_index(second_index);
}

#[test]
fn buffer_frame_reset_does_not_free_buffers() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut frame = BufferFrame::with_capacity(2);
    let first = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"one")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"two")
        .expect("alloc second frame buffer");
    frame.push_index(first).expect("push first frame index");
    frame.push_index(second).expect("push second frame index");

    assert_eq!(frame.len(), 2);
    assert_eq!(pool.in_use(), 2);

    frame.reset();

    assert!(frame.is_empty());
    assert_eq!(pool.in_use(), 2);
    assert_eq!(
        pool.copy_current_chain(first)
            .expect("first buffer remains live"),
        b"one"
    );
    assert_eq!(
        pool.copy_current_chain(second)
            .expect("second buffer remains live"),
        b"two"
    );

    pool.free_index(first);
    pool.free_index(second);
}

#[test]
fn buffer_pool_free_frame_releases_all_indices_and_reuses_frame() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut frame = BufferFrame::with_capacity(2);
    let first = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"one")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"two")
        .expect("alloc second frame buffer");
    frame.push_index(first).expect("push first frame index");
    frame.push_index(second).expect("push second frame index");
    let capacity = frame.capacity();

    pool.free_frame(&mut frame);

    assert!(frame.is_empty());
    assert_eq!(frame.capacity(), capacity);
    assert_eq!(pool.in_use(), 0);
    assert!(pool.get(first).is_err());
    assert!(pool.get(second).is_err());

    let next = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"next")
        .expect("alloc after free_frame");
    frame.push_index(next).expect("reuse frame allocation");
    assert_eq!(frame.indices(), &[next]);
    pool.free_frame(&mut frame);
}

#[test]
fn buffer_frame_drain_indices_preserves_order_without_freeing() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut frame = BufferFrame::with_capacity(2);
    let first = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second frame buffer");
    frame.push_index(first).expect("push first frame index");
    frame.push_index(second).expect("push second frame index");

    let drained = frame.drain_indices().collect::<Vec<_>>();

    assert!(frame.is_empty());
    assert_eq!(pool.in_use(), 2);
    assert_eq!(drained, vec![first, second]);
    assert_eq!(
        pool.copy_current_chain(drained[0])
            .expect("first buffer remains live"),
        b"first"
    );
    assert_eq!(
        pool.copy_current_chain(drained[1])
            .expect("second buffer remains live"),
        b"second"
    );

    for index in drained {
        pool.free_index(index);
    }
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn buffer_frame_push_index_respects_preallocated_capacity() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut frame = BufferFrame::with_capacity(1);
    let first = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second frame buffer");

    frame.push_index(first).expect("push first frame index");
    assert!(frame.push_index(second).is_err());
    assert_eq!(frame.indices(), &[first]);
    assert_eq!(pool.in_use(), 2);

    pool.free_frame(&mut frame);
    pool.free_index(second);
}

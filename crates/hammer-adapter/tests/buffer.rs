use hammer_adapter::{BufferFrame, BufferPool, RouteMetadata};

#[test]
fn buffer_handle_drop_releases_slot_for_reuse_with_new_generation() {
    let pool = BufferPool::with_capacity(128, 1);
    let first_index = {
        let buffer = pool
            .alloc_with_bytes(RouteMetadata::default(), b"hello")
            .expect("alloc first buffer");
        let index = buffer.index();
        assert_eq!(pool.in_use(), 1);
        assert_eq!(buffer.current(), b"hello");
        index
    };

    assert_eq!(pool.in_use(), 0);

    let second = pool
        .alloc_with_bytes(RouteMetadata::default(), b"world")
        .expect("alloc second buffer");
    let second_index = second.index();

    assert_eq!(second_index.slot(), first_index.slot());
    assert_ne!(second_index.generation(), first_index.generation());
    assert!(pool.get(first_index).is_err());
    assert_eq!(second.current(), b"world");
}

#[test]
fn buffer_cursor_headroom_and_append_manage_current_bytes() {
    let pool = BufferPool::with_capacity(16, 2);
    let mut buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"payload")
        .expect("alloc buffer");

    buffer.advance(3).expect("advance current");
    assert_eq!(buffer.current(), b"load");

    buffer.prepend(b"pre").expect("prepend into headroom");
    assert_eq!(buffer.current(), b"preload");

    buffer.append(b"-tail").expect("append current");
    assert_eq!(buffer.copy_current_chain(), b"preload-tail");

    buffer.truncate_current(7).expect("truncate current");
    assert_eq!(buffer.current(), b"preload");
}

#[test]
fn append_beyond_one_slot_creates_and_frees_chain() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"123456")
        .expect("alloc buffer");

    buffer.append(b"7890abcdef").expect("append chain");

    assert!(buffer.next_buffer().is_some());
    assert_eq!(buffer.copy_current_chain(), b"1234567890abcdef");
    assert!(pool.in_use() > 1);

    drop(buffer);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn alloc_with_bytes_beyond_one_slot_creates_chain() {
    let pool = BufferPool::with_capacity(4, 4);
    let buffer = pool
        .alloc_with_bytes(RouteMetadata::default(), b"abcdefghijkl")
        .expect("alloc chained buffer");

    assert!(buffer.next_buffer().is_some());
    assert_eq!(buffer.copy_current_chain(), b"abcdefghijkl");
    assert_eq!(pool.in_use(), 3);
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

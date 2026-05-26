use hammer_adapter::{BufferPool, RouteMetadata};

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
fn buffer_frame_drop_releases_all_buffers() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut frame = pool.frame_with_capacity(2);
    frame
        .push(
            pool.alloc_with_bytes(RouteMetadata::default(), b"one")
                .expect("alloc first frame buffer"),
        )
        .expect("push first frame buffer");
    frame
        .push(
            pool.alloc_with_bytes(RouteMetadata::default(), b"two")
                .expect("alloc second frame buffer"),
        )
        .expect("push second frame buffer");

    assert_eq!(frame.len(), 2);
    assert_eq!(pool.in_use(), 2);

    drop(frame);

    assert_eq!(pool.in_use(), 0);
}

#[test]
fn buffer_frame_drain_preserves_order_and_moves_ownership() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut frame = pool.frame_with_capacity(2);
    frame
        .push(
            pool.alloc_with_bytes(RouteMetadata::default(), b"first")
                .expect("alloc first frame buffer"),
        )
        .expect("push first frame buffer");
    frame
        .push(
            pool.alloc_with_bytes(RouteMetadata::default(), b"second")
                .expect("alloc second frame buffer"),
        )
        .expect("push second frame buffer");

    let drained = frame.drain().collect::<Vec<_>>();

    assert!(frame.is_empty());
    assert_eq!(pool.in_use(), 2);
    assert_eq!(drained[0].current(), b"first");
    assert_eq!(drained[1].current(), b"second");

    drop(drained);

    assert_eq!(pool.in_use(), 0);
}

#[test]
fn buffer_frame_retain_drops_removed_buffers() {
    let pool = BufferPool::with_capacity(8, 4);
    let mut frame = pool.frame_with_capacity(3);
    frame
        .push(
            pool.alloc_with_bytes(RouteMetadata::default(), b"keep")
                .expect("alloc kept frame buffer"),
        )
        .expect("push kept frame buffer");
    let drop_index = {
        let buffer = pool
            .alloc_with_bytes(RouteMetadata::default(), b"drop")
            .expect("alloc dropped frame buffer");
        let index = buffer.index();
        frame.push(buffer).expect("push dropped frame buffer");
        index
    };
    frame
        .push(
            pool.alloc_with_bytes(RouteMetadata::default(), b"also")
                .expect("alloc second kept frame buffer"),
        )
        .expect("push second kept frame buffer");

    frame.retain(|index| index != drop_index);

    let payloads = frame
        .iter_indices()
        .map(|index| frame.get(*index).expect("frame buffer").current().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(payloads, vec![b"keep".to_vec(), b"also".to_vec()]);
    assert_eq!(pool.in_use(), 2);
}

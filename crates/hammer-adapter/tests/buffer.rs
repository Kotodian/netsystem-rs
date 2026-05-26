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

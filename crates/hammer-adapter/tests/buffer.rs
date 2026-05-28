use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use hammer_adapter::{BufferPool, DataPlaneRuntime, RouteMetadata};

#[derive(Default)]
struct WakeCounter {
    wakes: AtomicUsize,
}

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

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
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
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

    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
    pool.free_index(first);
    pool.free_index(second);
}

#[test]
fn buffer_pool_free_frame_releases_all_indices_and_reuses_frame() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
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
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
}

#[test]
fn buffer_frame_drain_indices_preserves_order_without_freeing() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
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
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn buffer_frame_tracks_pending_indices_until_drained() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second frame buffer");

    assert!(!frame.has_pending());
    assert_eq!(frame.pending_len(), 0);
    frame.push_index(first).expect("push first frame index");
    frame.push_index(second).expect("push second frame index");

    assert!(frame.has_pending());
    assert_eq!(frame.pending_len(), 2);
    assert_eq!(frame.pending_indices(), &[first, second]);

    let drained = frame.drain_pending().collect::<Vec<_>>();

    assert_eq!(drained, vec![first, second]);
    assert!(!frame.has_pending());
    assert_eq!(frame.pending_len(), 0);
    assert_eq!(pool.in_use(), 2);

    for index in drained {
        pool.free_index(index);
    }
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn buffer_frame_pending_future_wakes_when_index_is_pushed() {
    let runtime = DataPlaneRuntime::with_capacities(8, 2, 1, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let index = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc frame buffer");
    let wake_counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&wake_counter));
    let mut context = Context::from_waker(&waker);
    let mut pending = frame.pending();

    assert!(matches!(
        Pin::new(&mut pending).poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 0);

    frame.push_index(index).expect("push frame index");

    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 1);
    assert!(matches!(
        Pin::new(&mut pending).poll(&mut context),
        Poll::Ready(())
    ));

    pool.free_frame(&mut frame);
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
}

#[test]
fn buffer_frame_push_indices_batches_one_wake() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second frame buffer");
    let wake_counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&wake_counter));
    let mut context = Context::from_waker(&waker);
    let mut pending = frame.pending();

    assert!(matches!(
        Pin::new(&mut pending).poll(&mut context),
        Poll::Pending
    ));

    frame
        .push_indices([first, second])
        .expect("push batched frame indices");

    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 1);
    assert_eq!(frame.pending_indices(), &[first, second]);

    pool.free_frame(&mut frame);
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
}

#[test]
fn buffer_frame_pending_future_observes_reset_before_processing() {
    let runtime = DataPlaneRuntime::with_capacities(8, 2, 1, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"second")
        .expect("alloc second frame buffer");
    let wake_counter = Arc::new(WakeCounter::default());
    let waker = Waker::from(Arc::clone(&wake_counter));
    let mut context = Context::from_waker(&waker);
    let mut pending = frame.pending();

    assert!(matches!(
        Pin::new(&mut pending).poll(&mut context),
        Poll::Pending
    ));
    frame.push_index(first).expect("push first frame index");
    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 1);

    frame.reset();
    assert!(matches!(
        Pin::new(&mut pending).poll(&mut context),
        Poll::Pending
    ));

    pool.free_index(first);
    frame.push_index(second).expect("push second frame index");
    assert_eq!(wake_counter.wakes.load(Ordering::SeqCst), 2);
    assert!(matches!(
        Pin::new(&mut pending).poll(&mut context),
        Poll::Ready(())
    ));

    pool.free_frame(&mut frame);
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
}

#[test]
fn buffer_frame_push_index_respects_preallocated_capacity() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 1, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
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
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
    pool.free_index(second);
}

#[test]
fn data_plane_runtime_allocates_frame_indices_from_reusable_pool() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let frame_index = runtime.alloc_frame_index().expect("alloc pooled frame");
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"one")
        .expect("alloc first buffer");
    let second = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"two")
        .expect("alloc second buffer");

    runtime
        .with_frame_mut(frame_index, |frame| frame.push_indices([first, second]))
        .expect("mutate frame")
        .expect("push frame indices");

    assert_eq!(runtime.frames_in_use(), 1);
    assert_eq!(runtime.in_use_buffers(), 2);
    assert!(runtime.alloc_frame_index().is_err());
    assert_eq!(
        runtime
            .with_frame(frame_index, |frame| frame.indices().to_vec())
            .expect("read frame"),
        vec![first, second]
    );

    runtime
        .free_frame_index(frame_index)
        .expect("free pooled frame");

    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert!(runtime.copy_packet(first).is_err());
    assert!(runtime.copy_packet(second).is_err());

    let reused_frame_index = runtime.alloc_frame_index().expect("reuse pooled frame");
    assert_eq!(reused_frame_index.slot(), frame_index.slot());
    assert_ne!(reused_frame_index.generation(), frame_index.generation());
    assert!(runtime.with_frame(frame_index, |_| ()).is_err());
    assert!(
        runtime
            .with_frame(reused_frame_index, |frame| frame.is_empty())
            .expect("read reused frame")
    );

    runtime
        .free_frame_index(reused_frame_index)
        .expect("free reused frame");
}

#[test]
fn data_plane_runtime_checks_out_pooled_frame_for_packet_interfaces() {
    let runtime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let frame_index = frame.index();
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"pkt")
        .expect("alloc packet buffer");

    frame.push_index(buffer).expect("push packet buffer");

    assert_eq!(runtime.frames_in_use(), 1);
    assert!(runtime.alloc_pooled_frame().is_err());
    assert_eq!(frame.indices(), &[buffer]);

    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");

    assert_eq!(runtime.frames_in_use(), 0);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert!(runtime.copy_packet(buffer).is_err());

    let reused_frame = runtime.alloc_pooled_frame().expect("reuse pooled frame");
    assert_eq!(reused_frame.index().slot(), frame_index.slot());
    assert_ne!(reused_frame.index().generation(), frame_index.generation());
    assert!(reused_frame.is_empty());
    runtime
        .release_pooled_frame(reused_frame)
        .expect("release reused pooled frame");
}

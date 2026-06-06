use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use hammer_adapter::{
    BufferFrame, BufferFramePairBatch, BufferFrameQuadBatch, BufferIndex, BufferPacketCursor,
    BufferPool, BufferPoolArena, DataPlaneHandoff, DataPlaneInstructionSet, DataPlaneRuntime,
    DataWorkerId, FrameBatchWidth, RouteMetadata,
};

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
fn buffer_batch_mut_processes_multiple_buffers_under_one_borrow() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_buffer_capacity(32, 2);
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"alpha")
        .expect("alloc first buffer");
    let second = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"bravo")
        .expect("alloc second buffer");

    {
        let mut batch = runtime.buffer_batch_mut();
        batch
            .with_buffer_mut(first, |buffer| {
                buffer.current_mut()[0] = b'A';
            })
            .expect("mutate first buffer");
        batch
            .with_buffer_mut(second, |buffer| {
                buffer.current_mut()[0] = b'B';
            })
            .expect("mutate second buffer");
    }

    assert_eq!(
        runtime.copy_current(first).expect("first current"),
        b"Alpha"
    );
    assert_eq!(
        runtime.copy_current(second).expect("second current"),
        b"Bravo"
    );
    runtime.free_index(first);
    runtime.free_index(second);
}

#[test]
fn buffer_batch_mut_exposes_direct_buffer_refs() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_buffer_capacity(32, 1);
    let index = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"alpha")
        .expect("alloc buffer");

    {
        let mut batch = runtime.buffer_batch_mut();
        assert_eq!(batch.buffer(index).expect("buffer").current(), b"alpha");
        batch.buffer_mut(index).expect("buffer mut").current_mut()[0] = b'A';
    }

    assert_eq!(runtime.copy_current(index).expect("current"), b"Alpha");
    runtime.free_index(index);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn buffer_header_and_packet_data_start_cacheline_aligned() {
    let pool = BufferPool::with_capacity(128, 1);
    let buffer = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc buffer");

    {
        let buffer = pool.get(buffer).expect("buffer ref");
        assert_eq!(buffer.buffer_ptr() as usize % 64, 0);
        assert_eq!(buffer.current().as_ptr() as usize % 64, 0);
    }

    pool.free_index(buffer);
}

#[test]
fn buffer_exposes_vpp_style_current_pointer_and_advance() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(128, 1, 1, 1);
    let buffer = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"network-transport")
        .expect("alloc buffer");
    {
        let buffer_ref = runtime.get_buffer(buffer).expect("buffer ref");
        assert_eq!(buffer_ref.current_data(), 0);
        assert_eq!(buffer_ref.current_len(), b"network-transport".len());
        assert_eq!(unsafe { *buffer_ref.current_ptr() }, b'n');
    }

    runtime.advance(buffer, b"network-".len()).expect("advance");

    {
        let mut buffer_ref = runtime.get_buffer_mut(buffer).expect("buffer ref mut");
        assert_eq!(buffer_ref.current_data(), b"network-".len());
        assert_eq!(buffer_ref.current_len(), b"transport".len());
        assert_eq!(unsafe { *buffer_ref.current_ptr() }, b't');
        unsafe {
            *buffer_ref.current_mut_ptr() = b'T';
        }
    }

    assert_eq!(runtime.copy_current(buffer).expect("current"), b"Transport");
    runtime.free_index(buffer);
}

#[test]
fn buffer_packet_cursor_lives_in_buffer_control_area() {
    let pool = BufferPool::with_capacity(128, 1);
    let buffer = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"packet")
        .expect("alloc buffer");
    let cursor = BufferPacketCursor::new()
        .with_packet_len(64)
        .with_network_header(0, 20)
        .with_transport_header(20, 8)
        .with_transport_payload_offset(28);

    pool.get_mut(buffer)
        .expect("buffer mut")
        .set_packet_cursor(cursor);

    assert_eq!(pool.packet_cursor(buffer).expect("read cursor"), cursor);
    assert_eq!(
        pool.metadata(buffer).expect("metadata"),
        RouteMetadata::default()
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
fn buffer_chain_can_detach_and_append_existing_chain_without_copying_payload() {
    let pool = BufferPool::with_capacity(8, 8);
    let head = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"head")
        .expect("alloc head");
    let tail = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"tail-data")
        .expect("alloc tail chain");
    let tail_ptr = pool.current_ptr(tail).expect("tail ptr") as usize;

    pool.append_existing_chain(head, tail)
        .expect("append existing chain");

    assert!(pool.is_chained(head).expect("head is chained"));
    assert_eq!(
        pool.copy_current_chain(head).expect("combined chain"),
        b"headtail-data"
    );
    assert_eq!(
        pool.current_ptr(tail).expect("tail ptr after append") as usize,
        tail_ptr
    );

    let detached = pool.detach_next(head).expect("detach next");

    assert_eq!(detached, Some(tail));
    assert!(!pool.is_chained(head).expect("head detached"));
    assert_eq!(pool.copy_current_chain(head).expect("head packet"), b"head");
    assert_eq!(
        pool.copy_current_chain(tail).expect("tail packet"),
        b"tail-data"
    );

    pool.free_index(head);
    pool.free_index(tail);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn buffer_chain_truncate_current_chain_frees_tail_beyond_limit() {
    let pool = BufferPool::with_capacity(4, 8);
    let packet = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"abcdefghijkl")
        .expect("alloc chained packet");

    assert_eq!(pool.in_use(), 3);

    pool.truncate_chain(packet, 6).expect("truncate chain");

    assert!(pool.is_chained(packet).expect("packet still chained"));
    assert_eq!(
        pool.copy_current_chain(packet).expect("truncated packet"),
        b"abcdef"
    );
    assert_eq!(pool.in_use(), 2);

    pool.free_index(packet);
    assert_eq!(pool.in_use(), 0);
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
fn buffer_pool_prefetch_read_is_best_effort_for_live_and_stale_indices() {
    let pool = BufferPool::with_capacity(4, 4);
    let buffer = pool
        .alloc_index_with_bytes(RouteMetadata::default(), b"abcdefgh")
        .expect("alloc chained buffer");

    pool.prefetch_read(buffer);
    pool.free_index(buffer);
    pool.prefetch_read(buffer);

    assert_eq!(pool.in_use(), 0);
}

#[test]
fn instruction_set_selects_preferred_frame_batch_width() {
    assert_eq!(
        DataPlaneInstructionSet::Scalar.preferred_frame_batch_width(),
        FrameBatchWidth::Pair
    );
    assert_eq!(
        DataPlaneInstructionSet::Sse2.preferred_frame_batch_width(),
        FrameBatchWidth::Pair
    );
    assert_eq!(
        DataPlaneInstructionSet::Avx2.preferred_frame_batch_width(),
        FrameBatchWidth::Quad
    );
    assert_eq!(
        DataPlaneInstructionSet::Neon.preferred_frame_batch_width(),
        FrameBatchWidth::Quad
    );
}

#[test]
fn data_plane_runtime_can_use_explicit_instruction_set() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities_and_instruction_set(
        8,
        4,
        2,
        1,
        DataPlaneInstructionSet::Avx2,
    );

    assert_eq!(runtime.instruction_set(), DataPlaneInstructionSet::Avx2);
    assert_eq!(runtime.preferred_frame_batch_width(), FrameBatchWidth::Quad);
}

#[test]
fn data_plane_runtime_defaults_to_native_instruction_set() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);

    assert_eq!(runtime.instruction_set(), DataPlaneInstructionSet::native());
    assert_eq!(
        runtime.preferred_frame_batch_width(),
        runtime.instruction_set().preferred_frame_batch_width()
    );
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
fn handoff_workers_share_buffer_arena_and_keep_per_worker_free_cache() {
    let arena = BufferPoolArena::with_capacity(8, 4);
    let handoff = DataPlaneHandoff::with_buffer_arena(2, 4, arena);
    let first: DataPlaneRuntime = DataPlaneRuntime::with_handoff_capacities(
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
        2,
        2,
        DataPlaneInstructionSet::Scalar,
    );
    let second: DataPlaneRuntime = DataPlaneRuntime::with_handoff_capacities(
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
        2,
        2,
        DataPlaneInstructionSet::Scalar,
    );

    let first_buffer = first
        .alloc_index_with_bytes(RouteMetadata::default(), b"one")
        .expect("alloc first worker buffer");
    let second_buffer = second
        .alloc_index_with_bytes(RouteMetadata::default(), b"two")
        .expect("alloc second worker buffer");

    assert_eq!(first_buffer.pool_id(), second_buffer.pool_id());
    assert_eq!(first.in_use_buffers(), 2);
    assert_eq!(second.in_use_buffers(), 2);

    first.free_index(first_buffer);
    second.free_index(second_buffer);

    assert_eq!(first.cached_free_buffers(), 1);
    assert_eq!(second.cached_free_buffers(), 1);
    assert_eq!(first.in_use_buffers(), 0);

    let first_reused = first
        .alloc_index_with_bytes(RouteMetadata::default(), b"uno")
        .expect("first worker reuses its cache");
    let second_reused = second
        .alloc_index_with_bytes(RouteMetadata::default(), b"dos")
        .expect("second worker reuses its cache");

    assert_eq!(first_reused.slot(), first_buffer.slot());
    assert_eq!(second_reused.slot(), second_buffer.slot());
    assert_ne!(first_reused.generation(), first_buffer.generation());
    assert_ne!(second_reused.generation(), second_buffer.generation());

    first.free_index(first_reused);
    second.free_index(second_reused);
}

#[test]
fn legacy_handoff_constructor_uses_first_runtime_buffer_arena() {
    let handoff = DataPlaneHandoff::new(2, 4);
    let first: DataPlaneRuntime = DataPlaneRuntime::with_handoff(
        DataPlaneRuntime::with_capacities(8, 4, 2, 2),
        DataWorkerId::new(0),
        handoff.worker(DataWorkerId::new(0)),
    );
    let second: DataPlaneRuntime = DataPlaneRuntime::with_handoff(
        DataPlaneRuntime::with_capacities(8, 4, 2, 2),
        DataWorkerId::new(1),
        handoff.worker(DataWorkerId::new(1)),
    );

    let first_buffer = first
        .alloc_index_with_bytes(RouteMetadata::default(), b"one")
        .expect("alloc first worker buffer");
    let second_buffer = second
        .alloc_index_with_bytes(RouteMetadata::default(), b"two")
        .expect("alloc second worker buffer");

    assert_eq!(first_buffer.pool_id(), second_buffer.pool_id());
    assert_eq!(first.in_use_buffers(), 2);
    assert_eq!(second.in_use_buffers(), 2);

    first.free_index(first_buffer);
    second.free_index(second_buffer);
    assert_eq!(first.in_use_buffers(), 0);
}

#[test]
fn buffer_frame_reset_does_not_free_buffers() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
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
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
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
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
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
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
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
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 2, 1, 1);
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
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
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
fn buffer_frame_pair_batch_cursor_splits_into_pairs_then_tail() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 8, 8, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let indices = (0..5)
        .map(|value| {
            pool.alloc_index_with_bytes(RouteMetadata::default(), &[value])
                .expect("alloc packet buffer")
        })
        .collect::<Vec<_>>();
    frame
        .push_indices(indices.iter().copied())
        .expect("push frame indices");

    let batches = frame.pair_batch_cursor().collect::<Vec<_>>();

    assert_eq!(
        batches,
        vec![
            BufferFramePairBatch::Pair([indices[0], indices[1]]),
            BufferFramePairBatch::Pair([indices[2], indices[3]]),
            BufferFramePairBatch::Single(indices[4]),
        ]
    );

    pool.free_frame(&mut frame);
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
}

#[test]
fn buffer_frame_quad_batch_cursor_splits_into_quad_pair_then_tail() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 8, 8, 1);
    let pool = runtime.buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let indices = (0..7)
        .map(|value| {
            pool.alloc_index_with_bytes(RouteMetadata::default(), &[value])
                .expect("alloc packet buffer")
        })
        .collect::<Vec<_>>();
    frame
        .push_indices(indices.iter().copied())
        .expect("push frame indices");

    let batches = frame.quad_batch_cursor().collect::<Vec<_>>();

    assert_eq!(
        batches,
        vec![
            BufferFrameQuadBatch::Quad([indices[0], indices[1], indices[2], indices[3]]),
            BufferFrameQuadBatch::Pair([indices[4], indices[5]]),
            BufferFrameQuadBatch::Single(indices[6]),
        ]
    );

    pool.free_frame(&mut frame);
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
}

#[test]
fn buffer_frame_batch_cursors_are_empty_for_empty_frame() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 2, 8, 1);
    let frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");

    assert_eq!(frame.pair_batch_cursor().next(), None);
    assert_eq!(frame.quad_batch_cursor().next(), None);

    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
}

#[test]
fn buffer_frame_batch_cursor_uses_requested_width() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 8, 8, 1);
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let indices = push_numbered_indices(&runtime, &mut frame, 5);

    let quad_batches = frame
        .batch_cursor(FrameBatchWidth::Quad)
        .map(|batch| batch.indices().collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let pair_batches = frame
        .batch_cursor(FrameBatchWidth::Pair)
        .map(|batch| batch.indices().collect::<Vec<_>>())
        .collect::<Vec<_>>();

    assert_eq!(quad_batches, vec![indices[0..4].to_vec(), vec![indices[4]]]);
    assert_eq!(
        pair_batches,
        vec![
            indices[0..2].to_vec(),
            indices[2..4].to_vec(),
            vec![indices[4]]
        ]
    );

    runtime.free_frame(&mut frame);
    runtime
        .release_pooled_frame(frame)
        .expect("release pooled frame");
}

#[test]
fn buffer_frame_batch_dispatch_uses_runtime_preferred_width() {
    let quad_runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities_and_instruction_set(
        8,
        8,
        8,
        1,
        DataPlaneInstructionSet::Avx2,
    );
    let pair_runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities_and_instruction_set(
        8,
        8,
        8,
        1,
        DataPlaneInstructionSet::Scalar,
    );
    let mut quad_frame = quad_runtime.alloc_pooled_frame().expect("quad frame");
    let mut pair_frame = pair_runtime.alloc_pooled_frame().expect("pair frame");
    let quad_indices = push_numbered_indices(&quad_runtime, &mut quad_frame, 7);
    let pair_indices = push_numbered_indices(&pair_runtime, &mut pair_frame, 5);

    let mut quad_seen = Vec::new();
    quad_frame
        .retain_indices_batched(quad_runtime.preferred_frame_batch_width(), |index| {
            quad_seen.push(index);
            Ok(true)
        })
        .expect("quad dispatch");
    let mut pair_seen = Vec::new();
    pair_frame
        .retain_indices_batched(pair_runtime.preferred_frame_batch_width(), |index| {
            pair_seen.push(index);
            Ok(true)
        })
        .expect("pair dispatch");

    assert_eq!(
        quad_runtime.preferred_frame_batch_width(),
        FrameBatchWidth::Quad
    );
    assert_eq!(
        pair_runtime.preferred_frame_batch_width(),
        FrameBatchWidth::Pair
    );
    assert_eq!(quad_seen, quad_indices);
    assert_eq!(pair_seen, pair_indices);

    quad_runtime.free_frame(&mut quad_frame);
    pair_runtime.free_frame(&mut pair_frame);
    quad_runtime
        .release_pooled_frame(quad_frame)
        .expect("release quad frame");
    pair_runtime
        .release_pooled_frame(pair_frame)
        .expect("release pair frame");
}

#[test]
fn buffer_frame_pending_future_observes_reset_before_processing() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 2, 1, 1);
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
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 1, 1);
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
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
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
fn frame_ref_mut_push_indices_batches_into_pooled_frame() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let frame_index = runtime.alloc_frame_index().expect("alloc pooled frame");
    let first = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"one")
        .expect("alloc first buffer");
    let second = runtime
        .alloc_index_with_bytes(RouteMetadata::default(), b"two")
        .expect("alloc second buffer");

    {
        let mut frame = runtime.get_frame_mut(frame_index).expect("frame ref mut");
        frame
            .push_indices([first, second])
            .expect("push frame indices");
    }

    assert_eq!(
        runtime
            .with_frame(frame_index, |frame| frame.indices().to_vec())
            .expect("read frame"),
        vec![first, second]
    );
    runtime
        .free_frame_index(frame_index)
        .expect("free pooled frame");
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn data_plane_runtime_checks_out_pooled_frame_for_packet_interfaces() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
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

#[test]
fn buffer_frame_lazy_state_retain_compacts_after_first_drop() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 8, 8, 1);
    let mut frame = runtime.alloc_pooled_frame().expect("alloc frame");
    let indices = push_numbered_indices(&runtime, &mut frame, 5);
    let mut prefetched = 0usize;

    frame
        .retain_indices_batched_with_prefetch_state_lazy(
            FrameBatchWidth::Quad,
            &mut prefetched,
            |prefetched, _index| {
                *prefetched += 1;
            },
            |_, index| Ok(index != indices[1] && index != indices[3]),
        )
        .expect("retain frame");

    assert_eq!(frame.indices(), &[indices[0], indices[2], indices[4]]);
    assert!(prefetched > 0);

    runtime.free_index(indices[1]);
    runtime.free_index(indices[3]);
    runtime
        .release_pooled_frame(frame)
        .expect("release retained frame");
    assert_eq!(runtime.in_use_buffers(), 0);
}

fn push_numbered_indices(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    count: u8,
) -> Vec<BufferIndex> {
    let indices = (0..count)
        .map(|value| {
            runtime
                .alloc_index_with_bytes(RouteMetadata::default(), &[value])
                .expect("alloc numbered buffer")
        })
        .collect::<Vec<_>>();
    frame
        .push_indices(indices.iter().copied())
        .expect("push numbered indices");
    indices
}

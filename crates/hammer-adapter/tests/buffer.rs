use std::future::Future;
use std::mem::{align_of, size_of};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use hammer_adapter::{
    BufferFrame, BufferFramePairBatch, BufferFrameQuadBatch, BufferIndex, BufferPacketCursor,
    BufferPool, BufferPoolArena, BufferRefMut, DataPlaneBuffers, DataPlaneHandoff,
    DataPlaneInstructionSet, DataPlaneRuntime, DataWorkerId, FrameBatchWidth,
};
use hammer_core::error::CoreResult;
use hammer_infra::vec::Vec;

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

fn chain_bytes(pool: &BufferPool, index: BufferIndex) -> CoreResult<Vec<u8>> {
    let mut out = Vec::new();
    for buffer in pool.chain(index) {
        out.extend_from_slice(buffer?.current());
    }
    Ok(out)
}

fn chain_len(pool: &BufferPool, index: BufferIndex) -> CoreResult<usize> {
    let mut len = 0usize;
    for buffer in pool.chain(index) {
        let _ = buffer?;
        len += 1;
    }
    Ok(len)
}

#[test]
fn buffer_header_keeps_hot_metadata_in_first_cacheline() {
    assert_eq!(align_of::<hammer_adapter::Buffer>(), 64);
    assert!(size_of::<BufferPacketCursor>() <= 32);
}

#[test]
fn buffer_pool_free_index_releases_slot_for_reuse_with_new_generation() {
    let pool = BufferPool::with_capacity(128, 1);
    let first_index = pool
        .alloc_index_with_bytes(b"hello")
        .expect("alloc first buffer");
    assert_eq!(pool.in_use(), 1);
    assert_eq!(
        pool.get(first_index).expect("first current").current(),
        b"hello"
    );
    pool.free_index(first_index);

    assert_eq!(pool.in_use(), 0);

    let second_index = pool
        .alloc_index_with_bytes(b"world")
        .expect("alloc second buffer");

    assert_eq!(second_index.slot(), first_index.slot());
    assert_ne!(second_index.generation(), first_index.generation());
    assert!(pool.get(first_index).is_err());
    assert_eq!(
        pool.get(second_index).expect("second current").current(),
        b"world"
    );
    pool.free_index(second_index);
}

#[test]
fn buffer_cursor_headroom_and_append_manage_current_bytes() {
    let pool = BufferPool::with_capacity(16, 2);
    let buffer = pool
        .alloc_index_with_bytes(b"payload")
        .expect("alloc buffer");

    pool.advance(buffer, 3).expect("advance current");
    assert_eq!(
        pool.get(buffer).expect("advanced current").current(),
        b"load"
    );

    pool.prepend(buffer, b"pre").expect("prepend into headroom");
    assert_eq!(
        pool.get(buffer).expect("prepended current").current(),
        b"preload"
    );

    pool.append(buffer, b"-tail").expect("append current");
    assert_eq!(
        chain_bytes(&pool, buffer).expect("appended buffer"),
        b"preload-tail"
    );

    pool.truncate_current(buffer, 7).expect("truncate current");
    assert_eq!(
        pool.get(buffer).expect("truncated current").current(),
        b"preload"
    );
    pool.free_index(buffer);
}

#[test]
fn empty_buffer_allocation_reserves_default_packet_headroom() {
    let pool = BufferPool::with_capacity(512, 1);
    let buffer = pool.alloc_index().expect("alloc empty buffer");

    pool.append(buffer, b"payload").expect("append payload");
    pool.prepend(buffer, b"header").expect("prepend header");

    assert_eq!(
        pool.get(buffer).expect("prepended packet").current(),
        b"headerpayload"
    );
    pool.free_index(buffer);
}

#[test]
fn runtime_get_buffer_mut_processes_multiple_buffers_sequentially() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_buffer_capacity(32, 2);
    let first = runtime
        .alloc_index_with_bytes(b"alpha")
        .expect("alloc first buffer");
    let second = runtime
        .alloc_index_with_bytes(b"bravo")
        .expect("alloc second buffer");

    {
        runtime
            .get_buffer_mut(first)
            .expect("first buffer mut")
            .current_mut()[0] = b'A';
        runtime
            .get_buffer_mut(second)
            .expect("second buffer mut")
            .current_mut()[0] = b'B';
    }

    assert_eq!(
        runtime.get_buffer(first).expect("first current").current(),
        b"Alpha"
    );
    assert_eq!(
        runtime
            .get_buffer(second)
            .expect("second current")
            .current(),
        b"Bravo"
    );
    runtime.free_index(first);
    runtime.free_index(second);
}

#[test]
fn runtime_get_buffer_exposes_direct_buffer_borrows() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_buffer_capacity(32, 1);
    let index = runtime
        .alloc_index_with_bytes(b"alpha")
        .expect("alloc buffer");

    {
        assert_eq!(
            runtime.get_buffer(index).expect("buffer").current(),
            b"alpha"
        );
        runtime
            .get_buffer_mut(index)
            .expect("buffer mut")
            .current_mut()[0] = b'A';
    }

    assert_eq!(
        runtime.get_buffer(index).expect("current").current(),
        b"Alpha"
    );
    runtime.free_index(index);
    assert_eq!(runtime.in_use_buffers(), 0);
}

#[test]
fn public_mut_buffer_accessors_keep_buffer_refmut_shape() {
    fn assert_pool_shape<'a>(
        pool: &'a BufferPool,
        index: BufferIndex,
    ) -> CoreResult<BufferRefMut<'a>> {
        pool.get_mut(index)
    }

    fn assert_runtime_shape<'a>(
        runtime: &'a DataPlaneRuntime,
        index: BufferIndex,
    ) -> CoreResult<BufferRefMut<'a>> {
        runtime.get_buffer_mut(index)
    }

    let pool = BufferPool::with_capacity(32, 1);
    let index = pool.alloc_index().expect("alloc buffer");
    let runtime = DataPlaneRuntime::with_buffer_capacity(32, 1);
    let runtime_index = runtime.alloc_index().expect("alloc runtime buffer");

    let _ = assert_pool_shape(&pool, index).expect("pool mut borrow");
    let _ = assert_runtime_shape(&runtime, runtime_index).expect("runtime mut borrow");
}

#[test]
fn public_refmut_tail_api_respects_inline_slot_capacity() {
    let pool = BufferPool::with_capacity(16, 1);
    let index = pool
        .alloc_index_with_bytes(b"abcd")
        .expect("alloc buffer with bytes");

    {
        let mut buffer = pool.get_mut(index).expect("mutable buffer");
        let tail = buffer.writable_tail_mut();
        assert_eq!(tail.len(), 12);
        tail[..3].copy_from_slice(b"xyz");
        buffer
            .commit_writable_tail(3)
            .expect("commit within remaining slot capacity");
        assert_eq!(buffer.current(), b"abcdxyz");
        assert!(buffer.commit_writable_tail(10).is_err());
    }
}

#[test]
fn append_after_truncating_pre_data_current_window_keeps_bytes_coherent() {
    let pool = BufferPool::with_capacity(16, 2);
    let index = pool.alloc_index().expect("alloc empty buffer");

    pool.prepend(index, &[0xAA; 32])
        .expect("prepend into pre-data headroom");
    pool.truncate_current(index, 16)
        .expect("truncate current within pre-data");
    pool.append(index, &[0xBB])
        .expect("append from pre-data tail");

    let buffer = pool.get(index).expect("buffer");
    let mut expected = [0xAA; 17];
    expected[16] = 0xBB;
    assert_eq!(buffer.current_data_offset(), -32);
    assert_eq!(buffer.current_len(), 17);
    assert_eq!(buffer.current(), &expected);
    assert_eq!(
        buffer.current_ptr() as usize + buffer.current_len(),
        pool.data_raw_ptr(index.slot()) as usize - 15
    );
}

#[test]
fn buffer_header_and_packet_data_start_cacheline_aligned() {
    let pool = BufferPool::with_capacity(128, 1);
    let buffer = pool
        .alloc_index_with_bytes(b"packet")
        .expect("alloc buffer");

    {
        let buffer = pool.get(buffer).expect("buffer");
        assert_eq!(std::ptr::from_ref(&*buffer) as usize % 64, 0);
        assert_eq!(buffer.current().as_ptr() as usize % 64, 0);
    }

    pool.free_index(buffer);
}

#[test]
fn buffer_exposes_vpp_style_current_pointer_and_advance() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(128, 1, 1, 1);
    let buffer = runtime
        .alloc_index_with_bytes(b"network-transport")
        .expect("alloc buffer");
    {
        let buffer = runtime.get_buffer(buffer).expect("buffer");
        assert_eq!(buffer.current_data(), 0);
        assert_eq!(buffer.current_len(), b"network-transport".len());
        assert_eq!(unsafe { *buffer.current_ptr() }, b'n');
    }

    runtime
        .buffers()
        .advance(buffer, b"network-".len() as isize)
        .expect("advance");

    {
        let mut buffer = runtime.get_buffer_mut(buffer).expect("buffer mut");
        assert_eq!(buffer.current_data(), b"network-".len());
        assert_eq!(buffer.current_len(), b"transport".len());
        assert_eq!(unsafe { *buffer.current_ptr() }, b't');
        unsafe {
            *buffer.current_mut_ptr() = b'T';
        }
    }

    assert_eq!(
        runtime.get_buffer(buffer).expect("current").current(),
        b"Transport"
    );
    runtime.free_index(buffer);
}

#[test]
fn buffer_packet_cursor_records_header_offsets() {
    let cursor = BufferPacketCursor::new()
        .with_packet_len(64)
        .with_network_header(0, 20)
        .with_transport_header(20, 8)
        .with_transport_payload_offset(28);

    assert_eq!(cursor.packet_len(), 64);
    assert_eq!(cursor.network_header_offset(), 0);
    assert_eq!(cursor.network_header_len(), 20);
    assert_eq!(cursor.transport_header_offset(), 20);
    assert_eq!(cursor.transport_header_len(), 8);
    assert_eq!(cursor.transport_payload_offset(), 28);
}

#[test]
fn append_beyond_one_slot_creates_and_frees_chain() {
    let pool = BufferPool::with_capacity(8, 4);
    let buffer = pool
        .alloc_index_with_bytes(b"123456")
        .expect("alloc buffer");

    pool.append(buffer, b"7890abcdef").expect("append chain");

    assert!(chain_len(&pool, buffer).expect("chain len") > 1);
    assert_eq!(
        chain_bytes(&pool, buffer).expect("chained buffer"),
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
        .alloc_index_with_bytes(b"abcdefghijkl")
        .expect("alloc chained buffer");

    assert!(chain_len(&pool, buffer).expect("chain len") > 1);
    assert_eq!(
        chain_bytes(&pool, buffer).expect("chained buffer"),
        b"abcdefghijkl"
    );
    assert_eq!(pool.in_use(), 3);
    pool.free_index(buffer);
}

#[test]
fn buffer_chain_buffer_links_existing_chain_without_copying_payload() {
    let pool = BufferPool::with_capacity(8, 8);
    let head = pool.alloc_index_with_bytes(b"head").expect("alloc head");
    let tail = pool
        .alloc_index_with_bytes(b"taildata")
        .expect("alloc tail chain");
    let tail_ptr = pool.current_ptr(tail).expect("tail ptr") as usize;

    pool.chain_buffer(head, tail)
        .expect("append existing chain");

    assert!(chain_len(&pool, head).expect("chain len") > 1);
    assert_eq!(
        chain_bytes(&pool, head).expect("combined chain"),
        b"headtaildata"
    );
    assert_eq!(
        pool.current_ptr(tail).expect("tail ptr after append") as usize,
        tail_ptr
    );

    pool.free_index(head);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn attach_clone_keeps_tail_alive_until_both_chains_are_freed() {
    let pool = BufferPool::with_capacity(8, 8);
    let session_tail = pool
        .alloc_index_with_bytes(b"taildata")
        .expect("alloc session tail");
    let output_head = pool
        .alloc_index_with_bytes(b"head")
        .expect("alloc output head");
    let tail_ptr = pool.current_ptr(session_tail).expect("tail ptr");

    pool.attach_clone(output_head, session_tail)
        .expect("attach clone");

    assert_eq!(
        chain_bytes(&pool, output_head).expect("output chain"),
        b"headtaildata"
    );
    assert_eq!(
        pool.current_ptr(session_tail)
            .expect("tail ptr after attach"),
        tail_ptr
    );
    assert_eq!(pool.in_use(), 2);

    pool.free_index(output_head);

    assert_eq!(
        chain_bytes(&pool, session_tail).expect("tail survives output free"),
        b"taildata"
    );
    assert_eq!(
        pool.current_ptr(session_tail).expect("tail ptr after free"),
        tail_ptr
    );
    assert_eq!(pool.in_use(), 1);

    pool.free_index(session_tail);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn freeing_head_with_attached_clone_does_not_free_session_tail() {
    let pool = BufferPool::with_capacity(8, 8);
    let session_tail = pool
        .alloc_index_with_bytes(b"payload")
        .expect("alloc session tail");
    let output_head = pool.alloc_index().expect("alloc output head");

    pool.prepend(output_head, b"hdr").expect("write head bytes");
    pool.attach_clone(output_head, session_tail)
        .expect("attach clone");

    pool.free_index(output_head);

    assert_eq!(
        chain_bytes(&pool, session_tail).expect("tail survives"),
        b"payload"
    );
    assert_eq!(pool.in_use(), 1);

    pool.free_index(session_tail);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn freeing_original_tail_after_output_head_returns_storage_once() {
    let pool = BufferPool::with_capacity(8, 8);
    let session_tail = pool
        .alloc_index_with_bytes(b"payload")
        .expect("alloc session tail");
    let output_head = pool
        .alloc_index_with_bytes(b"head")
        .expect("alloc output head");

    pool.attach_clone(output_head, session_tail)
        .expect("attach clone");

    pool.free_index(session_tail);

    assert_eq!(
        chain_bytes(&pool, output_head).expect("head keeps tail alive"),
        b"headpayload"
    );
    assert_eq!(pool.in_use(), 2);

    pool.free_index(output_head);
    assert_eq!(pool.in_use(), 0);

    let reused = pool
        .alloc_index_with_bytes(b"reuse")
        .expect("reuse freed storage");
    assert_eq!(pool.in_use(), 1);
    pool.free_index(reused);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn freeing_cloned_head_keeps_shared_tail_trace_mark_live() {
    let buffers = DataPlaneBuffers::with_buffer_capacity(8, 8);
    let session_tail = buffers
        .alloc_index_with_bytes(b"payload")
        .expect("alloc session tail");
    let output_head = buffers
        .alloc_index_with_bytes(b"head")
        .expect("alloc output head");

    buffers
        .get_buffer_mut(session_tail)
        .expect("tail buffer mut")
        .set_trace_handle(7);
    buffers
        .attach_clone(output_head, session_tail)
        .expect("attach clone");

    buffers.free_index(output_head);

    assert_eq!(
        buffers
            .get_buffer(session_tail)
            .expect("tail buffer")
            .trace_handle(),
        Some(7)
    );

    buffers.free_index(session_tail);
}

#[test]
fn shared_tail_rejects_payload_mutation_but_allows_independent_header_views() {
    let pool = BufferPool::with_capacity(8, 8);
    let session_tail = pool
        .alloc_index_with_bytes(b"payload")
        .expect("alloc session tail");
    let output_head = pool
        .alloc_index_with_bytes(b"head")
        .expect("alloc output head");

    pool.attach_clone(output_head, session_tail)
        .expect("attach clone");

    assert!(pool.get_mut(session_tail).is_err());
    assert!(pool.truncate_current(session_tail, 1).is_err());
    assert!(pool.prepend(session_tail, b"x").is_err());
    assert!(pool.append(session_tail, b"x").is_err());
    pool.advance(output_head, 2)
        .expect("advance cloned header view");
    assert_eq!(
        chain_bytes(&pool, output_head).expect("cloned view advanced"),
        b"adpayload"
    );
    assert_eq!(
        chain_bytes(&pool, session_tail).expect("original view intact"),
        b"payload"
    );

    pool.free_index(output_head);
    pool.free_index(session_tail);
}

#[test]
fn shared_tail_rejects_chain_link_mutation_but_allows_clone_head_truncate() {
    let pool = BufferPool::with_capacity(4, 8);
    let session_tail = pool
        .alloc_index_with_bytes(b"abcdefgh")
        .expect("alloc session tail");
    let output_head = pool
        .alloc_index_with_bytes(b"head")
        .expect("alloc output head");

    pool.attach_clone(output_head, session_tail)
        .expect("attach clone");

    let extra_tail = pool
        .alloc_index_with_bytes(b"tail")
        .expect("alloc extra tail");

    assert!(pool.chain_buffer(session_tail, extra_tail).is_err());
    pool.truncate_current(output_head, 4)
        .expect("truncate clone head only");

    assert_eq!(
        chain_bytes(&pool, output_head).expect("clone view trimmed"),
        b"head"
    );

    assert_eq!(
        chain_bytes(&pool, session_tail).expect("tail chain unchanged"),
        b"abcdefgh"
    );

    pool.free_index(extra_tail);
    pool.free_index(output_head);
    pool.free_index(session_tail);
}

#[test]
fn buffer_advance_can_discard_prefix_across_chain_segments() {
    let pool = BufferPool::with_capacity(4, 8);
    let packet = pool
        .alloc_index_with_bytes(b"abcdefghijkl")
        .expect("alloc chained buffer");

    pool.advance(packet, 6).expect("advance across chain");

    let buffer = pool.get(packet).expect("buffer");
    assert_eq!(buffer.current_len(), 0);
    assert_eq!(buffer.total_len_not_including_first(), 6);
    drop(buffer);

    assert_eq!(
        chain_bytes(&pool, packet).expect("remaining buffer"),
        b"ghijkl"
    );

    pool.free_index(packet);
    assert_eq!(pool.in_use(), 0);
}

#[test]
fn buffer_pool_reports_single_segment_and_chained_packets() {
    let pool = BufferPool::with_capacity(4, 8);
    let single = pool
        .alloc_index_with_bytes(b"abc")
        .expect("alloc single buffer");
    let chained = pool
        .alloc_index_with_bytes(b"abcdefghij")
        .expect("alloc chained buffer");

    assert_eq!(chain_len(&pool, single).expect("single chain len"), 1);
    assert!(chain_len(&pool, chained).expect("chained chain len") > 1);
    assert_eq!(pool.get(single).expect("single current").current(), b"abc");
    assert_eq!(chain_bytes(&pool, single).expect("single buffer"), b"abc");
    assert_eq!(
        pool.get(chained).expect("chained current").current(),
        b"abcd"
    );
    assert_eq!(
        chain_bytes(&pool, chained).expect("chained buffer"),
        b"abcdefghij"
    );

    pool.free_index(single);
    pool.free_index(chained);
}

#[test]
fn buffer_pool_prefetch_read_is_best_effort_for_live_and_stale_indices() {
    let pool = BufferPool::with_capacity(4, 4);
    let buffer = pool
        .alloc_index_with_bytes(b"abcdefgh")
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
    assert_eq!(
        DataPlaneInstructionSet::Avx512.preferred_frame_batch_width(),
        FrameBatchWidth::Octo
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
        .alloc_index_with_bytes(b"packet")
        .expect("alloc first pool buffer");
    let second_index = second_pool
        .alloc_index_with_bytes(b"other")
        .expect("alloc second pool buffer");

    assert_eq!(first_index.slot(), second_index.slot());
    assert_eq!(first_index.generation(), second_index.generation());
    assert_ne!(first_index.pool_id(), second_index.pool_id());
    assert!(second_pool.get(first_index).is_err());
    assert!(chain_bytes(&second_pool, first_index).is_err());
    assert!(
        second_pool
            .chain(first_index)
            .next()
            .expect("first chain item")
            .is_err()
    );

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
        .alloc_index_with_bytes(b"one")
        .expect("alloc first worker buffer");
    let second_buffer = second
        .alloc_index_with_bytes(b"two")
        .expect("alloc second worker buffer");

    assert_eq!(first_buffer.pool_id(), second_buffer.pool_id());
    assert_eq!(first.in_use_buffers(), 2);
    assert_eq!(second.in_use_buffers(), 2);

    first.free_index(first_buffer);
    second.free_index(second_buffer);

    assert!(first.cached_free_buffers() >= 1);
    assert!(second.cached_free_buffers() >= 1);
    assert_eq!(first.in_use_buffers(), 0);

    let first_reused = first
        .alloc_index_with_bytes(b"uno")
        .expect("first worker reuses its cache");
    let second_reused = second
        .alloc_index_with_bytes(b"dos")
        .expect("second worker reuses its cache");

    assert_eq!(first_reused.slot(), first_buffer.slot());
    assert_eq!(second_reused.slot(), second_buffer.slot());
    assert_ne!(first_reused.generation(), first_buffer.generation());
    assert_ne!(second_reused.generation(), second_buffer.generation());

    first.free_index(first_reused);
    second.free_index(second_reused);
}

#[test]
fn queue_only_handoff_constructor_keeps_runtime_buffer_arenas_separate() {
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
        .alloc_index_with_bytes(b"one")
        .expect("alloc first worker buffer");
    let second_buffer = second
        .alloc_index_with_bytes(b"two")
        .expect("alloc second worker buffer");

    assert_ne!(first_buffer.pool_id(), second_buffer.pool_id());
    assert_eq!(first.in_use_buffers(), 1);
    assert_eq!(second.in_use_buffers(), 1);

    first.free_index(first_buffer);
    second.free_index(second_buffer);
    assert_eq!(first.in_use_buffers(), 0);
}

#[test]
fn buffer_frame_reset_does_not_free_buffers() {
    let runtime: DataPlaneRuntime = DataPlaneRuntime::with_capacities(8, 4, 2, 1);
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(b"one")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(b"two")
        .expect("alloc second frame buffer");
    frame.push_index(first).expect("push first frame index");
    frame.push_index(second).expect("push second frame index");

    assert_eq!(frame.len(), 2);
    assert_eq!(pool.in_use(), 2);

    frame.reset();

    assert!(frame.is_empty());
    assert_eq!(pool.in_use(), 2);
    assert_eq!(
        chain_bytes(&pool, first).expect("first buffer remains live"),
        b"one"
    );
    assert_eq!(
        chain_bytes(&pool, second).expect("second buffer remains live"),
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(b"one")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(b"two")
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
        .alloc_index_with_bytes(b"next")
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(b"second")
        .expect("alloc second frame buffer");
    frame.push_index(first).expect("push first frame index");
    frame.push_index(second).expect("push second frame index");

    let drained = frame.drain_indices().collect::<Vec<_>>();

    assert!(frame.is_empty());
    assert_eq!(pool.in_use(), 2);
    assert_eq!(drained, vec![first, second]);
    assert_eq!(
        chain_bytes(&pool, drained[0]).expect("first buffer remains live"),
        b"first"
    );
    assert_eq!(
        chain_bytes(&pool, drained[1]).expect("second buffer remains live"),
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(b"second")
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let index = pool
        .alloc_index_with_bytes(b"packet")
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(b"second")
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let indices = (0..5)
        .map(|value| {
            pool.alloc_index_with_bytes(&[value])
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let indices = (0..7)
        .map(|value| {
            pool.alloc_index_with_bytes(&[value])
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(b"second")
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
    let pool = runtime.buffers().buffers();
    let mut frame = runtime.alloc_pooled_frame().expect("alloc pooled frame");
    let first = pool
        .alloc_index_with_bytes(b"first")
        .expect("alloc first frame buffer");
    let second = pool
        .alloc_index_with_bytes(b"second")
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
        .alloc_index_with_bytes(b"one")
        .expect("alloc first buffer");
    let second = runtime
        .alloc_index_with_bytes(b"two")
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
    assert!(chain_bytes(runtime.buffers().buffers(), first).is_err());
    assert!(chain_bytes(runtime.buffers().buffers(), second).is_err());

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
        .alloc_index_with_bytes(b"one")
        .expect("alloc first buffer");
    let second = runtime
        .alloc_index_with_bytes(b"two")
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
        .alloc_index_with_bytes(b"pkt")
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
    assert!(chain_bytes(runtime.buffers().buffers(), buffer).is_err());

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
        .buffer_node_inline(
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
                .alloc_index_with_bytes(&[value])
                .expect("alloc numbered buffer")
        })
        .collect::<Vec<_>>();
    frame
        .push_indices(indices.iter().copied())
        .expect("push numbered indices");
    indices
}

use std::mem::{align_of, size_of};

use hammer_core::data_plane::{
    BUFFER_CACHE_LINE_SIZE, BUFFER_IN_USE_FOLD_THRESHOLD, BUFFER_INVALID_INDEX,
    BUFFER_THREAD_CACHE_BATCH, BUFFER_THREAD_CACHE_HIGH_WATER, Buffer, BufferFlags, BufferFrame,
    BufferFrameBatch, BufferFrameBatchCursor, BufferFrameBatchIndices, BufferFrameBatchWidth,
    BufferFrameBatchWidthPolicy, BufferFrameDrain, BufferFramePairBatch,
    BufferFramePairBatchCursor, BufferFramePending, BufferFrameQuadBatch,
    BufferFrameQuadBatchCursor, BufferHeaderCacheline0, BufferHeaderCacheline1, BufferNodeError,
    BufferPacketCursor, BufferPool, BufferPoolArena, BufferRef, BufferRefMut, BufferThreadCache,
    DEFAULT_BUFFER_FRAME_CAPACITY, DEFAULT_BUFFER_FRAME_POOL_SIZE, DEFAULT_PACKET_HEADROOM,
    DEFAULT_PRE_DATA_SIZE, DataPlaneBufferChain, DataPlaneBufferConfig, DataPlaneBuffers, Frame,
    Index, Next, NodeId, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES, Pending, PrimaryOpaque,
    SecondaryOpaque, buffer_data_offset,
};
use hammer_infra::vec::Vec;

fn test_buffers(buffer_slot_capacity: usize, buffer_slots: usize) -> DataPlaneBuffers {
    DataPlaneBuffers::new(DataPlaneBufferConfig {
        buffer_slot_capacity,
        buffer_slots,
        frame_capacity: 4,
        frame_slots: 2,
        ..DataPlaneBufferConfig::default()
    })
}

fn chain_bytes(buffers: &DataPlaneBuffers, index: Index) -> Vec<u8> {
    let mut out = Vec::new();
    for buffer in buffers.chain(index) {
        out.extend_from_slice(buffer.expect("chain buffer").current());
    }
    out
}

#[test]
fn core_exports_buffer_and_frame_value_primitives() {
    assert_eq!(align_of::<Buffer>(), BUFFER_CACHE_LINE_SIZE);
    assert_eq!(size_of::<Buffer>(), BUFFER_CACHE_LINE_SIZE * 2);
    assert!(size_of::<BufferPacketCursor>() <= 32);
    assert!(DEFAULT_BUFFER_FRAME_CAPACITY > 0);
    assert!(DEFAULT_BUFFER_FRAME_POOL_SIZE > 0);
    assert!(DEFAULT_PACKET_HEADROOM >= DEFAULT_PRE_DATA_SIZE);
    assert_eq!(BUFFER_INVALID_INDEX, u32::MAX);
    assert!(BUFFER_THREAD_CACHE_BATCH <= BUFFER_THREAD_CACHE_HIGH_WATER);
    assert!(BUFFER_IN_USE_FOLD_THRESHOLD > 0);
    assert_eq!(PRIMARY_OPAQUE_BYTES, size_of::<PrimaryOpaque>());
    assert_eq!(PRIMARY_OPAQUE_ALIGN, align_of::<PrimaryOpaque>());
    assert!(size_of::<SecondaryOpaque>() >= PRIMARY_OPAQUE_BYTES);
    assert_eq!(align_of::<BufferHeaderCacheline0>(), BUFFER_CACHE_LINE_SIZE);
    assert_eq!(align_of::<BufferHeaderCacheline1>(), BUFFER_CACHE_LINE_SIZE);
    assert_eq!(
        buffer_data_offset(),
        size_of::<Buffer>() + DEFAULT_PRE_DATA_SIZE
    );
    assert_eq!(
        BufferFrameBatchWidth::Pair.buffer_frame_batch_width(),
        BufferFrameBatchWidth::Pair
    );

    type CoreFrameNext = Frame<Next>;
    type CoreFramePending = Frame<Pending>;
    type CoreBufferFrameDrain<'a> = BufferFrameDrain<'a>;
    type CoreBufferFramePairBatchCursor<'a> = BufferFramePairBatchCursor<'a>;
    type CoreBufferFrameQuadBatchCursor<'a> = BufferFrameQuadBatchCursor<'a>;
    type CoreBufferFrameBatchCursor<'a> = BufferFrameBatchCursor<'a>;
    type CoreBufferRef<'a> = BufferRef<'a>;
    type CoreBufferRefMut<'a> = BufferRefMut<'a>;
    let _ = size_of::<BufferFlags>();
    let _ = size_of::<BufferFrame>();
    let _ = size_of::<BufferFrameBatch>();
    let _ = size_of::<BufferFrameBatchIndices>();
    let _ = size_of::<BufferFramePairBatch>();
    let _ = size_of::<BufferFrameQuadBatch>();
    let _ = size_of::<BufferFramePending>();
    let _ = size_of::<BufferPool>();
    let _ = size_of::<BufferPoolArena>();
    let _ = size_of::<BufferThreadCache>();
    let _ = size_of::<DataPlaneBufferChain>();
    let _ = size_of::<CoreFrameNext>();
    let _ = size_of::<CoreFramePending>();
    let _ = size_of::<CoreBufferFrameDrain<'static>>();
    let _ = size_of::<CoreBufferFramePairBatchCursor<'static>>();
    let _ = size_of::<CoreBufferFrameQuadBatchCursor<'static>>();
    let _ = size_of::<CoreBufferFrameBatchCursor<'static>>();
    let _ = size_of::<CoreBufferRef<'static>>();
    let _ = size_of::<CoreBufferRefMut<'static>>();

    let buffers = test_buffers(128, 1);
    let first = buffers
        .alloc_index_with_bytes(b"first")
        .expect("first buffer");
    let mut frame = buffers.get_next_frame(NodeId::new(7)).expect("next frame");

    assert_eq!(
        first.pool_id(),
        buffers.try_buffers().expect("pool").pool_id()
    );
    assert_eq!(first.slot(), 1);
    assert_eq!(first.generation(), 1);
    assert_eq!(frame.next(), NodeId::new(7));
    assert_eq!(size_of::<Index>(), 16);

    frame.push_index(first).expect("push first");
    drop(frame);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);

    let second = buffers
        .alloc_index_with_bytes(b"second")
        .expect("second buffer");
    assert_eq!(second.slot(), first.slot());
    assert_ne!(second.generation(), first.generation());
    assert!(buffers.get_buffer(first).is_err());

    let mut next_frame = buffers.get_next_frame(NodeId::new(8)).expect("next frame");
    assert_eq!(next_frame.next(), NodeId::new(8));
    assert!(next_frame.is_empty());
    next_frame.push_index(second).expect("push second");
}

#[test]
fn core_buffer_cursor_chain_and_error_metadata_behave_as_data_plane_primitives() {
    let cursor = BufferPacketCursor::new()
        .with_packet_len(64)
        .with_network_header(14, 20)
        .with_transport_header(34, 20)
        .with_transport_payload_offset(54);
    assert_eq!(cursor.packet_len(), 64);
    assert_eq!(cursor.network_header_offset(), 14);
    assert_eq!(cursor.network_header_len(), 20);
    assert_eq!(cursor.transport_header_offset(), 34);
    assert_eq!(cursor.transport_header_len(), 20);
    assert_eq!(cursor.transport_payload_offset(), 54);

    let error = BufferNodeError::new(NodeId::new(9), 3);
    assert_eq!(error.node(), NodeId::new(9));
    assert_eq!(error.code(), 3);

    let buffers = test_buffers(4, 4);
    let head = buffers
        .alloc_index_with_bytes(b"head")
        .expect("head buffer");
    let tail = buffers
        .alloc_index_with_bytes(b"tail")
        .expect("tail buffer");
    buffers.chain_buffer(head, tail).expect("chain buffers");
    assert_eq!(chain_bytes(&buffers, head), b"headtail");

    let mut frame = buffers.get_next_frame(NodeId::new(0)).expect("next frame");
    frame.push_index(head).expect("push chain head");
}

#[test]
fn core_frame_pending_owner_returns_buffers_on_drop() {
    let buffers = test_buffers(128, 4);
    let mut next = buffers.get_next_frame(NodeId::new(1)).expect("next frame");
    let index = buffers.alloc_index().expect("buffer");
    next.push_index(index).expect("push index");

    let pending = next.into_pending().expect("pending frame");
    assert_eq!(buffers.in_use_buffers(), 1);
    assert_eq!(buffers.frames_in_use(), 1);

    drop(pending);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);
}

#[test]
fn core_frame_pending_owner_can_release_trace_handles_on_return() {
    let buffers = test_buffers(128, 4);
    let mut next = buffers.get_next_frame(NodeId::new(1)).expect("next frame");
    let index = buffers.alloc_index().expect("buffer");
    buffers
        .get_buffer_mut(index)
        .expect("buffer")
        .set_trace_handle(42);
    next.push_index(index).expect("push index");

    let pending = next.into_pending().expect("pending frame");
    let mut released = Vec::new();
    pending.return_with_trace_release(|handle| released.push(handle));

    assert_eq!(released, vec![42]);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);
}

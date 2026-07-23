use std::mem::{align_of, size_of};

use hammer_core::data_plane::{
    BUFFER_CACHE_LINE_SIZE, Buffer, BufferFlags, BufferFrame, BufferNodeError, BufferPacketCursor,
    BufferPoolArena, BufferRef, BufferRefMut, DEFAULT_BUFFER_FRAME_CAPACITY,
    DEFAULT_BUFFER_FRAME_POOL_SIZE, DEFAULT_PACKET_HEADROOM, DataPlaneBuffers, Frame,
    FrameBatchWidth, Index, Next, NodeId, PRIMARY_OPAQUE_ALIGN, PRIMARY_OPAQUE_BYTES, Pending,
    PrimaryOpaque, SecondaryOpaque,
};
use hammer_core::error::{BufferInvariant, DataPlaneError, DataPlaneResult};

fn test_buffers(buffer_slot_capacity: usize, buffer_slots: usize) -> DataPlaneBuffers {
    DataPlaneBuffers::from_arenas(
        [BufferPoolArena::with_capacity(
            buffer_slot_capacity,
            buffer_slots,
        )],
        2,
        0,
        0,
    )
}

fn chain_bytes(buffers: &DataPlaneBuffers, index: Index) -> Vec<u8> {
    let mut out = Vec::new();
    for buffer in buffers.chain(index) {
        out.extend_from_slice(buffer.expect("chain buffer").current());
    }
    out
}

#[test]
fn buffer_invariant_failure_is_structured() {
    let buffers = test_buffers(4, 1);
    let index = buffers.alloc_index().expect("buffer");
    let failure: DataPlaneResult<()> = buffers.attach_clone(index, index);

    assert!(matches!(
        failure.unwrap_err(),
        DataPlaneError::BufferInvariant(BufferInvariant::CloneRequiresDistinctBuffers)
    ));
}

#[test]
fn core_exports_buffer_and_frame_value_primitives() {
    assert_eq!(align_of::<Buffer>(), BUFFER_CACHE_LINE_SIZE);
    assert_eq!(size_of::<Buffer>(), BUFFER_CACHE_LINE_SIZE * 2);
    assert!(size_of::<BufferPacketCursor>() <= 32);
    assert!(DEFAULT_BUFFER_FRAME_CAPACITY > 0);
    assert!(DEFAULT_BUFFER_FRAME_POOL_SIZE > 0);
    assert!(DEFAULT_PACKET_HEADROOM > 0);
    assert_eq!(PRIMARY_OPAQUE_BYTES, size_of::<PrimaryOpaque>());
    assert_eq!(PRIMARY_OPAQUE_ALIGN, align_of::<PrimaryOpaque>());
    assert!(size_of::<SecondaryOpaque>() >= PRIMARY_OPAQUE_BYTES);
    let _ = size_of::<FrameBatchWidth>();

    type CoreFrameNext = Frame<Next>;
    type CoreFramePending = Frame<Pending>;
    type CoreBufferRef<'a> = BufferRef<'a>;
    type CoreBufferRefMut<'a> = BufferRefMut<'a>;
    let _ = size_of::<BufferFlags>();
    let _ = size_of::<BufferFrame>();
    let _ = size_of::<BufferPoolArena>();
    let _ = size_of::<CoreFrameNext>();
    let _ = size_of::<CoreFramePending>();
    let _ = size_of::<CoreBufferRef<'static>>();
    let _ = size_of::<CoreBufferRefMut<'static>>();

    let arena = BufferPoolArena::with_capacity(128, 1);
    let buffers = DataPlaneBuffers::from_arenas([arena.clone()], 2, 0, 0);
    let first = buffers
        .alloc_index_with_bytes(b"first")
        .expect("first buffer");
    let mut frame = buffers.get_next_frame(NodeId::new(7)).expect("next frame");

    assert_eq!(first.pool_id(), arena.pool_id());
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

#[test]
fn core_frame_empty_and_full_cleanup_release_owned_buffers_once() {
    let buffers = test_buffers(64, DEFAULT_BUFFER_FRAME_CAPACITY + 1);

    let empty = buffers.get_next_frame(NodeId::new(0)).expect("empty frame");
    assert!(empty.is_empty());
    assert_eq!(empty.capacity(), DEFAULT_BUFFER_FRAME_CAPACITY);
    drop(empty);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);

    let mut full = buffers.get_next_frame(NodeId::new(1)).expect("full frame");
    let mut owned = std::vec::Vec::new();
    for _ in 0..full.capacity() {
        let index = buffers.alloc_index().expect("buffer");
        full.push_index(index).expect("push within capacity");
        owned.push(index);
    }
    let overflow = buffers.alloc_index().expect("overflow buffer");
    assert!(full.push_index(overflow).is_err());
    assert_eq!(full.len(), full.capacity());
    assert_eq!(buffers.in_use_buffers(), full.capacity() + 1);

    drop(full);
    assert_eq!(buffers.in_use_buffers(), 1);
    assert!(buffers.get_buffer(overflow).is_ok());
    for index in owned {
        assert!(buffers.get_buffer(index).is_err());
    }

    let mut cleanup = buffers
        .get_next_frame(NodeId::new(2))
        .expect("cleanup frame");
    cleanup.push_index(overflow).expect("own overflow");
    drop(cleanup);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);
}

#[test]
fn core_frame_transfer_moves_index_without_double_release() {
    let buffers = test_buffers(64, 4);
    let first = buffers.alloc_index_with_bytes(b"a").expect("first");
    let second = buffers.alloc_index_with_bytes(b"b").expect("second");

    let mut source = buffers.get_next_frame(NodeId::new(3)).expect("source");
    source.push_index(first).expect("push first");
    source.push_index(second).expect("push second");
    assert_eq!(source.indices(), &[first, second]);

    let moved = source.indices()[0];
    source.discard_prefix(1);
    assert_eq!(source.indices(), &[second]);

    let mut destination = buffers.get_next_frame(NodeId::new(4)).expect("destination");
    destination.push_index(moved).expect("transfer");
    assert_eq!(destination.indices(), &[moved]);
    assert_eq!(buffers.in_use_buffers(), 2);

    drop(source);
    assert_eq!(buffers.in_use_buffers(), 1);
    assert!(buffers.get_buffer(first).is_ok());
    assert!(buffers.get_buffer(second).is_err());

    drop(destination);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert!(buffers.get_buffer(first).is_err());
}

#[test]
fn core_frame_retain_preserves_stable_order() {
    let buffers = test_buffers(64, 8);
    let indices: std::vec::Vec<_> = (0..4)
        .map(|i| buffers.alloc_index_with_bytes(&[i]).expect("alloc index"))
        .collect();

    let mut frame = buffers.get_next_frame(NodeId::new(5)).expect("frame");
    frame
        .push_indices(indices.iter().copied())
        .expect("push indices");

    let mut discarded = std::vec::Vec::new();
    frame
        .retain_indices(|index| {
            if index == indices[0] || index == indices[2] {
                Ok(true)
            } else {
                discarded.push(index);
                Ok(false)
            }
        })
        .expect("retain selected");
    assert_eq!(frame.indices(), &[indices[0], indices[2]]);
    assert_eq!(discarded, std::vec![indices[1], indices[3]]);

    let mut cleanup = buffers.get_next_frame(NodeId::new(6)).expect("cleanup");
    cleanup
        .push_indices(discarded)
        .expect("own discarded indexes");

    drop(frame);
    drop(cleanup);
    assert_eq!(buffers.in_use_buffers(), 0);
}

#[test]
fn frame_batch_widths_retain_and_rewrite_equivalently() {
    let buffers = test_buffers(64, 9);
    let indices: Vec<_> = (0..9)
        .map(|value| {
            buffers
                .alloc_index_with_bytes(&[value])
                .expect("allocate index")
        })
        .collect();

    for width in [
        FrameBatchWidth::Pair,
        FrameBatchWidth::Quad,
        FrameBatchWidth::Octo,
    ] {
        let mut retained = BufferFrame::with_capacity(9);
        retained
            .push_indices(indices.iter().copied())
            .expect("seed retained frame");
        retained
            .retain_indices_batched(width, |index| {
                Ok(index != indices[1] && index != indices[4] && index != indices[7])
            })
            .expect("retain selected indices");
        assert_eq!(
            retained.indices(),
            &[
                indices[0], indices[2], indices[3], indices[5], indices[6], indices[8]
            ]
        );

        let mut rewritten = BufferFrame::with_capacity(9);
        rewritten
            .push_indices(indices.iter().copied())
            .expect("seed rewritten frame");
        rewritten
            .rewrite_indices_batched(width, |index| {
                if index == indices[2] {
                    Ok(None)
                } else if index == indices[5] {
                    Ok(Some(indices[8]))
                } else {
                    Ok(Some(index))
                }
            })
            .expect("rewrite selected indices");
        assert_eq!(
            rewritten.indices(),
            &[
                indices[0], indices[1], indices[3], indices[4], indices[8], indices[6], indices[7],
                indices[8]
            ]
        );
    }

    let mut cleanup = buffers
        .get_next_frame(NodeId::new(9))
        .expect("cleanup frame");
    cleanup
        .push_indices(indices)
        .expect("own allocated indexes");
}

use hammer_core::data_plane::{BufferPoolArena, DataPlaneBuffers, Index, NodeId};
use hammer_core::error::DataPlaneError;

fn release(buffers: &DataPlaneBuffers, index: Index) {
    let mut frame = buffers
        .get_next_frame(NodeId::new(0))
        .expect("cleanup frame");
    frame.push_index(index).expect("push cleanup index");
}

fn buffers(
    buffer_slot_capacity: usize,
    buffer_slots: usize,
    frame_slots: usize,
) -> DataPlaneBuffers {
    DataPlaneBuffers::from_arenas(
        [BufferPoolArena::with_capacity(
            buffer_slot_capacity,
            buffer_slots,
        )],
        frame_slots,
        0,
        0,
    )
}

#[test]
fn index_is_sixteen_bytes_and_copyable_without_refcount_change() {
    assert_eq!(std::mem::size_of::<Index>(), 16);

    let buffers = buffers(64, 2, 2);
    let index = buffers.alloc_index_with_bytes(b"a").expect("alloc");
    let copied = index;
    assert_eq!(copied.pool_id(), index.pool_id());
    assert_eq!(copied.slot(), index.slot());
    assert_eq!(copied.generation(), index.generation());
    assert_eq!(buffers.get_buffer(index).expect("live").ref_count(), 1);
    assert_eq!(buffers.get_buffer(copied).expect("copy").ref_count(), 1);
    release(&buffers, index);
}

#[test]
fn buffer_and_frame_pools_share_nonzero_pool_id_namespace() {
    let buffers = buffers(64, 2, 2);
    let index = buffers.alloc_index().expect("buffer index");
    let buffer_pool_id = index.pool_id();
    assert_ne!(buffer_pool_id, 0);
    assert_eq!(index.pool_id(), buffer_pool_id);

    let other = BufferPoolArena::with_capacity(64, 1);
    assert_ne!(other.pool_id(), 0);
    assert_ne!(other.pool_id(), buffer_pool_id);
    release(&buffers, index);
}

#[test]
fn buffer_validation_reports_structured_foreign_stale_and_free_facts() {
    let first_buffers = buffers(64, 2, 4);
    let second_buffers = buffers(64, 2, 4);
    let index = first_buffers.alloc_index_with_bytes(b"x").expect("alloc");
    let second_index = second_buffers.alloc_index().expect("second alloc");

    match second_buffers.get_buffer(index).map(|_| ()).unwrap_err() {
        DataPlaneError::ForeignIndex {
            expected_pool_id,
            actual_pool_id,
        } => {
            assert_eq!(expected_pool_id, second_index.pool_id());
            assert_eq!(actual_pool_id, index.pool_id());
        }
        other => panic!("expected ForeignIndex, got {other:?}"),
    }
    release(&second_buffers, second_index);

    release(&first_buffers, index);
    match first_buffers.get_buffer(index).map(|_| ()).unwrap_err() {
        DataPlaneError::IndexSlotFree { pool_id, slot } => {
            assert_eq!(pool_id, index.pool_id());
            assert_eq!(slot, index.slot());
        }
        other => panic!("expected IndexSlotFree, got {other:?}"),
    }

    let reused = first_buffers.alloc_index_with_bytes(b"y").expect("realloc");
    assert_eq!(reused.slot(), index.slot());
    assert_ne!(reused.generation(), index.generation());
    match first_buffers.get_buffer(index).map(|_| ()).unwrap_err() {
        DataPlaneError::StaleIndex {
            slot,
            index_generation,
            current_generation,
        } => {
            assert_eq!(slot, index.slot());
            assert_eq!(index_generation, index.generation());
            assert_eq!(current_generation, reused.generation());
        }
        other => panic!("expected StaleIndex, got {other:?}"),
    }
    release(&first_buffers, reused);
}

use hammer_core::buffer::{BufferMain, BufferPoolArena, DataPlaneBuffers};
use hammer_core::error::{BufferInvariant, DataPlaneError, DataPlaneResult};
use hammer_core::graph::{NodeErrorIndex, NodeId};

// Derived from buffer_funcs.h's first-segment allocation used by icmp4.c and
// icmp6.c. This verifies storage semantics, not ICMP graph forwarding.
#[test]
fn independent_segment_survives_original_chain_release() -> DataPlaneResult<()> {
    hammer_infra::main_heap::init_default().unwrap();
    BufferMain::new(16, 3, &[0], 1, hammer_infra::PageSize::Default)?;
    let arena = BufferPoolArena::with_capacity(16, 3);
    let buffers = DataPlaneBuffers::from_arenas([arena], 2, 1, 0);
    let mut originals = buffers.get_next_frame(NodeId::new(0))?;
    let source = buffers.alloc_index_with_bytes(&[0x31; 32])?;
    originals.push_index(source)?;
    {
        let mut buffer = buffers.get_buffer_mut(source)?;
        buffer.advance(4);
        buffer.push_uninit(8).copy_from_slice(&[0x42; 8]);
        buffer.set_trace_handle(29);
        buffer.set_node_error_index(NodeErrorIndex::new(31).unwrap());
        // SAFETY: both opaque unions are initialized, u64-aligned storage;
        // write only their first word while holding the exclusive buffer borrow.
        unsafe {
            *std::ptr::from_mut(buffer.opaque_mut()).cast::<u64>() = 17;
            *std::ptr::from_mut(buffer.opaque2_mut()).cast::<u64>() = 23;
        }
    }
    let mut responses = buffers.get_next_frame(NodeId::new(1))?;
    let response = buffers.alloc_index_from(source)?;
    responses.push_index(response)?;
    assert_ne!(response, source);
    assert_eq!(buffers.chain(source).count(), 2);
    assert_eq!(buffers.chain(response).count(), 1);
    {
        let mut buffer = buffers.get_buffer_mut(response)?;
        assert_eq!(buffer.current_data_offset(), -4);
        assert_eq!(buffer.current_len(), 20);
        assert_eq!(&buffer.current()[..8], &[0x42; 8]);
        assert_eq!(&buffer.current()[8..], &[0x31; 12]);
        assert_eq!(buffer.total_len_not_including_first(), 0);
        assert_eq!(buffer.ref_count(), 1);
        assert_eq!(buffer.trace_handle(), None);
        assert_eq!(buffer.node_error_index(), None);
        // SAFETY: the source initialized these aligned words before allocation.
        unsafe {
            assert_eq!(*std::ptr::from_ref(buffer.opaque()).cast::<u64>(), 17);
            assert_eq!(*std::ptr::from_ref(buffer.opaque2()).cast::<u64>(), 23);
        }
        buffer.current_mut()[0] = 0x55;
    }
    // Pool capacity follows Physmem page carving rather than the requested
    // minimum. Exhaust its remaining slots before checking copy pressure.
    let mut retained = Vec::new();
    loop {
        match buffers.alloc_index() {
            Ok(index) => retained.push(index),
            Err(DataPlaneError::BufferInvariant(BufferInvariant::PoolExhausted)) => break,
            Err(error) => return Err(error),
        }
    }
    assert!(matches!(
        buffers.alloc_index_from(source),
        Err(DataPlaneError::BufferInvariant(
            BufferInvariant::PoolExhausted
        ))
    ));
    for index in retained {
        buffers.drop_index_owned_with_trace(index, |_| {});
    }
    assert_eq!(buffers.in_use_buffers(), 3);
    {
        let mut buffer = buffers.get_buffer_mut(source)?;
        assert_eq!(buffer.current()[0], 0x42);
        assert_eq!(buffer.current_len(), 20);
        assert_eq!(buffer.total_len_not_including_first(), 16);
        assert_eq!(buffer.node_error_index(), NodeErrorIndex::new(31));
        assert_eq!(buffer.take_trace_handle(), Some(29));
    }
    drop(originals);
    assert_eq!(buffers.in_use_buffers(), 1);
    assert_eq!(buffers.get_buffer(response)?.current()[0], 0x55);
    drop(responses);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);
    Ok(())
}

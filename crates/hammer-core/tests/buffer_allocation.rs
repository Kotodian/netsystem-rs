use hammer_core::buffer::BufferMain;
use hammer_core::error::DataPlaneResult;
use hammer_core::graph::NodeErrorIndex;

#[hammer_component_macros::buffer_opaque(primary)]
#[derive(Clone, Copy)]
struct PacketMetadata {
    identity: u64,
}

#[hammer_component_macros::buffer_opaque(secondary)]
#[derive(Clone, Copy)]
struct PacketSecondaryMetadata {
    identity: u64,
}

// Derived from buffer_funcs.h's first-segment allocation used by icmp4.c and
// icmp6.c. This verifies storage semantics, not ICMP graph forwarding.
#[test]
fn independent_segment_survives_original_chain_release() -> DataPlaneResult<()> {
    hammer_infra::main_heap::init_default().unwrap();
    BufferMain::new(16, 3, &[0], 1, hammer_infra::PageSize::Default)?;
    let buffers = BufferMain::global();
    let mut source = u32::MAX;
    assert_eq!(buffers.add_data(1, 0, &mut source, &[0x31; 32]), 32);
    {
        // SAFETY: the fixture owns this segment until the explicit free below.
        let buffer = unsafe { buffers.buffer_mut(source) };
        buffer.advance(4);
        buffer.push_uninit(8).copy_from_slice(&[0x42; 8]);
        buffer.set_trace_handle(29);
        buffer.set_node_error_index(NodeErrorIndex::new(31).unwrap());
        hammer_core::buffer_opaque!(mut buffer => PacketMetadata).identity = 17;
        hammer_core::buffer_opaque!(mut buffer => PacketSecondaryMetadata).identity = 23;
    }
    let response = buffers.copy_no_chain(1, source).unwrap();
    assert_ne!(response, source);
    // SAFETY: both allocations are retained and no mutable borrows exist.
    unsafe {
        assert!(buffers.buffer(source).next_buffer_slot().is_some());
        assert!(buffers.buffer(response).next_buffer_slot().is_none());
    }
    {
        // SAFETY: the fixture owns this segment until the explicit free below.
        let buffer = unsafe { buffers.buffer_mut(response) };
        assert_eq!(buffer.current_data_offset(), -4);
        assert_eq!(buffer.current_len(), 20);
        assert_eq!(&buffer.current()[..8], &[0x42; 8]);
        assert_eq!(&buffer.current()[8..], &[0x31; 12]);
        assert_eq!(buffer.total_len_not_including_first(), 0);
        assert_eq!(buffer.ref_count(), 1);
        assert_eq!(buffer.trace_handle(), None);
        assert_eq!(buffer.node_error_index(), None);
        assert_eq!(
            hammer_core::buffer_opaque!(&buffer => PacketMetadata).identity,
            17
        );
        assert_eq!(
            hammer_core::buffer_opaque!(&buffer => PacketSecondaryMetadata).identity,
            23
        );
        buffer.current_mut()[0] = 0x55;
    }
    // Pool capacity follows Physmem page carving rather than the requested
    // minimum. Exhaust its remaining slots before checking copy pressure.
    let mut retained = Vec::new();
    loop {
        let mut indices = [0; 32];
        let count = buffers.alloc_from_pool(1, &mut indices, 0);
        retained.extend_from_slice(&indices[..count]);
        if count == 0 {
            break;
        }
    }
    assert!(buffers.copy_no_chain(1, source).is_none());
    buffers.free_buffers(1, &retained, true, |_| {});
    let cached_free = buffers.cached_free_buffers(1, 0);
    {
        // SAFETY: the fixture owns this segment until the explicit free below.
        let buffer = unsafe { buffers.buffer_mut(source) };
        assert_eq!(buffer.current()[0], 0x42);
        assert_eq!(buffer.current_len(), 20);
        assert_eq!(buffer.total_len_not_including_first(), 16);
        assert_eq!(buffer.node_error_index(), NodeErrorIndex::new(31));
        assert_eq!(buffer.take_trace_handle(), Some(29));
    }
    BufferMain::global().free_buffers(1, &[source], true, |_| {});
    assert_eq!(buffers.cached_free_buffers(1, 0), cached_free + 2);
    // SAFETY: releasing the source chain did not release the independent copy.
    assert_eq!(unsafe { buffers.buffer(response) }.current()[0], 0x55);
    BufferMain::global().free_buffers(1, &[response], true, |_| {});
    assert_eq!(buffers.cached_free_buffers(1, 0), cached_free + 3);
    Ok(())
}

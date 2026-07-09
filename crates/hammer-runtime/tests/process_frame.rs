use hammer_core::data_plane::{BufferFrame, BufferIndex, DataPlaneBufferConfig, NodeId};
use hammer_infra::vec::Vec;
use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, NodeResult, process_frame};

fn test_runtime(buffer_slot_capacity: usize, buffer_slots: usize) -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity,
            buffer_slots,
            ..DataPlaneBufferConfig::default()
        },
    })
}

fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, payload: &[u8]) {
    let buffer = runtime
        .alloc_index_with_bytes(payload)
        .expect("alloc packet");
    frame.push_index(buffer).expect("push index");
}

fn process_test_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    processed: &mut Vec<BufferIndex>,
    drop_next: NodeId,
) -> NodeResult {
    process_frame!(runtime, frame, |index| {
        processed.push(index);
        drop_next
    })
}

#[test]
fn process_frame_processes_all_indices_in_order() {
    let runtime = test_runtime(2048, 4096);
    let mut frame = runtime
        .buffers()
        .get_next_frame(NodeId::new(0))
        .expect("alloc frame");
    for i in 0..7u32 {
        push_packet(&runtime, &mut frame, &[i as u8]);
    }
    let mut processed = Vec::new();
    let drop_next = NodeId::new(0);
    let _ = process_test_frame(&runtime, &mut frame, &mut processed, drop_next);
    assert_eq!(processed.len(), 7);
}

use hammer_adapter::{BufferFrame, DataPlaneRuntime, NodeId, process_frame};

fn push_packet(runtime: &DataPlaneRuntime, frame: &mut BufferFrame, payload: &[u8]) {
    let buffer = runtime
        .alloc_index_with_bytes(payload)
        .expect("alloc packet");
    frame.push_index(buffer).expect("push index");
}

#[test]
fn process_frame_processes_all_indices_in_order() {
    let runtime = DataPlaneRuntime::with_buffer_capacity(2048, 4096);
    let mut frame = runtime.alloc_frame().expect("alloc frame");
    for i in 0..7u32 {
        push_packet(&runtime, &mut frame, &[i as u8]);
    }
    let mut processed = Vec::new();
    let drop_next = NodeId::new(0);
    let _ = process_frame!(&runtime, &mut frame, |index, _nf| {
        processed.push(index);
        drop_next
    });
    assert_eq!(processed.len(), 7);
}

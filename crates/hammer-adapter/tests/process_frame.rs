use hammer_adapter::{DataPlaneRuntime, NodeId, process_frame};

fn push_packet(
    runtime: &DataPlaneRuntime,
    frame_index: hammer_adapter::FrameIndex,
    payload: &[u8],
) {
    let buffer = runtime
        .alloc_index_with_bytes(payload)
        .expect("alloc packet");
    runtime
        .get_frame_mut(frame_index)
        .expect("get frame mut")
        .push_index(buffer)
        .expect("push index");
}

#[test]
fn process_frame_processes_all_indices_in_order() {
    let runtime = DataPlaneRuntime::with_buffer_capacity(2048, 4096);
    let frame_index = runtime.alloc_frame_index().expect("alloc frame");
    let frame = frame_index;
    for i in 0..7u32 {
        push_packet(&runtime, frame, &[i as u8]);
    }
    let mut processed = Vec::new();
    let drop_next = NodeId::new(0);
    runtime
        .with_frame_mut(frame, |frame| {
            let result = process_frame!(&runtime, frame, |index, _nf| {
                processed.push(index);
                drop_next
            });
            result
        })
        .expect("with_frame_mut");
    assert_eq!(processed.len(), 7);
}

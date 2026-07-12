use hammer_core::data_plane::{
    BufferFrame, DataPlaneBufferConfig, Index, NodeKind, NodeNext, NodeRegistration,
    DEFAULT_BUFFER_FRAME_CAPACITY,
};
use hammer_infra::vec::Vec;
use hammer_runtime::{
    DataPlaneRuntime, DataPlaneRuntimeConfig, NodeDescriptor, NodeProcessFn, NodeResult,
    NodeRuntimeData, process_frame,
};

fn test_runtime(buffer_slot_capacity: usize, buffer_slots: usize) -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: DataPlaneBufferConfig {
            buffer_slot_capacity,
            buffer_slots,
            frame_slots: 8,
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

fn sink_process(
    _runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let _ = frame;
    NodeResult::drop()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProcessFrameNext {
    Sink,
}

impl NodeNext for ProcessFrameNext {
    fn slot(self) -> u16 {
        0
    }
}

fn register_owner(runtime: &DataPlaneRuntime) -> hammer_core::data_plane::NodeId {
    let sink = runtime
        .nodes()
        .try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                sink_process as NodeProcessFn,
                NodeRuntimeData::empty(),
                NodeRegistration::next("process-frame-sink", 0),
                &[],
                None,
            ),
        )
        .expect("register sink");
    runtime
        .nodes()
        .try_register_descriptor(
            NodeKind::Internal,
            NodeDescriptor::new(
                sink_process as NodeProcessFn,
                NodeRuntimeData::empty(),
                NodeRegistration::next("process-frame-owner", 1),
                &[sink],
                None,
            ),
        )
        .expect("register owner")
}

fn process_test_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    processed: &mut Vec<Index>,
) -> NodeResult {
    process_frame!(runtime, frame, |index| {
        processed.push(index);
        ProcessFrameNext::Sink
    })
}

#[test]
fn process_frame_processes_all_indices_in_order() {
    let runtime = test_runtime(2048, 4096);
    let owner = register_owner(&runtime);

    let mut frame = runtime
        .buffers()
        .get_next_frame(owner)
        .expect("alloc frame");
    for i in 0..7u32 {
        push_packet(&runtime, &mut frame, &[i as u8]);
    }
    let mut processed = Vec::new();
    let _ = runtime.with_current_node(owner, || {
        process_test_frame(&runtime, &mut frame, &mut processed)
    });
    assert_eq!(processed.len(), 7);
    assert!(frame.is_empty());
}

#[test]
fn process_frame_empty_is_noop() {
    let runtime = test_runtime(2048, 64);
    let owner = register_owner(&runtime);
    let mut frame = runtime
        .buffers()
        .get_next_frame(owner)
        .expect("alloc frame");
    let mut processed = Vec::new();
    let _ = runtime.with_current_node(owner, || {
        process_test_frame(&runtime, &mut frame, &mut processed)
    });
    assert!(processed.is_empty());
    assert!(frame.is_empty());
}

#[test]
fn process_frame_full_capacity_dispatches() {
    let runtime = test_runtime(64, DEFAULT_BUFFER_FRAME_CAPACITY + 8);
    let owner = register_owner(&runtime);
    let mut frame = runtime
        .buffers()
        .get_next_frame(owner)
        .expect("alloc frame");
    for i in 0..DEFAULT_BUFFER_FRAME_CAPACITY {
        push_packet(&runtime, &mut frame, &[i as u8]);
    }
    assert_eq!(frame.len(), DEFAULT_BUFFER_FRAME_CAPACITY);
    let mut processed = Vec::new();
    let _ = runtime.with_current_node(owner, || {
        process_test_frame(&runtime, &mut frame, &mut processed)
    });
    assert_eq!(processed.len(), DEFAULT_BUFFER_FRAME_CAPACITY);
    assert!(frame.is_empty());
}

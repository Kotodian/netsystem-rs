use hammer_adapter::buffer::{DataPlaneBufferConfig, DataPlaneBuffers, DataPlaneRuntimeConfig};
use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeId, NodeRegistration, NodeResult,
};

#[derive(Debug, Clone, Copy)]
struct GuardNode;

impl Node for GuardNode {
    fn process(&mut self, _runtime: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }
}

impl InternalNode for GuardNode {
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::Plain
    }
}

fn register_guard_node(runtime: &DataPlaneRuntime) -> NodeId {
    runtime.nodes().register_internal(GuardNode)
}

fn test_buffer_config(buffer_slot_capacity: usize, buffer_slots: usize) -> DataPlaneBufferConfig {
    DataPlaneBufferConfig {
        buffer_slot_capacity,
        buffer_slots,
        ..DataPlaneBufferConfig::default()
    }
}

fn test_buffers(buffer_slot_capacity: usize, buffer_slots: usize) -> DataPlaneBuffers {
    DataPlaneBuffers::new(test_buffer_config(buffer_slot_capacity, buffer_slots))
}

fn test_runtime(buffer_slot_capacity: usize, buffer_slots: usize) -> DataPlaneRuntime {
    DataPlaneRuntime::new(DataPlaneRuntimeConfig {
        buffers: test_buffer_config(buffer_slot_capacity, buffer_slots),
    })
}

#[test]
fn dropping_frame_next_returns_buffers_and_frame_slot() {
    let buffers = test_buffers(256, 128);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);

    {
        let mut frame = buffers.get_next_frame(NodeId::new(0)).expect("frame<next>");
        for _ in 0..8 {
            let index = buffers.alloc_index().expect("buffer");
            frame.push_index(index).expect("push index");
        }
        assert_eq!(frame.len(), 8);
        assert_eq!(buffers.in_use_buffers(), 8);
        assert_eq!(buffers.frames_in_use(), 1);
    }

    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);
}

#[test]
fn putting_frame_next_transfers_cleanup_to_pending_owner() {
    let runtime = test_runtime(256, 128);
    let guard = register_guard_node(&runtime);
    let mut frame = runtime
        .buffers()
        .get_next_frame(guard)
        .expect("frame<next>");
    let index = runtime.alloc_index().expect("buffer");
    frame.push_index(index).expect("push index");

    runtime.put_next_frame(frame).expect("put next frame");

    assert_eq!(runtime.in_use_buffers(), 1);
    assert_eq!(runtime.frames_in_use(), 1);
    assert_eq!(runtime.run_ready_nodes().expect("run guard node"), 1);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}

#[test]
fn put_next_frame_failure_drops_typed_owner_resources() {
    let runtime = test_runtime(256, 128);
    let mut frame = runtime
        .buffers()
        .get_next_frame(hammer_adapter::NodeId::new(99))
        .expect("frame<next>");
    let index = runtime.alloc_index().expect("buffer");
    frame.push_index(index).expect("push index");

    let err = runtime
        .put_next_frame(frame)
        .expect_err("invalid node must fail");

    assert!(err.to_string().contains("node id out of bounds"));
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}

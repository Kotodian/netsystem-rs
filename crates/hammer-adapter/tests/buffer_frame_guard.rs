use hammer_adapter::buffer::{DataPlaneBuffers, Frame, Next};
use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeId, NodeRegistration, NodeResult,
};

#[derive(Debug, Clone, Copy)]
struct GuardNode;

impl Node for GuardNode {
    fn process(&mut self, _runtime: &DataPlaneRuntime, frame: &mut BufferFrame) -> NodeResult {
        frame.clear();
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

#[test]
fn dropping_frame_next_returns_buffers_and_frame_slot() {
    let buffers = DataPlaneBuffers::with_buffer_capacity(256, 128);
    assert_eq!(buffers.in_use_buffers(), 0);
    assert_eq!(buffers.frames_in_use(), 0);

    {
        let mut frame = buffers.alloc_frame().expect("frame<next>");
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
fn submitting_frame_next_transfers_cleanup_to_pending_owner() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_buffer_capacity(256, 128);
    let guard = register_guard_node(&runtime);
    let mut frame = runtime.alloc_frame().expect("frame<next>");
    let index = runtime.alloc_index().expect("buffer");
    frame.push_index(index).expect("push index");

    runtime.submit_frame(frame, guard).expect("submit");

    let pending = runtime
        .take_pending_frame()
        .expect("frame<pending>")
        .expect("one frame<pending>");
    assert_eq!(pending.len(), 1);
    assert_eq!(runtime.in_use_buffers(), 1);
    drop(pending);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}

#[test]
fn pending_frame_converts_to_next_with_into_trait() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_buffer_capacity(256, 128);
    let first_guard = register_guard_node(&runtime);
    let second_guard = register_guard_node(&runtime);
    let mut frame = runtime.alloc_frame().expect("frame<next>");
    let index = runtime.alloc_index().expect("buffer");
    frame.push_index(index).expect("push index");

    runtime.submit_frame(frame, first_guard).expect("submit");

    let pending = runtime
        .take_pending_frame()
        .expect("frame<pending>")
        .expect("one frame<pending>");
    let next: Frame<Next> = pending.into();
    assert_eq!(next.len(), 1);

    runtime.submit_frame(next, second_guard).expect("resubmit");
    let pending = runtime
        .take_pending_frame()
        .expect("frame<pending>")
        .expect("resubmitted frame<pending>");
    drop(pending);
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}

#[test]
fn submit_frame_failure_drops_typed_owner_resources() {
    let runtime = hammer_adapter::DataPlaneRuntime::with_buffer_capacity(256, 128);
    let mut frame = runtime.alloc_frame().expect("frame<next>");
    let index = runtime.alloc_index().expect("buffer");
    frame.push_index(index).expect("push index");

    let err = runtime
        .submit_frame(frame, hammer_adapter::NodeId::new(99))
        .expect_err("invalid node must fail");

    assert!(err.to_string().contains("node id out of bounds"));
    assert_eq!(runtime.in_use_buffers(), 0);
    assert_eq!(runtime.frames_in_use(), 0);
}

use hammer_core::data_plane::{Frame, NodeId};
use hammer_runtime::{DataPlaneMain, Node, NodeRuntime, RuntimeResult};

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = register_tcp4_drop,
    name = "tcp4-drop",
    role = internal,
)]
pub struct Tcp4DropNode;

#[hammer_component_macros::graph_node(
    graph = tcp_worker,
    init = register_tcp6_drop,
    name = "tcp6-drop",
    role = internal,
)]
pub struct Tcp6DropNode;

pub fn register_tcp4_drop(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal(Tcp4DropNode::new())
}

pub fn register_tcp6_drop(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal(Tcp6DropNode::new())
}

impl Node for Tcp4DropNode {
    #[inline(always)]
    fn process(runtime: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        let processed_vectors = frame.len();
        runtime.buffer_free(frame.vector_args());
        processed_vectors
    }
}

impl Node for Tcp6DropNode {
    #[inline(always)]
    fn process(runtime: &mut DataPlaneMain, _: &mut NodeRuntime, frame: &mut Frame) -> usize {
        let processed_vectors = frame.len();
        runtime.buffer_free(frame.vector_args());
        processed_vectors
    }
}

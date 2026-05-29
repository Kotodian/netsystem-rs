use std::fmt;

use hammer_adapter::{BufferFrame, DataPlaneRuntime, Node, NodeResult};
use hammer_core::error::CoreResult;

pub(crate) type RuntimeDataPlaneRuntime = DataPlaneRuntime<RuntimeDataPlaneNode>;

pub(crate) fn new_worker_runtime(slot_capacity: usize, slots: usize) -> RuntimeDataPlaneRuntime {
    let runtime = RuntimeDataPlaneRuntime::with_buffer_capacity(slot_capacity, slots);
    runtime
        .nodes()
        .register(RuntimeDataPlaneNode::Drop(RuntimeDropNode));
    runtime
}

pub(crate) enum RuntimeDataPlaneNode {
    Drop(RuntimeDropNode),
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RuntimeDropNode;

impl RuntimeDropNode {
    fn process<G>(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.drain_pending() {
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }
}

impl fmt::Debug for RuntimeDataPlaneNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drop(_) => f.write_str("RuntimeDataPlaneNode::Drop"),
        }
    }
}

impl Node<RuntimeDataPlaneNode> for RuntimeDataPlaneNode {
    fn process(
        &mut self,
        runtime: &RuntimeDataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        match self {
            Self::Drop(node) => node.process(runtime, frame),
        }
    }
}

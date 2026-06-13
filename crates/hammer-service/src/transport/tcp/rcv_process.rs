use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult, NodeRuntimeData,
};
use hammer_core::error::CoreResult;

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    Drop,
}

pub struct TcpRcvProcessControlPlane {
    next: [NodeId; TcpRcvProcessNext::COUNT],
}

impl TcpRcvProcessControlPlane {
    #[inline]
    pub fn new(next: [NodeId; TcpRcvProcessNext::COUNT]) -> Self {
        Self { next }
    }

    #[inline]
    pub fn node(&self) -> TcpRcvProcessNode {
        TcpRcvProcessNode::new(self.next)
    }
}

#[hammer_component_macros::node(role = internal, next = TcpRcvProcessNext)]
pub struct TcpRcvProcessNode {
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl Node for TcpRcvProcessNode {
    #[inline(always)]
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        frame.clear();
        Ok(NodeResult::drop())
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_rcv_process_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn tcp_rcv_process_process(
    _runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    frame.clear();
    Ok(NodeResult::drop())
}

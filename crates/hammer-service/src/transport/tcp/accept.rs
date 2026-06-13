use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
};
use hammer_core::error::CoreResult;

#[hammer_component_macros::node_next]
pub enum TcpAcceptNext {
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpAcceptNext)]
pub struct TcpAcceptNode {}

impl Node for TcpAcceptNode {
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
        tcp_accept_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn tcp_accept_process(
    _runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    frame.clear();
    Ok(NodeResult::drop())
}

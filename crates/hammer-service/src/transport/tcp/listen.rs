use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::CoreResult;

#[hammer_component_macros::node_next]
pub enum TcpListenNext {
    Accept,
}

#[hammer_component_macros::node(role = internal, next = TcpListenNext)]
pub struct TcpListenNode {
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl Node for TcpListenNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        let accept = next[TcpListenNext::Accept as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |_index| Ok(Some(accept)),
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_listen_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn tcp_listen_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpListenNode::runtime_nexts(runtime)?;
    let accept = next[TcpListenNext::Accept as usize];
    let (result, _) =
        NodeVectorDispatch::new(None).route_frame_index(runtime, frame, move |index| {
            tcp_listen_next_for_index(index, accept)
        })?;
    Ok(result)
}

#[inline]
fn tcp_listen_next_for_index(_index: BufferIndex, accept: NodeId) -> CoreResult<Option<NodeId>> {
    Ok(Some(accept))
}

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeVectorDispatch,
};
use hammer_core::error::CoreResult;

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    Drop,
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
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpRcvProcessNext::Drop as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |_index| Ok(Some(drop_next)),
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_rcv_process_process
    }
}

fn tcp_rcv_process_process(
    runtime: &DataPlaneRuntime,
    _data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpRcvProcessNode::runtime_nexts(runtime)?;
    let drop_next = next[TcpRcvProcessNext::Drop as usize];
    let (result, _) =
        NodeVectorDispatch::new(None)
            .route_frame_index(runtime, frame, |_index| Ok(Some(drop_next)))?;
    Ok(result)
}

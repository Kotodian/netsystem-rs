use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeVectorDispatch,
};
use hammer_core::error::CoreResult;

#[hammer_component_macros::node_next]
pub enum TcpSynSentNext {
    Drop,
}

#[hammer_component_macros::node(role = internal, next = TcpSynSentNext)]
pub struct TcpSynSentNode {
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl Node for TcpSynSentNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpSynSentNext::Drop as usize];
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
        tcp_syn_sent_process
    }
}

fn tcp_syn_sent_process(
    runtime: &DataPlaneRuntime,
    _data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpSynSentNode::runtime_nexts(runtime)?;
    let drop_next = next[TcpSynSentNext::Drop as usize];
    let (result, _) =
        NodeVectorDispatch::new(None)
            .route_frame_index(runtime, frame, |_index| Ok(Some(drop_next)))?;
    Ok(result)
}

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeVectorDispatch,
};
use hammer_core::error::CoreResult;

#[hammer_component_macros::node_next]
pub enum TcpEstablishedNext {
    RcvProcess,
}

#[hammer_component_macros::node(role = internal, next = TcpEstablishedNext)]
pub struct TcpEstablishedNode {
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl Node for TcpEstablishedNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let next = Self::runtime_nexts(runtime)?;
        let rcv_process = next[TcpEstablishedNext::RcvProcess as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |_index| Ok(Some(rcv_process)),
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_established_process
    }
}

fn tcp_established_process(
    runtime: &DataPlaneRuntime,
    _data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = TcpEstablishedNode::runtime_nexts(runtime)?;
    let rcv_process = next[TcpEstablishedNext::RcvProcess as usize];
    let (result, _) =
        NodeVectorDispatch::new(None)
            .route_frame_index(runtime, frame, |_index| Ok(Some(rcv_process)))?;
    Ok(result)
}

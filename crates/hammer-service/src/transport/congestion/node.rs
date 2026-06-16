use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult, NodeRuntimeData,
    NodeVectorDispatch,
};
use hammer_core::error::CoreResult;

#[hammer_component_macros::node_next]
pub enum CongestionControlNext {
    Transmit,
    Defer,
    Drop,
}

#[hammer_component_macros::node(role = internal, next = CongestionControlNext)]
pub struct CongestionControlNode {}

impl Node for CongestionControlNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        congestion_control_process(runtime, NodeRuntimeData::default(), frame)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        congestion_control_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(NodeRuntimeData::default())
    }
}

fn congestion_control_process(
    runtime: &DataPlaneRuntime,
    _data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let next = CongestionControlNode::runtime_nexts(runtime)?;
    congestion_control_frame(runtime, frame, next)
}

pub(crate) fn congestion_control_frame(
    runtime: &DataPlaneRuntime,
    frame: &mut BufferFrame,
    next: [NodeId; CongestionControlNext::COUNT],
) -> CoreResult<NodeResult> {
    let transmit = next[CongestionControlNext::Transmit as usize];
    let drop = next[CongestionControlNext::Drop as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        let metadata = runtime.metadata(index)?;
        if metadata.source.is_none() || metadata.destination.is_none() {
            return Ok(Some(drop));
        }
        Ok(Some(transmit))
    })?;
    Ok(result)
}

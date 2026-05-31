use std::collections::HashMap;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Node, NodeId, NodeNextFrames,
    NodeResult, RouteDecision, RouteMetadata, RouteTarget, for_each_buffer_frame_index,
};
use hammer_core::error::{CoreError, CoreResult};

#[derive(Debug, Default)]
pub struct RouteLookupNode {
    outbounds: HashMap<String, NodeId>,
    endpoints: HashMap<String, NodeId>,
    reject: Option<NodeId>,
    hijack_dns: Option<NodeId>,
}

impl RouteLookupNode {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn with_outbound(mut self, id: impl Into<String>, node: NodeId) -> Self {
        self.register_outbound(id, node);
        self
    }

    #[inline]
    pub fn with_endpoint(mut self, id: impl Into<String>, node: NodeId) -> Self {
        self.register_endpoint(id, node);
        self
    }

    #[inline]
    pub fn with_reject(mut self, node: NodeId) -> Self {
        self.reject = Some(node);
        self
    }

    #[inline]
    pub fn with_hijack_dns(mut self, node: NodeId) -> Self {
        self.hijack_dns = Some(node);
        self
    }

    #[inline]
    pub fn register_outbound(&mut self, id: impl Into<String>, node: NodeId) {
        self.outbounds.insert(id.into(), node);
    }

    #[inline]
    pub fn register_endpoint(&mut self, id: impl Into<String>, node: NodeId) {
        self.endpoints.insert(id.into(), node);
    }

    #[inline(always)]
    fn target_for_index<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
    ) -> CoreResult<NodeId> {
        let buffer = runtime.get_buffer(index)?;
        self.target_for_metadata(buffer.metadata())
    }

    #[inline(always)]
    fn target_for_metadata(&self, metadata: &RouteMetadata) -> CoreResult<NodeId> {
        let decision = metadata
            .route_decision
            .as_ref()
            .ok_or_else(|| CoreError::internal("route decision is missing"))?;
        match decision {
            RouteDecision::Route {
                target: RouteTarget::Outbound(id),
            } => {
                self.outbounds.get(id.as_str()).copied().ok_or_else(|| {
                    CoreError::internal(format!("outbound route node not found: {id}"))
                })
            }
            RouteDecision::Route {
                target: RouteTarget::Endpoint(id),
            } => {
                self.endpoints.get(id.as_str()).copied().ok_or_else(|| {
                    CoreError::internal(format!("endpoint route node not found: {id}"))
                })
            }
            RouteDecision::Reject { .. } => self
                .reject
                .ok_or_else(|| CoreError::internal("reject route node not configured")),
            RouteDecision::HijackDns => self
                .hijack_dns
                .ok_or_else(|| CoreError::internal("dns hijack route node not configured")),
        }
    }
}

impl<G> Node<G> for RouteLookupNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut next_frames = NodeNextFrames::default();
        for_each_buffer_frame_index!(runtime, frame, |index| {
            let node = self.target_for_index(runtime, index)?;
            next_frames.enqueue(runtime, node, index)
        })?;
        frame.clear();
        next_frames.schedule(runtime)?;
        Ok(NodeResult::drop())
    }
}

impl<G> InternalNode<G> for RouteLookupNode {}

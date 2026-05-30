use std::collections::HashMap;
use std::ops::Deref;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, Node, NodeId, NodeNextFrames,
    NodeResult, RouteDecision, RouteMetadata, RouteTarget, Router,
};
use hammer_core::error::{CoreError, CoreResult};

pub struct RouteMatchNode<R> {
    router: R,
    next: NodeId,
}

impl<R> RouteMatchNode<R> {
    pub fn new(router: R, next: NodeId) -> Self {
        Self { router, next }
    }
}

impl<R, T, G> Node<G> for RouteMatchNode<R>
where
    R: Deref<Target = T>,
    T: Router + ?Sized,
{
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let mut cursor = frame.pair_batch_cursor();
        cursor.prefetch_next_pair(runtime);
        while let Some(batch) = cursor.next() {
            cursor.prefetch_next_pair(runtime);
            for index in batch.indices() {
                let mut buffer = runtime.get_buffer_mut(index)?;
                let metadata = buffer.metadata_mut();
                self.router.prepare_route_metadata(metadata)?;
                let decision = self.router.match_route(metadata)?;
                metadata.route_decision = Some(decision);
            }
        }
        Ok(NodeResult::next_current(self.next))
    }
}

impl<R, T, G> InternalNode<G> for RouteMatchNode<R>
where
    R: Deref<Target = T>,
    T: Router + ?Sized,
{
}

#[derive(Debug, Default)]
pub struct RouteLookupNode {
    outbounds: HashMap<String, NodeId>,
    endpoints: HashMap<String, NodeId>,
    reject: Option<NodeId>,
    hijack_dns: Option<NodeId>,
}

impl RouteLookupNode {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_outbound(mut self, id: impl Into<String>, node: NodeId) -> Self {
        self.register_outbound(id, node);
        self
    }

    pub fn with_endpoint(mut self, id: impl Into<String>, node: NodeId) -> Self {
        self.register_endpoint(id, node);
        self
    }

    pub fn with_reject(mut self, node: NodeId) -> Self {
        self.reject = Some(node);
        self
    }

    pub fn with_hijack_dns(mut self, node: NodeId) -> Self {
        self.hijack_dns = Some(node);
        self
    }

    pub fn register_outbound(&mut self, id: impl Into<String>, node: NodeId) {
        self.outbounds.insert(id.into(), node);
    }

    pub fn register_endpoint(&mut self, id: impl Into<String>, node: NodeId) {
        self.endpoints.insert(id.into(), node);
    }

    fn target_for_index<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        index: BufferIndex,
    ) -> CoreResult<NodeId> {
        let buffer = runtime.get_buffer(index)?;
        self.target_for_metadata(buffer.metadata())
    }

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
        let indices = frame.pending_indices().to_vec();
        frame.clear();
        let mut cursor = indices.as_slice().chunks_exact(2);
        for batch in cursor.by_ref() {
            for index in batch.iter().copied() {
                let node = self.target_for_index(runtime, index)?;
                next_frames.enqueue(runtime, node, index)?;
            }
        }
        for index in cursor.remainder().iter().copied() {
            let node = self.target_for_index(runtime, index)?;
            next_frames.enqueue(runtime, node, index)?;
        }
        next_frames.schedule(runtime)?;
        Ok(NodeResult::drop())
    }
}

impl<G> InternalNode<G> for RouteLookupNode {}

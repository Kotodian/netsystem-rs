use std::collections::HashMap;
use std::ops::Deref;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, InternalNode, NextFrame, Node, NodeId, NodeResult,
    RouteDecision, RouteMetadata, RouteTarget, Router,
};
use hammer_core::error::{CoreError, CoreResult};

const MAX_ROUTE_DISPATCH_GROUPS: usize = hammer_adapter::node::MAX_NODE_NEXT_FRAMES;

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
                runtime.with_metadata_mut(index, |metadata| {
                    self.router.prepare_route_metadata(metadata)?;
                    let decision = self.router.match_route(metadata)?;
                    metadata.route_decision = Some(decision);
                    Ok(())
                })??;
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
pub struct RouteDispatchNode {
    outbounds: HashMap<String, NodeId>,
    endpoints: HashMap<String, NodeId>,
    reject: Option<NodeId>,
    hijack_dns: Option<NodeId>,
}

impl RouteDispatchNode {
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
        runtime.with_metadata(index, |metadata| self.target_for_metadata(metadata))?
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

    fn collect_groups<G>(
        &self,
        runtime: &DataPlaneRuntime<G>,
        frame: &BufferFrame,
    ) -> CoreResult<RouteDispatchGroups> {
        let mut groups = RouteDispatchGroups::default();
        let mut cursor = frame.pair_batch_cursor();
        cursor.prefetch_next_pair(runtime);
        while let Some(batch) = cursor.next() {
            cursor.prefetch_next_pair(runtime);
            for index in batch.indices() {
                groups.push(self.target_for_index(runtime, index)?)?;
            }
        }
        Ok(groups)
    }
}

impl<G> Node<G> for RouteDispatchNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        let groups = self.collect_groups(runtime, frame)?;
        if groups.is_empty() {
            return Ok(NodeResult::drop());
        }

        let first = groups.node(0)?;
        if groups.len() == 1 {
            frame.retain_indices(|index| {
                let node = self.target_for_index(runtime, index)?;
                if node == first {
                    Ok(true)
                } else {
                    Err(CoreError::internal("route dispatch group changed"))
                }
            })?;
            return if frame.has_pending() {
                Ok(NodeResult::next_current(first))
            } else {
                Ok(NodeResult::drop())
            };
        }

        let mut extra_frames = [None; MAX_ROUTE_DISPATCH_GROUPS];
        for group_index in 1..groups.len() {
            extra_frames[group_index] = Some(runtime.alloc_frame_index()?);
        }

        let retain_result = frame.retain_indices(|index| {
            let node = self.target_for_index(runtime, index)?;
            if node == first {
                return Ok(true);
            }
            let group_index = groups
                .position(node)
                .ok_or_else(|| CoreError::internal("route dispatch group changed"))?;
            let frame_index = extra_frames[group_index]
                .ok_or_else(|| CoreError::internal("route dispatch frame missing"))?;
            runtime.with_frame_mut(frame_index, |frame| frame.push_index(index))??;
            Ok(false)
        });
        if let Err(err) = retain_result {
            for frame_index in extra_frames.iter().flatten().copied() {
                let _ = runtime.free_frame_index(frame_index);
            }
            return Err(err);
        }

        let mut result = NodeResult::drop();
        if frame.has_pending() {
            result.push(NextFrame::Current(first))?;
        }
        for group_index in 1..groups.len() {
            let frame_index = extra_frames[group_index]
                .ok_or_else(|| CoreError::internal("route dispatch frame missing"))?;
            if runtime.with_frame(frame_index, BufferFrame::has_pending)? {
                result.push(NextFrame::Frame {
                    node: groups.node(group_index)?,
                    frame: frame_index,
                })?;
            } else {
                runtime.free_frame_index(frame_index)?;
            }
        }
        Ok(result)
    }
}

impl<G> InternalNode<G> for RouteDispatchNode {}

#[derive(Debug, Clone, Copy)]
struct RouteDispatchGroups {
    nodes: [Option<NodeId>; MAX_ROUTE_DISPATCH_GROUPS],
    len: usize,
}

impl Default for RouteDispatchGroups {
    fn default() -> Self {
        Self {
            nodes: [None; MAX_ROUTE_DISPATCH_GROUPS],
            len: 0,
        }
    }
}

impl RouteDispatchGroups {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push(&mut self, node: NodeId) -> CoreResult<()> {
        if self.position(node).is_some() {
            return Ok(());
        }
        if self.len == MAX_ROUTE_DISPATCH_GROUPS {
            return Err(CoreError::internal(
                "route dispatch next frame capacity exceeded",
            ));
        }
        self.nodes[self.len] = Some(node);
        self.len += 1;
        Ok(())
    }

    fn node(&self, index: usize) -> CoreResult<NodeId> {
        self.nodes
            .get(index)
            .and_then(|node| *node)
            .ok_or_else(|| CoreError::internal("route dispatch group index out of bounds"))
    }

    fn position(&self, node: NodeId) -> Option<usize> {
        self.nodes[..self.len]
            .iter()
            .position(|candidate| *candidate == Some(node))
    }
}

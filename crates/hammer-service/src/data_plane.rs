use std::ops::Deref;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, InternalNode, Node, NodeId, NodeResult, Router,
};
use hammer_core::error::CoreResult;

#[derive(Debug, Clone, Copy, Default)]
pub struct DropNode;

impl DropNode {
    #[inline(always)]
    pub const fn new() -> Self {
        Self
    }
}

impl<G> Node<G> for DropNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime<G>,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        for index in frame.drain_pending() {
            runtime.free_index(index);
        }
        Ok(NodeResult::drop())
    }
}

impl<G> InternalNode<G> for DropNode {}

#[hammer_component_macros::node_next]
pub enum RouteMatchNext {
    Lookup,
}

pub struct RouteMatchNode<R> {
    router: R,
    next: [NodeId; RouteMatchNext::COUNT],
}

impl<R> RouteMatchNode<R> {
    pub fn new(router: R, next: [NodeId; RouteMatchNext::COUNT]) -> Self {
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
        let mut cursor = frame.batch_cursor(runtime.preferred_frame_batch_width());
        cursor.prefetch_next(runtime);
        while let Some(batch) = cursor.next() {
            cursor.prefetch_next(runtime);
            for index in batch.indices() {
                let mut buffer = runtime.get_buffer_mut(index)?;
                let metadata = buffer.metadata_mut();
                self.router.prepare_route_metadata(metadata)?;
                let decision = self.router.match_route(metadata)?;
                metadata.route_decision = Some(decision);
            }
        }
        Ok(NodeResult::next_current(
            self.next[RouteMatchNext::Lookup.slot()],
        ))
    }
}

impl<R, T, G> InternalNode<G> for RouteMatchNode<R>
where
    R: Deref<Target = T>,
    T: Router + ?Sized,
{
}

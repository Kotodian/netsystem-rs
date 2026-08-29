use hammer_core::data_plane::{Buffer, BufferFrame, Index, NodeId, NodeRegistration};
use hammer_runtime::RuntimeResult;
use hammer_runtime::{
    DataPlaneRuntime, InternalNode, Node, NodeErrorCode, NodeProcessFn, NodeResult,
    add_packet_trace,
};

pub use crate::feature_arc::{
    Feature, FeatureArc, FeatureArcControl, FeatureArcSpec, FeatureArcStart, FeatureArcStartHandle,
    FeatureArcStartNode, FeatureArcStartSlot, next_feature_frame, next_feature_slot_for_index,
};

/// Record a generated node-local error and store its preinstalled global
/// index in a packet buffer.
#[inline(always)]
pub fn set_buffer_node_error<E>(
    runtime: &DataPlaneRuntime,
    buffer: &mut Buffer,
    error: E,
) -> RuntimeResult<()>
where
    E: NodeErrorCode,
{
    let error = runtime.record_current_node_error(error)?;
    buffer.set_node_error_index(error);
    Ok(())
}

/// Record a generated node-local error and store its preinstalled global
/// index in the packet buffer identified by `index`.
#[inline(always)]
pub fn set_index_node_error<E>(
    runtime: &DataPlaneRuntime,
    index: Index,
    error: E,
) -> RuntimeResult<()>
where
    E: NodeErrorCode,
{
    let error = runtime.record_current_node_error(error)?;
    let mut buffer = runtime.get_buffer_mut(index)?;
    buffer.set_node_error_index(error);
    Ok(())
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::data_plane::register_drop,
)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DropNode;

impl DropNode {
    pub const NODE_NAME: &'static str = "drop";

    #[inline]
    pub fn new() -> Self {
        Self
    }
}

#[hammer_component_macros::graph_node(
    graph = service,
    init = crate::data_plane::register_handoff,
)]
#[derive(Debug, Clone, Copy, Default)]
pub struct HandoffNode;

impl HandoffNode {
    pub const NODE_NAME: &'static str = "handoff";

    #[inline]
    pub fn new() -> Self {
        Self
    }
}

pub fn register_drop(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    runtime.nodes().try_register_internal(DropNode)
}

pub fn register_handoff(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    runtime
        .nodes()
        .register_internal_with_handle(runtime.handoff_node_handle()?, HandoffNode)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct DropTrace {
    pub dropped: usize,
}

impl Node for DropNode {
    #[inline(always)]
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        drop_node_process
    }
}

fn drop_node_process(
    runtime: &DataPlaneRuntime,
    _data: hammer_runtime::node::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    let dropped = frame.pending_len();
    let indices = frame.pending_indices();
    let len = indices.len();
    let mut read = 0usize;
    while read + 4 <= len {
        if read + 4 < len {
            runtime.prefetch_header(indices[read + 4]);
        }
        if read + 5 < len {
            runtime.prefetch_header(indices[read + 5]);
        }
        if read + 6 < len {
            runtime.prefetch_header(indices[read + 6]);
        }
        if read + 7 < len {
            runtime.prefetch_header(indices[read + 7]);
        }
        let index0 = indices[read];
        let index1 = indices[read + 1];
        let index2 = indices[read + 2];
        let index3 = indices[read + 3];
        let _ = add_packet_trace!(runtime, index0, DropTrace { dropped });
        let _ = add_packet_trace!(runtime, index1, DropTrace { dropped });
        let _ = add_packet_trace!(runtime, index2, DropTrace { dropped });
        let _ = add_packet_trace!(runtime, index3, DropTrace { dropped });
        read += 4;
    }
    if read + 2 <= len {
        if read + 2 < len {
            runtime.prefetch_header(indices[read + 2]);
        }
        if read + 3 < len {
            runtime.prefetch_header(indices[read + 3]);
        }
        let index0 = indices[read];
        let index1 = indices[read + 1];
        let _ = add_packet_trace!(runtime, index0, DropTrace { dropped });
        let _ = add_packet_trace!(runtime, index1, DropTrace { dropped });
        read += 2;
    }
    while read < len {
        if read + 1 < len {
            runtime.prefetch_header(indices[read + 1]);
        }
        let index0 = indices[read];
        let _ = add_packet_trace!(runtime, index0, DropTrace { dropped });
        read += 1;
    }
    NodeResult::drop()
}

impl InternalNode for DropNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, 0)
    }
}

impl Node for HandoffNode {
    #[inline(always)]
    fn process(&mut self, _runtime: &DataPlaneRuntime, _frame: &mut BufferFrame) -> NodeResult {
        NodeResult::drop()
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        handoff_node_process
    }
}

fn handoff_node_process(
    runtime: &DataPlaneRuntime,
    _data: hammer_runtime::node::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> NodeResult {
    // Handoff continuation stores the destination as NodeId in current_config.
    // Direct get/push/put is allowed for Handoff; Graph Fanout stays worker-local
    // and does not resolve cross-worker continuation identities.
    let indices: Vec<_> = frame.indices().iter().copied().collect();
    frame.discard_prefix(indices.len());
    for index in indices {
        let next = runtime
            .current_config(index)
            .expect("handoff buffer must carry a continuation next");
        let mut next_frame = runtime
            .buffers()
            .get_next_frame(next)
            .expect("handoff continuation next frame");
        next_frame
            .push_index(index)
            .expect("handoff continuation push");
        runtime
            .put_next_frame(next_frame)
            .expect("handoff continuation put");
    }
    NodeResult::drop()
}

impl InternalNode for HandoffNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next(Self::NODE_NAME, 0)
    }
}

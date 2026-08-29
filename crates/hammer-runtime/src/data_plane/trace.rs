use super::*;

impl DataPlaneMain {
    pub fn set_trace_control(&self, control: Option<TraceControlHandle>) {
        self.trace.set_control(control);
    }

    #[inline]
    pub fn node_by_name(&self, name: &str) -> Option<NodeId> {
        self.nodes.node_by_name(name)
    }

    #[inline]
    pub(crate) fn set_current_node(&self, node: Option<NodeId>) {
        self.current_node.set(node);
    }

    #[inline]
    pub fn current_node(&self) -> Option<NodeId> {
        self.current_node.get()
    }

    /// Run `f` with `node` installed as the ambient current graph node.
    ///
    /// Used by Session Queue test helpers and any non-dispatch path that must
    /// flush through Graph Fanout against a concrete owner node's local nexts.
    #[inline]
    pub fn with_current_node<R>(&self, node: NodeId, f: impl FnOnce() -> R) -> R {
        let previous = self.current_node.get();
        self.current_node.set(Some(node));
        let result = f();
        self.flush_fanout_appendable();
        self.current_node.set(previous);
        result
    }

    #[inline]
    pub fn may_mark_trace(&self, node: NodeId) -> bool {
        self.trace.may_mark(node)
    }

    #[inline]
    pub fn try_mark_trace(&self, node: NodeId, index: Index) -> RuntimeResult<()> {
        if !self.trace.may_mark(node) {
            return Ok(());
        }
        if self.get_buffer(index)?.trace_handle().is_some() {
            return Ok(());
        }
        let node_name = self.nodes.node_name(node)?;
        if let Some(handle) = self.trace.try_mark(node, node_name) {
            self.get_buffer_mut(index)?.set_trace_handle(handle);
        }
        Ok(())
    }

    #[inline]
    pub fn add_trace<T: PacketTrace>(&self, index: Index, trace: T) -> RuntimeResult<()> {
        let Some(node) = self.current_node() else {
            return Ok(());
        };
        let Some(handle) = self.get_buffer(index)?.trace_handle() else {
            return Ok(());
        };
        let node_name = self.nodes.node_name(node)?;
        let formatter = self.nodes.node_trace_formatter(node)?;
        let payload_bytes = bincode::serialize(&trace)
            .map_err(|source| RuntimeError::PacketTraceSerialization { source })?;
        self.trace
            .add_entry(handle, node, node_name, formatter, payload_bytes);
        Ok(())
    }

    #[inline(always)]
    pub fn should_trace_packet(&self, index: Index) -> RuntimeResult<bool> {
        Ok(crate::unlikely(
            self.get_buffer(index)?.trace_handle().is_some(),
        ))
    }

    /// Record the current node's generated local error and return its global index.
    #[inline]
    pub fn record_current_node_error<E: NodeErrorCode>(
        &self,
        error: E,
    ) -> RuntimeResult<NodeErrorIndex> {
        let node = self
            .current_node()
            .ok_or(RuntimeError::NodeDispatchContextMissing)?;
        self.nodes.record_node_error(node, error.local_code())
    }

    #[inline]
    pub fn preferred_frame_batch_width(&self) -> FrameBatchWidth {
        preferred_frame_batch_width(self.simd_bytes)
    }
}

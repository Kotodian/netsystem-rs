use super::*;

impl DataPlaneMain {
    pub fn schedule_empty_frame(&self, node: NodeId) -> RuntimeResult<()> {
        let frame = self.buffers.get_next_frame(node)?;
        let pending = frame.into_pending()?;
        self.nodes.schedule_frame(node, pending, true)
    }

    #[inline]
    pub fn schedule_polling_driver_nodes(&self) -> RuntimeResult<usize> {
        self.schedule_polling_nodes(NodeKind::Driver)
    }

    #[inline]
    pub fn schedule_polling_pre_input_nodes(&self) -> RuntimeResult<usize> {
        self.schedule_polling_nodes(NodeKind::PreInput)
    }

    fn schedule_polling_nodes(&self, kind: NodeKind) -> RuntimeResult<usize> {
        let nodes = self.nodes.polling_nodes_to_schedule(kind)?;
        let scheduled = nodes.len();
        for node in nodes {
            self.schedule_empty_frame(node)?;
        }
        Ok(scheduled)
    }

    pub(crate) fn schedule_interrupt_driver_nodes(&self) -> RuntimeResult<usize> {
        self.schedule_interrupt_nodes(NodeKind::Driver)
    }

    pub(crate) fn schedule_interrupt_pre_input_nodes(&self) -> RuntimeResult<usize> {
        self.schedule_interrupt_nodes(NodeKind::PreInput)
    }

    fn schedule_interrupt_nodes(&self, kind: NodeKind) -> RuntimeResult<usize> {
        let mut scheduled = 0;
        let mut start = 0;
        while let Some(node) = self.nodes.next_interrupt_pending_for_kind(start, kind) {
            start = node.slot() as usize + 1;
            self.schedule_empty_frame(node)?;
            scheduled += 1;
        }
        Ok(scheduled)
    }

    #[inline]
    pub fn set_node_interrupt_pending(&self, node: NodeId) -> RuntimeResult<bool> {
        if !self.nodes.mark_interrupt_pending(node)? {
            return Ok(false);
        }
        if let Err(err) = self.schedule_empty_frame(node) {
            let _ = self.nodes.clear_interrupt_pending(node);
            return Err(err);
        }
        Ok(true)
    }

    /// VPP `vlib_node_set_interrupt_pending(target_vm, node)`.
    ///
    /// Atomically coalesces one exact node interrupt on the target Data
    /// Worker and wakes that worker. It carries no payload and performs no
    /// Session work. Invalid published worker/node identity is a Runtime
    /// invariant violation, not a per-packet recoverable failure.
    #[inline]
    pub fn set_worker_node_interrupt_pending(&self, worker: DataWorkerId, node: NodeId) {
        if let Some(handoff) = &self.handoff {
            handoff.set_worker_node_interrupt_pending(worker, node);
        }
    }

    pub(crate) fn attach_worker_interrupt_thread(&self) {
        if let Some(handoff) = &self.handoff {
            handoff.attach_current_thread();
        }
    }

    pub(crate) fn schedule_remote_interrupts(&self) -> RuntimeResult<usize> {
        let Some(handoff) = &self.handoff else {
            return Ok(0);
        };
        let mut scheduled = 0;
        handoff.drain_worker_interrupts(|node| {
            self.schedule_empty_frame(node)
                .expect("published worker interrupt node must schedule");
            scheduled += 1;
        });
        Ok(scheduled)
    }
}

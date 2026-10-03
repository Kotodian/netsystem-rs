use super::*;

impl DataPlaneMain {
    pub fn schedule_empty_frame(&self, node: NodeId) -> RuntimeResult<()> {
        self.nodes.schedule_node(node)
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

    pub(crate) fn schedule_worker_node_interrupts(&self) -> RuntimeResult<usize> {
        let worker = match self.data_worker_id() {
            Ok(worker) => worker,
            Err(_) => return Ok(0),
        };
        let descriptor = crate::ThreadMain::global()
            .thread_by_index(worker.thread_index())
            .expect("executing Data Worker has a thread descriptor");
        let mut scheduled = 0;
        for word in 0..self.nodes.node_count().div_ceil(64) {
            let Some(mut bits) = descriptor.take_node_interrupts(word) else {
                break;
            };
            while bits != 0 {
                let bit = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let node = NodeId::new((word * 64 + bit) as u32);
                scheduled += usize::from(self.set_node_interrupt_pending(node)?);
            }
        }
        Ok(scheduled)
    }
}

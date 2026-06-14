use std::time::Instant;

use hammer_adapter::{
    BufferFrame, DataPlaneRuntime, DataWorkerId, DriverNode, Node, NodeProcessFn, NodeRegistration,
    NodeResult, NodeRuntimeData,
};
use hammer_core::error::CoreResult;

use crate::session::node::SessionQueueHandle;

use super::TcpSessionProtocol;

#[derive(Debug)]
pub struct TcpSessionQueueNode {
    handle: SessionQueueHandle,
}

impl TcpSessionQueueNode {
    #[inline]
    pub fn new(worker: DataWorkerId) -> CoreResult<Self> {
        Ok(Self {
            handle: TcpSessionProtocol::register_queue(worker)?,
        })
    }

    #[inline]
    pub fn handle(&self) -> SessionQueueHandle {
        self.handle
    }
}

impl Node for TcpSessionQueueNode {
    fn process(
        &mut self,
        _runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        frame.clear();
        TcpSessionProtocol::with_queue(self.handle, |runtime| {
            runtime.run_once_at(Instant::now())?;
            Ok(NodeResult::drop())
        })
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_session_queue_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        Ok(self.handle.runtime_data())
    }
}

impl DriverNode for TcpSessionQueueNode {
    #[inline]
    fn node_registration(&self) -> NodeRegistration
    where
        Self: Sized,
    {
        NodeRegistration::next("session-queue", 0)
    }
}

fn tcp_session_queue_process(
    _runtime: &DataPlaneRuntime,
    data: NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    frame.clear();
    TcpSessionProtocol::with_queue(SessionQueueHandle::new(data), |runtime| {
        runtime.run_once_at(Instant::now())?;
        Ok(NodeResult::drop())
    })
}

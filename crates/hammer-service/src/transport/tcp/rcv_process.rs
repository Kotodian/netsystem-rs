use std::cell::RefCell;

use super::connection::TcpSessionAccessSlot;
use super::input::take_pending_tcp_app_ingress;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    Drop,
}

pub struct TcpRcvProcessControlPlane {
    next: [NodeId; TcpRcvProcessNext::COUNT],
}

impl TcpRcvProcessControlPlane {
    #[inline]
    pub fn new(next: [NodeId; TcpRcvProcessNext::COUNT]) -> Self {
        Self { next }
    }

    #[inline]
    pub fn node(&self) -> TcpRcvProcessNode {
        TcpRcvProcessNode::new(
            register_tcp_rcv_process_runtime(TcpSessionAccessSlot::new()),
            TcpSessionAccessSlot::new(),
            self.next,
        )
    }
}

#[derive(Clone)]
struct TcpRcvProcessRuntime {
    access: TcpSessionAccessSlot,
}

thread_local! {
    static TCP_RCV_PROCESS_RUNTIMES: RefCell<hammer_infra::vec::Vec<TcpRcvProcessRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

fn register_tcp_rcv_process_runtime(access: TcpSessionAccessSlot) -> NodeRuntimeData {
    TCP_RCV_PROCESS_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpRcvProcessRuntime { access });
        NodeRuntimeData::from_usize(slot).expect("TCP receive runtime slot overflow")
    })
}

fn tcp_rcv_process_runtime(data: NodeRuntimeData) -> CoreResult<TcpRcvProcessRuntime> {
    let slot = data.usize_word(0)?;
    TCP_RCV_PROCESS_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP receive runtime slot is invalid"))
    })
}

fn sync_tcp_rcv_process_runtime(
    data: NodeRuntimeData,
    access: TcpSessionAccessSlot,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    TCP_RCV_PROCESS_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP receive runtime slot is invalid"))?;
        runtime.access = access;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpRcvProcessNext)]
pub struct TcpRcvProcessNode {
    runtime_data: NodeRuntimeData,
    access: TcpSessionAccessSlot,
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl TcpRcvProcessNode {
    #[inline]
    pub fn with_session_access(mut self, access: TcpSessionAccessSlot) -> Self {
        if let Err(_err) = sync_tcp_rcv_process_runtime(self.runtime_data, access.clone()) {}
        self.access = access;
        self
    }
}

impl Node for TcpRcvProcessNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        sync_tcp_rcv_process_runtime(self.runtime_data, self.access.clone())?;
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpRcvProcessNext::Drop as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| tcp_rcv_process_next_for_index(runtime, index, drop_next, &self.access),
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_rcv_process_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_rcv_process_runtime(self.runtime_data, self.access.clone())?;
        Ok(self.runtime_data)
    }
}

fn tcp_rcv_process_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_rcv_process_runtime(data)?;
    let next = TcpRcvProcessNode::runtime_nexts(runtime)?;
    let drop_next = next[TcpRcvProcessNext::Drop as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        tcp_rcv_process_next_for_index(runtime, index, drop_next, &state.access)
    })?;
    Ok(result)
}

fn tcp_rcv_process_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: hammer_adapter::NodeId,
    access: &TcpSessionAccessSlot,
) -> CoreResult<Option<hammer_adapter::NodeId>> {
    let Some(pending) = take_pending_tcp_app_ingress(index)? else {
        return Ok(Some(drop_next));
    };
    let Some(target) = access.target_for_lookup(pending.connection_id)? else {
        return Ok(Some(drop_next));
    };
    target.post_recv_cqe_with_fin(runtime, index, pending.fin)?;
    Ok(None)
}

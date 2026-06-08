use std::cell::RefCell;
use std::sync::Arc;

use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeProcessFn, NodeResult, NodeRuntimeData,
    NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};

use super::TcpLookupId;
use super::input::take_pending_tcp_app_bridge;
use crate::app::{AppIngressRegistry, AppIngressTarget};

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    Drop,
}

#[derive(Clone)]
struct TcpRcvProcessRuntime {
    app_bridges: Arc<AppIngressRegistry<TcpLookupId>>,
}

thread_local! {
    static TCP_RCV_PROCESS_RUNTIMES: RefCell<Vec<TcpRcvProcessRuntime>> = const { RefCell::new(Vec::new()) };
}

fn register_tcp_rcv_process_runtime() -> NodeRuntimeData {
    TCP_RCV_PROCESS_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpRcvProcessRuntime {
            app_bridges: Arc::new(AppIngressRegistry::new()),
        });
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
    app_bridges: Arc<AppIngressRegistry<TcpLookupId>>,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    TCP_RCV_PROCESS_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP receive runtime slot is invalid"))?;
        runtime.app_bridges = app_bridges;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpRcvProcessNext)]
pub struct TcpRcvProcessNode {
    #[node(default = register_tcp_rcv_process_runtime())]
    runtime_data: NodeRuntimeData,
    #[node(default)]
    app_bridges: Arc<AppIngressRegistry<TcpLookupId>>,
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl TcpRcvProcessNode {
    #[inline]
    pub fn with_app_bridge(mut self, connection_id: TcpLookupId, target: AppIngressTarget) -> Self {
        let next = (*self.app_bridges)
            .clone()
            .with_target(connection_id, target);
        self.app_bridges = Arc::new(next);
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
        sync_tcp_rcv_process_runtime(self.runtime_data, Arc::clone(&self.app_bridges))?;
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpRcvProcessNext::Drop as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| {
                tcp_rcv_process_next_for_index(
                    runtime,
                    index,
                    drop_next,
                    Arc::clone(&self.app_bridges),
                )
            },
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
        sync_tcp_rcv_process_runtime(self.runtime_data, Arc::clone(&self.app_bridges))?;
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
        tcp_rcv_process_next_for_index(runtime, index, drop_next, Arc::clone(&state.app_bridges))
    })?;
    Ok(result)
}

fn tcp_rcv_process_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: hammer_adapter::NodeId,
    app_bridges: Arc<AppIngressRegistry<TcpLookupId>>,
) -> CoreResult<Option<hammer_adapter::NodeId>> {
    let Some(connection_id) = take_pending_tcp_app_bridge(index)? else {
        return Ok(Some(drop_next));
    };
    let Some(target) = app_bridges.get(&connection_id) else {
        return Ok(Some(drop_next));
    };
    target.deliver_recv(runtime, index)?;
    Ok(None)
}

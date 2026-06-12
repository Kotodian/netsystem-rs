use std::cell::RefCell;
use std::sync::Arc;

use super::TcpLookupId;
use super::input::take_pending_tcp_app_ingress;
use crate::app::{AppIngressRegistry, AppIngressTarget};
use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};

#[hammer_component_macros::node_next]
pub enum TcpRcvProcessNext {
    Drop,
}

#[derive(Clone)]
struct TcpRcvProcessSnapshot {
    app_ingress: AppIngressRegistry<TcpLookupId>,
}

impl TcpRcvProcessSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            app_ingress: AppIngressRegistry::new(),
        }
    }
}

#[derive(Clone)]
struct TcpRcvProcessSnapshotHandle {
    inner: Arc<ArcSwap<TcpRcvProcessSnapshot>>,
}

impl TcpRcvProcessSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<TcpRcvProcessSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<TcpRcvProcessSnapshot>> {
        self.inner.load()
    }

    #[inline]
    fn publish_app_ingress(
        &self,
        app_ingress: impl IntoIterator<Item = (TcpLookupId, AppIngressTarget)>,
    ) {
        let mut registry = AppIngressRegistry::new();
        for (connection_id, target) in app_ingress {
            registry.insert(connection_id, target);
        }
        self.inner.rcu(|current| {
            let mut next = TcpRcvProcessSnapshot::clone(current);
            next.app_ingress = registry.clone();
            next
        });
    }
}

pub struct TcpRcvProcessControlPlane {
    inner: Arc<ArcSwap<TcpRcvProcessSnapshot>>,
    next: [NodeId; TcpRcvProcessNext::COUNT],
}

impl TcpRcvProcessControlPlane {
    #[inline]
    pub fn new(next: [NodeId; TcpRcvProcessNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpRcvProcessSnapshot::new())),
            next,
        }
    }

    #[inline]
    pub fn publish_app_ingress(
        &self,
        app_ingress: impl IntoIterator<Item = (TcpLookupId, AppIngressTarget)>,
    ) -> CoreResult<()> {
        TcpRcvProcessSnapshotHandle::new(Arc::clone(&self.inner)).publish_app_ingress(app_ingress);
        Ok(())
    }

    #[inline]
    pub fn node(&self) -> TcpRcvProcessNode {
        TcpRcvProcessNode::new(
            register_tcp_rcv_process_runtime(TcpRcvProcessSnapshotHandle::new(Arc::clone(
                &self.inner,
            ))),
            TcpRcvProcessSnapshotHandle::new(Arc::clone(&self.inner)),
            self.next,
        )
    }
}

#[derive(Clone)]
struct TcpRcvProcessRuntime {
    snapshot: TcpRcvProcessSnapshotHandle,
}

thread_local! {
    static TCP_RCV_PROCESS_RUNTIMES: RefCell<hammer_infra::vec::Vec<TcpRcvProcessRuntime>> =
        const { RefCell::new(hammer_infra::vec::Vec::new()) };
}

fn register_tcp_rcv_process_runtime(snapshot: TcpRcvProcessSnapshotHandle) -> NodeRuntimeData {
    TCP_RCV_PROCESS_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpRcvProcessRuntime { snapshot });
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
    snapshot: TcpRcvProcessSnapshotHandle,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    TCP_RCV_PROCESS_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP receive runtime slot is invalid"))?;
        runtime.snapshot = snapshot;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpRcvProcessNext)]
pub struct TcpRcvProcessNode {
    runtime_data: NodeRuntimeData,
    snapshot: TcpRcvProcessSnapshotHandle,
    #[node(default)]
    cached_next: Option<hammer_adapter::NodeId>,
}

impl TcpRcvProcessNode {
    #[inline]
    pub fn with_app_ingress(self, connection_id: TcpLookupId, target: AppIngressTarget) -> Self {
        self.snapshot.publish_app_ingress([(connection_id, target)]);
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
        sync_tcp_rcv_process_runtime(self.runtime_data, self.snapshot.clone())?;
        let snapshot = self.snapshot.load();
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpRcvProcessNext::Drop as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| {
                tcp_rcv_process_next_for_index(runtime, index, drop_next, &snapshot.app_ingress)
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
        sync_tcp_rcv_process_runtime(self.runtime_data, self.snapshot.clone())?;
        Ok(self.runtime_data)
    }
}

fn tcp_rcv_process_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_rcv_process_runtime(data)?;
    let snapshot = state.snapshot.load();
    let next = TcpRcvProcessNode::runtime_nexts(runtime)?;
    let drop_next = next[TcpRcvProcessNext::Drop as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        tcp_rcv_process_next_for_index(runtime, index, drop_next, &snapshot.app_ingress)
    })?;
    Ok(result)
}

fn tcp_rcv_process_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: hammer_adapter::NodeId,
    app_ingress: &AppIngressRegistry<TcpLookupId>,
) -> CoreResult<Option<hammer_adapter::NodeId>> {
    let Some(pending) = take_pending_tcp_app_ingress(index)? else {
        return Ok(Some(drop_next));
    };
    let Some(target) = app_ingress.get(&pending.connection_id) else {
        return Ok(Some(drop_next));
    };
    target.post_recv_cqe_with_fin(runtime, index, pending.fin)?;
    Ok(None)
}

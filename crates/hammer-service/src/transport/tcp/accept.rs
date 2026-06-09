use std::cell::RefCell;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_infra::{map::FlatHashTable, vec::Vec as InfraVec};
use hammer_runtime::app::{AppContext, AppSocketId};

use super::TcpLookupId;
use super::input::take_pending_tcp_accept;

#[hammer_component_macros::node_next]
pub enum TcpAcceptNext {
    Drop,
}

pub trait TcpAcceptBackend: Send + Sync {
    fn accept(
        &self,
        listener_id: TcpLookupId,
        registration: &TcpAcceptRegistration,
        remote: SocketAddr,
        local: SocketAddr,
    ) -> CoreResult<()>;
}

#[derive(Clone)]
pub struct TcpAcceptRegistration {
    app: AppContext,
    listener: AppSocketId,
}

impl TcpAcceptRegistration {
    #[inline]
    pub fn new(app: AppContext, listener: AppSocketId) -> Self {
        Self { app, listener }
    }

    #[inline]
    pub fn app(&self) -> &AppContext {
        &self.app
    }

    #[inline]
    pub fn listener(&self) -> AppSocketId {
        self.listener
    }
}

impl std::fmt::Debug for TcpAcceptRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpAcceptRegistration")
            .field("listener", &self.listener)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Default)]
struct TcpAcceptRegistry {
    slots: FlatHashTable<TcpLookupId, u32>,
    registrations: InfraVec<TcpAcceptRegistration>,
}

impl TcpAcceptRegistry {
    #[inline]
    fn publish_listeners(
        &mut self,
        listeners: impl IntoIterator<Item = (TcpLookupId, TcpAcceptRegistration)>,
    ) {
        self.slots = FlatHashTable::new();
        self.registrations = InfraVec::new();
        for (listener_id, registration) in listeners {
            let slot = self.registrations.len() as u32;
            self.registrations.push(registration);
            self.slots.insert(listener_id, slot);
        }
    }

    #[inline]
    fn get(&self, listener_id: &TcpLookupId) -> Option<&TcpAcceptRegistration> {
        let slot = self.slots.lookup(listener_id)? as usize;
        self.registrations.get(slot)
    }
}

#[derive(Clone)]
struct TcpAcceptSnapshot {
    registry: TcpAcceptRegistry,
}

impl TcpAcceptSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            registry: TcpAcceptRegistry::default(),
        }
    }
}

#[derive(Clone)]
struct TcpAcceptSnapshotHandle {
    inner: Arc<ArcSwap<TcpAcceptSnapshot>>,
}

impl TcpAcceptSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<TcpAcceptSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<TcpAcceptSnapshot>> {
        self.inner.load()
    }

    #[inline]
    fn publish_listeners(
        &self,
        listeners: impl IntoIterator<Item = (TcpLookupId, TcpAcceptRegistration)>,
    ) {
        let mut registry = TcpAcceptRegistry::default();
        registry.publish_listeners(listeners);
        self.inner.rcu(|current| {
            let mut next = TcpAcceptSnapshot::clone(current);
            next.registry = registry.clone();
            next
        });
    }
}

pub struct TcpAcceptControlPlane {
    inner: Arc<ArcSwap<TcpAcceptSnapshot>>,
    backend: Arc<dyn TcpAcceptBackend>,
    next: [NodeId; TcpAcceptNext::COUNT],
}

impl TcpAcceptControlPlane {
    #[inline]
    pub fn new(backend: Arc<dyn TcpAcceptBackend>, next: [NodeId; TcpAcceptNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpAcceptSnapshot::new())),
            backend,
            next,
        }
    }

    #[inline]
    pub fn publish_listeners(
        &self,
        listeners: impl IntoIterator<Item = (TcpLookupId, TcpAcceptRegistration)>,
    ) -> CoreResult<()> {
        TcpAcceptSnapshotHandle::new(Arc::clone(&self.inner)).publish_listeners(listeners);
        Ok(())
    }

    #[inline]
    pub fn node(&self) -> TcpAcceptNode {
        TcpAcceptNode::new(
            register_tcp_accept_runtime(
                TcpAcceptSnapshotHandle::new(Arc::clone(&self.inner)),
                Arc::clone(&self.backend),
            ),
            TcpAcceptSnapshotHandle::new(Arc::clone(&self.inner)),
            Arc::clone(&self.backend),
            self.next,
        )
    }
}

#[derive(Clone)]
struct TcpAcceptRuntime {
    snapshot: TcpAcceptSnapshotHandle,
    backend: Arc<dyn TcpAcceptBackend>,
}

thread_local! {
    static TCP_ACCEPT_RUNTIMES: RefCell<InfraVec<TcpAcceptRuntime>> =
        const { RefCell::new(InfraVec::new()) };
}

fn register_tcp_accept_runtime(
    snapshot: TcpAcceptSnapshotHandle,
    backend: Arc<dyn TcpAcceptBackend>,
) -> NodeRuntimeData {
    TCP_ACCEPT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpAcceptRuntime { snapshot, backend });
        NodeRuntimeData::from_usize(slot).expect("TCP accept runtime slot overflow")
    })
}

fn tcp_accept_runtime(data: NodeRuntimeData) -> CoreResult<TcpAcceptRuntime> {
    let slot = data.usize_word(0)?;
    TCP_ACCEPT_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP accept runtime slot is invalid"))
    })
}

fn sync_tcp_accept_runtime(
    data: NodeRuntimeData,
    snapshot: TcpAcceptSnapshotHandle,
    backend: Arc<dyn TcpAcceptBackend>,
) -> CoreResult<()> {
    let slot = data.usize_word(0)?;
    TCP_ACCEPT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP accept runtime slot is invalid"))?;
        runtime.snapshot = snapshot;
        runtime.backend = backend;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpAcceptNext)]
pub struct TcpAcceptNode {
    runtime_data: NodeRuntimeData,
    snapshot: TcpAcceptSnapshotHandle,
    backend: Arc<dyn TcpAcceptBackend>,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl Node for TcpAcceptNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        sync_tcp_accept_runtime(
            self.runtime_data,
            self.snapshot.clone(),
            Arc::clone(&self.backend),
        )?;
        let snapshot = self.snapshot.load();
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpAcceptNext::Drop as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| tcp_accept_next_for_index(runtime, index, drop_next, &snapshot, &*self.backend),
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_accept_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_accept_runtime(
            self.runtime_data,
            self.snapshot.clone(),
            Arc::clone(&self.backend),
        )?;
        Ok(self.runtime_data)
    }
}

fn tcp_accept_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_accept_runtime(data)?;
    let snapshot = state.snapshot.load();
    let next = TcpAcceptNode::runtime_nexts(runtime)?;
    let drop_next = next[TcpAcceptNext::Drop as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        tcp_accept_next_for_index(runtime, index, drop_next, &snapshot, &*state.backend)
    })?;
    Ok(result)
}

fn tcp_accept_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: NodeId,
    snapshot: &TcpAcceptSnapshot,
    backend: &dyn TcpAcceptBackend,
) -> CoreResult<Option<NodeId>> {
    let Some(listener_id) = take_pending_tcp_accept(index)? else {
        return Ok(Some(drop_next));
    };
    let Some(registration) = snapshot.registry.get(&listener_id) else {
        return Ok(Some(drop_next));
    };
    let metadata = runtime.metadata(index)?;
    let remote = metadata
        .source
        .ok_or_else(|| CoreError::internal("tcp accept requires source metadata"))?;
    let local = metadata
        .destination
        .ok_or_else(|| CoreError::internal("tcp accept requires destination metadata"))?;
    backend.accept(
        listener_id,
        registration,
        SocketAddr::new(remote.host, remote.port),
        SocketAddr::new(local.host, local.port),
    )?;
    Ok(Some(drop_next))
}

use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{DataWorkerId, NodeId};
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::TcpConnectionId;
use hammer_infra::{map::FlatHashTable, vec::Vec as InfraVec};

use super::{TcpEstablishedBackend, TcpEstablishedNext, TcpEstablishedNode, TcpLookupId, TcpState};

const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpConnectionSnapshot {
    pub lookup_id: TcpLookupId,
    pub connection_id: Option<TcpConnectionId>,
    pub owner_worker: DataWorkerId,
    pub state: TcpState,
    pub local_port: u16,
    pub local: Option<SocketAddr>,
    pub remote: SocketAddr,
    pub iss: u32,
    pub irs: u32,
    pub snd_una: u32,
    pub snd_nxt: u32,
    pub snd_wnd: u32,
    pub rcv_nxt: u32,
    pub rcv_wnd: u32,
}

impl TcpConnectionSnapshot {
    #[inline]
    pub fn with_default_windows(
        lookup_id: TcpLookupId,
        connection_id: Option<TcpConnectionId>,
        owner_worker: DataWorkerId,
        state: TcpState,
        local_port: u16,
        local: Option<SocketAddr>,
        remote: SocketAddr,
    ) -> Self {
        Self {
            lookup_id,
            connection_id,
            owner_worker,
            state,
            local_port,
            local,
            remote,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: DEFAULT_TCP_WINDOW,
            rcv_nxt: 0,
            rcv_wnd: DEFAULT_TCP_WINDOW,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TcpConnectionSnapshotPool {
    connections: InfraVec<TcpConnectionSnapshot>,
    lookup_slots: FlatHashTable<TcpLookupId, usize>,
    connection_slots: FlatHashTable<u64, usize>,
}

impl TcpConnectionSnapshotPool {
    #[inline]
    pub fn empty() -> Self {
        Self {
            connections: InfraVec::new(),
            lookup_slots: FlatHashTable::new(),
            connection_slots: FlatHashTable::new(),
        }
    }

    #[inline]
    pub fn lookup_by_lookup_id(&self, lookup_id: TcpLookupId) -> Option<TcpConnectionSnapshot> {
        self.lookup_slots
            .lookup(&lookup_id)
            .and_then(|slot| self.connections.get(slot).copied())
    }

    #[inline]
    pub fn lookup_by_connection_id(
        &self,
        connection_id: TcpConnectionId,
    ) -> Option<TcpConnectionSnapshot> {
        self.connection_slots
            .lookup(&connection_id.get())
            .and_then(|slot| self.connections.get(slot).copied())
    }

    #[inline]
    pub(crate) fn insert(&mut self, snapshot: TcpConnectionSnapshot) {
        let slot = self.connections.len();
        self.lookup_slots.insert(snapshot.lookup_id, slot);
        if let Some(connection_id) = snapshot.connection_id {
            self.connection_slots.insert(connection_id.get(), slot);
        }
        self.connections.push(snapshot);
    }
}

impl Default for TcpConnectionSnapshotPool {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone)]
pub struct TcpWorkerOwnedConnectionState {
    owner_worker: DataWorkerId,
    pool: TcpConnectionSnapshotPool,
}

impl TcpWorkerOwnedConnectionState {
    #[inline]
    pub fn new(owner_worker: DataWorkerId) -> Self {
        Self {
            owner_worker,
            pool: TcpConnectionSnapshotPool::empty(),
        }
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub fn insert(&mut self, snapshot: TcpConnectionSnapshot) {
        debug_assert_eq!(snapshot.owner_worker, self.owner_worker);
        self.pool.insert(snapshot);
    }

    #[inline]
    pub fn publish_snapshot(&self) -> TcpConnectionSnapshotPool {
        self.pool.clone()
    }
}

#[derive(Clone)]
pub(crate) struct TcpEstablishedSnapshot {
    pub(crate) connections: TcpConnectionSnapshotPool,
}

impl TcpEstablishedSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            connections: TcpConnectionSnapshotPool::empty(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct TcpEstablishedSnapshotHandle {
    inner: Arc<ArcSwap<TcpEstablishedSnapshot>>,
}

impl TcpEstablishedSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<TcpEstablishedSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    pub(crate) fn load(&self) -> arc_swap::Guard<Arc<TcpEstablishedSnapshot>> {
        self.inner.load()
    }

    #[inline]
    fn publish_connections(&self, connections: TcpConnectionSnapshotPool) {
        self.inner.rcu(|current| {
            let mut next = TcpEstablishedSnapshot::clone(current);
            next.connections = connections.clone();
            next
        });
    }
}

fn default_tcp_established_snapshot() -> TcpEstablishedSnapshotHandle {
    TcpEstablishedSnapshotHandle::new(Arc::new(ArcSwap::from_pointee(
        TcpEstablishedSnapshot::new(),
    )))
}

pub struct TcpEstablishedControlPlane {
    inner: Arc<ArcSwap<TcpEstablishedSnapshot>>,
    backend: Option<Arc<dyn TcpEstablishedBackend>>,
    next: [NodeId; TcpEstablishedNext::COUNT],
}

impl TcpEstablishedControlPlane {
    #[inline]
    pub fn new(next: [NodeId; TcpEstablishedNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpEstablishedSnapshot::new())),
            backend: None,
            next,
        }
    }

    #[inline]
    pub fn with_backend<O>(mut self, backend: Arc<O>) -> Self
    where
        O: TcpEstablishedBackend + 'static,
    {
        self.backend = Some(backend);
        self
    }

    #[inline]
    pub fn publish_connections(&self, connections: TcpConnectionSnapshotPool) -> CoreResult<()> {
        TcpEstablishedSnapshotHandle::new(Arc::clone(&self.inner)).publish_connections(connections);
        Ok(())
    }

    #[inline]
    pub fn node(&self) -> TcpEstablishedNode {
        TcpEstablishedNode::new(self.next).with_runtime(
            TcpEstablishedSnapshotHandle::new(Arc::clone(&self.inner)),
            self.backend.clone(),
        )
    }
}

pub(crate) fn default_established_snapshot_handle() -> TcpEstablishedSnapshotHandle {
    default_tcp_established_snapshot()
}

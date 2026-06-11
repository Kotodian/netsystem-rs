use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;

use arc_swap::ArcSwap;
use hammer_adapter::{DataWorkerId, NodeId};
use hammer_core::error::CoreResult;
use hammer_core::protocol::tcp::{TcpConnectionId, TcpSeq};
use hammer_infra::{map::FlatHashTable, vec::Vec as InfraVec};

use super::congestion::TcpCongestionState;
use super::output::{DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TcpOutputRetransmitQueue, TcpOutputSendView};
use super::{TcpEstablishedBackend, TcpEstablishedNext, TcpEstablishedNode, TcpLookupId, TcpState};

const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;
const DEFAULT_TCP_MAX_SEGMENT_SIZE: u32 = DEFAULT_TCP_OUTPUT_PAYLOAD_LEN as u32;

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

#[derive(Debug, Clone)]
pub struct TcpDataPlaneConnection {
    lookup_id: TcpLookupId,
    connection_id: Option<TcpConnectionId>,
    owner_worker: DataWorkerId,
    state: TcpState,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    iss: u32,
    irs: u32,
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    rcv_nxt: u32,
    rcv_wnd: u32,
    retransmit_queue: TcpOutputRetransmitQueue,
    congestion: TcpCongestionState,
    next_output_at: Option<std::time::Instant>,
}

impl TcpDataPlaneConnection {
    #[inline]
    pub fn new(
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
            retransmit_queue: TcpOutputRetransmitQueue::new(),
            congestion: TcpCongestionState::new(DEFAULT_TCP_MAX_SEGMENT_SIZE),
            next_output_at: None,
        }
    }

    #[inline]
    pub fn lookup_id(&self) -> TcpLookupId {
        self.lookup_id
    }

    #[inline]
    pub fn connection_id(&self) -> Option<TcpConnectionId> {
        self.connection_id
    }

    #[inline]
    pub fn owner_worker(&self) -> DataWorkerId {
        self.owner_worker
    }

    #[inline]
    pub fn state(&self) -> TcpState {
        self.state
    }

    #[inline]
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    #[inline]
    pub fn local(&self) -> Option<SocketAddr> {
        self.local
    }

    #[inline]
    pub fn remote(&self) -> SocketAddr {
        self.remote
    }

    #[inline]
    pub fn iss(&self) -> u32 {
        self.iss
    }

    #[inline]
    pub fn irs(&self) -> u32 {
        self.irs
    }

    #[inline]
    pub fn snd_una(&self) -> u32 {
        self.snd_una
    }

    #[inline]
    pub fn snd_nxt(&self) -> u32 {
        self.snd_nxt
    }

    #[inline]
    pub fn snd_wnd(&self) -> u32 {
        self.snd_wnd
    }

    #[inline]
    pub fn rcv_nxt(&self) -> u32 {
        self.rcv_nxt
    }

    #[inline]
    pub fn rcv_wnd(&self) -> u32 {
        self.rcv_wnd
    }

    #[inline]
    pub fn congestion(&self) -> &TcpCongestionState {
        &self.congestion
    }

    #[inline]
    pub fn congestion_mut(&mut self) -> &mut TcpCongestionState {
        &mut self.congestion
    }

    #[inline]
    pub fn retransmit_queue(&self) -> &TcpOutputRetransmitQueue {
        &self.retransmit_queue
    }

    #[inline]
    pub fn retransmit_queue_mut(&mut self) -> &mut TcpOutputRetransmitQueue {
        &mut self.retransmit_queue
    }

    #[inline]
    pub fn output_send_view(&self) -> TcpOutputSendView {
        TcpOutputSendView {
            snd_una: self.snd_una,
            snd_nxt: self.snd_nxt,
            snd_wnd: self.snd_wnd,
            congestion_window: self.congestion.congestion_window(),
        }
    }

    #[inline]
    pub fn set_send_state(&mut self, snd_una: u32, snd_nxt: u32, snd_wnd: u32) {
        self.snd_una = snd_una;
        self.snd_nxt = snd_nxt;
        self.snd_wnd = snd_wnd;
    }

    #[inline]
    pub fn next_output_at(&self) -> Option<std::time::Instant> {
        self.next_output_at
    }

    #[inline]
    pub fn set_next_output_at(&mut self, deadline: Option<std::time::Instant>) {
        self.next_output_at = deadline;
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TcpReceiveProgress {
    pub(crate) state: Option<TcpState>,
    pub(crate) sequence: u32,
    pub(crate) acknowledgment: Option<u32>,
    pub(crate) advertised_window: u32,
    pub(crate) next_receive_sequence: Option<u32>,
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

    #[inline]
    pub(crate) fn apply_receive_progress(
        &mut self,
        lookup_id: TcpLookupId,
        progress: TcpReceiveProgress,
    ) {
        let Some(slot) = self.lookup_slots.lookup(&lookup_id) else {
            return;
        };
        let Some(connection) = self.connections.get_mut(slot) else {
            return;
        };
        if connection.irs == 0 && connection.rcv_nxt == 0 {
            connection.irs = progress.sequence.wrapping_sub(1);
            connection.rcv_nxt = progress.sequence;
        }
        if let Some(acknowledgment) = progress.acknowledgment {
            if connection.iss == 0 && connection.snd_una == 0 && connection.snd_nxt == 0 {
                connection.iss = acknowledgment.wrapping_sub(1);
            }
            connection.snd_una = tcp_seq_max(connection.snd_una, acknowledgment);
            connection.snd_nxt = tcp_seq_max(connection.snd_nxt, connection.snd_una);
            connection.snd_wnd = progress.advertised_window;
        }
        if let Some(next_receive_sequence) = progress.next_receive_sequence
            && connection.rcv_nxt == progress.sequence
        {
            connection.rcv_nxt = next_receive_sequence;
        }
        if let Some(state) = progress.state {
            connection.state = state;
        }
    }
}

impl Default for TcpConnectionSnapshotPool {
    #[inline]
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug, Clone)]
pub struct TcpConnectionTable {
    connections: InfraVec<TcpDataPlaneConnection>,
    lookup_slots: FlatHashTable<TcpLookupId, usize>,
    connection_slots: FlatHashTable<u64, usize>,
}

impl TcpConnectionTable {
    #[inline]
    pub fn empty() -> Self {
        Self {
            connections: InfraVec::new(),
            lookup_slots: FlatHashTable::new(),
            connection_slots: FlatHashTable::new(),
        }
    }

    #[inline]
    pub fn insert(&mut self, connection: TcpDataPlaneConnection) {
        let slot = self.connections.len();
        self.lookup_slots.insert(connection.lookup_id(), slot);
        if let Some(connection_id) = connection.connection_id() {
            self.connection_slots.insert(connection_id.get(), slot);
        }
        self.connections.push(connection);
    }

    #[inline]
    pub fn lookup_by_lookup_id(&self, lookup_id: TcpLookupId) -> Option<&TcpDataPlaneConnection> {
        self.lookup_slots
            .lookup(&lookup_id)
            .and_then(|slot| self.connections.get(slot))
    }

    #[inline]
    pub fn lookup_by_lookup_id_mut(
        &mut self,
        lookup_id: TcpLookupId,
    ) -> Option<&mut TcpDataPlaneConnection> {
        let slot = self.lookup_slots.lookup(&lookup_id)?;
        self.connections.get_mut(slot)
    }

    #[inline]
    pub fn lookup_by_connection_id(
        &self,
        connection_id: TcpConnectionId,
    ) -> Option<&TcpDataPlaneConnection> {
        self.connection_slots
            .lookup(&connection_id.get())
            .and_then(|slot| self.connections.get(slot))
    }

    #[inline]
    pub fn lookup_by_connection_id_mut(
        &mut self,
        connection_id: TcpConnectionId,
    ) -> Option<&mut TcpDataPlaneConnection> {
        let slot = self.connection_slots.lookup(&connection_id.get())?;
        self.connections.get_mut(slot)
    }
}

impl Default for TcpConnectionTable {
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

    #[inline]
    pub(crate) fn connection_state(&self, lookup_id: TcpLookupId) -> Option<TcpState> {
        self.load()
            .connections
            .lookup_by_lookup_id(lookup_id)
            .map(|connection| connection.state)
    }

    #[inline]
    pub(crate) fn connection(&self, lookup_id: TcpLookupId) -> Option<TcpConnectionSnapshot> {
        self.load().connections.lookup_by_lookup_id(lookup_id)
    }

    #[inline]
    pub(crate) fn apply_receive_progress(
        &self,
        lookup_id: TcpLookupId,
        progress: TcpReceiveProgress,
    ) {
        self.inner.rcu(|current| {
            let mut next = TcpEstablishedSnapshot::clone(current);
            next.connections.apply_receive_progress(lookup_id, progress);
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
    backend: Arc<RwLock<Option<Arc<dyn TcpEstablishedBackend>>>>,
    next: [NodeId; TcpEstablishedNext::COUNT],
}

impl TcpEstablishedControlPlane {
    #[inline]
    pub fn new(next: [NodeId; TcpEstablishedNext::COUNT]) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpEstablishedSnapshot::new())),
            backend: Arc::new(RwLock::new(None)),
            next,
        }
    }

    #[inline]
    pub fn with_backend<O>(self, backend: Arc<O>) -> Self
    where
        O: TcpEstablishedBackend + 'static,
    {
        self.install_backend(backend);
        self
    }

    #[inline]
    pub fn install_backend<O>(&self, backend: Arc<O>)
    where
        O: TcpEstablishedBackend + 'static,
    {
        *self
            .backend
            .write()
            .expect("tcp established backend lock poisoned") = Some(backend);
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
            self.backend
                .read()
                .expect("tcp established backend lock poisoned")
                .clone(),
        )
    }

    #[doc(hidden)]
    #[inline]
    pub fn connection_state_for_test(&self, lookup_id: TcpLookupId) -> Option<TcpState> {
        TcpEstablishedSnapshotHandle::new(Arc::clone(&self.inner)).connection_state(lookup_id)
    }

    #[doc(hidden)]
    #[inline]
    pub fn connection_snapshot_for_test(
        &self,
        lookup_id: TcpLookupId,
    ) -> Option<TcpConnectionSnapshot> {
        TcpEstablishedSnapshotHandle::new(Arc::clone(&self.inner)).connection(lookup_id)
    }
}

#[inline]
fn tcp_seq_max(current: u32, candidate: u32) -> u32 {
    if current == 0 || TcpSeq::new(current).before(TcpSeq::new(candidate)) {
        candidate
    } else {
        current
    }
}

pub(crate) fn default_established_snapshot_handle() -> TcpEstablishedSnapshotHandle {
    default_tcp_established_snapshot()
}

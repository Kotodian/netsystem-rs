use std::cell::RefCell;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{TcpHandshakeObservation, TcpSeq, TcpState};
use hammer_infra::{map::FlatHashTable, vec::Vec as InfraVec};

use super::{TcpLookupId, TcpV4PendingConnectionKey, TcpV6PendingConnectionKey};

#[hammer_component_macros::node_next]
pub enum TcpSynSentNext {
    Drop,
}

pub trait TcpSynSentBackend: Send + Sync {
    fn observe_syn_ack(&self, observation: TcpSynSentObservation) -> CoreResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpSynSentObservation {
    pub connection_id: TcpLookupId,
    pub remote: SocketAddr,
    pub local: SocketAddr,
    pub previous_state: TcpState,
    pub next_state: TcpState,
    pub transport: TcpHandshakeObservation,
}

impl TcpSynSentObservation {
    #[inline]
    pub fn new(
        connection_id: TcpLookupId,
        remote: SocketAddr,
        local: SocketAddr,
        previous_state: TcpState,
        next_state: TcpState,
        transport: TcpHandshakeObservation,
    ) -> Self {
        Self {
            connection_id,
            remote,
            local,
            previous_state,
            next_state,
            transport,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpSynSentRegistration {
    V4 {
        connection_id: TcpLookupId,
        key: TcpV4PendingConnectionKey,
    },
    V6 {
        connection_id: TcpLookupId,
        key: TcpV6PendingConnectionKey,
    },
}

impl TcpSynSentRegistration {
    #[inline]
    pub fn v4(connection_id: TcpLookupId, key: TcpV4PendingConnectionKey) -> Self {
        Self::V4 { connection_id, key }
    }

    #[inline]
    pub fn v6(connection_id: TcpLookupId, key: TcpV6PendingConnectionKey) -> Self {
        Self::V6 { connection_id, key }
    }
}

struct TcpSynSentBackendHandle {
    raw: *const (),
    clone_raw: fn(*const ()) -> *const (),
    drop_raw: fn(*const ()),
    observe: fn(*const (), TcpSynSentObservation) -> CoreResult<()>,
}

unsafe impl Send for TcpSynSentBackendHandle {}
unsafe impl Sync for TcpSynSentBackendHandle {}

impl Default for TcpSynSentBackendHandle {
    #[inline]
    fn default() -> Self {
        Self::noop()
    }
}

impl Clone for TcpSynSentBackendHandle {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            raw: (self.clone_raw)(self.raw),
            clone_raw: self.clone_raw,
            drop_raw: self.drop_raw,
            observe: self.observe,
        }
    }
}

impl Drop for TcpSynSentBackendHandle {
    #[inline]
    fn drop(&mut self) {
        (self.drop_raw)(self.raw);
    }
}

impl TcpSynSentBackendHandle {
    #[inline]
    fn noop() -> Self {
        Self {
            raw: std::ptr::null(),
            clone_raw: clone_noop_handle,
            drop_raw: drop_noop_handle,
            observe: observe_noop_syn_sent,
        }
    }

    #[inline]
    fn new<O>(backend: Arc<O>) -> Self
    where
        O: TcpSynSentBackend + 'static,
    {
        Self {
            raw: Arc::into_raw(backend) as *const (),
            clone_raw: clone_arc_handle::<O>,
            drop_raw: drop_arc_handle::<O>,
            observe: observe_syn_sent_with::<O>,
        }
    }

    #[inline]
    fn is_registered(&self) -> bool {
        !self.raw.is_null()
    }

    #[inline]
    fn observe_syn_ack(&self, observation: TcpSynSentObservation) -> CoreResult<()> {
        (self.observe)(self.raw, observation)
    }
}

#[inline]
fn clone_noop_handle(_raw: *const ()) -> *const () {
    std::ptr::null()
}

#[inline]
fn drop_noop_handle(_raw: *const ()) {}

#[inline]
fn observe_noop_syn_sent(_raw: *const (), _observation: TcpSynSentObservation) -> CoreResult<()> {
    Ok(())
}

#[inline]
fn clone_arc_handle<O>(raw: *const ()) -> *const ()
where
    O: TcpSynSentBackend + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            Arc::increment_strong_count(raw);
        }
    }
    raw.cast()
}

#[inline]
fn drop_arc_handle<O>(raw: *const ())
where
    O: TcpSynSentBackend + 'static,
{
    let raw = raw.cast::<O>();
    if !raw.is_null() {
        unsafe {
            drop(Arc::from_raw(raw));
        }
    }
}

#[inline]
fn observe_syn_sent_with<O>(raw: *const (), observation: TcpSynSentObservation) -> CoreResult<()>
where
    O: TcpSynSentBackend + 'static,
{
    let raw = raw.cast::<O>();
    if raw.is_null() {
        return Ok(());
    }
    unsafe { (&*raw).observe_syn_ack(observation) }
}

#[derive(Clone, Default)]
struct TcpSynSentRegistry {
    pending_v4: FlatHashTable<TcpV4PendingConnectionKey, TcpLookupId>,
    pending_v6: FlatHashTable<TcpV6PendingConnectionKey, TcpLookupId>,
}

impl TcpSynSentRegistry {
    #[inline]
    fn publish_connections(
        &mut self,
        connections: impl IntoIterator<Item = TcpSynSentRegistration>,
    ) {
        self.pending_v4 = FlatHashTable::new();
        self.pending_v6 = FlatHashTable::new();
        for connection in connections {
            match connection {
                TcpSynSentRegistration::V4 { connection_id, key } => {
                    self.pending_v4.insert(key, connection_id);
                }
                TcpSynSentRegistration::V6 { connection_id, key } => {
                    self.pending_v6.insert(key, connection_id);
                }
            }
        }
    }

    #[inline]
    fn lookup(&self, local: SocketAddr, remote: SocketAddr) -> Option<TcpLookupId> {
        match (local.ip(), remote.ip()) {
            (IpAddr::V4(_), IpAddr::V4(remote_addr)) => self.pending_v4.lookup(
                &TcpV4PendingConnectionKey::new(0, local.port(), remote_addr, remote.port()),
            ),
            (IpAddr::V6(_), IpAddr::V6(remote_addr)) => self.pending_v6.lookup(
                &TcpV6PendingConnectionKey::new(0, local.port(), remote_addr, remote.port()),
            ),
            _ => None,
        }
    }
}

#[derive(Clone)]
struct TcpSynSentSnapshot {
    registry: TcpSynSentRegistry,
}

impl TcpSynSentSnapshot {
    #[inline]
    fn new() -> Self {
        Self {
            registry: TcpSynSentRegistry::default(),
        }
    }
}

#[derive(Clone)]
struct TcpSynSentSnapshotHandle {
    inner: Arc<ArcSwap<TcpSynSentSnapshot>>,
}

impl TcpSynSentSnapshotHandle {
    #[inline]
    fn new(inner: Arc<ArcSwap<TcpSynSentSnapshot>>) -> Self {
        Self { inner }
    }

    #[inline]
    fn load(&self) -> arc_swap::Guard<Arc<TcpSynSentSnapshot>> {
        self.inner.load()
    }

    #[inline]
    fn publish_connections(&self, connections: impl IntoIterator<Item = TcpSynSentRegistration>) {
        let mut registry = TcpSynSentRegistry::default();
        registry.publish_connections(connections);
        self.inner.rcu(|current| {
            let mut next = TcpSynSentSnapshot::clone(current);
            next.registry = registry.clone();
            next
        });
    }
}

fn default_tcp_syn_sent_snapshot() -> TcpSynSentSnapshotHandle {
    TcpSynSentSnapshotHandle::new(Arc::new(ArcSwap::from_pointee(TcpSynSentSnapshot::new())))
}

pub struct TcpSynSentControlPlane {
    inner: Arc<ArcSwap<TcpSynSentSnapshot>>,
    backend: TcpSynSentBackendHandle,
    next: [NodeId; TcpSynSentNext::COUNT],
}

impl TcpSynSentControlPlane {
    #[inline]
    pub fn new<O>(backend: Arc<O>, next: [NodeId; TcpSynSentNext::COUNT]) -> Self
    where
        O: TcpSynSentBackend + 'static,
    {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpSynSentSnapshot::new())),
            backend: TcpSynSentBackendHandle::new(backend),
            next,
        }
    }

    #[inline]
    pub fn publish_connections(
        &self,
        connections: impl IntoIterator<Item = TcpSynSentRegistration>,
    ) -> CoreResult<()> {
        TcpSynSentSnapshotHandle::new(Arc::clone(&self.inner)).publish_connections(connections);
        Ok(())
    }

    #[inline]
    pub fn node(&self) -> TcpSynSentNode {
        TcpSynSentNode::new(self.next).with_runtime(
            TcpSynSentSnapshotHandle::new(Arc::clone(&self.inner)),
            self.backend.clone(),
        )
    }
}

#[derive(Clone)]
struct TcpSynSentRuntime {
    snapshot: TcpSynSentSnapshotHandle,
    backend: TcpSynSentBackendHandle,
}

thread_local! {
    static TCP_SYN_SENT_RUNTIMES: RefCell<InfraVec<TcpSynSentRuntime>> =
        const { RefCell::new(InfraVec::new()) };
}

#[inline]
fn has_tcp_syn_sent_runtime(data: NodeRuntimeData) -> bool {
    data.word(1) != 0
}

fn register_tcp_syn_sent_runtime(
    snapshot: TcpSynSentSnapshotHandle,
    backend: TcpSynSentBackendHandle,
) -> CoreResult<NodeRuntimeData> {
    TCP_SYN_SENT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let slot = runtimes.len();
        runtimes.push(TcpSynSentRuntime { snapshot, backend });
        Ok(NodeRuntimeData::from_words([
            u64::try_from(slot)
                .map_err(|_| CoreError::internal("TCP syn-sent runtime slot overflow"))?,
            1,
            0,
            0,
        ]))
    })
}

fn tcp_syn_sent_runtime(data: NodeRuntimeData) -> CoreResult<TcpSynSentRuntime> {
    if !has_tcp_syn_sent_runtime(data) {
        return Ok(TcpSynSentRuntime {
            snapshot: default_tcp_syn_sent_snapshot(),
            backend: TcpSynSentBackendHandle::default(),
        });
    }
    let slot = data.usize_word(0)?;
    TCP_SYN_SENT_RUNTIMES.with(|runtimes| {
        runtimes
            .borrow()
            .get(slot)
            .cloned()
            .ok_or_else(|| CoreError::internal("TCP syn-sent runtime slot is invalid"))
    })
}

fn sync_tcp_syn_sent_runtime(
    data: NodeRuntimeData,
    snapshot: TcpSynSentSnapshotHandle,
    backend: TcpSynSentBackendHandle,
) -> CoreResult<()> {
    if !has_tcp_syn_sent_runtime(data) {
        return Ok(());
    }
    let slot = data.usize_word(0)?;
    TCP_SYN_SENT_RUNTIMES.with(|runtimes| {
        let mut runtimes = runtimes.borrow_mut();
        let runtime = runtimes
            .get_mut(slot)
            .ok_or_else(|| CoreError::internal("TCP syn-sent runtime slot is invalid"))?;
        runtime.snapshot = snapshot;
        runtime.backend = backend;
        Ok(())
    })
}

#[hammer_component_macros::node(role = internal, next = TcpSynSentNext)]
pub struct TcpSynSentNode {
    #[node(default)]
    runtime_data: NodeRuntimeData,
    #[node(default = default_tcp_syn_sent_snapshot())]
    snapshot: TcpSynSentSnapshotHandle,
    #[node(default)]
    backend: TcpSynSentBackendHandle,
    #[node(default)]
    cached_next: Option<NodeId>,
}

impl TcpSynSentNode {
    #[inline]
    fn with_runtime(
        mut self,
        snapshot: TcpSynSentSnapshotHandle,
        backend: TcpSynSentBackendHandle,
    ) -> Self {
        if has_tcp_syn_sent_runtime(self.runtime_data) {
            let _ = sync_tcp_syn_sent_runtime(self.runtime_data, snapshot.clone(), backend.clone());
        } else if let Ok(runtime_data) =
            register_tcp_syn_sent_runtime(snapshot.clone(), backend.clone())
        {
            self.runtime_data = runtime_data;
        }
        self.snapshot = snapshot;
        self.backend = backend;
        self
    }
}

impl Node for TcpSynSentNode {
    #[inline(always)]
    fn process(
        &mut self,
        runtime: &DataPlaneRuntime,
        frame: &mut BufferFrame,
    ) -> CoreResult<NodeResult> {
        sync_tcp_syn_sent_runtime(
            self.runtime_data,
            self.snapshot.clone(),
            self.backend.clone(),
        )?;
        let snapshot = self.snapshot.load();
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpSynSentNext::Drop as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| {
                tcp_syn_sent_next_for_index(runtime, index, drop_next, &snapshot, &self.backend)
            },
        )?;
        self.cached_next = cached_next;
        Ok(result)
    }

    #[inline]
    fn node_process(&self) -> NodeProcessFn {
        tcp_syn_sent_process
    }

    #[inline]
    fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
        sync_tcp_syn_sent_runtime(
            self.runtime_data,
            self.snapshot.clone(),
            self.backend.clone(),
        )?;
        Ok(self.runtime_data)
    }
}

fn tcp_syn_sent_process(
    runtime: &DataPlaneRuntime,
    data: hammer_adapter::NodeRuntimeData,
    frame: &mut BufferFrame,
) -> CoreResult<NodeResult> {
    let state = tcp_syn_sent_runtime(data)?;
    let snapshot = state.snapshot.load();
    let next = TcpSynSentNode::runtime_nexts(runtime)?;
    let drop_next = next[TcpSynSentNext::Drop as usize];
    let (result, _) = NodeVectorDispatch::new(None).route_frame_index(runtime, frame, |index| {
        tcp_syn_sent_next_for_index(runtime, index, drop_next, &snapshot, &state.backend)
    })?;
    Ok(result)
}

fn tcp_syn_sent_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: NodeId,
    snapshot: &TcpSynSentSnapshot,
    backend: &TcpSynSentBackendHandle,
) -> CoreResult<Option<NodeId>> {
    if backend.is_registered() {
        if let Some(observation) =
            syn_sent_observation_for_index(runtime, index, &snapshot.registry)?
        {
            backend.observe_syn_ack(observation)?;
        }
    }
    Ok(Some(drop_next))
}

fn syn_sent_observation_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    registry: &TcpSynSentRegistry,
) -> CoreResult<Option<TcpSynSentObservation>> {
    let transport = tcp_handshake_observation(runtime, index)?;
    if !packet_is_syn_ack(transport) {
        return Ok(None);
    }
    let metadata = runtime.metadata(index)?;
    let Some(remote) = metadata.source else {
        return Ok(None);
    };
    let Some(local) = metadata.destination else {
        return Ok(None);
    };
    let remote = SocketAddr::new(remote.host, remote.port);
    let local = SocketAddr::new(local.host, local.port);
    let Some(connection_id) = registry.lookup(local, remote) else {
        return Ok(None);
    };
    Ok(Some(TcpSynSentObservation::new(
        connection_id,
        remote,
        local,
        TcpState::SynSent,
        TcpState::Established,
        transport,
    )))
}

fn packet_is_syn_ack(observation: TcpHandshakeObservation) -> bool {
    observation.syn()
        && observation.ack()
        && observation.acknowledgment.is_some()
        && !observation.fin()
        && observation.flags & 0x04 == 0
}

fn tcp_handshake_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpHandshakeObservation> {
    let cursor = runtime.get_buffer(index)?.packet_cursor();
    let packet: std::vec::Vec<u8> = runtime.copy_current_chain(index)?.into_iter().collect();
    let sequence_offset = cursor.transport_header_offset() + 4;
    let acknowledgment_offset = cursor.transport_header_offset() + 8;
    let flags_offset = cursor.transport_header_offset() + 13;
    let window_offset = cursor.transport_header_offset() + 14;
    let flags = packet.get(flags_offset).copied().unwrap_or_default();
    let sequence = packet
        .get(sequence_offset..sequence_offset + 4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("sequence bytes")))
        .unwrap_or_default();
    let acknowledgment = packet
        .get(acknowledgment_offset..acknowledgment_offset + 4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("ack bytes")))
        .filter(|_| flags & 0x10 != 0);
    let advertised_window = packet
        .get(window_offset..window_offset + 2)
        .map(|bytes| u16::from_be_bytes(bytes.try_into().expect("window bytes")) as u32)
        .unwrap_or_default();
    let payload_len = (cursor
        .packet_len()
        .saturating_sub(cursor.transport_payload_offset())) as u32;
    let next_sequence = TcpSeq::new(sequence)
        .advance(payload_len + u32::from(flags & 0x02 != 0) + u32::from(flags & 0x01 != 0))
        .raw();
    Ok(TcpHandshakeObservation::new(
        flags,
        sequence,
        acknowledgment,
        advertised_window,
        next_sequence,
    ))
}

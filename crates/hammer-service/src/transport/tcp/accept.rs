use std::cell::RefCell;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use arc_swap::ArcSwap;
use hammer_adapter::{
    BufferFrame, BufferIndex, DataPlaneRuntime, Node, NodeId, NodeProcessFn, NodeResult,
    NodeRuntimeData, NodeVectorDispatch,
};
use hammer_core::error::{CoreError, CoreResult};
use hammer_core::protocol::tcp::{
    TcpCapabilities, TcpConnectionKey, TcpHandshakeObservation, TcpListenerId, TcpListenerKey,
    TcpSeq, TcpWorkerEvent,
};
use hammer_infra::{map::FlatHashTable, vec::Vec as InfraVec};
use hammer_runtime::app::{AppContext, AppSocketId};

use super::TcpLookupId;
use super::input::take_pending_tcp_accept;
use super::options::tcp_capabilities_from_options;

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
        event: TcpWorkerEvent,
    ) -> CoreResult<()>;

    #[inline]
    fn observe_accept(
        &self,
        listener_id: TcpLookupId,
        registration: &TcpAcceptRegistration,
        remote: SocketAddr,
        local: SocketAddr,
        event: TcpWorkerEvent,
        _observation: TcpHandshakeObservation,
    ) -> CoreResult<()> {
        self.accept(listener_id, registration, remote, local, event)
    }
}

struct TcpAcceptBackendHandle {
    raw: *const (),
    clone_raw: fn(*const ()) -> *const (),
    drop_raw: fn(*const ()),
    accept: fn(
        *const (),
        TcpLookupId,
        &TcpAcceptRegistration,
        SocketAddr,
        SocketAddr,
        TcpWorkerEvent,
        TcpHandshakeObservation,
    ) -> CoreResult<()>,
}

unsafe impl Send for TcpAcceptBackendHandle {}
unsafe impl Sync for TcpAcceptBackendHandle {}

impl Clone for TcpAcceptBackendHandle {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            raw: (self.clone_raw)(self.raw),
            clone_raw: self.clone_raw,
            drop_raw: self.drop_raw,
            accept: self.accept,
        }
    }
}

impl Drop for TcpAcceptBackendHandle {
    #[inline]
    fn drop(&mut self) {
        (self.drop_raw)(self.raw);
    }
}

impl TcpAcceptBackendHandle {
    #[inline]
    fn new<O>(backend: Arc<O>) -> Self
    where
        O: TcpAcceptBackend + 'static,
    {
        Self {
            raw: Arc::into_raw(backend) as *const (),
            clone_raw: clone_tcp_accept_arc_handle::<O>,
            drop_raw: drop_tcp_accept_arc_handle::<O>,
            accept: accept_with::<O>,
        }
    }

    #[inline]
    fn accept(
        &self,
        listener_id: TcpLookupId,
        registration: &TcpAcceptRegistration,
        remote: SocketAddr,
        local: SocketAddr,
        event: TcpWorkerEvent,
        observation: TcpHandshakeObservation,
    ) -> CoreResult<()> {
        (self.accept)(
            self.raw,
            listener_id,
            registration,
            remote,
            local,
            event,
            observation,
        )
    }
}

#[inline]
fn clone_tcp_accept_arc_handle<O>(raw: *const ()) -> *const ()
where
    O: TcpAcceptBackend + 'static,
{
    let raw = raw.cast::<O>();
    unsafe {
        Arc::increment_strong_count(raw);
    }
    raw.cast()
}

#[inline]
fn drop_tcp_accept_arc_handle<O>(raw: *const ())
where
    O: TcpAcceptBackend + 'static,
{
    unsafe {
        drop(Arc::from_raw(raw.cast::<O>()));
    }
}

#[inline]
fn accept_with<O>(
    raw: *const (),
    listener_id: TcpLookupId,
    registration: &TcpAcceptRegistration,
    remote: SocketAddr,
    local: SocketAddr,
    event: TcpWorkerEvent,
    observation: TcpHandshakeObservation,
) -> CoreResult<()>
where
    O: TcpAcceptBackend + 'static,
{
    unsafe {
        (&*raw.cast::<O>()).observe_accept(
            listener_id,
            registration,
            remote,
            local,
            event,
            observation,
        )
    }
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
    backend: TcpAcceptBackendHandle,
    next: [NodeId; TcpAcceptNext::COUNT],
}

impl TcpAcceptControlPlane {
    #[inline]
    pub fn new<O>(backend: Arc<O>, next: [NodeId; TcpAcceptNext::COUNT]) -> Self
    where
        O: TcpAcceptBackend + 'static,
    {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(TcpAcceptSnapshot::new())),
            backend: TcpAcceptBackendHandle::new(backend),
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
                self.backend.clone(),
            ),
            TcpAcceptSnapshotHandle::new(Arc::clone(&self.inner)),
            self.backend.clone(),
            self.next,
        )
    }
}

#[derive(Clone)]
struct TcpAcceptRuntime {
    snapshot: TcpAcceptSnapshotHandle,
    backend: TcpAcceptBackendHandle,
}

thread_local! {
    static TCP_ACCEPT_RUNTIMES: RefCell<InfraVec<TcpAcceptRuntime>> =
        const { RefCell::new(InfraVec::new()) };
}

fn register_tcp_accept_runtime(
    snapshot: TcpAcceptSnapshotHandle,
    backend: TcpAcceptBackendHandle,
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
    backend: TcpAcceptBackendHandle,
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
    backend: TcpAcceptBackendHandle,
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
            self.backend.clone(),
        )?;
        let snapshot = self.snapshot.load();
        let next = Self::runtime_nexts(runtime)?;
        let drop_next = next[TcpAcceptNext::Drop as usize];
        let (result, cached_next) = NodeVectorDispatch::new(self.cached_next).route_frame_index(
            runtime,
            frame,
            |index| tcp_accept_next_for_index(runtime, index, drop_next, &snapshot, &self.backend),
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
            self.backend.clone(),
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
        tcp_accept_next_for_index(runtime, index, drop_next, &snapshot, &state.backend)
    })?;
    Ok(result)
}

fn tcp_accept_next_for_index(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
    drop_next: NodeId,
    snapshot: &TcpAcceptSnapshot,
    backend: &TcpAcceptBackendHandle,
) -> CoreResult<Option<NodeId>> {
    let Some(listener_id) = take_pending_tcp_accept(index)? else {
        return Ok(Some(drop_next));
    };
    let Some(registration) = snapshot.registry.get(&listener_id) else {
        return Ok(Some(drop_next));
    };
    let (remote, local) = tcp_accept_socket_addrs(runtime, index)?;
    let event = incoming_connection_event(listener_id, remote, local)?;
    backend.accept(
        listener_id,
        registration,
        remote,
        local,
        event,
        tcp_handshake_observation(runtime, index)?,
    )?;
    Ok(Some(drop_next))
}

fn tcp_handshake_observation(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<TcpHandshakeObservation> {
    let buffer = runtime.get_buffer(index)?;
    let cursor = buffer.packet_cursor();
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
    let capabilities = packet
        .get(cursor.transport_header_offset() + 20..cursor.transport_payload_offset())
        .map(tcp_capabilities_from_options)
        .unwrap_or_default();
    Ok(TcpHandshakeObservation::new(
        flags,
        sequence,
        acknowledgment,
        advertised_window,
        next_sequence,
    )
    .with_capabilities(capabilities))
}

fn tcp_accept_socket_addrs(
    runtime: &DataPlaneRuntime,
    index: BufferIndex,
) -> CoreResult<(SocketAddr, SocketAddr)> {
    let metadata = runtime.metadata(index)?;
    let buffer = runtime.get_buffer(index)?;
    let packet = buffer.current();
    let cursor = buffer.packet_cursor();
    let (packet_remote_ip, packet_local_ip) = tcp_accept_ips_from_packet(packet)?;
    let ports = tcp_accept_ports_from_packet(packet, cursor.transport_header_offset())?;
    let remote = metadata
        .source
        .map(|addr| SocketAddr::new(addr.host, addr.port))
        .unwrap_or_else(|| SocketAddr::new(packet_remote_ip, ports.0));
    let local = metadata
        .destination
        .map(|addr| SocketAddr::new(addr.host, addr.port))
        .unwrap_or_else(|| SocketAddr::new(packet_local_ip, ports.1));
    Ok((remote, local))
}

fn tcp_accept_ips_from_packet(packet: &[u8]) -> CoreResult<(IpAddr, IpAddr)> {
    match packet.first().map(|byte| byte >> 4) {
        Some(4) => {
            let remote = packet
                .get(12..16)
                .ok_or_else(|| CoreError::internal("tcp accept missing IPv4 source"))?;
            let local = packet
                .get(16..20)
                .ok_or_else(|| CoreError::internal("tcp accept missing IPv4 destination"))?;
            Ok((
                IpAddr::V4(Ipv4Addr::new(remote[0], remote[1], remote[2], remote[3])),
                IpAddr::V4(Ipv4Addr::new(local[0], local[1], local[2], local[3])),
            ))
        }
        Some(6) => {
            let remote: [u8; 16] = packet
                .get(8..24)
                .ok_or_else(|| CoreError::internal("tcp accept missing IPv6 source"))?
                .try_into()
                .map_err(|_| CoreError::internal("tcp accept invalid IPv6 source"))?;
            let local: [u8; 16] = packet
                .get(24..40)
                .ok_or_else(|| CoreError::internal("tcp accept missing IPv6 destination"))?
                .try_into()
                .map_err(|_| CoreError::internal("tcp accept invalid IPv6 destination"))?;
            Ok((
                IpAddr::V6(Ipv6Addr::from(remote)),
                IpAddr::V6(Ipv6Addr::from(local)),
            ))
        }
        Some(version) => Err(CoreError::internal(format!(
            "tcp accept does not support IP version {version}"
        ))),
        None => Err(CoreError::internal("tcp accept requires packet bytes")),
    }
}

fn tcp_accept_ports_from_packet(
    packet: &[u8],
    transport_header_offset: usize,
) -> CoreResult<(u16, u16)> {
    let ports = packet
        .get(transport_header_offset..transport_header_offset + 4)
        .ok_or_else(|| CoreError::internal("tcp accept missing TCP ports"))?;
    Ok((
        u16::from_be_bytes([ports[0], ports[1]]),
        u16::from_be_bytes([ports[2], ports[3]]),
    ))
}

fn incoming_connection_event(
    listener_id: TcpLookupId,
    remote: SocketAddr,
    local: SocketAddr,
) -> CoreResult<TcpWorkerEvent> {
    let listener = match local.ip() {
        std::net::IpAddr::V4(local_ip) => TcpListenerKey::v4(0, local_ip, local.port()),
        std::net::IpAddr::V6(local_ip) => TcpListenerKey::v6(0, local_ip, local.port()),
    };
    let key = match (local.ip(), remote.ip()) {
        (std::net::IpAddr::V4(local_ip), std::net::IpAddr::V4(remote_ip)) => {
            TcpConnectionKey::v4(0, local_ip, local.port(), remote_ip, remote.port())
        }
        (std::net::IpAddr::V6(local_ip), std::net::IpAddr::V6(remote_ip)) => {
            TcpConnectionKey::v6(0, local_ip, local.port(), remote_ip, remote.port())
        }
        _ => {
            return Err(CoreError::internal(format!(
                "tcp accept requires matching IP versions: local={local} remote={remote}"
            )));
        }
    };
    Ok(TcpWorkerEvent::IncomingConnection {
        listener_id: TcpListenerId::new(listener_id as u64),
        listener,
        key,
        capabilities: TcpCapabilities::default(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use hammer_runtime::spawn::DataRuntime;

    #[derive(Clone)]
    struct CountingAcceptBackend {
        accepted: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for CountingAcceptBackend {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl TcpAcceptBackend for CountingAcceptBackend {
        fn accept(
            &self,
            _listener_id: TcpLookupId,
            _registration: &TcpAcceptRegistration,
            _remote: SocketAddr,
            _local: SocketAddr,
            _event: TcpWorkerEvent,
        ) -> CoreResult<()> {
            self.accepted.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn tcp_accept_backend_handle_clones_forwards_and_drops_once() {
        let data_runtime = DataRuntime::new(1, "tcp-accept-backend-handle-test", 512 * 1024, 2)
            .expect("data runtime");
        let accepted = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let backend = Arc::new(CountingAcceptBackend {
            accepted: Arc::clone(&accepted),
            dropped: Arc::clone(&dropped),
        });
        let registration = TcpAcceptRegistration::new(
            AppContext::with_ring_capacity(data_runtime.context(), 1),
            AppSocketId::new(9),
        );

        let handle = TcpAcceptBackendHandle::new(Arc::clone(&backend));
        let cloned = handle.clone();
        drop(handle);
        drop(backend);

        assert_eq!(accepted.load(Ordering::Relaxed), 0);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);

        cloned
            .accept(
                7,
                &registration,
                "198.51.100.7:40007".parse().expect("remote"),
                "192.0.2.7:7443".parse().expect("local"),
                incoming_connection_event(
                    7,
                    "198.51.100.7:40007".parse().expect("remote"),
                    "192.0.2.7:7443".parse().expect("local"),
                )
                .expect("incoming connection event"),
                TcpHandshakeObservation::new(0, 0, None, 0, 0),
            )
            .expect("forward accept");

        assert_eq!(accepted.load(Ordering::Relaxed), 1);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);

        drop(cloned);
        assert_eq!(dropped.load(Ordering::Relaxed), 1);

        data_runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    }
}

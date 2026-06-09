use std::cell::UnsafeCell;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use hammer_adapter::DataWorkerId;
use hammer_control::{
    CertificateProviderManager, CertificateStore, ConnectionManager, NetworkManager, PauseManager,
    ServiceManager,
};
use hammer_core::config::{self, Options, TraceOptions};
use hammer_core::error::{CoreError, HammerError, HammerResult};
use hammer_core::lifecycle::{ALL_STAGES, LIFECYCLE_ORDER};
use hammer_core::log::{DiscardWriter, Factory, LogWriter, Logger};
use hammer_core::protocol::tcp::{
    TcpConnectionId, TcpConnectionKey, TcpListenerId, TcpWorkerEvent,
};
use hammer_core::registry::RuntimeRegistry;
use hammer_infra::{
    descriptor::{Descriptor, DescriptorTable},
    map::FlatHashTable,
    vec::Vec as InfraVec,
};
#[cfg(feature = "endpoint")]
use hammer_runtime::EndpointManager;
#[cfg(test)]
use hammer_runtime::adapter::NodeErrorCounters;
#[cfg(feature = "probe")]
use hammer_runtime::adapter::ProbeProtocolComponent;
use hammer_runtime::adapter::node::NodeRuntimeStatsRow;
use hammer_runtime::adapter::{
    DnsRouter as AdapterDnsRouter, Lifecycle, NetworkManager as _, NodeId, OutboundManager as _,
    PlatformInterface, ProbeReport, TraceControlPlane, TraceRecordSink,
};
#[cfg(feature = "endpoint")]
use hammer_runtime::adapter::{EndpointManager as _, InboundManager as _};
use hammer_runtime::app::{AppContext, AppControl, AppControlBackend, AppFlowId, AppSocketId};
#[cfg(feature = "endpoint")]
use hammer_runtime::endpoints::EndpointOutboundAdapter;
#[cfg(feature = "endpoint")]
use hammer_runtime::inbounds::RuntimeDnsRouter;
use hammer_runtime::protocol::tcp::TcpControlPlane as SharedTcpControlPlane;
use hammer_runtime::spawn::{DataPlaneExecutor, DataRuntime, DataRuntimeContext};
use hammer_runtime::{
    ControlEventSubscriptionHandle, ControlThread, ControlThreadHandle, ControlTimerHandle,
    EventSubscriberBuilder, InboundManager, MetricSample, MetricsRegistry, OutboundManager,
};
use std::time::Duration;

#[cfg(feature = "probe")]
use crate::ProbeManager;
use crate::app::{AppHost, AppIngressTarget};
use crate::transport::tcp::{
    TcpAcceptControlPlane, TcpAcceptNext, TcpAcceptRegistration, TcpCongestionRegistry,
    TcpConnectionState, TcpInputControlPlane, TcpInputNext, TcpListenerConfig, TcpLookupId,
    TcpLookupKind, TcpLookupSnapshot, TcpLookupValue, TcpRcvProcessControlPlane, TcpRcvProcessNext,
    TcpSynSentBackend, TcpSynSentControlPlane, TcpSynSentNext, TcpSynSentObservation,
    TcpSynSentRegistration, TcpV4ConnectionKey, TcpV4ListenerKey, TcpV4PendingConnectionKey,
    TcpV6ConnectionKey, TcpV6ListenerKey, TcpV6PendingConnectionKey,
};
use crate::transport::udp::input::UdpAppRegistration;
use crate::transport::udp::{UdpInputControlPlane, UdpInputNext};
use crate::{DnsRouter, DnsTransportManager, Router};

const CONTROL_THREAD_STACK_SIZE: usize = 512 * 1024;
const DATA_WORKER_THREADS: usize = 2;
const DATA_WORKER_STACK_SIZE: usize = 512 * 1024;
const DATA_MAX_BLOCKING_THREADS: usize = 4;
const METRICS_LOG_INTERVAL: Duration = Duration::from_secs(30);
const TRACE_DRAIN_INTERVAL: Duration = Duration::from_secs(1);
/// Time budget for the control thread to drain queued logs and emit a
/// final metrics dump on shutdown. 500ms was too tight when the log queue
/// (4096 entries) is full and each iOS write costs tens of milliseconds;
/// 2s leaves headroom for the worst-case drain without making `close()`
/// feel stuck on the FFI side.
const CONTROL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Time budget the data-plane runtime gets to abort in-flight tasks
/// during `close()`. Data tasks (TCP/UDP forwarders, DNS, probes) are
/// expected to drop fast once their lifecycles are closed; tasks still
/// running past this deadline are forcibly aborted by the runtime.
const DATA_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
/// Slack added on top of an inner async timeout when bouncing it through
/// the control thread, so the inner future has a chance to time out
/// cleanly and report a result before the outer wait gives up.
const CONTROL_ASYNC_BUFFER: Duration = Duration::from_secs(5);

fn debug_assert_lifecycle_order(lifecycles: &[Arc<dyn Lifecycle>]) {
    let mut previous = None;
    for lifecycle in lifecycles {
        let name = lifecycle.name();
        let Some(index) = LIFECYCLE_ORDER
            .iter()
            .position(|expected| *expected == name)
        else {
            debug_assert!(false, "unknown lifecycle '{name}'");
            continue;
        };
        if let Some(previous) = previous {
            debug_assert!(
                previous <= index,
                "Service lifecycles must follow LIFECYCLE_ORDER"
            );
        }
        previous = Some(index);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServiceState {
    NotStarted,
    Running,
    Closed,
}

pub struct RuntimeService {
    inner: Arc<Mutex<ServiceInner>>,
}

struct ServiceInner {
    state: ServiceState,
    log_factory: Arc<Factory>,
    control_handle: Option<Arc<ControlThreadHandle>>,
    control_thread: Option<JoinHandle<()>>,
    trace_drain_timer: Option<ControlTimerHandle>,
    runtime_dump_timer: Option<ControlTimerHandle>,
    event_subscriptions: Vec<ControlEventSubscriptionHandle>,
    metrics: Arc<MetricsRegistry>,
    _trace: TraceControlPlane,
    _platform: Arc<dyn PlatformInterface>,
    _registry: Arc<RuntimeRegistry>,
    lifecycles: Vec<Arc<dyn Lifecycle>>,
    pause: Arc<PauseManager>,
    network: Arc<NetworkManager>,
    dns_router: Arc<DnsRouter>,
    outbound: Arc<OutboundManager>,
    #[cfg(feature = "endpoint")]
    endpoint: Arc<EndpointManager>,
    #[cfg(feature = "probe")]
    probe: Arc<ProbeManager>,
    /// Data-plane runtime that hosts every business future spawned via
    /// `hammer_runtime::spawn::spawn`. Held in `Option` so `finish_close` can
    /// `take()` and consume it via `Runtime::shutdown_timeout` to bound
    /// the abort latency of in-flight tasks. If `None`, shutdown has
    /// already happened or the runtime was never installed.
    data_runtime: Option<DataRuntime>,
    data_context: DataRuntimeContext,
    app_context: AppContext,
    #[cfg_attr(not(test), allow(dead_code))]
    app_control: RuntimeAppControlHandle,
    _options: Options,
}

#[derive(Clone)]
struct TcpListenerRegistration {
    app: AppContext,
    socket: AppSocketId,
    lookup_id: TcpLookupId,
    owner_worker: DataWorkerId,
    bind: SocketAddr,
}

#[derive(Debug, Clone, Copy)]
struct UdpSocketRegistration {
    socket: AppSocketId,
    bind: SocketAddr,
}

#[derive(Clone)]
struct TcpConnectionRegistration {
    #[allow(dead_code)]
    flow: AppFlowId,
    connection_id: Option<TcpConnectionId>,
    lookup_id: TcpLookupId,
    owner_worker: DataWorkerId,
    state: hammer_core::protocol::tcp::TcpState,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    target: AppIngressTarget,
}

enum RuntimeSocketTag {}

type RuntimeSocketDescriptor = Descriptor<RuntimeSocketTag>;

enum RuntimeFlowTag {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeSocketKind {
    TcpListener,
    UdpSocket,
}

struct RuntimeAppControlState {
    sockets: DescriptorTable<RuntimeSocketKind, RuntimeSocketTag>,
    flows: DescriptorTable<(), RuntimeFlowTag>,
    next_tcp_lookup_id: TcpLookupId,
    next_tcp_ephemeral_port: u16,
    tcp_congestion: TcpCongestionRegistry,
    tcp_accept_control: Option<TcpAcceptControlPlane>,
    tcp_syn_sent_control: Option<TcpSynSentControlPlane>,
    shared_tcp_control: Option<SharedTcpControlPlane>,
    tcp_control: TcpInputControlPlane,
    tcp_rcv_process_control: TcpRcvProcessControlPlane,
    udp_control: UdpInputControlPlane,
    tcp_lookup: TcpLookupSnapshot,
    tcp_listeners: InfraVec<TcpListenerRegistration>,
    tcp_listener_slots: FlatHashTable<u64, usize>,
    tcp_connections: InfraVec<TcpConnectionRegistration>,
    udp_sockets: InfraVec<UdpSocketRegistration>,
    udp_socket_slots: FlatHashTable<u64, usize>,
}

struct RuntimeAppControlCell {
    inner: UnsafeCell<RuntimeAppControlState>,
}

impl RuntimeAppControlCell {
    #[inline]
    fn new(state: RuntimeAppControlState) -> Self {
        Self {
            inner: UnsafeCell::new(state),
        }
    }

    #[inline]
    unsafe fn get_mut(&self) -> &mut RuntimeAppControlState {
        unsafe { &mut *self.inner.get() }
    }
}

// SAFETY: access is serialized by RuntimeAppControlHandle through the single
// control thread. The cell is never mutated concurrently from multiple threads.
unsafe impl Send for RuntimeAppControlCell {}
// SAFETY: shared references may cross threads, but all dereferences route
// through the control-thread serialization contract above.
unsafe impl Sync for RuntimeAppControlCell {}

#[derive(Clone)]
struct RuntimeAppControlHandle {
    control_handle: Arc<ControlThreadHandle>,
    state: Arc<RuntimeAppControlCell>,
}

#[cfg(test)]
#[derive(Clone)]
struct RuntimeAppControlSnapshot {
    tcp_listeners: InfraVec<RuntimeTcpListenerSnapshot>,
    udp_sockets: InfraVec<UdpSocketRegistration>,
    tcp_lookup: TcpLookupSnapshot,
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct RuntimeTcpListenerSnapshot {
    socket: AppSocketId,
    lookup_id: TcpLookupId,
}

struct BindTcpListenerResult {
    socket: AppSocketId,
    shared_action: Option<hammer_core::protocol::tcp::TcpControlPlaneAction>,
}

struct CloseSocketResult {
    shared_action: Option<hammer_core::protocol::tcp::TcpControlPlaneAction>,
}

impl RuntimeAppControlState {
    fn new() -> HammerResult<Self> {
        let tcp_control = TcpInputControlPlane::new(TcpInputNext::nodes(
            unused_node_id(),
            unused_node_id(),
            unused_node_id(),
            unused_node_id(),
            unused_node_id(),
            unused_node_id(),
            unused_node_id(),
        ));
        tcp_control
            .publish_dispatch(crate::transport::tcp::TcpDispatchTable::default())
            .map_err(|err| {
                HammerError::internal(format!("publish tcp dispatch defaults: {err}"))
            })?;
        let tcp_rcv_process_control =
            TcpRcvProcessControlPlane::new(TcpRcvProcessNext::nodes(unused_node_id()));

        Ok(Self {
            sockets: DescriptorTable::new(),
            flows: DescriptorTable::new(),
            next_tcp_lookup_id: 1,
            next_tcp_ephemeral_port: 49_152,
            tcp_congestion: TcpCongestionRegistry::default(),
            tcp_accept_control: None,
            tcp_syn_sent_control: None,
            shared_tcp_control: None,
            tcp_control,
            tcp_rcv_process_control,
            udp_control: UdpInputControlPlane::new(UdpInputNext::nodes(
                unused_node_id(),
                unused_node_id(),
                unused_node_id(),
            )),
            tcp_lookup: TcpLookupSnapshot::empty(),
            tcp_listeners: InfraVec::new(),
            tcp_listener_slots: FlatHashTable::new(),
            tcp_connections: InfraVec::new(),
            udp_sockets: InfraVec::new(),
            udp_socket_slots: FlatHashTable::new(),
        })
    }

    fn install_tcp_accept_control(&mut self, control: TcpAcceptControlPlane) -> HammerResult<()> {
        self.tcp_accept_control = Some(control);
        self.publish_tcp_accept()
    }

    fn install_tcp_syn_sent_control(
        &mut self,
        control: TcpSynSentControlPlane,
    ) -> HammerResult<()> {
        self.tcp_syn_sent_control = Some(control);
        self.publish_tcp_lookup()
    }

    fn install_shared_tcp_control(&mut self, control: SharedTcpControlPlane) {
        self.shared_tcp_control = Some(control);
    }

    fn bind_tcp_listener(
        &mut self,
        app: &AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<BindTcpListenerResult> {
        if self
            .tcp_listeners
            .iter()
            .any(|registration| registration.bind == bind)
        {
            return Err(HammerError::internal(format!(
                "tcp listener {bind} is already registered"
            )));
        }

        let socket = self.alloc_socket(RuntimeSocketKind::TcpListener);
        let lookup_id = self.alloc_tcp_lookup_id()?;
        let listener_id = TcpListenerId::new(lookup_id as u64);
        let listener_key = socket_addr_to_listener_key(bind)?;
        let install_action = TcpListenerConfig::new().install_listener_action(
            &self.tcp_congestion,
            listener_id,
            listener_key,
        )?;
        self.tcp_listeners.push(TcpListenerRegistration {
            app: app.clone(),
            socket,
            lookup_id,
            owner_worker: worker_id(owner_worker)?,
            bind,
        });
        self.rebuild_tcp_listener_slots();
        self.publish_tcp_lookup()?;
        self.publish_tcp_accept()?;

        Ok(BindTcpListenerResult {
            socket,
            shared_action: self.shared_tcp_control.as_ref().map(|_| install_action),
        })
    }

    fn bind_udp_socket(
        &mut self,
        app: &AppContext,
        bind: SocketAddr,
        _owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        if self
            .udp_sockets
            .iter()
            .any(|registration| registration.bind.port() == bind.port())
        {
            return Err(HammerError::internal(format!(
                "udp socket port {} is already registered",
                bind.port()
            )));
        }

        let socket = self.alloc_socket(RuntimeSocketKind::UdpSocket);
        self.udp_control
            .register_app(bind.port(), UdpAppRegistration::socket(app.clone(), socket))
            .map_err(|err| HammerError::internal(format!("register udp app socket: {err}")))?;
        self.udp_sockets
            .push(UdpSocketRegistration { socket, bind });
        self.rebuild_udp_socket_slots();
        Ok(socket)
    }

    fn close_socket(&mut self, socket: AppSocketId) -> HammerResult<CloseSocketResult> {
        let descriptor = RuntimeSocketDescriptor::new(socket.value());
        let kind = self.sockets.remove(descriptor).ok_or_else(|| {
            HammerError::internal(format!(
                "app socket {} is not registered in runtime service",
                socket.value()
            ))
        })?;
        match kind {
            RuntimeSocketKind::TcpListener => {
                let slot = self
                    .tcp_listener_slots
                    .lookup(&socket.value())
                    .ok_or_else(|| {
                        HammerError::internal(format!(
                            "tcp listener {} missing listener slot entry",
                            socket.value()
                        ))
                    })?;
                let removed = self
                    .tcp_listeners
                    .drain(slot..slot + 1)
                    .next()
                    .expect("tcp listener exists at computed slot");
                self.rebuild_tcp_listener_slots();
                self.publish_tcp_lookup()?;
                self.publish_tcp_accept()?;
                Ok(CloseSocketResult {
                    shared_action: self.shared_tcp_control.as_ref().map(|_| {
                        hammer_core::protocol::tcp::TcpControlPlaneAction::RemoveListener {
                            listener_id: TcpListenerId::new(removed.lookup_id as u64),
                            reason: hammer_core::protocol::tcp::TcpCloseReason::LocalRequest,
                        }
                    }),
                })
            }
            RuntimeSocketKind::UdpSocket => {
                let slot = self
                    .udp_socket_slots
                    .lookup(&socket.value())
                    .ok_or_else(|| {
                        HammerError::internal(format!(
                            "udp socket {} missing socket slot entry",
                            socket.value()
                        ))
                    })?;
                let registration = self
                    .udp_sockets
                    .drain(slot..slot + 1)
                    .next()
                    .expect("udp socket exists at computed slot");
                self.udp_control
                    .unregister_port(registration.bind.port())
                    .map_err(|err| {
                        HammerError::internal(format!("unregister udp app socket: {err}"))
                    })?;
                self.rebuild_udp_socket_slots();
                Ok(CloseSocketResult {
                    shared_action: None,
                })
            }
        }
    }

    fn alloc_socket(&mut self, kind: RuntimeSocketKind) -> AppSocketId {
        AppSocketId::new(self.sockets.insert(kind).value())
    }

    fn alloc_flow(&mut self) -> AppFlowId {
        AppFlowId::new(self.flows.insert(()).value())
    }

    fn alloc_tcp_lookup_id(&mut self) -> HammerResult<TcpLookupId> {
        let id = self.next_tcp_lookup_id;
        self.next_tcp_lookup_id = self
            .next_tcp_lookup_id
            .checked_add(1)
            .ok_or_else(|| HammerError::internal("tcp lookup id overflow"))?;
        Ok(id)
    }

    fn accept_tcp_connection(
        &mut self,
        app: &AppContext,
        listener: AppSocketId,
        remote: SocketAddr,
        local: SocketAddr,
    ) -> HammerResult<AppFlowId> {
        let listener_slot = self
            .tcp_listener_slots
            .lookup(&listener.value())
            .ok_or_else(|| {
                HammerError::internal(format!(
                    "tcp listener {} is not registered in runtime service",
                    listener.value()
                ))
            })?;
        let listener_registration = self.tcp_listeners[listener_slot].clone();
        self.ensure_connection_absent(local, remote)?;
        let flow = self.alloc_flow();
        app.try_complete_accept(listener, flow)?;
        let lookup_id = self.alloc_tcp_lookup_id()?;
        self.tcp_connections.push(TcpConnectionRegistration {
            flow,
            connection_id: None,
            lookup_id,
            owner_worker: listener_registration.owner_worker,
            state: hammer_core::protocol::tcp::TcpState::Established,
            local_port: local.port(),
            local: Some(local),
            remote,
            target: AppIngressTarget::flow(app.clone(), flow),
        });
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(flow)
    }

    fn accept_tcp_connection_by_listener_id(
        &mut self,
        listener_id: TcpListenerId,
        remote: SocketAddr,
        local: SocketAddr,
    ) -> HammerResult<AppFlowId> {
        let registration = self
            .tcp_listeners
            .iter()
            .find(|registration| registration.lookup_id as u64 == listener_id.get())
            .cloned()
            .ok_or_else(|| {
                HammerError::internal(format!(
                    "tcp listener lookup {} is not registered in runtime service",
                    listener_id.get()
                ))
            })?;
        self.accept_tcp_connection(&registration.app, registration.socket, remote, local)
    }

    fn connect_tcp_stream(
        &mut self,
        app: &AppContext,
        peer: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppFlowId> {
        let flow = self.alloc_flow();
        let lookup_id = self.alloc_tcp_lookup_id()?;
        let local_port = self.alloc_tcp_ephemeral_port()?;
        self.tcp_connections.push(TcpConnectionRegistration {
            flow,
            connection_id: None,
            lookup_id,
            owner_worker: worker_id(owner_worker)?,
            state: hammer_core::protocol::tcp::TcpState::SynSent,
            local_port,
            local: None,
            remote: peer,
            target: AppIngressTarget::flow(app.clone(), flow),
        });
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(flow)
    }

    fn promote_pending_syn_sent_connection(
        &mut self,
        lookup_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> HammerResult<()> {
        let index = self
            .tcp_connections
            .iter()
            .position(|registration| registration.lookup_id == lookup_id)
            .ok_or_else(|| {
                HammerError::internal(format!(
                    "tcp syn-sent lookup {lookup_id} is not registered in runtime service"
                ))
            })?;
        let current =
            self.tcp_connections.get(index).cloned().ok_or_else(|| {
                HammerError::internal("tcp syn-sent registration slot is invalid")
            })?;
        if current.remote != remote {
            return Err(HammerError::internal(format!(
                "tcp syn-sent lookup {lookup_id} remote mismatch: expected {}, got {}",
                current.remote, remote
            )));
        }
        if current.local_port != local.port() {
            return Err(HammerError::internal(format!(
                "tcp syn-sent lookup {lookup_id} local port mismatch: expected {}, got {}",
                current.local_port,
                local.port()
            )));
        }
        if current.state == hammer_core::protocol::tcp::TcpState::Established
            && current.local == Some(local)
        {
            return Ok(());
        }
        if current.state != hammer_core::protocol::tcp::TcpState::SynSent {
            return Err(HammerError::internal(format!(
                "tcp syn-sent lookup {lookup_id} cannot promote from state {:?}",
                current.state
            )));
        }
        self.ensure_connection_absent(local, remote)?;
        let registration = self
            .tcp_connections
            .get_mut(index)
            .ok_or_else(|| HammerError::internal("tcp syn-sent registration slot is invalid"))?;
        registration.state = hammer_core::protocol::tcp::TcpState::Established;
        registration.local = Some(local);
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(())
    }

    fn observe_tcp_connection_state_change(
        &mut self,
        connection_id: TcpConnectionId,
        key: TcpConnectionKey,
        state: hammer_core::protocol::tcp::TcpState,
    ) -> HammerResult<()> {
        let (local, remote) = socket_addrs_from_connection_key(key);
        let index = self
            .tcp_connections
            .iter()
            .position(|registration| registration.connection_id == Some(connection_id))
            .or_else(|| {
                self.tcp_connections.iter().position(|registration| {
                    registration.connection_id.is_none()
                        && registration.local_port == local.port()
                        && registration.remote == remote
                })
            })
            .ok_or_else(|| {
                HammerError::internal(format!(
                    "tcp connection state change {} {} -> {} is not registered in runtime service",
                    connection_id.get(),
                    local,
                    remote
                ))
            })?;
        let current =
            self.tcp_connections.get(index).cloned().ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
        if current
            .connection_id
            .is_some_and(|current_id| current_id != connection_id)
        {
            return Err(HammerError::internal(format!(
                "tcp connection {} conflicts with registration {}",
                connection_id.get(),
                current
                    .connection_id
                    .map(TcpConnectionId::get)
                    .unwrap_or_default()
            )));
        }
        if current.local_port != local.port() {
            return Err(HammerError::internal(format!(
                "tcp connection {} local port mismatch: expected {}, got {}",
                connection_id.get(),
                current.local_port,
                local.port()
            )));
        }
        if current.remote != remote {
            return Err(HammerError::internal(format!(
                "tcp connection {} remote mismatch: expected {}, got {}",
                connection_id.get(),
                current.remote,
                remote
            )));
        }
        if current.state != hammer_core::protocol::tcp::TcpState::Established
            && state == hammer_core::protocol::tcp::TcpState::Established
        {
            self.ensure_connection_absent(local, remote)?;
        }
        let registration = self
            .tcp_connections
            .get_mut(index)
            .ok_or_else(|| HammerError::internal("tcp connection registration slot is invalid"))?;
        registration.connection_id = Some(connection_id);
        registration.local = Some(local);
        registration.state = state;
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(())
    }

    fn remove_tcp_connection_by_connection_id(
        &mut self,
        connection_id: TcpConnectionId,
    ) -> HammerResult<bool> {
        let Some(index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.connection_id == Some(connection_id))
        else {
            return Ok(false);
        };
        let released =
            self.tcp_connections.get(index).cloned().ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
        self.tcp_connections.drain(index..index + 1);
        if released.local_port < self.next_tcp_ephemeral_port {
            self.next_tcp_ephemeral_port = released.local_port;
        }
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(true)
    }

    fn publish_tcp_lookup(&mut self) -> HammerResult<()> {
        let mut snapshot = TcpLookupSnapshot::empty();
        let mut syn_sent_connections = InfraVec::new();
        for registration in self.tcp_listeners.iter().cloned() {
            let value = TcpLookupValue {
                kind: TcpLookupKind::Listener,
                id: registration.lookup_id,
                owner_worker: registration.owner_worker,
            };
            match registration.bind.ip() {
                IpAddr::V4(addr) => snapshot.insert_listener_v4(
                    TcpV4ListenerKey::new(0, addr, registration.bind.port()),
                    value,
                ),
                IpAddr::V6(addr) => snapshot.insert_listener_v6(
                    TcpV6ListenerKey::new(0, addr, registration.bind.port()),
                    value,
                ),
            }
        }
        for registration in self.tcp_connections.iter().cloned() {
            let value = TcpLookupValue {
                kind: match registration.state {
                    hammer_core::protocol::tcp::TcpState::SynSent => {
                        TcpLookupKind::SynSentConnection
                    }
                    _ => TcpLookupKind::EstablishedConnection,
                },
                id: registration.lookup_id,
                owner_worker: registration.owner_worker,
            };
            if registration.state == hammer_core::protocol::tcp::TcpState::SynSent {
                match registration.remote.ip() {
                    IpAddr::V4(remote) => {
                        let key = TcpV4PendingConnectionKey::new(
                            0,
                            registration.local_port,
                            remote,
                            registration.remote.port(),
                        );
                        snapshot.insert_syn_sent_connection_v4(key, value);
                        syn_sent_connections
                            .push(TcpSynSentRegistration::v4(registration.lookup_id, key));
                    }
                    IpAddr::V6(remote) => {
                        let key = TcpV6PendingConnectionKey::new(
                            0,
                            registration.local_port,
                            remote,
                            registration.remote.port(),
                        );
                        snapshot.insert_syn_sent_connection_v6(key, value);
                        syn_sent_connections
                            .push(TcpSynSentRegistration::v6(registration.lookup_id, key));
                    }
                }
                continue;
            }
            let Some(local) = registration.local else {
                continue;
            };
            match (local.ip(), registration.remote.ip()) {
                (IpAddr::V4(local_ip), IpAddr::V4(remote)) => snapshot.insert_connection_v4(
                    TcpV4ConnectionKey::new(
                        0,
                        local_ip,
                        local.port(),
                        remote,
                        registration.remote.port(),
                    ),
                    value,
                ),
                (IpAddr::V6(local_ip), IpAddr::V6(remote)) => snapshot.insert_connection_v6(
                    TcpV6ConnectionKey::new(
                        0,
                        local_ip,
                        local.port(),
                        remote,
                        registration.remote.port(),
                    ),
                    value,
                ),
                _ => {
                    return Err(HammerError::internal(format!(
                        "tcp connection {} -> {} mixes IP versions",
                        registration.remote, local
                    )));
                }
            }
        }
        self.tcp_control
            .publish_lookup(snapshot.clone())
            .map_err(|err| HammerError::internal(format!("publish tcp lookup snapshot: {err}")))?;
        if let Some(control) = &self.tcp_syn_sent_control {
            control
                .publish_connections(syn_sent_connections)
                .map_err(|err| {
                    HammerError::internal(format!("publish tcp syn-sent snapshot: {err}"))
                })?;
        }
        self.tcp_lookup = snapshot;
        Ok(())
    }

    fn alloc_tcp_ephemeral_port(&mut self) -> HammerResult<u16> {
        for _ in 0..u16::MAX {
            let port = self.next_tcp_ephemeral_port;
            self.next_tcp_ephemeral_port = self.next_tcp_ephemeral_port.wrapping_add(1).max(49_152);
            let in_use_by_listener = self
                .tcp_listeners
                .iter()
                .any(|registration| registration.bind.port() == port);
            let in_use_by_connection = self
                .tcp_connections
                .iter()
                .any(|registration| registration.local_port == port);
            if !in_use_by_listener && !in_use_by_connection {
                return Ok(port);
            }
        }
        Err(HammerError::internal(
            "no ephemeral tcp port is available for pending connect",
        ))
    }

    fn publish_tcp_accept(&self) -> HammerResult<()> {
        let Some(control) = &self.tcp_accept_control else {
            return Ok(());
        };
        control
            .publish_listeners(self.tcp_listeners.iter().cloned().map(|registration| {
                (
                    registration.lookup_id,
                    TcpAcceptRegistration::new(registration.app, registration.socket),
                )
            }))
            .map_err(|err| HammerError::internal(format!("publish tcp accept listeners: {err}")))
    }

    fn publish_tcp_app_ingress(&self) -> HammerResult<()> {
        self.tcp_control
            .publish_app_ingress(
                self.tcp_connections
                    .iter()
                    .map(|registration| registration.lookup_id),
            )
            .map_err(|err| HammerError::internal(format!("publish tcp app ingress ids: {err}")))?;
        self.tcp_rcv_process_control
            .publish_app_ingress(
                self.tcp_connections
                    .iter()
                    .cloned()
                    .map(|registration| (registration.lookup_id, registration.target)),
            )
            .map_err(|err| {
                HammerError::internal(format!("publish tcp rcv-process ingress targets: {err}"))
            })?;
        Ok(())
    }

    fn ensure_connection_absent(&self, local: SocketAddr, remote: SocketAddr) -> HammerResult<()> {
        let duplicate = match (local.ip(), remote.ip()) {
            (IpAddr::V4(local_ip), IpAddr::V4(remote_ip)) => self
                .tcp_lookup
                .lookup_connection_v4(TcpV4ConnectionKey::new(
                    0,
                    local_ip,
                    local.port(),
                    remote_ip,
                    remote.port(),
                ))
                .is_some(),
            (IpAddr::V6(local_ip), IpAddr::V6(remote_ip)) => self
                .tcp_lookup
                .lookup_connection_v6(TcpV6ConnectionKey::new(
                    0,
                    local_ip,
                    local.port(),
                    remote_ip,
                    remote.port(),
                ))
                .is_some(),
            _ => {
                return Err(HammerError::internal(format!(
                    "tcp connection {} -> {} mixes IP versions",
                    remote, local
                )));
            }
        };
        if duplicate {
            return Err(HammerError::internal(format!(
                "tcp connection {} -> {} is already registered",
                remote, local
            )));
        }
        Ok(())
    }

    fn rebuild_tcp_listener_slots(&mut self) {
        let mut slots = FlatHashTable::new();
        for (index, registration) in self.tcp_listeners.iter().cloned().enumerate() {
            slots.insert(registration.socket.value(), index);
        }
        self.tcp_listener_slots = slots;
    }

    fn rebuild_udp_socket_slots(&mut self) {
        let mut slots = FlatHashTable::new();
        for (index, registration) in self.udp_sockets.iter().copied().enumerate() {
            slots.insert(registration.socket.value(), index);
        }
        self.udp_socket_slots = slots;
    }
}

impl RuntimeAppControlHandle {
    #[inline]
    fn new(control_handle: Arc<ControlThreadHandle>, state: RuntimeAppControlState) -> Self {
        Self {
            control_handle,
            state: Arc::new(RuntimeAppControlCell::new(state)),
        }
    }

    #[inline]
    fn install_tcp_accept_control(&self, control: TcpAcceptControlPlane) -> HammerResult<()> {
        self.with_state_mut(move |state| state.install_tcp_accept_control(control))
    }

    #[inline]
    fn install_tcp_syn_sent_control(&self, control: TcpSynSentControlPlane) -> HammerResult<()> {
        self.with_state_mut(move |state| state.install_tcp_syn_sent_control(control))
    }

    #[inline]
    fn install_shared_tcp_control(&self, control: SharedTcpControlPlane) -> HammerResult<()> {
        self.with_state_mut(move |state| {
            state.install_shared_tcp_control(control);
            Ok(())
        })
    }

    #[inline]
    fn connect_tcp_stream(
        &self,
        app: &AppContext,
        peer: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppFlowId> {
        let app = app.clone();
        self.with_state_mut(move |state| state.connect_tcp_stream(&app, peer, owner_worker))
    }

    fn bind_tcp_listener(
        &self,
        app: &AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        let app = app.clone();
        let result =
            self.with_state_mut(move |state| state.bind_tcp_listener(&app, bind, owner_worker))?;
        if let Some(action) = result.shared_action {
            let Some(control) = self.with_state(|state| Ok(state.shared_tcp_control.clone()))?
            else {
                return Ok(result.socket);
            };
            control.apply(action).map_err(|err| {
                HammerError::internal(format!("install shared tcp listener control: {err}"))
            })?;
        }
        Ok(result.socket)
    }

    fn close_socket(&self, socket: AppSocketId) -> HammerResult<()> {
        let result = self.with_state_mut(move |state| state.close_socket(socket))?;
        if let Some(action) = result.shared_action {
            let Some(control) = self.with_state(|state| Ok(state.shared_tcp_control.clone()))?
            else {
                return Ok(());
            };
            control.apply(action).map_err(|err| {
                HammerError::internal(format!("apply shared tcp close action: {err}"))
            })?;
        }
        Ok(())
    }

    fn handle_tcp_syn_sent_observation(
        &self,
        observation: TcpSynSentObservation,
    ) -> HammerResult<()> {
        let state = Arc::clone(&self.state);
        self.control_handle
            .schedule_once(Duration::ZERO, move || {
                let state = Arc::clone(&state);
                async move {
                    // SAFETY: this closure runs on the single control thread.
                    let state = unsafe { state.get_mut() };
                    let result = state.promote_pending_syn_sent_connection(
                        observation.connection_id,
                        observation.local,
                        observation.remote,
                    );
                    debug_assert!(
                        result.is_ok(),
                        "runtime tcp syn-sent observation failed: {result:?}"
                    );
                    let _ = result;
                }
            })
            .map(|_| ())
    }

    #[inline]
    fn handle_projected_tcp_worker_event(&self, event: TcpWorkerEvent) -> HammerResult<()> {
        Self::schedule_projected_tcp_worker_event(
            Arc::clone(&self.control_handle),
            Arc::downgrade(&self.state),
            event,
        )
    }

    fn schedule_projected_tcp_worker_event(
        control_handle: Arc<ControlThreadHandle>,
        state: Weak<RuntimeAppControlCell>,
        event: TcpWorkerEvent,
    ) -> HammerResult<()> {
        match event {
            TcpWorkerEvent::IncomingConnection {
                listener_id, key, ..
            } => {
                control_handle
                    .schedule_once(Duration::ZERO, move || {
                        let state = state.clone();
                        async move {
                            let Some(state) = state.upgrade() else {
                                return;
                            };
                            let (local, remote) = socket_addrs_from_connection_key(key);
                            // SAFETY: this closure runs on the single control thread.
                            let state = unsafe { state.get_mut() };
                            let result = state.accept_tcp_connection_by_listener_id(
                                listener_id,
                                remote,
                                local,
                            );
                            debug_assert!(
                                result.is_ok(),
                                "runtime tcp incoming-connection event failed: {result:?}"
                            );
                            let _ = result;
                        }
                    })
                    .map(|_| ())
            }
            TcpWorkerEvent::StateChanged {
                connection_id,
                key,
                state: tcp_state,
            } => {
                control_handle
                    .schedule_once(Duration::ZERO, move || {
                        let state_ref = state.clone();
                        async move {
                            let Some(state_cell) = state_ref.upgrade() else {
                                return;
                            };
                            // SAFETY: this closure runs on the single control thread.
                            let state_cell = unsafe { state_cell.get_mut() };
                            let result = state_cell.observe_tcp_connection_state_change(
                                connection_id,
                                key,
                                tcp_state,
                            );
                            debug_assert!(
                                result.is_ok(),
                                "runtime tcp state-change event failed: {result:?}"
                            );
                            let _ = result;
                        }
                    })
                    .map(|_| ())
            }
            TcpWorkerEvent::ShutdownObserved { .. } => Ok(()),
            TcpWorkerEvent::Closed { connection_id, .. } => {
                control_handle
                    .schedule_once(Duration::ZERO, move || {
                        let state = state.clone();
                        async move {
                            let Some(state_cell) = state.upgrade() else {
                                return;
                            };
                            // SAFETY: this closure runs on the single control thread.
                            let state_cell = unsafe { state_cell.get_mut() };
                            let result =
                                state_cell.remove_tcp_connection_by_connection_id(connection_id);
                            debug_assert!(
                                result.is_ok(),
                                "runtime tcp close event failed: {result:?}"
                            );
                            let _ = result;
                        }
                    })
                    .map(|_| ())
            }
            other => Err(HammerError::internal(format!(
                "runtime tcp worker event is not supported yet: {other:?}"
            ))),
        }
    }

    #[inline]
    fn handle_tcp_worker_event(&self, event: TcpWorkerEvent) -> HammerResult<()> {
        match event {
            TcpWorkerEvent::IncomingConnection { .. } => {
                self.handle_projected_tcp_worker_event(event)
            }
            TcpWorkerEvent::StateChanged {
                connection_id,
                key,
                state: tcp_state,
            } => {
                let Some((control, congestion)) = self.with_state(|state| {
                    Ok(state
                        .shared_tcp_control
                        .clone()
                        .map(|control| (control, state.tcp_congestion)))
                })?
                else {
                    return self.handle_projected_tcp_worker_event(TcpWorkerEvent::StateChanged {
                        connection_id,
                        key,
                        state: tcp_state,
                    });
                };
                if control.has_connection(connection_id) {
                    control
                        .apply(hammer_core::protocol::tcp::TcpControlPlaneAction::TransitionConnection {
                            connection_id,
                            state: tcp_state,
                        })
                        .map_err(|err| {
                            HammerError::internal(format!(
                                "transition shared tcp connection control: {err}"
                            ))
                        })
                } else {
                    let action = TcpConnectionState::new(&congestion, None)?
                        .install_connection_action(connection_id, key, tcp_state);
                    control.apply(action).map_err(|err| {
                        HammerError::internal(format!(
                            "install shared tcp connection control: {err}"
                        ))
                    })
                }
            }
            TcpWorkerEvent::ShutdownObserved {
                connection_id,
                direction,
                reason,
            } => {
                let Some(control) =
                    self.with_state(|state| Ok(state.shared_tcp_control.clone()))?
                else {
                    return Ok(());
                };
                if !control.has_connection(connection_id) {
                    return Ok(());
                }
                control
                    .apply(
                        hammer_core::protocol::tcp::TcpControlPlaneAction::ShutdownConnection {
                            connection_id,
                            direction,
                            reason,
                        },
                    )
                    .map_err(|err| {
                        HammerError::internal(format!(
                            "shutdown shared tcp connection control: {err}"
                        ))
                    })
            }
            TcpWorkerEvent::Closed {
                connection_id,
                reason,
            } => {
                let Some(control) =
                    self.with_state(|state| Ok(state.shared_tcp_control.clone()))?
                else {
                    return self.handle_projected_tcp_worker_event(TcpWorkerEvent::Closed {
                        connection_id,
                        reason,
                    });
                };
                if !control.has_connection(connection_id) {
                    return self.handle_projected_tcp_worker_event(TcpWorkerEvent::Closed {
                        connection_id,
                        reason,
                    });
                }
                control
                    .apply(
                        hammer_core::protocol::tcp::TcpControlPlaneAction::CloseConnection {
                            connection_id,
                            reason,
                        },
                    )
                    .map_err(|err| {
                        HammerError::internal(format!("close shared tcp connection control: {err}"))
                    })
            }
            other => Err(HammerError::internal(format!(
                "runtime tcp worker event is not supported yet: {other:?}"
            ))),
        }
    }

    #[inline]
    fn with_state_mut<R>(
        &self,
        f: impl FnOnce(&mut RuntimeAppControlState) -> HammerResult<R> + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let state = Arc::clone(&self.state);
        self.control_handle.call_blocking(move || {
            // SAFETY: RuntimeAppControlState is owned by the control plane.
            // All mutable access routes through this control-thread dispatch.
            let state = unsafe { state.get_mut() };
            f(state)
        })?
    }

    #[cfg_attr(not(test), allow(dead_code))]
    #[inline]
    fn with_state<R>(
        &self,
        f: impl FnOnce(&RuntimeAppControlState) -> HammerResult<R> + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let state = Arc::clone(&self.state);
        self.control_handle.call_blocking(move || {
            // SAFETY: RuntimeAppControlState reads are also serialized on
            // the control thread, keeping the ownership model single-threaded.
            let state = unsafe { &*state.inner.get() };
            f(state)
        })?
    }

    #[cfg(test)]
    fn snapshot_for_test(&self) -> HammerResult<RuntimeAppControlSnapshot> {
        self.with_state(|state| {
            let mut tcp_listeners = InfraVec::new();
            for registration in state.tcp_listeners.iter() {
                tcp_listeners.push(RuntimeTcpListenerSnapshot {
                    socket: registration.socket,
                    lookup_id: registration.lookup_id,
                });
            }
            Ok(RuntimeAppControlSnapshot {
                tcp_listeners,
                udp_sockets: state.udp_sockets.clone(),
                tcp_lookup: state.tcp_lookup.clone(),
            })
        })
    }
}

impl AppControlBackend for RuntimeAppControlHandle {
    fn bind_tcp_listener(
        &self,
        app: &AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        RuntimeAppControlHandle::bind_tcp_listener(self, app, bind, owner_worker)
    }

    fn connect_tcp_stream(
        &self,
        app: &AppContext,
        peer: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppFlowId> {
        RuntimeAppControlHandle::connect_tcp_stream(self, app, peer, owner_worker)
    }

    fn bind_udp_socket(
        &self,
        app: &AppContext,
        bind: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<AppSocketId> {
        let app = app.clone();
        self.with_state_mut(move |state| state.bind_udp_socket(&app, bind, owner_worker))
    }

    fn close_socket(&self, _app: &AppContext, socket: AppSocketId) -> HammerResult<()> {
        RuntimeAppControlHandle::close_socket(self, socket)
    }
}

impl crate::transport::tcp::TcpAcceptBackend for RuntimeAppControlHandle {
    fn accept(
        &self,
        _listener_id: TcpLookupId,
        _registration: &TcpAcceptRegistration,
        _remote: SocketAddr,
        _local: SocketAddr,
        event: TcpWorkerEvent,
    ) -> Result<(), CoreError> {
        self.handle_tcp_worker_event(event)
            .map_err(|err| CoreError::internal(format!("runtime tcp accept: {err}")))
    }
}

impl TcpSynSentBackend for RuntimeAppControlHandle {
    fn observe_syn_ack(&self, observation: TcpSynSentObservation) -> Result<(), CoreError> {
        self.handle_tcp_syn_sent_observation(observation)
            .map_err(|err| CoreError::internal(format!("runtime tcp syn-sent: {err}")))
    }
}

fn socket_addrs_from_connection_key(key: TcpConnectionKey) -> (SocketAddr, SocketAddr) {
    let local = SocketAddr::new(key.local_addr(), key.local_port());
    let remote = SocketAddr::new(key.remote_addr(), key.remote_port());
    (local, remote)
}

fn socket_addr_to_listener_key(
    bind: SocketAddr,
) -> HammerResult<hammer_core::protocol::tcp::TcpListenerKey> {
    Ok(match bind.ip() {
        IpAddr::V4(addr) => hammer_core::protocol::tcp::TcpListenerKey::v4(0, addr, bind.port()),
        IpAddr::V6(addr) => hammer_core::protocol::tcp::TcpListenerKey::v6(0, addr, bind.port()),
    })
}

impl RuntimeService {
    pub fn new(
        config_content: &str,
        platform: Arc<dyn PlatformInterface>,
        writer: Arc<dyn LogWriter>,
    ) -> HammerResult<Arc<Self>> {
        Self::new_with_event_subscribers(
            config_content,
            platform,
            writer,
            crate::event_subscribers::build_standard_event_subscribers,
        )
    }

    pub fn new_with_event_subscribers(
        config_content: &str,
        platform: Arc<dyn PlatformInterface>,
        writer: Arc<dyn LogWriter>,
        build_event_subscribers: EventSubscriberBuilder,
    ) -> HammerResult<Arc<Self>> {
        hammer_runtime::install_default_crypto_provider();

        let options = config::parse_config(config_content)?;
        let metrics = MetricsRegistry::new();
        let trace = TraceControlPlane::new(options.trace.record_capacity);
        let base_time = Instant::now();
        // Data-plane runtime: multiple fixed data threads, each with its own
        // current-thread reactor and thread-local packet buffer pool. Futures
        // spawned onto a data thread are not work-stolen by another data
        // thread, so a flow can keep buffer indices local to its worker.
        let data_runtime = DataRuntime::new(
            DATA_WORKER_THREADS,
            "hammer-data",
            DATA_WORKER_STACK_SIZE,
            DATA_MAX_BLOCKING_THREADS,
        )?;
        let data_context = data_runtime.context();
        let app_context = AppContext::with_ring_capacity(data_context.clone(), 256);
        let writer: Arc<dyn LogWriter> = if options.log.disabled {
            Arc::new(DiscardWriter)
        } else {
            writer
        };
        let (control_handle, control_loop) = ControlThread::new(
            base_time,
            writer,
            Arc::clone(&metrics),
            METRICS_LOG_INTERVAL,
            options.log.level,
        );
        let control_thread = match spawn_control_thread(control_loop) {
            Ok(handle) => handle,
            Err(err) => {
                // Avoid leaking the data runtime if control setup fails —
                // its workers and reactor would otherwise live until the
                // process exits.
                data_runtime.shutdown_timeout(DATA_SHUTDOWN_TIMEOUT);
                return Err(err);
            }
        };
        let app_control = RuntimeAppControlHandle::new(
            Arc::clone(&control_handle),
            RuntimeAppControlState::new()?,
        );
        let app_control_backend: Arc<dyn AppControlBackend> = Arc::new(app_control.clone());
        app_context.install_control(AppControl::new(app_control_backend))?;
        let writer: Arc<dyn LogWriter> = Arc::clone(&control_handle) as Arc<dyn LogWriter>;
        let log_factory = Factory::new_with_min_level(base_time, writer, options.log.level);
        app_control.install_tcp_accept_control(TcpAcceptControlPlane::new(
            Arc::new(app_control.clone()),
            TcpAcceptNext::nodes(unused_node_id()),
        ))?;
        app_control.install_tcp_syn_sent_control(TcpSynSentControlPlane::new(
            Arc::new(app_control.clone()),
            TcpSynSentNext::nodes(unused_node_id()),
        ))?;
        let projected_control_handle = Arc::clone(&control_handle);
        let projected_state = Arc::downgrade(&app_control.state);
        app_control.install_shared_tcp_control(SharedTcpControlPlane::new(
            Arc::clone(&control_handle),
            move |event| {
                let result = RuntimeAppControlHandle::schedule_projected_tcp_worker_event(
                    Arc::clone(&projected_control_handle),
                    projected_state.clone(),
                    event,
                );
                debug_assert!(
                    result.is_ok(),
                    "runtime projected tcp control event failed: {result:?}"
                );
                let _ = result;
            },
        ))?;
        let trace_enabled_with_inputs = trace_worker_control(&options.trace);
        let packet_graph = ServicePacketGraphDeclarations::default();
        trace.publish_options(&options.trace, |name| packet_graph.resolve(name))?;
        data_context.set_trace_control_on_workers(
            trace_enabled_with_inputs.then(|| trace.handle()),
            options.trace.packet_capacity,
        )?;
        let event_subscriptions = build_event_subscribers(
            new_logger(&log_factory, "control-event"),
            Arc::clone(&control_handle),
        )?;

        let registry = RuntimeRegistry::new();
        let pause = Arc::new(PauseManager::new());

        let cert_store = Arc::new(CertificateStore::new(
            new_logger(&log_factory, "certificate-store"),
            false,
        ));
        let cert_provider = Arc::new(CertificateProviderManager::new(new_logger(
            &log_factory,
            "certificate-provider",
        )));
        #[cfg(feature = "endpoint")]
        let endpoint = Arc::new(EndpointManager::from_options_with_platform_and_control(
            new_logger(&log_factory, "endpoint"),
            &options.endpoints,
            Arc::clone(&platform),
            Arc::clone(&control_handle),
        )?);
        let connection = Arc::new(ConnectionManager::new());
        let network = NetworkManager::with_platform(
            new_logger(&log_factory, "network"),
            options.route.auto_detect_interface,
            Arc::clone(&platform),
            Arc::clone(&pause),
            Arc::clone(&connection),
        );
        let outbound = Arc::new(
            OutboundManager::from_options_with_platform_metrics_and_control(
                new_logger(&log_factory, "outbound"),
                options.route.final_.clone(),
                &options.outbounds,
                Arc::clone(&platform),
                Arc::clone(&metrics),
                Arc::clone(&control_handle),
            )?,
        );
        // Auto-register an `EndpointOutboundAdapter` for every declared
        // endpoint **before** `bind_aggregates`: urltest and friends will
        // resolve `<endpoint-id>` against the same OutboundManager when
        // their children include endpoint ids, and DNS `via = "<endpoint-id>"`
        // requires the adapter to be findable through `outbound.get()`.
        #[cfg(feature = "endpoint")]
        register_endpoint_outbound_adapters(&outbound, &endpoint, &log_factory)?;
        // Aggregate outbounds (urltest) need a `Weak<dyn OutboundManager>`
        // before any children can be looked up. Bind once here, after every
        // declared outbound has been registered, so the urltest's first dial /
        // probe can resolve every outbound child without races.
        outbound.bind_aggregates();
        let default_domain_resolver = options
            .route
            .default_domain_resolver
            .as_ref()
            .map(|d| d.server.as_str());
        let dns_transport = Arc::new(DnsTransportManager::from_options_with_runtime(
            new_logger(&log_factory, "dns-transport"),
            &options.dns,
            Arc::clone(&outbound),
            Arc::clone(&platform),
            default_domain_resolver,
        )?);
        let dns_router = Arc::new(
            DnsRouter::new_with_manager(
                new_logger(&log_factory, "dns-router"),
                Arc::clone(&dns_transport),
                options.dns.strategy,
            )
            .with_rules(&options.dns.rules)?
            .with_control_handle(Arc::clone(&control_handle))?,
        );
        #[cfg(feature = "endpoint")]
        let router = Arc::new(Router::from_options_with_metrics_and_endpoint_ids(
            new_logger(&log_factory, "router"),
            options.route.clone(),
            Arc::clone(&outbound),
            Arc::clone(&metrics),
            options.endpoints.iter().map(|endpoint| endpoint.id.clone()),
        )?);
        #[cfg(not(feature = "endpoint"))]
        let router = Arc::new(Router::from_options_with_metrics(
            new_logger(&log_factory, "router"),
            options.route.clone(),
            Arc::clone(&outbound),
            Arc::clone(&metrics),
        )?);
        let inbound_dns_router: Arc<dyn AdapterDnsRouter> = dns_router.clone();
        let inbound = Arc::new(InboundManager::from_options_with_runtime_and_metrics(
            new_logger(&log_factory, "inbound"),
            &options.inbounds,
            Arc::clone(&router),
            inbound_dns_router,
            Arc::clone(&outbound),
            Arc::clone(&platform),
            Arc::clone(&metrics),
        )?);
        let service_mgr = Arc::new(ServiceManager::new(new_logger(&log_factory, "service")));

        registry.set::<CertificateStore>(Arc::clone(&cert_store));
        registry.set::<CertificateProviderManager>(Arc::clone(&cert_provider));
        #[cfg(feature = "endpoint")]
        registry.set::<EndpointManager>(Arc::clone(&endpoint));
        registry.set::<NetworkManager>(Arc::clone(&network));
        registry.set::<DnsTransportManager>(Arc::clone(&dns_transport));
        registry.set::<OutboundManager>(Arc::clone(&outbound));
        registry.set::<DnsRouter>(Arc::clone(&dns_router));
        registry.set::<Router>(Arc::clone(&router));
        registry.set::<InboundManager>(Arc::clone(&inbound));
        registry.set::<ServiceManager>(Arc::clone(&service_mgr));
        registry.set::<ConnectionManager>(Arc::clone(&connection));
        registry.set::<PauseManager>(Arc::clone(&pause));
        registry.set::<MetricsRegistry>(Arc::clone(&metrics));

        let mut lifecycles: Vec<Arc<dyn Lifecycle>> = vec![
            cert_store as Arc<dyn Lifecycle>,
            cert_provider as Arc<dyn Lifecycle>,
        ];
        #[cfg(feature = "endpoint")]
        lifecycles.push(Arc::clone(&endpoint) as Arc<dyn Lifecycle>);
        lifecycles.extend([
            Arc::clone(&network) as Arc<dyn Lifecycle>,
            dns_transport as Arc<dyn Lifecycle>,
            Arc::clone(&outbound) as Arc<dyn Lifecycle>,
            Arc::clone(&dns_router) as Arc<dyn Lifecycle>,
            router as Arc<dyn Lifecycle>,
            inbound as Arc<dyn Lifecycle>,
            service_mgr as Arc<dyn Lifecycle>,
            connection as Arc<dyn Lifecycle>,
        ]);

        debug_assert_lifecycle_order(&lifecycles);

        #[cfg(feature = "probe")]
        let probe = Arc::new(ProbeManager::new(Arc::clone(&outbound)));

        let trace_drain_timer = if trace_enabled_with_inputs {
            Some(schedule_trace_drain(
                Arc::clone(&control_handle),
                data_context.clone(),
                trace.sink(),
                new_logger(&log_factory, "trace-control"),
            )?)
        } else {
            None
        };
        let runtime_dump_timer = if options.runtime.enabled {
            match schedule_runtime_dump(
                Arc::clone(&control_handle),
                data_context.clone(),
                options.runtime.interval,
                new_logger(&log_factory, "runtime-control"),
            ) {
                Ok(timer) => Some(timer),
                Err(err) => {
                    if let Some(timer) = &trace_drain_timer {
                        timer.cancel_timeout(CONTROL_SHUTDOWN_TIMEOUT);
                    }
                    return Err(err);
                }
            }
        } else {
            None
        };

        Ok(Arc::new(Self {
            inner: Arc::new(Mutex::new(ServiceInner {
                state: ServiceState::NotStarted,
                log_factory,
                control_handle: Some(control_handle),
                control_thread: Some(control_thread),
                trace_drain_timer,
                runtime_dump_timer,
                event_subscriptions,
                metrics,
                _trace: trace,
                _platform: platform,
                _registry: registry,
                lifecycles,
                pause,
                network,
                dns_router,
                outbound,
                #[cfg(feature = "endpoint")]
                endpoint,
                #[cfg(feature = "probe")]
                probe,
                data_runtime: Some(data_runtime),
                data_context,
                app_context,
                app_control,
                _options: options,
            })),
        }))
    }

    #[inline]
    pub fn app_context(&self) -> AppContext {
        self.inner
            .lock()
            .expect("service mutex poisoned")
            .app_context
            .clone()
    }

    /// Run a one-shot latency probe to every registered outbound's probe endpoint
    /// and return one report per outbound (order matches
    /// `OutboundManager::list()`). `protocol` selects the probe
    /// implementation (V1 only `"icmp"`); `timeout` applies per
    /// outbound, not to the batch.
    ///
    /// Probe failures (timeout, connection refused, unsupported
    /// network) live inside each `ProbeReport.result` so the caller
    /// always sees the full outbound list. Only invalid arguments
    /// (unknown protocol) bubble up as `Err`.
    #[cfg(feature = "probe")]
    pub fn probe_outbounds(
        &self,
        protocol: &str,
        timeout: Duration,
    ) -> HammerResult<Vec<ProbeReport>> {
        let protocol = protocol.to_owned();
        let outer_timeout = timeout.saturating_add(CONTROL_ASYNC_BUFFER);
        self.control_async_call(outer_timeout, move |inner, data, done| {
            let probe = build_probe_protocol(&protocol)?;
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            let batch = inner.probe.prepare_all(probe);
            data.execute(async move {
                let reports = batch.run(timeout).await;
                let _ = done.send(Ok(reports));
            });
            Ok(())
        })
    }

    /// Return the id of the child currently selected by an aggregate
    /// outbound (e.g. urltest). Leaf outbounds — and any unknown id —
    /// return `None` so the FFI layer can map it to a nullable string.
    pub fn current_selection(&self, outbound_id: &str) -> Option<String> {
        let outbound_id = outbound_id.to_owned();
        self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return None;
            }
            inner
                .outbound
                .get(&outbound_id)
                .and_then(|o| o.runtime().now())
        })
        .ok()
        .flatten()
    }

    /// Trigger a one-shot probe sweep on an aggregate outbound and
    /// collect per-child latency reports. Mirrors sing-box's
    /// `URLTest()`: the call drives the same probe path used by the
    /// PostStart kickoff, then returns the resulting samples.
    ///
    /// `timeout` is forwarded to each per-child probe. A zero value
    /// means "use the value baked into the outbound config".
    pub fn urltest(&self, outbound_id: &str, timeout: Duration) -> HammerResult<Vec<ProbeReport>> {
        let timeout_outbound_id = outbound_id.to_owned();
        let effective_timeout = self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            let outbound = inner.outbound.get(&timeout_outbound_id).ok_or_else(|| {
                HammerError::config_validation(format!(
                    "outbound '{timeout_outbound_id}' is not registered"
                ))
            })?;
            Ok(outbound.runtime().probe_group_timeout(timeout))
        })??;

        let outbound_id = outbound_id.to_owned();
        let outer_timeout = effective_timeout.saturating_add(CONTROL_ASYNC_BUFFER);
        self.control_async_call(outer_timeout, move |inner, data, done| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            let outbound = inner.outbound.get(&outbound_id).ok_or_else(|| {
                HammerError::config_validation(format!(
                    "outbound '{outbound_id}' is not registered"
                ))
            })?;
            data.execute(async move {
                let reports = outbound.runtime().probe_group(timeout).await;
                let _ = done.send(reports);
            });
            Ok(())
        })
    }

    pub fn start(&self) -> HammerResult<()> {
        let result = self.control_blocking_call(start_inner)?;
        if result.is_err() {
            let _ = self.close();
        }
        result
    }

    pub fn close(&self) -> HammerResult<()> {
        let needs_lifecycle_close =
            { self.inner.lock().expect("service mutex poisoned").state != ServiceState::Closed };
        let result = if needs_lifecycle_close {
            let Some(control_handle) = self.control_handle() else {
                return Ok(());
            };
            let inner = Arc::clone(&self.inner);
            match control_handle.call_blocking(move || {
                let mut inner = inner.lock().expect("service mutex poisoned");
                let _dispatch_guard =
                    tracing::dispatcher::set_default(inner.log_factory.dispatch());
                close_inner(&mut inner)
            }) {
                Ok(result) => result,
                Err(err) => Err(err),
            }
        } else {
            Ok(())
        };
        finish_close(&self.inner, result)
    }

    pub fn pause(&self) {
        let _ = self.control_call(|inner| inner.pause.pause());
    }

    pub fn wake(&self) {
        let _ = self.control_call(|inner| inner.pause.wake());
    }

    pub fn reset_network(&self) {
        let _ = self.control_call(|inner| {
            if inner.state != ServiceState::Running {
                return;
            }
            inner.network.reset_network();
            // Drop cached outbound clients (e.g. hysteria2 QUIC connections)
            // alongside the inbound + DNS reset. Without this, sing-box-style
            // `InterfaceUpdated` semantics never reach our outbounds, so a
            // stale cached_client survives every network reset and the next
            // dial blocks on the dead QUIC connection's max_idle_timeout.
            inner.outbound.reset_network();
            #[cfg(feature = "endpoint")]
            inner.endpoint.reset_network();
            inner.dns_router.reset_network();
            inner.outbound.ensure_connected();
        });
    }

    pub fn need_wifi_state(&self) -> bool {
        self.control_call(|inner| inner.network.need_wifi_state())
            .unwrap_or(false)
    }

    pub fn update_wifi_state(&self) {
        let _ = self.control_call(|inner| inner.network.update_wifi_state());
    }

    /// Snapshot the live metric registry. Returns an empty vector when the
    /// service is closed — the control thread already emits a final dump
    /// during shutdown, so callers querying after `close()` should consult
    /// the log instead of digging into post-mortem registry state.
    pub fn metrics_snapshot(&self) -> Vec<MetricSample> {
        self.control_call(|inner| inner.metrics.snapshot())
            .unwrap_or_default()
    }

    pub fn spawn_app_on_worker<F, Fut>(&self, worker: usize, factory: F) -> HammerResult<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }
            inner.data_context.spawn_local_on_worker(worker, factory)
        })?
    }

    pub fn register_app_host<T>(&self, host: Arc<T>) -> HammerResult<()>
    where
        T: AppHost + 'static,
    {
        self.control_call(move |inner| {
            if inner.state == ServiceState::Closed {
                return Err(HammerError::service_closed());
            }

            inner._registry.set::<T>(Arc::clone(&host));
            let lifecycle = host as Arc<dyn Lifecycle>;
            if inner.state == ServiceState::Running {
                start_lifecycle_now(&lifecycle)?;
            }
            inner.lifecycles.push(lifecycle);
            Ok(())
        })?
    }

    fn control_call<R>(
        &self,
        f: impl FnOnce(&mut ServiceInner) -> R + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let control_handle = self
            .control_handle()
            .ok_or_else(HammerError::service_closed)?;
        let inner = Arc::clone(&self.inner);
        control_handle.call(move || {
            let mut inner = inner.lock().expect("service mutex poisoned");
            let _dispatch_guard = tracing::dispatcher::set_default(inner.log_factory.dispatch());
            let data_context = inner.data_context.clone();
            data_context.enter(|| f(&mut inner))
        })
    }

    fn control_blocking_call<R>(
        &self,
        f: impl FnOnce(&mut ServiceInner) -> R + Send + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let control_handle = self
            .control_handle()
            .ok_or_else(HammerError::service_closed)?;
        let inner = Arc::clone(&self.inner);
        control_handle.call_blocking(move || {
            let mut inner = inner.lock().expect("service mutex poisoned");
            let _dispatch_guard = tracing::dispatcher::set_default(inner.log_factory.dispatch());
            let data_context = inner.data_context.clone();
            data_context.enter(|| f(&mut inner))
        })
    }

    fn control_async_call<R>(
        &self,
        timeout: Duration,
        f: impl FnOnce(
            &mut ServiceInner,
            DataPlaneExecutor,
            std::sync::mpsc::Sender<HammerResult<R>>,
        ) -> HammerResult<()>
        + Send
        + 'static,
    ) -> HammerResult<R>
    where
        R: Send + 'static,
    {
        let control_handle = self
            .control_handle()
            .ok_or_else(HammerError::service_closed)?;
        let inner = Arc::clone(&self.inner);
        control_handle.call_async(timeout, move |done| {
            let mut inner = inner.lock().expect("service mutex poisoned");
            let _dispatch_guard = tracing::dispatcher::set_default(inner.log_factory.dispatch());
            let data = inner.data_context.executor();
            f(&mut inner, data, done)
        })
    }

    fn control_handle(&self) -> Option<Arc<ControlThreadHandle>> {
        self.inner
            .lock()
            .expect("service mutex poisoned")
            .control_handle
            .clone()
    }
}

fn new_logger(factory: &Arc<Factory>, id: &str) -> Logger {
    factory.new_logger(id.to_owned())
}

#[inline]
const fn unused_node_id() -> NodeId {
    NodeId::new(0)
}

#[inline]
fn worker_id(worker: usize) -> HammerResult<DataWorkerId> {
    u32::try_from(worker)
        .map(DataWorkerId::new)
        .map_err(|_| HammerError::internal(format!("worker index {worker} does not fit into u32")))
}

fn trace_worker_control(options: &TraceOptions) -> bool {
    options.enabled && !options.inputs.is_empty()
}

#[derive(Debug, Clone, Copy)]
struct ServicePacketGraphDeclarations;

impl Default for ServicePacketGraphDeclarations {
    #[inline]
    fn default() -> Self {
        Self
    }
}

impl ServicePacketGraphDeclarations {
    fn resolve(&self, name: &str) -> Option<NodeId> {
        SERVICE_PACKET_GRAPH_NODES
            .iter()
            .position(|node| *node == name)
            .and_then(|slot| u32::try_from(slot).ok())
            .map(NodeId::new)
    }
}

const SERVICE_PACKET_GRAPH_NODES: &[&str] = &[
    "tun-input-driver-node",
    "ip-input-node",
    "route-match-node",
    "ip-lookup-node",
    "adjacency-rewrite-node",
    "ip-local-node",
    "tcp-input-node",
    "tcp-listen-node",
    "tcp-accept-node",
    "tcp-rcv-process-node",
    "tcp-syn-sent-node",
    "tcp-established-node",
    "tcp-reset-node",
    "ip-receive-node",
    "ip-reassembly-node",
    "icmp-echo-request-node",
    "icmp-error-node",
    "interface-output-node",
    "tun-output-driver-node",
    "drop-node",
];

fn schedule_trace_drain(
    control_handle: Arc<ControlThreadHandle>,
    data_context: DataRuntimeContext,
    sink: TraceRecordSink,
    logger: Logger,
) -> HammerResult<ControlTimerHandle> {
    control_handle.schedule_interval(TRACE_DRAIN_INTERVAL, TRACE_DRAIN_INTERVAL, move || {
        let data_context = data_context.clone();
        let sink = sink.clone();
        let logger = logger.clone();
        async move {
            if let Err(err) =
                data_context.drain_trace_records_on_workers_with_logger(sink, logger.clone())
            {
                logger.warn(format!("drain packet trace records: {err}"));
            }
        }
    })
}

fn schedule_runtime_dump(
    control_handle: Arc<ControlThreadHandle>,
    data_context: DataRuntimeContext,
    interval: Duration,
    logger: Logger,
) -> HammerResult<ControlTimerHandle> {
    control_handle.schedule_interval(interval, interval, move || {
        let data_context = data_context.clone();
        let logger = logger.clone();
        async move {
            match data_context.runtime_stats_on_workers_async().await {
                Ok(stats) => {
                    for line in render_runtime_stats_lines(&stats) {
                        logger.debug(line);
                    }
                }
                Err(err) => logger.warn(format!("dump node runtime stats: {err}")),
            }
        }
    })
}

fn render_runtime_stats_lines(stats: &[(usize, Vec<NodeRuntimeStatsRow>)]) -> Vec<String> {
    let mut lines = Vec::new();
    for (worker, rows) in stats {
        lines.push(format!(
            "show runtime worker={worker} Name State Calls Vectors Suspends AvgNs Vectors/Call MaxNs"
        ));
        let mut rows = rows.iter().filter(|row| row.calls > 0).collect::<Vec<_>>();
        rows.sort_by(|a, b| compare_runtime_rows(a, b));
        for row in rows {
            lines.push(format_runtime_stats_row(*worker, row));
        }
    }
    lines
}

fn format_runtime_stats_row(worker: usize, row: &NodeRuntimeStatsRow) -> String {
    let avg_ns = if row.vectors > 0 {
        row.total_elapsed_ns / row.vectors
    } else if row.calls > 0 {
        row.total_elapsed_ns / row.calls
    } else {
        0
    };
    let vectors_per_call = if row.calls > 0 {
        format!("{:.2}", row.vectors as f64 / row.calls as f64)
    } else {
        "0.00".to_owned()
    };
    format!(
        "show runtime worker={} {:<32} {:<5} {:>10} {:>10} {:>8} {:>10} {:>12} {:>10}",
        worker,
        runtime_row_name(row),
        "active",
        row.calls,
        row.vectors,
        row.suspends,
        avg_ns,
        vectors_per_call,
        row.max_elapsed_ns,
    )
}

fn runtime_row_name(row: &NodeRuntimeStatsRow) -> String {
    row.node_name
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("node-{}", row.node_id.slot()))
}

fn compare_runtime_rows(a: &NodeRuntimeStatsRow, b: &NodeRuntimeStatsRow) -> std::cmp::Ordering {
    match (a.node_name, b.node_name) {
        (Some(a_name), Some(b_name)) => a_name
            .cmp(b_name)
            .then_with(|| a.node_id.slot().cmp(&b.node_id.slot())),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.node_id.slot().cmp(&b.node_id.slot()),
    }
}

/// Wraps every declared `Endpoint` in an `EndpointOutboundAdapter` and
/// registers it into the `OutboundManager` under the endpoint's id. This
/// is what lets `[[dns.servers]] via = "<endpoint-id>"` resolve through
/// the same `outbound.get(...)` path used by every other transport.
///
/// Must run after the user's `[[outbounds]]` are loaded (so duplicate-id
/// validation lines up with the parse-time `validate_unique_ids` check)
/// and before `bind_aggregates()` (so urltest children that reference an
/// endpoint id resolve to the adapter rather than an empty slot).
#[cfg(feature = "endpoint")]
fn register_endpoint_outbound_adapters(
    outbound: &Arc<OutboundManager>,
    endpoint: &Arc<EndpointManager>,
    log_factory: &Arc<Factory>,
) -> HammerResult<()> {
    for component in endpoint.list() {
        let id = component.meta().id().to_owned();
        let logger = new_logger(log_factory, &format!("endpoint-outbound/{id}"));
        let adapter =
            EndpointOutboundAdapter::arc(logger, id.clone(), Arc::clone(component.runtime()));
        outbound.register_outbound(adapter)?;
    }
    Ok(())
}

fn start_inner(inner: &mut ServiceInner) -> HammerResult<()> {
    match inner.state {
        ServiceState::Closed => return Err(HammerError::service_closed()),
        ServiceState::Running => return Ok(()),
        ServiceState::NotStarted => {}
    }
    inner.state = ServiceState::Running;
    let lifecycles = inner.lifecycles.clone();
    for stage in ALL_STAGES {
        for lc in &lifecycles {
            if let Err(err) = lc.start(stage) {
                inner.state = ServiceState::Closed;
                let close_err = close_lifecycles(&lifecycles);
                let combined = HammerError::lifecycle(stage.name(), err.to_string());
                return match close_err {
                    Ok(()) => Err(combined),
                    Err(close_err) => Err(HammerError::lifecycle(
                        stage.name(),
                        format!("{combined}; close after failure: {close_err}"),
                    )),
                };
            }
        }
    }
    Ok(())
}

fn close_inner(inner: &mut ServiceInner) -> HammerResult<()> {
    if inner.state == ServiceState::Closed {
        Ok(())
    } else {
        inner.state = ServiceState::Closed;
        close_lifecycles(&inner.lifecycles)
    }
}

fn close_lifecycles(lifecycles: &[Arc<dyn Lifecycle>]) -> HammerResult<()> {
    let mut errors = Vec::new();
    for lc in lifecycles.iter().rev() {
        if let Err(err) = lc.close() {
            errors.push(format!("{}: {}", lc.name(), err));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(HammerError::internal(errors.join("; ")))
    }
}

fn start_lifecycle_now(lifecycle: &Arc<dyn Lifecycle>) -> HammerResult<()> {
    for stage in ALL_STAGES {
        lifecycle
            .start(stage)
            .map_err(|err| HammerError::lifecycle(stage.name(), err.to_string()))?;
    }
    Ok(())
}

fn finish_close(
    inner: &Arc<Mutex<ServiceInner>>,
    close_result: HammerResult<()>,
) -> HammerResult<()> {
    // Lifecycles have already been closed on hammer-main, so worker-owned tasks
    // have had a chance to emit their shutdown logs while the control handle was
    // still open. The Tokio runtime itself must outlive this step because the
    // control loop is driven by a handle to that same runtime.
    let mut result = close_result;
    let event_subscriptions = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .event_subscriptions
            .drain(..)
            .collect::<Vec<_>>()
    };
    for subscription in &event_subscriptions {
        subscription.cancel();
    }
    drop(event_subscriptions);
    let trace_drain_timer = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .trace_drain_timer
            .take()
    };
    if let Some(timer) = trace_drain_timer {
        timer.cancel_timeout(CONTROL_SHUTDOWN_TIMEOUT);
    }
    let runtime_dump_timer = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .runtime_dump_timer
            .take()
    };
    if let Some(timer) = runtime_dump_timer {
        timer.cancel_timeout(CONTROL_SHUTDOWN_TIMEOUT);
    }
    let control_handle = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .control_handle
            .clone()
    };
    if let Some(control_handle) = control_handle
        && !control_handle.is_closed()
    {
        if !control_handle.flush_timeout(CONTROL_SHUTDOWN_TIMEOUT) {
            result = combine_close_error(result, "control thread log flush timed out");
        }
        if !control_handle.shutdown_timeout(CONTROL_SHUTDOWN_TIMEOUT) {
            result = combine_close_error(result, "control thread shutdown timed out");
        }
    }
    let control_thread = {
        inner
            .lock()
            .expect("service mutex poisoned")
            .control_thread
            .take()
    };
    let mut control_finished = control_thread.is_none();
    if let Some(control_thread) = control_thread {
        match join_control_thread_timeout(control_thread, CONTROL_SHUTDOWN_TIMEOUT) {
            Ok(()) => {
                inner
                    .lock()
                    .expect("service mutex poisoned")
                    .control_handle
                    .take();
                control_finished = true;
            }
            Err(JoinControlThreadError::Timeout(control_thread)) => {
                inner.lock().expect("service mutex poisoned").control_thread = Some(control_thread);
                result = combine_close_error(result, "control thread join timed out");
            }
            Err(JoinControlThreadError::Panicked) => {
                inner
                    .lock()
                    .expect("service mutex poisoned")
                    .control_handle
                    .take();
                result = combine_close_error(result, "control thread panicked");
                // A panicked control thread is dead — its `block_on` has
                // unwound, so the data runtime is safe to abort below.
                control_finished = true;
            }
        }
    }
    // Tear down the data-plane runtime explicitly so any in-flight task
    // is bounded by `DATA_SHUTDOWN_TIMEOUT` rather than the (unbounded)
    // implicit drop. Skipped if the control thread join timed out —
    // its log/metrics work would race with abort. The next `close()`
    // attempt will retry the join and reach this branch.
    if control_finished {
        let data_runtime = {
            let mut inner = inner.lock().expect("service mutex poisoned");
            inner.data_runtime.take()
        };
        if let Some(data_runtime) = data_runtime {
            data_runtime.shutdown_timeout(DATA_SHUTDOWN_TIMEOUT);
        }
    }
    result
}

fn spawn_control_thread(control_loop: ControlThread) -> HammerResult<JoinHandle<()>> {
    // Build the control-plane runtime on the caller's thread so a build
    // failure surfaces synchronously as `HammerError`. Doing it inside
    // the spawned closure would force a panic on a background thread,
    // which the FFI layer cannot translate into a typed error.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| HammerError::internal(format!("build control runtime: {e}")))?;
    thread::Builder::new()
        .name("hammer-main".to_owned())
        .stack_size(CONTROL_THREAD_STACK_SIZE)
        .spawn(move || {
            runtime.block_on(control_loop.run());
            // `runtime` drops here, after the control loop exits. A
            // current_thread runtime drop is non-blocking (it has no
            // worker threads to join), so this is the natural teardown
            // point for the control plane.
        })
        .map_err(|e| HammerError::internal(format!("spawn control thread: {e}")))
}

fn join_control_thread_timeout(
    control_thread: JoinHandle<()>,
    timeout: Duration,
) -> Result<(), JoinControlThreadError> {
    let deadline = Instant::now() + timeout;
    while !control_thread.is_finished() {
        let now = Instant::now();
        if now >= deadline {
            return Err(JoinControlThreadError::Timeout(control_thread));
        }
        thread::sleep((deadline - now).min(Duration::from_millis(10)));
    }
    control_thread
        .join()
        .map_err(|_| JoinControlThreadError::Panicked)
}

enum JoinControlThreadError {
    Timeout(JoinHandle<()>),
    Panicked,
}

impl fmt::Display for JoinControlThreadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinControlThreadError::Timeout(_) => f.write_str("control thread join timed out"),
            JoinControlThreadError::Panicked => f.write_str("control thread panicked"),
        }
    }
}

fn combine_close_error(result: HammerResult<()>, message: impl Into<String>) -> HammerResult<()> {
    let message = message.into();
    match result {
        Ok(()) => Err(HammerError::internal(message)),
        Err(err) => Err(HammerError::internal(format!("{err}; {message}"))),
    }
}

#[cfg(feature = "probe")]
fn build_probe_protocol(protocol: &str) -> HammerResult<ProbeProtocolComponent> {
    crate::ProbeProtocolFactorySet::standard().build(protocol)
}

#[cfg(test)]
impl RuntimeService {
    fn app_control_snapshot_for_test(&self) -> RuntimeAppControlSnapshot {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner
            .app_control
            .snapshot_for_test()
            .expect("snapshot runtime app control")
    }

    fn handle_tcp_worker_event_for_test(&self, event: TcpWorkerEvent) -> HammerResult<()> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.app_control.handle_tcp_worker_event(event)
    }

    fn promote_pending_syn_sent_for_test(
        &self,
        observation: TcpSynSentObservation,
    ) -> HammerResult<()> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.app_control.with_state_mut(move |state| {
            state.promote_pending_syn_sent_connection(
                observation.connection_id,
                observation.local,
                observation.remote,
            )
        })
    }

    fn shared_tcp_listener_installed_for_test(&self, listener_id: TcpListenerId) -> bool {
        let inner = self.inner.lock().expect("service mutex poisoned");
        let control = inner
            .app_control
            .with_state(move |state| Ok(state.shared_tcp_control.clone()))
            .ok()
            .flatten();
        drop(inner);
        control.is_some_and(|control| control.has_listener(listener_id))
    }

    fn shared_tcp_connection_state_for_test(
        &self,
        connection_id: TcpConnectionId,
    ) -> Option<crate::transport::tcp::TcpState> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        let control = inner
            .app_control
            .with_state(move |state| Ok(state.shared_tcp_control.clone()))
            .ok()
            .flatten()
            .and_then(|control| control.connection_state_for_test(connection_id));
        drop(inner);
        control
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tcp::TcpSynSentObservation;
    use hammer_core::StartStage;
    use hammer_core::protocol::tcp::{
        TcpCapabilities, TcpCloseReason, TcpConnectionId, TcpConnectionKey, TcpListenerId,
        TcpListenerKey, TcpWorkerEvent,
    };
    use hammer_runtime::app::{
        AppBufferLease, AppCqeData, AppFlowId, AppObjectRef, AppOpcode, AppSqeData,
        AppSqeDescriptor, AppUserData,
    };
    use hammer_runtime::spawn::with_data_plane_buffers;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    struct NoopPlatform;

    fn wait_for(timeout: Duration, mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if ready() {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(ready(), "condition was not satisfied before timeout");
    }

    impl PlatformInterface for NoopPlatform {
        fn open_tun(&self, _options: hammer_runtime::adapter::TunOptions) -> HammerResult<i32> {
            Ok(42)
        }

        fn use_platform_auto_detect_interface_control(&self) -> bool {
            false
        }

        fn auto_detect_interface_control(&self, _fd: i32) -> HammerResult<()> {
            Ok(())
        }

        fn start_default_interface_monitor(
            &self,
            _listener: Arc<dyn hammer_runtime::adapter::DefaultInterfaceUpdateListener>,
        ) -> HammerResult<()> {
            Ok(())
        }

        fn close_default_interface_monitor(
            &self,
            _listener: Arc<dyn hammer_runtime::adapter::DefaultInterfaceUpdateListener>,
        ) -> HammerResult<()> {
            Ok(())
        }

        fn get_interfaces(&self) -> HammerResult<Vec<hammer_runtime::adapter::NetworkInterface>> {
            Ok(Vec::new())
        }

        fn read_wifi_state(&self) -> Option<hammer_runtime::adapter::WifiState> {
            None
        }
    }

    struct TestAppHost {
        starts: AtomicUsize,
        closes: AtomicUsize,
    }

    impl TestAppHost {
        fn new() -> Self {
            Self {
                starts: AtomicUsize::new(0),
                closes: AtomicUsize::new(0),
            }
        }
    }

    impl Lifecycle for TestAppHost {
        fn name(&self) -> &str {
            "test-app-host"
        }

        fn start(&self, _stage: StartStage) -> Result<(), HammerError> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn close(&self) -> Result<(), HammerError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn minimal_config(extra: &str) -> String {
        format!(
            r#"
[log]
level = "debug"

[tun]
interface_name = "utun"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]
auto_route = false
strict_route = true
mtu = 1400
stack = "disabled"
[dns]
server = "udp://1.1.1.1"

[[outbounds]]
type = "direct"
id = "direct"

[route]
final = "direct"

{extra}
"#
        )
    }

    fn new_test_service(trace: &str) -> Arc<RuntimeService> {
        RuntimeService::new(
            &minimal_config(trace),
            Arc::new(NoopPlatform),
            Arc::new(DiscardWriter),
        )
        .expect("test service should build")
    }

    fn has_trace_drain_timer(service: &RuntimeService) -> bool {
        service
            .inner
            .lock()
            .expect("service mutex poisoned")
            .trace_drain_timer
            .is_some()
    }

    fn has_runtime_dump_timer(service: &RuntimeService) -> bool {
        service
            .inner
            .lock()
            .expect("service mutex poisoned")
            .runtime_dump_timer
            .is_some()
    }

    #[test]
    fn join_control_thread_timeout_returns_without_waiting_forever() {
        let handle = thread::spawn(|| thread::sleep(Duration::from_millis(200)));
        let start = Instant::now();

        let result = join_control_thread_timeout(handle, Duration::from_millis(20));

        assert!(result.is_err(), "slow join should time out");
        assert!(
            start.elapsed() < Duration::from_millis(120),
            "join timeout should return promptly"
        );
    }

    #[test]
    fn join_control_thread_timeout_returns_handle_on_timeout() {
        let handle = thread::spawn(|| thread::sleep(Duration::from_millis(80)));

        let result = join_control_thread_timeout(handle, Duration::from_millis(10));

        let Err(JoinControlThreadError::Timeout(handle)) = result else {
            panic!("expected timeout with recoverable join handle");
        };
        handle.join().expect("thread should still be joinable");
    }

    #[test]
    fn trace_worker_control_is_only_installed_for_enabled_inputs() {
        let mut options = TraceOptions {
            enabled: false,
            record_capacity: 8,
            packet_capacity: 4,
            inputs: vec![config::TraceInputOptions {
                node: "tun-input-driver-node".to_owned(),
                count: 1,
            }],
        };
        assert!(!trace_worker_control(&options));

        options.enabled = true;
        options.inputs.clear();
        assert!(!trace_worker_control(&options));

        options.inputs.push(config::TraceInputOptions {
            node: "tun-input-driver-node".to_owned(),
            count: 1,
        });
        assert!(trace_worker_control(&options));
    }

    #[test]
    fn trace_drain_timer_is_only_installed_for_enabled_inputs() {
        let disabled = new_test_service(
            r#"[trace]
enabled = false

[[trace.inputs]]
node = "tun-input-driver-node"
count = 1
"#,
        );
        assert!(!has_trace_drain_timer(&disabled));
        disabled.close().expect("close disabled trace service");

        let empty = new_test_service(
            r#"[trace]
enabled = true
"#,
        );
        assert!(!has_trace_drain_timer(&empty));
        empty.close().expect("close empty trace service");
    }

    #[test]
    fn runtime_dump_timer_is_only_installed_when_enabled() {
        let disabled = new_test_service("");
        assert!(!has_runtime_dump_timer(&disabled));
        disabled.close().expect("close disabled runtime service");

        let enabled = new_test_service(
            r#"[runtime]
enabled = true
interval = "5s"
"#,
        );
        assert!(has_runtime_dump_timer(&enabled));
        enabled.close().expect("close enabled runtime service");
        assert!(!has_runtime_dump_timer(&enabled));
    }

    #[test]
    fn runtime_stats_rendering_filters_idle_nodes_and_formats_vpp_columns() {
        let lines = render_runtime_stats_lines(&[
            (
                1,
                vec![
                    NodeRuntimeStatsRow {
                        node_id: NodeId::new(0),
                        node_name: Some("idle-node"),
                        error_counters: NodeErrorCounters::default(),
                        calls: 0,
                        vectors: 0,
                        suspends: 0,
                        total_elapsed_ns: 0,
                        max_elapsed_ns: 0,
                    },
                    NodeRuntimeStatsRow {
                        node_id: NodeId::new(2),
                        node_name: Some("ip-input-node"),
                        error_counters: NodeErrorCounters::default(),
                        calls: 2,
                        vectors: 5,
                        suspends: 0,
                        total_elapsed_ns: 50,
                        max_elapsed_ns: 30,
                    },
                    NodeRuntimeStatsRow {
                        node_id: NodeId::new(10),
                        node_name: None,
                        error_counters: NodeErrorCounters::default(),
                        calls: 1,
                        vectors: 1,
                        suspends: 0,
                        total_elapsed_ns: 1,
                        max_elapsed_ns: 1,
                    },
                    NodeRuntimeStatsRow {
                        node_id: NodeId::new(2),
                        node_name: None,
                        error_counters: NodeErrorCounters::default(),
                        calls: 1,
                        vectors: 1,
                        suspends: 0,
                        total_elapsed_ns: 1,
                        max_elapsed_ns: 1,
                    },
                ],
            ),
            (2, Vec::new()),
        ]);

        assert_eq!(lines.len(), 5);
        assert!(
            lines[0].contains("Name State Calls Vectors Suspends AvgNs Vectors/Call MaxNs"),
            "{lines:?}"
        );
        assert!(!lines.iter().any(|line| line.contains("idle-node")));
        assert!(lines[1].contains("ip-input-node"), "{lines:?}");
        assert!(lines[1].contains("active"), "{lines:?}");
        assert!(lines[1].contains("         2"), "{lines:?}");
        assert!(lines[1].contains("         5"), "{lines:?}");
        assert!(
            lines[4].contains(
                "show runtime worker=2 Name State Calls Vectors Suspends AvgNs Vectors/Call MaxNs"
            ),
            "{lines:?}"
        );
        assert!(lines[1].contains("        10"), "{lines:?}");
        assert!(lines[1].contains("        2.50"), "{lines:?}");
        assert!(lines[1].contains("        30"), "{lines:?}");
        assert!(lines[2].contains("node-2"), "{lines:?}");
        assert!(lines[3].contains("node-10"), "{lines:?}");
    }

    #[test]
    fn service_packet_graph_resolves_tcp_nodes() {
        let graph = ServicePacketGraphDeclarations::default();

        assert!(graph.resolve("tcp-input-node").is_some());
        assert!(graph.resolve("tcp-listen-node").is_some());
        assert!(graph.resolve("tcp-rcv-process-node").is_some());
        assert!(graph.resolve("tcp-syn-sent-node").is_some());
        assert!(graph.resolve("tcp-established-node").is_some());
        assert!(graph.resolve("tcp-reset-node").is_some());
    }

    #[test]
    fn runtime_service_app_context_runs_app_flow_on_service_data_workers() {
        let service = new_test_service("");
        let app = service.app_context();
        let flow = AppFlowId::new(3);

        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime")
            .block_on(async {
                app.spawn_on_flow(flow, move |worker| async move {
                    let recv_future = worker.runtime().recv();
                    let recv_sqe = worker
                        .backend()
                        .next_sqe_descriptor()
                        .await
                        .expect("recv sqe descriptor");
                    let runtime = with_data_plane_buffers(Clone::clone);
                    let index = runtime
                        .alloc_index_with_bytes(Default::default(), b"service-app-context")
                        .expect("alloc buffer");
                    worker
                        .backend()
                        .complete_recv(AppBufferLease::from_buffer(runtime.clone(), index))
                        .await
                        .expect("complete recv");
                    assert_eq!(recv_sqe.opcode(), hammer_runtime::app::AppOpcode::Recv);
                    let recv = recv_future.await.expect("recv app payload");
                    let payload = recv.lease().copy_current().expect("payload copy");
                    recv.release();
                    (
                        worker.owner_worker(),
                        std::thread::current()
                            .name()
                            .map(ToOwned::to_owned)
                            .unwrap_or_default(),
                        payload,
                    )
                })
                .await
                .expect("spawn app flow")
            });

        assert_eq!(result.0, 1);
        assert!(result.1.contains("hammer-data-1"), "thread={}", result.1);
        assert_eq!(result.2, b"service-app-context");
        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_app_context_installs_control_backend_and_publishes_binds() {
        let service = new_test_service("");
        let app = service.app_context();
        let listener = app
            .bind_tcp_listener("127.0.0.1:7000".parse().expect("tcp bind"), 1)
            .expect("bind tcp listener");
        let socket = app
            .bind_udp_socket("127.0.0.1:9000".parse().expect("udp bind"), 0)
            .expect("bind udp socket");

        assert_eq!(
            app.owner_worker_for_socket(listener)
                .expect("tcp listener owner"),
            1
        );
        assert_eq!(
            app.owner_worker_for_socket(socket)
                .expect("udp socket owner"),
            0
        );

        let state = service.app_control_snapshot_for_test();
        assert_eq!(state.tcp_listeners.len(), 1);
        assert_eq!(state.tcp_listeners[0].socket, listener);
        assert_eq!(state.udp_sockets.len(), 1);
        assert_eq!(state.udp_sockets[0].socket, socket);

        let published = state
            .tcp_lookup
            .lookup_listener_v4(TcpV4ListenerKey::new(0, Ipv4Addr::new(127, 0, 0, 1), 7000))
            .expect("published tcp listener lookup");
        assert_eq!(published.kind, TcpLookupKind::Listener);
        assert_eq!(published.owner_worker, DataWorkerId::new(1));
        assert_eq!(published.id, state.tcp_listeners[0].lookup_id);

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_app_context_rebinds_after_close_socket() {
        let service = new_test_service("");
        let app = service.app_context();
        let listener = app
            .bind_tcp_listener("127.0.0.1:7100".parse().expect("tcp bind"), 0)
            .expect("bind tcp listener");
        let socket = app
            .bind_udp_socket("127.0.0.1:9100".parse().expect("udp bind"), 0)
            .expect("bind udp socket");

        let tcp_err = app
            .bind_tcp_listener("127.0.0.1:7100".parse().expect("tcp bind"), 0)
            .expect_err("duplicate tcp listener must fail");
        assert!(
            tcp_err.to_string().contains("already registered"),
            "err={tcp_err}"
        );
        let udp_err = app
            .bind_udp_socket("127.0.0.1:9100".parse().expect("udp bind"), 0)
            .expect_err("duplicate udp socket must fail");
        assert!(
            udp_err.to_string().contains("already registered"),
            "err={udp_err}"
        );

        app.close_socket(listener).expect("close tcp listener");
        app.close_socket(socket).expect("close udp socket");
        assert!(
            app.owner_worker_for_socket(listener).is_err(),
            "stale tcp listener handle must be invalid after close"
        );
        assert!(
            app.owner_worker_for_socket(socket).is_err(),
            "stale udp socket handle must be invalid after close"
        );

        let rebound_listener = app
            .bind_tcp_listener("127.0.0.1:7100".parse().expect("tcp bind"), 1)
            .expect("rebind tcp listener");
        let rebound_socket = app
            .bind_udp_socket("127.0.0.1:9100".parse().expect("udp bind"), 1)
            .expect("rebind udp socket");
        assert_eq!(
            listener.value() as u32,
            rebound_listener.value() as u32,
            "tcp listener rebind should reuse freed descriptor slot"
        );
        assert_ne!(
            (listener.value() >> 32) as u32,
            (rebound_listener.value() >> 32) as u32,
            "tcp listener rebind should advance descriptor generation"
        );
        assert_eq!(
            socket.value() as u32,
            rebound_socket.value() as u32,
            "udp socket rebind should reuse freed descriptor slot"
        );
        assert_ne!(
            (socket.value() >> 32) as u32,
            (rebound_socket.value() >> 32) as u32,
            "udp socket rebind should advance descriptor generation"
        );

        assert_eq!(
            app.owner_worker_for_socket(rebound_listener)
                .expect("rebound tcp listener owner"),
            1
        );
        assert_eq!(
            app.owner_worker_for_socket(rebound_socket)
                .expect("rebound udp socket owner"),
            1
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_accepts_tcp_listener_into_established_lookup() {
        let service = new_test_service("");
        let app = service.app_context();
        let listener = app
            .bind_tcp_listener("127.0.0.1:7200".parse().expect("tcp bind"), 1)
            .expect("bind tcp listener");
        let remote: SocketAddr = "198.51.100.72:40720".parse().expect("remote addr");
        let local: SocketAddr = "127.0.0.1:7200".parse().expect("local addr");
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (tx, rx) = std::sync::mpsc::channel();

        service
            .spawn_app_on_worker(1, {
                let app = app.clone();
                move || async move {
                    let backend = app
                        .local_backend_for_socket(listener)
                        .expect("listener backend");
                    backend
                        .try_push_sqe_descriptor(AppSqeDescriptor::new(
                            AppOpcode::Accept,
                            AppUserData::new(91),
                            AppObjectRef::Socket(listener),
                            AppSqeData::Accept,
                        ))
                        .expect("push accept sqe");
                    ready_tx.send(()).expect("signal accept ready");

                    let accept_cqe = backend
                        .next_cqe_descriptor()
                        .await
                        .expect("accept cqe descriptor");
                    tx.send(accept_cqe).expect("send accept result");
                }
            })
            .expect("spawn accept worker");

        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("wait accept ready");
        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::IncomingConnection {
                listener_id: TcpListenerId::new(1),
                listener: TcpListenerKey::v4(0, Ipv4Addr::new(127, 0, 0, 1), 7200),
                key: TcpConnectionKey::v4(
                    0,
                    Ipv4Addr::new(127, 0, 0, 1),
                    7200,
                    Ipv4Addr::new(198, 51, 100, 72),
                    40_720,
                ),
                capabilities: TcpCapabilities::default(),
            })
            .expect("accept tcp connection");
        let accept_cqe = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive accept result");
        match accept_cqe.payload() {
            AppCqeData::Accepted {
                listener: cqe_listener,
                flow: cqe_flow,
            } => {
                assert_eq!(cqe_listener, listener);
                assert_eq!(app.owner_worker_for_flow(cqe_flow).expect("flow owner"), 1);
            }
            other => panic!("unexpected accept cqe payload: {other:?}"),
        }

        let state = service.app_control_snapshot_for_test();
        let published = state
            .tcp_lookup
            .lookup_connection_v4(TcpV4ConnectionKey::new(
                0,
                local
                    .ip()
                    .to_string()
                    .parse::<Ipv4Addr>()
                    .expect("local ipv4"),
                local.port(),
                remote
                    .ip()
                    .to_string()
                    .parse::<Ipv4Addr>()
                    .expect("remote ipv4"),
                remote.port(),
            ))
            .expect("published tcp connection lookup");
        assert_eq!(published.kind, TcpLookupKind::EstablishedConnection);
        assert_eq!(published.owner_worker, DataWorkerId::new(1));

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_connect_publishes_pending_syn_sent_lookup() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

        let state = service.app_control_snapshot_for_test();
        let pending = state
            .tcp_lookup
            .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                0,
                49_152,
                Ipv4Addr::new(198, 51, 100, 88),
                443,
            ))
            .expect("published pending syn-sent lookup");
        assert_eq!(pending.kind, TcpLookupKind::SynSentConnection);
        assert_eq!(pending.owner_worker, DataWorkerId::new(1));

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_bind_tcp_listener_updates_shared_tcp_control_plane() {
        let service = new_test_service("");
        let app = service.app_context();
        let bind: SocketAddr = "127.0.0.1:7301".parse().expect("listener bind");

        let listener = app.bind_tcp_listener(bind, 1).expect("bind tcp listener");
        let listener_id = {
            let state = service.app_control_snapshot_for_test();
            let registration = state
                .tcp_listeners
                .iter()
                .find(|registration| registration.socket == listener)
                .expect("listener registration");
            TcpListenerId::new(registration.lookup_id as u64)
        };

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_listener_installed_for_test(listener_id)
        });
        assert!(service.shared_tcp_listener_installed_for_test(listener_id));

        app.close_socket(listener).expect("close tcp listener");

        wait_for(Duration::from_secs(1), || {
            !service.shared_tcp_listener_installed_for_test(listener_id)
        });
        assert!(!service.shared_tcp_listener_installed_for_test(listener_id));

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_connect_promotes_pending_syn_sent_lookup_to_established_lookup() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local addr");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

        let pending = service
            .app_control_snapshot_for_test()
            .tcp_lookup
            .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                0,
                local.port(),
                Ipv4Addr::new(198, 51, 100, 88),
                443,
            ))
            .expect("published pending syn-sent lookup");

        service
            .promote_pending_syn_sent_for_test(TcpSynSentObservation::new(
                pending.id,
                peer,
                local,
                crate::transport::tcp::TcpState::SynSent,
                crate::transport::tcp::TcpState::Established,
            ))
            .expect("promote syn-sent connection");

        let state = service.app_control_snapshot_for_test();
        assert!(
            state
                .tcp_lookup
                .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                    0,
                    local.port(),
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_none(),
            "pending syn-sent lookup must be revoked after promotion"
        );
        let established = state
            .tcp_lookup
            .lookup_connection_v4(TcpV4ConnectionKey::new(
                0,
                Ipv4Addr::new(192, 0, 2, 44),
                local.port(),
                Ipv4Addr::new(198, 51, 100, 88),
                443,
            ))
            .expect("published established tcp lookup");
        assert_eq!(established.kind, TcpLookupKind::EstablishedConnection);
        assert_eq!(established.id, pending.id);
        assert_eq!(established.owner_worker, DataWorkerId::new(1));

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_state_events_update_shared_tcp_control_plane() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let connection_id = TcpConnectionId::new(191);

        let _flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::StateChanged {
                connection_id,
                key: TcpConnectionKey::v4(
                    0,
                    Ipv4Addr::new(192, 0, 2, 44),
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ),
                state: crate::transport::tcp::TcpState::Established,
            })
            .expect("ingest state change");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });
        assert_eq!(
            service.shared_tcp_connection_state_for_test(connection_id),
            Some(crate::transport::tcp::TcpState::Established)
        );

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::Closed {
                connection_id,
                reason: TcpCloseReason::LocalRequest,
            })
            .expect("ingest close event");

        wait_for(Duration::from_secs(1), || {
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none()
        });
        assert!(
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none()
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_state_changed_promotes_pending_connect_to_established_lookup() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local addr");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");

        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::StateChanged {
                connection_id: TcpConnectionId::new(91),
                key: TcpConnectionKey::v4(
                    0,
                    Ipv4Addr::new(192, 0, 2, 44),
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ),
                state: crate::transport::tcp::TcpState::Established,
            })
            .expect("promote pending connect via state change");

        wait_for(Duration::from_secs(1), || {
            let state = service.app_control_snapshot_for_test();
            state
                .tcp_lookup
                .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                    0,
                    local.port(),
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_none()
                && state
                    .tcp_lookup
                    .lookup_connection_v4(TcpV4ConnectionKey::new(
                        0,
                        Ipv4Addr::new(192, 0, 2, 44),
                        local.port(),
                        Ipv4Addr::new(198, 51, 100, 88),
                        443,
                    ))
                    .is_some()
        });

        let state = service.app_control_snapshot_for_test();
        let established = state
            .tcp_lookup
            .lookup_connection_v4(TcpV4ConnectionKey::new(
                0,
                Ipv4Addr::new(192, 0, 2, 44),
                local.port(),
                Ipv4Addr::new(198, 51, 100, 88),
                443,
            ))
            .expect("published established tcp lookup");
        assert_eq!(established.kind, TcpLookupKind::EstablishedConnection);
        assert_eq!(established.owner_worker, DataWorkerId::new(1));

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_closed_reclaims_active_connect_lookup_and_ephemeral_port() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local addr");
        let connection_id = TcpConnectionId::new(92);

        let first = app
            .connect_tcp_stream(peer, 1)
            .expect("first connect tcp stream");
        assert_eq!(app.owner_worker_for_flow(first).expect("flow owner"), 1);

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::StateChanged {
                connection_id,
                key: TcpConnectionKey::v4(
                    0,
                    Ipv4Addr::new(192, 0, 2, 44),
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ),
                state: crate::transport::tcp::TcpState::Established,
            })
            .expect("bind established active connect");
        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::Closed {
                connection_id,
                reason: TcpCloseReason::LocalRequest,
            })
            .expect("close active connect");

        wait_for(Duration::from_secs(1), || {
            let state = service.app_control_snapshot_for_test();
            state
                .tcp_lookup
                .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                    0,
                    local.port(),
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_none()
                && state
                    .tcp_lookup
                    .lookup_connection_v4(TcpV4ConnectionKey::new(
                        0,
                        Ipv4Addr::new(192, 0, 2, 44),
                        local.port(),
                        Ipv4Addr::new(198, 51, 100, 88),
                        443,
                    ))
                    .is_none()
        });

        let second = app
            .connect_tcp_stream(peer, 1)
            .expect("second connect tcp stream");
        assert_ne!(first, second);

        let reused = service
            .app_control_snapshot_for_test()
            .tcp_lookup
            .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                0,
                49_152,
                Ipv4Addr::new(198, 51, 100, 88),
                443,
            ))
            .expect("reused pending syn-sent lookup");
        assert_eq!(reused.kind, TcpLookupKind::SynSentConnection);
        assert_eq!(reused.owner_worker, DataWorkerId::new(1));

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_spawns_app_task_on_requested_worker() {
        let service = new_test_service("");
        let (tx, rx) = std::sync::mpsc::channel();

        service
            .spawn_app_on_worker(1, move || async move {
                tx.send(
                    std::thread::current()
                        .name()
                        .map(ToOwned::to_owned)
                        .unwrap_or_default(),
                )
                .expect("send worker thread");
            })
            .expect("spawn app task");

        let thread = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive worker thread");
        assert!(thread.contains("hammer-data-1"), "thread={thread}");
        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_rejects_invalid_app_worker_index() {
        let service = new_test_service("");
        let err = service
            .spawn_app_on_worker(99, || async {})
            .expect_err("invalid worker must fail");
        assert!(err.to_string().contains("invalid app worker"), "err={err}");
        assert!(err.to_string().contains("worker_count="), "err={err}");
        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_register_app_host_defers_start_until_service_start() {
        let service = new_test_service("");
        let host = Arc::new(TestAppHost::new());

        service
            .register_app_host(Arc::clone(&host))
            .expect("register app host");

        assert_eq!(host.starts.load(Ordering::SeqCst), 0);
        service.start().expect("start service");
        assert_eq!(host.starts.load(Ordering::SeqCst), ALL_STAGES.len());
        service.close().expect("close service");
        assert_eq!(host.closes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn runtime_service_register_app_host_starts_immediately_when_running() {
        let service = new_test_service("");
        service.start().expect("start service");
        let host = Arc::new(TestAppHost::new());

        service
            .register_app_host(Arc::clone(&host))
            .expect("register running app host");

        assert_eq!(host.starts.load(Ordering::SeqCst), ALL_STAGES.len());
        service.close().expect("close service");
        assert_eq!(host.closes.load(Ordering::SeqCst), 1);
    }
}

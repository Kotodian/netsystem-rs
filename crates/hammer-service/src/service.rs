use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
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
    TcpCloseReason, TcpConnectionId, TcpConnectionKey, TcpHandshakeObservation, TcpListenerId,
    TcpSeq, TcpShutdownDirection, TcpWorkerEvent,
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
use crate::transport::tcp::output::{tcp_available_send_window, tcp_payload_len_in_send_window};
use crate::transport::tcp::{
    TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TcpAcceptControlPlane, TcpAcceptNext,
    TcpAcceptRegistration, TcpCongestionRegistry, TcpConnectionSnapshot, TcpConnectionSnapshotPool,
    TcpConnectionState, TcpEstablishedAckObservation, TcpEstablishedBackend,
    TcpEstablishedControlPlane, TcpEstablishedNext, TcpEstablishedObservation,
    TcpInputControlPlane, TcpInputNext, TcpListenerConfig, TcpLookupId, TcpLookupKind,
    TcpLookupSnapshot, TcpLookupValue, TcpOutputBackend, TcpOutputBackendSlot,
    TcpOutputRetransmitQueue, TcpOutputSegment, TcpRcvProcessControlPlane, TcpRcvProcessNext,
    TcpSynSentBackend, TcpSynSentControlPlane, TcpSynSentNext, TcpSynSentObservation,
    TcpSynSentRegistration, TcpV4ConnectionKey, TcpV4ListenerKey, TcpV4PendingConnectionKey,
    TcpV6ConnectionKey, TcpV6ListenerKey, TcpV6PendingConnectionKey,
    build_tcp_output_segment_with_flags,
};
use crate::transport::udp::input::UdpAppRegistration;
use crate::transport::udp::{UdpInputControlPlane, UdpInputNext};
use crate::{DnsRouter, DnsTransportManager, Router};
use tokio::sync::Notify;

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
const CLOSED_TCP_CONNECTION_TOMBSTONE_LIMIT: usize = 1024;
const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const TCP_OUTPUT_RETRANSMIT_TIMEOUT: Duration = Duration::from_millis(50);
const DEFAULT_TCP_WINDOW: u32 = u16::MAX as u32;

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

#[derive(Debug, Clone)]
struct TcpConnectionRegistration {
    #[allow(dead_code)]
    flow: AppFlowId,
    connection_id: Option<TcpConnectionId>,
    lookup_id: TcpLookupId,
    owner_worker: DataWorkerId,
    state: hammer_core::protocol::tcp::TcpState,
    shutdown: Option<(TcpShutdownDirection, TcpCloseReason)>,
    local_port: u16,
    local: Option<SocketAddr>,
    remote: SocketAddr,
    listener: Option<AppSocketId>,
    target: AppIngressTarget,
    pending_fin: bool,
    pending_send_payloads: InfraVec<std::vec::Vec<u8>>,
    retransmit_queue: TcpOutputRetransmitQueue,
    retransmit_pending: bool,
    iss: u32,
    irs: u32,
    snd_una: u32,
    snd_nxt: u32,
    snd_wnd: u32,
    rcv_nxt: u32,
    rcv_wnd: u32,
    send_state_initialized: bool,
    receive_state_initialized: bool,
}

#[derive(Debug)]
struct TcpTransportSendQueue {
    connection_id: TcpConnectionId,
    payloads: InfraVec<std::vec::Vec<u8>>,
}

#[derive(Debug)]
struct TcpOutputWorkItem {
    connection_id: TcpConnectionId,
    segment: TcpOutputSegment,
    payload_len: usize,
    include_fin: bool,
    retransmit: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpOutputDrainAction {
    Emit,
    Idle,
    Closed,
}

#[derive(Clone, Default)]
struct TcpOutputSignalRegistry {
    inner: Arc<Mutex<HashMap<u64, Arc<Notify>>>>,
}

#[derive(Clone, Default)]
struct TcpOutputRetransmitTimerRegistry {
    inner: Arc<Mutex<HashMap<u64, ControlTimerHandle>>>,
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
    next_tcp_connection_id: u64,
    next_tcp_ephemeral_port: u16,
    tcp_congestion: TcpCongestionRegistry,
    tcp_accept_control: Option<TcpAcceptControlPlane>,
    tcp_syn_sent_control: Option<TcpSynSentControlPlane>,
    shared_tcp_control: Option<SharedTcpControlPlane>,
    tcp_control: TcpInputControlPlane,
    tcp_established_control: TcpEstablishedControlPlane,
    tcp_rcv_process_control: TcpRcvProcessControlPlane,
    udp_control: UdpInputControlPlane,
    tcp_lookup: TcpLookupSnapshot,
    tcp_connection_snapshot: TcpConnectionSnapshotPool,
    tcp_listeners: InfraVec<TcpListenerRegistration>,
    tcp_listener_slots: FlatHashTable<u64, usize>,
    tcp_connections: InfraVec<TcpConnectionRegistration>,
    tcp_transport_send_queues: InfraVec<TcpTransportSendQueue>,
    closed_tcp_connections: InfraVec<TcpConnectionId>,
    udp_sockets: InfraVec<UdpSocketRegistration>,
    udp_socket_slots: FlatHashTable<u64, usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TcpRetransmitTimerAction {
    None,
    Cancel,
    Rearm,
}

struct TcpEstablishedAckProgressResult {
    connection_id: TcpConnectionId,
    timer_action: TcpRetransmitTimerAction,
    wake_output_flow: Option<AppFlowId>,
    shared_update: Option<(
        SharedTcpControlPlane,
        TcpCongestionRegistry,
        TcpConnectionKey,
        hammer_core::protocol::tcp::TcpState,
    )>,
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
    tcp_output: TcpOutputBackendSlot,
    tcp_output_signals: TcpOutputSignalRegistry,
    tcp_retransmit_timers: TcpOutputRetransmitTimerRegistry,
}

#[cfg(test)]
#[derive(Clone)]
struct RuntimeAppControlSnapshot {
    tcp_listeners: InfraVec<RuntimeTcpListenerSnapshot>,
    udp_sockets: InfraVec<UdpSocketRegistration>,
    tcp_lookup: TcpLookupSnapshot,
    tcp_connections: TcpConnectionSnapshotPool,
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

struct CloseTcpFlowResult {
    shared_action: Option<hammer_core::protocol::tcp::TcpControlPlaneAction>,
}

struct ConnectTcpFlowResult {
    flow: AppFlowId,
    shared_actions: InfraVec<hammer_core::protocol::tcp::TcpControlPlaneAction>,
}

struct IncomingTcpConnectionResult {
    shared_actions: InfraVec<hammer_core::protocol::tcp::TcpControlPlaneAction>,
    accepted_flow: Option<(AppContext, AppFlowId, usize)>,
    wake_output_flow: Option<AppFlowId>,
}

impl std::fmt::Debug for IncomingTcpConnectionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingTcpConnectionResult")
            .field("shared_actions", &self.shared_actions)
            .field(
                "accepted_flow",
                &self
                    .accepted_flow
                    .as_ref()
                    .map(|(_, flow, owner_worker)| (flow.value(), *owner_worker)),
            )
            .field(
                "wake_output_flow",
                &self.wake_output_flow.map(AppFlowId::value),
            )
            .finish()
    }
}

struct PromotePendingSynSentResult {
    shared_actions: InfraVec<hammer_core::protocol::tcp::TcpControlPlaneAction>,
    wake_output_flow: Option<AppFlowId>,
}

impl TcpOutputSignalRegistry {
    fn signal(&self, flow: AppFlowId) -> Arc<Notify> {
        let notify = {
            let mut signals = self.inner.lock().expect("tcp output signals poisoned");
            Arc::clone(
                signals
                    .entry(flow.value())
                    .or_insert_with(|| Arc::new(Notify::new())),
            )
        };
        notify.notify_one();
        notify
    }

    fn subscribe(&self, flow: AppFlowId) -> Arc<Notify> {
        let mut signals = self.inner.lock().expect("tcp output signals poisoned");
        Arc::clone(
            signals
                .entry(flow.value())
                .or_insert_with(|| Arc::new(Notify::new())),
        )
    }
}

impl TcpOutputRetransmitTimerRegistry {
    fn replace(&self, connection_id: TcpConnectionId, handle: ControlTimerHandle) {
        let mut timers = self.inner.lock().expect("tcp retransmit timers poisoned");
        if let Some(previous) = timers.insert(connection_id.get(), handle) {
            let _ = previous.cancel();
        }
    }

    fn cancel(&self, connection_id: TcpConnectionId) {
        let previous = self
            .inner
            .lock()
            .expect("tcp retransmit timers poisoned")
            .remove(&connection_id.get());
        if let Some(previous) = previous {
            let _ = previous.cancel();
        }
    }

    fn clear_fired(&self, connection_id: TcpConnectionId) {
        let _ = self
            .inner
            .lock()
            .expect("tcp retransmit timers poisoned")
            .remove(&connection_id.get());
    }

    fn cancel_all(&self) {
        let handles = self
            .inner
            .lock()
            .expect("tcp retransmit timers poisoned")
            .drain()
            .map(|(_, handle)| handle)
            .collect::<Vec<_>>();
        for handle in handles {
            let _ = handle.cancel();
        }
    }
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
        let tcp_established_control =
            TcpEstablishedControlPlane::new(TcpEstablishedNext::nodes(unused_node_id()));
        let tcp_rcv_process_control =
            TcpRcvProcessControlPlane::new(TcpRcvProcessNext::nodes(unused_node_id()));

        Ok(Self {
            sockets: DescriptorTable::new(),
            flows: DescriptorTable::new(),
            next_tcp_lookup_id: 1,
            next_tcp_connection_id: 1,
            next_tcp_ephemeral_port: 49_152,
            tcp_congestion: TcpCongestionRegistry::default(),
            tcp_accept_control: None,
            tcp_syn_sent_control: None,
            shared_tcp_control: None,
            tcp_control,
            tcp_established_control,
            tcp_rcv_process_control,
            udp_control: UdpInputControlPlane::new(UdpInputNext::nodes(
                unused_node_id(),
                unused_node_id(),
                unused_node_id(),
            )),
            tcp_lookup: TcpLookupSnapshot::empty(),
            tcp_connection_snapshot: TcpConnectionSnapshotPool::empty(),
            tcp_listeners: InfraVec::new(),
            tcp_listener_slots: FlatHashTable::new(),
            tcp_connections: InfraVec::new(),
            tcp_transport_send_queues: InfraVec::new(),
            closed_tcp_connections: InfraVec::new(),
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

    fn install_tcp_established_backend<O>(&mut self, backend: Arc<O>)
    where
        O: TcpEstablishedBackend + 'static,
    {
        self.tcp_established_control.install_backend(backend);
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

    fn handle_incoming_tcp_connection(
        &mut self,
        listener_registration: TcpListenerRegistration,
        remote: SocketAddr,
        local: SocketAddr,
        observation: Option<TcpHandshakeObservation>,
    ) -> HammerResult<IncomingTcpConnectionResult> {
        if let Some(index) = self.tcp_connections.iter().position(|registration| {
            registration.local == Some(local) && registration.remote == remote
        }) {
            let current = self.tcp_connections.get(index).cloned().ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
            if current.state == hammer_core::protocol::tcp::TcpState::Established {
                let wake_output_flow = {
                    let registration = self.tcp_connections.get_mut(index).ok_or_else(|| {
                        HammerError::internal("tcp connection registration slot is invalid")
                    })?;
                    let was_output_ready = tcp_transport_state_ready(registration);
                    if let Some(observation) = observation {
                        tcp_apply_passive_accept_observation(registration, observation)?;
                    }
                    (!was_output_ready
                        && tcp_transport_state_ready(registration)
                        && tcp_state_allows_transport_output(registration.state))
                    .then_some(registration.flow)
                };
                self.publish_tcp_lookup()?;
                self.publish_tcp_app_ingress()?;
                return Ok(IncomingTcpConnectionResult {
                    shared_actions: InfraVec::new(),
                    accepted_flow: None,
                    wake_output_flow,
                });
            }
            if current.state != hammer_core::protocol::tcp::TcpState::SynRcvd {
                return Err(HammerError::internal(format!(
                    "tcp incoming connection {} -> {} cannot promote from state {:?}",
                    remote, local, current.state
                )));
            }

            let mut complete_accept = None;
            let (connection_id, wake_output_flow) = {
                let registration = self.tcp_connections.get_mut(index).ok_or_else(|| {
                    HammerError::internal("tcp connection registration slot is invalid")
                })?;
                let was_output_ready = tcp_transport_state_ready(registration);
                registration.state = hammer_core::protocol::tcp::TcpState::Established;
                registration.local = Some(local);
                if let Some(observation) = observation {
                    tcp_apply_passive_accept_observation(registration, observation)?;
                }
                if let Some(listener) = registration.listener.take() {
                    complete_accept = Some((
                        registration.target.app().clone(),
                        listener,
                        registration.flow,
                        registration.owner_worker,
                    ));
                }
                (
                    registration.connection_id.ok_or_else(|| {
                        HammerError::internal(
                            "passive-open tcp connection is missing connection id",
                        )
                    })?,
                    (!was_output_ready
                        && tcp_transport_state_ready(registration)
                        && tcp_state_allows_transport_output(registration.state))
                    .then_some(registration.flow),
                )
            };

            let accepted_flow = if let Some((app, listener, flow, owner_worker)) = complete_accept {
                app.try_complete_accept(listener, flow)?;
                Some((app, flow, owner_worker.slot()))
            } else {
                None
            };
            self.publish_tcp_lookup()?;
            self.publish_tcp_app_ingress()?;

            let mut shared_actions = InfraVec::new();
            if self.shared_tcp_control.is_some() {
                let congestion = TcpConnectionState::new(&self.tcp_congestion, None)?;
                shared_actions.push(congestion.upsert_connection_action(
                    connection_id,
                    socket_addrs_to_connection_key(local, remote)?,
                    hammer_core::protocol::tcp::TcpState::Established,
                ));
            }
            return Ok(IncomingTcpConnectionResult {
                shared_actions,
                accepted_flow,
                wake_output_flow,
            });
        }

        self.ensure_connection_absent(local, remote)?;
        let flow = self.alloc_flow();
        let lookup_id = self.alloc_tcp_lookup_id()?;
        let connection_id = self.alloc_tcp_connection_id();
        let listener_app = listener_registration.app.clone();
        self.tcp_connections.push(TcpConnectionRegistration {
            flow,
            connection_id: Some(connection_id),
            lookup_id,
            owner_worker: listener_registration.owner_worker,
            state: hammer_core::protocol::tcp::TcpState::SynRcvd,
            shutdown: None,
            local_port: local.port(),
            local: Some(local),
            remote,
            listener: Some(listener_registration.socket),
            target: AppIngressTarget::flow(listener_app.clone(), flow),
            pending_fin: false,
            pending_send_payloads: InfraVec::new(),
            retransmit_queue: TcpOutputRetransmitQueue::new(),
            retransmit_pending: false,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: 0,
            rcv_nxt: 0,
            rcv_wnd: DEFAULT_TCP_WINDOW,
            send_state_initialized: false,
            receive_state_initialized: false,
        });
        if let Some(observation) = observation {
            let registration = self
                .tcp_connections
                .last_mut()
                .ok_or_else(|| HammerError::internal("missing passive-open registration"))?;
            tcp_apply_passive_accept_observation(registration, observation)?;
        }
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;

        let mut shared_actions = InfraVec::new();
        if self.shared_tcp_control.is_some() {
            let congestion = TcpConnectionState::new(&self.tcp_congestion, None)?;
            shared_actions.push(congestion.install_connection_action(
                connection_id,
                socket_addrs_to_connection_key(local, remote)?,
                hammer_core::protocol::tcp::TcpState::SynRcvd,
            ));
        }
        Ok(IncomingTcpConnectionResult {
            shared_actions,
            accepted_flow: None,
            wake_output_flow: None,
        })
    }

    fn handle_incoming_tcp_connection_by_listener_id(
        &mut self,
        listener_id: TcpListenerId,
        remote: SocketAddr,
        local: SocketAddr,
        observation: Option<TcpHandshakeObservation>,
    ) -> HammerResult<IncomingTcpConnectionResult> {
        let listener_registration = self
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
        self.handle_incoming_tcp_connection(listener_registration, remote, local, observation)
    }

    fn connect_tcp_stream(
        &mut self,
        app: &AppContext,
        peer: SocketAddr,
        owner_worker: usize,
    ) -> HammerResult<ConnectTcpFlowResult> {
        let flow = self.alloc_flow();
        let lookup_id = self.alloc_tcp_lookup_id()?;
        let connection_id = self.alloc_tcp_connection_id();
        let local_port = self.alloc_tcp_ephemeral_port()?;
        let local = SocketAddr::new(unspecified_ip_for(peer.ip()), local_port);
        self.tcp_connections.push(TcpConnectionRegistration {
            flow,
            connection_id: Some(connection_id),
            lookup_id,
            owner_worker: worker_id(owner_worker)?,
            state: hammer_core::protocol::tcp::TcpState::SynSent,
            shutdown: None,
            local_port,
            local: None,
            remote: peer,
            listener: None,
            target: AppIngressTarget::flow(app.clone(), flow),
            pending_fin: false,
            pending_send_payloads: InfraVec::new(),
            retransmit_queue: TcpOutputRetransmitQueue::new(),
            retransmit_pending: false,
            iss: 0,
            irs: 0,
            snd_una: 0,
            snd_nxt: 0,
            snd_wnd: 0,
            rcv_nxt: 0,
            rcv_wnd: DEFAULT_TCP_WINDOW,
            send_state_initialized: false,
            receive_state_initialized: false,
        });
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        let mut shared_actions = InfraVec::new();
        if self.shared_tcp_control.is_some() {
            let congestion = TcpConnectionState::new(&self.tcp_congestion, None)?;
            shared_actions.push(congestion.install_connection_action(
                connection_id,
                socket_addrs_to_connection_key(local, peer)?,
                hammer_core::protocol::tcp::TcpState::SynSent,
            ));
            shared_actions.push(
                hammer_core::protocol::tcp::TcpControlPlaneAction::ArmTimer {
                    connection_id,
                    timer_id: hammer_core::protocol::tcp::TcpTimerId::new(lookup_id as u64),
                    kind: hammer_core::protocol::tcp::TcpTimerKind::Connect,
                    timeout: TCP_CONNECT_TIMEOUT,
                },
            );
        }
        Ok(ConnectTcpFlowResult {
            flow,
            shared_actions,
        })
    }

    fn close_tcp_flow(&mut self, flow: AppFlowId) -> HammerResult<CloseTcpFlowResult> {
        let Some(index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.flow == flow)
        else {
            return Err(HammerError::internal(format!(
                "tcp flow {} is not registered in runtime service",
                flow.value()
            )));
        };
        let released =
            self.tcp_connections.get(index).cloned().ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;

        if let Some(connection_id) = released.connection_id {
            self.remember_closed_connection(connection_id);
            if released.state == hammer_core::protocol::tcp::TcpState::SynSent {
                self.tcp_connections.drain(index..index + 1);
                self.remove_tcp_transport_send_queue(connection_id);
                if released.local_port < self.next_tcp_ephemeral_port {
                    self.next_tcp_ephemeral_port = released.local_port;
                }
                self.publish_tcp_lookup()?;
                self.publish_tcp_app_ingress()?;
            }
            return Ok(CloseTcpFlowResult {
                shared_action: self.shared_tcp_control.as_ref().map(|_| {
                    hammer_core::protocol::tcp::TcpControlPlaneAction::CloseConnection {
                        connection_id,
                        reason: hammer_core::protocol::tcp::TcpCloseReason::LocalRequest,
                    }
                }),
            });
        }

        self.tcp_connections.drain(index..index + 1);
        if released.local_port < self.next_tcp_ephemeral_port {
            self.next_tcp_ephemeral_port = released.local_port;
        }
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(CloseTcpFlowResult {
            shared_action: None,
        })
    }

    fn promote_pending_syn_sent_connection(
        &mut self,
        lookup_id: TcpLookupId,
        local: SocketAddr,
        remote: SocketAddr,
        observation: Option<TcpHandshakeObservation>,
    ) -> HammerResult<PromotePendingSynSentResult> {
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
        let already_bound = current.local == Some(local)
            && current.state != hammer_core::protocol::tcp::TcpState::SynSent;
        if !already_bound && current.state != hammer_core::protocol::tcp::TcpState::SynSent {
            return Err(HammerError::internal(format!(
                "tcp syn-sent lookup {lookup_id} cannot promote from state {:?}",
                current.state
            )));
        }
        if !already_bound {
            self.ensure_connection_absent(local, remote)?;
        }
        let connection_id = current.connection_id.ok_or_else(|| {
            HammerError::internal("tcp syn-sent registration is missing connection id")
        })?;
        let (published_state, wake_output_flow) = {
            let registration = self.tcp_connections.get_mut(index).ok_or_else(|| {
                HammerError::internal("tcp syn-sent registration slot is invalid")
            })?;
            let was_output_ready = tcp_transport_state_ready(registration);
            registration.local = Some(local);
            if current.state == hammer_core::protocol::tcp::TcpState::SynSent {
                registration.state = hammer_core::protocol::tcp::TcpState::Established;
                registration.pending_send_payloads.clear();
                tcp_apply_deferred_local_shutdown(registration);
            }
            if let Some(observation) = observation {
                tcp_apply_syn_sent_observation(registration, observation)?;
            }
            (
                registration.state,
                (!was_output_ready
                    && tcp_transport_state_ready(registration)
                    && tcp_state_allows_transport_output(registration.state))
                .then_some(registration.flow),
            )
        };
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        let mut shared_actions = InfraVec::new();
        if self.shared_tcp_control.is_some() {
            let congestion = TcpConnectionState::new(&self.tcp_congestion, None)?;
            shared_actions.push(congestion.upsert_connection_action(
                connection_id,
                socket_addrs_to_connection_key(local, remote)?,
                published_state,
            ));
            shared_actions.push(
                hammer_core::protocol::tcp::TcpControlPlaneAction::CancelTimer {
                    connection_id,
                    kind: hammer_core::protocol::tcp::TcpTimerKind::Connect,
                },
            );
        }
        Ok(PromotePendingSynSentResult {
            shared_actions,
            wake_output_flow,
        })
    }

    fn observe_tcp_connection_state_change(
        &mut self,
        connection_id: TcpConnectionId,
        key: TcpConnectionKey,
        state: hammer_core::protocol::tcp::TcpState,
    ) -> HammerResult<hammer_core::protocol::tcp::TcpState> {
        if self.discard_state_change_for_closed_connection(connection_id, key)? {
            return Ok(state);
        }
        let (local, remote) = socket_addrs_from_connection_key(key);
        let Some(index) = self
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
        else {
            return Ok(state);
        };
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
            && !(current.local == Some(local) && current.remote == remote)
        {
            self.ensure_connection_absent(local, remote)?;
        }
        let mut complete_accept = None;
        let mut flush_pending_payloads = false;
        {
            let registration = self.tcp_connections.get_mut(index).ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
            let previous_state = registration.state;
            let was_established =
                previous_state == hammer_core::protocol::tcp::TcpState::Established;
            let was_payload_sendable = tcp_state_allows_transport_payload_send(previous_state);
            registration.connection_id = Some(connection_id);
            registration.local = Some(local);
            registration.state = state;
            if !was_payload_sendable && tcp_state_allows_transport_payload_send(state) {
                flush_pending_payloads = true;
            }
            tcp_apply_deferred_local_shutdown(registration);
            if tcp_state_acknowledges_retransmit_queue(previous_state, registration.state) {
                let _ = registration
                    .retransmit_queue
                    .acknowledge_through(registration.snd_nxt);
                registration.retransmit_pending = false;
            }
            if !was_established && state == hammer_core::protocol::tcp::TcpState::Established {
                if let Some(listener) = registration.listener.take() {
                    complete_accept = Some((
                        registration.target.app().clone(),
                        listener,
                        registration.flow,
                    ));
                }
            }
        }
        if flush_pending_payloads {
            self.flush_pending_tcp_transport_send_queue(index)?;
        }
        if let Some((app, listener, flow)) = complete_accept {
            app.try_complete_accept(listener, flow)?;
        }
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(self
            .tcp_connections
            .get(index)
            .ok_or_else(|| HammerError::internal("tcp connection registration slot is invalid"))?
            .state)
    }

    fn enqueue_tcp_transport_send(
        &mut self,
        connection_id: TcpConnectionId,
        payload: std::vec::Vec<u8>,
    ) -> HammerResult<()> {
        let index = self
            .tcp_transport_send_queues
            .iter()
            .position(|queue| queue.connection_id == connection_id)
            .unwrap_or_else(|| {
                self.tcp_transport_send_queues.push(TcpTransportSendQueue {
                    connection_id,
                    payloads: InfraVec::new(),
                });
                self.tcp_transport_send_queues.len() - 1
            });
        let queue = self
            .tcp_transport_send_queues
            .get_mut(index)
            .ok_or_else(|| HammerError::internal("tcp transport send queue slot is invalid"))?;
        queue.payloads.push(payload);
        Ok(())
    }

    fn remove_tcp_transport_send_queue(&mut self, connection_id: TcpConnectionId) {
        let Some(index) = self
            .tcp_transport_send_queues
            .iter()
            .position(|queue| queue.connection_id == connection_id)
        else {
            return;
        };
        let _ = self.tcp_transport_send_queues.drain(index..index + 1);
    }

    fn flush_pending_tcp_transport_send_queue(
        &mut self,
        connection_index: usize,
    ) -> HammerResult<()> {
        let registration = self
            .tcp_connections
            .get_mut(connection_index)
            .ok_or_else(|| HammerError::internal("tcp connection registration slot is invalid"))?;
        registration.pending_send_payloads.clear();
        Ok(())
    }

    fn take_next_tcp_output_work_for_flow(
        &mut self,
        flow: AppFlowId,
    ) -> HammerResult<Option<TcpOutputWorkItem>> {
        let Some(connection_index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.flow == flow)
        else {
            return Ok(None);
        };
        let (connection_id, state, local, pending_fin, retransmit_pending, retransmit_segment) = {
            let registration = self.tcp_connections.get(connection_index).ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
            (
                registration.connection_id,
                registration.state,
                registration.local,
                registration.pending_fin,
                registration.retransmit_pending,
                registration
                    .retransmit_queue
                    .front()
                    .map(|segment| segment.segment.clone()),
            )
        };
        if !tcp_state_allows_transport_output(state) {
            return Ok(None);
        }
        let Some(connection_id) = connection_id else {
            return Ok(None);
        };
        let Some(local) = local else {
            return Ok(None);
        };
        if retransmit_pending {
            let Some(segment) = retransmit_segment else {
                return Ok(None);
            };
            return Ok(Some(TcpOutputWorkItem {
                connection_id,
                payload_len: segment.payload.len(),
                include_fin: segment.flags & TCP_FLAG_FIN != 0,
                retransmit: true,
                segment,
            }));
        }
        let queue_index = self
            .tcp_transport_send_queues
            .iter()
            .position(|queue| queue.connection_id == connection_id);
        if queue_index.is_none() && !pending_fin {
            return Ok(None);
        }
        let registration = self
            .tcp_connections
            .get(connection_index)
            .cloned()
            .ok_or_else(|| HammerError::internal("tcp connection registration slot is invalid"))?;
        if !tcp_transport_state_ready(&registration) {
            return Ok(None);
        }
        let snapshot = tcp_connection_snapshot_from_registration(&registration);
        let (payload, payload_len, include_fin) = if let Some(queue_index) = queue_index {
            let queue = self
                .tcp_transport_send_queues
                .get(queue_index)
                .ok_or_else(|| HammerError::internal("tcp transport send queue slot is invalid"))?;
            if let Some(staged) = queue.payloads.get(0) {
                let requested_payload_len = staged
                    .len()
                    .min(crate::transport::tcp::DEFAULT_TCP_OUTPUT_PAYLOAD_LEN);
                let can_drain_payload =
                    queue.payloads.len() == 1 && requested_payload_len == staged.len();
                let payload_len_with_fin = if pending_fin && can_drain_payload {
                    tcp_payload_len_in_send_window(snapshot, requested_payload_len, 1)
                } else {
                    0
                };
                let include_fin = pending_fin
                    && can_drain_payload
                    && payload_len_with_fin == requested_payload_len;
                let payload_len = if include_fin {
                    payload_len_with_fin
                } else {
                    tcp_payload_len_in_send_window(snapshot, requested_payload_len, 0)
                };
                let payload = staged[..payload_len].to_vec();
                (payload, payload_len, include_fin)
            } else {
                (Vec::new(), 0, false)
            }
        } else {
            (
                Vec::new(),
                0,
                pending_fin && tcp_available_send_window(snapshot) != 0,
            )
        };
        if payload.is_empty() && !include_fin {
            return Ok(None);
        }
        let flags = TCP_FLAG_ACK
            | if payload.is_empty() { 0 } else { TCP_FLAG_PSH }
            | if include_fin { TCP_FLAG_FIN } else { 0 };
        let segment = {
            build_tcp_output_segment_with_flags(snapshot, local, &payload, flags)
                .map_err(|err| HammerError::internal(format!("build tcp output segment: {err}")))?
        };
        Ok(Some(TcpOutputWorkItem {
            connection_id,
            payload_len,
            include_fin,
            retransmit: false,
            segment,
        }))
    }

    fn observe_tcp_output_emitted(
        &mut self,
        work: &TcpOutputWorkItem,
    ) -> HammerResult<Option<TcpConnectionId>> {
        let Some(connection_index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.connection_id == Some(work.connection_id))
        else {
            return Ok(None);
        };
        {
            let registration = self
                .tcp_connections
                .get_mut(connection_index)
                .ok_or_else(|| {
                    HammerError::internal("tcp connection registration slot is invalid")
                })?;
            if work.retransmit {
                registration.retransmit_pending = false;
            } else {
                if !tcp_transport_state_ready(registration) {
                    return Err(HammerError::internal(
                        "tcp output emitted before handshake transport state initialized",
                    ));
                }
                if work.include_fin {
                    registration.pending_fin = false;
                }
                if work.segment.consumes_sequence_space() {
                    registration.snd_nxt = work.segment.next_send_sequence();
                    let _ = registration.retransmit_queue.track_segment(&work.segment);
                }
            }
        }
        if !work.retransmit && work.payload_len != 0 {
            let Some(queue_index) = self
                .tcp_transport_send_queues
                .iter()
                .position(|queue| queue.connection_id == work.connection_id)
            else {
                return Err(HammerError::internal(
                    "tcp transport send queue missing after emit",
                ));
            };
            let queue_empty = {
                let queue = self
                    .tcp_transport_send_queues
                    .get_mut(queue_index)
                    .ok_or_else(|| {
                        HammerError::internal("tcp transport send queue slot is invalid")
                    })?;
                let Some(staged) = queue.payloads.get_mut(0) else {
                    return Err(HammerError::internal(
                        "tcp transport send queue payload missing after emit",
                    ));
                };
                if work.payload_len >= staged.len() {
                    let _ = queue.payloads.drain(0..1);
                } else {
                    staged.drain(0..work.payload_len);
                }
                queue.payloads.is_empty()
            };
            if queue_empty {
                self.remove_tcp_transport_send_queue(work.connection_id);
            }
        }
        self.publish_tcp_lookup()?;
        if work.retransmit || work.segment.consumes_sequence_space() {
            Ok(Some(work.connection_id))
        } else {
            Ok(None)
        }
    }

    fn observe_tcp_retransmit_timeout(
        &mut self,
        connection_id: TcpConnectionId,
    ) -> HammerResult<Option<AppFlowId>> {
        let Some(connection_index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.connection_id == Some(connection_id))
        else {
            return Ok(None);
        };
        let registration = self
            .tcp_connections
            .get_mut(connection_index)
            .ok_or_else(|| HammerError::internal("tcp connection registration slot is invalid"))?;
        if registration.retransmit_queue.is_empty() {
            registration.retransmit_pending = false;
            return Ok(None);
        }
        registration.retransmit_pending = true;
        Ok(Some(registration.flow))
    }

    fn observe_tcp_connection_shutdown(
        &mut self,
        connection_id: TcpConnectionId,
        direction: TcpShutdownDirection,
        reason: TcpCloseReason,
    ) -> HammerResult<Option<(AppFlowId, Option<hammer_core::protocol::tcp::TcpState>)>> {
        let Some(index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.connection_id == Some(connection_id))
        else {
            return Ok(None);
        };
        let (flow, next_state) = {
            let registration = self.tcp_connections.get_mut(index).ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
            let was_write_shutdown = tcp_write_side_is_shutdown(registration.shutdown);
            let merged_shutdown = merge_tcp_shutdown(registration.shutdown, direction, reason);
            registration.shutdown = Some(merged_shutdown);
            let next_state = tcp_state_after_local_shutdown(registration.state, merged_shutdown.0);
            if let Some(next_state) = next_state {
                registration.state = next_state;
            }
            if !was_write_shutdown && tcp_state_requires_local_fin(registration.state) {
                registration.pending_fin = true;
            }
            (registration.flow, next_state)
        };
        if next_state.is_some() {
            self.publish_tcp_lookup()?;
            self.publish_tcp_app_ingress()?;
        }
        Ok(Some((flow, next_state)))
    }

    fn observe_tcp_flow_shutdown(
        &mut self,
        flow: AppFlowId,
        direction: TcpShutdownDirection,
        reason: TcpCloseReason,
    ) -> HammerResult<
        Option<(
            TcpConnectionId,
            Option<hammer_core::protocol::tcp::TcpState>,
        )>,
    > {
        let Some(index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.flow == flow)
        else {
            return Ok(None);
        };
        let (connection_id, next_state) = {
            let registration = self.tcp_connections.get_mut(index).ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
            let was_write_shutdown = tcp_write_side_is_shutdown(registration.shutdown);
            let merged_shutdown = merge_tcp_shutdown(registration.shutdown, direction, reason);
            registration.shutdown = Some(merged_shutdown);
            let next_state = tcp_state_after_local_shutdown(registration.state, merged_shutdown.0);
            if let Some(next_state) = next_state {
                registration.state = next_state;
            }
            if !was_write_shutdown && tcp_state_requires_local_fin(registration.state) {
                registration.pending_fin = true;
            }
            (registration.connection_id, next_state)
        };
        if next_state.is_some() {
            self.publish_tcp_lookup()?;
            self.publish_tcp_app_ingress()?;
        }
        Ok(connection_id.map(|connection_id| (connection_id, next_state)))
    }

    fn observe_tcp_flow_send(
        &mut self,
        flow: AppFlowId,
        payload: std::vec::Vec<u8>,
    ) -> HammerResult<()> {
        let Some(index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.flow == flow)
        else {
            return Ok(());
        };
        if payload.is_empty() {
            return Ok(());
        }
        let connection_id = {
            let registration = self.tcp_connections.get_mut(index).ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
            if tcp_write_side_is_shutdown(registration.shutdown) {
                return Ok(());
            }
            if !tcp_state_allows_transport_payload_send(registration.state) {
                registration.pending_send_payloads.push(payload.clone());
                registration.connection_id
            } else {
                Some(registration.connection_id.ok_or_else(|| {
                    HammerError::internal("tcp connection registration is missing connection id")
                })?)
            }
        };
        if let Some(connection_id) = connection_id {
            self.enqueue_tcp_transport_send(connection_id, payload)?;
        }
        Ok(())
    }

    fn observe_tcp_ack_progress(
        &mut self,
        observation: TcpEstablishedAckObservation,
    ) -> HammerResult<TcpEstablishedAckProgressResult> {
        let Some(index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.connection_id == Some(observation.connection_id))
        else {
            return Ok(TcpEstablishedAckProgressResult {
                connection_id: observation.connection_id,
                timer_action: TcpRetransmitTimerAction::None,
                wake_output_flow: None,
                shared_update: None,
            });
        };
        let publish_lookup;
        let mut shared_update = None;
        let wake_output_flow;
        let timer_action = {
            let registration = self.tcp_connections.get_mut(index).ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
            let previous_snd_una = registration.snd_una;
            let previous_snd_wnd = registration.snd_wnd;
            let previous_state = registration.state;
            if !registration.send_state_initialized {
                registration.iss = observation.accepted_acknowledgment.wrapping_sub(1);
                registration.send_state_initialized = true;
            }
            registration.snd_una =
                tcp_seq_max(registration.snd_una, observation.accepted_acknowledgment);
            registration.snd_nxt = tcp_seq_max(registration.snd_nxt, registration.snd_una);
            registration.snd_wnd = observation.advertised_window;
            let released = registration
                .retransmit_queue
                .acknowledge_through(observation.accepted_acknowledgment);
            if let Some(next_state) = observation.ack_state_transition {
                registration.state = next_state;
            }
            let send_window_progressed = registration.snd_una != previous_snd_una
                || registration.snd_wnd != previous_snd_wnd;
            publish_lookup = registration.snd_una != previous_snd_una
                || registration.snd_wnd != previous_snd_wnd
                || registration.state != previous_state;
            wake_output_flow = (send_window_progressed
                && tcp_state_allows_transport_output(registration.state))
            .then_some(registration.flow);
            if registration.state != previous_state
                && let Some(local) = registration.local
            {
                shared_update = Some((
                    self.shared_tcp_control.clone().ok_or_else(|| {
                        HammerError::internal("shared tcp control missing for ACK progress")
                    })?,
                    self.tcp_congestion,
                    socket_addrs_to_connection_key(local, registration.remote)?,
                    registration.state,
                ));
            }
            if registration.retransmit_queue.is_empty() {
                registration.retransmit_pending = false;
                TcpRetransmitTimerAction::Cancel
            } else if released != 0 {
                registration.retransmit_pending = false;
                TcpRetransmitTimerAction::Rearm
            } else {
                TcpRetransmitTimerAction::None
            }
        };
        if publish_lookup {
            self.publish_tcp_lookup()?;
        }
        let wake_output_flow = wake_output_flow.and_then(|flow| {
            self.tcp_connections
                .iter()
                .find(|registration| registration.flow == flow)
                .and_then(|registration| {
                    let has_queued_payload =
                        registration.connection_id.is_some_and(|connection_id| {
                            self.tcp_transport_send_queues.iter().any(|queue| {
                                queue.connection_id == connection_id && !queue.payloads.is_empty()
                            })
                        });
                    (has_queued_payload || registration.pending_fin).then_some(flow)
                })
        });
        Ok(TcpEstablishedAckProgressResult {
            connection_id: observation.connection_id,
            timer_action,
            wake_output_flow,
            shared_update,
        })
    }

    fn take_next_tcp_transport_send_for_flow(
        &mut self,
        flow: AppFlowId,
    ) -> HammerResult<Option<std::vec::Vec<u8>>> {
        let Some(registration) = self
            .tcp_connections
            .iter()
            .find(|registration| registration.flow == flow)
        else {
            return Ok(None);
        };
        if !tcp_state_allows_transport_output(registration.state) {
            return Ok(None);
        }
        let Some(connection_id) = registration.connection_id else {
            return Ok(None);
        };
        let Some(index) = self
            .tcp_transport_send_queues
            .iter()
            .position(|queue| queue.connection_id == connection_id)
        else {
            return Ok(None);
        };
        let queue = self
            .tcp_transport_send_queues
            .get_mut(index)
            .ok_or_else(|| HammerError::internal("tcp transport send queue slot is invalid"))?;
        Ok(queue.payloads.drain(0..1).next())
    }

    fn tcp_transport_send_payloads_for_flow_for_test(
        &self,
        flow: AppFlowId,
    ) -> HammerResult<Vec<Vec<u8>>> {
        let Some(connection_id) = self
            .tcp_connections
            .iter()
            .find(|registration| registration.flow == flow)
            .and_then(|registration| registration.connection_id)
        else {
            return Ok(Vec::new());
        };
        let Some(queue) = self
            .tcp_transport_send_queues
            .iter()
            .find(|queue| queue.connection_id == connection_id)
        else {
            return Ok(Vec::new());
        };
        Ok(queue.payloads.iter().cloned().collect())
    }

    fn take_tcp_transport_send_payload_for_flow_for_test(
        &mut self,
        flow: AppFlowId,
    ) -> HammerResult<Option<Vec<u8>>> {
        self.take_next_tcp_transport_send_for_flow(flow)
    }

    fn remove_tcp_connection_by_connection_id(
        &mut self,
        connection_id: TcpConnectionId,
    ) -> HammerResult<Option<TcpConnectionRegistration>> {
        self.remember_closed_connection(connection_id);
        let Some(index) = self
            .tcp_connections
            .iter()
            .position(|registration| registration.connection_id == Some(connection_id))
        else {
            return Ok(None);
        };
        let released =
            self.tcp_connections.get(index).cloned().ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
        self.tcp_connections.drain(index..index + 1);
        self.remove_tcp_transport_send_queue(connection_id);
        if released.local_port < self.next_tcp_ephemeral_port {
            self.next_tcp_ephemeral_port = released.local_port;
        }
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(Some(released))
    }

    fn discard_state_change_for_closed_connection(
        &mut self,
        connection_id: TcpConnectionId,
        key: TcpConnectionKey,
    ) -> HammerResult<bool> {
        if !self
            .closed_tcp_connections
            .iter()
            .any(|closed| *closed == connection_id)
        {
            return Ok(false);
        }
        let (local, remote) = socket_addrs_from_connection_key(key);
        let Some(index) = self.tcp_connections.iter().position(|registration| {
            registration.connection_id == Some(connection_id)
                || (registration.connection_id.is_none()
                    && registration.local_port == local.port()
                    && registration.remote == remote)
        }) else {
            return Ok(true);
        };
        let released =
            self.tcp_connections.get(index).cloned().ok_or_else(|| {
                HammerError::internal("tcp connection registration slot is invalid")
            })?;
        self.tcp_connections.drain(index..index + 1);
        self.remove_tcp_transport_send_queue(connection_id);
        if released.local_port < self.next_tcp_ephemeral_port {
            self.next_tcp_ephemeral_port = released.local_port;
        }
        self.publish_tcp_lookup()?;
        self.publish_tcp_app_ingress()?;
        Ok(true)
    }

    #[inline]
    fn is_closed_tcp_connection(&self, connection_id: TcpConnectionId) -> bool {
        self.closed_tcp_connections
            .iter()
            .any(|closed| *closed == connection_id)
    }

    fn remember_closed_connection(&mut self, connection_id: TcpConnectionId) {
        if self
            .closed_tcp_connections
            .iter()
            .any(|closed| *closed == connection_id)
        {
            return;
        }
        self.closed_tcp_connections.push(connection_id);
        if self.closed_tcp_connections.len() > CLOSED_TCP_CONNECTION_TOMBSTONE_LIMIT {
            self.closed_tcp_connections.drain(0..1);
        }
    }

    fn publish_tcp_lookup(&mut self) -> HammerResult<()> {
        let mut snapshot = TcpLookupSnapshot::empty();
        let mut connection_snapshot = TcpConnectionSnapshotPool::empty();
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
            connection_snapshot.insert(tcp_connection_snapshot_from_registration(&registration));
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
        self.tcp_control
            .publish_connections(connection_snapshot.clone())
            .map_err(|err| {
                HammerError::internal(format!("publish tcp input connection snapshot: {err}"))
            })?;
        self.tcp_established_control
            .publish_connections(connection_snapshot.clone())
            .map_err(|err| {
                HammerError::internal(format!("publish tcp connection snapshot: {err}"))
            })?;
        if let Some(control) = &self.tcp_syn_sent_control {
            control
                .publish_connections(syn_sent_connections)
                .map_err(|err| {
                    HammerError::internal(format!("publish tcp syn-sent snapshot: {err}"))
                })?;
        }
        self.tcp_lookup = snapshot;
        self.tcp_connection_snapshot = connection_snapshot;
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

    #[inline]
    fn alloc_tcp_connection_id(&mut self) -> TcpConnectionId {
        let connection_id = TcpConnectionId::new(self.next_tcp_connection_id);
        self.next_tcp_connection_id = self.next_tcp_connection_id.wrapping_add(1).max(1);
        connection_id
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
            tcp_output: TcpOutputBackendSlot::new(),
            tcp_output_signals: TcpOutputSignalRegistry::default(),
            tcp_retransmit_timers: TcpOutputRetransmitTimerRegistry::default(),
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
    fn install_tcp_established_backend(&self) -> HammerResult<()> {
        let backend = Arc::new(self.clone());
        self.with_state_mut(move |state| {
            state.install_tcp_established_backend(backend);
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
        let connect_app = app.clone();
        let result = self.with_state_mut(move |state| {
            state.connect_tcp_stream(&connect_app, peer, owner_worker)
        })?;
        self.start_tcp_output_pump(&app, result.flow, owner_worker)?;
        self.start_tcp_send_pump(&app, result.flow, owner_worker)?;
        self.start_tcp_shutdown_pump(&app, result.flow, owner_worker)?;
        let Some(control) = self.with_state(|state| Ok(state.shared_tcp_control.clone()))? else {
            return Ok(result.flow);
        };
        for action in result.shared_actions.iter().copied() {
            control.apply(action).map_err(|err| {
                HammerError::internal(format!("apply shared tcp connect action: {err}"))
            })?;
        }
        Ok(result.flow)
    }

    fn start_tcp_shutdown_pump(
        &self,
        app: &AppContext,
        flow: AppFlowId,
        owner_worker: usize,
    ) -> HammerResult<()> {
        let app = app.clone();
        let handle = self.clone();
        app.spawn_detached_on_flow_owner(flow, owner_worker, move |worker| async move {
            let backend = worker.backend();
            while let Some(shutdown) = backend.next_tcp_shutdown().await {
                if handle
                    .shutdown_tcp_flow(shutdown.flow(), shutdown.how())
                    .is_err()
                {
                    break;
                }
            }
        })
    }

    fn start_tcp_output_pump(
        &self,
        app: &AppContext,
        flow: AppFlowId,
        owner_worker: usize,
    ) -> HammerResult<()> {
        let app = app.clone();
        let handle = self.clone();
        let notify = self.tcp_output_signals.subscribe(flow);
        app.spawn_detached_on_flow_owner(flow, owner_worker, move |_worker| async move {
            loop {
                notify.notified().await;
                let action = match handle.drive_tcp_output(flow) {
                    Ok(action) => action,
                    Err(_) => break,
                };
                if action == TcpOutputDrainAction::Closed {
                    break;
                }
            }
        })
    }

    fn start_tcp_send_pump(
        &self,
        app: &AppContext,
        flow: AppFlowId,
        owner_worker: usize,
    ) -> HammerResult<()> {
        let app = app.clone();
        let handle = self.clone();
        app.spawn_detached_on_flow_owner(flow, owner_worker, move |worker| async move {
            let backend = worker.backend();
            while let Some(send) = backend.next_send().await {
                let payload = match send.lease().copy_current() {
                    Ok(payload) => payload.to_vec(),
                    Err(_) => break,
                };
                if handle.record_tcp_flow_send(flow, payload).is_err() {
                    break;
                }
            }
        })
    }

    fn drive_tcp_output(&self, flow: AppFlowId) -> HammerResult<TcpOutputDrainAction> {
        let mut emitted = false;
        loop {
            let next =
                self.with_state_mut(move |state| state.take_next_tcp_output_work_for_flow(flow))?;
            let Some(work) = next else {
                return Ok(if emitted {
                    TcpOutputDrainAction::Emit
                } else if self.tcp_flow_exists(flow)? {
                    TcpOutputDrainAction::Idle
                } else {
                    TcpOutputDrainAction::Closed
                });
            };
            let segment = work.segment.clone();
            self.tcp_output
                .emit_segment(segment)
                .map_err(|err| HammerError::internal(format!("emit tcp output segment: {err}")))?;
            if let Some(connection_id) =
                self.with_state_mut(move |state| state.observe_tcp_output_emitted(&work))?
            {
                self.arm_tcp_retransmit_timer(connection_id)?;
            }
            emitted = true;
        }
    }

    fn arm_tcp_retransmit_timer(&self, connection_id: TcpConnectionId) -> HammerResult<()> {
        let state = Arc::downgrade(&self.state);
        let signals = self.tcp_output_signals.clone();
        let timers = self.tcp_retransmit_timers.clone();
        let handle =
            self.control_handle
                .schedule_once(TCP_OUTPUT_RETRANSMIT_TIMEOUT, move || {
                    let state = state.clone();
                    let signals = signals.clone();
                    let timers = timers.clone();
                    async move {
                        timers.clear_fired(connection_id);
                        let Some(state) = state.upgrade() else {
                            return;
                        };
                        let flow = {
                            // SAFETY: retransmit timers run on the control thread runtime.
                            let state = unsafe { state.get_mut() };
                            state
                                .observe_tcp_retransmit_timeout(connection_id)
                                .ok()
                                .flatten()
                        };
                        if let Some(flow) = flow {
                            signals.signal(flow);
                        }
                    }
                })?;
        self.tcp_retransmit_timers.replace(connection_id, handle);
        Ok(())
    }

    fn cancel_tcp_retransmit_timer(&self, connection_id: TcpConnectionId) {
        self.tcp_retransmit_timers.cancel(connection_id);
    }

    fn cancel_all_tcp_retransmit_timers(&self) {
        self.tcp_retransmit_timers.cancel_all();
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

    fn close_tcp_flow(&self, flow: AppFlowId) -> HammerResult<()> {
        let result = self.with_state_mut(move |state| state.close_tcp_flow(flow))?;
        self.tcp_output_signals.signal(flow);
        if let Some(action) = result.shared_action {
            let Some(control) = self.with_state(|state| Ok(state.shared_tcp_control.clone()))?
            else {
                return Ok(());
            };
            control.apply(action).map_err(|err| {
                HammerError::internal(format!("apply shared tcp flow close action: {err}"))
            })?;
        }
        Ok(())
    }

    fn handle_tcp_syn_sent_observation(
        &self,
        observation: TcpSynSentObservation,
    ) -> HammerResult<()> {
        let result = self.with_state_mut(move |state| {
            state.promote_pending_syn_sent_connection(
                observation.connection_id,
                observation.local,
                observation.remote,
                Some(observation.transport),
            )
        })?;
        if let Some(flow) = result.wake_output_flow {
            self.tcp_output_signals.signal(flow);
        }
        let Some(control) = self.with_state(|state| Ok(state.shared_tcp_control.clone()))? else {
            return Ok(());
        };
        for action in result.shared_actions.iter().copied() {
            control.apply(action).map_err(|err| {
                HammerError::internal(format!("apply shared tcp syn-sent action: {err}"))
            })?;
        }
        Ok(())
    }

    fn handle_tcp_accept_observation(
        &self,
        listener_id: TcpLookupId,
        remote: SocketAddr,
        local: SocketAddr,
        observation: TcpHandshakeObservation,
    ) -> HammerResult<()> {
        let result = self.with_state_mut(move |state| {
            state.handle_incoming_tcp_connection_by_listener_id(
                TcpListenerId::new(listener_id as u64),
                remote,
                local,
                Some(observation),
            )
        })?;
        if let Some(control) = self.with_state(|state| Ok(state.shared_tcp_control.clone()))? {
            for action in result.shared_actions.iter().copied() {
                control.apply(action).map_err(|err| {
                    HammerError::internal(format!(
                        "apply shared tcp accept observation action: {err}"
                    ))
                })?;
            }
        }
        if let Some((app, flow, owner_worker)) = result.accepted_flow {
            self.start_tcp_output_pump(&app, flow, owner_worker)?;
            self.start_tcp_send_pump(&app, flow, owner_worker)?;
            self.start_tcp_shutdown_pump(&app, flow, owner_worker)?;
        }
        if let Some(flow) = result.wake_output_flow {
            self.tcp_output_signals.signal(flow);
        }
        Ok(())
    }

    fn shutdown_tcp_flow(&self, flow: AppFlowId, how: std::net::Shutdown) -> HammerResult<()> {
        let direction = tcp_shutdown_direction(how);
        let reason = TcpCloseReason::LocalShutdown;
        let (connection, control) = self.with_state_mut(move |state| {
            let connection = state.observe_tcp_flow_shutdown(flow, direction, reason)?;
            Ok((connection, state.shared_tcp_control.clone()))
        })?;
        let Some((connection_id, transition_state)) = connection else {
            return Ok(());
        };
        self.tcp_output_signals.signal(flow);
        let Some(control) = control else {
            return Ok(());
        };
        if let Some(state) = transition_state {
            match control.apply(
                hammer_core::protocol::tcp::TcpControlPlaneAction::TransitionConnection {
                    connection_id,
                    state,
                },
            ) {
                Ok(()) => {}
                Err(_err) if !control.has_connection(connection_id) => return Ok(()),
                Err(err) => {
                    return Err(HammerError::internal(format!(
                        "transition shared tcp connection control on shutdown: {err}"
                    )));
                }
            }
        }
        match control.apply(
            hammer_core::protocol::tcp::TcpControlPlaneAction::ShutdownConnection {
                connection_id,
                direction,
                reason,
            },
        ) {
            Ok(()) => Ok(()),
            Err(_err) if !control.has_connection(connection_id) => Ok(()),
            Err(err) => Err(HammerError::internal(format!(
                "shutdown shared tcp connection control: {err}"
            ))),
        }
    }

    fn record_tcp_flow_send(
        &self,
        flow: AppFlowId,
        payload: std::vec::Vec<u8>,
    ) -> HammerResult<()> {
        let result = self.with_state_mut(move |state| state.observe_tcp_flow_send(flow, payload));
        if result.is_ok() {
            self.tcp_output_signals.signal(flow);
        }
        result
    }

    fn handle_tcp_established_ack_progress(
        &self,
        observation: TcpEstablishedAckObservation,
    ) -> HammerResult<()> {
        let result =
            self.with_state_mut(move |state| state.observe_tcp_ack_progress(observation))?;
        match result.timer_action {
            TcpRetransmitTimerAction::None => {}
            TcpRetransmitTimerAction::Cancel => {
                self.cancel_tcp_retransmit_timer(result.connection_id);
            }
            TcpRetransmitTimerAction::Rearm => {
                self.arm_tcp_retransmit_timer(result.connection_id)?;
            }
        }
        if let Some(flow) = result.wake_output_flow {
            self.tcp_output_signals.signal(flow);
        }
        let Some((control, congestion, key, state)) = result.shared_update else {
            return Ok(());
        };
        let action = TcpConnectionState::new(&congestion, None)?.upsert_connection_action(
            result.connection_id,
            key,
            state,
        );
        control.apply(action).map_err(|err| {
            HammerError::internal(format!("upsert shared tcp ACK state progress: {err}"))
        })
    }

    fn handle_tcp_established_close(
        &self,
        observation: TcpEstablishedObservation,
    ) -> HammerResult<()> {
        match observation.reason {
            TcpCloseReason::RemoteReset => self.handle_tcp_worker_event(TcpWorkerEvent::Closed {
                connection_id: observation.connection_id,
                reason: observation.reason,
            }),
            TcpCloseReason::RemoteFin => {
                let connection_id = observation.connection_id;
                let reason = observation.reason;
                let key = socket_addrs_to_connection_key(observation.local, observation.remote)?;
                let next_state = self.with_state(move |state| {
                    Ok(state
                        .tcp_connections
                        .iter()
                        .find(|registration| registration.connection_id == Some(connection_id))
                        .and_then(|registration| tcp_state_after_remote_fin(registration.state)))
                })?;
                if let Some(next_state) = next_state {
                    self.handle_tcp_worker_event(TcpWorkerEvent::StateChanged {
                        connection_id,
                        key,
                        state: next_state,
                    })?;
                }
                self.handle_tcp_worker_event(TcpWorkerEvent::ShutdownObserved {
                    connection_id,
                    direction: TcpShutdownDirection::Read,
                    reason,
                })
            }
            _ => Ok(()),
        }
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
                            let result = state.handle_incoming_tcp_connection_by_listener_id(
                                listener_id,
                                remote,
                                local,
                                None,
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
            TcpWorkerEvent::TimerExpired { .. } => Ok(()),
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
                            if let Ok(Some(released)) = &result
                                && let Some(flow) = released.target.flow_id()
                            {
                                let close_result =
                                    released.target.app().try_complete_closed_flow(flow);
                                debug_assert!(
                                    close_result.is_ok(),
                                    "runtime tcp closed completion failed: {close_result:?}"
                                );
                                let _ = close_result;
                            }
                            let _ = result;
                        }
                    })
                    .map(|_| ())
            }
        }
    }

    #[inline]
    fn handle_tcp_worker_event(&self, event: TcpWorkerEvent) -> HammerResult<()> {
        match event {
            TcpWorkerEvent::IncomingConnection {
                listener_id, key, ..
            } => {
                let (local, remote) = socket_addrs_from_connection_key(key);
                let result = self.with_state_mut(move |state| {
                    state.handle_incoming_tcp_connection_by_listener_id(
                        listener_id,
                        remote,
                        local,
                        None,
                    )
                })?;
                if let Some(control) =
                    self.with_state(|state| Ok(state.shared_tcp_control.clone()))?
                {
                    for action in result.shared_actions.iter().copied() {
                        control.apply(action).map_err(|err| {
                            HammerError::internal(format!(
                                "apply shared tcp incoming-connection action: {err}"
                            ))
                        })?;
                    }
                }
                if let Some((app, flow, owner_worker)) = result.accepted_flow {
                    self.start_tcp_output_pump(&app, flow, owner_worker)?;
                    self.start_tcp_send_pump(&app, flow, owner_worker)?;
                    self.start_tcp_shutdown_pump(&app, flow, owner_worker)?;
                }
                if let Some(flow) = result.wake_output_flow {
                    self.tcp_output_signals.signal(flow);
                }
                Ok(())
            }
            TcpWorkerEvent::StateChanged {
                connection_id,
                key,
                state: tcp_state,
            } => {
                let shared = self.with_state_mut(move |state| {
                    let was_closed = state.is_closed_tcp_connection(connection_id);
                    let observed_state =
                        state.observe_tcp_connection_state_change(connection_id, key, tcp_state)?;
                    Ok((
                        was_closed,
                        observed_state,
                        state
                            .tcp_connections
                            .iter()
                            .find(|registration| registration.connection_id == Some(connection_id))
                            .map(|registration| registration.flow),
                        state
                            .shared_tcp_control
                            .clone()
                            .map(|control| (control, state.tcp_congestion)),
                    ))
                })?;
                let (closed, observed_state, flow, shared) = shared;
                if closed {
                    return Ok(());
                }
                let Some((control, congestion)) = shared else {
                    return Ok(());
                };
                if let Some(flow) = flow {
                    self.tcp_output_signals.signal(flow);
                }
                let action = TcpConnectionState::new(&congestion, None)?.upsert_connection_action(
                    connection_id,
                    key,
                    observed_state,
                );
                control.apply(action).map_err(|err| {
                    HammerError::internal(format!("upsert shared tcp connection control: {err}"))
                })
            }
            TcpWorkerEvent::ShutdownObserved {
                connection_id,
                direction,
                reason,
            } => {
                let shutdown = self.with_state_mut(move |state| {
                    state.observe_tcp_connection_shutdown(connection_id, direction, reason)
                })?;
                let Some((flow, transition_state)) = shutdown else {
                    return Ok(());
                };
                self.tcp_output_signals.signal(flow);
                let Some(control) =
                    self.with_state(|state| Ok(state.shared_tcp_control.clone()))?
                else {
                    return Ok(());
                };
                if let Some(state) = transition_state {
                    match control.apply(
                        hammer_core::protocol::tcp::TcpControlPlaneAction::TransitionConnection {
                            connection_id,
                            state,
                        },
                    ) {
                        Ok(()) => {}
                        Err(_err) if !control.has_connection(connection_id) => return Ok(()),
                        Err(err) => {
                            return Err(HammerError::internal(format!(
                                "transition shared tcp connection control on worker shutdown: {err}"
                            )));
                        }
                    }
                }
                let action =
                    hammer_core::protocol::tcp::TcpControlPlaneAction::ShutdownConnection {
                        connection_id,
                        direction,
                        reason,
                    };
                match control.apply(action) {
                    Ok(()) => Ok(()),
                    Err(_err) if !control.has_connection(connection_id) => Ok(()),
                    Err(err) => Err(HammerError::internal(format!(
                        "shutdown shared tcp connection control: {err}"
                    ))),
                }
            }
            TcpWorkerEvent::TimerExpired { .. } => Ok(()),
            TcpWorkerEvent::Closed {
                connection_id,
                reason,
            } => {
                self.cancel_tcp_retransmit_timer(connection_id);
                self.with_state_mut(move |state| {
                    state.remember_closed_connection(connection_id);
                    Ok(())
                })?;
                let Some(control) =
                    self.with_state(|state| Ok(state.shared_tcp_control.clone()))?
                else {
                    return self.handle_projected_tcp_worker_event(TcpWorkerEvent::Closed {
                        connection_id,
                        reason,
                    });
                };
                let result = control
                    .apply(
                        hammer_core::protocol::tcp::TcpControlPlaneAction::CloseConnection {
                            connection_id,
                            reason,
                        },
                    )
                    .map_err(|err| {
                        HammerError::internal(format!("close shared tcp connection control: {err}"))
                    });
                self.handle_projected_tcp_worker_event(TcpWorkerEvent::Closed {
                    connection_id,
                    reason,
                })?;
                result
            }
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

    fn tcp_flow_exists(&self, flow: AppFlowId) -> HammerResult<bool> {
        self.with_state(move |state| {
            Ok(state
                .tcp_connections
                .iter()
                .any(|registration| registration.flow == flow))
        })
    }

    fn install_tcp_output_backend<O>(&self, backend: Arc<O>)
    where
        O: TcpOutputBackend + 'static,
    {
        self.tcp_output.install(backend);
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
                tcp_connections: state.tcp_connection_snapshot.clone(),
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

    fn close_tcp_flow(&self, _app: &AppContext, flow: AppFlowId) -> HammerResult<()> {
        RuntimeAppControlHandle::close_tcp_flow(self, flow)
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

    fn observe_accept(
        &self,
        listener_id: TcpLookupId,
        _registration: &TcpAcceptRegistration,
        remote: SocketAddr,
        local: SocketAddr,
        _event: TcpWorkerEvent,
        observation: TcpHandshakeObservation,
    ) -> Result<(), CoreError> {
        self.handle_tcp_accept_observation(listener_id, remote, local, observation)
            .map_err(|err| CoreError::internal(format!("runtime tcp accept: {err}")))
    }
}

impl TcpSynSentBackend for RuntimeAppControlHandle {
    fn observe_syn_ack(&self, observation: TcpSynSentObservation) -> Result<(), CoreError> {
        self.handle_tcp_syn_sent_observation(observation)
            .map_err(|err| CoreError::internal(format!("runtime tcp syn-sent: {err}")))
    }
}

impl TcpEstablishedBackend for RuntimeAppControlHandle {
    fn observe_ack_progress(
        &self,
        observation: TcpEstablishedAckObservation,
    ) -> Result<(), CoreError> {
        self.handle_tcp_established_ack_progress(observation)
            .map_err(|err| CoreError::internal(format!("runtime tcp ACK progress: {err}")))
    }

    fn observe_close(&self, observation: TcpEstablishedObservation) -> Result<(), CoreError> {
        self.handle_tcp_established_close(observation)
            .map_err(|err| CoreError::internal(format!("runtime tcp established close: {err}")))
    }
}

fn socket_addrs_from_connection_key(key: TcpConnectionKey) -> (SocketAddr, SocketAddr) {
    let local = SocketAddr::new(key.local_addr(), key.local_port());
    let remote = SocketAddr::new(key.remote_addr(), key.remote_port());
    (local, remote)
}

fn socket_addrs_to_connection_key(
    local: SocketAddr,
    remote: SocketAddr,
) -> HammerResult<TcpConnectionKey> {
    match (local.ip(), remote.ip()) {
        (IpAddr::V4(local_ip), IpAddr::V4(remote_ip)) => Ok(TcpConnectionKey::v4(
            0,
            local_ip,
            local.port(),
            remote_ip,
            remote.port(),
        )),
        (IpAddr::V6(local_ip), IpAddr::V6(remote_ip)) => Ok(TcpConnectionKey::v6(
            0,
            local_ip,
            local.port(),
            remote_ip,
            remote.port(),
        )),
        _ => Err(HammerError::internal(format!(
            "tcp connection {} -> {} mixes IP versions",
            remote, local
        ))),
    }
}

#[inline]
fn unspecified_ip_for(remote: IpAddr) -> IpAddr {
    match remote {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED),
    }
}

#[inline]
fn tcp_shutdown_direction(how: std::net::Shutdown) -> TcpShutdownDirection {
    match how {
        std::net::Shutdown::Read => TcpShutdownDirection::Read,
        std::net::Shutdown::Write => TcpShutdownDirection::Write,
        std::net::Shutdown::Both => TcpShutdownDirection::Both,
    }
}

#[inline]
fn tcp_write_side_is_shutdown(shutdown: Option<(TcpShutdownDirection, TcpCloseReason)>) -> bool {
    matches!(
        shutdown,
        Some((TcpShutdownDirection::Write | TcpShutdownDirection::Both, _))
    )
}

#[inline]
fn tcp_transport_state_ready(registration: &TcpConnectionRegistration) -> bool {
    registration.send_state_initialized && registration.receive_state_initialized
}

#[inline]
fn tcp_apply_syn_sent_observation(
    registration: &mut TcpConnectionRegistration,
    observation: TcpHandshakeObservation,
) -> HammerResult<()> {
    let acknowledgment = observation.acknowledgment.ok_or_else(|| {
        HammerError::internal("active-open SYN-ACK observation is missing acknowledgment")
    })?;
    registration.iss = acknowledgment.wrapping_sub(1);
    registration.snd_una = acknowledgment;
    registration.snd_nxt = acknowledgment;
    registration.snd_wnd = observation.advertised_window;
    registration.send_state_initialized = true;
    registration.irs = observation.sequence;
    registration.rcv_nxt = observation.next_sequence;
    registration.receive_state_initialized = true;
    Ok(())
}

#[inline]
fn tcp_apply_passive_accept_observation(
    registration: &mut TcpConnectionRegistration,
    observation: TcpHandshakeObservation,
) -> HammerResult<()> {
    if observation.syn() {
        registration.irs = observation.sequence;
        registration.rcv_nxt = observation.next_sequence;
        registration.receive_state_initialized = true;
        registration.snd_wnd = observation.advertised_window;
    }
    if let Some(acknowledgment) = observation.acknowledgment {
        registration.iss = acknowledgment.wrapping_sub(1);
        registration.snd_una = acknowledgment;
        registration.snd_nxt = acknowledgment;
        registration.snd_wnd = observation.advertised_window;
        registration.send_state_initialized = true;
        if registration.receive_state_initialized {
            registration.rcv_nxt = tcp_seq_max(registration.rcv_nxt, observation.next_sequence);
        } else {
            registration.irs = observation.sequence.wrapping_sub(1);
            registration.rcv_nxt = observation.next_sequence;
            registration.receive_state_initialized = true;
        }
    }
    Ok(())
}

#[inline]
fn tcp_state_allows_transport_payload_send(state: hammer_core::protocol::tcp::TcpState) -> bool {
    matches!(
        state,
        hammer_core::protocol::tcp::TcpState::Established
            | hammer_core::protocol::tcp::TcpState::CloseWait
    )
}

#[inline]
fn tcp_state_allows_transport_output(state: hammer_core::protocol::tcp::TcpState) -> bool {
    tcp_state_allows_transport_payload_send(state)
        || matches!(
            state,
            hammer_core::protocol::tcp::TcpState::FinWait1
                | hammer_core::protocol::tcp::TcpState::LastAck
        )
}

#[inline]
fn tcp_state_requires_local_fin(state: hammer_core::protocol::tcp::TcpState) -> bool {
    matches!(
        state,
        hammer_core::protocol::tcp::TcpState::FinWait1
            | hammer_core::protocol::tcp::TcpState::LastAck
    )
}

#[inline]
fn tcp_apply_deferred_local_shutdown(registration: &mut TcpConnectionRegistration) {
    let Some((direction, _)) = registration.shutdown else {
        return;
    };
    let Some(next_state) = tcp_state_after_local_shutdown(registration.state, direction) else {
        return;
    };
    registration.state = next_state;
    registration.pending_fin = true;
}

#[inline]
fn tcp_state_acknowledges_retransmit_queue(
    previous: hammer_core::protocol::tcp::TcpState,
    next: hammer_core::protocol::tcp::TcpState,
) -> bool {
    matches!(
        (previous, next),
        (
            hammer_core::protocol::tcp::TcpState::FinWait1,
            hammer_core::protocol::tcp::TcpState::FinWait2
                | hammer_core::protocol::tcp::TcpState::TimeWait,
        ) | (
            hammer_core::protocol::tcp::TcpState::Closing,
            hammer_core::protocol::tcp::TcpState::TimeWait,
        ) | (
            hammer_core::protocol::tcp::TcpState::LastAck,
            hammer_core::protocol::tcp::TcpState::Closed,
        )
    )
}

#[inline]
fn tcp_connection_snapshot_from_registration(
    registration: &TcpConnectionRegistration,
) -> TcpConnectionSnapshot {
    TcpConnectionSnapshot {
        lookup_id: registration.lookup_id,
        connection_id: registration.connection_id,
        owner_worker: registration.owner_worker,
        state: registration.state,
        local_port: registration.local_port,
        local: registration.local,
        remote: registration.remote,
        iss: registration.iss,
        irs: registration.irs,
        snd_una: registration.snd_una,
        snd_nxt: registration.snd_nxt,
        snd_wnd: registration.snd_wnd,
        rcv_nxt: registration.rcv_nxt,
        rcv_wnd: registration.rcv_wnd,
    }
}

#[inline]
fn merge_tcp_shutdown(
    current: Option<(TcpShutdownDirection, TcpCloseReason)>,
    direction: TcpShutdownDirection,
    reason: TcpCloseReason,
) -> (TcpShutdownDirection, TcpCloseReason) {
    let Some((current_direction, current_reason)) = current else {
        return (direction, reason);
    };
    let merged_direction = merge_tcp_shutdown_direction(current_direction, direction);
    let merged_reason = if tcp_write_side_is_shutdown(Some((current_direction, current_reason))) {
        current_reason
    } else if tcp_write_side_is_shutdown(Some((merged_direction, reason))) {
        reason
    } else {
        current_reason
    };
    (merged_direction, merged_reason)
}

#[inline]
fn merge_tcp_shutdown_direction(
    current: TcpShutdownDirection,
    next: TcpShutdownDirection,
) -> TcpShutdownDirection {
    match (current, next) {
        (TcpShutdownDirection::Both, _)
        | (_, TcpShutdownDirection::Both)
        | (TcpShutdownDirection::Read, TcpShutdownDirection::Write)
        | (TcpShutdownDirection::Write, TcpShutdownDirection::Read) => TcpShutdownDirection::Both,
        (TcpShutdownDirection::Read, TcpShutdownDirection::Read) => TcpShutdownDirection::Read,
        (TcpShutdownDirection::Write, TcpShutdownDirection::Write) => TcpShutdownDirection::Write,
    }
}

#[inline]
fn tcp_state_after_local_shutdown(
    state: hammer_core::protocol::tcp::TcpState,
    direction: TcpShutdownDirection,
) -> Option<hammer_core::protocol::tcp::TcpState> {
    if matches!(direction, TcpShutdownDirection::Read) {
        return None;
    }
    match state {
        hammer_core::protocol::tcp::TcpState::Established => {
            Some(hammer_core::protocol::tcp::TcpState::FinWait1)
        }
        hammer_core::protocol::tcp::TcpState::CloseWait => {
            Some(hammer_core::protocol::tcp::TcpState::LastAck)
        }
        _ => None,
    }
}

#[inline]
fn tcp_state_after_remote_fin(
    state: hammer_core::protocol::tcp::TcpState,
) -> Option<hammer_core::protocol::tcp::TcpState> {
    match state {
        hammer_core::protocol::tcp::TcpState::Established => {
            Some(hammer_core::protocol::tcp::TcpState::CloseWait)
        }
        hammer_core::protocol::tcp::TcpState::FinWait1 => {
            Some(hammer_core::protocol::tcp::TcpState::Closing)
        }
        hammer_core::protocol::tcp::TcpState::FinWait2 => {
            Some(hammer_core::protocol::tcp::TcpState::TimeWait)
        }
        _ => None,
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
        app_control.install_tcp_established_backend()?;
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

    #[doc(hidden)]
    pub fn tcp_shutdown_for_flow_for_test(
        &self,
        flow: AppFlowId,
    ) -> Option<(TcpShutdownDirection, TcpCloseReason)> {
        self.control_call(move |inner| {
            // SAFETY: this helper already runs on the single control thread.
            let state = unsafe { &*inner.app_control.state.inner.get() };
            state
                .tcp_connections
                .iter()
                .find(|registration| registration.flow == flow)
                .and_then(|registration| registration.shutdown)
        })
        .ok()
        .flatten()
    }

    #[doc(hidden)]
    pub fn tcp_pending_send_payloads_for_flow_for_test(&self, flow: AppFlowId) -> Vec<Vec<u8>> {
        self.control_call(move |inner| {
            // SAFETY: this helper already runs on the single control thread.
            let state = unsafe { &*inner.app_control.state.inner.get() };
            state
                .tcp_connections
                .iter()
                .find(|registration| registration.flow == flow)
                .map(|registration| registration.pending_send_payloads.iter().cloned().collect())
                .unwrap_or_default()
        })
        .unwrap_or_default()
    }

    #[doc(hidden)]
    pub fn tcp_transport_send_payloads_for_flow_for_test(&self, flow: AppFlowId) -> Vec<Vec<u8>> {
        self.control_call(move |inner| {
            // SAFETY: this helper already runs on the single control thread.
            let state = unsafe { &*inner.app_control.state.inner.get() };
            state.tcp_transport_send_payloads_for_flow_for_test(flow)
        })
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default()
    }

    #[doc(hidden)]
    pub fn tcp_retransmit_queue_len_for_flow_for_test(&self, flow: AppFlowId) -> usize {
        self.control_call(move |inner| {
            // SAFETY: this helper already runs on the single control thread.
            let state = unsafe { &*inner.app_control.state.inner.get() };
            state
                .tcp_connections
                .iter()
                .find(|registration| registration.flow == flow)
                .map(|registration| registration.retransmit_queue.len())
                .unwrap_or_default()
        })
        .unwrap_or_default()
    }

    #[doc(hidden)]
    pub fn tcp_take_transport_send_payload_for_flow_for_test(
        &self,
        flow: AppFlowId,
    ) -> Option<Vec<u8>> {
        self.control_call(move |inner| {
            // SAFETY: this helper already runs on the single control thread.
            let state = unsafe { &mut *inner.app_control.state.inner.get() };
            state.take_tcp_transport_send_payload_for_flow_for_test(flow)
        })
        .ok()
        .and_then(Result::ok)
        .flatten()
    }

    #[doc(hidden)]
    pub fn install_tcp_output_backend_for_test<O>(&self, backend: Arc<O>)
    where
        O: TcpOutputBackend + 'static,
    {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.app_control.install_tcp_output_backend(backend);
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
        inner.app_control.cancel_all_tcp_retransmit_timers();
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

    fn handle_tcp_established_ack_progress_for_test(
        &self,
        observation: TcpEstablishedAckObservation,
    ) -> HammerResult<()> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner
            .app_control
            .handle_tcp_established_ack_progress(observation)
    }

    fn handle_tcp_established_close_for_test(
        &self,
        observation: TcpEstablishedObservation,
    ) -> HammerResult<()> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner.app_control.handle_tcp_established_close(observation)
    }

    fn promote_pending_syn_sent_for_test(
        &self,
        observation: TcpSynSentObservation,
    ) -> HammerResult<()> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner
            .app_control
            .handle_tcp_syn_sent_observation(observation)
    }

    fn handle_tcp_accept_observation_for_test(
        &self,
        listener_id: TcpLookupId,
        remote: SocketAddr,
        local: SocketAddr,
        observation: TcpHandshakeObservation,
    ) -> HammerResult<()> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        inner
            .app_control
            .handle_tcp_accept_observation(listener_id, remote, local, observation)
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

    fn tcp_connection_id_for_flow_for_test(&self, flow: AppFlowId) -> Option<TcpConnectionId> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        let connection_id = inner
            .app_control
            .with_state(move |state| {
                Ok(state
                    .tcp_connections
                    .iter()
                    .find(|registration| registration.flow == flow)
                    .and_then(|registration| registration.connection_id))
            })
            .ok()
            .flatten();
        drop(inner);
        connection_id
    }

    fn shared_tcp_has_timer_for_test(
        &self,
        connection_id: TcpConnectionId,
        kind: hammer_core::protocol::tcp::TcpTimerKind,
    ) -> bool {
        let inner = self.inner.lock().expect("service mutex poisoned");
        let control = inner
            .app_control
            .with_state(move |state| Ok(state.shared_tcp_control.clone()))
            .ok()
            .flatten();
        drop(inner);
        control.is_some_and(|control| control.has_timer_for_test(connection_id, kind))
    }

    fn apply_shared_tcp_action_for_test(
        &self,
        action: hammer_core::protocol::tcp::TcpControlPlaneAction,
    ) -> HammerResult<()> {
        let inner = self.inner.lock().expect("service mutex poisoned");
        let control = inner
            .app_control
            .with_state(move |state| Ok(state.shared_tcp_control.clone()))
            .ok()
            .flatten()
            .ok_or_else(|| HammerError::internal("shared tcp control is not installed"))?;
        drop(inner);
        control
            .apply(action)
            .map_err(|err| HammerError::internal(format!("apply shared tcp test action: {err}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::tcp::{TcpOutputBackend, TcpOutputSegment, TcpSynSentObservation};
    use hammer_core::StartStage;
    use hammer_core::protocol::tcp::{
        TcpCapabilities, TcpCloseReason, TcpConnectionKey, TcpControlPlaneAction,
        TcpHandshakeObservation, TcpListenerId, TcpListenerKey, TcpTimerId, TcpTimerKind,
        TcpWorkerEvent,
    };
    use hammer_runtime::app::{
        AppBufferLease, AppCqeData, AppFlowId, AppObjectRef, AppOpcode, AppSend, AppSqeData,
        AppSqeDescriptor, AppUserData,
    };
    use hammer_runtime::spawn::with_data_plane_buffers;
    use std::net::SocketAddr;
    use std::net::{Ipv4Addr, Shutdown};
    use std::panic::{self, PanicHookInfo};
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    struct NoopPlatform;

    #[derive(Default)]
    struct RecordingTcpOutputBackend {
        emitted: Arc<Mutex<Vec<TcpOutputSegment>>>,
    }

    impl TcpOutputBackend for RecordingTcpOutputBackend {
        fn emit_segment(&self, segment: TcpOutputSegment) -> Result<(), CoreError> {
            self.emitted
                .lock()
                .map_err(|_| CoreError::internal("recording tcp output backend poisoned"))?
                .push(segment);
            Ok(())
        }
    }

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

    fn tcp_syn_ack_observation(
        sequence: u32,
        acknowledgment: u32,
        advertised_window: u32,
    ) -> TcpHandshakeObservation {
        TcpHandshakeObservation::new(
            crate::transport::tcp::TCP_FLAG_SYN | crate::transport::tcp::TCP_FLAG_ACK,
            sequence,
            Some(acknowledgment),
            advertised_window,
            sequence.wrapping_add(1),
        )
    }

    fn tcp_ack_observation(
        sequence: u32,
        acknowledgment: u32,
        advertised_window: u32,
    ) -> TcpHandshakeObservation {
        TcpHandshakeObservation::new(
            crate::transport::tcp::TCP_FLAG_ACK,
            sequence,
            Some(acknowledgment),
            advertised_window,
            sequence,
        )
    }

    fn tcp_syn_observation(sequence: u32, advertised_window: u32) -> TcpHandshakeObservation {
        TcpHandshakeObservation::new(
            crate::transport::tcp::TCP_FLAG_SYN,
            sequence,
            None,
            advertised_window,
            sequence.wrapping_add(1),
        )
    }

    fn observe_active_syn_ack_for_test(
        service: &Arc<RuntimeService>,
        connection_id: TcpConnectionId,
        peer: SocketAddr,
        local: SocketAddr,
    ) {
        let lookup_id = service
            .app_control_snapshot_for_test()
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .expect("active connect snapshot")
            .lookup_id;
        service
            .promote_pending_syn_sent_for_test(TcpSynSentObservation::new(
                lookup_id,
                peer,
                local,
                crate::transport::tcp::TcpState::SynSent,
                crate::transport::tcp::TcpState::Established,
                tcp_syn_ack_observation(0x5566_7788, 0x1020_3041, 0x3456),
            ))
            .expect("observe active syn-ack");
    }

    fn panic_capture_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn panic_message(info: &PanicHookInfo<'_>) -> String {
        if let Some(message) = info.payload().downcast_ref::<&str>() {
            return (*message).to_owned();
        }
        if let Some(message) = info.payload().downcast_ref::<String>() {
            return message.clone();
        }
        format!("{info}")
    }

    fn capture_panics<T>(f: impl FnOnce() -> T) -> (T, Vec<String>) {
        let _guard = panic_capture_lock()
            .lock()
            .expect("panic capture lock poisoned");
        let messages = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&messages);
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            captured
                .lock()
                .expect("captured panic messages poisoned")
                .push(panic_message(info));
        }));
        let result = f();
        panic::set_hook(previous);
        let messages = match Arc::try_unwrap(messages) {
            Ok(messages) => messages
                .into_inner()
                .expect("captured panic messages poisoned"),
            Err(messages) => messages
                .lock()
                .expect("captured panic messages poisoned")
                .clone(),
        };
        (result, messages)
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

    fn request_tcp_shutdown(app: &AppContext, flow: AppFlowId, how: Shutdown) {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime")
            .block_on(async {
                app.spawn_on_flow(flow, move |worker| async move {
                    worker
                        .runtime()
                        .shutdown(how)
                        .await
                        .expect("enqueue tcp shutdown");
                })
                .await
                .expect("spawn flow task");
            });
    }

    fn request_tcp_send(app: &AppContext, flow: AppFlowId, payload: &[u8]) {
        let payload = payload.to_vec();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("driver runtime")
            .block_on(async move {
                app.spawn_on_flow(flow, move |worker| async move {
                    let buffers = with_data_plane_buffers(Clone::clone);
                    let index = buffers
                        .alloc_index_with_bytes(Default::default(), &payload)
                        .expect("alloc tcp send buffer");
                    worker
                        .runtime()
                        .send(AppSend::new(AppBufferLease::from_buffer(buffers, index)))
                        .await
                        .expect("enqueue tcp send");
                })
                .await
                .expect("spawn flow task");
            });
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
    fn runtime_service_accepts_tcp_listener_after_final_ack() {
        let ((), panics) = capture_panics(|| {
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
            let incoming = TcpWorkerEvent::IncomingConnection {
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
            };

            service
                .handle_tcp_worker_event_for_test(incoming)
                .expect("register passive-open tcp connection");
            assert!(
                rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "initial SYN/passive-open must not complete listener accept"
            );

            let syn_rcvd = service
                .app_control_snapshot_for_test()
                .tcp_lookup
                .lookup_connection_v4(TcpV4ConnectionKey::new(
                    0,
                    Ipv4Addr::new(127, 0, 0, 1),
                    local.port(),
                    Ipv4Addr::new(198, 51, 100, 72),
                    remote.port(),
                ))
                .expect("published passive-open lookup");
            let connection_id = service
                .app_control_snapshot_for_test()
                .tcp_connections
                .lookup_by_lookup_id(syn_rcvd.id)
                .expect("published passive-open snapshot")
                .connection_id
                .expect("passive-open connection id");
            assert_eq!(syn_rcvd.kind, TcpLookupKind::EstablishedConnection);
            assert_eq!(syn_rcvd.owner_worker, DataWorkerId::new(1));
            assert_eq!(
                service
                    .app_control_snapshot_for_test()
                    .tcp_connections
                    .lookup_by_lookup_id(syn_rcvd.id)
                    .expect("published passive-open snapshot")
                    .state,
                crate::transport::tcp::TcpState::SynRcvd
            );
            wait_for(Duration::from_secs(1), || {
                service.shared_tcp_connection_state_for_test(connection_id)
                    == Some(crate::transport::tcp::TcpState::SynRcvd)
            });

            service
                .handle_tcp_worker_event_for_test(incoming)
                .expect("complete passive-open tcp connection");
            let accept_cqe = rx
                .recv_timeout(Duration::from_secs(1))
                .expect("receive accept result");
            let accepted_flow = match accept_cqe.payload() {
                AppCqeData::Accepted {
                    listener: cqe_listener,
                    flow: cqe_flow,
                } => {
                    assert_eq!(cqe_listener, listener);
                    assert_eq!(app.owner_worker_for_flow(cqe_flow).expect("flow owner"), 1);
                    cqe_flow
                }
                other => panic!("unexpected accept cqe payload: {other:?}"),
            };

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
            assert_eq!(
                state
                    .tcp_connections
                    .lookup_by_lookup_id(published.id)
                    .expect("published established snapshot")
                    .state,
                crate::transport::tcp::TcpState::Established
            );
            wait_for(Duration::from_secs(1), || {
                service.shared_tcp_connection_state_for_test(connection_id)
                    == Some(crate::transport::tcp::TcpState::Established)
            });

            request_tcp_shutdown(&app, accepted_flow, Shutdown::Write);
            wait_for(Duration::from_secs(1), || {
                service.tcp_shutdown_for_flow_for_test(accepted_flow)
                    == Some((
                        hammer_core::protocol::tcp::TcpShutdownDirection::Write,
                        TcpCloseReason::LocalShutdown,
                    ))
            });

            service.close().expect("close service");
        });

        assert!(
            panics.is_empty(),
            "unexpected background panic(s): {panics:#?}"
        );
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
    fn runtime_service_publishes_tcp_connection_receive_snapshots() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local addr");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        let state = service.app_control_snapshot_for_test();
        let pending = state
            .tcp_connections
            .lookup_by_lookup_id(1)
            .expect("published pending connection snapshot");
        assert_eq!(pending.connection_id, Some(connection_id));
        assert_eq!(pending.owner_worker, DataWorkerId::new(1));
        assert_eq!(pending.state, crate::transport::tcp::TcpState::SynSent);
        assert_eq!(pending.local_port, 49_152);
        assert_eq!(
            pending.local,
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 49_152))
        );
        assert_eq!(pending.remote, peer);
        assert_eq!(pending.snd_wnd, 0);
        assert_eq!(pending.rcv_wnd, 65_535);

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
            .expect("promote pending connect via state change");

        wait_for(Duration::from_secs(1), || {
            service
                .app_control_snapshot_for_test()
                .tcp_connections
                .lookup_by_connection_id(connection_id)
                .is_some_and(|snapshot| {
                    snapshot.state == crate::transport::tcp::TcpState::Established
                        && snapshot.local == Some(local)
                })
        });

        let established = service
            .app_control_snapshot_for_test()
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .expect("published established connection snapshot");
        assert_eq!(established.lookup_id, 1);
        assert_eq!(established.connection_id, Some(connection_id));
        assert_eq!(established.owner_worker, DataWorkerId::new(1));
        assert_eq!(
            established.state,
            crate::transport::tcp::TcpState::Established
        );
        assert_eq!(established.local_port, local.port());
        assert_eq!(established.local, Some(local));
        assert_eq!(established.remote, peer);

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_connect_installs_shared_syn_sent_connection_and_timer() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::SynSent)
                && service.shared_tcp_has_timer_for_test(connection_id, TcpTimerKind::Connect)
        });

        assert_eq!(
            service.shared_tcp_connection_state_for_test(connection_id),
            Some(crate::transport::tcp::TcpState::SynSent)
        );
        assert!(
            service.shared_tcp_has_timer_for_test(connection_id, TcpTimerKind::Connect),
            "active connect must arm a control-plane connect timer"
        );

        let state = service.app_control_snapshot_for_test();
        assert!(
            state
                .tcp_lookup
                .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                    0,
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_some(),
            "connect must remain published as pending syn-sent until promotion"
        );
        assert!(
            state
                .tcp_lookup
                .lookup_connection_v4(TcpV4ConnectionKey::new(
                    0,
                    Ipv4Addr::UNSPECIFIED,
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_none(),
            "installing shared syn-sent state must not publish an established lookup"
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_app_send_moves_beyond_pending_connect_buffer_after_established_transition() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        request_tcp_send(&app, flow, b"pending-send");

        wait_for(Duration::from_secs(1), || {
            service.tcp_pending_send_payloads_for_flow_for_test(flow)
                == vec![b"pending-send".to_vec()]
        });
        assert_eq!(
            service.tcp_pending_send_payloads_for_flow_for_test(flow),
            vec![b"pending-send".to_vec()]
        );

        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });
        wait_for(Duration::from_secs(1), || {
            service
                .tcp_pending_send_payloads_for_flow_for_test(flow)
                .is_empty()
        });
        assert!(
            service
                .tcp_pending_send_payloads_for_flow_for_test(flow)
                .is_empty(),
            "pending-connect staging should hand off sends once the connection is established"
        );
        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .len()
                == 1
        });
        assert!(
            service
                .tcp_transport_send_payloads_for_flow_for_test(flow)
                .is_empty(),
            "transport staging must drain once the output pump emits a real segment"
        );
        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        let segment = emitted.first().expect("emitted tcp output segment");
        assert_eq!(segment.connection_id, connection_id);
        assert_eq!(segment.local, local);
        assert_eq!(segment.remote, peer);
        assert_eq!(segment.sequence, 0x1020_3041);
        assert_eq!(segment.acknowledgment, 0x5566_7789);
        assert_eq!(segment.payload, b"pending-send".to_vec());
        assert!(segment.flags & 0x10 != 0, "data segment must carry ACK");
        assert!(
            !segment.packet.is_empty(),
            "emitted packet bytes must exist"
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_state_change_waits_for_syn_ack_transport_state_before_emitting_payload() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local addr");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        request_tcp_send(&app, flow, b"pending-send");
        wait_for(Duration::from_secs(1), || {
            service.tcp_pending_send_payloads_for_flow_for_test(flow)
                == vec![b"pending-send".to_vec()]
        });

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
            .expect("bind established connection");

        std::thread::sleep(Duration::from_millis(50));
        assert!(
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .is_empty(),
            "state-change alone must not emit payload before syn-ack transport state is observed"
        );

        let lookup_id = service
            .app_control_snapshot_for_test()
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .expect("connection snapshot")
            .lookup_id;
        service
            .promote_pending_syn_sent_for_test(TcpSynSentObservation::new(
                lookup_id,
                peer,
                local,
                crate::transport::tcp::TcpState::SynSent,
                crate::transport::tcp::TcpState::Established,
                tcp_syn_ack_observation(0x5566_7788, 0x1020_3041, 0x3456),
            ))
            .expect("observe syn-ack transport state");

        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .len()
                == 1
        });

        let snapshot = service.app_control_snapshot_for_test();
        let connection = snapshot
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .expect("updated connection snapshot");
        assert_eq!(connection.iss, 0x1020_3040);
        assert_eq!(connection.snd_una, 0x1020_3041);
        assert_eq!(connection.snd_nxt, 0x1020_304d);
        assert_eq!(connection.irs, 0x5566_7788);
        assert_eq!(connection.rcv_nxt, 0x5566_7789);
        assert_eq!(connection.snd_wnd, 0x3456);

        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        let segment = emitted.first().expect("emitted tcp output segment");
        assert_eq!(segment.sequence, 0x1020_3041);
        assert_eq!(segment.acknowledgment, 0x5566_7789);
        assert_eq!(segment.payload, b"pending-send".to_vec());

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_retransmits_unacked_payload_after_timeout() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        request_tcp_send(&app, flow, b"retransmit-me");

        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .len()
                >= 2
        });

        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        assert!(
            emitted.len() >= 2,
            "unacked payload should be retransmitted after timeout"
        );
        let first = emitted.first().expect("first output segment");
        let second = emitted.get(1).expect("retransmitted output segment");
        assert_eq!(first.connection_id, connection_id);
        assert_eq!(second.connection_id, connection_id);
        assert_eq!(first.sequence, second.sequence);
        assert_eq!(first.acknowledgment, second.acknowledgment);
        assert_eq!(first.flags, second.flags);
        assert_eq!(first.payload, second.payload);

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
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

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
                tcp_syn_ack_observation(0x5566_7788, 0x1020_3041, 0x3456),
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
        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
                && !service.shared_tcp_has_timer_for_test(
                    connection_id,
                    hammer_core::protocol::tcp::TcpTimerKind::Connect,
                )
        });

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_passive_accept_observation_initializes_transport_state_for_first_payload() {
        let service = new_test_service("");
        let app = service.app_context();
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let listener = app
            .bind_tcp_listener("127.0.0.1:7201".parse().expect("listener bind"), 1)
            .expect("bind tcp listener");
        let listener_lookup_id = {
            let state = service.app_control_snapshot_for_test();
            state
                .tcp_listeners
                .iter()
                .find(|registration| registration.socket == listener)
                .expect("listener registration")
                .lookup_id
        };

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (accept_tx, accept_rx) = std::sync::mpsc::channel();
        let owner_worker = app
            .owner_worker_for_socket(listener)
            .expect("listener owner worker");
        let app_for_accept = app.clone();
        service
            .spawn_app_on_worker(owner_worker, move || async move {
                let backend = app_for_accept
                    .local_backend_for_socket(listener)
                    .expect("listener backend");
                backend
                    .try_push_sqe_descriptor(AppSqeDescriptor::new(
                        AppOpcode::Accept,
                        AppUserData::new(92),
                        AppObjectRef::Socket(listener),
                        AppSqeData::Accept,
                    ))
                    .expect("push accept sqe");
                ready_tx.send(()).expect("signal accept ready");

                let accept_cqe = backend
                    .next_cqe_descriptor()
                    .await
                    .expect("accept cqe descriptor");
                accept_tx.send(accept_cqe).expect("send accept result");
            })
            .expect("spawn accept worker");
        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("wait accept ready");

        let remote: SocketAddr = "198.51.100.72:40720".parse().expect("remote addr");
        let local: SocketAddr = "127.0.0.1:7201".parse().expect("local addr");
        service
            .handle_tcp_accept_observation_for_test(
                listener_lookup_id,
                remote,
                local,
                tcp_syn_observation(0x4455_6677, 0x1234),
            )
            .expect("observe passive-open syn");

        let syn_rcvd = service
            .app_control_snapshot_for_test()
            .tcp_lookup
            .lookup_connection_v4(TcpV4ConnectionKey::new(
                0,
                Ipv4Addr::new(127, 0, 0, 1),
                local.port(),
                Ipv4Addr::new(198, 51, 100, 72),
                remote.port(),
            ))
            .expect("published passive-open lookup");
        let syn_snapshot = service
            .app_control_snapshot_for_test()
            .tcp_connections
            .lookup_by_lookup_id(syn_rcvd.id)
            .expect("syn-rcvd snapshot");
        assert_eq!(syn_snapshot.irs, 0x4455_6677);
        assert_eq!(syn_snapshot.rcv_nxt, 0x4455_6678);
        assert_eq!(syn_snapshot.snd_wnd, 0x1234);

        service
            .handle_tcp_accept_observation_for_test(
                listener_lookup_id,
                remote,
                local,
                tcp_ack_observation(0x4455_6678, 0x1020_3041, 0x2345),
            )
            .expect("observe passive-open final ack");

        let accept_cqe = accept_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive accept result");
        let accepted_flow = match accept_cqe.payload() {
            AppCqeData::Accepted { flow, .. } => flow,
            other => panic!("unexpected accept cqe payload: {other:?}"),
        };

        request_tcp_send(&app, accepted_flow, b"accepted-data");
        wait_for(Duration::from_secs(1), || {
            !output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .is_empty()
        });

        let state = service.app_control_snapshot_for_test();
        let connection_id = state
            .tcp_connections
            .lookup_by_lookup_id(syn_rcvd.id)
            .expect("accepted snapshot")
            .connection_id
            .expect("passive connection id");
        let connection = state
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .expect("accepted connection snapshot");
        assert_eq!(connection.iss, 0x1020_3040);
        assert_eq!(connection.snd_una, 0x1020_3041);
        assert_eq!(connection.irs, 0x4455_6677);
        assert_eq!(connection.rcv_nxt, 0x4455_6678);
        assert_eq!(connection.snd_wnd, 0x2345);

        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        let first = emitted.first().expect("first passive-open payload");
        assert_eq!(first.sequence, 0x1020_3041);
        assert_eq!(first.acknowledgment, 0x4455_6678);
        assert_eq!(first.payload, b"accepted-data".to_vec());

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_state_events_update_shared_tcp_control_plane() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

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
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

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

        let first = app
            .connect_tcp_stream(peer, 1)
            .expect("first connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(first)
            .expect("active connect connection id");
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
    fn runtime_service_close_tcp_flow_reclaims_pending_connect_lookup_and_ephemeral_port() {
        let ((), panics) = capture_panics(|| {
            let service = new_test_service("");
            let app = service.app_context();
            let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

            let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
            assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

            app.close_tcp_flow(flow).expect("close pending tcp flow");

            let state = service.app_control_snapshot_for_test();
            assert!(
                state
                    .tcp_lookup
                    .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                        0,
                        49_152,
                        Ipv4Addr::new(198, 51, 100, 88),
                        443,
                    ))
                    .is_none(),
                "pending lookup must be revoked after app close"
            );

            let second = app
                .connect_tcp_stream(peer, 1)
                .expect("second connect tcp stream");
            assert_ne!(second, flow);
            let reused = service
                .app_control_snapshot_for_test()
                .tcp_lookup
                .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                    0,
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .expect("reused pending lookup");
            assert_eq!(reused.kind, TcpLookupKind::SynSentConnection);
            assert_eq!(reused.owner_worker, DataWorkerId::new(1));

            service.close().expect("close service");
        });

        assert!(
            panics.is_empty(),
            "unexpected background panic(s): {panics:#?}"
        );
    }

    #[test]
    fn runtime_service_close_tcp_flow_routes_established_connection_through_shared_control_plane() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
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
            .expect("bind established connection");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        app.close_tcp_flow(flow)
            .expect("close established tcp flow");

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
    fn runtime_service_closed_before_state_changed_does_not_revive_pending_connect() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local addr");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::Closed {
                connection_id,
                reason: TcpCloseReason::LocalRequest,
            })
            .expect("ingest early close");
        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::StateChanged {
                connection_id,
                key: TcpConnectionKey::v4(
                    0,
                    Ipv4Addr::new(192, 0, 2, 44),
                    local.port(),
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ),
                state: crate::transport::tcp::TcpState::Established,
            })
            .expect("ingest late state change");

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
        });

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
            "pending lookup must not survive close-before-state ordering"
        );
        assert!(
            state
                .tcp_lookup
                .lookup_connection_v4(TcpV4ConnectionKey::new(
                    0,
                    Ipv4Addr::new(192, 0, 2, 44),
                    local.port(),
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_none(),
            "late state change must not revive a closed active connect"
        );
        assert!(
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none(),
            "shared control plane must not retain a revived connection"
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_duplicate_closed_worker_event_is_idempotent() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

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
            .expect("bind established connection");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::Closed {
                connection_id,
                reason: TcpCloseReason::LocalRequest,
            })
            .expect("ingest first close");
        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::Closed {
                connection_id,
                reason: TcpCloseReason::LocalRequest,
            })
            .expect("ingest duplicate close");

        wait_for(Duration::from_secs(1), || {
            let state = service.app_control_snapshot_for_test();
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none()
                && state
                    .tcp_lookup
                    .lookup_connection_v4(TcpV4ConnectionKey::new(
                        0,
                        Ipv4Addr::new(192, 0, 2, 44),
                        49_152,
                        Ipv4Addr::new(198, 51, 100, 88),
                        443,
                    ))
                    .is_none()
        });

        assert!(
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none(),
            "duplicate close must leave shared control plane closed"
        );
        assert!(
            service
                .app_control_snapshot_for_test()
                .tcp_lookup
                .lookup_connection_v4(TcpV4ConnectionKey::new(
                    0,
                    Ipv4Addr::new(192, 0, 2, 44),
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_none(),
            "duplicate close must not republish projected connection lookup"
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_shutdown_before_state_changed_does_not_error_or_block_late_install() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::ShutdownObserved {
                connection_id,
                direction: hammer_core::protocol::tcp::TcpShutdownDirection::Write,
                reason: TcpCloseReason::LocalShutdown,
            })
            .expect("ingest early shutdown");
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
            .expect("ingest late state change");
        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::FinWait1)
        });
        assert_eq!(
            service.shared_tcp_connection_state_for_test(connection_id),
            Some(crate::transport::tcp::TcpState::FinWait1)
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_worker_shutdown_observed_transitions_established_connection_to_fin_wait1() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

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
            .expect("bind established connection");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::ShutdownObserved {
                connection_id,
                direction: hammer_core::protocol::tcp::TcpShutdownDirection::Write,
                reason: TcpCloseReason::LocalShutdown,
            })
            .expect("observe worker shutdown");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::FinWait1)
        });
        assert_eq!(
            service.shared_tcp_connection_state_for_test(connection_id),
            Some(crate::transport::tcp::TcpState::FinWait1)
        );
        assert_eq!(
            service
                .app_control_snapshot_for_test()
                .tcp_connections
                .lookup_by_connection_id(connection_id)
                .map(|connection| connection.state),
            Some(crate::transport::tcp::TcpState::FinWait1)
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_app_shutdown_transitions_established_connection_to_fin_wait1() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

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
            .expect("bind established connection");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        request_tcp_shutdown(&app, flow, Shutdown::Write);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::FinWait1)
        });
        assert_eq!(
            service.shared_tcp_connection_state_for_test(connection_id),
            Some(crate::transport::tcp::TcpState::FinWait1)
        );
        assert_eq!(
            service
                .app_control_snapshot_for_test()
                .tcp_connections
                .lookup_by_connection_id(connection_id)
                .map(|connection| connection.state),
            Some(crate::transport::tcp::TcpState::FinWait1)
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_app_shutdown_emits_fin_ack_segment_for_established_connection() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        request_tcp_shutdown(&app, flow, Shutdown::Write);

        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .iter()
                .any(|segment| segment.connection_id == connection_id && segment.flags & 0x01 != 0)
        });

        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        let fin = emitted
            .iter()
            .find(|segment| segment.connection_id == connection_id && segment.flags & 0x01 != 0)
            .expect("FIN segment emitted for shutdown");
        assert_eq!(fin.local, local);
        assert_eq!(fin.remote, peer);
        assert_eq!(fin.payload, Vec::<u8>::new());
        assert!(fin.flags & 0x10 != 0, "shutdown FIN must also ACK");

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_payload_send_enters_retransmit_bookkeeping_and_timer_reemits_segment() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        request_tcp_send(&app, flow, b"retransmit-me");

        wait_for(Duration::from_secs(1), || {
            !output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .is_empty()
                && service.tcp_retransmit_queue_len_for_flow_for_test(flow) == 1
        });
        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .len()
                >= 2
        });

        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        assert!(
            emitted.len() >= 2,
            "retransmit timer should re-emit the outstanding payload segment"
        );
        let first = &emitted[0];
        let second = &emitted[1];
        assert_eq!(first.connection_id, connection_id);
        assert_eq!(first.payload, b"retransmit-me".to_vec());
        assert_eq!(second.connection_id, connection_id);
        assert_eq!(second.sequence, first.sequence);
        assert_eq!(second.acknowledgment, first.acknowledgment);
        assert_eq!(second.flags, first.flags);
        assert_eq!(second.payload, first.payload);
        assert_eq!(service.tcp_retransmit_queue_len_for_flow_for_test(flow), 1);

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_ack_progress_prunes_fin_retransmit_queue_before_timer_refires() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        request_tcp_shutdown(&app, flow, Shutdown::Write);

        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .iter()
                .any(|segment| {
                    segment.connection_id == connection_id && segment.flags & TCP_FLAG_FIN != 0
                })
                && service.tcp_retransmit_queue_len_for_flow_for_test(flow) == 1
        });

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
                state: crate::transport::tcp::TcpState::FinWait2,
            })
            .expect("observe FIN acknowledgment");

        wait_for(Duration::from_secs(1), || {
            service.tcp_retransmit_queue_len_for_flow_for_test(flow) == 0
        });

        let emitted_before = output
            .emitted
            .lock()
            .expect("tcp output capture poisoned")
            .len();
        std::thread::sleep(TCP_OUTPUT_RETRANSMIT_TIMEOUT + Duration::from_millis(40));
        let emitted_after = output
            .emitted
            .lock()
            .expect("tcp output capture poisoned")
            .len();
        assert_eq!(
            emitted_after, emitted_before,
            "ACK-driven FIN progression must prune retransmit bookkeeping before the timer re-fires"
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_ack_progress_prunes_payload_retransmit_queue_and_stops_retransmit() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.188:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.144:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        request_tcp_send(&app, flow, b"acked-payload");

        wait_for(Duration::from_secs(1), || {
            !output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .is_empty()
                && service.tcp_retransmit_queue_len_for_flow_for_test(flow) == 1
        });

        let first = output
            .emitted
            .lock()
            .expect("tcp output capture poisoned")
            .first()
            .cloned()
            .expect("initial payload segment");
        let emitted_before_ack = output
            .emitted
            .lock()
            .expect("tcp output capture poisoned")
            .len();
        let lookup_id = service
            .app_control_snapshot_for_test()
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .map(|connection| connection.lookup_id)
            .expect("lookup id for connected flow");

        service
            .handle_tcp_established_ack_progress_for_test(TcpEstablishedAckObservation {
                lookup_id,
                connection_id,
                accepted_acknowledgment: first.next_send_sequence(),
                advertised_window: 0x4321,
                previous_state: crate::transport::tcp::TcpState::Established,
                ack_state_transition: None,
                acknowledges_local_fin: false,
            })
            .expect("observe payload ACK progress");

        wait_for(Duration::from_secs(1), || {
            service.tcp_retransmit_queue_len_for_flow_for_test(flow) == 0
        });

        let snapshot = service.app_control_snapshot_for_test();
        let connection = snapshot
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .expect("updated connection snapshot");
        assert_eq!(connection.snd_una, first.next_send_sequence());
        assert_eq!(connection.snd_wnd, 0x4321);

        std::thread::sleep(TCP_OUTPUT_RETRANSMIT_TIMEOUT + Duration::from_millis(40));
        let emitted_after_ack = output
            .emitted
            .lock()
            .expect("tcp output capture poisoned")
            .len();
        assert_eq!(
            emitted_after_ack, emitted_before_ack,
            "ACK progress must clear payload retransmit bookkeeping so no further retransmit fires"
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_send_respects_send_window_and_ack_progress_restarts_output() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.190:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.146:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        let lookup_id = service
            .app_control_snapshot_for_test()
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .map(|connection| connection.lookup_id)
            .expect("lookup id for connected flow");

        service
            .handle_tcp_established_ack_progress_for_test(TcpEstablishedAckObservation {
                lookup_id,
                connection_id,
                accepted_acknowledgment: 0x1020_3041,
                advertised_window: 0,
                previous_state: crate::transport::tcp::TcpState::Established,
                ack_state_transition: None,
                acknowledges_local_fin: false,
            })
            .expect("close send window");

        request_tcp_send(&app, flow, b"windowed-send");
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .is_empty(),
            "zero send window must keep payload queued"
        );

        service
            .handle_tcp_established_ack_progress_for_test(TcpEstablishedAckObservation {
                lookup_id,
                connection_id,
                accepted_acknowledgment: 0x1020_3041,
                advertised_window: 4,
                previous_state: crate::transport::tcp::TcpState::Established,
                ack_state_transition: None,
                acknowledges_local_fin: false,
            })
            .expect("re-open send window");

        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .len()
                >= 1
        });

        let first = output
            .emitted
            .lock()
            .expect("tcp output capture poisoned")
            .first()
            .cloned()
            .expect("first window-limited segment");
        assert_eq!(first.payload, b"wind".to_vec());

        service
            .handle_tcp_established_ack_progress_for_test(TcpEstablishedAckObservation {
                lookup_id,
                connection_id,
                accepted_acknowledgment: first.next_send_sequence(),
                advertised_window: 4,
                previous_state: crate::transport::tcp::TcpState::Established,
                ack_state_transition: None,
                acknowledges_local_fin: false,
            })
            .expect("advance send window with ACK");

        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .len()
                >= 2
        });

        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        assert_eq!(emitted[1].payload, b"owed".to_vec());

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_remote_fin_observation_transitions_connection_and_records_read_shutdown() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.189:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
        let local: SocketAddr = "192.0.2.145:49152".parse().expect("local");

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::StateChanged {
                connection_id,
                key: TcpConnectionKey::v4(
                    0,
                    Ipv4Addr::new(192, 0, 2, 145),
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 189),
                    443,
                ),
                state: crate::transport::tcp::TcpState::Established,
            })
            .expect("bind established connection");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        let lookup_id = service
            .app_control_snapshot_for_test()
            .tcp_connections
            .lookup_by_connection_id(connection_id)
            .map(|connection| connection.lookup_id)
            .expect("lookup id for connected flow");

        service
            .handle_tcp_established_close_for_test(TcpEstablishedObservation {
                lookup_id,
                connection_id,
                local,
                remote: peer,
                reason: TcpCloseReason::RemoteFin,
            })
            .expect("observe remote FIN");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::CloseWait)
        });
        assert_eq!(
            service.tcp_shutdown_for_flow_for_test(flow),
            Some((TcpShutdownDirection::Read, TcpCloseReason::RemoteFin))
        );
        assert_eq!(
            service
                .app_control_snapshot_for_test()
                .tcp_connections
                .lookup_by_connection_id(connection_id)
                .map(|connection| connection.state),
            Some(crate::transport::tcp::TcpState::CloseWait)
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_app_shutdown_transitions_close_wait_connection_to_last_ack() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

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
                state: crate::transport::tcp::TcpState::CloseWait,
            })
            .expect("bind close-wait connection");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::CloseWait)
        });

        request_tcp_shutdown(&app, flow, Shutdown::Write);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::LastAck)
        });
        assert_eq!(
            service.shared_tcp_connection_state_for_test(connection_id),
            Some(crate::transport::tcp::TcpState::LastAck)
        );
        assert_eq!(
            service
                .app_control_snapshot_for_test()
                .tcp_connections
                .lookup_by_connection_id(connection_id)
                .map(|connection| connection.state),
            Some(crate::transport::tcp::TcpState::LastAck)
        );
        assert_eq!(
            service.tcp_shutdown_for_flow_for_test(flow),
            Some((
                hammer_core::protocol::tcp::TcpShutdownDirection::Write,
                TcpCloseReason::LocalShutdown,
            ))
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_app_shutdown_before_state_changed_is_observed_and_allows_late_install() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

        request_tcp_shutdown(&app, flow, Shutdown::Write);

        wait_for(Duration::from_secs(1), || {
            service.tcp_shutdown_for_flow_for_test(flow)
                == Some((
                    hammer_core::protocol::tcp::TcpShutdownDirection::Write,
                    TcpCloseReason::LocalShutdown,
                ))
        });
        assert_eq!(
            service.tcp_shutdown_for_flow_for_test(flow),
            Some((
                hammer_core::protocol::tcp::TcpShutdownDirection::Write,
                TcpCloseReason::LocalShutdown,
            ))
        );

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
            .expect("ingest late state change");
        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::FinWait1)
        });
        assert_eq!(
            service.shared_tcp_connection_state_for_test(connection_id),
            Some(crate::transport::tcp::TcpState::FinWait1)
        );
        assert_eq!(
            service.tcp_shutdown_for_flow_for_test(flow),
            Some((
                hammer_core::protocol::tcp::TcpShutdownDirection::Write,
                TcpCloseReason::LocalShutdown,
            ))
        );
        wait_for(Duration::from_secs(1), || {
            output
                .emitted
                .lock()
                .expect("tcp output capture poisoned")
                .iter()
                .any(|segment| {
                    segment.connection_id == connection_id && segment.flags & TCP_FLAG_FIN != 0
                })
        });
        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        let fin = emitted
            .iter()
            .find(|segment| {
                segment.connection_id == connection_id && segment.flags & TCP_FLAG_FIN != 0
            })
            .expect("FIN segment emitted after late install");
        assert_eq!(fin.local, local);
        assert_eq!(fin.remote, peer);
        assert!(fin.payload.is_empty());
        assert_eq!(
            service
                .app_control_snapshot_for_test()
                .tcp_connections
                .lookup_by_connection_id(connection_id)
                .map(|connection| connection.state),
            Some(crate::transport::tcp::TcpState::FinWait1)
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_late_shutdown_flushes_pending_payload_before_fin() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let local: SocketAddr = "192.0.2.44:49152".parse().expect("local");
        let output = Arc::new(RecordingTcpOutputBackend::default());
        service.install_tcp_output_backend_for_test(Arc::clone(&output));

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

        request_tcp_send(&app, flow, b"queued-before-establish");
        request_tcp_shutdown(&app, flow, Shutdown::Write);

        wait_for(Duration::from_secs(1), || {
            service.tcp_pending_send_payloads_for_flow_for_test(flow)
                == vec![b"queued-before-establish".to_vec()]
        });

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
            .expect("ingest late state change");
        observe_active_syn_ack_for_test(&service, connection_id, peer, local);

        wait_for(Duration::from_secs(1), || {
            let emitted = output.emitted.lock().expect("tcp output capture poisoned");
            emitted.iter().any(|segment| {
                segment.connection_id == connection_id && !segment.payload.is_empty()
            }) && emitted.iter().any(|segment| {
                segment.connection_id == connection_id && segment.flags & TCP_FLAG_FIN != 0
            })
        });
        let emitted = output.emitted.lock().expect("tcp output capture poisoned");
        let payload_index = emitted
            .iter()
            .position(|segment| {
                segment.connection_id == connection_id && !segment.payload.is_empty()
            })
            .expect("payload segment emitted");
        let fin_index = emitted
            .iter()
            .position(|segment| {
                segment.connection_id == connection_id && segment.flags & TCP_FLAG_FIN != 0
            })
            .expect("FIN segment emitted");
        assert!(
            payload_index <= fin_index,
            "queued payload must be transmitted before or together with the local FIN"
        );
        assert_eq!(
            emitted[payload_index].payload,
            b"queued-before-establish".to_vec()
        );
        if payload_index == fin_index {
            assert!(
                emitted[fin_index].flags & TCP_FLAG_FIN != 0,
                "coalesced payload segment must carry FIN"
            );
        } else {
            assert!(emitted[fin_index].payload.is_empty());
        }
        assert_eq!(
            service.tcp_pending_send_payloads_for_flow_for_test(flow),
            Vec::<Vec<u8>>::new()
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_app_close_then_closed_event_is_idempotent() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
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
            .expect("bind established connection");

        app.close_tcp_flow(flow).expect("close established flow");
        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::Closed {
                connection_id,
                reason: TcpCloseReason::LocalRequest,
            })
            .expect("duplicate close remains idempotent");

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
    fn runtime_service_closed_worker_event_posts_app_closed_cqe() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (tx, rx) = std::sync::mpsc::channel();

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");

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
            .expect("bind established connection");

        service
            .spawn_app_on_worker(1, {
                let app = app.clone();
                move || async move {
                    let backend = app.local_backend_for_flow(flow).expect("flow backend");
                    ready_tx.send(()).expect("signal closed cqe ready");
                    let cqe = backend
                        .next_cqe_descriptor()
                        .await
                        .expect("closed cqe descriptor");
                    tx.send(cqe).expect("send closed cqe");
                }
            })
            .expect("spawn closed cqe worker");

        ready_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("wait closed cqe ready");

        service
            .handle_tcp_worker_event_for_test(TcpWorkerEvent::Closed {
                connection_id,
                reason: TcpCloseReason::LocalRequest,
            })
            .expect("ingest close event");

        let closed_cqe = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive closed cqe");
        match closed_cqe.payload() {
            AppCqeData::Closed {
                flow: Some(cqe_flow),
                socket: None,
            } => {
                assert_eq!(cqe_flow, flow);
            }
            other => panic!("unexpected closed cqe payload: {other:?}"),
        }

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_terminal_timer_expiry_is_projected_without_panicking() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

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
            .expect("bind established connection");

        wait_for(Duration::from_secs(1), || {
            service.shared_tcp_connection_state_for_test(connection_id)
                == Some(crate::transport::tcp::TcpState::Established)
        });

        service
            .apply_shared_tcp_action_for_test(TcpControlPlaneAction::ArmTimer {
                connection_id,
                timer_id: TcpTimerId::new(16),
                kind: TcpTimerKind::KeepAlive,
                timeout: Duration::from_millis(20),
            })
            .expect("arm keepalive timer");

        wait_for(Duration::from_secs(1), || {
            let state = service.app_control_snapshot_for_test();
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none()
                && state
                    .tcp_lookup
                    .lookup_connection_v4(TcpV4ConnectionKey::new(
                        0,
                        Ipv4Addr::new(192, 0, 2, 44),
                        49_152,
                        Ipv4Addr::new(198, 51, 100, 88),
                        443,
                    ))
                    .is_none()
        });

        assert!(
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none(),
            "terminal timer expiry must close the shared connection"
        );
        assert!(
            service
                .app_control_snapshot_for_test()
                .tcp_lookup
                .lookup_connection_v4(TcpV4ConnectionKey::new(
                    0,
                    Ipv4Addr::new(192, 0, 2, 44),
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_none(),
            "terminal timer expiry must revoke projected established lookup"
        );

        service.close().expect("close service");
    }

    #[test]
    fn runtime_service_connect_timer_expiry_reclaims_pending_active_connect() {
        let service = new_test_service("");
        let app = service.app_context();
        let peer: SocketAddr = "198.51.100.88:443".parse().expect("tcp peer");

        let flow = app.connect_tcp_stream(peer, 1).expect("connect tcp stream");
        let connection_id = service
            .tcp_connection_id_for_flow_for_test(flow)
            .expect("active connect connection id");
        assert_eq!(app.owner_worker_for_flow(flow).expect("flow owner"), 1);

        service
            .apply_shared_tcp_action_for_test(TcpControlPlaneAction::ArmTimer {
                connection_id,
                timer_id: TcpTimerId::new(77),
                kind: TcpTimerKind::Connect,
                timeout: Duration::from_millis(20),
            })
            .expect("re-arm connect timer");

        wait_for(Duration::from_secs(1), || {
            let state = service.app_control_snapshot_for_test();
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none()
                && state
                    .tcp_lookup
                    .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                        0,
                        49_152,
                        Ipv4Addr::new(198, 51, 100, 88),
                        443,
                    ))
                    .is_none()
        });

        assert!(
            service
                .shared_tcp_connection_state_for_test(connection_id)
                .is_none(),
            "connect timeout must remove the shared syn-sent connection"
        );
        assert!(
            service
                .app_control_snapshot_for_test()
                .tcp_lookup
                .lookup_pending_v4(TcpV4PendingConnectionKey::new(
                    0,
                    49_152,
                    Ipv4Addr::new(198, 51, 100, 88),
                    443,
                ))
                .is_none(),
            "connect timeout must revoke the pending syn-sent lookup"
        );

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

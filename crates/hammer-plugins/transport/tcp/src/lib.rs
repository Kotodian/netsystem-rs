//! Dynamic `tcp` plugin (`libhammer_plugin_tcp`).

hammer_component_macros::declare_plugin!(
    name = "tcp",
    load_after = ["ip", "session"],
    init_functions = [__INIT_FN_TCP_INIT],
    config_functions = [__CONFIG_FN_TCP_CONFIG],
    main_loop_enter_functions = [],
    main_loop_exit_functions = [],
    worker_init_functions = [__INIT_FN_TCP_WORKER_INIT],
    graph_nodes = [
        input::__TCP_WORKER_GRAPH_NODE_TCP_INPUT_NODE,
        output::__TCP_WORKER_GRAPH_NODE_TCP_OUTPUT_NODE,
        established::__TCP_WORKER_GRAPH_NODE_TCP_ESTABLISHED_NODE,
        reset::__SERVICE_GRAPH_NODE_TCP_RESET_NODE,
        listen::__TCP_WORKER_GRAPH_NODE_TCP_LISTEN_NODE,
        rcv_process::__TCP_WORKER_GRAPH_NODE_TCP_RCV_PROCESS_NODE,
        syn_sent::__TCP_WORKER_GRAPH_NODE_TCP_SYN_SENT_NODE,
    ],
    node_functions = [
        output::__NODE_FUNCTION_TCP_OUTPUT_NODE_PROCESS_SIMD_SCALAR,
        output::__NODE_FUNCTION_TCP_OUTPUT_NODE_PROCESS_SIMD_SIMD128,
        output::__NODE_FUNCTION_TCP_OUTPUT_NODE_PROCESS_SIMD_SIMD256,
        output::__NODE_FUNCTION_TCP_OUTPUT_NODE_PROCESS_SIMD_SIMD512,
    ],
    process_nodes = [],
);

use std::cell::{RefCell, RefMut, UnsafeCell};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::OnceLock;

use hammer_core::data_plane::{BufferPacketCursor, NodeId, NodeState};
use hammer_runtime::app::SessionHandle;
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, Node, NodeRuntime, RuntimeError, RuntimeResult,
    SessionConnectEndpoint, SessionListenEndpoint,
};
use thiserror::Error;

use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::pool::Pool;
use hammer_plugin_session::{
    IpSessionEndpoint, IpSessionMain, IpTransportEndpoint, IpTransportEndpointConfig,
    IpTransportMain,
};
use hammer_service::session::node::{SessionQueueNode, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionTransport, SessionWorker, dispatch_session_queue_events,
};
use hammer_service::session::{
    SessionError, SessionHandle as ServiceSessionHandle, SessionQueueNext,
};
use hammer_service::transport::{
    Transport, TransportMain, TransportOptions, TransportSendParams, TransportServiceType,
    TransportTxMode, TransportVft, register_transport,
};

pub mod config;
pub mod congestion;
pub mod connection;
pub mod established;
pub mod input;
pub mod listen;
mod listener_control;
pub mod lookup;
pub mod output;
pub mod policy;
pub mod protocol;
pub mod rcv_process;
pub mod recovery;
pub mod reset;
mod sack;
pub mod segment;
pub mod syn_sent;
mod timers;
pub(crate) mod worker;

pub use protocol::*;

pub use connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TCP_MAX_RETRANSMIT_TIMEOUT, TCP_MIN_RETRANSMIT_TIMEOUT,
    TcpConnection, TcpRetransmitTimeoutState,
};
pub use established::{TcpEstablishedNext, TcpEstablishedNode};
pub use input::{TcpInputControlPlane, TcpInputNode, TcpInputTrace};
pub use listen::{TcpListenNext, TcpListenNode};
pub use output::{DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TcpOutputNext, TcpOutputNode};
pub use policy::{TcpPolicy, active_tcp_policy, publish_tcp_policy, tcp_policy};
pub use rcv_process::{TcpRcvProcessNext, TcpRcvProcessNode};
pub use recovery::{TcpRecoveryAck, TcpRecoveryState};
pub use reset::{TcpResetNext, TcpResetNode};
use segment::TcpSegment;
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};

pub use worker::TcpWorker;

#[hammer_component_macros::runtime_error(subsystem = "tcp")]
#[derive(Debug, Error)]
enum TcpWorkerError {
    #[error("required graph node `{name}` is not registered")]
    NodeMissing { name: &'static str },
    #[error("runtime thread {thread_index} is not a data worker")]
    WorkerUnavailable { thread_index: u32 },
    #[error("TCP worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("Session transport registration")]
    SessionTransportRegistration {
        #[source]
        source: SessionError,
    },
}

pub(crate) fn publish_tcp_connection(
    sessions: &mut SessionWorker,
    tcp: &mut TcpWorker,
    session_id: u32,
) -> RuntimeResult<()> {
    let index = sessions
        .transport_connection_index(session_id)
        .ok_or(TcpNodeError::SessionMissing)?;
    let (close, half_open) = {
        let TcpWorker {
            connections,
            lookup,
            ..
        } = tcp;
        let connection = connections.get(index).ok_or(TcpNodeError::SessionMissing)?;
        (
            lookup.publish_connection(session_id, connection),
            connection.state() == TcpState::SynSent,
        )
    };
    if half_open {
        return Ok(());
    }
    let rollback = |sessions: &mut SessionWorker, tcp: &mut TcpWorker| {
        tcp.lookup.forget_session(session_id);
        tcp.lookup.forget_pending_open(session_id);
        let session_cleanup = sessions.rollback_session_creation(session_id);
        tcp.remove_connection(index);
        match session_cleanup {
            Err(error) => Err(error),
            Ok(Some(rollback_index)) if rollback_index != index => {
                Err(TcpNodeError::SessionMissing.into())
            }
            Ok(_) => Ok(()),
        }
    };
    if close {
        let initial = match sessions.connection_published(session_id) {
            Ok(initial) => initial,
            Err(error) => {
                if let Err(cleanup_error) = rollback(sessions, tcp) {
                    tracing::error!(
                        ?session_id,
                        %cleanup_error,
                        "TCP connection publication rollback failed"
                    );
                }
                return Err(error);
            }
        };
        let close_reason = tcp.connection(index).and_then(TcpConnection::close_reason);
        if initial {
            let error = TcpError::ConnectionClosed.into();
            if let Err(cleanup_error) = rollback(sessions, tcp) {
                tracing::error!(
                    ?session_id,
                    %cleanup_error,
                    "closed TCP connection publication rollback failed"
                );
            }
            return Err(error);
        }
        if close_reason == Some(TcpCloseReason::RemoteReset) {
            sessions.notify_transport_reset(session_id, index)?;
        } else {
            sessions.notify_transport_closed(session_id, index)?;
        }
        tcp.remove_connection(index);
        sessions.notify_transport_deleted(session_id, index)?;
    } else if let Err(error) = sessions.complete_stream_connect(session_id) {
        if let Err(cleanup_error) = rollback(sessions, tcp) {
            tracing::error!(
                ?session_id,
                %cleanup_error,
                "App publication rollback failed"
            );
        }
        return Err(error);
    }
    Ok(())
}

#[repr(C)]
struct TcpWorkerSlot {
    cacheline0: CacheLineAlignMark,
    worker: RefCell<TcpWorker>,
}

impl TcpWorkerSlot {
    fn new(worker: TcpWorker) -> Self {
        Self {
            cacheline0: CacheLineAlignMark,
            worker: RefCell::new(worker),
        }
    }
}

pub struct TcpMain {
    protocol: u8,
    control: TcpInputControlPlane,
    listener_control: listener_control::TcpListenerControlHandle,
    listeners: UnsafeCell<Pool<TcpConnection>>,
    workers: Box<[TcpWorkerSlot]>,
}

// SAFETY: every RefCell slot is permanently assigned to one Data Worker.
// Main Thread never borrows worker slots after startup; each worker selects
// only its own immutable runtime thread index.
unsafe impl Sync for TcpMain {}

impl TcpMain {
    fn new(protocol: u8, worker_count: usize) -> Self {
        let control = TcpInputControlPlane::new();
        let listener_control = listener_control::TcpListenerControlHandle::new(control.clone());
        let workers = (0..worker_count)
            .map(|worker| {
                TcpWorkerSlot::new(TcpWorker::new(DataWorkerId::new(worker as u32), protocol))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            protocol,
            control,
            listener_control,
            listeners: UnsafeCell::new(Pool::new()),
            workers,
        }
    }

    fn worker(&self, thread_index: u32) -> RuntimeResult<RefMut<'_, TcpWorker>> {
        let worker = DataWorkerId::try_from(thread_index)
            .map_err(|_| TcpWorkerError::WorkerUnavailable { thread_index })?;
        self.workers
            .get(worker.slot())
            .ok_or_else(|| TcpWorkerError::WorkerOutOfRange {
                worker: worker.slot(),
            })
            .map(|slot| slot.worker.borrow_mut())
            .map_err(RuntimeError::from)
    }

    pub fn control(&self) -> &TcpInputControlPlane {
        &self.control
    }

    pub const fn protocol(&self) -> u8 {
        self.protocol
    }

    fn bind_tcp_listener(
        &self,
        bind: SocketAddr,
        owner_worker: DataWorkerId,
        capabilities: TcpCapabilities,
        session_listener: SessionHandle,
    ) -> RuntimeResult<lookup::TcpLookupId> {
        let lookup_id =
            self.listener_control
                .bind(bind, owner_worker, capabilities, session_listener)?;
        let remote = SocketAddr::new(
            match bind.ip() {
                IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            },
            0,
        );
        let mut connection = TcpConnection::new(
            Some(TcpConnectionId::new(u64::from(lookup_id))),
            owner_worker,
            self.protocol,
            bind.port(),
            Some(bind),
            remote,
        );
        connection.listen_state();
        connection.base.session = session_listener.into();
        connection.base.connection_index = lookup_id;
        unsafe { &mut *self.listeners.get() }.insert(connection);
        Ok(lookup_id)
    }

    #[inline]
    fn listener_connection(&self, connection_index: u32) -> Option<&TcpConnection> {
        unsafe { &*self.listeners.get() }
            .iter()
            .find_map(|(_, connection)| {
                (connection.connection_id()
                    == Some(TcpConnectionId::new(u64::from(connection_index))))
                .then_some(connection)
            })
    }

    fn remove_listener_connection(&self, connection_index: u32) {
        let listeners = unsafe { &mut *self.listeners.get() };
        let index = listeners.iter().find_map(|(index, connection)| {
            (connection.connection_id() == Some(TcpConnectionId::new(u64::from(connection_index))))
                .then_some(index)
        });
        if let Some(index) = index {
            listeners.remove(index);
        }
    }

    pub fn publish_connection(
        &self,
        sessions: &IpSessionMain,
        worker_index: u32,
        connection_index: u32,
    ) -> Result<(), SessionError> {
        let Some(connection) = self.connection(connection_index, worker_index) else {
            return Err(SessionError::NoSession);
        };
        sessions.publish(connection.base.endpoint, connection.base.session)
    }
}

pub struct TcpTransportAttribute;

impl Transport<IpTransportEndpointConfig> for TcpMain {
    type Connection = TcpConnection;
    type Attribute = TcpTransportAttribute;

    fn options(&self) -> TransportOptions {
        TransportOptions::new(TransportTxMode::Peek, TransportServiceType::VirtualCircuit)
    }

    fn connect(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: ServiceSessionHandle,
    ) -> Result<u32, SessionError> {
        let local = SocketAddr::new(endpoint.local.address, endpoint.local.port);
        let remote = SocketAddr::new(endpoint.peer.address, endpoint.peer.port);
        if local.is_ipv4() != remote.is_ipv4() || local.port() == 0 || remote.port() == 0 {
            return Err(SessionError::Invalid);
        }
        let Some(slot) = self.workers.get(session.worker_index as usize) else {
            return Err(SessionError::Invalid);
        };
        let worker = unsafe { &mut *slot.worker.as_ptr() };
        let initial_sequence = worker.lookup.next_initial_sequence(local, remote);
        let mut connection = TcpConnection::new(
            None,
            DataWorkerId::new(session.worker_index),
            self.protocol,
            local.port(),
            Some(local),
            remote,
        );
        connection.connect_state(initial_sequence);
        let connection_index = worker.insert_connection(connection);
        let Some(connection) = worker.connection_mut(connection_index) else {
            return Err(SessionError::NoSession);
        };
        if connection.attach_session(session.session_index).is_err() {
            worker.remove_connection(connection_index);
            return Err(SessionError::Invalid);
        }
        let published = {
            let TcpWorker {
                connections,
                lookup,
                ..
            } = worker;
            let Some(connection) = connections.get(connection_index) else {
                return Err(SessionError::NoSession);
            };
            lookup.publish_connection(session.session_index, connection)
        };
        if published {
            worker.remove_connection(connection_index);
            return Err(SessionError::PortInUse);
        }
        Ok(connection_index)
    }

    fn connect_stream(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: ServiceSessionHandle,
    ) -> Result<u32, SessionError> {
        drop((endpoint, session));
        Err(SessionError::NotSupported)
    }

    fn start_listen(
        &self,
        endpoint: &IpTransportEndpointConfig,
        session: ServiceSessionHandle,
    ) -> Result<u32, SessionError> {
        if hammer_runtime::ensure_main_thread_with_barrier().is_err() {
            return Err(SessionError::Invalid);
        }
        let session_endpoint = IpSessionEndpoint::new(*endpoint, self.protocol);
        let transport = IpTransportMain::global()?;
        transport.mark_used(&session_endpoint)?;
        let local = endpoint.local;
        let bind = std::net::SocketAddr::new(local.address, local.port);
        let result = self.bind_tcp_listener(
            bind,
            DataWorkerId::new(session.worker_index),
            listener_capabilities(),
            session.into(),
        );
        match result {
            Ok(index) => Ok(index),
            Err(error) => {
                debug_assert!(transport.release(&session_endpoint).is_ok());
                Err(SessionError::TransportOpFailed {
                    source: RuntimeError::from(error),
                })
            }
        }
    }

    fn stop_listen(&self, connection_index: u32) -> Result<u32, SessionError> {
        if hammer_runtime::ensure_main_thread_with_barrier().is_err() {
            return Err(SessionError::Invalid);
        }
        let endpoint = self
            .listener_connection(connection_index)
            .and_then(|connection| {
                connection.local().map(|local| {
                    IpSessionEndpoint::new(
                        tcp_endpoint_pair(local, connection.remote()).0,
                        self.protocol,
                    )
                })
            });
        match self
            .listener_control
            .close_connection_index(connection_index)
        {
            Ok(()) => {
                self.remove_listener_connection(connection_index);
                if let Some(endpoint) = endpoint {
                    IpTransportMain::global()?.release(&endpoint)?;
                }
                Ok(connection_index)
            }
            Err(error) => Err(SessionError::TransportOpFailed {
                source: RuntimeError::from(error),
            }),
        }
    }

    fn half_close(&self, connection_index: u32, worker_index: u32) {
        self.close(connection_index, worker_index);
    }

    fn close(&self, connection_index: u32, worker_index: u32) {
        if let Some(slot) = self.workers.get(worker_index as usize) {
            let worker = unsafe { &mut *slot.worker.as_ptr() };
            let TcpWorker {
                connections,
                timers,
                ..
            } = worker;
            if let Some(connection) = connections.get_mut(connection_index) {
                connection.on_session_close(connection_index, timers);
                return;
            }
        }
        if self
            .listener_control
            .close_connection_index(connection_index)
            .is_ok()
        {
            self.remove_listener_connection(connection_index);
        }
    }

    fn reset(&self, connection_index: u32, worker_index: u32) {
        self.close(connection_index, worker_index);
    }

    fn cleanup(&self, connection_index: u32, worker_index: u32) {
        if let Some(slot) = self.workers.get(worker_index as usize) {
            let worker = unsafe { &mut *slot.worker.as_ptr() };
            if let Some(session_index) = worker
                .connection(connection_index)
                .map(|connection| connection.base.session.session_index)
            {
                worker.lookup.forget_session(session_index);
                worker.lookup.forget_pending_open(session_index);
                drop(worker.remove_connection(connection_index));
                return;
            }
        }
        if self
            .listener_control
            .close_connection_index(connection_index)
            .is_ok()
        {
            self.remove_listener_connection(connection_index);
        }
    }

    fn cleanup_half_open(&self, connection_index: u32) {
        for slot in &self.workers {
            let worker = unsafe { &mut *slot.worker.as_ptr() };
            if worker
                .connection(connection_index)
                .is_some_and(|connection| connection.state() == TcpState::SynSent)
            {
                let session_index = worker
                    .connection(connection_index)
                    .expect("checked TCP half-open remains live")
                    .base
                    .session
                    .session_index;
                worker.lookup.forget_pending_open(session_index);
                drop(worker.remove_connection(connection_index));
                return;
            }
        }
    }

    fn push_header(&self, _: u32, _: u32, _: &mut [u32], _: u32) -> u32 {
        0
    }

    fn send_params(&self, _: u32, _: u32) -> TransportSendParams {
        TransportSendParams {
            send_mss: u16::try_from(active_tcp_policy().mss).unwrap_or(u16::MAX),
            ..TransportSendParams::default()
        }
    }

    fn update_time(&self, _: f64, _: u32) {}

    fn flush_data(&self, _: u32, _: u32) {}

    fn custom_tx(&self, _: ServiceSessionHandle, _: &mut TransportSendParams) -> usize {
        0
    }

    fn app_rx_event(&self, _: u32, _: u32) -> Result<(), SessionError> {
        Err(SessionError::NotSupported)
    }

    #[inline]
    fn connection(&self, connection_index: u32, worker_index: u32) -> Option<&Self::Connection> {
        let slot = self.workers.get(worker_index as usize)?;
        unsafe { (&*slot.worker.as_ptr()).connection(connection_index) }
    }

    #[inline]
    fn listener(&self, connection_index: u32) -> Option<&Self::Connection> {
        self.listener_connection(connection_index)
    }

    #[inline]
    fn half_open(&self, connection_index: u32) -> Option<&Self::Connection> {
        self.workers.iter().find_map(|slot| {
            let connection = unsafe { (&*slot.worker.as_ptr()).connection(connection_index) }?;
            (connection.state() == TcpState::SynSent).then_some(connection)
        })
    }

    fn endpoint(
        &self,
        connection_index: u32,
        worker_index: u32,
    ) -> (IpTransportEndpointConfig, IpTransportEndpointConfig) {
        let connection = <Self as Transport<IpTransportEndpointConfig>>::connection(
            self,
            connection_index,
            worker_index,
        )
        .expect("TCP endpoint retrieval requires a live connection");
        let local = connection
            .local()
            .expect("TCP endpoint retrieval requires a local address");
        tcp_endpoint_pair(local, connection.remote())
    }

    fn listener_endpoint(
        &self,
        connection_index: u32,
    ) -> (IpTransportEndpointConfig, IpTransportEndpointConfig) {
        let connection = self
            .listener_connection(connection_index)
            .expect("TCP listener endpoint retrieval requires a live listener");
        let local = connection
            .local()
            .expect("TCP listener endpoint retrieval requires a local address");
        tcp_endpoint_pair(local, connection.remote())
    }

    fn attribute(&self, _: u32, _: u32, _: &mut Self::Attribute) -> Result<(), SessionError> {
        Err(SessionError::NotSupported)
    }
}

fn tcp_endpoint_pair(
    local: SocketAddr,
    remote: SocketAddr,
) -> (IpTransportEndpointConfig, IpTransportEndpointConfig) {
    let local = IpTransportEndpoint {
        address: local.ip(),
        port: local.port(),
        sw_if_index: u32::MAX,
        fib_index: 0,
    };
    let remote = IpTransportEndpoint {
        address: remote.ip(),
        port: remote.port(),
        sw_if_index: u32::MAX,
        fib_index: 0,
    };
    let forward = IpTransportEndpointConfig {
        local,
        peer: remote,
        next_node_index: u32::MAX,
        next_node_opaque: 0,
        mss: u16::try_from(active_tcp_policy().mss).unwrap_or(u16::MAX),
        dscp: 0,
        transport_flags: 0,
    };
    let reverse = IpTransportEndpointConfig {
        local: remote,
        peer: local,
        ..forward
    };
    (forward, reverse)
}

// VPP alignment: `tcp_main_t tcp_main;` is a file-scope global in VPP's
// `tcp.c`; nodes read it directly and `tcp_init` publishes the configured
// instance before workers start.
pub static TCP_MAIN: OnceLock<TcpMain> = OnceLock::new();
static TCP_CONFIG: OnceLock<crate::config::TcpPluginConfig> = OnceLock::new();

pub fn protocol() -> RuntimeResult<u8> {
    TCP_MAIN
        .get()
        .map(TcpMain::protocol)
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })
}

pub(crate) fn start_listen(
    listener: SessionHandle,
    _: u32,
    _: Option<u64>,
    endpoint: SessionListenEndpoint,
) -> RuntimeResult<u32> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    main.bind_tcp_listener(
        endpoint.local(),
        endpoint.worker(),
        listener_capabilities(),
        listener,
    )
    .map(|lookup_id| lookup_id)
}

pub(crate) fn stop_listen(connection_index: u32) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    let main = TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let result = main
        .listener_control
        .close_connection_index(connection_index);
    if result.is_ok() {
        main.remove_listener_connection(connection_index);
    }
    result
}

pub(crate) fn connect(
    sessions: &mut SessionWorker,
    endpoint: SessionConnectEndpoint,
) -> RuntimeResult<()> {
    let local = endpoint.local.ok_or(TcpError::InvalidConnection)?;
    if local.is_ipv4() != endpoint.remote.is_ipv4() || local.port() == 0 {
        return Err(TcpError::InvalidConnection.into());
    }
    let connection = endpoint
        .connection
        .ok_or(hammer_service::session::error::SessionError::ApplicationConnectionMissing)?;
    let mut tcp = TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?
        .worker(endpoint.worker.thread_index())?;
    start_connect(sessions, &mut tcp, connection, local, endpoint.remote)
}

fn start_connect(
    sessions: &mut SessionWorker,
    tcp: &mut TcpWorker,
    connection: u32,
    local: SocketAddr,
    remote: SocketAddr,
) -> RuntimeResult<()> {
    let initial_sequence = tcp.lookup.next_initial_sequence(local, remote);
    let mut transport = TcpConnection::new(
        None,
        sessions.worker(),
        tcp.protocol(),
        local.port(),
        Some(local),
        remote,
    );
    transport.connect_state(initial_sequence);
    let connection_index = tcp.insert_connection(transport);
    let session_id =
        match sessions.stream_connect_pending(tcp.protocol(), connection_index, connection) {
            Ok(session_id) => session_id,
            Err(error) => {
                let _ = tcp.remove_connection(connection_index);
                return Err(error);
            }
        };
    let connection = tcp
        .connection_mut(connection_index)
        .ok_or(TcpNodeError::SessionMissing)?;
    connection.attach_session(session_id)?;
    publish_tcp_connection(sessions, tcp, session_id)?;
    sessions.mark_ready(session_id);
    Ok(())
}

#[hammer_component_macros::config_function(
    name = "tcp_config",
    section = "plugin.tcp",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_tcp(config: crate::config::TcpPluginConfig) -> RuntimeResult<()> {
    config.validate()?;
    assert!(
        TCP_CONFIG.set(config).is_ok(),
        "TCP configuration callback executes once"
    );
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "tcp_init",
    runs_after = ["ip_transport_main_init", "session_init"]
)]
fn init_tcp() -> RuntimeResult<()> {
    assert!(
        TCP_MAIN.get().is_none(),
        "TCP initialization callback executes once"
    );
    let protocol = register_transport(TransportVft::new(
        Some(start_listen),
        Some(stop_listen),
        Some(connect),
        None,
        None,
        None,
        None,
        None,
    ))
    .map_err(RuntimeError::from)?;
    IpSessionMain::global()
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?
        .register_transport_type(protocol, TransportTxMode::Peek, u32::MAX)
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?;
    let config = TCP_CONFIG
        .get()
        .expect("TCP configuration is installed before initialization");
    let main = configured_tcp_main(
        config,
        protocol,
        hammer_runtime::config::worker::worker_count(),
    )?;
    assert!(
        TCP_MAIN.set(main).is_ok(),
        "TCP initialization callback executes once"
    );
    Ok(())
}

fn configured_tcp_main(
    tcp: &crate::config::TcpPluginConfig,
    protocol: u8,
    worker_count: usize,
) -> RuntimeResult<TcpMain> {
    publish_tcp_policy(TcpPolicy::from_plugin_config(tcp));
    Ok(TcpMain::new(protocol, worker_count))
}

fn listener_capabilities() -> TcpCapabilities {
    let policy = active_tcp_policy();
    let mut window_scale = 0u8;
    while window_scale < connection::TCP_MAX_WINDOW_SCALE
        && (policy.receive_window >> window_scale) > u32::from(u16::MAX)
    {
        window_scale += 1;
    }
    TcpCapabilities {
        max_segment_size: Some(u16::try_from(policy.mss).unwrap_or(u16::MAX)),
        window_scale: Some(window_scale),
        sack: true,
        timestamps: true,
        ..TcpCapabilities::default()
    }
}

pub fn register_tcp_input(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let main = TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let node = if let Some(node) = runtime.nodes().node_by_name("tcp-input") {
        node
    } else {
        runtime.nodes().try_register_internal_with_next_names(
            main.control().node(None),
            &TcpInputNext::NEXT_NAMES,
        )?
    };
    hammer_plugin_ip::register_ip4_protocol(runtime.nodes(), 6, node)?;
    hammer_plugin_ip::register_ip6_protocol(runtime.nodes(), 6, node)?;
    Ok(node)
}

fn bind_worker_graph(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let session_queue =
        engine
            .node_by_name("session-queue")
            .ok_or(TcpWorkerError::NodeMissing {
                name: "session-queue",
            })?;
    let tcp_output =
        engine
            .node_by_name(TcpOutputNode::NODE_NAME)
            .ok_or(TcpWorkerError::NodeMissing {
                name: TcpOutputNode::NODE_NAME,
            })?;
    let tcp_input = engine
        .node_by_name("tcp-input")
        .ok_or_else(|| TcpWorkerError::NodeMissing { name: "tcp-input" })?;
    let tcp_listen = engine
        .node_by_name("tcp-listen")
        .ok_or_else(|| TcpWorkerError::NodeMissing { name: "tcp-listen" })?;
    let tcp_established =
        engine
            .node_by_name("tcp-established")
            .ok_or(TcpWorkerError::NodeMissing {
                name: "tcp-established",
            })?;
    let tcp_rcv_process =
        engine
            .node_by_name("tcp-rcv-process")
            .ok_or(TcpWorkerError::NodeMissing {
                name: "tcp-rcv-process",
            })?;
    let tcp_syn_sent =
        engine
            .node_by_name("tcp-syn-sent")
            .ok_or_else(|| TcpWorkerError::NodeMissing {
                name: "tcp-syn-sent",
            })?;

    let session_queue_data = engine.nodes().node_runtime_data(session_queue)?;
    let session_queue_output =
        SessionQueueNode::existing_output_next(engine, session_queue, tcp_output)?;
    SessionQueueNode::install_worker_attachment(
        engine,
        session_queue_data,
        session_queue_output,
        tcp_session_queue_update_time,
        tcp_session_queue_dispatch,
    )?;
    let main = TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let input_data = main.control().node(Some(worker)).node_runtime_data()?;
    let listen_data = TcpListenNode::new().node_runtime_data()?;
    let established_data = TcpEstablishedNode::new().node_runtime_data()?;
    let rcv_process_data = TcpRcvProcessNode::new().node_runtime_data()?;
    let syn_sent_data = TcpSynSentNode::new().node_runtime_data()?;

    // A worker graph clone can retain the old polling state. Keep the node
    // dormant until its replacement SessionWorker owns a live readiness file.
    engine
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    engine.set_worker_node_runtime_data(tcp_input, input_data)?;
    engine.set_worker_node_runtime_data(tcp_listen, listen_data)?;
    engine.set_worker_node_runtime_data(tcp_established, established_data)?;
    engine.set_worker_node_runtime_data(tcp_rcv_process, rcv_process_data)?;
    engine.set_worker_node_runtime_data(tcp_syn_sent, syn_sent_data)?;

    Ok(())
}

#[hammer_component_macros::worker_init_function(
    name = "tcp_worker_init",
    runs_after = ["session_worker_init"]
)]
fn init_tcp_worker(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    bind_worker_graph(engine)
}

fn tcp_session_queue_update_time(
    runtime: &mut DataPlaneMain,
    sessions: &mut SessionWorker,
    _: NodeRuntime,
    output_next: SessionQueueNext,
    now: std::time::Instant,
    frame: &mut hammer_core::data_plane::Frame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    let mut tcp = TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?
        .worker(runtime.thread_index())?;
    tcp.update_time(sessions, runtime, output_next, frame, output, now)
}

fn tcp_session_queue_dispatch(
    runtime: &mut DataPlaneMain,
    sessions: &mut SessionWorker,
    _: NodeRuntime,
    output_next: SessionQueueNext,
    now: std::time::Instant,
    frame: &mut hammer_core::data_plane::Frame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    let mut tcp = TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?
        .worker(runtime.thread_index())?;
    dispatch_session_queue_events(
        runtime,
        sessions,
        &mut *tcp,
        output_next,
        frame,
        output,
        now,
    )
    .map(|_| ())
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TcpNodeError {
    #[error("invalid connection")]
    SessionMissing,
    #[error("invalid connection")]
    EstablishedSessionMissing,
    #[error("invalid connection")]
    EstablishedSessionRouteMissing,
    #[error("invalid connection")]
    RcvProcessSessionMissing,
    #[error("invalid connection")]
    RcvProcessSessionRouteMissing,
    #[error("invalid connection")]
    SynSentSessionMissing,
    #[error("invalid connection")]
    SynSentSessionRouteMissing,
    #[error("dispatch error")]
    TimerUpdateFailed,
    #[error("dispatch error")]
    TxOffsetOverflow,
    #[error("bad TCP checksum")]
    BadChecksum,
    #[error("RST received")]
    ResetReceived,
    #[error("bad segment")]
    BadSegment,
    #[error("no listener")]
    NoListener,
    #[error("connection create failed")]
    ConnectionCreate,
    #[error("RACK retransmit")]
    RackRetransmit,
    #[error("TLP probe")]
    TlpProbe,
    #[error("RTO retransmit")]
    Retransmit,
    #[error("pacing limited")]
    PacingLimited,
    #[error("persist probe")]
    PersistProbe,
    #[error("BBR congestion")]
    BbrCongestion,
    #[error("bad window")]
    BadWindow,
    #[error("keepalive probe")]
    KeepaliveProbe,
}

impl hammer_runtime::node::NodeErrorCode for TcpNodeError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl TcpNodeError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

impl From<TcpNodeError> for TcpError {
    #[inline]
    fn from(error: TcpNodeError) -> Self {
        match error {
            TcpNodeError::SessionMissing
            | TcpNodeError::EstablishedSessionMissing
            | TcpNodeError::EstablishedSessionRouteMissing
            | TcpNodeError::RcvProcessSessionMissing
            | TcpNodeError::RcvProcessSessionRouteMissing
            | TcpNodeError::SynSentSessionMissing
            | TcpNodeError::SynSentSessionRouteMissing
            | TcpNodeError::ConnectionCreate => TcpError::InvalidConnection,
            TcpNodeError::TimerUpdateFailed
            | TcpNodeError::TxOffsetOverflow
            | TcpNodeError::RackRetransmit
            | TcpNodeError::TlpProbe
            | TcpNodeError::Retransmit
            | TcpNodeError::PacingLimited
            | TcpNodeError::PersistProbe
            | TcpNodeError::BbrCongestion
            | TcpNodeError::KeepaliveProbe => TcpError::Dispatch,
            TcpNodeError::BadChecksum | TcpNodeError::BadSegment => TcpError::SegmentInvalid,
            TcpNodeError::ResetReceived => TcpError::ConnectionClosed,
            TcpNodeError::NoListener => TcpError::NoListener,
            TcpNodeError::BadWindow => TcpError::RcvWnd,
        }
    }
}

impl From<TcpNodeError> for RuntimeError {
    #[inline]
    fn from(error: TcpNodeError) -> Self {
        TcpError::from(error).into()
    }
}

#[hammer_component_macros::runtime_error(subsystem = "tcp")]
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TcpOutputError {
    #[error("not a TCP header")]
    NoTcpHeader,
    #[error("missing TCP egress endpoints")]
    MissingEgressEndpoints,
    #[error("unsupported TCP egress address family")]
    UnsupportedEgress,
    #[error("TCP segment is too long for its IP packet")]
    SegmentTooLong,
}

impl hammer_runtime::node::NodeErrorCode for TcpOutputError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl TcpOutputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TcpResetError {
    #[error("bad TCP header")]
    BadTcpHeader,
}

impl hammer_runtime::node::NodeErrorCode for TcpResetError {
    #[inline(always)]
    fn local_code(self) -> u16 {
        self as u16
    }
}

impl TcpResetError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[hammer_component_macros::buffer_opaque(secondary)]
#[derive(Clone, Copy)]
#[repr(C)]
struct TcpRouteOpaque {
    session_raw: u64,
    owner_worker: u32,
    next: u8,
    present: u8,
    reserved: [u8; 42],
}

const _: () = assert!(std::mem::size_of::<TcpRouteOpaque>() == 56);

impl Default for TcpRouteOpaque {
    #[inline]
    fn default() -> Self {
        Self {
            session_raw: 0,
            owner_worker: 0,
            next: 0,
            present: 0,
            reserved: [0; 42],
        }
    }
}

/// Stamped on TX buffers so tcp-output can push L3 like VPP `tcp_output_push_ip`
/// (which reads `c_lcl_ip` / `c_rmt_ip` via connection_index).
const TCP_EGRESS_TAG: u32 = 0x5443_5045; // "TCPE"

#[derive(Clone, Copy)]
#[hammer_component_macros::buffer_opaque(secondary)]
#[repr(C)]
struct TcpEgressOpaque {
    tag: u32,
    version: u8,
    pad: [u8; 3],
    local: [u8; 16],
    remote: [u8; 16],
    reserved: [u8; 16],
}

const _: () = assert!(std::mem::size_of::<TcpEgressOpaque>() == 56);

#[inline(always)]
pub(crate) fn write_tcp_egress_endpoints(
    opaque: &mut TcpEgressOpaque,
    local: std::net::IpAddr,
    remote: std::net::IpAddr,
) {
    let (version, local_bytes, remote_bytes) = match (local, remote) {
        (std::net::IpAddr::V4(local), std::net::IpAddr::V4(remote)) => {
            let mut local_bytes = [0u8; 16];
            let mut remote_bytes = [0u8; 16];
            local_bytes[..4].copy_from_slice(&local.octets());
            remote_bytes[..4].copy_from_slice(&remote.octets());
            (4u8, local_bytes, remote_bytes)
        }
        (std::net::IpAddr::V6(local), std::net::IpAddr::V6(remote)) => {
            (6u8, local.octets(), remote.octets())
        }
        _ => return,
    };
    *opaque = TcpEgressOpaque {
        tag: TCP_EGRESS_TAG,
        version,
        pad: [0; 3],
        local: local_bytes,
        remote: remote_bytes,
        reserved: [0; 16],
    };
}

#[inline(always)]
pub(crate) fn read_tcp_egress_endpoints(
    opaque: &TcpEgressOpaque,
) -> Option<(std::net::IpAddr, std::net::IpAddr)> {
    if opaque.tag != TCP_EGRESS_TAG {
        return None;
    }
    match opaque.version {
        4 => Some((
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                opaque.local[0],
                opaque.local[1],
                opaque.local[2],
                opaque.local[3],
            )),
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                opaque.remote[0],
                opaque.remote[1],
                opaque.remote[2],
                opaque.remote[3],
            )),
        )),
        6 => Some((
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(opaque.local)),
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(opaque.remote)),
        )),
        _ => None,
    }
}

/// Skip a leading IPv4/IPv6 header when present (tcp-output push), else treat as bare TCP.

#[inline(always)]
pub(crate) fn write_session_route_opaque(
    opaque: &mut TcpRouteOpaque,
    session_id: u32,
    owner: DataWorkerId,
    next: TcpInputNext,
) {
    *opaque = TcpRouteOpaque {
        session_raw: session_id.into(),
        owner_worker: owner.slot() as u32,
        next: next as u8,
        present: 1,
        reserved: [0; 42],
    };
}

#[inline(always)]
pub(crate) fn read_session_route_opaque(
    opaque: &TcpRouteOpaque,
) -> Option<(u32, DataWorkerId, TcpInputNext)> {
    if opaque.present == 0 {
        return None;
    }
    Some((
        u32::try_from(opaque.session_raw).ok()?,
        DataWorkerId::new(opaque.owner_worker),
        match opaque.next {
            value if value == TcpInputNext::Listen as u8 => TcpInputNext::Listen,
            value if value == TcpInputNext::RcvProcess as u8 => TcpInputNext::RcvProcess,
            value if value == TcpInputNext::SynSent as u8 => TcpInputNext::SynSent,
            value if value == TcpInputNext::Established as u8 => TcpInputNext::Established,
            value if value == TcpInputNext::Reset as u8 => TcpInputNext::Reset,
            _ => TcpInputNext::Punt,
        },
    ))
}

#[inline(always)]
pub(crate) fn read_session_id(runtime: &DataPlaneMain, index: u32) -> RuntimeResult<Option<u32>> {
    let buffer = runtime.buffer(index);
    Ok(
        read_session_route_opaque(
            hammer_core::buffer_opaque!(buffer => TcpSecondaryOpaque).route(),
        )
        .map(|(session_id, _, _)| session_id),
    )
}

pub fn tcp_control_cursor(packet: &[u8]) -> Result<BufferPacketCursor, TcpControlPacketParseError> {
    let Some(version_ihl) = packet.first().copied() else {
        return Err(TcpControlPacketParseError::EmptyPacket);
    };
    let (network_header_len, packet_len) = match version_ihl >> 4 {
        4 => {
            if packet.len() < 40 {
                return Err(TcpControlPacketParseError::PacketTooShort);
            }
            (
                usize::from(version_ihl & 0x0f) * 4,
                u16::from_be_bytes([packet[2], packet[3]]) as usize,
            )
        }
        6 => {
            if packet.len() < 60 {
                return Err(TcpControlPacketParseError::PacketTooShort);
            }
            let payload_len = u16::from_be_bytes([packet[4], packet[5]]) as usize;
            (40, 40 + payload_len)
        }
        _ => return Err(TcpControlPacketParseError::UnsupportedIpVersion),
    };
    if packet_len > packet.len() || network_header_len < 20 || network_header_len >= packet_len {
        return Err(TcpControlPacketParseError::InvalidCursor);
    }
    let tcp_offset = network_header_len;
    let tcp_header_len = usize::from(packet[tcp_offset + 12] >> 4) * 4;
    if tcp_header_len < 20 || tcp_offset + tcp_header_len > packet_len {
        return Err(TcpControlPacketParseError::InvalidHeaderLength);
    }
    Ok(BufferPacketCursor::new()
        .with_packet_len(packet_len)
        .with_network_header(0, network_header_len)
        .with_transport_header(tcp_offset, tcp_header_len)
        .with_transport_payload_offset(tcp_offset + tcp_header_len))
}

fn enqueue_tcp_segment(
    runtime: &mut DataPlaneMain,
    frame: &mut hammer_core::data_plane::Frame,
    output_next: SessionQueueNext,
    output: &mut SessionQueueOutput,
    segment: TcpSegment,
) -> RuntimeResult<()> {
    if output.remaining_io_budget() == 0 {
        return Ok(());
    }
    let mut index = 0;
    if runtime.buffer_alloc(core::slice::from_mut(&mut index)) != 1 {
        return Err(hammer_core::error::DataPlaneError::from(
            hammer_core::error::BufferInvariant::PoolExhausted,
        )
        .into());
    }
    segment.write_to_buffer(&mut *runtime.buffer_mut(index))?;
    let _ = output.try_enqueue_io(frame, output_next, index)?;
    Ok(())
}

#[hammer_component_macros::node_next]
pub enum TcpInputNext {
    Drop,
    #[next("drop")]
    Punt,
    #[next("tcp-listen")]
    Listen,
    #[next("tcp-rcv-process")]
    RcvProcess,
    #[next("tcp-syn-sent")]
    SynSent,
    #[next("tcp-established")]
    Established,
    #[next("tcp-reset")]
    Reset,
}

#[hammer_component_macros::buffer_opaque(secondary)]
#[derive(Clone, Copy)]
pub(crate) union TcpSecondaryOpaque {
    route: TcpRouteOpaque,
    egress: TcpEgressOpaque,
}

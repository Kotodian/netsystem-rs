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
        input::__TCP_WORKER_GRAPH_NODE_TCP4_INPUT_NODE,
        input::__TCP_WORKER_GRAPH_NODE_TCP6_INPUT_NODE,
        drop::__TCP_WORKER_GRAPH_NODE_TCP4_DROP_NODE,
        drop::__TCP_WORKER_GRAPH_NODE_TCP6_DROP_NODE,
        output::__TCP_WORKER_GRAPH_NODE_TCP4_OUTPUT_NODE,
        output::__TCP_WORKER_GRAPH_NODE_TCP6_OUTPUT_NODE,
        established::__TCP_WORKER_GRAPH_NODE_TCP4_ESTABLISHED_NODE,
        established::__TCP_WORKER_GRAPH_NODE_TCP6_ESTABLISHED_NODE,
        reset::__SERVICE_GRAPH_NODE_TCP4_RESET_NODE,
        reset::__SERVICE_GRAPH_NODE_TCP6_RESET_NODE,
        listen::__TCP_WORKER_GRAPH_NODE_TCP4_LISTEN_NODE,
        listen::__TCP_WORKER_GRAPH_NODE_TCP6_LISTEN_NODE,
        rcv_process::__TCP_WORKER_GRAPH_NODE_TCP4_RCV_PROCESS_NODE,
        rcv_process::__TCP_WORKER_GRAPH_NODE_TCP6_RCV_PROCESS_NODE,
        syn_sent::__TCP_WORKER_GRAPH_NODE_TCP4_SYN_SENT_NODE,
        syn_sent::__TCP_WORKER_GRAPH_NODE_TCP6_SYN_SENT_NODE,
    ],
    node_functions = [
        output::__NODE_FUNCTION_TCP4_OUTPUT_NODE_PROCESS_SIMD_SCALAR,
        output::__NODE_FUNCTION_TCP4_OUTPUT_NODE_PROCESS_SIMD_SIMD128,
        output::__NODE_FUNCTION_TCP4_OUTPUT_NODE_PROCESS_SIMD_SIMD256,
        output::__NODE_FUNCTION_TCP4_OUTPUT_NODE_PROCESS_SIMD_SIMD512,
        output::__NODE_FUNCTION_TCP6_OUTPUT_NODE_PROCESS_SIMD_SCALAR,
        output::__NODE_FUNCTION_TCP6_OUTPUT_NODE_PROCESS_SIMD_SIMD128,
        output::__NODE_FUNCTION_TCP6_OUTPUT_NODE_PROCESS_SIMD_SIMD256,
        output::__NODE_FUNCTION_TCP6_OUTPUT_NODE_PROCESS_SIMD_SIMD512,
    ],
    process_nodes = [],
);

use std::cell::{RefCell, RefMut, UnsafeCell};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::OnceLock;

use hammer_core::data_plane::{BufferPacketCursor, NodeId};
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, Node, NodeRuntime, RuntimeError, RuntimeResult,
};
use thiserror::Error;

use hammer_infra::align::CacheLineAlignMark;
use hammer_infra::pool::Pool;
use hammer_plugin_session::{
    IpSessionEndpoint, IpSessionMain, IpTransportConnectionId, IpTransportEndpoint,
    IpTransportEndpointConfig, IpTransportMain,
};
use hammer_service::session::node::SessionQueueNode;
use hammer_service::session::{
    SessionError, SessionEventType, SessionHandle as ServiceSessionHandle, SessionQueueNext,
    SessionMain as ServiceSessionMain, SessionWorker as ServiceSessionWorker,
};
use hammer_service::transport::{
    Transport, TransportMain, TransportOptions, TransportSendFlags, TransportSendParams,
    TransportServiceType, TransportTxMode,
};

pub mod config;
pub mod congestion;
pub mod connection;
mod drop;
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
pub use established::{Tcp4EstablishedNode, Tcp6EstablishedNode, TcpEstablishedNext};
pub use input::{Tcp4InputNode, Tcp6InputNode, TcpInputTrace};
pub use listen::{Tcp4ListenNode, Tcp6ListenNode, TcpListenNext};
pub use output::{DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, Tcp4OutputNode, Tcp6OutputNode, TcpOutputNext};
pub use policy::{TcpPolicy, active_tcp_policy, publish_tcp_policy, tcp_policy};
pub use rcv_process::{Tcp4RcvProcessNode, Tcp6RcvProcessNode, TcpRcvProcessNext};
pub use recovery::{TcpRecoveryAck, TcpRecoveryState};
pub use reset::{Tcp4ResetNode, Tcp6ResetNode, TcpResetNext};
use segment::TcpSegment;
pub use syn_sent::{Tcp4SynSentNode, Tcp6SynSentNode, TcpSynSentNext};

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
    listener_control: listener_control::TcpListenerControl,
    listeners: UnsafeCell<Pool<TcpConnection>>,
    workers: Box<[TcpWorkerSlot]>,
}

// SAFETY: every RefCell slot is permanently assigned to one Data Worker.
// Main Thread never borrows worker slots after startup; each worker selects
// only its own immutable runtime thread index.
unsafe impl Sync for TcpMain {}

impl TcpMain {
    fn new(protocol: u8, worker_count: usize) -> Self {
        let listener_control = listener_control::TcpListenerControl::new();
        let workers = (0..worker_count)
            .map(|worker| {
                TcpWorkerSlot::new(TcpWorker::new(DataWorkerId::new(worker as u32), protocol))
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            protocol,
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

    pub const fn protocol(&self) -> u8 {
        self.protocol
    }

    fn bind_tcp_listener(
        &self,
        bind: SocketAddr,
        owner_worker: DataWorkerId,
        capabilities: TcpCapabilities,
        session_listener: ServiceSessionHandle,
        fib_index: u32,
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
        connection.base.endpoint = match (bind, remote) {
            (SocketAddr::V4(local), SocketAddr::V4(remote)) => {
                IpTransportConnectionId::from((fib_index, local, remote, self.protocol))
            }
            (SocketAddr::V6(local), SocketAddr::V6(remote)) => {
                IpTransportConnectionId::from((fib_index, local, remote, self.protocol))
            }
            _ => unreachable!("listener and remote endpoint share one IP family"),
        };
        connection.base.session = session_listener;
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
            DataWorkerId::new(0),
            listener_capabilities(),
            session,
            local.fib_index,
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
                    let mut transport = tcp_endpoint_pair(local, connection.remote()).0;
                    transport.local.fib_index = connection.base.endpoint.fib_index();
                    IpSessionEndpoint::new(transport, self.protocol)
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
                timer_wheel: timers,
                ..
            } = worker;
            if let Some(connection) = connections.get_mut(connection_index) {
                connection.on_session_close(connection_index, timers);
                return;
            }
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

    /// VPP: tcp_output.c:980-1035, `tcp_session_push_header`.
    fn push_header(
        &self,
        connection_index: u32,
        worker_index: u32,
        buffers: &mut [u32],
        _: u32,
    ) -> u32 {
        let thread_index = DataWorkerId::new(worker_index).thread_index();
        let mut tcp = self
            .worker(thread_index)
            .expect("TCP header push runs on its owner Data Worker");
        let TcpWorker {
            connections,
            lookup,
            timer_wheel,
            ..
        } = &mut *tcp;
        let connection = connections
            .get_mut(connection_index)
            .expect("Session Queue retains a live TCP connection while pushing headers");
        let capabilities = lookup
            .pending_open_capabilities(connection.session_id())
            .unwrap_or_default();
        let now = std::time::Instant::now();
        let buffer_main = hammer_core::buffer::BufferMain::global();
        for &index in buffers.iter() {
            // SAFETY: Session Queue transfers each live TX Buffer to this
            // transport operation and does not retain an overlapping borrow.
            let buffer = unsafe { buffer_main.buffer_mut_for_worker(thread_index, index) };
            let payload_len = buffer.current_len() + buffer.total_len_not_including_first();
            let segment = connection
                .tx_segment(payload_len, capabilities)
                .expect("Session Queue pushes TCP headers only for a sendable connection");
            segment
                .write_to_buffer(buffer)
                .expect("Session Queue supplies a Buffer with TCP header headroom");
            let egress = hammer_core::buffer_opaque!(mut buffer => TcpSecondaryOpaque).egress_mut();
            egress.connection_index = connection_index;
            egress.worker_index = worker_index;
            egress.fib_index = connection.base.endpoint.fib_index();
            connection
                .commit_payload_tx(payload_len, now)
                .expect("validated TCP payload length commits after header construction");
        }
        if !buffers.is_empty() {
            connection
                .sync_payload_tx_timers(connection_index, timer_wheel, now)
                .expect("TCP timer interval remains valid after payload transmission");
        }
        0
    }

    /// VPP: tcp.c:1170-1198, `tcp_session_send_params`.
    fn send_params(&self, connection_index: u32, worker_index: u32) -> TransportSendParams {
        let mut tcp = self
            .worker(DataWorkerId::new(worker_index).thread_index())
            .expect("TCP send-params runs on its owner Data Worker");
        let TcpWorker {
            connections,
            lookup,
            ..
        } = &mut *tcp;
        let connection = connections
            .get_mut(connection_index)
            .expect("Session Queue retains a live TCP connection while querying send space");
        let _ = connection.refresh_path_mtu_from_cache();
        let start = if connection.state() == TcpState::SynSent {
            connection.iss()
        } else {
            connection.snd_una()
        };
        let tx_offset = TcpSeq::from(start).distance_to(connection.tx_payload_sequence());
        let capabilities = lookup
            .pending_open_capabilities(connection.session_id())
            .unwrap_or_default();
        let send_space = connection.tx_payload_budget(
            u32::MAX as usize,
            std::time::Instant::now(),
            capabilities,
        );
        TransportSendParams {
            send_space: u32::try_from(send_space).unwrap_or(u32::MAX),
            tx_offset,
            send_mss: u16::try_from(connection.send_goal_size()).unwrap_or(u16::MAX),
            flags: TransportSendFlags {
                deschedule: send_space == 0,
                postpone: false,
            },
            ..TransportSendParams::default()
        }
    }

    /// VPP: tcp.c:1361-1369, `tcp_update_time`; tcp.h:76-122,
    /// `tcp_worker_ctx_t.timer_wheel` and pending timer FIFO.
    fn update_time(&self, now: f64, worker_index: u32) {
        let mut tcp = self
            .worker(DataWorkerId::new(worker_index).thread_index())
            .expect("TCP update-time subscriber runs on its owner Data Worker");
        assert!(
            now.is_finite() && now >= 0.0,
            "TCP worker time must be finite and nonnegative"
        );
        tcp.time_us = now;
        tcp.time_tstamp = ((now * 1_000.0) as u64) as u32;
        let origin_seconds = *tcp.time_origin_seconds.get_or_insert(now);
        let elapsed = (now - origin_seconds).max(0.0);
        let now = tcp
            .time_origin
            .checked_add(std::time::Duration::from_secs_f64(elapsed))
            .expect("TCP worker monotonic time remains representable");
        tcp.advance_timer_wheel(now);
    }

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
    let protocol = IpSessionMain::global()
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?
        .register_transport_type(TransportTxMode::Peek, u32::MAX)
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?;
    let config = TCP_CONFIG
        .get()
        .expect("TCP configuration is installed before initialization");
    publish_tcp_policy(TcpPolicy::from_plugin_config(config));
    let main = TcpMain::new(protocol, hammer_runtime::config::worker::worker_count());
    assert!(
        TCP_MAIN.set(main).is_ok(),
        "TCP initialization callback executes once"
    );
    ServiceSessionMain::global()
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?
        .register_transport_io(protocol, tcp_session_io, tcp_session_update_time)
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?;
    Ok(())
}

/// VPP: session_node.c:1858-1915, the TCP session type's IO dispatch.
fn tcp_session_io(
    runtime: &mut DataPlaneMain,
    sessions: &mut ServiceSessionWorker,
    session_index: u32,
    event: SessionEventType,
) -> Result<(usize, bool), SessionError> {
    let tcp = TCP_MAIN
        .get()
        .expect("TCP Main remains published while its Session type is registered");
    let session = sessions
        .session(session_index)
        .ok_or(SessionError::NoSession)?;
    let connection_index = session.connection_index();
    let worker_index = sessions.worker_index();
    match event {
        SessionEventType::Tx | SessionEventType::TxFlush => {
            if event == SessionEventType::TxFlush {
                <TcpMain as Transport<IpTransportEndpointConfig>>::flush_data(
                    tcp, connection_index, worker_index,
                );
            }
            let next = {
                let worker = tcp
                    .worker(runtime.thread_index())
                    .map_err(|source| SessionError::TransportOpFailed { source })?;
                let connection = worker
                    .connection(connection_index)
                    .ok_or(SessionError::NoSession)?;
                worker.tco_next_node[usize::from(!connection.remote().is_ipv4())]
            };
            sessions.tx_fifo_peek_and_send(runtime, session_index, tcp, next)
        }
        SessionEventType::Rx => {
            // VPP: tcp.c:1382-1401, tcp_session_app_rx_evt.
            let rx = session
                .rx_fifo()
                .expect("accepted TCP Session retains its RX FIFO");
            let available = rx.max_enqueue();
            let minimum = (rx.size() >> 3).clamp(4 << 10, 128 << 10);
            let mut worker = tcp
                .worker(runtime.thread_index())
                .map_err(|source| SessionError::TransportOpFailed { source })?;
            let next_nodes = worker.tco_next_node;
            let connection = worker
                .connection_mut(connection_index)
                .ok_or(SessionError::NoSession)?;
            connection.set_rcv_wnd(available);
            if !connection.zero_receive_window_sent() {
                return Ok((0, false));
            }
            if available < minimum {
                rx.want_deq_notification();
                return Ok((0, false));
            }
            let mut candidate = connection.clone();
            let segment = candidate
                .receive_window_update_segment(available)
                .map_err(|source| SessionError::TransportOpFailed { source })?;
            let next = next_nodes[usize::from(!candidate.remote().is_ipv4())];
            if !enqueue_session_tcp_segment(runtime, sessions, connection_index, next, segment)? {
                return Ok((0, true));
            }
            *connection = candidate;
            Ok((0, false))
        }
        _ => unreachable!("TCP Session IO dispatch receives only stream events"),
    }
}

/// VPP: tcp_output.c and session_node.c:1456-1468. TCP owns the header;
/// Session owns the pending Buffer fanout to the TCP output node.
fn enqueue_session_tcp_segment(
    runtime: &mut DataPlaneMain,
    sessions: &mut ServiceSessionWorker,
    connection_index: u32,
    next: SessionQueueNext,
    segment: TcpSegment,
) -> Result<bool, SessionError> {
    let mut buffers = [0u32; 1];
    if runtime.buffer_alloc(&mut buffers) == 0 {
        return Ok(false);
    }
    if let Err(source) = segment.write_to_buffer(runtime.buffer_mut(buffers[0])) {
        runtime.buffer_free_one(buffers[0]);
        return Err(SessionError::TransportOpFailed { source });
    }
    let egress = hammer_core::buffer_opaque!(mut runtime.buffer_mut(buffers[0]) => TcpSecondaryOpaque)
        .egress_mut();
    egress.connection_index = connection_index;
    egress.worker_index = sessions.worker_index();
    sessions.add_pending_tx_buffer(runtime, buffers[0], next);
    Ok(true)
}

/// VPP: tcp.c:1361-1369, tcp_update_time; tcp.c:1293-1335,
/// tcp_dispatch_pending_timers.
fn tcp_session_update_time(
    runtime: &mut DataPlaneMain,
    sessions: &mut ServiceSessionWorker,
    now: f64,
) -> Result<(), SessionError> {
    let tcp = TCP_MAIN
        .get()
        .expect("TCP Main remains published while its time subscriber is registered");
    let worker_index = sessions.worker_index();
    <TcpMain as Transport<IpTransportEndpointConfig>>::update_time(tcp, now, worker_index);
    let mut worker = tcp
        .worker(runtime.thread_index())
        .map_err(|source| SessionError::TransportOpFailed { source })?;
    let now = worker.last_timer_update;
    for _ in 0..worker.max_timers_per_loop {
        let Some(token) = worker.take_pending_timer() else {
            break;
        };
        let (session_index, outcome, is_ip4) = {
            let worker::TcpWorker {
                connections,
                lookup,
                timer_wheel,
                ..
            } = &mut *worker;
            let Some(connection) = connections.get_mut(token.index) else {
                continue;
            };
            let session_index = connection.session_id();
            let capabilities = lookup
                .pending_open_capabilities(session_index)
                .unwrap_or_default();
            let outcome = connection
                .on_typed_timer_expiry(token.index, timer_wheel, token.kind, capabilities, now)
                .map_err(|source| SessionError::TransportOpFailed { source })?;
            (session_index, outcome, connection.remote().is_ipv4())
        };
        if let Some(action) = outcome.action {
            let counter = match action {
                connection::TcpTimerAction::RtoRetransmit => TcpNodeError::Retransmit,
                connection::TcpTimerAction::RackRetransmit => TcpNodeError::RackRetransmit,
                connection::TcpTimerAction::TlpProbe => TcpNodeError::TlpProbe,
                connection::TcpTimerAction::PersistProbe => TcpNodeError::PersistProbe,
                connection::TcpTimerAction::KeepaliveProbe => TcpNodeError::KeepaliveProbe,
            };
            runtime
                .record_current_node_error(counter)
                .expect("TCP timer runs in the registered Session Queue Node");
        }
        let handle = ServiceSessionHandle {
            worker_index,
            session_index,
        };
        if let Some(segment) = outcome.segment {
            if segment.payload_len() == 0 {
                let next = worker.tco_next_node[usize::from(!is_ip4)];
                enqueue_session_tcp_segment(runtime, sessions, token.index, next, segment)?;
            } else if sessions.session_from_handle(handle).is_some() {
                sessions.enqueue_ready(handle, tcp.protocol)?;
            }
        } else if matches!(
            token.kind,
            timers::TcpTimerKind::Retransmit
                | timers::TcpTimerKind::Rack
                | timers::TcpTimerKind::Tlp
                | timers::TcpTimerKind::Persist
                | timers::TcpTimerKind::Pacing
        ) && sessions.session_from_handle(handle).is_some()
        {
            sessions.enqueue_ready(handle, tcp.protocol)?;
        }
    }
    Ok(())
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

pub fn register_tcp4_input(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = if let Some(node) = runtime.nodes().node_by_name("tcp4-input") {
        node
    } else {
        runtime.nodes().try_register_internal_with_next_names(
            Tcp4InputNode::new(),
            &TcpInputNext::NEXT_NAMES,
        )?
    };
    hammer_plugin_ip::register_ip4_protocol(runtime.nodes(), 6, node)?;
    Ok(node)
}

pub fn register_tcp6_input(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = if let Some(node) = runtime.nodes().node_by_name("tcp6-input") {
        node
    } else {
        runtime.nodes().try_register_internal_with_next_names(
            Tcp6InputNode::new(),
            &[
                "tcp6-drop",
                "ip6-punt",
                "tcp6-listen",
                "tcp6-rcv-process",
                "tcp6-syn-sent",
                "tcp6-established",
                "tcp6-reset",
            ],
        )?
    };
    hammer_plugin_ip::register_ip6_protocol(runtime.nodes(), 6, node)?;
    Ok(node)
}

fn bind_worker_graph(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    let session_queue =
        engine
            .node_by_name("session-queue")
            .ok_or(TcpWorkerError::NodeMissing {
                name: "session-queue",
            })?;
    let tcp4_output =
        engine
            .node_by_name(Tcp4OutputNode::NODE_NAME)
            .ok_or(TcpWorkerError::NodeMissing {
                name: Tcp4OutputNode::NODE_NAME,
            })?;
    let tcp6_output =
        engine
            .node_by_name(Tcp6OutputNode::NODE_NAME)
            .ok_or(TcpWorkerError::NodeMissing {
                name: Tcp6OutputNode::NODE_NAME,
            })?;

    let session_queue_output4 =
        SessionQueueNode::existing_output_next(engine, session_queue, tcp4_output)?;
    let session_queue_output6 =
        SessionQueueNode::existing_output_next(engine, session_queue, tcp6_output)?;
    let main = TCP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let mut tcp = main.worker(engine.thread_index())?;
    tcp.tco_next_node = [session_queue_output4, session_queue_output6];

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
    connection_index: u32,
    worker_index: u32,
    fib_index: u32,
    reserved: [u8; 4],
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
        connection_index: u32::MAX,
        worker_index: u32::MAX,
        fib_index: u32::MAX,
        reserved: [0; 4],
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

#[hammer_component_macros::node_next]
pub enum TcpInputNext {
    #[next("tcp4-drop")]
    Drop,
    #[next("ip4-punt")]
    Punt,
    #[next("tcp4-listen")]
    Listen,
    #[next("tcp4-rcv-process")]
    RcvProcess,
    #[next("tcp4-syn-sent")]
    SynSent,
    #[next("tcp4-established")]
    Established,
    #[next("tcp4-reset")]
    Reset,
}

#[hammer_component_macros::buffer_opaque(secondary)]
#[derive(Clone, Copy)]
pub(crate) union TcpSecondaryOpaque {
    route: TcpRouteOpaque,
    egress: TcpEgressOpaque,
}

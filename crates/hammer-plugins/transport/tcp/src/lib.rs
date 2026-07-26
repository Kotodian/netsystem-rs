//! Dynamic `tcp` plugin (`libhammer_plugin_tcp`).

hammer_component_macros::declare_plugin!(
    name = "tcp",
    load_after = ["ip"],
    init_functions = [__INIT_FN_TCP_INIT],
    config_functions = [],
    early_config_functions = [__CONFIG_FN_TCP_CONFIG],
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

use std::mem::transmute;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_core::data_plane::{BufferPacketCursor, NodeHandle, NodeId, NodeState, SecondaryOpaque};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Engine, Node, NodeProcessFn, NodeRuntimeData, PluginError,
    RuntimeError, RuntimeResult,
};
use thiserror::Error;

use hammer_infra::align::CacheLine;
use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::thread_owned::{ThreadOwned, ThreadOwnedError};
use hammer_service::session::node::{SessionQueueNode, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionMain, SessionTransport, SessionWorker, dispatch_session_queue_pending,
};
use hammer_service::session::{SessionId, SessionQueueNext};

pub mod config;
pub mod congestion;
pub mod connection;
mod control_plane;
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
pub use control_plane::TcpControlPlane;
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

#[cfg(test)]
mod session_driver_tests;

#[derive(Debug, Error)]
enum TcpWorkerError {
    #[error("required graph node `{name}` is not registered")]
    NodeMissing { name: &'static str },
    #[error("runtime thread {thread_index} is not a data worker")]
    WorkerUnavailable { thread_index: u32 },
    #[error("TCP worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("TCP worker {worker} is already installed")]
    WorkerAlreadyInstalled { worker: usize },
    #[error("TCP worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: ThreadOwnedError,
    },
}

pub(crate) fn publish_tcp_connection(
    sessions: &mut SessionWorker<PoolIndex>,
    tcp: &mut TcpWorker,
    session_id: SessionId,
) -> RuntimeResult<()> {
    let (_, index) = sessions
        .session_transport(session_id)
        .ok_or(TcpNodeError::SessionMissing)?;
    let close = {
        let TcpWorker {
            connections,
            lookup,
            ..
        } = tcp;
        let connection = connections.get(index).ok_or(TcpNodeError::SessionMissing)?;
        lookup.publish_connection(session_id, connection)
    };
    let initial = sessions.connection_published(session_id)?;
    if close {
        if initial {
            return Err(TcpError::ConnectionClosed.into());
        }
        sessions.notify_transport_closed(session_id, index)?;
        let _ = tcp.remove_connection(index);
        sessions.notify_transport_deleted(session_id, index);
    }
    Ok(())
}

pub struct TcpMain {
    control: TcpInputControlPlane,
    listeners: listener_control::TcpListenerControlHandle,
    ip_output: output::IpOutputFunctions,
    input_process: NodeProcessFn,
    listen_process: NodeProcessFn,
    established_process: NodeProcessFn,
    rcv_process: NodeProcessFn,
    syn_sent_process: NodeProcessFn,
    sessions: Arc<SessionMain>,
    workers: Box<[CacheLine<ThreadOwned<TcpWorker>>]>,
}

impl TcpMain {
    fn new(
        worker_count: usize,
        sessions: Arc<SessionMain>,
        ip_output: output::IpOutputFunctions,
    ) -> Self {
        let control = TcpInputControlPlane::new();
        let listeners = listener_control::TcpListenerControlHandle::new(control.clone());
        let workers = (0..worker_count)
            .map(|_| CacheLine::new(ThreadOwned::new()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            control,
            listeners,
            ip_output,
            input_process: input::tcp_input_process,
            listen_process: listen::tcp_listen_process,
            established_process: established::tcp_established_process,
            rcv_process: rcv_process::tcp_rcv_process_process,
            syn_sent_process: syn_sent::tcp_syn_sent_process,
            sessions,
            workers,
        }
    }

    fn worker(&self, worker: DataWorkerId) -> RuntimeResult<&ThreadOwned<TcpWorker>> {
        self.workers
            .get(worker.slot())
            .map(|slot| &**slot)
            .ok_or_else(|| {
                RuntimeError::subsystem(
                    "tcp",
                    TcpWorkerError::WorkerOutOfRange {
                        worker: worker.slot(),
                    },
                )
            })
    }

    fn with_worker<R>(
        &self,
        runtime: &DataPlaneRuntime,
        operation: impl FnOnce(&mut SessionWorker<PoolIndex>, &mut TcpWorker) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let thread_index = runtime.thread_index();
        let worker = thread_index
            .checked_sub(1)
            .map(DataWorkerId::new)
            .ok_or_else(|| {
                RuntimeError::subsystem("tcp", TcpWorkerError::WorkerUnavailable { thread_index })
            })?;
        self.sessions.with_worker_mut(runtime, |sessions| {
            self.worker(worker)?
                .with_mut(|tcp| operation(sessions, tcp))
                .map_err(|source| {
                    RuntimeError::subsystem(
                        "tcp",
                        TcpWorkerError::WorkerAccess {
                            worker: worker.slot(),
                            source,
                        },
                    )
                })?
        })
    }

    pub fn control(&self) -> &TcpInputControlPlane {
        &self.control
    }

    pub(crate) const fn ip_output(&self) -> output::IpOutputFunctions {
        self.ip_output
    }

    fn bind_tcp_listener(
        &self,
        bind: SocketAddr,
        owner_worker: DataWorkerId,
        capabilities: TcpCapabilities,
    ) -> RuntimeResult<lookup::TcpLookupId> {
        self.listeners.bind(bind, owner_worker, capabilities)
    }
}

// VPP alignment: `tcp_main_t tcp_main;` is a file-scope global in VPP's
// `tcp.c`; nodes read it directly and `tcp_init` publishes the configured
// instance before workers start.
pub static TCP_MAIN: ArcSwapOption<TcpMain> = ArcSwapOption::const_empty();

#[hammer_component_macros::config_function(
    name = "tcp_config",
    section = "plugin.tcp",
    early = true,
    runs_after = ["runtime_worker_config"]
)]
fn configure_tcp(
    config: crate::config::TcpPluginConfig,
) -> RuntimeResult<Arc<crate::config::TcpPluginConfig>> {
    config.validate()?;
    Ok(Arc::new(config))
}

#[hammer_component_macros::init_function(
    name = "tcp_init",
    runs_after = ["transport_init", "session_init"],
    runs_before = ["install_packet_graph"]
)]
fn init_tcp(
    engine: &mut Engine,
    config: Arc<crate::config::TcpPluginConfig>,
    sessions: Arc<SessionMain>,
) -> RuntimeResult<()> {
    let main = Arc::new(configured_tcp_main(
        config.as_ref(),
        engine.configured_worker_count(),
        sessions,
        output::plugin_functions(engine.plugin_main())?,
    )?);
    TCP_MAIN.store(Some(Arc::clone(&main)));
    Ok(())
}

fn configured_tcp_main(
    tcp: &crate::config::TcpPluginConfig,
    worker_count: usize,
    sessions: Arc<SessionMain>,
    ip_output: output::IpOutputFunctions,
) -> RuntimeResult<TcpMain> {
    publish_tcp_policy(TcpPolicy::from_plugin_config(tcp));
    let main = TcpMain::new(worker_count, sessions, ip_output);
    for entry in &tcp.listen {
        main.bind_tcp_listener(
            entry.address,
            DataWorkerId::new(0),
            TcpCapabilities::default(),
        )?;
    }
    Ok(main)
}

pub fn register_tcp_input(runtime: &DataPlaneRuntime) -> RuntimeResult<NodeId> {
    let main = TCP_MAIN
        .load_full()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let node = if let Some(node) = runtime.nodes().node_by_name("tcp-input") {
        node
    } else {
        runtime.nodes().try_register_internal_with_next_names(
            main.control().node(main.input_process, None),
            &TcpInputNext::NEXT_NAMES,
        )?
    };
    main.ip_output()
        .get()
        .register_protocol(6, node)
        .into_result()
        .map_err(|source| {
            RuntimeError::from(PluginError::CapabilityCall {
                plugin: "ip",
                capability: "register protocol",
                source,
            })
        })?;
    Ok(node)
}

fn bind_worker_graph(engine: &mut Engine) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let handoff = engine.runtime.handoff_node_handle()?;
    let session_queue = engine
        .runtime
        .node_by_name("session-queue")
        .ok_or_else(|| {
            RuntimeError::subsystem(
                "tcp",
                TcpWorkerError::NodeMissing {
                    name: "session-queue",
                },
            )
        })?;
    let tcp_output = engine
        .runtime
        .node_by_name(TcpOutputNode::NODE_NAME)
        .ok_or_else(|| {
            RuntimeError::subsystem(
                "tcp",
                TcpWorkerError::NodeMissing {
                    name: TcpOutputNode::NODE_NAME,
                },
            )
        })?;
    let tcp_input = engine.runtime.node_by_name("tcp-input").ok_or_else(|| {
        RuntimeError::subsystem("tcp", TcpWorkerError::NodeMissing { name: "tcp-input" })
    })?;
    let tcp_listen = engine.runtime.node_by_name("tcp-listen").ok_or_else(|| {
        RuntimeError::subsystem("tcp", TcpWorkerError::NodeMissing { name: "tcp-listen" })
    })?;
    let tcp_established = engine
        .runtime
        .node_by_name("tcp-established")
        .ok_or_else(|| {
            RuntimeError::subsystem(
                "tcp",
                TcpWorkerError::NodeMissing {
                    name: "tcp-established",
                },
            )
        })?;
    let tcp_rcv_process = engine
        .runtime
        .node_by_name("tcp-rcv-process")
        .ok_or_else(|| {
            RuntimeError::subsystem(
                "tcp",
                TcpWorkerError::NodeMissing {
                    name: "tcp-rcv-process",
                },
            )
        })?;
    let tcp_syn_sent = engine.runtime.node_by_name("tcp-syn-sent").ok_or_else(|| {
        RuntimeError::subsystem(
            "tcp",
            TcpWorkerError::NodeMissing {
                name: "tcp-syn-sent",
            },
        )
    })?;

    let session_queue_node = SessionQueueNode::new()?;
    let session_queue_data = session_queue_node.node_runtime_data()?;
    let session_queue_output =
        SessionQueueNode::existing_output_next(&engine.runtime, session_queue, tcp_output)?;
    SessionQueueNode::install_worker_attachment(
        session_queue_data,
        session_queue_output,
        tcp_session_queue_dispatch,
    )?;
    let main_guard = TCP_MAIN.load();
    let main = main_guard
        .as_deref()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    let input_data = main
        .control()
        .node(main.input_process, Some((handoff, worker)))
        .node_runtime_data()?;
    let listen_data = TcpListenNode::new(main.listen_process).node_runtime_data()?;
    let established_data = TcpEstablishedNode::new(main.established_process).node_runtime_data()?;
    let rcv_process_data = TcpRcvProcessNode::new(main.rcv_process).node_runtime_data()?;
    let syn_sent_data = TcpSynSentNode::new(main.syn_sent_process).node_runtime_data()?;

    // A worker graph clone can retain the old polling state. Keep the node
    // dormant until its replacement SessionWorker owns a live readiness file.
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    engine.set_worker_node_runtime_data(session_queue, session_queue_data)?;
    engine.set_worker_node_runtime_data(tcp_input, input_data)?;
    engine.set_worker_node_runtime_data(tcp_listen, listen_data)?;
    engine.set_worker_node_runtime_data(tcp_established, established_data)?;
    engine.set_worker_node_runtime_data(tcp_rcv_process, rcv_process_data)?;
    engine.set_worker_node_runtime_data(tcp_syn_sent, syn_sent_data)?;

    if main
        .worker(worker)?
        .install(TcpWorker::new(worker))
        .is_err()
    {
        return Err(RuntimeError::subsystem(
            "tcp",
            TcpWorkerError::WorkerAlreadyInstalled {
                worker: worker.slot(),
            },
        ));
    }
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Polling)?;
    Ok(())
}

#[hammer_component_macros::worker_init_function(
    name = "tcp_worker_init",
    runs_after = ["session_worker_init"]
)]
fn init_tcp_worker(engine: &mut Engine) -> RuntimeResult<()> {
    TCP_MAIN
        .load_full()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?;
    bind_worker_graph(engine)
}

fn tcp_session_queue_dispatch(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: std::time::Instant,
    frame: &mut hammer_core::data_plane::BufferFrame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    TCP_MAIN
        .load_full()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "tcp" })?
        .with_worker(runtime, |sessions, tcp| {
            dispatch_session_queue_pending(runtime, sessions, tcp, output_next, frame, output, now)
                .map(|_| ())
        })
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
    #[error("RACK timeout")]
    RackTimeout,
    #[error("TLP probe")]
    TlpProbe,
    #[error("retransmit")]
    Retransmit,
    #[error("pacing limited")]
    PacingLimited,
    #[error("persist timer")]
    PersistTimer,
    #[error("BBR congestion")]
    BbrCongestion,
    #[error("bad window")]
    BadWindow,
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
            | TcpNodeError::RackTimeout
            | TcpNodeError::TlpProbe
            | TcpNodeError::Retransmit
            | TcpNodeError::PacingLimited
            | TcpNodeError::PersistTimer
            | TcpNodeError::BbrCongestion => TcpError::Dispatch,
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

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TcpOutputError {
    #[error("not a TCP header")]
    NoTcpHeader,
    #[error("missing TCP egress endpoints")]
    MissingEgressEndpoints,
    #[error("unsupported TCP egress address family")]
    UnsupportedEgress,
    #[error("IP output service is unavailable")]
    IpOutputUnavailable,
    #[error("TCP segment is too long for its IP packet")]
    SegmentTooLong,
    #[error("IP output rejected the prepended IP header")]
    IpHeaderRejected,
}

impl TcpOutputError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

impl From<TcpOutputError> for RuntimeError {
    #[inline]
    fn from(error: TcpOutputError) -> Self {
        Self::subsystem("tcp", error)
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TcpResetError {
    #[error("bad TCP header")]
    BadTcpHeader,
}

impl TcpResetError {
    #[inline(always)]
    pub const fn code(self) -> u16 {
        self as u16
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct TcpRouteOpaque {
    session_raw: u64,
    owner_worker: u32,
    next: u8,
    present: u8,
    reserved: [u8; 42],
}

const _: () =
    assert!(std::mem::size_of::<TcpRouteOpaque>() == std::mem::size_of::<SecondaryOpaque>());

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
#[repr(C)]
struct TcpEgressOpaque {
    tag: u32,
    version: u8,
    pad: [u8; 3],
    local: [u8; 16],
    remote: [u8; 16],
    reserved: [u8; 16],
}

const _: () =
    assert!(std::mem::size_of::<TcpEgressOpaque>() == std::mem::size_of::<SecondaryOpaque>());

#[inline(always)]
pub(crate) fn write_tcp_egress_endpoints(
    opaque: &mut SecondaryOpaque,
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
    let egress = unsafe { transmute::<&mut SecondaryOpaque, &mut TcpEgressOpaque>(opaque) };
    *egress = TcpEgressOpaque {
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
    opaque: &SecondaryOpaque,
) -> Option<(std::net::IpAddr, std::net::IpAddr)> {
    let egress = unsafe { *transmute::<&SecondaryOpaque, &TcpEgressOpaque>(opaque) };
    if egress.tag != TCP_EGRESS_TAG {
        return None;
    }
    match egress.version {
        4 => Some((
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                egress.local[0],
                egress.local[1],
                egress.local[2],
                egress.local[3],
            )),
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                egress.remote[0],
                egress.remote[1],
                egress.remote[2],
                egress.remote[3],
            )),
        )),
        6 => Some((
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(egress.local)),
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(egress.remote)),
        )),
        _ => None,
    }
}

/// Skip a leading IPv4/IPv6 header when present (tcp-output push), else treat as bare TCP.
#[cfg(test)]
#[inline]
pub(crate) fn tcp_bytes_after_l3(packet: &[u8]) -> &[u8] {
    let offset = match packet.first().copied().map(|b| b >> 4) {
        Some(4) if packet.len() >= 20 => usize::from(packet[0] & 0x0f) * 4,
        Some(6) if packet.len() >= 40 => 40,
        _ => 0,
    };
    packet.get(offset..).unwrap_or(packet)
}

#[inline(always)]
pub(crate) fn write_session_route_opaque(
    opaque: &mut SecondaryOpaque,
    session_id: SessionId,
    owner: DataWorkerId,
    next: TcpInputNext,
) {
    let route = unsafe { transmute::<&mut SecondaryOpaque, &mut TcpRouteOpaque>(opaque) };
    *route = TcpRouteOpaque {
        session_raw: session_id.get(),
        owner_worker: owner.slot() as u32,
        next: next as u8,
        present: 1,
        reserved: [0; 42],
    };
}

#[inline(always)]
pub(crate) fn read_session_route_opaque(
    opaque: &SecondaryOpaque,
) -> Option<(SessionId, DataWorkerId, TcpInputNext)> {
    let route = unsafe { *transmute::<&SecondaryOpaque, &TcpRouteOpaque>(opaque) };
    if route.present == 0 {
        return None;
    }
    Some((
        SessionId::from_raw(route.session_raw),
        DataWorkerId::new(route.owner_worker),
        match route.next {
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
pub(crate) fn read_session_id(
    runtime: &DataPlaneRuntime,
    index: hammer_core::data_plane::Index,
) -> RuntimeResult<Option<SessionId>> {
    let buffer = runtime.get_buffer(index)?;
    Ok(read_session_route_opaque(buffer.opaque2()).map(|(session_id, _, _)| session_id))
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
    runtime: &DataPlaneRuntime,
    frame: &mut hammer_core::data_plane::BufferFrame,
    output_next: SessionQueueNext,
    output: &mut SessionQueueOutput,
    segment: TcpSegment,
) -> RuntimeResult<()> {
    if output.remaining_io_budget() == 0 {
        return Ok(());
    }
    let index = runtime.buffers().alloc_index()?;
    segment.write_to_buffer(runtime.buffers(), index)?;
    let _ = output.try_enqueue_io(frame, output_next, index)?;
    Ok(())
}

#[cfg(test)]
#[doc(hidden)]
pub(crate) fn closing_session_for_test() -> (
    SessionWorker<PoolIndex>,
    TcpWorker,
    SessionId,
    std::net::SocketAddr,
    std::net::SocketAddr,
) {
    let local: std::net::SocketAddr = "192.0.2.10:443".parse().expect("local");
    let remote: std::net::SocketAddr = "198.51.100.20:50001".parse().expect("remote");
    let worker = DataWorkerId::new(0);
    let runtime = hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
        buffers: hammer_runtime::DataPlaneBufferConfig {
            buffer_slot_capacity: 2048,
            buffer_slots: 4,
            frame_slots: 4,
            ..hammer_runtime::DataPlaneBufferConfig::default()
        },
    });
    let mut sessions = SessionWorker::new(worker).expect("session worker for test");
    let mut tcp = TcpWorker::new(worker);
    let connection = TcpConnection::established_for_time_wait_test(
        None,
        worker,
        local.port(),
        Some(local),
        remote,
    );
    let connection_index = tcp
        .insert_connection(connection)
        .expect("insert TCP connection");
    let session_id = sessions
        .stream_accept(TcpWorker::ID, connection_index, 0)
        .expect("accept stream session");
    tcp.connection_mut(connection_index)
        .expect("TCP connection")
        .attach_session(session_id)
        .expect("attach stream session");
    publish_tcp_connection(&mut sessions, &mut tcp, session_id).expect("publish TCP connection");
    sessions
        .connected(session_id)
        .expect("notify accepted session");
    (sessions, tcp, session_id, local, remote)
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

#[cfg(test)]
mod init_tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use abi_stable::{
        RRef,
        sabi_trait::TD_Opaque,
        std_types::{RBoxError, ROk, RResult, RSlice, RSliceMut},
    };
    use hammer_core::data_plane::NodeId;
    use hammer_runtime::IpOutput;
    use hammer_runtime::RuntimeRegistry;
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

    use super::*;

    struct TestIpOutput;

    impl IpOutput for TestIpOutput {
        fn register_protocol(&self, _: u8, _: NodeId) -> RResult<(), RBoxError> {
            ROk(())
        }

        fn write_ipv4_header(
            &self,
            _: RSliceMut<'_, u8>,
            _: RSlice<'_, u8>,
            _: RSlice<'_, u8>,
            _: u8,
            _: u16,
        ) -> bool {
            false
        }

        fn write_ipv6_header(
            &self,
            _: RSliceMut<'_, u8>,
            _: RSlice<'_, u8>,
            _: RSlice<'_, u8>,
            _: u8,
            _: u16,
        ) -> bool {
            false
        }
    }

    static TEST_IP_OUTPUT: hammer_runtime::IpOutput_CTO<'static, 'static> =
        hammer_runtime::IpOutput_CTO::from_const(&TestIpOutput, TD_Opaque);

    fn test_engine() -> Engine {
        let mut engine = Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        );
        engine
            .plugin_main_mut()
            .register_builtin_image(hammer_service::registration_image());
        let plugin = crate::plugin_module();
        engine
            .plugin_main_mut()
            .register_builtin_image(plugin.registration_image().get());
        engine
    }

    #[test]
    fn tcp_config_binds_configured_listens() {
        let mut engine = test_engine();
        engine
            .configure_early(
                r#"
[[plugin.tcp.listen]]
address = "10.0.0.1:7"
"#,
            )
            .expect("dispatch tcp config");

        let config = engine
            .registry
            .require::<crate::config::TcpPluginConfig>()
            .expect("published TCP config");
        let main = configured_tcp_main(
            config.as_ref(),
            1,
            Arc::new(SessionMain::new(1)),
            RRef::new(&TEST_IP_OUTPUT),
        )
        .expect("build configured TCP main");
        let entry = main
            .control()
            .lookup_listener(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7));
        assert!(entry.is_some(), "configured listen must be in lookup");
        assert_eq!(entry.unwrap().id, 1);
    }
}

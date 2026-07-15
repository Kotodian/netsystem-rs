//! Dynamic `tcp` plugin (`libhammer_plugin_tcp`).

pub use hammer_core::protocol::tcp::{TcpInputFlags, TcpSeq, TcpState};

hammer_component_macros::declare_plugin!(name = "tcp", load_after = ["ip"]);

use std::mem::transmute;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_core::config::{Config, SessionBackend};
use hammer_core::data_plane::{BufferPacketCursor, NodeHandle, NodeId, NodeState, SecondaryOpaque};
use hammer_core::error::{CoreError, CoreResult, HammerResult};
use hammer_core::protocol::tcp::TcpCapabilities;
use hammer_core::protocol::tcp::{TcpControlPacketParseError, TcpError};
use hammer_core::registry::RuntimeRegistry;
use hammer_runtime::{DataPlaneRuntime, DataWorkerId, Engine, Node, NodeRuntimeData};
use thiserror::Error;

use hammer_infra::pool::Index as PoolIndex;
use hammer_runtime::app::SessionSegment;
use hammer_service::session::app::SessionAppRuntimeCreate;
use hammer_service::session::{
    SessionId, SessionQueueHandle, SessionQueueNext,
    node::{SessionQueueNode, SessionQueueOutput},
    runtime::SessionDriverRuntime,
    runtime::dispatch_registered_session_queue_once_at,
};
use hammer_service::transport::congestion::CongestionController;

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
pub mod rcv_process;
pub mod recovery;
pub mod reset;
mod sack;
pub mod segment;
pub mod syn_sent;
mod timers;
pub(crate) mod worker;

pub use connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TCP_MAX_RETRANSMIT_TIMEOUT, TCP_MIN_RETRANSMIT_TIMEOUT,
    TcpConnection, TcpRetransmitTimeoutState,
};
pub use established::{TcpEstablishedNext, TcpEstablishedNode};
pub use input::{TcpInputControlPlane, TcpInputNode, TcpInputTrace};
pub use listen::{TcpListenNext, TcpListenNode};
pub use output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TCP_FLAG_SYN,
    TcpOutputNext, TcpOutputNode,
};
pub use policy::{TcpPolicy, active_tcp_policy, publish_tcp_policy, tcp_policy};
pub use rcv_process::{TcpRcvProcessNext, TcpRcvProcessNode};
pub use recovery::{TcpRecoveryAck, TcpRecoveryState};
pub use reset::{TcpResetNext, TcpResetNode};
use segment::TcpSegment;
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};

pub use worker::TcpWorker;

#[cfg(test)]
mod session_driver_tests;

type TcpDriver<C, Seg> = SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>;

#[inline]
pub fn tcp_session<C, Seg>(
    driver: &TcpDriver<C, Seg>,
    session_id: SessionId,
) -> Option<&TcpConnection<C>>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let (_, index) = driver.sessions().session_transport(session_id)?;
    driver.transports().0.connection(index)
}

#[inline]
pub fn tcp_session_mut<C, Seg>(
    driver: &mut TcpDriver<C, Seg>,
    session_id: SessionId,
) -> Option<&mut TcpConnection<C>>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let (_, index) = driver.sessions().session_transport(session_id)?;
    driver.transports_mut().0.connection_mut(index)
}

pub fn insert_tcp_session<C, Seg, F>(
    driver: &mut TcpDriver<C, Seg>,
    create: F,
) -> CoreResult<SessionId>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    F: FnOnce(SessionId) -> TcpConnection<C>,
{
    if !driver.transports().0.has_connection_capacity() {
        return Err(CoreError::internal(
            "TCP connection pool capacity exhausted",
        ));
    }
    driver.insert_session_with_transport(
        <TcpWorker<C> as hammer_service::session::runtime::SessionTransport<PoolIndex, Seg>>::ID,
        |session_id, transports| transports.0.insert_connection(create(session_id)),
    )
}

#[inline]
pub fn mark_tcp_session_ready<C, Seg>(driver: &mut TcpDriver<C, Seg>, session_id: SessionId)
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    driver.sessions_mut().mark_ready(session_id);
}

pub fn rollback_tcp_session<C, Seg>(
    driver: &mut TcpDriver<C, Seg>,
    session_id: SessionId,
) -> CoreResult<bool>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let Some((_, index)) = driver.sessions().session_transport(session_id) else {
        return Ok(false);
    };
    driver.transports_mut().0.lookup.forget_session(session_id);
    driver
        .transports_mut()
        .0
        .lookup
        .forget_pending_open(session_id);
    let _ = driver.transports_mut().0.remove_connection(index);
    Ok(driver.sessions_mut().remove_session_entry(session_id))
}

pub struct TcpMain {
    congestion: config::CongestionController,
    control: TcpInputControlPlane,
    listeners: listener_control::TcpListenerControlHandle,
}

impl TcpMain {
    pub fn new(congestion: config::CongestionController) -> Self {
        let control = TcpInputControlPlane::new();
        let listeners = listener_control::TcpListenerControlHandle::new(control.clone());
        Self {
            congestion,
            control,
            listeners,
        }
    }

    pub fn congestion(&self) -> config::CongestionController {
        self.congestion
    }

    pub fn control(&self) -> &TcpInputControlPlane {
        &self.control
    }

    fn bind_tcp_listener(
        &self,
        bind: SocketAddr,
        owner_worker: DataWorkerId,
        capabilities: TcpCapabilities,
    ) -> HammerResult<lookup::TcpLookupId> {
        self.listeners.bind(bind, owner_worker, capabilities)
    }

    /// Register the configured `TcpInputNode<C, Seg>` in the main graph.
    ///
    /// Per-worker TCP state is bound later through node runtime data. `TcpMain`
    /// only supplies the cross-worker control plane shared by every clone.
    fn register_tcp_input<C, Seg>(
        &self,
        runtime: &DataPlaneRuntime,
        queue: SessionQueueHandle<SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>>,
        handoff: Option<(NodeHandle, DataWorkerId)>,
    ) -> CoreResult<NodeId>
    where
        C: CongestionController + 'static,
        Seg: SessionSegment,
        hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let next = [NodeId::new(0); TcpInputNext::COUNT];
        let node = self.control.node::<C, Seg>(next, Some(queue), handoff);
        runtime
            .nodes()
            .try_register_internal_with_next_names(node, &TcpInputNext::NEXT_NAMES)
    }
}

// VPP alignment: `tcp_main_t tcp_main;` is a file-scope global in VPP's
// `tcp.c`; nodes read it via `&tcp_main` (lock-free direct deref). `tcp_init`
// fills it once and `vlib_test_cleanup` resets it between tests. The Rust
// mirror is a `pub static ArcSwapOption<TcpMain>`: `.load()` is lock-free on
// the hot path, and `store(None)` makes it resettable for test isolation —
// neither of which `OnceLock` provides.
pub static TCP_MAIN: ArcSwapOption<TcpMain> = ArcSwapOption::const_empty();

#[cfg(test)]
pub fn reset_for_test() {
    TCP_MAIN.store(None);
    policy::reset_tcp_policy_for_test();
}

pub fn init(reg: &RuntimeRegistry) -> HammerResult<()> {
    let config = reg.require::<Config>()?;
    let main = configured_tcp_main(config.as_ref())?;
    TCP_MAIN.store(Some(Arc::new(main)));
    Ok(())
}

#[hammer_component_macros::init_function(
    name = "tcp_init",
    runs_after = ["transport_init"],
    runs_before = ["install_packet_graph"]
)]
fn init_tcp(engine: &mut Engine, config: Arc<Config>) -> HammerResult<()> {
    let main = configured_tcp_main(config.as_ref())?;
    register_configured_main_graph(
        &engine.runtime,
        &main,
        configured_session_backend(config.as_ref()),
    )?;
    TCP_MAIN.store(Some(Arc::new(main)));
    Ok(())
}

fn configured_tcp_main(config: &Config) -> HammerResult<TcpMain> {
    let tcp = config.plugin_config::<crate::config::TcpPluginConfig>("tcp")?;
    tcp.validate()?;
    publish_tcp_policy(TcpPolicy::from_plugin_config(&tcp));
    let main = TcpMain::new(tcp.congestion);
    for entry in &tcp.listen {
        main.bind_tcp_listener(
            entry.address,
            DataWorkerId::new(0),
            TcpCapabilities::default(),
        )?;
    }
    Ok(main)
}

fn configured_session_backend(config: &Config) -> SessionBackend {
    config
        .network
        .session
        .as_ref()
        .map(|session| session.backend)
        .unwrap_or_default()
}

pub fn register_tcp_input(runtime: &DataPlaneRuntime, _: usize) -> CoreResult<NodeId> {
    runtime
        .nodes()
        .node_by_name("tcp-input")
        .ok_or_else(|| CoreError::internal("TCP worker graph is not registered"))
}

fn register_typed_main_graph<C, Seg>(runtime: &DataPlaneRuntime, main: &TcpMain) -> CoreResult<()>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    // The main graph carries only the monomorphized process function and edge
    // shape. Its Session Queue stays disabled until each worker binds a driver.
    let queue = SessionQueueHandle::<TcpDriver<C, Seg>>::new(NodeRuntimeData::empty());
    let tcp_output = output::register_tcp_output(runtime, 0)?;
    let session_queue = hammer_service::session::node::register_session_queue_node(runtime, 0)?;
    main.register_tcp_input::<C, Seg>(runtime, queue, None)?;
    let control = main.control().clone();
    runtime.nodes().try_register_internal_with_next_names(
        TcpListenNode::<C, Seg>::new(control, queue, [NodeId::new(0); TcpListenNext::COUNT]),
        &TcpListenNext::NEXT_NAMES,
    )?;
    runtime.nodes().try_register_internal_with_next_names(
        TcpEstablishedNode::<C, Seg>::new(queue, [NodeId::new(0); TcpEstablishedNext::COUNT]),
        &TcpEstablishedNext::NEXT_NAMES,
    )?;
    runtime.nodes().try_register_internal_with_next_names(
        TcpRcvProcessNode::<C, Seg>::new(queue, [NodeId::new(0); TcpRcvProcessNext::COUNT]),
        &TcpRcvProcessNext::NEXT_NAMES,
    )?;
    runtime.nodes().try_register_internal_with_next_names(
        TcpSynSentNode::<C, Seg>::new(queue, [NodeId::new(0); TcpSynSentNext::COUNT]),
        &TcpSynSentNext::NEXT_NAMES,
    )?;
    SessionQueueNode::attach_queue_by_runtime_data(
        runtime,
        session_queue,
        SessionQueueNode::registered_runtime_data()?,
        queue,
        tcp_output,
        dispatch_registered_session_queue_once_at::<(TcpWorker<C>, ()), Seg, PoolIndex>,
    )?;
    runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    Ok(())
}

fn register_configured_main_graph(
    runtime: &DataPlaneRuntime,
    main: &TcpMain,
    backend: SessionBackend,
) -> CoreResult<()> {
    use crate::config::CongestionController as CongestionKind;
    use hammer_service::transport::congestion::BbrController;

    let congestion = main.congestion();
    match congestion {
        CongestionKind::Bbr => match backend {
            SessionBackend::Local => {
                type C = BbrController;
                type Seg = hammer_infra::segment::Local;
                register_typed_main_graph::<C, Seg>(runtime, main)?;
            }
            SessionBackend::Svm => {
                type C = BbrController;
                type Seg = hammer_infra::segment::Svm;
                register_typed_main_graph::<C, Seg>(runtime, main)?;
            }
        },
    }
    Ok(())
}

fn bind_typed_worker_graph<C, Seg>(engine: &mut Engine, driver: TcpDriver<C, Seg>) -> CoreResult<()>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let worker = engine.data_worker_id()?;
    let handoff = engine.runtime.handoff_node_handle()?;
    let session_queue = engine
        .runtime
        .node_by_name("session-queue")
        .ok_or_else(|| CoreError::internal("session-queue is not registered"))?;
    let tcp_output = engine
        .runtime
        .node_by_name(TcpOutputNode::NODE_NAME)
        .ok_or_else(|| CoreError::internal("tcp-output is not registered"))?;
    let tcp_input = engine
        .runtime
        .node_by_name("tcp-input")
        .ok_or_else(|| CoreError::internal("tcp-input is not registered"))?;
    let tcp_listen = engine
        .runtime
        .node_by_name("tcp-listen")
        .ok_or_else(|| CoreError::internal("tcp-listen is not registered"))?;
    let tcp_established = engine
        .runtime
        .node_by_name("tcp-established")
        .ok_or_else(|| CoreError::internal("tcp-established is not registered"))?;
    let tcp_rcv_process = engine
        .runtime
        .node_by_name("tcp-rcv-process")
        .ok_or_else(|| CoreError::internal("tcp-rcv-process is not registered"))?;
    let tcp_syn_sent = engine
        .runtime
        .node_by_name("tcp-syn-sent")
        .ok_or_else(|| CoreError::internal("tcp-syn-sent is not registered"))?;

    let queue = hammer_service::session::node::register_session_queue(driver)?;
    let session_queue_node = SessionQueueNode::new()?;
    let session_queue_data = session_queue_node.node_runtime_data()?;
    SessionQueueNode::attach_queue_by_runtime_data(
        &engine.runtime,
        session_queue,
        session_queue_data,
        queue,
        tcp_output,
        dispatch_registered_session_queue_once_at::<(TcpWorker<C>, ()), Seg, PoolIndex>,
    )?;

    let main_guard = TCP_MAIN.load();
    let main = main_guard
        .as_deref()
        .ok_or_else(|| CoreError::internal("tcp main not initialized"))?;
    let input_data = main
        .control()
        .node::<C, Seg>(
            [NodeId::new(0); TcpInputNext::COUNT],
            Some(queue),
            Some((handoff, worker)),
        )
        .node_runtime_data()?;
    let control = main.control().clone();
    let listen_data =
        TcpListenNode::<C, Seg>::new(control, queue, [NodeId::new(0); TcpListenNext::COUNT])
            .node_runtime_data()?;
    let established_data =
        TcpEstablishedNode::<C, Seg>::new(queue, [NodeId::new(0); TcpEstablishedNext::COUNT])
            .node_runtime_data()?;
    let rcv_process_data =
        TcpRcvProcessNode::<C, Seg>::new(queue, [NodeId::new(0); TcpRcvProcessNext::COUNT])
            .node_runtime_data()?;
    let syn_sent_data =
        TcpSynSentNode::<C, Seg>::new(queue, [NodeId::new(0); TcpSynSentNext::COUNT])
            .node_runtime_data()?;

    engine.set_worker_node_runtime_data(session_queue, session_queue_data)?;
    engine.set_worker_node_runtime_data(tcp_input, input_data)?;
    engine.set_worker_node_runtime_data(tcp_listen, listen_data)?;
    engine.set_worker_node_runtime_data(tcp_established, established_data)?;
    engine.set_worker_node_runtime_data(tcp_rcv_process, rcv_process_data)?;
    engine.set_worker_node_runtime_data(tcp_syn_sent, syn_sent_data)?;
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Polling)?;
    Ok(())
}

#[hammer_component_macros::worker_init_function(name = "tcp_worker_init")]
fn init_tcp_worker(engine: &mut Engine) -> HammerResult<()> {
    use crate::config::CongestionController as CongestionKind;
    use hammer_service::transport::congestion::BbrController;

    let worker = engine.data_worker_id()?;
    let tcp_main = TCP_MAIN.load();
    let congestion = tcp_main
        .as_deref()
        .ok_or_else(|| CoreError::internal("tcp main not initialized"))?
        .congestion();
    let config = engine.registry.require::<Config>()?;
    let backend = configured_session_backend(config.as_ref());

    match congestion {
        CongestionKind::Bbr => match backend {
            SessionBackend::Local => {
                type C = BbrController;
                type Seg = hammer_infra::segment::Local;
                let driver = TcpDriver::<C, Seg>::new(
                    worker,
                    engine.runtime.buffers().clone(),
                    (TcpWorker::new(worker), ()),
                );
                bind_typed_worker_graph::<C, Seg>(engine, driver)?;
            }
            SessionBackend::Svm => {
                type C = BbrController;
                type Seg = hammer_infra::segment::Svm;
                let driver = TcpDriver::<C, Seg>::new_svm(
                    worker,
                    engine.runtime.buffers().clone(),
                    (TcpWorker::new(worker), ()),
                    hammer_runtime::app::AppSessionConfig::default(),
                );
                bind_typed_worker_graph::<C, Seg>(engine, driver)?;
            }
        },
    }
    Ok(())
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

impl From<TcpNodeError> for CoreError {
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
) -> CoreResult<Option<SessionId>> {
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
) -> CoreResult<()> {
    if output.remaining_io_budget() == 0 {
        return Ok(());
    }
    let index = runtime.buffers().alloc_index()?;
    segment.write_to_buffer(runtime.buffers(), index)?;
    let _ = output.try_enqueue_io(frame, output_next, index)?;
    Ok(())
}

fn publish_tcp_connection<C, Seg>(
    driver: &mut SessionDriverRuntime<(TcpWorker<C>, ()), Seg, PoolIndex>,
    session_id: SessionId,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let (_, index) = driver
        .sessions()
        .session_transport(session_id)
        .ok_or(TcpNodeError::SessionMissing)?;
    let close = {
        let TcpWorker {
            connections,
            lookup,
            ..
        } = &mut driver.transports_mut().0;
        let connection = connections.get(index).ok_or(TcpNodeError::SessionMissing)?;
        lookup.publish_connection(session_id, connection)
    };
    if close {
        driver
            .sessions_mut()
            .notify_transport_closed(session_id, index)?;
        let _ = driver.transports_mut().0.remove_connection(index);
        driver
            .sessions_mut()
            .notify_transport_deleted(session_id, index);
    }
    Ok(())
}

#[cfg(test)]
#[doc(hidden)]
pub(crate) fn closing_session_for_test<C>() -> (
    SessionDriverRuntime<(TcpWorker<C>, ()), hammer_infra::segment::Local, PoolIndex>,
    SessionId,
    std::net::SocketAddr,
    std::net::SocketAddr,
)
where
    C: CongestionController + 'static,
{
    let local: std::net::SocketAddr = "192.0.2.10:443".parse().expect("local");
    let remote: std::net::SocketAddr = "198.51.100.20:50001".parse().expect("remote");
    let mut driver = SessionDriverRuntime::new(
        DataWorkerId::new(0),
        hammer_runtime::DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig {
            buffers: hammer_core::data_plane::DataPlaneBufferConfig {
                buffer_slot_capacity: 2048,
                buffer_slots: 4,
                frame_slots: 4,
                ..hammer_core::data_plane::DataPlaneBufferConfig::default()
            },
        })
        .buffers()
        .clone(),
        (TcpWorker::new(DataWorkerId::new(0)), ()),
    );
    let session_id = insert_tcp_session(&mut driver, |session_id: SessionId| {
        TcpConnection::established_for_time_wait_test(
            Some(hammer_core::protocol::tcp::TcpConnectionId::new(
                session_id.get(),
            )),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    })
    .expect("insert session");
    publish_tcp_connection(&mut driver, session_id).expect("refresh session route");
    (driver, session_id, local, remote)
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
    use std::sync::Arc;

    use hammer_core::config::Config;
    use hammer_core::config::loader::parse_config;
    use hammer_core::registry::RuntimeRegistry;

    use super::*;

    #[test]
    fn tcp_init_binds_configured_listens() {
        hammer_service::reset_subsystem_mains_for_plugin_test();
        let cfg = parse_config(
            r#"
plugins = ["tcp"]

[[plugin.tcp.listen]]
address = "10.0.0.1:7"
"#,
        )
        .expect("parse");
        let registry = RuntimeRegistry::new();
        registry.set::<Config>(Arc::new(cfg));
        init(registry.as_ref()).expect("tcp_init");

        let entry = TCP_MAIN
            .load()
            .as_deref()
            .expect("tcp main")
            .control()
            .lookup_listener(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7));
        assert!(entry.is_some(), "configured listen must be in lookup");
        assert_eq!(entry.unwrap().id, 1);
    }

    #[test]
    fn tcp_graph_backend_follows_session_config() {
        let local = parse_config("plugins = [\"tcp\"]").expect("local config");
        assert_eq!(configured_session_backend(&local), SessionBackend::Local);

        let svm = parse_config(
            r#"
plugins = ["tcp"]

[network.session]
backend = "svm"
attach_socket_path = "/tmp/hammer-session.sock"
"#,
        )
        .expect("SVM config");
        assert_eq!(configured_session_backend(&svm), SessionBackend::Svm);
    }
}

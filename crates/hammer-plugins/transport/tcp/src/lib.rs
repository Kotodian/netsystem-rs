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

use std::cell::RefCell;
use std::mem::transmute;
use std::net::SocketAddr;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_core::data_plane::{BufferPacketCursor, NodeHandle, NodeId, NodeState, SecondaryOpaque};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, Engine, Node, NodeRuntimeData, RuntimeError, RuntimeResult,
};
use thiserror::Error;

use hammer_infra::pool::Index as PoolIndex;
use hammer_infra::segment::{Local, Svm};
use hammer_runtime::app::SessionSegment;
use hammer_service::session::app::SessionAppRuntimeCreate;
use hammer_service::session::node::{SessionQueueNode, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionTransport, SessionWorker, dispatch_session_queue_pending,
};
use hammer_service::session::{SessionBackend, SessionId, SessionQueueNext};
use hammer_service::transport::congestion::{BbrController, CongestionController};

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

pub(crate) struct TcpWorkerState<C, Seg>
where
    C: CongestionController,
    Seg: SessionSegment,
{
    pub(crate) sessions: SessionWorker<PoolIndex, Seg>,
    pub(crate) tcp: TcpWorker<C>,
}

impl<C, Seg> TcpWorkerState<C, Seg>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
{
    fn new(sessions: SessionWorker<PoolIndex, Seg>, tcp: TcpWorker<C>) -> Self {
        Self { sessions, tcp }
    }
}

// VPP keeps session worker state and TCP worker state adjacent for each data
// worker. The plugin owns the TCP half; `SessionWorker` owns queue readiness
// and removes its FileMain record before this state is replaced.
thread_local! {
    static TCP_WORKER_STATE: RefCell<Option<TcpWorkerState<BbrController, Local>>> = const { RefCell::new(None) };
    static TCP_WORKER_STATE_SVM: RefCell<Option<TcpWorkerState<BbrController, Svm>>> = const { RefCell::new(None) };
}

pub(crate) trait TcpWorkerStore<C>: SessionSegment
where
    C: CongestionController + 'static,
{
    fn take_worker() -> RuntimeResult<Option<TcpWorkerState<C, Self>>>
    where
        Self: Sized;

    fn install_worker(state: &mut Option<TcpWorkerState<C, Self>>) -> RuntimeResult<()>
    where
        Self: Sized;

    fn with_worker_mut<R>(
        f: impl FnOnce(&mut TcpWorkerState<C, Self>) -> RuntimeResult<R>,
    ) -> RuntimeResult<R>
    where
        Self: Sized;
}

impl TcpWorkerStore<BbrController> for Local {
    fn take_worker() -> RuntimeResult<Option<TcpWorkerState<BbrController, Self>>> {
        TCP_WORKER_STATE.with(|slot| {
            slot.try_borrow_mut()
                .map_err(|_| RuntimeError::from(TcpError::Dispatch))
                .map(|mut state| state.take())
        })
    }

    fn install_worker(
        state: &mut Option<TcpWorkerState<BbrController, Self>>,
    ) -> RuntimeResult<()> {
        TCP_WORKER_STATE.with(|slot| {
            let mut slot = slot
                .try_borrow_mut()
                .map_err(|_| RuntimeError::from(TcpError::Dispatch))?;
            if slot.is_some() {
                return Err(RuntimeError::from(TcpError::Dispatch));
            }
            *slot = state.take();
            Ok(())
        })
    }

    fn with_worker_mut<R>(
        f: impl FnOnce(&mut TcpWorkerState<BbrController, Self>) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        TCP_WORKER_STATE.with(|slot| {
            let mut slot = slot
                .try_borrow_mut()
                .map_err(|_| RuntimeError::from(TcpError::Dispatch))?;
            f(slot
                .as_mut()
                .ok_or_else(|| RuntimeError::from(TcpError::Dispatch))?)
        })
    }
}

impl TcpWorkerStore<BbrController> for Svm {
    fn take_worker() -> RuntimeResult<Option<TcpWorkerState<BbrController, Self>>> {
        TCP_WORKER_STATE_SVM.with(|slot| {
            slot.try_borrow_mut()
                .map_err(|_| RuntimeError::from(TcpError::Dispatch))
                .map(|mut state| state.take())
        })
    }

    fn install_worker(
        state: &mut Option<TcpWorkerState<BbrController, Self>>,
    ) -> RuntimeResult<()> {
        TCP_WORKER_STATE_SVM.with(|slot| {
            let mut slot = slot
                .try_borrow_mut()
                .map_err(|_| RuntimeError::from(TcpError::Dispatch))?;
            if slot.is_some() {
                return Err(RuntimeError::from(TcpError::Dispatch));
            }
            *slot = state.take();
            Ok(())
        })
    }

    fn with_worker_mut<R>(
        f: impl FnOnce(&mut TcpWorkerState<BbrController, Self>) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        TCP_WORKER_STATE_SVM.with(|slot| {
            let mut slot = slot
                .try_borrow_mut()
                .map_err(|_| RuntimeError::from(TcpError::Dispatch))?;
            f(slot
                .as_mut()
                .ok_or_else(|| RuntimeError::from(TcpError::Dispatch))?)
        })
    }
}

pub(crate) fn with_tcp_worker_mut<C, Seg, R>(
    f: impl FnOnce(&mut TcpWorkerState<C, Seg>) -> RuntimeResult<R>,
) -> RuntimeResult<R>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
{
    Seg::with_worker_mut(f)
}

pub(crate) fn insert_tcp_session<C, Seg, F>(
    state: &mut TcpWorkerState<C, Seg>,
    create: F,
) -> RuntimeResult<SessionId>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    F: FnOnce(SessionId) -> TcpConnection<C>,
{
    let session_id = state.sessions.insert_creating_session(TcpWorker::<C>::ID)?;
    let handle = hammer_runtime::app::SessionHandle::new(
        session_id.pool_index().slot(),
        state.sessions.worker().slot() as u32,
    );
    let app_session = match state.sessions.app().create_app_session(
        handle,
        state.sessions.app_session_config(),
        state.sessions.app().tx_evt_q().clone(),
    ) {
        Ok(session) => session,
        Err(error) => {
            state.sessions.remove_session_entry(session_id);
            return Err(error);
        }
    };
    state
        .sessions
        .app_mut()
        .attach_session(session_id, app_session);
    let index = match state.tcp.insert_connection(create(session_id)) {
        Ok(index) => index,
        Err(error) => {
            state.sessions.remove_session_entry(session_id);
            return Err(error);
        }
    };
    if let Err(error) = state.sessions.finish_session_creation(session_id, index) {
        let _ = state.tcp.remove_connection(index);
        state.sessions.remove_session_entry(session_id);
        return Err(error);
    }
    Ok(session_id)
}

pub(crate) fn rollback_tcp_session<C, Seg>(
    state: &mut TcpWorkerState<C, Seg>,
    session_id: SessionId,
) -> RuntimeResult<bool>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let Some((_, index)) = state.sessions.session_transport(session_id) else {
        return Ok(false);
    };
    state.tcp.lookup.forget_session(session_id);
    state.tcp.lookup.forget_pending_open(session_id);
    let _ = state.tcp.remove_connection(index);
    Ok(state.sessions.remove_session_entry(session_id))
}

pub struct TcpMain {
    congestion: config::CongestionController,
    control: TcpInputControlPlane,
    listeners: listener_control::TcpListenerControlHandle,
    ip_output: output::IpOutputFunctions,
}

impl TcpMain {
    pub fn new(
        congestion: config::CongestionController,
        ip_output: output::IpOutputFunctions,
    ) -> Self {
        let control = TcpInputControlPlane::new();
        let listeners = listener_control::TcpListenerControlHandle::new(control.clone());
        Self {
            congestion,
            control,
            listeners,
            ip_output,
        }
    }

    pub fn congestion(&self) -> config::CongestionController {
        self.congestion
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

    /// Register the configured `TcpInputNode<C, Seg>` in the main graph.
    ///
    /// Per-worker TCP state remains in plugin-local worker storage; `TcpMain`
    /// only supplies the cross-worker control plane shared by every clone.
    fn register_tcp_input<C, Seg>(
        &self,
        runtime: &DataPlaneRuntime,
        handoff: Option<(NodeHandle, DataWorkerId)>,
    ) -> RuntimeResult<NodeId>
    where
        C: CongestionController + 'static,
        Seg: TcpWorkerStore<C>,
        hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
    {
        let node = self.control.node::<C, Seg>(handoff);
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
    runs_after = ["transport_init"],
    runs_before = ["install_packet_graph"]
)]
fn init_tcp(engine: &mut Engine, config: Arc<crate::config::TcpPluginConfig>) -> RuntimeResult<()> {
    let main = Arc::new(configured_tcp_main(
        config.as_ref(),
        output::plugin_functions(engine.plugin_main())?,
    )?);
    TCP_MAIN.store(Some(Arc::clone(&main)));
    register_configured_main_graph(
        &engine.runtime,
        main.as_ref(),
        hammer_service::transport::session_backend().unwrap_or_default(),
    )?;
    Ok(())
}

fn configured_tcp_main(
    tcp: &crate::config::TcpPluginConfig,
    ip_output: output::IpOutputFunctions,
) -> RuntimeResult<TcpMain> {
    publish_tcp_policy(TcpPolicy::from_plugin_config(tcp));
    let main = TcpMain::new(tcp.congestion, ip_output);
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
    runtime
        .nodes()
        .node_by_name("tcp-input")
        .ok_or_else(|| RuntimeError::invariant("TCP worker graph is not registered"))
}

fn register_typed_main_graph<C, Seg>(
    runtime: &DataPlaneRuntime,
    main: &TcpMain,
) -> RuntimeResult<()>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    // The main graph carries only the monomorphized process function and edge
    // shape. Its Session Queue stays disabled until each worker binds state.
    let tcp_output = output::register_tcp_output(runtime)?;
    let session_queue = hammer_service::session::node::register_session_queue_node(runtime)?;
    main.register_tcp_input::<C, Seg>(runtime, None)?;
    let control = main.control().clone();
    runtime.nodes().try_register_internal_with_next_names(
        TcpListenNode::for_worker::<C, Seg>(control),
        &TcpListenNext::NEXT_NAMES,
    )?;
    runtime.nodes().try_register_internal_with_next_names(
        TcpEstablishedNode::for_worker::<C, Seg>(),
        &TcpEstablishedNext::NEXT_NAMES,
    )?;
    runtime.nodes().try_register_internal_with_next_names(
        TcpRcvProcessNode::for_worker::<C, Seg>(),
        &TcpRcvProcessNext::NEXT_NAMES,
    )?;
    runtime.nodes().try_register_internal_with_next_names(
        TcpSynSentNode::for_worker::<C, Seg>(),
        &TcpSynSentNext::NEXT_NAMES,
    )?;
    let _ = SessionQueueNode::compile_output_next(runtime, session_queue, tcp_output)?;
    runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Disabled)?;
    Ok(())
}

fn register_configured_main_graph(
    runtime: &DataPlaneRuntime,
    main: &TcpMain,
    backend: SessionBackend,
) -> RuntimeResult<()> {
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

fn bind_typed_worker_graph<C, Seg>(
    engine: &mut Engine,
    state: TcpWorkerState<C, Seg>,
) -> RuntimeResult<()>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let worker = engine.data_worker_id()?;
    let handoff = engine.runtime.handoff_node_handle()?;
    let session_queue = engine
        .runtime
        .node_by_name("session-queue")
        .ok_or_else(|| RuntimeError::invariant("session-queue is not registered"))?;
    let tcp_output = engine
        .runtime
        .node_by_name(TcpOutputNode::NODE_NAME)
        .ok_or_else(|| RuntimeError::invariant("tcp-output is not registered"))?;
    let tcp_input = engine
        .runtime
        .node_by_name("tcp-input")
        .ok_or_else(|| RuntimeError::invariant("tcp-input is not registered"))?;
    let tcp_listen = engine
        .runtime
        .node_by_name("tcp-listen")
        .ok_or_else(|| RuntimeError::invariant("tcp-listen is not registered"))?;
    let tcp_established = engine
        .runtime
        .node_by_name("tcp-established")
        .ok_or_else(|| RuntimeError::invariant("tcp-established is not registered"))?;
    let tcp_rcv_process = engine
        .runtime
        .node_by_name("tcp-rcv-process")
        .ok_or_else(|| RuntimeError::invariant("tcp-rcv-process is not registered"))?;
    let tcp_syn_sent = engine
        .runtime
        .node_by_name("tcp-syn-sent")
        .ok_or_else(|| RuntimeError::invariant("tcp-syn-sent is not registered"))?;

    let session_queue_node = SessionQueueNode::new()?;
    let session_queue_data = session_queue_node.node_runtime_data()?;
    let session_queue_output =
        SessionQueueNode::existing_output_next(&engine.runtime, session_queue, tcp_output)?;
    SessionQueueNode::install_worker_attachment(
        session_queue_data,
        session_queue_output,
        tcp_session_queue_dispatch::<C, Seg>,
    )?;
    let main_guard = TCP_MAIN.load();
    let main = main_guard
        .as_deref()
        .ok_or_else(|| RuntimeError::invariant("tcp main not initialized"))?;
    let input_data = main
        .control()
        .node::<C, Seg>(Some((handoff, worker)))
        .node_runtime_data()?;
    let control = main.control().clone();
    let listen_data = TcpListenNode::for_worker::<C, Seg>(control).node_runtime_data()?;
    let established_data = TcpEstablishedNode::for_worker::<C, Seg>().node_runtime_data()?;
    let rcv_process_data = TcpRcvProcessNode::for_worker::<C, Seg>().node_runtime_data()?;
    let syn_sent_data = TcpSynSentNode::for_worker::<C, Seg>().node_runtime_data()?;

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

    // Keep the old record live until the replacement has registered its own
    // descriptor. The queue node stays disabled throughout this transition,
    // so neither record can dispatch while ownership changes hands.
    let mut replacement = Some(state);
    let Some(replacement_state) = replacement.as_mut() else {
        return Err(RuntimeError::from(TcpError::Dispatch));
    };
    replacement_state
        .sessions
        .install_queue_readiness(engine, session_queue)?;

    let previous = match Seg::take_worker() {
        Ok(previous) => previous,
        Err(error) => {
            let Some(replacement_state) = replacement.as_mut() else {
                return Err(error);
            };
            replacement_state.sessions.remove_queue_readiness(engine)?;
            return Err(error);
        }
    };
    if let Some(mut previous) = previous {
        if let Err(error) = previous.sessions.remove_queue_readiness(engine) {
            let Some(replacement_state) = replacement.as_mut() else {
                return Err(error);
            };
            replacement_state.sessions.remove_queue_readiness(engine)?;
            let mut restore = Some(previous);
            Seg::install_worker(&mut restore)?;
            return Err(error);
        }
    }
    if let Err(error) = Seg::install_worker(&mut replacement) {
        let Some(replacement_state) = replacement.as_mut() else {
            return Err(error);
        };
        replacement_state.sessions.remove_queue_readiness(engine)?;
        return Err(error);
    }
    engine
        .runtime
        .nodes()
        .set_node_state(session_queue, NodeState::Polling)?;
    Ok(())
}

#[hammer_component_macros::worker_init_function(name = "tcp_worker_init")]
fn init_tcp_worker(engine: &mut Engine) -> RuntimeResult<()> {
    use crate::config::CongestionController as CongestionKind;
    use hammer_service::transport::congestion::BbrController;

    let worker = engine.data_worker_id()?;
    let tcp_main = TCP_MAIN.load();
    let congestion = tcp_main
        .as_deref()
        .ok_or_else(|| RuntimeError::invariant("tcp main not initialized"))?
        .congestion();
    let backend = hammer_service::transport::session_backend().unwrap_or_default();

    match congestion {
        CongestionKind::Bbr => match backend {
            SessionBackend::Local => {
                type C = BbrController;
                type Seg = hammer_infra::segment::Local;
                let state = TcpWorkerState::new(
                    SessionWorker::new(worker, engine.runtime.buffers().clone()),
                    TcpWorker::new(worker),
                );
                bind_typed_worker_graph::<C, Seg>(engine, state)?;
            }
            SessionBackend::Svm => {
                type C = BbrController;
                type Seg = hammer_infra::segment::Svm;
                let state = TcpWorkerState::new(
                    SessionWorker::new_svm(
                        worker,
                        engine.runtime.buffers().clone(),
                        hammer_runtime::app::AppSessionConfig::default(),
                    ),
                    TcpWorker::new(worker),
                );
                bind_typed_worker_graph::<C, Seg>(engine, state)?;
            }
        },
    }
    Ok(())
}

fn tcp_session_queue_dispatch<C, Seg>(
    runtime: &DataPlaneRuntime,
    _: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: std::time::Instant,
    frame: &mut hammer_core::data_plane::BufferFrame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()>
where
    C: CongestionController + 'static,
    Seg: TcpWorkerStore<C>,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    with_tcp_worker_mut::<C, Seg, _>(|state| {
        dispatch_session_queue_pending(
            runtime,
            &mut state.sessions,
            &mut state.tcp,
            output_next,
            frame,
            output,
            now,
        )
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

fn publish_tcp_connection<C, Seg>(
    state: &mut TcpWorkerState<C, Seg>,
    session_id: SessionId,
) -> RuntimeResult<()>
where
    C: CongestionController + 'static,
    Seg: SessionSegment,
    hammer_service::session::SessionAppRuntime<Seg>: SessionAppRuntimeCreate<Seg>,
{
    let (_, index) = state
        .sessions
        .session_transport(session_id)
        .ok_or(TcpNodeError::SessionMissing)?;
    let close = {
        let TcpWorker {
            connections,
            lookup,
            ..
        } = &mut state.tcp;
        let connection = connections.get(index).ok_or(TcpNodeError::SessionMissing)?;
        lookup.publish_connection(session_id, connection)
    };
    if close {
        state.sessions.notify_transport_closed(session_id, index)?;
        let _ = state.tcp.remove_connection(index);
        state.sessions.notify_transport_deleted(session_id, index);
    }
    Ok(())
}

#[cfg(test)]
#[doc(hidden)]
pub(crate) fn closing_session_for_test<C>() -> (
    TcpWorkerState<C, Local>,
    SessionId,
    std::net::SocketAddr,
    std::net::SocketAddr,
)
where
    C: CongestionController + 'static,
{
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
    let mut state = TcpWorkerState::new(
        SessionWorker::new(worker, runtime.buffers().clone()),
        TcpWorker::new(worker),
    );
    let session_id = insert_tcp_session(&mut state, |session_id: SessionId| {
        TcpConnection::established_for_time_wait_test(
            Some(crate::TcpConnectionId::new(session_id.get())),
            worker,
            local.port(),
            Some(local),
            remote,
        )
    })
    .expect("insert session");
    publish_tcp_connection(&mut state, session_id).expect("refresh session route");
    (state, session_id, local, remote)
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
        std_types::{RSlice, RSliceMut},
    };
    use hammer_runtime::IpOutput;
    use hammer_runtime::RuntimeRegistry;
    use hammer_runtime::{DataPlaneRuntime, DataPlaneRuntimeConfig, Engine};

    use super::*;

    struct TestIpOutput;

    impl IpOutput for TestIpOutput {
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
        hammer_service::reset_subsystem_mains_for_plugin_test();
        reset_for_test();
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
        let main = configured_tcp_main(config.as_ref(), RRef::new(&TEST_IP_OUTPUT))
            .expect("build configured TCP main");
        let entry = main
            .control()
            .lookup_listener(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 7));
        assert!(entry.is_some(), "configured listen must be in lookup");
        assert_eq!(entry.unwrap().id, 1);
    }

    #[test]
    fn tcp_graph_backend_follows_session_config() {
        hammer_service::reset_subsystem_mains_for_plugin_test();
        reset_for_test();
        let mut local = test_engine();
        local
            .configure_early(
                r#"
[network.session]
backend = "local"
"#,
            )
            .expect("dispatch local session config");
        assert_eq!(
            hammer_service::transport::session_backend(),
            Some(SessionBackend::Local)
        );

        hammer_service::reset_subsystem_mains_for_plugin_test();
        reset_for_test();
        let mut svm = test_engine();
        svm.configure_early(
            r#"
[network.session]
backend = "svm"
attach_socket_path = "/tmp/hammer-session.sock"
"#,
        )
        .expect("dispatch SVM session config");
        assert_eq!(
            hammer_service::transport::session_backend(),
            Some(SessionBackend::Svm)
        );
    }
}

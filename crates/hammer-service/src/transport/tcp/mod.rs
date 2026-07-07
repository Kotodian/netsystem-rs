pub use hammer_core::protocol::tcp::{TcpInputFlags, TcpSeq, TcpState};

use std::mem::transmute;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use hammer_adapter::{
    BufferPacketCursor, DataPlaneRuntime, DataWorkerId, NodeId, NodeRuntimeData, NodeState,
    SecondaryOpaque,
};
use hammer_core::error::{CoreError, CoreResult, HammerResult};
#[cfg(test)]
use hammer_core::protocol::tcp::{TcpCapabilities, TcpFastOpenCookie};
use hammer_core::protocol::tcp::{TcpControlPacketParseError, TcpError};
use hammer_core::registry::RuntimeRegistry;
use thiserror::Error;

use crate::session::{
    SessionId, SessionQueueHandle, SessionQueueNext,
    node::{SessionQueueNode, SessionQueueOutput},
    protocol::SessionQueueControlContext,
    runtime::SessionDriverRuntime,
    runtime::dispatch_registered_session_queue_once_at,
    runtime::{SessionQueueProtocol, TransportSendFlags, TransportSendParams, TxBatchBuffer},
};
use crate::transport::congestion::CongestionController;

pub mod congestion;
pub mod connection;
pub mod established;
pub mod input;
pub mod listen;
pub mod lookup;
pub mod output;
pub mod rcv_process;
pub mod recovery;
pub mod reset;
mod sack;
pub mod segment;
pub mod syn_sent;

pub use connection::{
    TCP_INITIAL_RETRANSMIT_TIMEOUT, TCP_MAX_RETRANSMIT_TIMEOUT, TCP_MIN_RETRANSMIT_TIMEOUT,
    TCP_TIMER_COUNT, TCP_TIMER_DELAYED_ACK, TCP_TIMER_KEEP_ALIVE, TCP_TIMER_PACING,
    TCP_TIMER_PERSIST, TCP_TIMER_RACK, TCP_TIMER_RETRANSMIT, TCP_TIMER_TIME_WAIT, TCP_TIMER_TLP,
    TcpConnection,
    TcpRetransmitTimeoutState,
};
pub(crate) use connection::{sync_all_tcp_timers, sync_tcp_timer};
pub use established::{TcpEstablishedNext, TcpEstablishedNode};
pub use input::{TcpInputControlPlane, TcpInputNode, TcpInputTrace};
pub use listen::{TcpListenNext, TcpListenNode};
pub use output::{
    DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, TCP_FLAG_ACK, TCP_FLAG_FIN, TCP_FLAG_PSH, TCP_FLAG_SYN,
    TcpOutputNext, TcpOutputNode,
};
pub use rcv_process::{TcpRcvProcessNext, TcpRcvProcessNode};
pub use recovery::{TcpRecoveryAck, TcpRecoveryState};
pub use reset::{TcpResetNext, TcpResetNode};
use segment::TcpSegment;
pub use syn_sent::{TcpSynSentNext, TcpSynSentNode};

#[cfg(test)]
pub(crate) use lookup::set_tcp_worker_state;
pub(crate) use lookup::{
    TcpWorkerOwnedState, install_tcp_worker_state, tcp_worker_state, tcp_worker_state_mut,
};

pub(crate) type TcpSessionDriver<C> = SessionDriverRuntime<TcpConnection<C>>;
pub(crate) type TcpQueue<C> = SessionQueueHandle<TcpSessionDriver<C>>;

pub struct TcpMain {
    control: TcpInputControlPlane,
}

impl TcpMain {
    pub fn new() -> Self {
        Self {
            control: TcpInputControlPlane::new(),
        }
    }

    pub fn control(&self) -> &TcpInputControlPlane {
        &self.control
    }

    /// Build and register this worker's `TcpInputNode<C>`.
    ///
    /// Per-worker state (TCP owned state + the session-queue runtime data)
    /// lives in the worker thread's `TcpWorkerOwnedState` TLS, set up when the
    /// worker starts — never shared across workers, so no synchronization is
    /// needed. `TcpMain` itself only holds the cross-worker control plane
    /// (`TcpInputControlPlane`, internally `Arc<ArcSwap>`).
    pub fn register_tcp_input<C: CongestionController + 'static>(
        &self,
        rt: &DataPlaneRuntime,
        worker: usize,
    ) -> CoreResult<NodeId> {
        let worker_id = DataWorkerId::new(
            u32::try_from(worker)
                .map_err(|_| CoreError::internal("worker index does not fit into u32"))?,
        );

        let runtime_data = ensure_tcp_session_queue::<C>(rt, worker)?;
        let queue = TcpQueue::<C>::new(runtime_data);
        let handoff = rt.handoff_node_handle()?;
        let next = [NodeId::new(0); TcpInputNext::COUNT];
        let node = self
            .control
            .node::<C>(next, Some(queue), Some((handoff, worker_id)));
        rt.nodes()
            .try_register_internal_with_next_names(node, &TcpInputNext::NEXT_NAMES)
    }
}

pub(crate) fn ensure_tcp_session_queue<C: CongestionController + 'static>(
    rt: &DataPlaneRuntime,
    worker: usize,
) -> CoreResult<NodeRuntimeData> {
    if let Some(data) = tcp_worker_state().queue_runtime_data() {
        return Ok(data);
    }

    let worker_id = DataWorkerId::new(
        u32::try_from(worker)
            .map_err(|_| CoreError::internal("worker index does not fit into u32"))?,
    );

    let backend = crate::transport::session_backend()
        .ok_or_else(|| CoreError::internal("transport main not initialized"))?;

    let runtime_data = match backend {
        hammer_core::config::SessionBackend::Local => {
            type Seg = hammer_infra::segment::Local;
            let queue = crate::session::node::register_session_queue(SessionDriverRuntime::<
                TcpConnection<C>,
                Seg,
            >::new(
                worker_id,
                rt.buffers().clone(),
            ))?;
            queue.runtime_data()
        }
        hammer_core::config::SessionBackend::Svm => {
            type Seg = hammer_infra::segment::Svm;
            let queue = crate::session::node::register_session_queue(SessionDriverRuntime::<
                TcpConnection<C>,
                Seg,
            >::new_svm(
                worker_id,
                rt.buffers().clone(),
                hammer_runtime::app::AppSessionConfig::default(),
            ))?;
            queue.runtime_data()
        }
    };

    tcp_worker_state_mut().set_queue_runtime_data(runtime_data);
    Ok(runtime_data)
}

// VPP alignment: `tcp_main_t tcp_main;` is a file-scope global in VPP's
// `tcp.c`; nodes read it via `&tcp_main` (lock-free direct deref). `tcp_init`
// fills it once and `vlib_test_cleanup` resets it between tests. The Rust
// mirror is a `pub static ArcSwapOption<TcpMain>`: `.load()` is lock-free on
// the hot path, and `store(None)` makes it resettable for test isolation —
// neither of which `OnceLock` provides.
pub static TCP_MAIN: ArcSwapOption<TcpMain> = ArcSwapOption::const_empty();

#[cfg(test)]
pub(crate) fn reset_for_test() {
    TCP_MAIN.store(None);
}

pub fn init(_reg: &RuntimeRegistry) -> HammerResult<()> {
    TCP_MAIN.store(Some(Arc::new(TcpMain::new())));
    Ok(())
}

#[hammer_component_macros::init_function(name = "tcp_init")]
fn init_tcp(engine: &mut hammer_runtime::Engine) -> HammerResult<()> {
    init(&engine.registry)
}

pub fn register_tcp_input(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<NodeId> {
    crate::with_congestion!(|C| {
        TCP_MAIN
            .load()
            .as_deref()
            .ok_or_else(|| CoreError::internal("tcp main not initialized"))?
            .register_tcp_input::<C>(runtime, worker)
    })
}

pub fn wire_worker_graph(runtime: &DataPlaneRuntime, worker: usize) -> CoreResult<()> {
    crate::with_congestion!(|C| {
        crate::with_segment!(|Seg| {
            let queue_data = ensure_tcp_session_queue::<C>(runtime, worker)?;
            let queue = TcpQueue::<C>::new(queue_data);
            let session_queue = runtime
                .nodes()
                .node_by_name("session-queue")
                .ok_or_else(|| CoreError::internal("session-queue not registered"))?;
            let tcp_output = runtime
                .nodes()
                .node_by_name("tcp-output")
                .ok_or_else(|| CoreError::internal("tcp-output not registered"))?;
            SessionQueueNode::attach_queue_by_runtime_data(
                SessionQueueNode::registered_runtime_data()?,
                queue,
                tcp_output.into(),
                dispatch_registered_session_queue_once_at::<TcpConnection<C>, Seg>,
            )?;
            runtime
                .nodes()
                .set_node_state(session_queue, NodeState::Polling)?;
            Ok(())
        })
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
    index: hammer_adapter::BufferIndex,
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
    output_next: SessionQueueNext,
    output: &mut SessionQueueOutput,
    segment: TcpSegment,
) -> CoreResult<()> {
    let mut owner = runtime.buffers().get_next_frame(output_next.node())?;
    let index = runtime.buffers().alloc_index()?;
    owner.push_index(index)?;
    segment.write_to_buffer(runtime.buffers(), index)?;
    output.enqueue_frame(runtime, owner)?;
    Ok(())
}

fn publish_tcp_connection<C>(
    driver: &mut TcpSessionDriver<C>,
    session_id: SessionId,
) -> CoreResult<()>
where
    C: CongestionController + 'static,
{
    let connection = driver
        .session(session_id)
        .ok_or(TcpNodeError::SessionMissing)? as *const TcpConnection<C>;
    let close = tcp_worker_state_mut().publish_connection(session_id, unsafe { &*connection });
    if close {
        let _ = driver.close_session(session_id)?;
    }
    Ok(())
}

#[cfg(test)]
fn connect_tcp_session<C>(
    driver: &mut TcpSessionDriver<C>,
    local: std::net::SocketAddr,
    remote: std::net::SocketAddr,
) -> CoreResult<SessionId>
where
    C: CongestionController + 'static,
{
    let owner = driver.worker();
    let initial_sequence = tcp_worker_state_mut().next_initial_sequence(local, remote);
    let cached_fast_open: Option<(TcpFastOpenCookie, Option<u16>)> =
        tcp_worker_state().fast_open_cookie(local, remote);
    let session_id = driver.insert_session_with_id(|session_id: SessionId| {
        let connection_id = hammer_core::protocol::tcp::TcpConnectionId::new(session_id.get());
        let mut connection = TcpConnection::new(
            Some(connection_id),
            owner,
            local.port(),
            Some(local),
            remote,
        );
        if let Some((cookie, max_segment_size)) = cached_fast_open {
            connection.set_fast_open_cookie(Some(cookie));
        }
        connection.connect_state(initial_sequence);
        connection
    })?;
    tcp_worker_state_mut().remember_pending_open(
        session_id,
        Some(local),
        remote,
        owner,
        TcpInputNext::SynSent,
        TcpCapabilities {
            max_segment_size: cached_fast_open.and_then(|(_, max_segment_size)| max_segment_size),
            window_scale: None,
            sack: false,
            timestamps: false,
            ecn: false,
            accurate_ecn: false,
            fast_open: cached_fast_open.is_some(),
        },
    );
    if let Some(connection) = driver.session_mut(session_id) {
        connection.timer_set(TCP_TIMER_RETRANSMIT);
    }
    driver.mark_ready(session_id);
    Ok(session_id)
}

#[cfg(test)]
#[doc(hidden)]
pub(crate) fn closing_session_for_test<C>() -> (
    SessionDriverRuntime<TcpConnection<C>>,
    SessionId,
    std::net::SocketAddr,
    std::net::SocketAddr,
)
where
    C: CongestionController + 'static,
{
    let local: std::net::SocketAddr = "192.0.2.10:443".parse().expect("local");
    let remote: std::net::SocketAddr = "198.51.100.20:50001".parse().expect("remote");
    let worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
    install_tcp_worker_state(worker_state);
    let mut driver = SessionDriverRuntime::new(
        DataWorkerId::new(0),
        hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
            buffers: hammer_adapter::DataPlaneBufferConfig {
                buffer_slot_capacity: 2048,
                buffer_slots: 4,
                frame_capacity: 4,
                frame_slots: 4,
                ..hammer_adapter::DataPlaneBufferConfig::default()
            },
        })
        .buffers()
        .clone(),
    );
    let session_id = driver
        .insert_session_with_id(|session_id: SessionId| {
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

impl<C> SessionQueueProtocol for TcpConnection<C>
where
    C: CongestionController + 'static,
{
    fn send_params(
        &mut self,
        context: &mut SessionQueueControlContext,
        pending_len: usize,
        now: std::time::Instant,
    ) -> CoreResult<TransportSendParams> {
        let start = if self.state() == TcpState::SynSent {
            self.iss()
        } else {
            self.snd_una()
        };
        let tx_offset =
            usize::try_from(TcpSeq::from(start).distance_to(self.tx_payload_sequence()))
                .map_err(|_| TcpNodeError::TxOffsetOverflow)?;
        let pending_len = pending_len.saturating_sub(tx_offset);
        let snd_space = self.tx_payload_budget(
            pending_len,
            now,
            tcp_worker_state()
                .pending_open_capabilities(context.session_id())
                .unwrap_or_default(),
        );
        Ok(TransportSendParams {
            snd_space,
            tx_offset,
            send_goal_size: self.send_goal_size(),
            flags: TransportSendFlags::default(),
        })
    }

    fn handle_expired_timer(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut SessionQueueControlContext,
        timer_id: u32,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<bool> {
        let local_capabilities = tcp_worker_state()
            .pending_open_capabilities(context.session_id())
            .unwrap_or_default();
        let session = context.session_id().pool_index();
        let now = std::time::Instant::now();
        let control = self.on_tcp_timer_expiry(timer_id, local_capabilities);
        sync_tcp_timer(context.timer_wheel(), self, session, timer_id, now)?;
        if let Some(segment) = control {
            if segment.payload_len() == 0 {
                enqueue_tcp_segment(runtime, output_next, output, segment)?;
                sync_all_tcp_timers(context.timer_wheel(), self, session, now)?;
            } else {
                context.mark_ready();
            }
        } else if matches!(
            timer_id,
            TCP_TIMER_RETRANSMIT
                | TCP_TIMER_RACK
                | TCP_TIMER_TLP
                | TCP_TIMER_PERSIST
                | TCP_TIMER_PACING
        ) {
            context.mark_ready();
        }
        Ok(self.state() == TcpState::Closed)
    }

    fn handle_ready_session(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut SessionQueueControlContext,
        close_requested: bool,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
    ) -> CoreResult<bool> {
        if close_requested {
            self.on_session_close();
        }
        let now = std::time::Instant::now();
        let _ = self.custom_tx(runtime, context, output_next, output, 1, now)?;
        Ok(self.state() == TcpState::Closed)
    }

    fn push_header(
        &mut self,
        context: &mut SessionQueueControlContext,
        batch: &[TxBatchBuffer],
        now: std::time::Instant,
    ) -> CoreResult<()> {
        let local_capabilities = tcp_worker_state()
            .pending_open_capabilities(context.session_id())
            .unwrap_or_default();
        for entry in batch {
            let segment = self.tx_segment(entry.payload_len, local_capabilities)?;
            segment.write_to_buffer(context.buffers(), entry.index)?;
            let _ = self.commit_payload_tx(entry.payload_len, now)?;
        }
        let session = context.session_id().pool_index();
        sync_all_tcp_timers(
            context.timer_wheel(),
            self,
            session,
            std::time::Instant::now(),
        )?;
        Ok(())
    }

    fn custom_tx(
        &mut self,
        runtime: &DataPlaneRuntime,
        context: &mut SessionQueueControlContext,
        output_next: SessionQueueNext,
        output: &mut SessionQueueOutput,
        _: usize,
        now: std::time::Instant,
    ) -> CoreResult<usize> {
        let segment = self.on_tcp_ready(
            context.has_pending_tx(),
            tcp_worker_state()
                .pending_open_capabilities(context.session_id())
                .unwrap_or_default(),
        );
        let mut emitted = 0;
        if let Some(segment) = segment {
            if segment.payload_len() == 0 {
                enqueue_tcp_segment(runtime, output_next, output, segment)?;
                emitted = 1;
            } else {
                context.mark_ready();
            }
        }
        let session = context.session_id().pool_index();
        sync_all_tcp_timers(
            context.timer_wheel(),
            self,
            session,
            now,
        )?;
        if emitted == 0 && !context.has_pending_tx() && self.has_pending_sack_output() {
            context.mark_ready();
        }
        Ok(emitted)
    }

    fn on_close(&mut self, context: &mut SessionQueueControlContext) {
        let session_id = context.session_id();
        tcp_worker_state_mut().forget_session(session_id);
        tcp_worker_state_mut().forget_pending_open(session_id);
    }
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
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex, OnceLock};
    use std::time::Instant;

    use hammer_adapter::{
        BufferFrame, DataPlaneRuntime, DataWorkerId, InternalNode, Node, NodeHandle, NodeId,
        NodeProcessFn, NodeRegistration, NodeResult, NodeRuntimeData,
    };
    use hammer_core::config::network::CongestionController as ConfigCongestionController;
    use hammer_core::error::{CoreError, CoreResult};
    use hammer_core::protocol::tcp::{
        TcpCapabilities, TcpConnectionId, TcpPacket, TcpSackBlock, TcpSegmentFlags, TcpSeq,
    };

    use super::*;
    use crate::session::SessionId;
    use crate::session::runtime::{
        SessionDriverRuntime, dispatch_session_queue_for_ticks, dispatch_session_queue_pending,
    };
    use crate::transport::congestion::BbrController;

    #[derive(Default)]
    struct CaptureState {
        packets: std::vec::Vec<std::vec::Vec<u8>>,
    }

    struct CaptureNode {
        runtime_data: NodeRuntimeData,
    }

    impl CaptureNode {
        fn new(state: Arc<Mutex<CaptureState>>) -> Self {
            let mut states = capture_states().lock().expect("capture registry");
            let slot = states.len();
            states.push(state);
            Self {
                runtime_data: NodeRuntimeData::from_usize(slot).expect("capture slot"),
            }
        }
    }

    impl Node for CaptureNode {
        fn process(&mut self, _: &DataPlaneRuntime, _: &mut BufferFrame) -> NodeResult {
            NodeResult::drop()
        }

        fn node_process(&self) -> NodeProcessFn {
            capture_process
        }

        fn node_runtime_data(&self) -> CoreResult<NodeRuntimeData> {
            Ok(self.runtime_data)
        }
    }

    impl InternalNode for CaptureNode {
        fn node_registration(&self) -> NodeRegistration
        where
            Self: Sized,
        {
            NodeRegistration::Plain
        }
    }

    fn capture_states() -> &'static Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>> {
        static STATES: OnceLock<Mutex<std::vec::Vec<Arc<Mutex<CaptureState>>>>> = OnceLock::new();
        STATES.get_or_init(|| Mutex::new(std::vec::Vec::new()))
    }

    fn capture_process(
        runtime: &DataPlaneRuntime,
        data: NodeRuntimeData,
        frame: &mut BufferFrame,
    ) -> NodeResult {
        let slot = match data.usize_word(0) {
            Ok(s) => s,
            Err(_) => return NodeResult::drop(),
        };
        let state = {
            let states = capture_states().lock().expect("capture registry");
            match states.get(slot) {
                Some(s) => Arc::clone(s),
                None => return NodeResult::drop(),
            }
        };
        let mut state = state.lock().expect("capture state");
        for &index in frame.pending_indices() {
            let packet = match runtime.get_buffer(index) {
                Ok(buf) => buf.current().to_vec(),
                Err(_) => return NodeResult::drop(),
            };
            state.packets.push(packet);
        }
        NodeResult::drop()
    }

    fn tcp_output_graph(
        runtime: &DataPlaneRuntime,
    ) -> (NodeId, Arc<Mutex<CaptureState>>, Arc<Mutex<CaptureState>>) {
        let lookup_state = Arc::new(Mutex::new(CaptureState::default()));
        let drop_state = Arc::new(Mutex::new(CaptureState::default()));
        let lookup = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&lookup_state)));
        let drop = runtime
            .nodes()
            .register_internal(CaptureNode::new(Arc::clone(&drop_state)));
        let output = runtime
            .nodes()
            .register_internal(TcpOutputNode::new(TcpOutputNext::nodes(drop, lookup)));
        (output, lookup_state, drop_state)
    }

    fn established_tcp_connection() -> TcpConnection<BbrController> {
        let local: SocketAddr = "192.0.2.10:443".parse().expect("local");
        let remote: SocketAddr = "198.51.100.20:50001".parse().expect("remote");
        TcpConnection::established_with_sack_for_test(
            Some(TcpConnectionId::new(1)),
            DataWorkerId::new(0),
            local.port(),
            Some(local),
            remote,
        )
    }

    fn expire_tcp_timers(
        runtime: &DataPlaneRuntime,
        driver: &mut SessionDriverRuntime<TcpConnection<BbrController>>,
        ticks: u32,
        output_node: NodeId,
    ) {
        let next: crate::session::SessionQueueNext = output_node.into();
        let _ = dispatch_session_queue_for_ticks(runtime, driver, ticks, next)
            .expect("dispatch session queue for timer ticks");
        let _ = dispatch_session_queue_for_ticks(runtime, driver, 0, next)
            .expect("dispatch session queue follow-up");
        let _ = runtime.run_ready_nodes().expect("run timer output");
    }

    fn tcp_segment_payload(packet: &[u8]) -> &[u8] {
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        &packet[segment.header_len()..]
    }

    fn tcp_ack_number(packet: &[u8]) -> u32 {
        etherparse::TcpSlice::from_slice(packet)
            .expect("tcp segment")
            .acknowledgment_number()
    }

    fn tcp_sequence_number(packet: &[u8]) -> u32 {
        etherparse::TcpSlice::from_slice(packet)
            .expect("tcp segment")
            .sequence_number()
    }

    #[test]
    fn tcp_custom_tx_handles_special_output_without_normal_packetization() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 32,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let (output_node, lookup_state, drop_state) = tcp_output_graph(&runtime);
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), runtime.buffers().clone());
        let session_id = driver
            .insert_session_with_id(|_| established_tcp_connection())
            .expect("insert session");
        publish_tcp_connection(&mut driver, session_id).expect("refresh session route");

        {
            let connection = driver.session_mut(session_id).expect("connection");
            connection.on_session_close();
        }

        let mut output = SessionQueueOutput::default();
        let output_next: crate::session::SessionQueueNext = output_node.into();
        let now = std::time::Instant::now();
        let timer_wheel = driver.timers_mut() as *mut _;
        let ready = driver.ready_mut_ptr();
        let buffers = driver.buffers() as *const _;
        let mut context =
            SessionQueueControlContext::new(timer_wheel, ready, buffers, session_id, false);
        let emitted = driver
            .session_mut(session_id)
            .expect("connection")
            .custom_tx(&runtime, &mut context, output_next, &mut output, 1, now)
            .expect("custom tx");
        let _ = runtime.run_ready_nodes().expect("run tcp output");

        assert_eq!(emitted, 1);
        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let segment = etherparse::TcpSlice::from_slice(&packets[0]).expect("tcp segment");
        assert_eq!(
            tcp_flags(&segment),
            TcpSegmentFlags::ACK | TcpSegmentFlags::FIN
        );
        assert_eq!(tcp_segment_payload(&packets[0]), b"");
        assert_eq!(
            driver
                .app()
                .pending_send_len(session_id)
                .expect("pending send len"),
            None
        );
    }

    #[test]
    fn tcp_normal_tx_retains_fifo_until_ack_cleanup() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 32,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let (output_node, lookup_state, drop_state) = tcp_output_graph(&runtime);
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), runtime.buffers().clone());
        let session_id = driver
            .insert_session_with_id(|_| established_tcp_connection())
            .expect("insert session");
        publish_tcp_connection(&mut driver, session_id).expect("refresh session route");

        let app_session = Arc::new(
            hammer_runtime::app::AppSession::new_in_segment(
                hammer_infra::segment::Local::default(),
                hammer_runtime::app::AppSessionConfig::new(256, 64),
                hammer_runtime::app::SessionHandle::new(session_id.pool_index().slot() as u32, 0),
                driver.app().tx_evt_q().clone(),
            )
            .expect("create app session"),
        );
        let payload = b"ping";
        app_session.send_bytes(payload).expect("send tx payload");
        driver.app_mut().attach_session(session_id, app_session);
        driver.mark_ready(session_id);

        let initial_snd_nxt = driver
            .session(session_id)
            .expect("connection")
            .snd_nxt();

        let next: crate::session::SessionQueueNext = output_node.into();
        let dispatched =
            dispatch_session_queue_for_ticks(&runtime, &mut driver, 0, next).expect("dispatch tx");
        assert!(dispatched.ready_sessions >= 1);
        let _ = runtime.run_ready_nodes().expect("run tcp output");

        let connection = driver.session(session_id).expect("connection");
        assert_eq!(
            connection.snd_nxt(),
            initial_snd_nxt.wrapping_add(payload.len() as u32)
        );
        assert_eq!(
            driver
                .app()
                .pending_send_len(session_id)
                .expect("pending send len"),
            Some(payload.len())
        );

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        assert_eq!(tcp_segment_payload(&packets[0]), payload);
    }

    #[test]
    fn tcp_timer_dispatch_uses_exact_timer_token() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 32,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let (output_node, lookup_state, drop_state) = tcp_output_graph(&runtime);
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), runtime.buffers().clone());
        let session_id = driver
            .insert_session_with_id(|_| established_tcp_connection())
            .expect("insert session");
        publish_tcp_connection(&mut driver, session_id).expect("refresh session route");

        let expected_sequence = {
            let connection = driver.session_mut(session_id).expect("connection");
            connection.timer_set(TCP_TIMER_DELAYED_ACK);
            connection.timer_set(TCP_TIMER_KEEP_ALIVE);
            connection.snd_nxt()
        };

        let session = session_id.pool_index();
        driver
            .timers_mut()
            .update_timer(
                session.slot(),
                session.generation(),
                TCP_TIMER_DELAYED_ACK,
                1,
            )
            .expect("arm delayed ack");

        expire_tcp_timers(&runtime, &mut driver, 1, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        assert_eq!(tcp_flags(&segment), TcpSegmentFlags::ACK);
        assert_eq!(tcp_segment_payload(packet), b"");
        assert_eq!(tcp_sequence_number(packet), expected_sequence);
        let connection = driver.session(session_id).expect("connection");
        assert!(!connection.timer_is_active(TCP_TIMER_DELAYED_ACK));
        assert!(connection.timer_is_active(TCP_TIMER_KEEP_ALIVE));
    }

    #[test]
    fn session_tcp_delayed_ack_timer_emits_ack_after_first_clean_payload() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 32,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let (output_node, lookup_state, drop_state) = tcp_output_graph(&runtime);
        let mut worker_state = TcpWorkerOwnedState::new(DataWorkerId::new(0));
        set_tcp_worker_state(&mut worker_state);
        let mut driver = SessionDriverRuntime::new(DataWorkerId::new(0), runtime.buffers().clone());
        let session_id = driver
            .insert_session_with_id(|_| established_tcp_connection())
            .expect("insert session");
        publish_tcp_connection(&mut driver, session_id).expect("refresh session route");

        let (local, remote, sequence, acknowledgment) = {
            let connection = driver.session(session_id).expect("connection");
            (
                connection.local().expect("local"),
                connection.remote(),
                connection.rcv_nxt(),
                connection.snd_nxt(),
            )
        };
        let payload = runtime.buffers().alloc_index().expect("payload");
        runtime
            .buffers()
            .append(payload, b"hello")
            .expect("payload bytes");
        let packet = TcpPacket {
            local: remote,
            remote: local,
            sequence: sequence.into(),
            acknowledgment: Some(acknowledgment.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::ACK | TcpSegmentFlags::PSH,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 5,
        };
        let control = {
            let connection = driver.session_mut(session_id).expect("connection");
            let (control, _) = connection
                .receive_established(&packet)
                .expect("receive data");
            assert!(connection.accept_payload(&packet).is_some());
            assert!(!connection.on_clean_in_order_payload());
            control
        };
        {
            let mut buffer = runtime
                .buffers()
                .get_buffer_mut(payload)
                .expect("payload buffer");
            buffer
                .advance(packet.payload_offset as isize)
                .expect("advance payload");
            buffer
                .truncate(packet.payload_len)
                .expect("truncate payload");
        }
        let enqueue = driver
            .enqueue_rx(session_id, payload, 0, false)
            .expect("enqueue rx");
        {
            let connection = driver.session_mut(session_id).expect("connection");
            connection.receive_payload(
                packet.sequence,
                0,
                enqueue.delivered_len,
                enqueue.newest_ooo_start,
                enqueue.newest_ooo_len,
            );
        }
        if enqueue.delivered_len != 0 {
            driver.mark_ready(session_id);
        }
        {
            let now = std::time::Instant::now();
            let session = session_id.pool_index();
            let connection: *const TcpConnection<BbrController> =
                driver.session(session_id).expect("connection") as *const _;
            let connection = unsafe { &*connection };
            let timers = driver.timers_mut();
            if connection.timer_is_active(TCP_TIMER_DELAYED_ACK) {
                let Some(ticks) = connection.timer_ticks(TCP_TIMER_DELAYED_ACK, now) else {
                    let _ = timers.cancel_timer(
                        session.slot(),
                        session.generation(),
                        TCP_TIMER_DELAYED_ACK,
                    );
                    panic!("refresh delayed ack timer");
                };
                timers
                    .update_timer(
                        session.slot(),
                        session.generation(),
                        TCP_TIMER_DELAYED_ACK,
                        ticks,
                    )
                    .expect("refresh delayed ack timer");
            } else {
                let _ = timers.cancel_timer(
                    session.slot(),
                    session.generation(),
                    TCP_TIMER_DELAYED_ACK,
                );
            }
        }

        assert!(control.is_none());
        assert!(drop_state.lock().expect("drop").packets.is_empty());
        assert!(lookup_state.lock().expect("lookup").packets.is_empty());

        expire_tcp_timers(&runtime, &mut driver, 1, output_node);

        assert!(drop_state.lock().expect("drop").packets.is_empty());
        let packets = &lookup_state.lock().expect("lookup").packets;
        assert_eq!(packets.len(), 1);
        let packet = &packets[0];
        let segment = etherparse::TcpSlice::from_slice(packet).expect("tcp segment");
        assert_eq!(tcp_flags(&segment), TcpSegmentFlags::ACK);
        assert_eq!(tcp_segment_payload(packet), b"");
        assert_eq!(tcp_ack_number(packet), sequence.wrapping_add(5));
    }

    fn drive_fin_ack_to_time_wait<C>(
        driver: &mut SessionDriverRuntime<TcpConnection<C>>,
        session_id: SessionId,
        local: SocketAddr,
        remote: SocketAddr,
    ) where
        C: CongestionController + 'static,
    {
        {
            let connection = driver.session_mut(session_id).expect("connection");
            connection.on_session_close();
            let _ = connection.on_tcp_ready(false, TcpCapabilities::default());
        }
        let (rcv_nxt, snd_nxt) = {
            let connection = driver.session(session_id).expect("connection");
            (connection.rcv_nxt(), connection.snd_nxt())
        };
        let packet = TcpPacket {
            local: remote,
            remote: local,
            sequence: rcv_nxt.into(),
            acknowledgment: Some(snd_nxt.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        };
        let now = {
            let connection = driver.session_mut(session_id).expect("connection");
            let _ = connection
                .receive_close_side(&packet)
                .expect("receive fin ack");
            std::time::Instant::now()
        };
        let session = session_id.pool_index();
        let connection: *const TcpConnection<C> =
            driver.session(session_id).expect("connection") as *const _;
        sync_all_tcp_timers(
            driver.timers_mut(),
            unsafe { &*connection },
            session,
            now,
        )
        .expect("sync time wait timer");
    }

    fn peer_fin_packet(
        local: SocketAddr,
        remote: SocketAddr,
        rcv_nxt: u32,
        snd_nxt: u32,
    ) -> TcpPacket {
        TcpPacket {
            local: remote,
            remote: local,
            sequence: rcv_nxt.into(),
            acknowledgment: Some(snd_nxt.into()),
            advertised_window: u16::MAX,
            flags: TcpSegmentFlags::FIN | TcpSegmentFlags::ACK,
            capabilities: TcpCapabilities::default(),
            sack_blocks: hammer_infra::vec::Vec::new(),
            timestamp: None,
            fast_open_cookie: None,
            ip_ecn: None,
            payload_offset: 0,
            payload_len: 0,
        }
    }

    #[test]
    fn tcp_passive_close_out_of_order_fin_does_not_transition() {
        let (mut driver, session_id, local, remote) = closing_session_for_test::<BbrController>();

        let (rcv_nxt, snd_nxt) = {
            let connection = driver.session(session_id).expect("connection");
            (connection.rcv_nxt(), connection.snd_nxt())
        };
        let packet = peer_fin_packet(local, remote, rcv_nxt.wrapping_add(1), snd_nxt);
        let connection = driver.session_mut(session_id).expect("connection");
        let (segment, _) = connection
            .receive_established(&packet)
            .expect("receive out-of-order fin");

        assert!(segment.is_none());
        assert_eq!(connection.state(), TcpState::Established);
        assert_eq!(connection.rcv_nxt(), rcv_nxt);
    }

    #[test]
    fn tcp_time_wait_expiry_closes_session() {
        let runtime =
            hammer_adapter::DataPlaneRuntime::new(hammer_adapter::DataPlaneRuntimeConfig {
                buffers: hammer_adapter::DataPlaneBufferConfig {
                    buffer_slot_capacity: 2048,
                    buffer_slots: 16,
                    frame_capacity: 8,
                    frame_slots: 8,
                    ..hammer_adapter::DataPlaneBufferConfig::default()
                },
            });
        let (output_node, _, _) = tcp_output_graph(&runtime);
        let (mut driver, session_id, local, remote) = closing_session_for_test::<BbrController>();

        drive_fin_ack_to_time_wait(&mut driver, session_id, local, remote);

        expire_tcp_timers(
            &runtime,
            &mut driver,
            u32::try_from(crate::transport::tcp::connection::TCP_TIME_WAIT_TICKS)
                .expect("time wait ticks fit u32"),
            output_node,
        );

        assert!(driver.session(session_id).is_none());
    }
}

#[cfg(test)]
fn tcp_flags(segment: &etherparse::TcpSlice<'_>) -> hammer_core::protocol::tcp::TcpSegmentFlags {
    let mut flags = hammer_core::protocol::tcp::TcpSegmentFlags::empty();
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::NS,
        segment.ns(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::FIN,
        segment.fin(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::SYN,
        segment.syn(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::RST,
        segment.rst(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::PSH,
        segment.psh(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::ACK,
        segment.ack(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::URG,
        segment.urg(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::ECE,
        segment.ece(),
    );
    flags.set(
        hammer_core::protocol::tcp::TcpSegmentFlags::CWR,
        segment.cwr(),
    );
    flags
}

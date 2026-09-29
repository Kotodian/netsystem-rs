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
use hammer_runtime::node::{NodeErrorDescriptor, NodeErrorSeverity};
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
    SessionMain as ServiceSessionMain, SessionTxOutcome, SessionWorker as ServiceSessionWorker,
};
use hammer_service::transport::{
    Transport, TransportMain, TransportOptions, TransportSendFlags, TransportSendParams,
    TransportServiceType, TransportTxTarget,
};

pub mod config;
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
pub use input::{
    Tcp4InputNode, Tcp4InputNoLookupNode, Tcp6InputNode, Tcp6InputNoLookupNode, TcpInputTrace,
};
pub use listen::{Tcp4ListenNode, Tcp6ListenNode, TcpListenNext};
pub use output::{DEFAULT_TCP_OUTPUT_PAYLOAD_LEN, Tcp4OutputNode, Tcp6OutputNode, TcpOutputNext};
pub use policy::{TcpPolicy, active_tcp_policy, publish_tcp_policy, tcp_policy};
pub use rcv_process::{Tcp4RcvProcessNode, Tcp6RcvProcessNode, TcpRcvProcessNext};
pub use recovery::{TcpRecoveryAck, TcpRecoveryState};
pub use reset::{Tcp4ResetNode, Tcp6ResetNode, TcpResetNext};
use segment::TcpSegment;
pub use syn_sent::{Tcp4SynSentNode, Tcp6SynSentNode, TcpSynSentNext};

pub use worker::TcpWorker;

// VPP tcp_input.c:1149-1211; session.h:685-829. Keep the packet chain as
// the source for the single copy into the Session RX FIFO.
pub(crate) fn expose_payload(
    runtime: &mut DataPlaneMain,
    buffer_index: u32,
    packet: &TcpPacket,
    trim: usize,
) -> RuntimeResult<()> {
    let mut prefix_left = packet.payload_offset + trim;
    let mut payload_left = packet.payload_len - trim;
    let mut next = Some(buffer_index);
    while let Some(index) = next {
        let buffer = runtime.buffer_mut(index);
        next = buffer.next_buffer_slot();
        let skip = prefix_left.min(buffer.current_len());
        buffer.advance(skip as isize);
        prefix_left -= skip;
        let keep = payload_left.min(buffer.current_len());
        buffer.truncate(keep)?;
        payload_left -= keep;
    }
    assert_eq!(prefix_left, 0, "parsed TCP header fits the Buffer chain");
    assert_eq!(payload_left, 0, "parsed TCP payload fits the Buffer chain");
    Ok(())
}

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
        TransportOptions::new(TransportServiceType::VirtualCircuit)
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

    fn half_close(
        &self,
        runtime: &mut DataPlaneMain,
        sessions: &mut ServiceSessionWorker,
        connection_index: u32,
        _: u32,
    ) {
        let mut worker = self.worker(runtime.thread_index())
            .expect("TCP close executes on its owner Data Worker");
        let Some(connection) = worker.connections.get(connection_index) else {
            return;
        };
        let handle = connection.base.session;
        let tx_queued = sessions.session_from_handle(handle)
            .and_then(|session| session.tx_fifo())
            .is_some_and(|fifo| fifo.max_dequeue() != 0);
        let (segment, next) = {
            let worker::TcpWorker { connections, timer_wheel, tco_next_node, .. } = &mut *worker;
            let connection = connections.get_mut(connection_index)
                .expect("closing TCP connection remains allocated");
            let next = tco_next_node[usize::from(!connection.remote().is_ipv4())];
            (connection.on_session_close(connection_index, timer_wheel, tx_queued), next)
        };
        drop(worker);
        if let Some(segment) = segment {
            let sent = enqueue_session_tcp_segment(runtime, sessions, connection_index, next, segment)
                .expect("valid TCP FIN fits a control Buffer");
            let mut worker = self.worker(runtime.thread_index())
                .expect("TCP close retains its owner Data Worker");
            let worker::TcpWorker { connections, timer_wheel, .. } = &mut *worker;
            let connection = connections.get_mut(connection_index)
                .expect("FIN retains its TCP connection");
            let interval = if sent {
                connection.retransmit_timeout().retransmit_timeout()
            } else {
                active_tcp_policy().allocation_retry
            };
            timers::update(timer_wheel, connection_index, connection.timer_state_mut(),
                timers::TcpTimerKind::Retransmit, interval)
                .expect("validated FIN retry interval fits TCP wheel");
        }
    }

    fn close(
        &self,
        runtime: &mut DataPlaneMain,
        sessions: &mut ServiceSessionWorker,
        connection_index: u32,
        worker_index: u32,
    ) {
        let (unread, half_open) = {
            let worker = self.worker(runtime.thread_index())
                .expect("TCP close executes on its owner Data Worker");
            let Some(connection) = worker.connections.get(connection_index) else {
                return;
            };
            (connection.state() == TcpState::Established
                && sessions.session_from_handle(connection.base.session)
                    .and_then(|session| session.rx_fifo())
                    .is_some_and(|fifo| fifo.max_dequeue() != 0),
                connection.state() == TcpState::SynSent)
        };
        if unread || half_open {
            self.reset(runtime, sessions, connection_index, worker_index);
        } else {
            self.half_close(runtime, sessions, connection_index, worker_index);
        }
    }

    fn reset(
        &self,
        runtime: &mut DataPlaneMain,
        sessions: &mut ServiceSessionWorker,
        connection_index: u32,
        _: u32,
    ) {
        let mut worker = self.worker(runtime.thread_index())
            .expect("TCP reset executes on its owner Data Worker");
        let (segment, next, handle) = {
            let worker::TcpWorker { connections, timer_wheel, tco_next_node, .. } = &mut *worker;
            let Some(connection) = connections.get_mut(connection_index) else {
                return;
            };
            let handle = connection.base.session;
            let next = tco_next_node[usize::from(!connection.remote().is_ipv4())];
            let segment = if connection.state() == TcpState::SynSent {
                None
            } else {
                let local = connection.local().expect("reset TCP has a local endpoint");
                Some(connection.control_segment(local, connection.remote(),
                    TcpSegmentFlags::RST | TcpSegmentFlags::ACK,
                    None, TcpCapabilities::default()))
            };
            for id in 0..timers::TCP_TIMER_KIND_COUNT as u32 {
                let kind = timers::TcpTimerKind::from_id(id)
                    .expect("TCP timer count covers every registered kind");
                timers::reset(timer_wheel, connection_index, connection.timer_state_mut(), kind);
            }
            connection.state = TcpState::Closed;
            (segment, next, handle)
        };
        worker.program_cleanup(connection_index);
        drop(worker);
        if let Some(segment) = segment {
            enqueue_session_tcp_segment(runtime, sessions, connection_index, next, segment)
                .expect("valid TCP reset fits a control Buffer");
        }
        if sessions.session_from_handle(handle).is_some() {
            sessions.transport_closed(runtime, handle, connection_index)
                .expect("reset retains its Session until closed notification");
        }
    }

    fn cleanup(&self, connection_index: u32, worker_index: u32) {
        if let Some(slot) = self.workers.get(worker_index as usize) {
            let worker = unsafe { &mut *slot.worker.as_ptr() };
            if worker.connection(connection_index).is_some() {
                worker.program_cleanup(connection_index);
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
        runtime: &mut DataPlaneMain,
        sessions: &ServiceSessionWorker,
        target: TransportTxTarget,
        buffers: &[u32],
        available_bytes: u32,
    ) {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        let worker_index = sessions.worker_index();
        let mut tcp = self
            .worker(runtime.thread_index())
            .expect("TCP header push runs on its owner Data Worker");
        let TcpWorker {
            connections,
            timer_wheel,
            cached_opts,
            cached_segment,
            ..
        } = &mut *tcp;
        let connection = connections
            .get_mut(connection_index)
            .expect("Session Queue retains a live TCP connection while pushing headers");
        let now = std::time::Instant::now();
        let outstanding = connection.snd_nxt().wrapping_sub(connection.snd_una());
        let max_dequeue = outstanding.checked_add(available_bytes)
            .expect("TCP flight and Session FIFO bytes fit u32");
        let segment = (*cached_segment)
            .expect("send params prepares TCP options before pushing a burst");
        let options_len = segment.header_len() - core::mem::size_of::<TcpHeader>();
        for &index in buffers.iter() {
            let buffer = runtime.buffer_mut(index);
            connection.push_one_header(buffer, &segment, &cached_opts[..options_len], now)
                .expect("Session Queue supplies a sendable TCP Buffer and headroom");
            let egress = hammer_core::buffer_opaque!(mut buffer => TcpSecondaryOpaque).egress_mut();
            egress.connection_index = connection_index;
            egress.worker_index = worker_index;
            egress.fib_index = connection.base.endpoint.fib_index();
        }
        if !buffers.is_empty() {
            connection.update_cwnd_limited(max_dequeue);
            connection
                .sync_payload_tx_timers(connection_index, timer_wheel, now)
                .expect("TCP timer interval remains valid after payload transmission");
        }
    }

    /// VPP: tcp.c:1170-1198, `tcp_session_send_params`.
    fn send_params(
        &self,
        runtime: &DataPlaneMain,
        sessions: &ServiceSessionWorker,
        target: TransportTxTarget,
        params: &mut TransportSendParams,
    ) {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        let mut tcp = self
            .worker(runtime.thread_index())
            .expect("TCP send-params runs on its owner Data Worker");
        let TcpWorker { connections, cached_opts, cached_segment, time_us, .. } = &mut *tcp;
        let connection = connections
            .get_mut(connection_index)
            .expect("Session Queue retains a live TCP connection while querying send space");
        connection.refresh_path_mtu_from_cache();
        let fifo = sessions
            .session_from_handle(connection.base.session)
            .and_then(|session| session.tx_fifo())
            .expect("sendable TCP connection retains its Session TX FIFO");
        let rx_available = sessions
            .session_from_handle(connection.base.session)
            .and_then(|session| session.rx_fifo())
            .expect("sendable TCP connection retains its Session RX FIFO")
            .max_enqueue();
        connection.set_rcv_wnd(rx_available);
        let segment = connection.tx_segment(0, TcpCapabilities::default())
            .expect("ready TCP connection prepares established burst options");
        let options_len = segment.cache_options(cached_opts);
        connection.update_burst_send_vars(fifo, options_len, (*time_us * 1_000_000.0) as u64);
        *cached_segment = Some(segment);
        let flight = connection.snd_nxt().wrapping_sub(connection.snd_una());
        let send_space = connection
            .send_space()
            .min(connection.snd_wnd().saturating_sub(flight));
        params.send_space = send_space;
        params.tx_offset = flight;
        params.send_mss = u16::try_from(connection.send_mss)
            .expect("TCP effective MSS fits transport parameter");
        params.flags = TransportSendFlags {
            deschedule: send_space == 0,
            postpone: false,
        };
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

    fn flush_data(&self, sessions: &ServiceSessionWorker, target: TransportTxTarget) {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        let mut tcp = self
            .worker(DataWorkerId::new(sessions.worker_index()).thread_index())
            .expect("TCP flush runs on its owner Data Worker");
        let connection = tcp.connections.get_mut(connection_index)
            .expect("TX flush retains its TCP connection");
        let fifo = sessions.session_from_handle(connection.base.session)
            .and_then(|session| session.tx_fifo())
            .expect("TX flush retains its Session TX FIFO");
        connection.flush_data(fifo);
    }

    fn custom_tx(
        &self,
        runtime: &mut DataPlaneMain,
        sessions: &mut ServiceSessionWorker,
        target: TransportTxTarget,
        params: &mut TransportSendParams,
    ) -> usize {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP registers only the connection peek TX entry");
        };
        let mut tcp = self.worker(runtime.thread_index())
            .expect("custom TX runs on its TCP owner worker");
        let retransmit = {
            let connection = tcp.connections.get_mut(connection_index)
                .expect("custom TX retains the TCP connection");
            let pending = connection.recovery.in_recovery()
                && connection.retransmit_pending;
            if pending {
                connection.retransmit_pending = false;
            }
            pending
        };
        let packets = if retransmit {
            tcp.retransmit(
                runtime, sessions, connection_index, params.max_burst_size as usize,
            )
        } else {
            0
        };
        let send_ack = {
            let connection = tcp.connections.get_mut(connection_index)
                .expect("custom TX retains the TCP connection");
            let pending = connection.send_ack_pending;
            connection.send_ack_pending = false;
            pending && (packets == 0 || connection.pending_dupacks != 0)
        };
        if !send_ack {
            return packets;
        }
        if packets >= params.max_burst_size as usize {
            tcp.program_ack(runtime, sessions, connection_index, false);
            return packets;
        }
        packets + tcp.send_acks(
            runtime, sessions, connection_index, params.max_burst_size as usize - packets,
        )
    }

    fn app_rx_event(
        &self,
        runtime: &mut DataPlaneMain,
        sessions: &mut ServiceSessionWorker,
        connection_index: u32,
    ) {
        let mut tcp = self.worker(runtime.thread_index())
            .expect("TCP RX event runs on its owner Data Worker");
        let next_nodes = tcp.tco_next_node;
        let connection = tcp.connections.get_mut(connection_index)
            .expect("RX event retains its TCP connection");
        if !connection.zero_receive_window_sent() {
            return;
        }
        let rx = sessions.session_from_handle(connection.base.session)
            .and_then(|session| session.rx_fifo())
            .expect("RX event retains its Session RX FIFO");
        let available = rx.max_enqueue();
        let minimum = (rx.size() >> 3).clamp(4 << 10, 128 << 10);
        if available < minimum {
            rx.want_deq_notification();
            return;
        }
        let segment = connection.receive_window_update_segment(available)
            .expect("zero-window TCP Session remains established");
        let next = next_nodes[usize::from(!connection.remote().is_ipv4())];
        drop(tcp);
        match enqueue_session_tcp_segment(runtime, sessions, connection_index, next, segment) {
            Ok(true) | Ok(false) => {}
            Err(error) => panic!("TCP RX window ACK remains sendable: {error}"),
        }
    }

    fn is_descheduled(&self, runtime: &DataPlaneMain, target: TransportTxTarget) -> bool {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        self.worker(runtime.thread_index())
            .expect("transport is on its TCP worker")
            .connections.get(connection_index)
            .expect("Session retains its TCP connection")
            .base.is_descheduled()
    }

    fn deschedule(&self, runtime: &DataPlaneMain, target: TransportTxTarget) {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        self.worker(runtime.thread_index())
            .expect("transport is on its TCP worker")
            .connections.get_mut(connection_index)
            .expect("Session retains its TCP connection")
            .base.flags.descheduled = true;
    }

    fn clear_descheduled(&self, runtime: &DataPlaneMain, target: TransportTxTarget) {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        let mut tcp = self.worker(runtime.thread_index())
            .expect("transport is on its TCP worker");
        let now_us = (tcp.time_us * 1_000_000.0) as u64;
        tcp.connections.get_mut(connection_index)
            .expect("Session retains its TCP connection")
            .base.clear_descheduled(now_us);
    }

    fn is_tx_paced(&self, runtime: &DataPlaneMain, target: TransportTxTarget) -> bool {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        self.worker(runtime.thread_index())
            .expect("transport is on its TCP worker")
            .connections.get(connection_index)
            .expect("Session retains its TCP connection")
            .base.is_tx_paced()
    }

    fn tx_pacer_burst(&self, runtime: &DataPlaneMain, target: TransportTxTarget) -> u32 {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        let mut tcp = self.worker(runtime.thread_index())
            .expect("transport is on its TCP worker");
        let now = (tcp.time_us * 1_000_000.0) as u64;
        tcp.connections.get_mut(connection_index)
            .expect("Session retains its TCP connection")
            .base.pacer.update(now)
    }

    fn tx_pacer_update_bytes(&self, runtime: &DataPlaneMain, target: TransportTxTarget, bytes: u32) {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        self.worker(runtime.thread_index())
            .expect("transport is on its TCP worker")
            .connections.get_mut(connection_index)
            .expect("Session retains its TCP connection")
            .base.pacer.update_bytes(bytes);
    }

    fn tx_pacer_reset_bucket(&self, runtime: &DataPlaneMain, target: TransportTxTarget, bucket: u32) {
        let TransportTxTarget::Connection(connection_index) = target else {
            panic!("TCP peek TX requires a connection");
        };
        self.worker(runtime.thread_index())
            .expect("transport is on its TCP worker")
            .connections.get_mut(connection_index)
            .expect("Session retains its TCP connection")
            .base.pacer.bucket = i64::from(bucket);
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
    let protocol = ServiceSessionMain::global()
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?
        .register_transport_protocol()
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
    let session_main = ServiceSessionMain::global()
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?;
    session_main
        .register_transport_io(protocol, tcp_session_io, tcp_session_update_time)
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?;
    session_main
        .register_transport_control(protocol, tcp_session_control)
        .map_err(|source| TcpWorkerError::SessionTransportRegistration { source })?;
    Ok(())
}

/// VPP: session_node.c:1766-1790, session.c:1641-1709. The Session worker
/// chooses the registered protocol; TCP calls its concrete Transport trait.
fn tcp_session_control(
    sessions: &mut ServiceSessionWorker,
    runtime: &mut DataPlaneMain,
    session_index: u32,
    event: SessionEventType,
) -> Result<(), SessionError> {
    let tcp = TCP_MAIN
        .get()
        .expect("TCP Main remains published while its Session type is registered");
    let session = sessions.session(session_index).ok_or(SessionError::NoSession)?;
    let connection_index = session.connection_index();
    let worker_index = sessions.worker_index();
    match event {
        SessionEventType::HalfClose => {
            <TcpMain as Transport<IpTransportEndpointConfig>>::half_close(
                tcp,
                runtime,
                sessions,
                connection_index,
                worker_index,
            );
        }
        SessionEventType::Close => {
            <TcpMain as Transport<IpTransportEndpointConfig>>::close(
                tcp,
                runtime,
                sessions,
                connection_index,
                worker_index,
            );
        }
        SessionEventType::Reset => {
            <TcpMain as Transport<IpTransportEndpointConfig>>::reset(
                tcp,
                runtime,
                sessions,
                connection_index,
                worker_index,
            );
        }
        _ => unreachable!("TCP transport control receives only half-close, close, or reset"),
    }
    Ok(())
}

/// VPP: session_node.c:1680-1694, the registered TCP peek TX entry.
fn tcp_session_tx(
    sessions: &mut ServiceSessionWorker,
    runtime: &mut DataPlaneMain,
    node: &mut NodeRuntime,
    event_index: u32,
    packets: &mut usize,
) -> SessionTxOutcome {
    let tcp = TCP_MAIN
        .get()
        .expect("registered TCP Session type retains TCP Main");
    sessions.tx_fifo_peek_and_send(runtime, node, event_index, packets, tcp)
}

/// VPP: session_node.c:1858-1915, the TCP protocol's RX dispatch.
fn tcp_session_io(
    runtime: &mut DataPlaneMain,
    sessions: &mut ServiceSessionWorker,
    session_index: u32,
    event: SessionEventType,
) -> Result<(usize, bool), SessionError> {
    let tcp = TCP_MAIN
        .get()
        .expect("TCP Main remains published while its Session type is registered");
    let connection_index = sessions
        .session(session_index)
        .ok_or(SessionError::NoSession)?
        .connection_index();
    match event {
        SessionEventType::Tx | SessionEventType::TxFlush => {
            unreachable!("TCP TX events use the registered Session type entry")
        }
        SessionEventType::Rx => {
            <TcpMain as Transport<IpTransportEndpointConfig>>::app_rx_event(
                tcp,
                runtime,
                sessions,
                connection_index,
            );
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
    {
        let mut worker = tcp.worker(runtime.thread_index())
            .map_err(|source| SessionError::TransportOpFailed { source })?;
        worker.handle_cleanups(std::time::Instant::now(), sessions);
    }
    <TcpMain as Transport<IpTransportEndpointConfig>>::update_time(tcp, now, worker_index);
    let mut worker = tcp
        .worker(runtime.thread_index())
        .map_err(|source| SessionError::TransportOpFailed { source })?;
    let now = worker.last_timer_update;
    let mut timer_budget = worker.max_timers_per_loop as usize;
    while timer_budget != 0 {
        let Some(token) = worker.take_pending_timer(&mut timer_budget) else {
            break;
        };
        let (session_index, outcome, is_ip4, prior_state, state) = {
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
            let prior_state = connection.state();
            let capabilities = lookup
                .pending_open_capabilities(session_index)
                .unwrap_or_default();
            let outcome = connection
                .on_typed_timer_expiry(token.index, timer_wheel, token.kind, capabilities, now)
                .map_err(|source| SessionError::TransportOpFailed { source })?;
            (session_index, outcome, connection.remote().is_ipv4(),
                prior_state, connection.state())
        };
        let payload_retransmit = matches!(
            outcome.action,
            Some(connection::TcpTimerAction::RtoRetransmit)
        ) && outcome.segment.is_none();
        if matches!(outcome.action, Some(connection::TcpTimerAction::RackRetransmit)) {
            worker.program_retransmit(runtime, sessions, token.index);
        }
        if matches!(outcome.action, Some(connection::TcpTimerAction::TlpProbe)) {
            worker.send_tlp_probe(runtime, sessions, token.index);
            let worker::TcpWorker { connections, timer_wheel, .. } = &mut *worker;
            let connection = connections.get_mut(token.index)
                .expect("TLP expiry retains its TCP connection");
            let interval = connection.retransmit_timeout().retransmit_timeout();
            timers::update(
                timer_wheel,
                token.index,
                connection.timer_state_mut(),
                timers::TcpTimerKind::Retransmit,
                interval,
            ).map_err(|source| SessionError::TransportOpFailed { source })?;
        }
        if payload_retransmit {
            let connection = worker.connections.get(token.index)
                .expect("expired TCP timer retains its connection");
            let sequence = connection.tx_intent_sequence
                .expect("payload retransmit timer selected its sequence");
            let offset = connection.snd_una().distance_to(sequence);
            let length = connection.tx_intent_payload_len.min(connection.send_mss);
            let written = worker.prepare_segment(
                runtime, sessions, token.index, offset, length, true,
            );
            if matches!(outcome.action, Some(connection::TcpTimerAction::RtoRetransmit)) {
                let worker::TcpWorker { connections, timer_wheel, .. } = &mut *worker;
                let connection = connections.get_mut(token.index)
                    .expect("RTO retains its TCP connection");
                let interval = if written == 0 {
                    active_tcp_policy().allocation_retry
                } else {
                    connection.observe_retransmit_timeout()
                };
                timers::update(
                    timer_wheel,
                    token.index,
                    connection.timer_state_mut(),
                    timers::TcpTimerKind::Retransmit,
                    interval,
                ).map_err(|source| SessionError::TransportOpFailed { source })?;
            }
            if written != 0 {
                let connection = worker.connections.get_mut(token.index)
                    .expect("sent retransmit retains its TCP connection");
                if matches!(outcome.action, Some(connection::TcpTimerAction::RtoRetransmit)) {
                    connection.recovery.on_retransmit_sent(written as u32);
                }
                connection.recovery.commit_retransmit(sequence, now);
                if matches!(outcome.action, Some(connection::TcpTimerAction::RtoRetransmit)) {
                    worker.program_retransmit(runtime, sessions, token.index);
                }
            }
        }
        let handle = ServiceSessionHandle {
            worker_index,
            session_index,
        };
        let mut control_sent = false;
        if let Some(segment) = outcome.segment {
            if segment.payload_len() == 0 {
                let next = worker.tco_next_node[usize::from(!is_ip4)];
                control_sent = enqueue_session_tcp_segment(
                    runtime, sessions, token.index, next, segment,
                )?;
            } else if sessions.session_from_handle(handle).is_some() {
                sessions.enqueue_ready(handle, tcp.protocol)?;
            }
        } else if matches!(token.kind, timers::TcpTimerKind::Persist
            | timers::TcpTimerKind::Pacing)
            && sessions.session_from_handle(handle).is_some()
        {
            sessions.enqueue_ready(handle, tcp.protocol)?;
        }
        if token.kind == timers::TcpTimerKind::WaitClose && state != prior_state {
            if prior_state != TcpState::TimeWait
                && matches!(state, TcpState::LastAck | TcpState::Closed)
                && sessions.session_from_handle(handle).is_some()
            {
                sessions.transport_closed(runtime, handle, token.index)?;
            }
            if prior_state == TcpState::CloseWait && state == TcpState::LastAck {
                let worker::TcpWorker { connections, timer_wheel, .. } = &mut *worker;
                let connection = connections.get_mut(token.index)
                    .expect("WAITCLOSE FIN retains its TCP connection");
                let interval = if control_sent {
                    connection.retransmit_timeout().retransmit_timeout()
                } else {
                    active_tcp_policy().allocation_retry
                };
                timers::update(timer_wheel, token.index, connection.timer_state_mut(),
                    timers::TcpTimerKind::Retransmit, interval)
                    .map_err(|source| SessionError::TransportOpFailed { source })?;
            }
            if state == TcpState::Closed {
                worker.program_cleanup(token.index);
            }
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
    runtime.register_node_errors(node, &TCP_ERRORS)?;
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
    runtime.register_node_errors(node, &TCP_ERRORS)?;
    hammer_plugin_ip::register_ip6_protocol(runtime.nodes(), 6, node)?;
    Ok(node)
}

pub fn register_tcp4_input_nolookup(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = if let Some(node) = runtime.nodes().node_by_name("tcp4-input-nolookup") {
        node
    } else {
        runtime.nodes().try_register_internal_with_next_names(
            Tcp4InputNoLookupNode::new(),
            &TcpInputNext::NEXT_NAMES,
        )?
    };
    runtime.register_node_errors(node, &TCP_ERRORS)?;
    Ok(node)
}

pub fn register_tcp6_input_nolookup(runtime: &DataPlaneMain) -> RuntimeResult<NodeId> {
    let node = if let Some(node) = runtime.nodes().node_by_name("tcp6-input-nolookup") {
        node
    } else {
        runtime.nodes().try_register_internal_with_next_names(
            Tcp6InputNoLookupNode::new(),
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
    runtime.register_node_errors(node, &TCP_ERRORS)?;
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

const TCP_ERRORS: [NodeErrorDescriptor; 12] = [
    NodeErrorDescriptor::new("wrong-thread", NodeErrorSeverity::Error, "Wrong TCP worker"),
    NodeErrorDescriptor::new("length", NodeErrorSeverity::Error, "Inconsistent IP/TCP length"),
    NodeErrorDescriptor::new("no-listener", NodeErrorSeverity::Error, "No TCP listener"),
    NodeErrorDescriptor::new("lookup-drops", NodeErrorSeverity::Error, "TCP lookup dropped packet"),
    NodeErrorDescriptor::new("dispatch", NodeErrorSeverity::Error, "TCP dispatch failed"),
    NodeErrorDescriptor::new("segment-invalid", NodeErrorSeverity::Error, "Invalid TCP segment"),
    NodeErrorDescriptor::new("ack-invalid", NodeErrorSeverity::Error, "Invalid TCP ACK"),
    NodeErrorDescriptor::new("connection-invalid", NodeErrorSeverity::Error, "Invalid TCP connection"),
    NodeErrorDescriptor::new("connection-closed", NodeErrorSeverity::Info, "TCP connection closed"),
    NodeErrorDescriptor::new("options", NodeErrorSeverity::Error, "TCP options invalid"),
    NodeErrorDescriptor::new("paws", NodeErrorSeverity::Error, "TCP PAWS rejected packet"),
    NodeErrorDescriptor::new("receive-window", NodeErrorSeverity::Error, "TCP receive window rejected packet"),
];

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
    #[error("payload enqueued")]
    Enqueued,
    #[error("out-of-order payload enqueued")]
    EnqueuedOoo,
    #[error("receive FIFO full")]
    FifoFull,
    #[error("payload partially enqueued")]
    PartiallyEnqueued,
    #[error("old segment")]
    SegmentOld,
    #[error("zero receive window")]
    ZeroRwnd,
}

const TCP_NODE_ERRORS: [hammer_runtime::node::NodeErrorDescriptor; 28] = [
    hammer_runtime::node::NodeErrorDescriptor::new("session-missing", hammer_runtime::node::NodeErrorSeverity::Error, "Session missing"),
    hammer_runtime::node::NodeErrorDescriptor::new("established-session-missing", hammer_runtime::node::NodeErrorSeverity::Error, "Established Session missing"),
    hammer_runtime::node::NodeErrorDescriptor::new("established-session-route-missing", hammer_runtime::node::NodeErrorSeverity::Error, "Established Session route missing"),
    hammer_runtime::node::NodeErrorDescriptor::new("receive-session-missing", hammer_runtime::node::NodeErrorSeverity::Error, "Receive Session missing"),
    hammer_runtime::node::NodeErrorDescriptor::new("receive-session-route-missing", hammer_runtime::node::NodeErrorSeverity::Error, "Receive Session route missing"),
    hammer_runtime::node::NodeErrorDescriptor::new("syn-sent-session-missing", hammer_runtime::node::NodeErrorSeverity::Error, "SYN-SENT Session missing"),
    hammer_runtime::node::NodeErrorDescriptor::new("syn-sent-session-route-missing", hammer_runtime::node::NodeErrorSeverity::Error, "SYN-SENT Session route missing"),
    hammer_runtime::node::NodeErrorDescriptor::new("timer-update-failed", hammer_runtime::node::NodeErrorSeverity::Error, "Timer update failed"),
    hammer_runtime::node::NodeErrorDescriptor::new("tx-offset-overflow", hammer_runtime::node::NodeErrorSeverity::Error, "TX offset overflow"),
    hammer_runtime::node::NodeErrorDescriptor::new("bad-checksum", hammer_runtime::node::NodeErrorSeverity::Error, "Bad TCP checksum"),
    hammer_runtime::node::NodeErrorDescriptor::new("reset-received", hammer_runtime::node::NodeErrorSeverity::Info, "Reset received"),
    hammer_runtime::node::NodeErrorDescriptor::new("bad-segment", hammer_runtime::node::NodeErrorSeverity::Error, "Bad TCP segment"),
    hammer_runtime::node::NodeErrorDescriptor::new("no-listener", hammer_runtime::node::NodeErrorSeverity::Error, "No TCP listener"),
    hammer_runtime::node::NodeErrorDescriptor::new("connection-create", hammer_runtime::node::NodeErrorSeverity::Error, "TCP connection creation failed"),
    hammer_runtime::node::NodeErrorDescriptor::new("rack-retransmit", hammer_runtime::node::NodeErrorSeverity::Info, "RACK retransmit"),
    hammer_runtime::node::NodeErrorDescriptor::new("tlp-probe", hammer_runtime::node::NodeErrorSeverity::Info, "TLP probe"),
    hammer_runtime::node::NodeErrorDescriptor::new("retransmit", hammer_runtime::node::NodeErrorSeverity::Info, "RTO retransmit"),
    hammer_runtime::node::NodeErrorDescriptor::new("pacing-limited", hammer_runtime::node::NodeErrorSeverity::Info, "Pacing limited"),
    hammer_runtime::node::NodeErrorDescriptor::new("persist-probe", hammer_runtime::node::NodeErrorSeverity::Info, "Persist probe"),
    hammer_runtime::node::NodeErrorDescriptor::new("bbr-congestion", hammer_runtime::node::NodeErrorSeverity::Info, "BBR congestion"),
    hammer_runtime::node::NodeErrorDescriptor::new("bad-window", hammer_runtime::node::NodeErrorSeverity::Error, "Bad receive window"),
    hammer_runtime::node::NodeErrorDescriptor::new("keepalive-probe", hammer_runtime::node::NodeErrorSeverity::Info, "Keepalive probe"),
    hammer_runtime::node::NodeErrorDescriptor::new("enqueued", hammer_runtime::node::NodeErrorSeverity::Info, "Packets pushed into RX FIFO"),
    hammer_runtime::node::NodeErrorDescriptor::new("enqueued-ooo", hammer_runtime::node::NodeErrorSeverity::Warn, "OOO packets pushed into RX FIFO"),
    hammer_runtime::node::NodeErrorDescriptor::new("fifo-full", hammer_runtime::node::NodeErrorSeverity::Error, "Packets dropped for lack of RX FIFO space"),
    hammer_runtime::node::NodeErrorDescriptor::new("partially-enqueued", hammer_runtime::node::NodeErrorSeverity::Warn, "Packets partially pushed into RX FIFO"),
    hammer_runtime::node::NodeErrorDescriptor::new("segment-old", hammer_runtime::node::NodeErrorSeverity::Warn, "Old segment"),
    hammer_runtime::node::NodeErrorDescriptor::new("zero-rwnd", hammer_runtime::node::NodeErrorSeverity::Warn, "Zero receive window"),
];

pub(crate) fn register_tcp_node_errors(runtime: &DataPlaneMain, node: NodeId) -> RuntimeResult<()> {
    runtime.register_node_errors(node, &TCP_NODE_ERRORS)
}

/// VPP tcp_input.c:1003-1111, in-order and OOO FIFO packet classification.
#[inline(always)]
pub(crate) fn payload_node_error(
    delivery: hammer_service::session::RxDelivery,
    requested: u32,
    send_mss: u32,
    offset: u32,
) -> TcpNodeError {
    match delivery {
        hammer_service::session::RxDelivery::NotAccepted { rx_available } => {
            if offset == 0 && rx_available < send_mss {
                TcpNodeError::ZeroRwnd
            } else {
                TcpNodeError::FifoFull
            }
        }
        hammer_service::session::RxDelivery::InOrder { accepted, .. } => {
            if accepted.get() == requested {
                TcpNodeError::Enqueued
            } else {
                TcpNodeError::PartiallyEnqueued
            }
        }
        hammer_service::session::RxDelivery::OutOfOrder { accepted, .. } => {
            if accepted.get() == requested {
                TcpNodeError::EnqueuedOoo
            } else {
                TcpNodeError::FifoFull
            }
        }
    }
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
            TcpNodeError::Enqueued
            | TcpNodeError::EnqueuedOoo
            | TcpNodeError::FifoFull
            | TcpNodeError::PartiallyEnqueued
            | TcpNodeError::SegmentOld
            | TcpNodeError::ZeroRwnd => {
                panic!("TCP packet accounting is not a control-plane error")
            }
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
    connection_index: u32,
    next: u8,
    present: u8,
    reserved: [u8; 38],
}

const _: () = assert!(std::mem::size_of::<TcpRouteOpaque>() == 56);

impl Default for TcpRouteOpaque {
    #[inline]
    fn default() -> Self {
        Self {
            session_raw: 0,
            owner_worker: 0,
            connection_index: u32::MAX,
            next: 0,
            present: 0,
            reserved: [0; 38],
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
    connection_index: u32,
    owner: DataWorkerId,
    next: TcpInputNext,
) {
    *opaque = TcpRouteOpaque {
        session_raw: session_id.into(),
        owner_worker: owner.slot() as u32,
        connection_index,
        next: next as u8,
        present: 1,
        reserved: [0; 38],
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

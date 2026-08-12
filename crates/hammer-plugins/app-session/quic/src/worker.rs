//! QUIC's worker-local transport state and registration.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use hammer_core::data_plane::BufferFrame;
use hammer_infra::bytes::BytesBuffer;
use hammer_infra::fifo::{Fifo, FifoError};
use hammer_infra::pool::{Index, Pool};
use hammer_infra::thread_owned::ThreadOwnedError;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::app::{
    ApplicationId, SessionAppContext, SessionAppId, SessionConnectError, SessionDgramHeader,
    SessionFlags, SessionHandle,
};
use hammer_runtime::session::{SessionApplicationErrorCode, SessionStreamDirection};
use hammer_runtime::{
    DataPlaneRuntime, DataWorkerId, NodeRuntimeData, RuntimeError, RuntimeResult,
    SessionConnectionId, SessionListenerId,
};
use hammer_service::session::ApplicationMain;
use hammer_service::session::SessionId;
use hammer_service::session::error::SessionTransportActionError;
use hammer_service::session::node::{SessionQueueNext, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionTransport, SessionTransportId, SessionTransportWorkerActions, SessionWorker,
    TransportInternalTransport, TransportInternalTx, dispatch_session_queue_events,
};

use crate::listener::QUIC_MAIN;
use quinn_proto::{
    Connection, ConnectionError, ConnectionHandle, DatagramEvent, Endpoint, EndpointConfig, Event,
    PartialDecode, StreamEvent, StreamId, VarInt,
};

use crate::config::ConfigId;
use crate::stream_io::StreamIoTable;

pub(super) const QUIC_CONTEXT_CAPACITY: usize = 4_096;

const MAX_PACKET_SIZE: usize = 1280;
const RX_DATAGRAM_BURST: usize = 16;
const TX_PACKET_BURST: usize = 10;
const CONNECTION_TX_PENDING: u8 = 1;
const STREAM_ENGINE_CLOSED: u8 = 1;
const STREAM_APP_CLOSED_TX: u8 = 1 << 1;
const STREAM_APP_CLOSE_PENDING: u8 = 1 << 2;
const STREAM_RECV_FIN: u8 = 1 << 3;
/// App error code carried by local STOP_SENDING/RESET_STREAM frames (VPP
/// `QUICLY_ERROR_FROM_APPLICATION_ERROR_CODE(ctx->app_err_code)`, which
/// defaults to 0).
const RESET_APP_ERROR_CODE: quinn_proto::VarInt = quinn_proto::VarInt::from_u32(0);

/// QUIC-local wrapper around `SessionApplicationErrorCode` (foreign) so the
/// checked `TryFrom` conversion into `VarInt` (foreign) can be implemented
/// under the orphan rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(super) struct QuicAppErrorCode(SessionApplicationErrorCode);

impl TryFrom<QuicAppErrorCode> for VarInt {
    type Error = u64;

    #[inline]
    fn try_from(code: QuicAppErrorCode) -> Result<Self, Self::Error> {
        let raw = u64::from(code.0);
        VarInt::try_from(raw).map_err(|_| raw)
    }
}
const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const TIMER_MAX_TICKS_PER_UPDATE: u32 = 1_024;
const TIMER_EXPIRY_BUDGET: usize = 256;
const TIMER_WHEEL_MAX_INTERVAL_TICKS: u64 = crate::config::MAX_CONNECTION_TIMEOUT as u64;

#[inline]
fn stream_has_send_side(connection: &Connection, stream: StreamId) -> bool {
    stream.dir() == quinn_proto::Dir::Bi || stream.initiator() == connection.side()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuicConnectionError {
    TlsAlert { alert: u8 },
    QuicVersionUnsupported,
    TimedOut,
    ConnectionRefused,
    ConnectionReset,
    PeerClosed { code: u64 },
    QuicTransportError { code: u64 },
    LocalResourceExhausted,
    LocalClosed,
}

impl From<quinn_proto::TransportError> for QuicConnectionError {
    fn from(error: quinn_proto::TransportError) -> Self {
        let code = u64::from(error.code);
        if (0x100..0x200).contains(&code) {
            Self::TlsAlert {
                alert: (code - 0x100) as u8,
            }
        } else {
            Self::QuicTransportError { code }
        }
    }
}

impl From<quinn_proto::ConnectionClose> for QuicConnectionError {
    fn from(error: quinn_proto::ConnectionClose) -> Self {
        if error.error_code == quinn_proto::TransportErrorCode::CONNECTION_REFUSED {
            Self::ConnectionRefused
        } else {
            Self::QuicTransportError {
                code: u64::from(error.error_code),
            }
        }
    }
}

impl From<quinn_proto::ApplicationClose> for QuicConnectionError {
    fn from(error: quinn_proto::ApplicationClose) -> Self {
        Self::PeerClosed {
            code: u64::from(error.error_code),
        }
    }
}

impl From<ConnectionError> for QuicConnectionError {
    fn from(error: ConnectionError) -> Self {
        match error {
            ConnectionError::VersionMismatch => Self::QuicVersionUnsupported,
            ConnectionError::TransportError(error) => error.into(),
            ConnectionError::ConnectionClosed(error) => error.into(),
            ConnectionError::ApplicationClosed(error) => error.into(),
            ConnectionError::Reset => Self::ConnectionReset,
            ConnectionError::TimedOut => Self::TimedOut,
            ConnectionError::LocallyClosed => Self::LocalClosed,
            ConnectionError::CidsExhausted => Self::LocalResourceExhausted,
        }
    }
}

impl From<QuicConnectionError> for SessionConnectError {
    fn from(error: QuicConnectionError) -> Self {
        match error {
            QuicConnectionError::TlsAlert { alert } => Self::TlsAlert { alert },
            QuicConnectionError::QuicVersionUnsupported => Self::QuicVersionUnsupported,
            QuicConnectionError::TimedOut => Self::TimedOut,
            QuicConnectionError::ConnectionRefused => Self::ConnectionRefused,
            QuicConnectionError::ConnectionReset => Self::ConnectionReset,
            QuicConnectionError::PeerClosed { code } => Self::PeerClosed { code },
            QuicConnectionError::QuicTransportError { code } => Self::QuicTransportError { code },
            QuicConnectionError::LocalResourceExhausted => Self::LocalResourceExhausted,
            QuicConnectionError::LocalClosed => Self::LocalClosed,
        }
    }
}

#[repr(C, align(64))]
struct RxDatagramScratch {
    data: [u8; MAX_PACKET_SIZE],
}

impl Default for RxDatagramScratch {
    fn default() -> Self {
        Self {
            data: [0; MAX_PACKET_SIZE],
        }
    }
}

/// Generation-checked identity for one QUIC listener, connection, or stream
/// context. Main Thread listener contexts and Data Worker transport contexts
/// use the same packed pool identity, but each owner retains its own pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(super) struct ContextId(u64);

impl From<u64> for ContextId {
    #[inline]
    fn from(raw: u64) -> Self {
        Self(raw)
    }
}

impl From<ContextId> for u64 {
    #[inline]
    fn from(context: ContextId) -> Self {
        context.0
    }
}

impl From<Index> for ContextId {
    #[inline]
    fn from(index: Index) -> Self {
        Self(u64::from(index.slot()) | (u64::from(index.generation()) << 32))
    }
}

impl From<ContextId> for Index {
    #[inline]
    fn from(context: ContextId) -> Self {
        Self::new(context.0 as u32, (context.0 >> 32) as u32)
    }
}

/// Worker-local role state stored in one QUIC context slot.
#[repr(C)]
#[derive(Debug, Clone)]
pub(super) struct ListenerContext {
    pub(crate) outer_listener: SessionListenerId,
    pub(crate) outer_application: ApplicationId,
    pub(crate) inner_application_listener: hammer_runtime::app::ApplicationListenerId,
    pub(crate) inner_session_listener: SessionListenerId,
    pub(crate) configuration: ConfigId,
    pub(crate) connection_timeout: u32,
    pub(crate) server_config: Option<Arc<quinn_proto::ServerConfig>>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Handshaking,
    Established,
    ActiveClosing,
    PassiveClosing,
    PassiveClosingAppClosed,
    PassiveClosingQuicClosed,
    TransportClosed,
}

struct EngineConnection {
    handle: Option<ConnectionHandle>,
    connection: Option<Connection>,
    remote: Option<SocketAddr>,
    local: Option<SocketAddr>,
    server_config: Option<Arc<quinn_proto::ServerConfig>>,
    application: ApplicationId,
    app: Option<SessionAppId>,
    /// Application-supplied opaque for child Stream Sessions. Not VPP
    /// `quic_ctx_t.client_opaque`.
    app_opaque: Option<u64>,
    /// VPP `quic_ctx_t.client_opaque`: outer active Application Connection
    /// correlation retained through the handshake.
    client_opaque: Option<SessionConnectionId>,
    /// Outer Session listener retained through a passive handshake.
    outer_listener: Option<SessionListenerId>,
    pending_connect_error: Option<SessionConnectError>,
    client_config: Option<Arc<quinn_proto::ClientConfig>>,
    client_server_name: Option<String>,
    client_local: Option<SocketAddr>,
    client_remote: Option<SocketAddr>,
    io_table: Box<StreamIoTable>,
}

impl EngineConnection {
    fn pending(
        server_config: Option<Arc<quinn_proto::ServerConfig>>,
        application: ApplicationId,
        app: Option<SessionAppId>,
        app_opaque: Option<u64>,
        client_opaque: Option<SessionConnectionId>,
        outer_listener: Option<SessionListenerId>,
    ) -> Self {
        Self {
            handle: None,
            connection: None,
            remote: None,
            local: None,
            server_config,
            application,
            app,
            app_opaque,
            client_opaque,
            outer_listener,
            pending_connect_error: None,
            client_config: None,
            client_server_name: None,
            client_local: None,
            client_remote: None,
            io_table: StreamIoTable::new(),
        }
    }

    fn client(
        config: Arc<quinn_proto::ClientConfig>,
        server_name: String,
        local: SocketAddr,
        remote: SocketAddr,
        application: ApplicationId,
        app: Option<SessionAppId>,
        app_opaque: Option<u64>,
        client_opaque: Option<SessionConnectionId>,
    ) -> Self {
        Self {
            handle: None,
            connection: None,
            remote: None,
            local: None,
            server_config: None,
            application,
            app,
            app_opaque,
            client_opaque,
            outer_listener: None,
            pending_connect_error: None,
            client_config: Some(config),
            client_server_name: Some(server_name),
            client_local: Some(local),
            client_remote: Some(remote),
            io_table: StreamIoTable::new(),
        }
    }

    fn connection_mut(&mut self) -> RuntimeResult<&mut Connection> {
        self.connection
            .as_mut()
            .ok_or_else(|| QuicWorkerError::ConnectionMissing.into())
    }
}

#[repr(C)]
struct ConnectionContext {
    engine: Option<Box<EngineConnection>>,
    lower_session: SessionId,
    connection_session: Option<SessionId>,
    listener: Option<ContextId>,
    state: ConnectionState,
    flags: u8,
    reserved: [u8; 6],
}

#[repr(C)]
struct StreamContext {
    parent: Index,
    session: SessionId,
    stream: quinn_proto::StreamId,
    bytes_written: u64,
    app_tx_data_len: u64,
    flags: u8,
    reserved: [u8; 15],
}

#[repr(C)]
enum ContextRole {
    Listener(ListenerContext),
    Connection(ConnectionContext),
    Stream(StreamContext),
}

/// Cache-line-aligned QUIC worker context.
#[repr(align(64))]
pub(super) struct Context {
    role: ContextRole,
}

impl Context {
    pub(super) fn listener(
        outer_listener: SessionListenerId,
        outer_application: ApplicationId,
        inner_application_listener: hammer_runtime::app::ApplicationListenerId,
        inner_session_listener: SessionListenerId,
        configuration: ConfigId,
        connection_timeout: u32,
        server_config: Option<Arc<quinn_proto::ServerConfig>>,
    ) -> Self {
        Self {
            role: ContextRole::Listener(ListenerContext {
                outer_listener,
                outer_application,
                inner_application_listener,
                inner_session_listener,
                configuration,
                connection_timeout,
                server_config,
            }),
        }
    }

    pub(super) fn listener_context(&self) -> Option<ListenerContext> {
        match &self.role {
            ContextRole::Listener(listener) => Some(listener.clone()),
            ContextRole::Connection(_) | ContextRole::Stream(_) => None,
        }
    }

    fn connection_with_listener(
        lower_session: SessionId,
        listener: Option<ContextId>,
        application: ApplicationId,
        listener_context: Option<&ListenerContext>,
        app: Option<SessionAppId>,
        app_opaque: Option<u64>,
    ) -> Self {
        let server_config = listener_context.and_then(|listener| listener.server_config.clone());
        Self {
            role: ContextRole::Connection(ConnectionContext {
                engine: Some(Box::new(EngineConnection::pending(
                    server_config,
                    application,
                    app,
                    app_opaque,
                    None,
                    listener_context.map(|listener| listener.outer_listener),
                ))),
                lower_session,
                connection_session: None,
                listener,
                state: ConnectionState::Handshaking,
                flags: 0,
                reserved: [0; 6],
            }),
        }
    }

    fn connection_with_client(
        lower_session: SessionId,
        application_connection: SessionConnectionId,
        application: ApplicationId,
        app: Option<SessionAppId>,
        app_opaque: Option<u64>,
        config: Arc<quinn_proto::ClientConfig>,
        server_name: String,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Self {
        Self {
            role: ContextRole::Connection(ConnectionContext {
                engine: Some(Box::new(EngineConnection::client(
                    config,
                    server_name,
                    local,
                    remote,
                    application,
                    app,
                    app_opaque,
                    Some(application_connection),
                ))),
                lower_session,
                connection_session: None,
                listener: None,
                state: ConnectionState::Handshaking,
                flags: 0,
                reserved: [0; 6],
            }),
        }
    }

    fn stream(parent: Index, session: SessionId, stream: quinn_proto::StreamId) -> Self {
        Self {
            role: ContextRole::Stream(StreamContext {
                parent,
                session,
                stream,
                bytes_written: 0,
                app_tx_data_len: 0,
                flags: 0,
                reserved: [0; 15],
            }),
        }
    }

    fn lower_session(&self) -> Option<SessionId> {
        match &self.role {
            ContextRole::Connection(connection) => Some(connection.lower_session),
            ContextRole::Stream(_) | ContextRole::Listener(_) => None,
        }
    }

    fn transport_session(&self) -> Option<SessionId> {
        match &self.role {
            ContextRole::Connection(connection) => connection.connection_session,
            ContextRole::Stream(stream) => Some(stream.session),
            ContextRole::Listener(_) => None,
        }
    }

    fn connection_index(&self, index: Index) -> Option<Index> {
        match &self.role {
            ContextRole::Connection(_) => Some(index),
            ContextRole::Stream(stream) => Some(stream.parent),
            ContextRole::Listener(_) => None,
        }
    }

    fn connection(&self) -> Option<&ConnectionContext> {
        match &self.role {
            ContextRole::Connection(connection) => Some(connection),
            ContextRole::Listener(_) | ContextRole::Stream(_) => None,
        }
    }

    fn connection_mut(&mut self) -> Option<&mut ConnectionContext> {
        match &mut self.role {
            ContextRole::Connection(connection) => Some(connection),
            ContextRole::Listener(_) | ContextRole::Stream(_) => None,
        }
    }

    fn engine_mut(&mut self, context: ContextId) -> RuntimeResult<&mut EngineConnection> {
        self.connection_mut()
            .and_then(|connection| connection.engine.as_deref_mut())
            .ok_or_else(|| QuicWorkerError::EngineMissing { context }.into())
    }

    fn stream_ref(&self) -> Option<&StreamContext> {
        match &self.role {
            ContextRole::Stream(stream) => Some(stream),
            ContextRole::Connection(_) | ContextRole::Listener(_) => None,
        }
    }

    fn stream_mut(&mut self) -> Option<&mut StreamContext> {
        match &mut self.role {
            ContextRole::Stream(stream) => Some(stream),
            ContextRole::Listener(_) | ContextRole::Connection(_) => None,
        }
    }
}

const _: () = {
    assert!(std::mem::size_of::<Context>() == 64);
    assert!(std::mem::align_of::<Context>() == 64);
};

#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuicTimerKind {
    Handshake = 0,
    Transmit = 1,
}

impl QuicTimerKind {
    #[inline]
    const fn id(self) -> u32 {
        self as u32
    }

    #[inline]
    const fn from_id(id: u32) -> Option<Self> {
        match id {
            0 => Some(Self::Handshake),
            1 => Some(Self::Transmit),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct QuicTimerToken {
    context: ContextId,
    kind: QuicTimerKind,
}

struct QuicTimers {
    wheel: TimerWheel1t2w2048sl<u32>,
    expired: Vec<u32>,
    pending: VecDeque<QuicTimerToken>,
    last_update: Instant,
}

impl QuicTimers {
    fn new(last_update: Instant) -> Self {
        Self {
            wheel: TimerWheel1t2w2048sl::with_timer_ids(TIMER_EXPIRY_BUDGET, 2),
            expired: Vec::new(),
            pending: VecDeque::new(),
            last_update,
        }
    }

    fn set(
        &mut self,
        context: ContextId,
        kind: QuicTimerKind,
        interval: Duration,
    ) -> RuntimeResult<()> {
        self.wheel
            .arm_timer(
                context.0 as u32,
                (context.0 >> 32) as u32,
                kind.id(),
                self.duration_ticks(interval),
            )
            .map_err(|_| QuicWorkerError::TimerUpdateFailed { context }.into())
    }

    fn stop(&mut self, context: ContextId, kind: QuicTimerKind) {
        let _ = self
            .wheel
            .cancel_timer(context.0 as u32, (context.0 >> 32) as u32, kind.id());
    }

    fn advance(&mut self, now: Instant) {
        let elapsed_ticks = self.elapsed_ticks(now);
        if elapsed_ticks == 0 {
            return;
        }
        if self.wheel.is_empty() {
            self.fast_forward_empty_wheel(elapsed_ticks);
            return;
        }
        let requested_ticks = elapsed_ticks.min(u128::from(TIMER_MAX_TICKS_PER_UPDATE)) as u32;
        self.expired.clear();
        let tick_before = self.wheel.current_tick();
        self.wheel.expire(requested_ticks, &mut self.expired);
        let consumed_ticks = self.wheel.current_tick() - tick_before;
        assert!(
            consumed_ticks <= u64::from(requested_ticks),
            "QUIC timer wheel must not consume more ticks than requested"
        );
        let consumed_ticks = consumed_ticks as u32;
        self.last_update += TIMER_RESOLUTION * consumed_ticks;
        for payload in self.expired.as_slice() {
            let Some((slot, generation, kind_id)) = self.wheel.take_expired_timer(*payload) else {
                continue;
            };
            let Some(kind) = QuicTimerKind::from_id(kind_id) else {
                continue;
            };
            self.pending.push_back(QuicTimerToken {
                context: ContextId(u64::from(slot) | (u64::from(generation) << 32)),
                kind,
            });
        }
    }

    fn take_pending(&mut self) -> Option<QuicTimerToken> {
        self.pending.pop_front()
    }

    fn duration_ticks(&self, duration: Duration) -> u64 {
        (duration
            .as_nanos()
            .div_ceil(TIMER_RESOLUTION.as_nanos())
            .max(1)
            .min(u64::MAX as u128) as u64)
            .min(TIMER_WHEEL_MAX_INTERVAL_TICKS)
    }

    fn elapsed_ticks(&self, now: Instant) -> u128 {
        let elapsed = now.saturating_duration_since(self.last_update);
        elapsed.as_nanos() / TIMER_RESOLUTION.as_nanos()
    }

    fn fast_forward_empty_wheel(&mut self, elapsed_ticks: u128) {
        let elapsed_nanos = elapsed_ticks * TIMER_RESOLUTION.as_nanos();
        let seconds = (elapsed_nanos / 1_000_000_000) as u64;
        let nanos = (elapsed_nanos % 1_000_000_000) as u32;
        self.last_update += Duration::new(seconds, nanos);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QuicRxOutcome {
    Processed,
    Dropped,
}

/// Data Worker-owned QUIC context pool and sans-I/O endpoint.
#[hammer_component_macros::session_transport(
    name = "quic",
    start_listen = crate::listener::start_listen,
    stop_listen = crate::listener::stop_listen,
    connect = crate::listener::connect,
    connect_stream = crate::listener::connect_stream,
)]
pub struct QuicWorker {
    endpoint: Endpoint,
    contexts: Pool<Context>,
    timers: QuicTimers,
    rx_datagrams: [Option<Box<RxDatagramScratch>>; RX_DATAGRAM_BURST],
    rx_packet_descriptors: Vec<PartialDecode>,
    stream_io_events: Vec<crate::stream_io::StreamIoEvent>,
    tx_bufs: BytesBuffer,
    connection_tx_pending: Vec<ContextId>,
    connection_tx_ready: Vec<ContextId>,
    rx_datagram_drops: u64,
    rx_packet_drops: u64,
    stream_data_errors: u64,
}

impl QuicWorker {
    pub fn new(_: DataWorkerId) -> Self {
        Self {
            endpoint: Endpoint::new(Arc::new(EndpointConfig::default()), None, false, None),
            contexts: Pool::with_capacity(QUIC_CONTEXT_CAPACITY),
            timers: QuicTimers::new(Instant::now()),
            rx_datagrams: std::array::from_fn(|_| Some(Box::new(RxDatagramScratch::default()))),
            rx_packet_descriptors: Vec::with_capacity(64),
            stream_io_events: Vec::with_capacity(64),
            tx_bufs: BytesBuffer::with_capacity(TX_PACKET_BURST * MAX_PACKET_SIZE),
            connection_tx_pending: Vec::with_capacity(QUIC_CONTEXT_CAPACITY),
            connection_tx_ready: Vec::with_capacity(QUIC_CONTEXT_CAPACITY),
            rx_datagram_drops: 0,
            rx_packet_drops: 0,
            stream_data_errors: 0,
        }
    }

    pub(super) fn accept_connection(
        &mut self,
        lower_session: SessionId,
        listener_id: ContextId,
        listener: &ListenerContext,
    ) -> RuntimeResult<ContextId> {
        let context = self
            .contexts
            .insert(Context::connection_with_listener(
                lower_session,
                Some(listener_id),
                listener.outer_application,
                Some(listener),
                None,
                None,
            ))
            .map(ContextId::from)
            .ok_or_else(|| QuicWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            })?;
        if let Err(timer) = self.timers.set(
            context,
            QuicTimerKind::Handshake,
            Duration::from_millis(u64::from(listener.connection_timeout)),
        ) {
            return match self.remove_context(context) {
                Ok(()) => Err(timer),
                Err(cleanup) => Err(QuicWorkerError::TimerUpdateCleanupFailed {
                    context,
                    timer,
                    cleanup,
                }
                .into()),
            };
        }
        Ok(context)
    }

    pub(super) fn allocate_client_connect(
        &mut self,
        config: Arc<quinn_proto::ClientConfig>,
        server_name: String,
        local: SocketAddr,
        remote: SocketAddr,
        application: ApplicationId,
        app: Option<SessionAppId>,
        app_opaque: Option<u64>,
        application_connection: SessionConnectionId,
    ) -> RuntimeResult<ContextId> {
        self.allocate_client_connect_with_timeout(
            config,
            server_name,
            local,
            remote,
            application,
            app,
            app_opaque,
            application_connection,
            crate::config::DEFAULT_CONNECTION_TIMEOUT,
        )
    }

    pub(super) fn allocate_client_connect_with_timeout(
        &mut self,
        config: Arc<quinn_proto::ClientConfig>,
        server_name: String,
        local: SocketAddr,
        remote: SocketAddr,
        application: ApplicationId,
        app: Option<SessionAppId>,
        app_opaque: Option<u64>,
        application_connection: SessionConnectionId,
        connection_timeout: u32,
    ) -> RuntimeResult<ContextId> {
        if self.contexts.len() == self.contexts.capacity() {
            return Err(QuicWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            }
            .into());
        }
        let context = self
            .contexts
            .insert(Context::connection_with_client(
                SessionId::from_raw(0),
                application_connection,
                application,
                app,
                app_opaque,
                config,
                server_name,
                local,
                remote,
            ))
            .map(ContextId::from)
            .ok_or_else(|| QuicWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            })?;
        if let Err(error) = self.timers.set(
            context,
            QuicTimerKind::Handshake,
            Duration::from_millis(u64::from(connection_timeout)),
        ) {
            return match self.remove_context(context) {
                Ok(()) => Err(error),
                Err(cleanup) => Err(QuicWorkerError::TimerUpdateCleanupFailed {
                    context,
                    timer: error,
                    cleanup,
                }
                .into()),
            };
        }
        Ok(context)
    }

    pub(super) fn connect_connection(
        &mut self,
        context: ContextId,
        lower_session: SessionId,
        now: Instant,
    ) -> RuntimeResult<ContextId> {
        let context_index = context.into();
        {
            let connection = self
                .contexts
                .get_mut(context_index)
                .and_then(Context::connection_mut)
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
            connection.lower_session = lower_session;
        }
        let (config, server_name, local, remote, io) = {
            let engine = self
                .contexts
                .get_mut(context_index)
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                .engine_mut(context)?;
            (
                engine
                    .client_config
                    .take()
                    .ok_or(QuicWorkerError::ClientConfigurationMissing)?,
                engine
                    .client_server_name
                    .take()
                    .ok_or(QuicWorkerError::ClientConfigurationMissing)?,
                engine
                    .client_local
                    .take()
                    .ok_or(QuicWorkerError::ClientConfigurationMissing)?,
                engine
                    .client_remote
                    .take()
                    .ok_or(QuicWorkerError::ClientConfigurationMissing)?,
                engine.io_table.io(),
            )
        };
        let (handle, mut connection) =
            match self
                .endpoint
                .connect(now, config.as_ref().clone(), remote, &server_name)
            {
                Ok(connection) => connection,
                Err(source) => {
                    return match self.remove_context(context) {
                        Ok(()) => {
                            Err(QuicWorkerError::ClientConnectFailed { context, source }.into())
                        }
                        Err(cleanup) => Err(QuicWorkerError::ClientConnectCleanupFailed {
                            context,
                            connect: source,
                            cleanup,
                        }
                        .into()),
                    };
                }
            };
        connection.set_stream_data_io(Some(io));
        {
            let engine = self
                .contexts
                .get_mut(context_index)
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                .engine_mut(context)?;
            engine.handle = Some(handle);
            engine.connection = Some(connection);
            engine.remote = Some(remote);
            engine.local = Some(local);
        }
        Ok(context)
    }

    pub(super) fn connect_stream(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        parent: SessionHandle,
        connection: SessionConnectionId,
        flags: SessionFlags,
    ) -> RuntimeResult<SessionId> {
        let parent_session = sessions
            .session_id_from_handle(parent)
            .ok_or(QuicWorkerError::ParentSessionMissing { parent })?;
        let (transport, parent_index) = sessions
            .session_transport(parent_session)
            .ok_or(QuicWorkerError::ParentSessionInvalid { parent })?;
        if transport != Self::ID
            || !self
                .contexts
                .get(parent_index)
                .is_some_and(|context| context.connection().is_some())
        {
            return Err(QuicWorkerError::ParentSessionInvalid { parent }.into());
        }

        let child_index = self
            .contexts
            .insert(Context::stream(
                parent_index,
                SessionId::from_raw(0),
                quinn_proto::StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0),
            ))
            .ok_or_else(|| QuicWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            })?;
        let child_session = match sessions.stream_connect_pending(Self::ID, child_index, connection)
        {
            Ok(session) => session,
            Err(error) => {
                let cleanup = self.remove_context(ContextId::from(child_index)).err();
                return match cleanup {
                    Some(cleanup) => Err(QuicWorkerError::StreamConnectCleanupFailed {
                        context: ContextId::from(child_index),
                        primary: error,
                        cleanup,
                    }
                    .into()),
                    None => Err(error),
                };
            }
        };
        let direction = if flags.contains(SessionFlags::UNIDIRECTIONAL) {
            quinn_proto::Dir::Uni
        } else {
            quinn_proto::Dir::Bi
        };
        let stream = self
            .contexts
            .get_mut(parent_index)
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing {
                context: ContextId::from(parent_index),
            })?
            .engine
            .as_mut()
            .ok_or_else(|| QuicWorkerError::EngineMissing {
                context: ContextId::from(parent_index),
            })?
            .connection_mut()?
            .streams()
            .open(direction)
            .ok_or_else(|| QuicWorkerError::StreamLimitReached {
                context: ContextId::from(parent_index),
                direction,
            })
            .map_err(RuntimeError::from);
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                let session_cleanup = sessions.rollback_session_creation(child_session).err();
                let context_cleanup = self.remove_context(ContextId::from(child_index)).err();
                let cleanup = session_cleanup.or(context_cleanup);
                return match cleanup {
                    Some(cleanup) => Err(QuicWorkerError::StreamConnectCleanupFailed {
                        context: ContextId::from(child_index),
                        primary: error,
                        cleanup,
                    }
                    .into()),
                    None => Err(error),
                };
            }
        };

        // Builtin and external Session FIFOs are owned by SessionEntry;
        // AppSession attachment is only publication metadata.
        let (rx_fifo, tx_fifo) = sessions
            .fifo_pair(child_session)
            .map(|(rx, tx)| (Arc::clone(rx), Arc::clone(tx)))
            .ok_or(QuicWorkerError::SessionMissing {
                session: child_session,
            })?;
        let app_tx_data_len = tx_fifo.max_dequeue() as u64;
        self.contexts
            .get_mut(child_index)
            .and_then(Context::stream_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing {
                context: ContextId::from(child_index),
            })?
            .stream = stream;
        self.contexts
            .get_mut(child_index)
            .and_then(Context::stream_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing {
                context: ContextId::from(child_index),
            })?
            .session = child_session;
        self.contexts
            .get_mut(parent_index)
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing {
                context: ContextId::from(parent_index),
            })?
            .engine
            .as_mut()
            .ok_or_else(|| QuicWorkerError::EngineMissing {
                context: ContextId::from(parent_index),
            })?
            .io_table
            .install_stream(
                stream,
                child_index,
                child_session,
                rx_fifo,
                tx_fifo,
                0,
                app_tx_data_len,
            );
        if let Err(error) = sessions.complete_stream_connect(child_session) {
            let _ = self
                .contexts
                .get_mut(parent_index)
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .map(|engine| engine.io_table.remove_stream(stream));
            let _ = self.remove_context(ContextId::from(child_index));
            return Err(error);
        }
        Ok(child_session)
    }

    /// Opens one stream child of an established QUIC connection Session;
    /// mirrors VPP `session_open_stream` (session.c:1393) driving the QUIC
    /// transport's `quic_connect_stream` (quic.c:164). `parent` must be
    /// owned by a QUIC connection context (never a stream) whose worker-local
    /// connection is live (quic.c:175-206); the child inherits the parent's
    /// application endpoint and carries the supplied `app_context` as both
    /// its Session App context and its VPP `opaque` (`s->opaque = sep->opaque`,
    /// session.c:1410). The Application Connection is registered here on
    /// `applications` exactly like the CONNECT_STREAM control path
    /// (session/control.rs `application_connect`), which also rolls it back
    /// when the transport connect fails; every validation precedes
    /// registration, and failures after registration roll the connection
    /// back so no orphaned Application connection, Session, or context is
    /// left behind. The remaining open work reuses `connect_stream` and its
    /// rollback paths.
    #[allow(dead_code)] // consumed by the upcoming session action slice
    pub(super) fn open_stream(
        &mut self,
        applications: &ApplicationMain,
        sessions: &mut SessionWorker<Index>,
        parent: SessionId,
        direction: SessionStreamDirection,
        app_context: SessionAppContext,
    ) -> RuntimeResult<SessionId> {
        let parent_handle = sessions.session_handle(parent);
        let (transport, parent_index) =
            sessions
                .session_transport(parent)
                .ok_or(QuicWorkerError::ParentSessionMissing {
                    parent: parent_handle,
                })?;
        if transport != Self::ID
            || !self.contexts.get(parent_index).is_some_and(|context| {
                context.connection().is_some_and(|connection| {
                    connection
                        .engine
                        .as_deref()
                        .is_some_and(|engine| engine.connection.is_some())
                })
            })
        {
            return Err(QuicWorkerError::ParentSessionInvalid {
                parent: parent_handle,
            }
            .into());
        }

        let (application, app, _parent_opaque, server_name) = sessions
            .session_app_endpoint(parent)
            .ok_or(QuicWorkerError::SessionMissing { session: parent })?;
        // VPP session_open_stream hands the stream to the parent's app worker
        // (quic.c:230-231 copies `qctx->parent_app_wrk_id`); the parent's
        // allocation owner is inherited by the child Session through the
        // registered Application Connection (stream_connect_pending reads
        // `connection.context()` as the allocation owner). It must exist
        // before any allocation so a missing owner fails typed instead of
        // registering a zeroed connection.
        let owner = sessions.session_allocation_owner(parent).ok_or(
            QuicWorkerError::ParentAllocationOwnerMissing {
                parent: parent_handle,
            },
        )?;
        let server_name = server_name.map(str::to_owned);
        let connection = applications
            .register_connection(application, owner, server_name, app, Some(app_context))
            .map_err(|error| RuntimeError::subsystem("application", error))?;
        let flags = match direction {
            SessionStreamDirection::Bidi => SessionFlags::STREAM,
            SessionStreamDirection::Uni => SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL,
        };
        let child = match self.connect_stream(
            sessions,
            parent_handle,
            SessionConnectionId::from_raw(connection.raw()),
            flags,
        ) {
            Ok(child) => child,
            Err(primary) => {
                // VPP session.c:1425-1433: when the transport fails to open
                // the stream the Session is freed and the app worker is
                // notified so its connection object is dropped; the
                // registered Application Connection is that counterpart and
                // must not be left behind.
                let cleanup = applications
                    .remove_connection(application, connection)
                    .map_err(|error| RuntimeError::subsystem("application", error))
                    .err();
                return match cleanup {
                    Some(cleanup) => Err(QuicWorkerError::OpenStreamCleanupFailed {
                        parent: parent_handle,
                        primary,
                        cleanup,
                    }
                    .into()),
                    None => Err(primary),
                };
            }
        };
        if let Err(primary) = sessions.set_app_session(child, app_context) {
            // VPP session.c:1425-1433 frees the stream Session and its
            // transport context before the app worker is notified so its
            // connection object is dropped; mirror that reverse ownership
            // order (Session rollback, context removal, Application
            // Connection removal), attempting each step independently so
            // one cleanup failure does not skip the later steps. The child
            // context index is resolved up front O(1) so context removal
            // runs even when the Session rollback itself fails; the
            // aggregation matches connect_stream's rollback path above.
            let child_index = sessions.session_transport(child).map(|(_, index)| index);
            let session_cleanup = sessions.rollback_session_creation(child).err();
            let context_cleanup = match child_index {
                Some(index) => self.remove_context(ContextId::from(index)).err(),
                None => None,
            };
            let connection_cleanup = applications
                .remove_connection(application, connection)
                .map_err(|error| RuntimeError::subsystem("application", error))
                .err();
            let cleanup = session_cleanup.or(context_cleanup).or(connection_cleanup);
            return match cleanup {
                Some(cleanup) => Err(QuicWorkerError::OpenStreamCleanupFailed {
                    parent: parent_handle,
                    primary,
                    cleanup,
                }
                .into()),
                None => Err(primary),
            };
        }
        Ok(child)
    }

    /// Resolves `session` through the Session Worker into this worker's QUIC
    /// context index in O(1) (no pool scan). Rejects sessions unknown to the
    /// Session Worker, owned by another transport, or not backed by a stream
    /// or connection context.
    #[allow(dead_code)] // consumed by the upcoming session action slice
    fn session_context(
        &self,
        sessions: &SessionWorker<Index>,
        session: SessionId,
    ) -> Result<ContextId, QuicWorkerError> {
        let (transport, index) = sessions
            .session_transport(session)
            .ok_or(QuicWorkerError::SessionMissing { session })?;
        if transport != Self::ID {
            return Err(QuicWorkerError::SessionNotQuic { session });
        }
        let context = ContextId::from(index);
        if !self
            .contexts
            .get(index)
            .is_some_and(|context| context.connection().is_some() || context.stream_ref().is_some())
        {
            return Err(QuicWorkerError::ContextMissing { context });
        }
        Ok(context)
    }

    /// Checked conversion of a Session application error code to a QUIC
    /// varint. VPP stores the code in `quic_app_err_code_t` (signed `i64`,
    /// quic.h:128) and encodes it as a varint through
    /// `QUICLY_ERROR_FROM_APPLICATION_ERROR_CODE`; the varint range is
    /// `0..2^62-1`, so anything at or above `2^62` is rejected.
    #[allow(dead_code)] // consumed by the upcoming session action slice
    fn app_error_code_varint(
        session: SessionId,
        code: SessionApplicationErrorCode,
    ) -> Result<VarInt, QuicWorkerError> {
        VarInt::try_from(QuicAppErrorCode(code)).map_err(|invalid| {
            QuicWorkerError::ApplicationErrorCodeInvalid {
                session,
                code: invalid,
            }
        })
    }

    pub(super) fn lower_session(&self, context: ContextId) -> RuntimeResult<SessionId> {
        self.contexts
            .get(context.into())
            .and_then(Context::lower_session)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context }.into())
    }

    pub(super) fn lower_session_if_present(&self, context: ContextId) -> Option<SessionId> {
        self.contexts
            .get(context.into())
            .and_then(Context::lower_session)
    }

    pub(super) fn listener_context_id(&self, context: ContextId) -> Option<ContextId> {
        self.contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.listener)
    }

    pub(super) fn connection_index(&self, index: Index) -> RuntimeResult<Index> {
        self.contexts
            .get(index)
            .and_then(|context| context.connection_index(index))
            .ok_or_else(|| {
                QuicWorkerError::ContextMissing {
                    context: ContextId::from(index),
                }
                .into()
            })
    }

    pub(super) fn remove_context(&mut self, context: ContextId) -> RuntimeResult<()> {
        self.timers.stop(context, QuicTimerKind::Handshake);
        self.timers.stop(context, QuicTimerKind::Transmit);
        self.contexts
            .remove(context.into())
            .map(drop)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context }.into())
    }

    pub(super) fn process_udp_rx(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        lower_session: SessionId,
        context: ContextId,
        now: Instant,
    ) -> RuntimeResult<()> {
        let mut consumed = 0usize;
        for datagram_slot in 0..RX_DATAGRAM_BURST {
            let (header, payload_len, record_len) = {
                let (rx_fifo, _) = sessions.fifo_pair(lower_session).ok_or_else(|| {
                    QuicWorkerError::SessionMissing {
                        session: lower_session,
                    }
                })?;
                if rx_fifo.max_dequeue() < SessionDgramHeader::SIZE {
                    break;
                }
                let mut header_bytes = [0u8; SessionDgramHeader::SIZE];
                if rx_fifo.peek(0, header_bytes.len(), &mut header_bytes) != header_bytes.len() {
                    break;
                }
                let Some(header) = SessionDgramHeader::from_bytes(&header_bytes) else {
                    rx_fifo.dequeue_drop(SessionDgramHeader::SIZE);
                    self.rx_datagram_drops += 1;
                    consumed = consumed.saturating_add(SessionDgramHeader::SIZE);
                    continue;
                };
                let payload_len = header.data_length() as usize;
                let Some(record_len) = header.total_len() else {
                    rx_fifo.dequeue_drop(SessionDgramHeader::SIZE);
                    self.rx_datagram_drops += 1;
                    consumed = consumed.saturating_add(SessionDgramHeader::SIZE);
                    continue;
                };
                if rx_fifo.max_dequeue() < record_len {
                    break;
                }
                if payload_len > MAX_PACKET_SIZE {
                    rx_fifo.dequeue_drop(record_len);
                    self.rx_datagram_drops += 1;
                    consumed = consumed.saturating_add(record_len);
                    continue;
                }
                (header, payload_len, record_len)
            };
            let Some(mut scratch) = self.rx_datagrams[datagram_slot].take() else {
                // The scratch slot is still owned by an earlier burst
                // iteration; the datagram cannot be staged. Drop the record
                // and report the node error instead of panicking.
                if let Some((rx_fifo, _)) = sessions.fifo_pair(lower_session) {
                    rx_fifo.dequeue_drop(record_len);
                }
                return Err(QuicWorkerError::RxDatagramScratchBusy {
                    context,
                    slot: datagram_slot,
                }
                .into());
            };
            let copied = match sessions.fifo_pair(lower_session) {
                Some((rx_fifo, _)) => rx_fifo.peek(
                    SessionDgramHeader::SIZE,
                    payload_len,
                    &mut scratch.data[..payload_len],
                ),
                None => {
                    self.rx_datagrams[datagram_slot] = Some(scratch);
                    return Err(QuicWorkerError::SessionMissing {
                        session: lower_session,
                    }
                    .into());
                }
            };
            if copied != payload_len {
                self.rx_datagrams[datagram_slot] = Some(scratch);
                break;
            }
            let remote = header.remote();
            let local = header.local();
            let mut packet_descriptors = std::mem::take(&mut self.rx_packet_descriptors);
            let process_result = self.process_one_datagram(
                sessions,
                context,
                local,
                remote,
                &mut scratch.data[..payload_len],
                &mut packet_descriptors,
                now,
            );
            self.rx_packet_descriptors = packet_descriptors;
            self.rx_datagrams[datagram_slot] = Some(scratch);
            let outcome = match process_result {
                Ok(outcome) => outcome,
                Err(error) => {
                    if let Some((rx_fifo, _)) = sessions.fifo_pair(lower_session) {
                        debug_assert_eq!(rx_fifo.dequeue_drop(record_len), record_len);
                    }
                    return Err(error);
                }
            };
            if outcome == QuicRxOutcome::Dropped {
                self.rx_datagram_drops += 1;
            }
            let (rx_fifo, _) = sessions.fifo_pair(lower_session).ok_or_else(|| {
                QuicWorkerError::SessionMissing {
                    session: lower_session,
                }
            })?;
            let dropped = rx_fifo.dequeue_drop(record_len);
            debug_assert_eq!(dropped, record_len);
            consumed = consumed.saturating_add(dropped);
            self.drain_io_events(sessions, context)?;
        }
        if consumed != 0 {
            sessions.publish_rx_dequeue(lower_session, consumed)?;
        }
        self.schedule_connection_outputs(sessions, now)?;
        Ok(())
    }

    fn process_one_datagram(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        local: SocketAddr,
        remote: SocketAddr,
        data: &mut [u8],
        packet_descriptors: &mut Vec<PartialDecode>,
        now: Instant,
    ) -> RuntimeResult<QuicRxOutcome> {
        let has_connection = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .map(|engine| engine.connection.is_some())
            .unwrap_or(false);
        if !has_connection {
            return self.accept_first_datagram(
                sessions,
                context,
                local,
                remote,
                data,
                packet_descriptors,
                now,
            );
        }
        let mut engine = self
            .contexts
            .get_mut(context.into())
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
            .connection_mut()
            .and_then(|connection| connection.engine.take())
            .ok_or(QuicWorkerError::EngineMissing { context })?;
        engine.remote = Some(remote);
        engine.local = Some(local);
        let application = engine.application;
        let app = engine.app;
        let opaque = engine.app_opaque;
        let stream_error = {
            let (connection, io_table) = (&mut engine.connection, &mut engine.io_table);
            let connection = connection
                .as_mut()
                .ok_or_else(|| QuicWorkerError::ConnectionMissing)?;
            let local_side = connection.side();
            let mut stream_setup = |stream| {
                self.create_stream_context_with_io(
                    sessions,
                    context,
                    stream,
                    stream.initiator() != local_side,
                    application,
                    app,
                    opaque,
                    io_table,
                )
                .map_err(|_| quinn_proto::StreamDataError::StreamMissing { stream })
            };
            connection.handle_datagram_with_stream_setup_scratch(
                now,
                remote,
                None,
                data,
                packet_descriptors,
                &mut stream_setup,
            );
            connection.take_stream_data_error()
        };
        self.contexts
            .get_mut(context.into())
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
            .connection_mut()
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
            .engine = Some(engine);
        if let Some(error) = stream_error {
            self.rx_packet_drops += 1;
            self.handle_stream_data_error(sessions, context, error)?;
            return Ok(QuicRxOutcome::Dropped);
        }
        self.drain_connection_events(sessions, context, now)?;
        Ok(QuicRxOutcome::Processed)
    }

    fn handle_stream_data_error(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        error: quinn_proto::StreamDataError,
    ) -> RuntimeResult<()> {
        self.stream_data_errors = self.stream_data_errors.saturating_add(1);
        self.close_connection(
            sessions,
            context,
            Some(SessionConnectError::LocalResourceExhausted),
        )?;
        Err(QuicWorkerError::StreamData { context, error }.into())
    }

    fn accept_first_datagram(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        local: SocketAddr,
        remote: SocketAddr,
        data: &mut [u8],
        packet_descriptors: &mut Vec<PartialDecode>,
        now: Instant,
    ) -> RuntimeResult<QuicRxOutcome> {
        let server_config = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .and_then(|engine| engine.server_config.clone())
            .ok_or(QuicWorkerError::ServerConfigMissing { context })?;
        self.endpoint
            .set_server_config(Some(Arc::clone(&server_config)));
        // Quinn treats `tx_bufs` as an append-only transmit buffer whose
        // length carries over between calls; the worker owns the scratch
        // (VPP `quic_quicly_send_packets` writes into a per-thread tx buffer
        // from the start each call), so reset it before every output call.
        self.tx_bufs.clear();
        let event = self.endpoint.handle_scratch_with_descriptors(
            now,
            remote,
            Some(local.ip()),
            None,
            data,
            packet_descriptors,
            &mut *self.tx_bufs,
        );
        let Some(event) = event else {
            self.rx_packet_drops += 1;
            return Ok(QuicRxOutcome::Dropped);
        };
        match event {
            DatagramEvent::NewConnection(incoming) => {
                self.tx_bufs.clear();
                let (handle, mut connection) = match self.endpoint.accept(
                    incoming,
                    now,
                    &mut *self.tx_bufs,
                    Some(Arc::clone(&server_config)),
                ) {
                    Ok(accepted) => accepted,
                    Err(_) => {
                        self.rx_packet_drops += 1;
                        return Ok(QuicRxOutcome::Dropped);
                    }
                };
                let engine = self
                    .contexts
                    .get_mut(context.into())
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                    .engine_mut(context)?;
                let io = engine.io_table.io();
                connection.set_stream_data_io(Some(io));
                engine.handle = Some(handle);
                engine.connection = Some(connection);
                engine.remote = Some(remote);
                engine.local = Some(local);
                self.queue_connection_output(context)?;
                self.drain_connection_events(sessions, context, now)?;
                Ok(QuicRxOutcome::Processed)
            }
            DatagramEvent::Response(transmit) => {
                let mut response = [0u8; MAX_PACKET_SIZE];
                response[..transmit.size].copy_from_slice(&self.tx_bufs[..transmit.size]);
                self.send_response(
                    sessions,
                    context,
                    local,
                    remote,
                    &response[..transmit.size],
                    now,
                )?;
                Ok(QuicRxOutcome::Processed)
            }
            DatagramEvent::ConnectionEvent(_, _) => Ok(QuicRxOutcome::Processed),
        }
    }

    fn drain_connection_events(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.drain_connection_events_inner(sessions, context, now)?;
        self.schedule_connection_outputs(sessions, now)
    }

    fn drain_connection_events_inner(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        now: Instant,
    ) -> RuntimeResult<()> {
        for _ in 0..8 {
            let (events, endpoint_events, handled) = {
                let Some(engine_context) = self.contexts.get_mut(context.into()) else {
                    return Err(QuicWorkerError::ContextMissing { context }.into());
                };
                let engine = engine_context.engine_mut(context)?;
                let Some(connection) = engine.connection.as_mut() else {
                    return Ok(());
                };
                let mut events = Vec::new();
                let mut endpoint_events = Vec::new();
                while let Some(event) = connection.poll() {
                    events.push(event);
                }
                while let Some(event) = connection.poll_endpoint_events() {
                    endpoint_events.push(event);
                }
                let mut handled = !events.is_empty() || !endpoint_events.is_empty();
                if let Some(deadline) = connection.poll_timeout()
                    && deadline <= now
                {
                    connection.handle_timeout(now);
                    handled = true;
                }
                (events, endpoint_events, handled)
            };
            for event in events {
                self.handle_connection_event(sessions, context, event)?;
            }
            for event in endpoint_events {
                if let Some(handle) = self
                    .contexts
                    .get(context.into())
                    .and_then(Context::connection)
                    .and_then(|connection| connection.engine.as_ref())
                    .and_then(|engine| engine.handle)
                {
                    self.endpoint.handle_event(handle, event);
                }
            }
            if !handled {
                break;
            }
        }
        if self.contexts.contains_key(context.into()) {
            self.maybe_finalize_connection(sessions, context)?;
        }
        Ok(())
    }

    fn handle_connection_event(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        event: Event,
    ) -> RuntimeResult<()> {
        let mut close_reason = None;
        let mut passive_close = false;
        let mut stream_event = None;
        match event {
            Event::Connected => {
                let (application_connection, listener, outer_listener, state) = self
                    .contexts
                    .get(context.into())
                    .and_then(Context::connection)
                    .map(|connection| {
                        (
                            connection
                                .engine
                                .as_ref()
                                .and_then(|engine| engine.client_opaque),
                            connection.listener,
                            connection
                                .engine
                                .as_ref()
                                .and_then(|engine| engine.outer_listener),
                            connection.state,
                        )
                    })
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                if state != ConnectionState::Handshaking {
                    return Ok(());
                }
                let upper = if let Some(connection) = application_connection {
                    sessions.stream_connect(QuicWorker::ID, context.into(), connection)?
                } else if listener.is_some() {
                    let outer_listener = outer_listener
                        .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                    let upper =
                        sessions.stream_accept(QuicWorker::ID, context.into(), outer_listener)?;
                    sessions.complete_stream_connect(upper)?;
                    upper
                } else {
                    return Err(QuicWorkerError::ContextMissing { context }.into());
                };
                let connection_context = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::connection_mut)
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                connection_context.connection_session = Some(upper);
                connection_context.state = ConnectionState::Established;
                self.timers.stop(context, QuicTimerKind::Handshake);
                if let Some(engine) = connection_context.engine.as_mut() {
                    engine.client_opaque = None;
                }
            }
            Event::HandshakeDataReady => {}
            Event::ConnectionLost { reason } => {
                let mapped_reason: SessionConnectError =
                    QuicConnectionError::from(reason.clone()).into();
                if matches!(
                    reason,
                    ConnectionError::ApplicationClosed(_) | ConnectionError::ConnectionClosed(_)
                ) {
                    let state = self
                        .contexts
                        .get(context.into())
                        .and_then(Context::connection)
                        .map(|connection| connection.state)
                        .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                    match state {
                        ConnectionState::Handshaking => close_reason = Some(mapped_reason),
                        ConnectionState::Established => {
                            self.notify_streams_closing(sessions, context)?;
                            if let Some(session) = self
                                .contexts
                                .get(context.into())
                                .and_then(Context::connection)
                                .and_then(|connection| connection.connection_session)
                                && sessions.has_session(session)
                            {
                                sessions.notify_transport_closing(None, session, context.into())?;
                            }
                            self.contexts
                                .get_mut(context.into())
                                .and_then(Context::connection_mut)
                                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                                .state = ConnectionState::PassiveClosing;
                            passive_close = true;
                        }
                        ConnectionState::ActiveClosing
                        | ConnectionState::PassiveClosing
                        | ConnectionState::PassiveClosingAppClosed
                        | ConnectionState::PassiveClosingQuicClosed
                        | ConnectionState::TransportClosed => {
                            passive_close = true;
                        }
                    }
                } else {
                    close_reason = Some(mapped_reason);
                }
            }
            Event::Stream(event) => stream_event = Some(event),
            Event::DatagramReceived | Event::DatagramsUnblocked => {}
        }
        if let Some(reason) = close_reason {
            self.close_connection(sessions, context, Some(reason))?;
        }
        if passive_close && self.contexts.contains_key(context.into()) {
            self.close_connection(sessions, context, None)?;
        }
        if let Some(event) = stream_event {
            self.handle_stream_event(sessions, context, event)?;
        }
        Ok(())
    }

    fn handle_stream_event(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        event: StreamEvent,
    ) -> RuntimeResult<()> {
        let mut to_create = Vec::new();
        let mut to_check_fin = Vec::new();
        let mut to_close = None;
        let mut writable_context = None;
        let writable = matches!(&event, StreamEvent::Writable { .. });
        match event {
            StreamEvent::Opened { dir } => {
                let mut accepted_streams = Vec::new();
                {
                    let streams = self
                        .contexts
                        .get_mut(context.into())
                        .and_then(Context::connection_mut)
                        .and_then(|connection| connection.engine.as_mut())
                        .and_then(|engine| engine.connection_mut().ok())
                        .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                    while let Some(stream) = streams.streams().accept(dir) {
                        accepted_streams.push(stream);
                    }
                }
                for stream in accepted_streams {
                    let missing = self
                        .contexts
                        .get(context.into())
                        .and_then(Context::connection)
                        .and_then(|connection| connection.engine.as_ref())
                        .map(|engine| engine.io_table.stream_session(stream).is_none())
                        .unwrap_or(true);
                    to_check_fin.push(stream);
                    if missing {
                        to_create.push((stream, true));
                    }
                }
            }
            StreamEvent::Readable { id } | StreamEvent::Writable { id } => {
                let missing = self
                    .contexts
                    .get(context.into())
                    .and_then(Context::connection)
                    .and_then(|connection| connection.engine.as_ref())
                    .map(|engine| engine.io_table.stream_session(id).is_none())
                    .unwrap_or(true);
                if missing {
                    let accepted = self
                        .contexts
                        .get(context.into())
                        .and_then(Context::connection)
                        .and_then(|connection| connection.engine.as_ref())
                        .and_then(|engine| engine.connection.as_ref())
                        .map(|connection| id.initiator() != connection.side())
                        .unwrap_or(true);
                    to_create.push((id, accepted));
                }
                if !writable {
                    to_check_fin.push(id);
                }
                if !missing && writable {
                    writable_context = self
                        .contexts
                        .get(context.into())
                        .and_then(Context::connection)
                        .and_then(|connection| connection.engine.as_ref())
                        .and_then(|engine| {
                            Some((
                                engine.io_table.stream_context(id)?,
                                engine.io_table.stream_session(id)?,
                            ))
                        });
                }
            }
            StreamEvent::Finished { id } => to_close = Some((id, false)),
            StreamEvent::Stopped { id, .. } => to_close = Some((id, true)),
            StreamEvent::Available { .. } => {}
        }
        for (stream, accepted) in to_create {
            self.create_stream_context(sessions, context, stream, accepted)?;
        }
        for stream in to_check_fin {
            self.notify_stream_receive_closed(sessions, context, stream)?;
        }
        if let Some((stream_context, stream_session)) = writable_context {
            self.stream_tx_event(sessions, stream_session, stream_context, Instant::now())?;
        }
        if let Some((stream, reset)) = to_close {
            self.close_stream_context(sessions, context, stream, reset)?;
        }
        Ok(())
    }

    /// Notifies the child Session exactly once when the peer finished sending
    /// on the stream (RX half-close, VPP `quic_quicly_on_receive` check_eos ->
    /// `session_transport_closing_notify`). The engine stream and child
    /// Session stay live; only the notification is deduplicated.
    fn notify_stream_receive_closed(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        stream: quinn_proto::StreamId,
    ) -> RuntimeResult<()> {
        let Some(engine) = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
        else {
            return Ok(());
        };
        let transfer_complete = engine
            .connection_mut()
            .ok()
            .map(|connection| connection.recv_stream(stream).receive_transfer_complete())
            .unwrap_or(false);
        if !transfer_complete {
            return Ok(());
        }
        let Some((stream_context, stream_session)) = engine
            .io_table
            .stream_context(stream)
            .zip(engine.io_table.stream_session(stream))
        else {
            return Ok(());
        };
        let Some(stream) = self
            .contexts
            .get_mut(stream_context)
            .and_then(Context::stream_mut)
        else {
            return Ok(());
        };
        if stream.flags & STREAM_RECV_FIN != 0 {
            return Ok(());
        }
        stream.flags |= STREAM_RECV_FIN;
        sessions.notify_transport_closing(None, stream_session, stream_context)?;
        Ok(())
    }

    fn close_stream_context(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        stream: quinn_proto::StreamId,
        reset: bool,
    ) -> RuntimeResult<()> {
        let stream_context = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.io_table.stream_context(stream))
            .or_else(|| {
                self.contexts
                    .iter()
                    .find_map(|(index, value)| match &value.role {
                        ContextRole::Stream(stream_context)
                            if stream_context.parent == context.into()
                                && stream_context.stream == stream =>
                        {
                            Some(index)
                        }
                        ContextRole::Listener(_)
                        | ContextRole::Connection(_)
                        | ContextRole::Stream(_) => None,
                    })
            })
            .ok_or_else(|| QuicWorkerError::StreamMissing { stream })?;
        let (stream_session, engine_closed) = self
            .contexts
            .get(stream_context)
            .and_then(|value| match &value.role {
                ContextRole::Stream(stream_context) => Some((
                    stream_context.session,
                    stream_context.flags & STREAM_ENGINE_CLOSED != 0,
                )),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or_else(|| QuicWorkerError::StreamMissing { stream })?;
        if engine_closed {
            if sessions.session_app_closed(stream_session) {
                sessions.notify_transport_closed(stream_session, stream_context)?;
                sessions.notify_transport_deleted(stream_session, stream_context)?;
                self.remove_context(ContextId::from(stream_context))?;
            }
            return Ok(());
        }
        if let Some(engine) = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
        {
            engine.io_table.remove_stream(stream);
        }
        if let Some(stream_context) = self
            .contexts
            .get_mut(stream_context)
            .and_then(Context::stream_mut)
        {
            stream_context.flags |= STREAM_ENGINE_CLOSED;
        }
        if sessions.session_app_closed(stream_session) {
            sessions.notify_transport_closed(stream_session, stream_context)?;
            sessions.notify_transport_deleted(stream_session, stream_context)?;
            self.remove_context(ContextId::from(stream_context))?;
        } else if reset {
            sessions.notify_transport_reset(stream_session, stream_context)?;
        } else {
            sessions.notify_transport_closing(None, stream_session, stream_context)?;
        }
        Ok(())
    }

    fn create_stream_context(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        stream: quinn_proto::StreamId,
        accepted: bool,
    ) -> RuntimeResult<()> {
        if self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .map(|engine| engine.io_table.stream_session(stream).is_some())
            .unwrap_or(false)
        {
            return Ok(());
        }
        let connection_context = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        let (application, app, opaque) = connection_context
            .engine
            .as_ref()
            .map(|engine| (engine.application, engine.app, engine.app_opaque))
            .unwrap_or((ApplicationId::new(0, 0), None, None));
        let (stream_context, session_id, rx_fifo, tx_fifo, app_tx_data_len) = self
            .allocate_stream_context(
                sessions,
                context,
                stream,
                accepted,
                application,
                app,
                opaque,
            )?;
        let engine = self
            .contexts
            .get_mut(context.into())
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
            .engine_mut(context)?;
        engine.io_table.install_stream(
            stream,
            stream_context,
            session_id,
            rx_fifo,
            tx_fifo,
            0,
            app_tx_data_len,
        );
        Ok(())
    }

    fn create_stream_context_with_io(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        stream: quinn_proto::StreamId,
        accepted: bool,
        application: ApplicationId,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
        io_table: &mut StreamIoTable,
    ) -> RuntimeResult<()> {
        if io_table.stream_session(stream).is_some() {
            return Ok(());
        }
        let (stream_context, session_id, rx_fifo, tx_fifo, app_tx_data_len) = self
            .allocate_stream_context(
                sessions,
                context,
                stream,
                accepted,
                application,
                app,
                opaque,
            )?;
        io_table.install_stream(
            stream,
            stream_context,
            session_id,
            rx_fifo,
            tx_fifo,
            0,
            app_tx_data_len,
        );
        Ok(())
    }

    fn allocate_stream_context(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        stream: quinn_proto::StreamId,
        accepted: bool,
        application: ApplicationId,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
    ) -> RuntimeResult<(Index, SessionId, Arc<Fifo>, Arc<Fifo>, u64)> {
        let parent = context.into();
        let parent_session = self
            .contexts
            .get(parent)
            .and_then(Context::connection)
            .and_then(|connection| connection.connection_session);
        // Peer-accepted children are external even when the lower Session
        // carries an app identity.
        let child_app = if accepted { None } else { app };
        // VPP `quic_quicly_on_stream_open` inherits the parent connection's
        // app worker (`sctx->parent_app_wrk_id = qctx->parent_app_wrk_id`).
        let allocation_owner = if let Some(parent_session) = parent_session {
            if let Some(owner) = sessions.session_allocation_owner(parent_session) {
                owner
            } else if child_app.is_some() {
                // Internal children: construct_app_transport_session ignores
                // the allocation owner.
                0
            } else {
                return Err(QuicWorkerError::SessionMissing {
                    session: parent_session,
                }
                .into());
            }
        } else if child_app.is_some() {
            0
        } else {
            return Err(QuicWorkerError::ConnectionMissing.into());
        };
        // VPP `quic_quicly_on_stream_open` allocates the stream transport
        // context first (`quic_ctx_alloc`), then binds the stream Session to
        // it (`stream_session->connection_index = sctx->c_c_index`).
        let Some(stream_context) =
            self.contexts
                .insert(Context::stream(parent, SessionId::from_raw(0), stream))
        else {
            return Err(QuicWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            }
            .into());
        };
        let session_id = match sessions.construct_transport_session(
            QuicWorker::ID,
            stream_context,
            allocation_owner,
            application,
            child_app,
            opaque,
            None,
            accepted,
        ) {
            Ok(session_id) => session_id,
            Err(primary) => {
                let cleanup = self.remove_context(ContextId::from(stream_context)).err();
                return Err(match cleanup {
                    Some(cleanup) => QuicWorkerError::StreamConnectCleanupFailed {
                        context: ContextId::from(stream_context),
                        primary,
                        cleanup,
                    }
                    .into(),
                    None => primary,
                });
            }
        };
        self.contexts
            .get_mut(stream_context)
            .and_then(Context::stream_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing {
                context: ContextId::from(stream_context),
            })?
            .session = session_id;
        if accepted {
            let flags = if stream.dir() == quinn_proto::Dir::Uni {
                SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL
            } else {
                SessionFlags::STREAM
            };
            let listener = parent_session
                .map(|session| sessions.session_handle(session))
                .ok_or(QuicWorkerError::ConnectionMissing)?;
            let publication = sessions
                .set_session_flags(session_id, flags)
                .and_then(|()| sessions.pin_accepted_listener(session_id, listener))
                .and_then(|()| sessions.publish_accepted_transport_session(session_id));
            if let Err(primary) = publication {
                // publish_accepted_transport_session already removes the
                // Session on publication failure, so roll back only a
                // still-live child Session.
                let session_cleanup = if sessions.has_session(session_id) {
                    sessions.rollback_session_creation(session_id).err()
                } else {
                    None
                };
                let context_cleanup = self.remove_context(ContextId::from(stream_context)).err();
                let cleanup = session_cleanup.or(context_cleanup);
                return Err(match cleanup {
                    Some(cleanup) => QuicWorkerError::StreamConnectCleanupFailed {
                        context: ContextId::from(stream_context),
                        primary,
                        cleanup,
                    }
                    .into(),
                    None => primary,
                });
            }
        }
        let (rx_fifo, tx_fifo) = sessions
            .app_session(session_id)
            .map(|session| (Arc::clone(session.rx_fifo()), Arc::clone(session.tx_fifo())))
            .ok_or_else(|| QuicWorkerError::SessionMissing {
                session: session_id,
            })?;
        let app_tx_data_len = tx_fifo.max_dequeue() as u64;
        Ok((
            stream_context,
            session_id,
            rx_fifo,
            tx_fifo,
            app_tx_data_len,
        ))
    }

    fn drain_io_events(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
    ) -> RuntimeResult<()> {
        {
            let engine = self
                .contexts
                .get_mut(context.into())
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                .engine_mut(context)?;
            engine.io_table.take_events(&mut self.stream_io_events);
        }
        for event in &self.stream_io_events {
            if event.rx != 0 {
                sessions.publish_rx_enqueue(event.session, event.rx as usize)?;
            }
            if event.tx_deq != 0 {
                sessions.publish_tx_dequeue(event.session, event.tx_deq as usize)?;
            }
            if let Some(stream) =
                self.contexts
                    .get_mut(event.context)
                    .and_then(|value| match &mut value.role {
                        ContextRole::Stream(stream) => Some(stream),
                        ContextRole::Listener(_) | ContextRole::Connection(_) => None,
                    })
            {
                stream.bytes_written = event.bytes_written;
            }
        }
        self.stream_io_events.clear();
        Ok(())
    }

    fn schedule_connection_outputs(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        now: Instant,
    ) -> RuntimeResult<()> {
        std::mem::swap(
            &mut self.connection_tx_pending,
            &mut self.connection_tx_ready,
        );
        while let Some(context) = self.connection_tx_ready.pop() {
            if let Some(connection) = self
                .contexts
                .get_mut(context.into())
                .and_then(Context::connection_mut)
            {
                connection.flags &= !CONNECTION_TX_PENDING;
            }
            self.send_packets(sessions, context, now)?;
        }
        Ok(())
    }

    fn sync_connection_deadline(
        &mut self,
        context: ContextId,
        now: Instant,
        deadline: Option<Instant>,
    ) -> RuntimeResult<bool> {
        match deadline {
            Some(deadline) if deadline <= now => {
                if let Some(connection) = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::connection_mut)
                    .and_then(|connection| connection.engine.as_mut())
                    .and_then(|engine| engine.connection.as_mut())
                {
                    connection.handle_timeout(now);
                }
                self.timers.stop(context, QuicTimerKind::Transmit);
                self.queue_connection_output(context)?;
                Ok(true)
            }
            Some(deadline) => self
                .timers
                .set(
                    context,
                    QuicTimerKind::Transmit,
                    deadline.saturating_duration_since(now),
                )
                .map(|()| false),
            None => {
                self.timers.stop(context, QuicTimerKind::Transmit);
                Ok(false)
            }
        }
    }

    fn queue_connection_output(&mut self, context: ContextId) -> RuntimeResult<()> {
        let connection = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        if connection.flags & CONNECTION_TX_PENDING != 0 {
            return Ok(());
        }
        if self.connection_tx_pending.len() >= self.connection_tx_pending.capacity() {
            return Err(QuicWorkerError::OutputQueueCapacityExceeded {
                context,
                capacity: self.connection_tx_pending.capacity(),
            }
            .into());
        }
        connection.flags |= CONNECTION_TX_PENDING;
        self.connection_tx_pending.push(context);
        Ok(())
    }

    pub(super) fn send_packets(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        now: Instant,
    ) -> RuntimeResult<()> {
        let connection_context = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        let lower_session = connection_context.lower_session;
        let engine = connection_context
            .engine
            .as_deref_mut()
            .ok_or_else(|| QuicWorkerError::EngineMissing { context })?;
        let (remote, local) = match (engine.remote, engine.local) {
            (Some(remote), Some(local)) => (remote, local),
            _ => return Ok(()),
        };
        SessionDgramHeader::new(local, remote, MAX_PACKET_SIZE).ok_or(
            QuicWorkerError::InvalidEndpoint {
                context,
                local,
                remote,
            },
        )?;
        let record_size = SessionDgramHeader::SIZE + MAX_PACKET_SIZE;
        let packet_budget = {
            let (_, tx_fifo) = sessions.fifo_pair(lower_session).ok_or_else(|| {
                QuicWorkerError::SessionMissing {
                    session: lower_session,
                }
            })?;
            let budget = (tx_fifo.max_enqueue() / record_size).min(TX_PACKET_BURST);
            if budget < 2 {
                tx_fifo.want_deq_notification();
                let deadline = engine.connection_mut()?.poll_timeout();
                let deadline_due = self.sync_connection_deadline(context, now, deadline)?;
                if deadline_due {
                    self.drain_connection_events_inner(sessions, context, now)?;
                }
                return Ok(());
            }
            budget
        };
        let mut produced = 0usize;
        for _ in 0..packet_budget {
            // VPP provisions UDP FIFO chunks before `quicly_send`, so a
            // resource failure cannot follow a committed QUIC transmit.
            let mut reservation = {
                let (_, tx_fifo) = sessions.fifo_pair(lower_session).ok_or_else(|| {
                    QuicWorkerError::SessionMissing {
                        session: lower_session,
                    }
                })?;
                tx_fifo.reserve_write(record_size).map_err(|source| {
                    QuicWorkerError::OutputReservationFailed {
                        context,
                        bytes: record_size,
                        source,
                    }
                })?
            };
            // Fresh transmit window per burst: quinn appends to the buffer
            // from its current length, so stale handshake bytes would corrupt
            // the accounting (VPP `quic_quicly_send_packets` resets the
            // per-thread tx buffer before `quicly_send`).
            self.tx_bufs.clear();
            let Some(transmit) = engine
                .connection_mut()?
                .poll_transmit(now, 1, &mut *self.tx_bufs)
            else {
                reservation.cancel();
                break;
            };
            let payload_len = transmit.size;
            if !(1..=MAX_PACKET_SIZE).contains(&payload_len) || payload_len > self.tx_bufs.len() {
                return Err(QuicWorkerError::EnginePacketTooLarge {
                    context,
                    bytes: payload_len,
                }
                .into());
            }
            let header = SessionDgramHeader::new(local, remote, payload_len).ok_or(
                QuicWorkerError::InvalidEndpoint {
                    context,
                    local,
                    remote,
                },
            )?;
            let record_len =
                header
                    .total_len()
                    .ok_or_else(|| QuicWorkerError::InvalidDatagram {
                        session: lower_session,
                        length: payload_len as u32,
                    })?;
            let header_bytes = header.to_bytes();
            let written = reservation
                .copy_from_segments([
                    header_bytes.as_slice(),
                    &self.tx_bufs.as_slice()[..payload_len],
                ])
                .map_err(|source| QuicWorkerError::OutputReservationFailed {
                    context,
                    bytes: record_len,
                    source,
                })?;
            let committed = reservation.commit(written).map_err(|source| {
                QuicWorkerError::OutputReservationFailed {
                    context,
                    bytes: record_len,
                    source,
                }
            })?;
            if committed != record_len {
                return Err(QuicWorkerError::OutputCommitLengthMismatch {
                    context,
                    expected: record_len,
                    actual: committed,
                }
                .into());
            }
            produced = produced.saturating_add(record_len);
        }
        if produced != 0 {
            sessions.publish_tx_enqueue(lower_session, produced)?;
        }
        let stream_data_error = engine.connection_mut()?.take_stream_data_error();
        if let Some(error) = stream_data_error {
            return self.handle_stream_data_error(sessions, context, error);
        }
        let deadline = engine.connection_mut()?.poll_timeout();
        let deadline_due = self.sync_connection_deadline(context, now, deadline)?;
        if deadline_due {
            self.drain_connection_events_inner(sessions, context, now)?;
        }
        Ok(())
    }

    fn send_response(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        local: SocketAddr,
        remote: SocketAddr,
        payload: &[u8],
        now: Instant,
    ) -> RuntimeResult<()> {
        let lower_session = self.lower_session(context)?;
        let header = SessionDgramHeader::new(local, remote, payload.len()).ok_or(
            QuicWorkerError::InvalidEndpoint {
                context,
                local,
                remote,
            },
        )?;
        let header_bytes = header.to_bytes();
        let record_len = header
            .total_len()
            .ok_or_else(|| QuicWorkerError::InvalidDatagram {
                session: lower_session,
                length: payload.len() as u32,
            })?;
        {
            let (_, tx_fifo) = sessions.fifo_pair(lower_session).ok_or_else(|| {
                QuicWorkerError::SessionMissing {
                    session: lower_session,
                }
            })?;
            let mut reservation = tx_fifo.reserve_write(record_len).map_err(|source| {
                QuicWorkerError::OutputReservationFailed {
                    context,
                    bytes: record_len,
                    source,
                }
            })?;
            reservation
                .copy_from_segments([header_bytes.as_slice(), payload])
                .map_err(|source| QuicWorkerError::OutputReservationFailed {
                    context,
                    bytes: record_len,
                    source,
                })?;
            reservation.commit(record_len).map_err(|source| {
                QuicWorkerError::OutputReservationFailed {
                    context,
                    bytes: record_len,
                    source,
                }
            })?;
        }
        sessions.publish_tx_enqueue(lower_session, record_len)?;
        self.timers
            .set(context, QuicTimerKind::Transmit, TIMER_RESOLUTION)
            .map_err(|_| QuicWorkerError::TimerUpdateFailed { context })?;
        let _ = now;
        Ok(())
    }

    pub(super) fn app_rx_evt(&mut self, index: Index, rx_available: usize) -> RuntimeResult<bool> {
        let context = ContextId::from(index);
        let (stream_id, parent) = {
            let stream = self
                .contexts
                .get(index)
                .and_then(|value| match &value.role {
                    ContextRole::Stream(stream) => Some(stream),
                    ContextRole::Listener(_) | ContextRole::Connection(_) => None,
                })
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
            (stream.stream, stream.parent)
        };
        let parent_context = ContextId::from(parent);
        let consumed = self
            .contexts
            .get_mut(parent)
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.io_table.app_rx_consumed(stream_id))
            .unwrap_or(0);
        if consumed != 0 {
            let should_transmit = self
                .contexts
                .get_mut(parent)
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .and_then(|engine| engine.connection_mut().ok())
                .map(|connection| connection.recv_stream(stream_id).credit_read(consumed))
                .unwrap_or(Ok(Default::default()));
            if should_transmit.is_ok() {
                if let Some(engine) = self
                    .contexts
                    .get_mut(parent)
                    .and_then(Context::connection_mut)
                    .and_then(|connection| connection.engine.as_mut())
                {
                    engine.io_table.confirm_app_rx_consumed(stream_id);
                }
            }
            if should_transmit
                .map(|value| value.should_transmit())
                .unwrap_or(false)
            {
                self.queue_connection_output(parent_context)?;
            }
            self.sync_connection_deadline_from_engine(parent_context, Instant::now())?;
        }
        Ok(rx_available == 0)
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.timers.advance(now);
        while let Some(token) = self.timers.take_pending() {
            match token.kind {
                QuicTimerKind::Handshake => {
                    if self.contexts.contains_key(token.context.into()) {
                        self.close_connection(
                            sessions,
                            token.context,
                            Some(SessionConnectError::TimedOut),
                        )?;
                    }
                }
                QuicTimerKind::Transmit => {
                    if let Some(connection) = self
                        .contexts
                        .get_mut(token.context.into())
                        .and_then(Context::connection_mut)
                        .and_then(|connection| connection.engine.as_mut())
                        .and_then(|engine| engine.connection.as_mut())
                    {
                        connection.handle_timeout(now);
                    }
                    self.drain_connection_events(sessions, token.context, now)?;
                }
            }
        }
        self.schedule_connection_outputs(sessions, now)
    }

    fn stream_contexts(&self, parent: ContextId) -> Vec<(Index, SessionId)> {
        let parent = parent.into();
        self.contexts
            .iter()
            .filter_map(|(index, value)| match &value.role {
                ContextRole::Stream(stream) if stream.parent == parent => {
                    Some((index, stream.session))
                }
                ContextRole::Listener(_) | ContextRole::Connection(_) | ContextRole::Stream(_) => {
                    None
                }
            })
            .collect()
    }

    fn connection_is_drained(&self, context: ContextId) -> bool {
        self.contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .and_then(|engine| engine.connection.as_ref())
            .is_some_and(Connection::is_drained)
    }

    fn sync_connection_deadline_from_engine(
        &mut self,
        context: ContextId,
        now: Instant,
    ) -> RuntimeResult<()> {
        let deadline = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.connection.as_mut())
            .and_then(|connection| connection.poll_timeout());
        self.sync_connection_deadline(context, now, deadline)
            .map(|_| ())
    }

    fn notify_streams_reset(
        &self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
    ) -> RuntimeResult<()> {
        for (index, session) in self.stream_contexts(context) {
            if sessions.has_session(session) {
                sessions.notify_transport_reset(session, index)?;
            }
        }
        Ok(())
    }

    fn notify_streams_closing(
        &self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
    ) -> RuntimeResult<()> {
        for (index, session) in self.stream_contexts(context) {
            if sessions.has_session(session) {
                sessions.notify_transport_closing(None, session, index)?;
            }
        }
        Ok(())
    }

    fn finalize_connection(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
    ) -> RuntimeResult<()> {
        let stream_contexts = self.stream_contexts(context);
        for (index, session) in stream_contexts {
            if sessions.has_session(session) {
                sessions.notify_transport_deleted(session, index)?;
            }
            if self.contexts.contains_key(index) {
                self.remove_context(ContextId::from(index))?;
            }
        }

        let connection_session = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.connection_session);
        if let Some(session) = connection_session
            && sessions.has_session(session)
        {
            sessions.notify_transport_deleted(session, context.into())?;
        }
        if let Some(lower_session) = self
            .contexts
            .get(context.into())
            .and_then(Context::lower_session)
        {
            if sessions.has_session(lower_session) {
                sessions.set_app_session(lower_session, 0)?;
                sessions.schedule_disconnect(lower_session);
            }
        }

        self.connection_tx_pending
            .retain(|queued| *queued != context);
        self.connection_tx_ready.retain(|queued| *queued != context);
        self.timers.stop(context, QuicTimerKind::Handshake);
        self.timers.stop(context, QuicTimerKind::Transmit);
        if let Some(engine) = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
        {
            engine.io_table = StreamIoTable::new();
            let _ = engine.connection.take();
        }
        self.remove_context(context)
    }

    fn maybe_finalize_connection(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
    ) -> RuntimeResult<()> {
        let (state, connection_session) = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .map(|connection| (connection.state, connection.connection_session))
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        let application_closed =
            connection_session.is_none_or(|session| sessions.session_app_closed(session));
        if self.connection_is_drained(context)
            && application_closed
            && matches!(
                state,
                ConnectionState::ActiveClosing | ConnectionState::PassiveClosingAppClosed
            )
        {
            return self.finalize_connection(sessions, context);
        }
        self.sync_connection_deadline_from_engine(context, Instant::now())
    }

    fn begin_connection_close(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        now: Instant,
    ) -> RuntimeResult<()> {
        let state = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .map(|connection| connection.state)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        match state {
            ConnectionState::Established => {
                let connection = self
                    .contexts
                    .get_mut(context.into())
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                    .engine_mut(context)?
                    .connection_mut()?;
                connection.close(
                    now,
                    quinn_proto::VarInt::from_u32(0),
                    Bytes::from_static(b"application closed"),
                );
                self.contexts
                    .get_mut(context.into())
                    .and_then(Context::connection_mut)
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                    .state = ConnectionState::ActiveClosing;
                self.queue_connection_output(context)?;
                self.sync_connection_deadline_from_engine(context, now)
            }
            ConnectionState::PassiveClosing | ConnectionState::PassiveClosingQuicClosed => {
                self.contexts
                    .get_mut(context.into())
                    .and_then(Context::connection_mut)
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                    .state = ConnectionState::PassiveClosingAppClosed;
                self.maybe_finalize_connection(sessions, context)
            }
            ConnectionState::ActiveClosing | ConnectionState::PassiveClosingAppClosed => {
                self.maybe_finalize_connection(sessions, context)
            }
            ConnectionState::TransportClosed => self.finalize_connection(sessions, context),
            ConnectionState::Handshaking => Ok(()),
        }
    }

    pub(super) fn transport_closed(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
    ) -> RuntimeResult<()> {
        let (application_connection, state) = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .map(|connection| {
                (
                    connection
                        .engine
                        .as_ref()
                        .and_then(|engine| engine.client_opaque),
                    connection.state,
                )
            })
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        if state == ConnectionState::Handshaking {
            if let Some(connection) = application_connection
                && !sessions
                    .stream_connect_failed(connection, SessionConnectError::ConnectionReset)?
            {
                let engine = self
                    .contexts
                    .get_mut(context.into())
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                    .engine_mut(context)?;
                engine.pending_connect_error = Some(SessionConnectError::ConnectionReset);
                self.timers.stop(context, QuicTimerKind::Transmit);
                self.timers
                    .set(context, QuicTimerKind::Handshake, TIMER_RESOLUTION)?;
                return Ok(());
            }
            return self.finalize_connection(sessions, context);
        }

        self.contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
            .state = ConnectionState::TransportClosed;
        self.notify_streams_reset(sessions, context)?;
        if let Some(session) = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.connection_session)
            && sessions.has_session(session)
        {
            sessions.notify_transport_reset(session, context.into())?;
        }
        self.finalize_connection(sessions, context)
    }

    pub(super) fn close_connection(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        reason: Option<SessionConnectError>,
    ) -> RuntimeResult<()> {
        let (application_connection, pending_error, state) = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .map(|connection| {
                (
                    connection
                        .engine
                        .as_ref()
                        .and_then(|engine| engine.client_opaque),
                    connection
                        .engine
                        .as_ref()
                        .and_then(|engine| engine.pending_connect_error),
                    connection.state,
                )
            })
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        if state == ConnectionState::Handshaking
            && let Some(connection) = application_connection
        {
            let error = pending_error
                .or(reason)
                .unwrap_or(SessionConnectError::LocalClosed);
            if !sessions.stream_connect_failed(connection, error)? {
                let engine = self
                    .contexts
                    .get_mut(context.into())
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                    .engine_mut(context)?;
                engine.pending_connect_error = Some(error);
                self.timers.stop(context, QuicTimerKind::Transmit);
                self.timers
                    .set(context, QuicTimerKind::Handshake, TIMER_RESOLUTION)?;
                return Ok(());
            }
            let engine = self
                .contexts
                .get_mut(context.into())
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                .engine_mut(context)?;
            engine.pending_connect_error = None;
            engine.client_opaque = None;
        }

        if state == ConnectionState::Handshaking {
            return self.finalize_connection(sessions, context);
        }

        if state == ConnectionState::Established {
            self.notify_streams_reset(sessions, context)?;
            if let Some(session) = self
                .contexts
                .get(context.into())
                .and_then(Context::connection)
                .and_then(|connection| connection.connection_session)
                && sessions.has_session(session)
            {
                sessions.notify_transport_reset(session, context.into())?;
            }
            self.contexts
                .get_mut(context.into())
                .and_then(Context::connection_mut)
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                .state = ConnectionState::PassiveClosingQuicClosed;
        } else if state == ConnectionState::PassiveClosing {
            self.contexts
                .get_mut(context.into())
                .and_then(Context::connection_mut)
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                .state = ConnectionState::PassiveClosingQuicClosed;
        } else if state == ConnectionState::TransportClosed {
            return self.finalize_connection(sessions, context);
        }
        self.maybe_finalize_connection(sessions, context)
    }

    pub(super) fn stream_tx_event(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        session_id: SessionId,
        index: Index,
        now: Instant,
    ) -> RuntimeResult<()> {
        let (stream_id, parent, bytes_written, app_close_pending) = {
            let stream = self
                .contexts
                .get(index)
                .and_then(|value| match &value.role {
                    ContextRole::Stream(stream) => Some(stream),
                    ContextRole::Listener(_) | ContextRole::Connection(_) => None,
                })
                .ok_or_else(|| QuicWorkerError::ContextMissing {
                    context: ContextId::from(index),
                })?;
            (
                stream.stream,
                stream.parent,
                stream.bytes_written,
                stream.flags & STREAM_APP_CLOSE_PENDING != 0,
            )
        };
        let parent_context = ContextId::from(parent);
        if !self
            .contexts
            .get(parent)
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .and_then(|engine| engine.connection.as_ref())
            .is_some_and(|connection| stream_has_send_side(connection, stream_id))
        {
            return Ok(());
        }
        let pending = sessions.pending_send_len(session_id)?.unwrap_or(0);
        let end_offset = bytes_written.saturating_add(pending as u64);
        let sync_result = if pending == 0 {
            Ok(())
        } else {
            let connection = self
                .contexts
                .get_mut(parent)
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .and_then(|engine| engine.connection.as_mut())
                .ok_or_else(|| QuicWorkerError::ContextMissing {
                    context: parent_context,
                })?;
            if connection.is_closed() {
                Err(quinn_proto::WriteError::Blocked)
            } else {
                connection
                    .send_stream(stream_id)
                    .sync(end_offset)
                    .map(|_| ())
            }
        };
        match sync_result {
            Ok(()) => {}
            Err(quinn_proto::WriteError::Blocked) => return Ok(()),
            Err(quinn_proto::WriteError::Stopped(_) | quinn_proto::WriteError::ClosedStream) => {
                if app_close_pending
                    && let Some(stream) = self.contexts.get_mut(index).and_then(Context::stream_mut)
                {
                    stream.flags =
                        (stream.flags | STREAM_APP_CLOSED_TX) & !STREAM_APP_CLOSE_PENDING;
                }
                return Ok(());
            }
        }
        if app_close_pending {
            let finish_result = {
                let connection = self
                    .contexts
                    .get_mut(parent)
                    .and_then(Context::connection_mut)
                    .and_then(|connection| connection.engine.as_mut())
                    .and_then(|engine| engine.connection.as_mut())
                    .ok_or_else(|| QuicWorkerError::ContextMissing {
                        context: parent_context,
                    })?;
                connection.send_stream(stream_id).finish()
            };
            match finish_result {
                Ok(())
                | Err(
                    quinn_proto::FinishError::Stopped(_) | quinn_proto::FinishError::ClosedStream,
                ) => {
                    if let Some(stream) = self.contexts.get_mut(index).and_then(Context::stream_mut)
                    {
                        stream.flags =
                            (stream.flags | STREAM_APP_CLOSED_TX) & !STREAM_APP_CLOSE_PENDING;
                    }
                }
            }
        }
        if let Some(stream) = self
            .contexts
            .get_mut(index)
            .and_then(|value| value.stream_mut())
        {
            stream.app_tx_data_len = end_offset;
        }
        self.queue_connection_output(parent_context)?;
        self.schedule_connection_outputs(sessions, now)
    }
}

pub(crate) fn quic_session_queue_update_time(
    _runtime: &DataPlaneRuntime,
    sessions: &mut SessionWorker<Index>,
    _: NodeRuntimeData,
    _: SessionQueueNext,
    now: Instant,
    _: &mut BufferFrame,
    _: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    main.with_worker_and_sessions(sessions, |sessions, quic| quic.update_time(sessions, now))
}

pub(crate) fn quic_session_queue_dispatch(
    runtime: &DataPlaneRuntime,
    sessions: &mut SessionWorker<Index>,
    _: NodeRuntimeData,
    output_next: SessionQueueNext,
    now: Instant,
    frame: &mut BufferFrame,
    output: &mut SessionQueueOutput,
) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    main.with_worker_and_sessions(sessions, |sessions, quic| {
        dispatch_session_queue_events(runtime, sessions, quic, output_next, frame, output, now)
            .map(|_| ())
    })
}

/// Opens one stream child of `parent` through the Session Worker's
/// worker-local transport action table; the `open_stream` callback installed
/// by `QuicMain::install_transport_actions` (listener.rs). Mirrors VPP
/// resolving the transport VFT and invoking `quic_connect_stream`
/// (transport.c:500-505, quic.c:164) from the worker-local registration:
/// the Session Worker resolves the owning worker from its own
/// `DataWorkerId` in O(1), and the QUIC Main (with the Application
/// authority) comes from the same QUIC_MAIN channel the session queue
/// entry points use. No scan, allocation, or lock on the dispatch path.
pub(crate) fn quic_transport_open_stream(
    sessions: &mut SessionWorker<Index>,
    parent: SessionId,
    direction: SessionStreamDirection,
    app_context: SessionAppContext,
) -> RuntimeResult<SessionId> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    let applications = main.applications();
    main.with_worker_and_sessions(sessions, |sessions, quic| {
        quic.open_stream(applications, sessions, parent, direction, app_context)
    })
}

/// Builds the QUIC transport's worker-local action table, installed once per
/// owning worker at the bind point (`bind_worker_graph` in listener.rs).
/// Mirrors VPP registering the transport VFT with
/// `.connect_stream = quic_connect_stream` (quic.c:902) and dispatch
/// resolving it from the session entry's transport proto
/// (session.c:1422, transport.c:500-505).
///
/// Temporary constraint: `SessionTransportWorkerActions::new` requires all
/// four fn pointers, but this slice implements only `open_stream` — nothing
/// in the QUIC worker invokes `SessionWorker::reset_stream`, `stop_sending`
/// or `close_connection` yet, so dispatch of those three slots is
/// unreachable. Each slot therefore holds a minimal typed stub that returns
/// an explicit `TransportActionUnsupported` error with zero side effects;
/// replace each stub with the real action in the sub-seam that introduces
/// its dispatch.
pub(crate) fn quic_transport_actions() -> SessionTransportWorkerActions<Index> {
    SessionTransportWorkerActions::new(
        quic_transport_open_stream,
        transport_reset_unsupported,
        transport_stop_sending_unsupported,
        transport_close_connection_unsupported,
    )
}

/// Minimal typed stub for the unreachable `reset_stream` slot (see
/// `quic_transport_actions`): explicit unsupported error, no side effects.
fn transport_reset_unsupported(
    _sessions: &mut SessionWorker<Index>,
    _session: SessionId,
    _code: SessionApplicationErrorCode,
) -> RuntimeResult<()> {
    Err(RuntimeError::from(
        QuicWorkerError::TransportActionUnsupported {
            action: "reset_stream",
        },
    ))
}

/// Minimal typed stub for the unreachable `stop_sending` slot (see
/// `quic_transport_actions`): explicit unsupported error, no side effects.
fn transport_stop_sending_unsupported(
    _sessions: &mut SessionWorker<Index>,
    _session: SessionId,
    _code: SessionApplicationErrorCode,
) -> RuntimeResult<()> {
    Err(RuntimeError::from(
        QuicWorkerError::TransportActionUnsupported {
            action: "stop_sending",
        },
    ))
}

/// Minimal typed stub for the unreachable `close_connection` slot (see
/// `quic_transport_actions`): explicit unsupported error, no side effects.
fn transport_close_connection_unsupported(
    _sessions: &mut SessionWorker<Index>,
    _connection: SessionId,
    _code: SessionApplicationErrorCode,
    _reason: &[u8],
) -> RuntimeResult<()> {
    Err(RuntimeError::from(
        QuicWorkerError::TransportActionUnsupported {
            action: "close_connection",
        },
    ))
}

impl QuicWorker {
    fn begin_stream_close(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        now: Instant,
    ) -> RuntimeResult<()> {
        let (stream, parent, bytes_written, session, flags) = self
            .contexts
            .get(context.into())
            .and_then(|value| match &value.role {
                ContextRole::Stream(stream) => Some((
                    stream.stream,
                    stream.parent,
                    stream.bytes_written,
                    stream.session,
                    stream.flags,
                )),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        if flags & STREAM_ENGINE_CLOSED != 0 {
            sessions.notify_transport_closed(session, context.into())?;
            sessions.notify_transport_deleted(session, context.into())?;
            return self.remove_context(context);
        }
        if flags & STREAM_APP_CLOSED_TX != 0 {
            return Ok(());
        }
        let pending = sessions.pending_send_len(session)?.unwrap_or(0);
        let end_offset = bytes_written.saturating_add(pending as u64);
        let parent_context = ContextId::from(parent);
        let sync_result = {
            let connection = self
                .contexts
                .get_mut(parent)
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .and_then(|engine| engine.connection.as_mut())
                .ok_or_else(|| QuicWorkerError::ContextMissing {
                    context: parent_context,
                })?;
            if !stream_has_send_side(connection, stream) {
                return Ok(());
            }
            if connection.is_closed() {
                Err(quinn_proto::WriteError::Blocked)
            } else {
                connection.send_stream(stream).sync(end_offset)
            }
        };
        match sync_result {
            Ok(_) => {}
            Err(quinn_proto::WriteError::Blocked) => {
                if let Some(stream) = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::stream_mut)
                {
                    stream.flags |= STREAM_APP_CLOSE_PENDING;
                }
                return Ok(());
            }
            Err(quinn_proto::WriteError::Stopped(_) | quinn_proto::WriteError::ClosedStream) => {
                if let Some(stream) = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::stream_mut)
                {
                    stream.flags |= STREAM_APP_CLOSED_TX;
                }
                return Ok(());
            }
        }
        let finish_result = {
            let connection = self
                .contexts
                .get_mut(parent)
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .and_then(|engine| engine.connection.as_mut())
                .ok_or_else(|| QuicWorkerError::ContextMissing {
                    context: parent_context,
                })?;
            connection.send_stream(stream).finish()
        };
        match finish_result {
            Ok(()) => {}
            Err(quinn_proto::FinishError::Stopped(_) | quinn_proto::FinishError::ClosedStream) => {
                if let Some(stream) = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::stream_mut)
                {
                    stream.flags |= STREAM_APP_CLOSED_TX;
                }
                return Ok(());
            }
        }
        if let Some(stream) = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::stream_mut)
        {
            stream.app_tx_data_len = end_offset;
            stream.flags = (stream.flags | STREAM_APP_CLOSED_TX) & !STREAM_APP_CLOSE_PENDING;
        }
        self.queue_connection_output(parent_context)?;
        self.schedule_connection_outputs(sessions, now)
    }
}

impl SessionTransport<Index> for QuicWorker {
    type Tx = TransportInternalTx;

    const ID: SessionTransportId = SessionTransportId::new(3);

    fn connection_index(&self, index: Index) -> RuntimeResult<Index> {
        self.connection_index(index)
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.update_time(sessions, now)
    }

    fn app_rx_evt(
        &mut self,
        index: Index,
        rx_available: usize,
        _: usize,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
    ) -> RuntimeResult<bool> {
        self.app_rx_evt(index, rx_available)
    }

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        let context = ContextId::from(index);
        let role = self
            .contexts
            .get(index)
            .map(|value| matches!(value.role, ContextRole::Stream(_)))
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        if role {
            return self.begin_stream_close(sessions, context, now);
        }

        let session = self
            .contexts
            .get(index)
            .and_then(Context::transport_session);
        if session.is_none_or(|session| sessions.session_app_closed(session)) {
            let state = self
                .contexts
                .get(index)
                .and_then(Context::connection)
                .map(|connection| connection.state)
                .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
            if state == ConnectionState::Handshaking {
                return self.close_connection(
                    sessions,
                    context,
                    Some(SessionConnectError::LocalClosed),
                );
            }
            self.begin_connection_close(sessions, context, now)?;
        }
        Ok(())
    }

    fn reset(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        index: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        let context = ContextId::from(index);
        let role = self
            .contexts
            .get(index)
            .map(|value| matches!(value.role, ContextRole::Stream(_)))
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        if !role {
            // VPP `quic_quicly_on_app_reset` refuses connection-level reset
            // (QUIC_ERR "Trying to reset connection"): it is not folded into
            // `close`.
            return Ok(());
        }
        // VPP `SESSION_CTRL_EVT_RESET` -> `session_transport_reset` ->
        // `quic_quicly_on_app_reset`: STOP_SENDING on the RX side when open
        // and not transfer-complete, RESET_STREAM on the TX side when open
        // and not transfer-complete, then mark the app-closed state and
        // queue connection output. The child Session and stream context stay
        // live; only a destroyed engine stream (`!ctx->stream`) notifies
        // transport-closed/deleted and frees the context.
        let (stream, parent, session, flags) = self
            .contexts
            .get(index)
            .and_then(|value| match &value.role {
                ContextRole::Stream(stream) => {
                    Some((stream.stream, stream.parent, stream.session, stream.flags))
                }
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        if flags & STREAM_APP_CLOSED_TX != 0 {
            return Ok(());
        }
        if flags & STREAM_ENGINE_CLOSED != 0 {
            sessions.notify_transport_closed(session, context.into())?;
            sessions.notify_transport_deleted(session, context.into())?;
            return self.remove_context(context);
        }
        let parent_context = ContextId::from(parent);
        {
            let connection = self
                .contexts
                .get_mut(parent)
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .and_then(|engine| engine.connection.as_mut())
                .ok_or_else(|| QuicWorkerError::ContextMissing {
                    context: parent_context,
                })?;
            if !connection.recv_stream(stream).receive_transfer_complete() {
                // VPP `quicly_request_stop`: RX open and not transfer
                // complete. An already-closed recv side errors and is left
                // untouched.
                let _ = connection.recv_stream(stream).stop(RESET_APP_ERROR_CODE);
            }
            if stream_has_send_side(connection, stream)
                && !connection.send_stream(stream).send_transfer_complete()
            {
                // VPP `quicly_reset_stream`: TX open and not transfer
                // complete. An already-reset send side errors and is left
                // untouched.
                let _ = connection.send_stream(stream).reset(RESET_APP_ERROR_CODE);
            }
        }
        if let Some(stream) = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::stream_mut)
        {
            stream.flags |= STREAM_APP_CLOSED_TX;
        }
        self.queue_connection_output(parent_context)?;
        self.schedule_connection_outputs(sessions, now)
    }
}

impl TransportInternalTransport<Index> for QuicWorker {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        session_id: SessionId,
        index: Index,
        _: &DataPlaneRuntime,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.stream_tx_event(sessions, session_id, index, now)
    }
}

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, thiserror::Error)]
pub(super) enum QuicWorkerError {
    #[error("QUIC worker context capacity {capacity} is exhausted")]
    ContextCapacityExhausted { capacity: usize },
    #[error("QUIC context {context:?} is not a worker connection or stream")]
    ContextMissing { context: ContextId },
    #[error("QUIC stream {stream:?} is missing")]
    StreamMissing { stream: quinn_proto::StreamId },
    #[error("QUIC engine is not installed for context {context:?}")]
    EngineMissing { context: ContextId },
    #[error("QUIC protocol connection is not established")]
    ConnectionMissing,
    #[error("QUIC server configuration is missing for context {context:?}")]
    ServerConfigMissing { context: ContextId },
    #[error("QUIC client configuration is missing for active connect")]
    ClientConfigurationMissing,
    #[error("QUIC client connect failed for context {context:?}: {source}")]
    ClientConnectFailed {
        context: ContextId,
        #[source]
        source: quinn_proto::ConnectError,
    },
    #[error(
        "QUIC client connect failed for context {context:?}: {connect}; context cleanup failed: {cleanup}"
    )]
    ClientConnectCleanupFailed {
        context: ContextId,
        #[source]
        connect: quinn_proto::ConnectError,
        cleanup: RuntimeError,
    },
    #[error("QUIC Session {session:?} is missing")]
    SessionMissing { session: SessionId },
    #[error("QUIC datagram for Session {session:?} has invalid length {length}")]
    InvalidDatagram { session: SessionId, length: u32 },
    #[error("QUIC RX datagram scratch slot {slot} is busy for context {context:?}")]
    RxDatagramScratchBusy { context: ContextId, slot: usize },
    #[error("QUIC context {context:?} has incompatible endpoints {local} and {remote}")]
    InvalidEndpoint {
        context: ContextId,
        local: SocketAddr,
        remote: SocketAddr,
    },
    #[error("QUIC context {context:?} could not reserve {bytes} output bytes")]
    OutputReservationFailed {
        context: ContextId,
        bytes: usize,
        #[source]
        source: FifoError,
    },
    #[error(
        "QUIC connection TX queue is full while scheduling context {context:?} (capacity {capacity})"
    )]
    OutputQueueCapacityExceeded { context: ContextId, capacity: usize },
    #[error("QUIC engine produced an invalid packet for context {context:?}: {bytes} bytes")]
    EnginePacketTooLarge { context: ContextId, bytes: usize },
    #[error(
        "QUIC output commit length mismatch for context {context:?}: expected {expected}, got {actual}"
    )]
    OutputCommitLengthMismatch {
        context: ContextId,
        expected: usize,
        actual: usize,
    },
    #[error("QUIC timer update failed for context {context:?}")]
    TimerUpdateFailed { context: ContextId },
    #[error(
        "QUIC timer update failed for context {context:?}: {timer}; context cleanup failed: {cleanup}"
    )]
    TimerUpdateCleanupFailed {
        context: ContextId,
        #[source]
        timer: RuntimeError,
        cleanup: RuntimeError,
    },
    #[error("QUIC Session FIFO stream data error: {error}")]
    StreamData {
        context: ContextId,
        #[source]
        error: quinn_proto::StreamDataError,
    },
    #[error("QUIC stream {stream:?} write sync failed for context {context:?}: {source}")]
    StreamWrite {
        context: ContextId,
        stream: quinn_proto::StreamId,
        #[source]
        source: quinn_proto::WriteError,
    },
    #[error("QUIC stream {stream:?} finish failed for context {context:?}: {source}")]
    StreamFinish {
        context: ContextId,
        stream: quinn_proto::StreamId,
        #[source]
        source: quinn_proto::FinishError,
    },
    #[error("parent Session {parent:?} is missing on the QUIC worker")]
    ParentSessionMissing { parent: SessionHandle },
    #[error("parent Session {parent:?} is not owned by a QUIC connection")]
    ParentSessionInvalid { parent: SessionHandle },
    #[error("parent Session {parent:?} has no allocation owner")]
    ParentAllocationOwnerMissing { parent: SessionHandle },
    #[error("QUIC {direction:?} stream limit is exhausted for parent context {context:?}")]
    StreamLimitReached {
        context: ContextId,
        direction: quinn_proto::Dir,
    },
    #[error(
        "QUIC stream connect failed for parent context {context:?}: {primary}; context cleanup failed: {cleanup}"
    )]
    StreamConnectCleanupFailed {
        context: ContextId,
        #[source]
        primary: RuntimeError,
        cleanup: RuntimeError,
    },
    #[error(
        "open stream failed for parent {parent:?}: {primary}; rollback cleanup failed: {cleanup}"
    )]
    OpenStreamCleanupFailed {
        parent: SessionHandle,
        #[source]
        primary: RuntimeError,
        cleanup: RuntimeError,
    },
    #[error("Session {session:?} is not owned by the QUIC worker")]
    SessionNotQuic { session: SessionId },
    #[error("QUIC application error code {code} for Session {session:?} is not a valid varint")]
    ApplicationErrorCodeInvalid { session: SessionId, code: u64 },
    #[error("QUIC transport action `{action}` is not supported yet")]
    TransportActionUnsupported { action: &'static str },
    #[error("QUIC transport action table install failed")]
    TransportActionsInstall {
        #[source]
        source: SessionTransportActionError,
    },
    #[error("QUIC worker {worker} is outside the configured worker range")]
    WorkerOutOfRange { worker: usize },
    #[error("QUIC worker {worker} is already installed")]
    WorkerAlreadyInstalled { worker: usize },
    #[error("QUIC worker {worker} cannot be accessed")]
    WorkerAccess {
        worker: usize,
        #[source]
        source: ThreadOwnedError,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use hammer_runtime::app::{SessionEvt, SessionEvtType};

    fn server_config() -> Arc<quinn_proto::ServerConfig> {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        let rustls = quinn_proto::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certified.cert.der().clone()],
                quinn_proto::rustls::pki_types::PrivateKeyDer::try_from(
                    certified.signing_key.serialize_der(),
                )
                .expect("encode QUIC server private key"),
            )
            .expect("build QUIC server rustls config");
        let crypto = quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls))
            .expect("build QUIC server crypto");
        let mut config = quinn_proto::ServerConfig::with_crypto(Arc::new(crypto));
        config.transport_config(Arc::new(quinn_proto::TransportConfig::default()));
        Arc::new(config)
    }

    fn client_config() -> Arc<quinn_proto::ClientConfig> {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        let mut roots = quinn_proto::rustls::RootCertStore::empty();
        roots
            .add(certified.cert.der().clone())
            .expect("add test trust anchor");
        let builder = quinn_proto::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let crypto = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(builder))
            .expect("build QUIC client crypto");
        let mut config = quinn_proto::ClientConfig::new(Arc::new(crypto));
        config.transport_config(Arc::new(quinn_proto::TransportConfig::default()));
        Arc::new(config)
    }

    /// Unique per-test AppServer socket path. Parallel test threads share
    /// the process PID, so a monotonic counter (not the PID) keeps paths
    /// distinct between tests.
    fn unique_socket_path(prefix: &str) -> String {
        static SOCKET_SEQ: AtomicU64 = AtomicU64::new(0);
        format!(
            "/tmp/{prefix}-{}.sock",
            SOCKET_SEQ.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Unique per-test allocation owner for shared SessionSegments.
    /// Parallel fixtures all run on the same worker slot, so a literal
    /// owner would make every fixture derive the same `hs-w{slot}-o{owner}`
    /// shm name; the monotonic counter (like `unique_socket_path`) keeps
    /// each fixture's segment name distinct.
    fn unique_allocation_owner() -> u64 {
        static OWNER_SEQ: AtomicU64 = AtomicU64::new(0);
        OWNER_SEQ.fetch_add(1, Ordering::Relaxed)
    }

    fn client_server_config_pair() -> (
        Arc<quinn_proto::ClientConfig>,
        Arc<quinn_proto::ServerConfig>,
    ) {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        let server_tls = quinn_proto::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certified.cert.der().clone()],
                quinn_proto::rustls::pki_types::PrivateKeyDer::try_from(
                    certified.signing_key.serialize_der(),
                )
                .expect("encode QUIC server private key"),
            )
            .expect("build QUIC server rustls config");
        let server_crypto =
            quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(server_tls))
                .expect("build QUIC server crypto");
        let mut server = quinn_proto::ServerConfig::with_crypto(Arc::new(server_crypto));
        server.transport_config(Arc::new(quinn_proto::TransportConfig::default()));

        let mut roots = quinn_proto::rustls::RootCertStore::empty();
        roots
            .add(certified.cert.der().clone())
            .expect("add QUIC test trust anchor");
        let client_tls = quinn_proto::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let client_crypto =
            quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(client_tls))
                .expect("build QUIC client crypto");
        let mut client = quinn_proto::ClientConfig::new(Arc::new(client_crypto));
        client.transport_config(Arc::new(quinn_proto::TransportConfig::default()));

        (Arc::new(client), Arc::new(server))
    }

    fn poll_connection_packets(
        connection: &mut Connection,
        now: Instant,
    ) -> (Vec<BytesMut>, Vec<quinn_proto::EndpointEvent>) {
        let mut packets = Vec::new();
        let mut endpoint_events = Vec::new();
        let mut buffer = BytesMut::with_capacity(MAX_PACKET_SIZE);
        while let Some(transmit) = connection.poll_transmit(now, 1, &mut buffer) {
            packets.push(buffer.split_to(transmit.size));
            buffer.reserve(MAX_PACKET_SIZE);
        }
        while let Some(event) = connection.poll_endpoint_events() {
            endpoint_events.push(event);
        }
        (packets, endpoint_events)
    }

    fn apply_endpoint_events(
        endpoint: &mut Endpoint,
        handle: ConnectionHandle,
        connection: &mut Connection,
        events: Vec<quinn_proto::EndpointEvent>,
    ) {
        for event in events {
            if let Some(event) = endpoint.handle_event(handle, event) {
                connection.handle_event(event);
            }
        }
    }

    fn worker_connection_mut(
        contexts: &mut Pool<Context>,
        context: ContextId,
    ) -> RuntimeResult<&mut Connection> {
        contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.connection.as_mut())
            .ok_or(QuicWorkerError::ContextMissing { context }.into())
    }

    fn drive_client_handshake(
        worker: &mut QuicWorker,
        context: ContextId,
        server_config: Arc<quinn_proto::ServerConfig>,
        client_addr: SocketAddr,
        server_addr: SocketAddr,
        now: Instant,
    ) -> RuntimeResult<()> {
        let mut server_endpoint = Endpoint::new(
            Arc::new(EndpointConfig::default()),
            Some(server_config.clone()),
            true,
            None,
        );
        let mut server_connection: Option<(ConnectionHandle, Connection)> = None;
        let mut server_packets = Vec::new();
        let mut client_connected = false;
        let mut client_scratch = BytesMut::with_capacity(MAX_PACKET_SIZE);
        let mut server_scratch = BytesMut::with_capacity(MAX_PACKET_SIZE);
        let mut packet_descriptors = Vec::new();

        for _ in 0..32 {
            let client_handle = worker
                .contexts
                .get(context.into())
                .and_then(Context::connection)
                .and_then(|connection| connection.engine.as_ref())
                .and_then(|engine| engine.handle)
                .ok_or(QuicWorkerError::ContextMissing { context })?;
            let (client_packets, client_endpoint_events) =
                poll_connection_packets(worker_connection_mut(&mut worker.contexts, context)?, now);
            if !client_endpoint_events.is_empty() {
                let connection = worker_connection_mut(&mut worker.contexts, context)?;
                apply_endpoint_events(
                    &mut worker.endpoint,
                    client_handle,
                    connection,
                    client_endpoint_events,
                );
            }

            for mut packet in client_packets {
                let event = server_endpoint.handle_scratch(
                    now,
                    client_addr,
                    None,
                    None,
                    &mut packet,
                    &mut server_scratch,
                );
                let Some(event) = event else {
                    continue;
                };
                match event {
                    DatagramEvent::NewConnection(incoming) => {
                        let accepted = server_endpoint
                            .accept(
                                incoming,
                                now,
                                &mut server_scratch,
                                Some(server_config.clone()),
                            )
                            .expect("accept QUIC test connection");
                        server_connection = Some(accepted);
                    }
                    DatagramEvent::Response(transmit) => {
                        server_packets.push(server_scratch.split_to(transmit.size));
                        server_scratch.reserve(MAX_PACKET_SIZE);
                    }
                    DatagramEvent::ConnectionEvent(handle, event) => {
                        if let Some((server_handle, connection)) = server_connection.as_mut()
                            && *server_handle == handle
                        {
                            connection.handle_event(event);
                        }
                    }
                }
            }

            if let Some((server_handle, connection)) = server_connection.as_mut() {
                let (packets, endpoint_events) = poll_connection_packets(connection, now);
                apply_endpoint_events(
                    &mut server_endpoint,
                    *server_handle,
                    connection,
                    endpoint_events,
                );
                server_packets.extend(packets);
            }

            for mut packet in server_packets.drain(..) {
                let event = worker.endpoint.handle_scratch_with_descriptors(
                    now,
                    server_addr,
                    None,
                    None,
                    &mut packet,
                    &mut packet_descriptors,
                    &mut client_scratch,
                );
                let Some(event) = event else {
                    continue;
                };
                if let DatagramEvent::ConnectionEvent(handle, event) = event
                    && handle == client_handle
                {
                    worker_connection_mut(&mut worker.contexts, context)?.handle_event(event);
                }
            }

            let connection = worker_connection_mut(&mut worker.contexts, context)?;
            while let Some(event) = connection.poll() {
                if matches!(event, Event::Connected) {
                    client_connected = true;
                }
            }
            if client_connected {
                return Ok(());
            }
        }

        Err(QuicWorkerError::ConnectionMissing.into())
    }

    fn test_listener_start(
        _: SessionListenerId,
        _: ApplicationId,
        _: Option<u64>,
        _: hammer_runtime::SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    fn register_test_outer_listener(
        applications: &Arc<hammer_service::session::ApplicationMain>,
        application: ApplicationId,
        sessions: &mut SessionWorker<Index>,
    ) -> RuntimeResult<SessionListenerId> {
        let main = Arc::new(hammer_service::session::runtime::SessionMain::new(
            1,
            Arc::clone(applications),
        ));
        let application_listener = applications
            .register_listener(application, None, None)
            .map_err(hammer_runtime::RuntimeError::from)?;
        let listener = main.listen(
            application_listener,
            hammer_runtime::SessionTransportRegistration::new(
                "quic-worker-test",
                Some(test_listener_start),
                None,
                None,
            ),
            hammer_runtime::SessionListenEndpoint::new(
                "127.0.0.1:0".parse().expect("test listener endpoint"),
                DataWorkerId::new(0),
            ),
        )?;
        sessions.set_listener_main(main);
        Ok(listener)
    }

    fn test_lower_session() -> RuntimeResult<(SessionWorker<Index>, SessionId)> {
        let applications = hammer_service::session::ApplicationMain::new(4);
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_application_mq_for_test(application)?;
        let lower = sessions.construct_transport_session(
            SessionTransportId::new(1),
            Index::new(1, 1),
            1,
            application,
            Some(hammer_runtime::app::SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        Ok((sessions, lower))
    }

    #[test]
    fn context_roles_fit_one_cache_line_and_pool_identity_is_generation_checked() {
        assert_eq!(std::mem::size_of::<Context>(), 64);
        assert_eq!(std::mem::align_of::<Context>(), 64);

        let mut contexts: Pool<Context> = Pool::with_capacity(1);
        let first = contexts
            .insert(Context {
                role: ContextRole::Listener(ListenerContext {
                    outer_listener: SessionListenerId::new(1, 1),
                    outer_application: ApplicationId::new(1, 1),
                    inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(
                        2, 1,
                    ),
                    inner_session_listener: SessionListenerId::new(3, 1),
                    configuration: ConfigId::from_raw(5),
                    connection_timeout: crate::config::DEFAULT_CONNECTION_TIMEOUT,
                    server_config: None,
                }),
            })
            .expect("listener context capacity");
        assert!(contexts.contains_key(first));
        let removed = contexts.remove(first).expect("remove listener context");
        assert!(matches!(removed.role, ContextRole::Listener(_)));

        let replacement = contexts
            .insert(Context {
                role: ContextRole::Stream(StreamContext {
                    parent: Index::new(5, 1),
                    session: SessionId::from_raw(6),
                    stream: quinn_proto::StreamId::new(
                        quinn_proto::Side::Client,
                        quinn_proto::Dir::Bi,
                        0,
                    ),
                    bytes_written: 0,
                    app_tx_data_len: 0,
                    flags: 0,
                    reserved: [0; 15],
                }),
            })
            .expect("stream context capacity");
        assert_ne!(first, replacement);
        assert!(!contexts.contains_key(first));
        assert!(contexts.contains_key(replacement));
    }

    #[test]
    fn worker_owns_one_context_pool() {
        let worker = QuicWorker::new(DataWorkerId::new(2));
        assert_eq!(worker.contexts.capacity(), QUIC_CONTEXT_CAPACITY);
    }

    #[test]
    fn accepted_connection_retains_listener_until_context_removal() {
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let listener_id = ContextId::from(0x1234u64);
        let listener = ListenerContext {
            outer_listener: SessionListenerId::new(1, 1),
            outer_application: ApplicationId::new(1, 1),
            inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(2, 1),
            inner_session_listener: SessionListenerId::new(3, 1),
            configuration: ConfigId::from_raw(4),
            connection_timeout: crate::config::DEFAULT_CONNECTION_TIMEOUT,
            server_config: None,
        };
        let context = worker
            .accept_connection(SessionId::from_raw(5), listener_id, &listener)
            .expect("accept connection");
        assert_eq!(worker.listener_context_id(context), Some(listener_id));
        worker.remove_context(context).expect("remove context");
        assert_eq!(worker.listener_context_id(context), None);
    }

    #[test]
    fn accepted_connection_uses_listener_timeout() -> RuntimeResult<()> {
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let listener = ListenerContext {
            outer_listener: SessionListenerId::new(1, 1),
            outer_application: ApplicationId::new(1, 1),
            inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(2, 1),
            inner_session_listener: SessionListenerId::new(3, 1),
            configuration: ConfigId::from_raw(4),
            connection_timeout: 5,
            server_config: None,
        };
        let context = worker.accept_connection(
            SessionId::from_raw(5),
            ContextId::from(0x1234u64),
            &listener,
        )?;

        worker
            .timers
            .advance(Instant::now() + Duration::from_millis(20));
        let token = worker
            .timers
            .take_pending()
            .expect("configured handshake timer");
        assert_eq!(token.context, context);
        assert_eq!(token.kind, QuicTimerKind::Handshake);
        Ok(())
    }

    #[test]
    fn timers_dispatch_exact_context_and_kind() {
        let mut timers = QuicTimers::new(Instant::now());
        let context = ContextId::from(Index::new(7, 11));
        timers
            .set(context, QuicTimerKind::Transmit, Duration::from_millis(1))
            .expect("arm timer");
        timers.advance(Instant::now() + Duration::from_millis(1));
        let token = timers.take_pending().expect("expired timer");
        assert_eq!(token.context, context);
        assert_eq!(token.kind, QuicTimerKind::Transmit);
        assert!(timers.take_pending().is_none());
    }

    #[test]
    fn due_connection_deadline_queues_output_immediately() -> RuntimeResult<()> {
        let now = Instant::now();
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let context = worker
            .contexts
            .insert(Context::connection_with_listener(
                SessionId::from_raw(1),
                None,
                ApplicationId::new(1, 1),
                None,
                None,
                None,
            ))
            .map(ContextId::from)
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;

        worker.sync_connection_deadline(context, now, Some(now))?;

        assert_eq!(worker.connection_tx_pending, vec![context]);
        Ok(())
    }

    #[test]
    fn due_connection_deadline_handles_timeout_before_rescheduling() -> RuntimeResult<()> {
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let context = worker.allocate_client_connect(
            client_config(),
            "localhost".to_owned(),
            local,
            remote,
            ApplicationId::new(7, 1),
            None,
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let start = Instant::now();
        worker.connect_connection(context, SessionId::from_raw(11), start)?;
        let mut tx_bufs = BytesBuffer::with_capacity(MAX_PACKET_SIZE);
        let _ = worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.connection.as_mut())
            .and_then(|connection| connection.poll_transmit(start, 1, &mut *tx_bufs))
            .ok_or(QuicWorkerError::ContextMissing { context })?;
        let deadline = worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.connection.as_mut())
            .and_then(|connection| connection.poll_timeout())
            .ok_or(QuicWorkerError::ContextMissing { context })?;
        let now = deadline + TIMER_RESOLUTION;

        worker.sync_connection_deadline(context, now, Some(deadline))?;

        let next_deadline = worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.connection.as_mut())
            .and_then(|connection| connection.poll_timeout());
        assert!(next_deadline.is_none_or(|deadline| deadline > now));
        Ok(())
    }

    #[test]
    fn session_queue_wrappers_match_worker_dispatch_contract() {
        let _: hammer_service::session::node::SessionQueueUpdateTimeFn =
            quic_session_queue_update_time;
        let _: hammer_service::session::node::SessionQueueDispatchFn = quic_session_queue_dispatch;
    }

    #[test]
    fn active_connect_allocates_context_then_builds_engine_and_first_initial() {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        let mut roots = quinn_proto::rustls::RootCertStore::empty();
        roots
            .add(certified.cert.der().clone())
            .expect("add test trust anchor");
        let builder = quinn_proto::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let crypto = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(builder))
            .expect("build QUIC client crypto");
        let mut config = quinn_proto::ClientConfig::new(Arc::new(crypto));
        config.transport_config(Arc::new(quinn_proto::TransportConfig::default()));

        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let context = worker
            .allocate_client_connect(
                Arc::new(config),
                "localhost".to_owned(),
                local,
                remote,
                ApplicationId::new(7, 1),
                None,
                Some(ConfigId::from_raw(9).raw()),
                SessionConnectionId::from_raw(7),
            )
            .expect("allocate active connect context before UDP connect");
        let now = Instant::now();
        worker
            .connect_connection(context, SessionId::from_raw(11), now)
            .expect("initialize client engine from preallocated context");

        let mut tx_bufs = BytesBuffer::with_capacity(1280);
        let transmit = {
            let engine = worker
                .contexts
                .get_mut(context.into())
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_deref_mut())
                .expect("active connect engine");
            assert_eq!(engine.remote, Some(remote));
            engine
                .connection
                .as_mut()
                .expect("connection")
                .poll_transmit(now, 1, &mut *tx_bufs)
                .expect("first client Initial")
        };
        assert!(transmit.size >= 1200);
    }

    #[test]
    fn local_bidirectional_stream_open_allocates_child_on_parent_context() -> RuntimeResult<()> {
        let applications = Arc::new(hammer_service::session::ApplicationMain::with_session_apps(
            4,
            [hammer_runtime::app::SessionAppRegistration::new(
                "test-session-app",
                |_| Ok(()),
                |_, _| {},
            )],
        ));
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let socket_path = unique_socket_path("hammer-quic-local-bidi");
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_session_app(
            SessionAppId::new(0),
            hammer_service::session::SessionAppCallbacks::all_none(),
        )?;
        sessions.install_application_mq_for_test(application)?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let (client_config, server_config) = client_server_config_pair();
        let parent_context = worker.allocate_client_connect(
            client_config,
            "localhost".to_owned(),
            local,
            remote,
            application,
            Some(SessionAppId::new(0)),
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(parent_context, SessionId::from_raw(11), now)?;
        drive_client_handshake(
            &mut worker,
            parent_context,
            server_config,
            local,
            remote,
            now,
        )?;
        let parent = sessions.construct_transport_session(
            QuicWorker::ID,
            parent_context.into(),
            unique_allocation_owner(),
            application,
            Some(SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .connection_session = Some(parent);
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .state = ConnectionState::Established;
        let application_connection = applications
            .register_connection(application, 8, None, Some(SessionAppId::new(0)), None)
            .map_err(hammer_runtime::RuntimeError::from)?;

        let child = worker.connect_stream(
            &mut sessions,
            hammer_runtime::app::SessionHandle::new(parent.pool_index().slot(), 0),
            SessionConnectionId::from_raw(application_connection.raw()),
            hammer_runtime::app::SessionFlags::STREAM,
        )?;
        let (_, child_context) = sessions
            .session_transport(child)
            .ok_or(QuicWorkerError::SessionMissing { session: child })?;
        let stream = worker
            .contexts
            .get(child_context)
            .and_then(|context| match &context.role {
                ContextRole::Stream(stream) => Some(stream),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(child_context),
            })?;

        assert_eq!(stream.parent, parent_context.into());
        assert_eq!(stream.stream.dir(), quinn_proto::Dir::Bi);
        assert_eq!(stream.session, child);
        drop(server);
        let _ = std::fs::remove_file(socket_path);
        Ok(())
    }

    #[test]
    fn local_unidirectional_stream_open_allocates_send_only_child() -> RuntimeResult<()> {
        let applications = Arc::new(hammer_service::session::ApplicationMain::new(4));
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let socket_path = unique_socket_path("hammer-quic-local-uni");
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_application_mq_for_test(application)?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let (client_config, server_config) = client_server_config_pair();
        let parent_context = worker.allocate_client_connect(
            client_config,
            "localhost".to_owned(),
            local,
            remote,
            application,
            None,
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(parent_context, SessionId::from_raw(11), now)?;
        drive_client_handshake(
            &mut worker,
            parent_context,
            server_config,
            local,
            remote,
            now,
        )?;
        let parent = sessions.construct_transport_session(
            QuicWorker::ID,
            parent_context.into(),
            unique_allocation_owner(),
            application,
            None,
            None,
            None,
            false,
        )?;
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .connection_session = Some(parent);
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .state = ConnectionState::Established;
        let application_connection = applications
            .register_connection(application, 9, None, None, None)
            .map_err(hammer_runtime::RuntimeError::from)?;

        let child = worker.connect_stream(
            &mut sessions,
            hammer_runtime::app::SessionHandle::new(parent.pool_index().slot(), 0),
            SessionConnectionId::from_raw(application_connection.raw()),
            hammer_runtime::app::SessionFlags::STREAM
                | hammer_runtime::app::SessionFlags::UNIDIRECTIONAL,
        )?;
        let (_, child_context) = sessions
            .session_transport(child)
            .ok_or(QuicWorkerError::SessionMissing { session: child })?;
        let stream = worker
            .contexts
            .get(child_context)
            .and_then(|context| match &context.role {
                ContextRole::Stream(stream) => Some(stream),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(child_context),
            })?;

        assert_eq!(stream.parent, parent_context.into());
        assert_eq!(stream.stream.dir(), quinn_proto::Dir::Uni);
        assert_eq!(stream.stream.initiator(), quinn_proto::Side::Client);
        assert_eq!(stream.session, child);
        drop(server);
        let _ = std::fs::remove_file(socket_path);
        Ok(())
    }

    #[test]
    fn open_stream_creates_direction_exact_child_carrying_app_context() -> RuntimeResult<()> {
        let applications = Arc::new(hammer_service::session::ApplicationMain::new(4));
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let socket_path = unique_socket_path("hammer-quic-open-stream");
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_session_app(
            SessionAppId::new(0),
            hammer_service::session::SessionAppCallbacks::all_none(),
        )?;
        sessions.install_application_mq_for_test(application)?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let (client_config, server_config) = client_server_config_pair();
        let parent_context = worker.allocate_client_connect(
            Arc::clone(&client_config),
            "localhost".to_owned(),
            local,
            remote,
            application,
            Some(SessionAppId::new(0)),
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(parent_context, SessionId::from_raw(11), now)?;
        drive_client_handshake(
            &mut worker,
            parent_context,
            server_config,
            local,
            remote,
            now,
        )?;
        // The parent must be attached with an allocation owner: open_stream
        // rejects a parent without one (VPP quic.c:230-231 inherits the
        // parent's app worker). The attached external path is the only
        // constructible route in these tests.
        let parent = sessions.construct_transport_session(
            QuicWorker::ID,
            parent_context.into(),
            unique_allocation_owner(),
            application,
            None,
            None,
            None,
            false,
        )?;
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .connection_session = Some(parent);
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .state = ConnectionState::Established;

        let bidi_context = 0xCAFE_BEEFu64;
        let bidi = worker.open_stream(
            &applications,
            &mut sessions,
            parent,
            SessionStreamDirection::Bidi,
            bidi_context,
        )?;
        let (transport, bidi_index) = sessions
            .session_transport(bidi)
            .ok_or(QuicWorkerError::SessionMissing { session: bidi })?;
        assert_eq!(transport, QuicWorker::ID);
        let stream = worker
            .contexts
            .get(bidi_index)
            .and_then(|context| match &context.role {
                ContextRole::Stream(stream) => Some(stream),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(bidi_index),
            })?;
        assert_eq!(stream.parent, parent_context.into());
        assert_eq!(stream.session, bidi);
        assert_eq!(stream.stream.dir(), quinn_proto::Dir::Bi);
        assert_eq!(
            sessions.session_app_endpoint(bidi),
            Some((application, None, Some(bidi_context), None)),
            "the child Session carries the supplied SessionAppContext"
        );

        let uni_context = 0xDEAD_BEEFu64;
        let uni = worker.open_stream(
            &applications,
            &mut sessions,
            parent,
            SessionStreamDirection::Uni,
            uni_context,
        )?;
        let (_, uni_index) = sessions
            .session_transport(uni)
            .ok_or(QuicWorkerError::SessionMissing { session: uni })?;
        let stream = worker
            .contexts
            .get(uni_index)
            .and_then(|context| match &context.role {
                ContextRole::Stream(stream) => Some(stream),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(uni_index),
            })?;
        assert_eq!(stream.parent, parent_context.into());
        assert_eq!(stream.session, uni);
        assert_eq!(stream.stream.dir(), quinn_proto::Dir::Uni);
        assert_eq!(
            sessions.session_app_endpoint(uni),
            Some((application, None, Some(uni_context), None)),
            "the child Session carries the supplied SessionAppContext"
        );

        // A parent Session unknown to the Session Worker is rejected before
        // any allocation (VPP session_open_stream SESSION_E_INVALID).
        let contexts = worker.contexts.len();
        let missing = SessionId::from_raw(0xDEAD);
        let error = worker
            .open_stream(
                &applications,
                &mut sessions,
                missing,
                SessionStreamDirection::Bidi,
                0,
            )
            .expect_err("unknown parent Session is rejected");
        assert!(
            matches!(
                &error,
                RuntimeError::Subsystem { source, .. }
                    if matches!(
                        source.downcast_ref::<QuicWorkerError>(),
                        Some(QuicWorkerError::ParentSessionMissing { .. })
                    )
            ),
            "typed ParentSessionMissing expected, got {error:?}"
        );

        // A stream child is not a valid parent (VPP quic.c:186-194 rejects a
        // stream context) and leaves no orphaned context.
        let error = worker
            .open_stream(
                &applications,
                &mut sessions,
                bidi,
                SessionStreamDirection::Uni,
                0,
            )
            .expect_err("stream Session parent is rejected");
        assert!(
            matches!(
                &error,
                RuntimeError::Subsystem { source, .. }
                    if matches!(
                        source.downcast_ref::<QuicWorkerError>(),
                        Some(QuicWorkerError::ParentSessionInvalid { .. })
                    )
            ),
            "typed ParentSessionInvalid expected, got {error:?}"
        );
        assert_eq!(worker.contexts.len(), contexts);

        // A connection whose worker-local engine has no live connection is
        // rejected up front (VPP quic.c:206 `if (!(conn = qctx->conn))`).
        let idle = worker.allocate_client_connect(
            Arc::clone(&client_config),
            "localhost".to_owned(),
            local,
            remote,
            application,
            Some(SessionAppId::new(0)),
            None,
            SessionConnectionId::from_raw(8),
        )?;
        let idle_session = sessions.construct_transport_session(
            QuicWorker::ID,
            idle.into(),
            unique_allocation_owner(),
            application,
            Some(SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        // Baseline after all setup: the idle connection context above is a
        // real allocation owned by the connection path, not by open_stream
        // (VPP quic.c:206 returns without freeing the connection).
        let idle_contexts = worker.contexts.len();
        let error = worker
            .open_stream(
                &applications,
                &mut sessions,
                idle_session,
                SessionStreamDirection::Bidi,
                0,
            )
            .expect_err("connection without a live engine is rejected");
        assert!(
            matches!(
                &error,
                RuntimeError::Subsystem { source, .. }
                    if matches!(
                        source.downcast_ref::<QuicWorkerError>(),
                        Some(QuicWorkerError::ParentSessionInvalid { .. })
                    )
            ),
            "typed ParentSessionInvalid expected, got {error:?}"
        );
        assert_eq!(
            worker.contexts.len(),
            idle_contexts,
            "the rejected open leaves the idle connection context and no stream context"
        );

        drop(server);
        let _ = std::fs::remove_file(socket_path);
        Ok(())
    }

    #[test]
    fn open_stream_rejects_parent_without_allocation_owner_before_side_effects() -> RuntimeResult<()>
    {
        let applications = Arc::new(hammer_service::session::ApplicationMain::new(4));
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let socket_path = unique_socket_path("hammer-quic-open-stream-owner");
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_session_app(
            SessionAppId::new(0),
            hammer_service::session::SessionAppCallbacks::all_none(),
        )?;
        sessions.install_application_mq_for_test(application)?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let (client_config, server_config) = client_server_config_pair();
        let parent_context = worker.allocate_client_connect(
            Arc::clone(&client_config),
            "localhost".to_owned(),
            local,
            remote,
            application,
            Some(SessionAppId::new(0)),
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(parent_context, SessionId::from_raw(11), now)?;
        drive_client_handshake(
            &mut worker,
            parent_context,
            server_config,
            local,
            remote,
            now,
        )?;
        // App-path construction never attaches the Session, so this parent
        // has an application endpoint but no allocation owner.
        let parent = sessions.construct_transport_session(
            QuicWorker::ID,
            parent_context.into(),
            unique_allocation_owner(),
            application,
            Some(SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .connection_session = Some(parent);
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .state = ConnectionState::Established;

        let contexts = worker.contexts.len();
        let error = worker
            .open_stream(
                &applications,
                &mut sessions,
                parent,
                SessionStreamDirection::Bidi,
                0,
            )
            .expect_err("parent without an allocation owner is rejected");
        assert!(
            matches!(
                &error,
                RuntimeError::Subsystem { source, .. }
                    if matches!(
                        source.downcast_ref::<QuicWorkerError>(),
                        Some(QuicWorkerError::ParentAllocationOwnerMissing { .. })
                    )
            ),
            "typed ParentAllocationOwnerMissing expected, got {error:?}"
        );
        assert_eq!(
            worker.contexts.len(),
            contexts,
            "the rejected open leaves no stream context"
        );

        drop(server);
        let _ = std::fs::remove_file(socket_path);
        Ok(())
    }

    #[test]
    fn open_stream_removes_application_connection_when_connect_stream_fails() -> RuntimeResult<()> {
        // One connection slot: a leaked CONNECTING entry from a failed open
        // would make the second attempt fail at registration instead.
        let applications = Arc::new(hammer_service::session::ApplicationMain::new(1));
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let socket_path = unique_socket_path("hammer-quic-open-stream-rollback");
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_session_app(
            SessionAppId::new(0),
            hammer_service::session::SessionAppCallbacks::all_none(),
        )?;
        sessions.install_application_mq_for_test(application)?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let (client_config, server_config) = client_server_config_pair();
        let parent_context = worker.allocate_client_connect(
            Arc::clone(&client_config),
            "localhost".to_owned(),
            local,
            remote,
            application,
            Some(SessionAppId::new(0)),
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(parent_context, SessionId::from_raw(11), now)?;
        drive_client_handshake(
            &mut worker,
            parent_context,
            server_config,
            local,
            remote,
            now,
        )?;
        let parent = sessions.construct_transport_session(
            QuicWorker::ID,
            parent_context.into(),
            unique_allocation_owner(),
            application,
            None,
            None,
            None,
            false,
        )?;
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .connection_session = Some(parent);
        worker
            .contexts
            .get_mut(parent_context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: parent_context,
            })?
            .state = ConnectionState::Established;

        // Exhaust the context pool so connect_stream fails after the
        // Application Connection is registered.
        while worker.contexts.len() < QUIC_CONTEXT_CAPACITY {
            worker
                .contexts
                .insert(Context::stream(
                    parent_context.into(),
                    SessionId::from_raw(0),
                    quinn_proto::StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0),
                ))
                .expect("dummy stream context fits the pool");
        }
        assert_eq!(worker.contexts.len(), QUIC_CONTEXT_CAPACITY);

        for attempt in 0..2 {
            let error = worker
                .open_stream(
                    &applications,
                    &mut sessions,
                    parent,
                    SessionStreamDirection::Bidi,
                    0,
                )
                .expect_err("connect_stream failure propagates");
            assert!(
                matches!(
                    &error,
                    RuntimeError::Subsystem { source, .. }
                        if matches!(
                            source.downcast_ref::<QuicWorkerError>(),
                            Some(QuicWorkerError::ContextCapacityExhausted { .. })
                        )
                ),
                "attempt {attempt}: ContextCapacityExhausted expected, got {error:?} (a leaked \
                 Application Connection would surface as ApplicationError::ConnectionCapacityExhausted)"
            );
            assert_eq!(worker.contexts.len(), QUIC_CONTEXT_CAPACITY);
        }

        drop(server);
        let _ = std::fs::remove_file(socket_path);
        Ok(())
    }

    #[test]
    fn app_close_enters_active_closing_and_keeps_engine() -> RuntimeResult<()> {
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let (mut sessions, _) = test_lower_session()?;
        let context = worker.allocate_client_connect(
            client_config(),
            "localhost".to_owned(),
            local,
            remote,
            ApplicationId::new(7, 1),
            None,
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(context, SessionId::from_raw(11), now)?;
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .state = ConnectionState::Established;

        worker.begin_connection_close(&mut sessions, context, now)?;

        let connection = worker
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .ok_or(QuicWorkerError::ContextMissing { context })?;
        assert_eq!(connection.state, ConnectionState::ActiveClosing);
        assert!(
            connection
                .engine
                .as_ref()
                .and_then(|engine| engine.connection.as_ref())
                .is_some_and(|connection| connection.is_closed())
        );
        assert_eq!(worker.connection_tx_pending, vec![context]);
        Ok(())
    }

    #[test]
    fn connected_event_publishes_exact_upper_transport_session() -> RuntimeResult<()> {
        let applications = hammer_service::session::ApplicationMain::new(4);
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_application_mq_for_test(application)?;
        let lower = sessions.construct_transport_session(
            SessionTransportId::new(1),
            Index::new(1, 1),
            1,
            application,
            Some(hammer_runtime::app::SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        let outer_listener =
            register_test_outer_listener(&applications, application, &mut sessions)?;

        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let listener_id = ContextId::from(0x1234u64);
        let listener = ListenerContext {
            outer_listener,
            outer_application: application,
            inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(2, 1),
            inner_session_listener: SessionListenerId::new(3, 1),
            configuration: ConfigId::from_raw(4),
            connection_timeout: crate::config::DEFAULT_CONNECTION_TIMEOUT,
            server_config: Some(server_config()),
        };
        let context = worker.accept_connection(lower, listener_id, &listener)?;
        worker.handle_connection_event(&mut sessions, context, Event::Connected)?;

        let connection_session = worker
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.connection_session)
            .expect("QUIC Connection Session is published exactly once");
        assert_eq!(
            sessions.session_transport(connection_session),
            Some((QuicWorker::ID, context.into()))
        );
        Ok(())
    }

    #[test]
    fn remote_close_retains_context_until_application_confirmation() -> RuntimeResult<()> {
        let applications = hammer_service::session::ApplicationMain::new(4);
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_application_mq_for_test(application)?;
        let lower = sessions.construct_transport_session(
            SessionTransportId::new(1),
            Index::new(1, 1),
            1,
            application,
            Some(hammer_runtime::app::SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        let outer_listener =
            register_test_outer_listener(&applications, application, &mut sessions)?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let listener = ListenerContext {
            outer_listener,
            outer_application: application,
            inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(2, 1),
            inner_session_listener: SessionListenerId::new(3, 1),
            configuration: ConfigId::from_raw(4),
            connection_timeout: crate::config::DEFAULT_CONNECTION_TIMEOUT,
            server_config: Some(server_config()),
        };
        let context = worker.accept_connection(lower, ContextId::from(0x1234u64), &listener)?;
        worker.handle_connection_event(&mut sessions, context, Event::Connected)?;
        let connection_session = worker
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.connection_session)
            .ok_or(QuicWorkerError::ContextMissing { context })?;
        let child_session = sessions.insert_session_for_test(QuicWorker::ID, Index::new(1, 1));
        let child_stream =
            quinn_proto::StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0);
        let child_context = worker
            .contexts
            .insert(Context::stream(context.into(), child_session, child_stream))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;

        worker.handle_connection_event(
            &mut sessions,
            context,
            Event::ConnectionLost {
                reason: ConnectionError::ApplicationClosed(quinn_proto::ApplicationClose {
                    error_code: quinn_proto::VarInt::from_u32(0),
                    reason: Bytes::new(),
                }),
            },
        )?;

        assert!(worker.contexts.contains_key(context.into()));
        assert_eq!(
            worker
                .contexts
                .get(context.into())
                .and_then(Context::connection)
                .map(|connection| connection.state),
            Some(ConnectionState::PassiveClosingQuicClosed)
        );
        assert_eq!(
            sessions.session_transport(connection_session),
            Some((QuicWorker::ID, context.into()))
        );
        assert!(!sessions.session_app_closed(connection_session));
        assert!(worker.contexts.contains_key(child_context));
        let child_app =
            sessions
                .app_session(child_session)
                .ok_or(QuicWorkerError::SessionMissing {
                    session: child_session,
                })?;
        let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
        let event_count = child_app.poll_events(&mut events);
        assert!(
            events[..event_count]
                .iter()
                .any(|event| event.evt_type == SessionEvtType::Disconnected)
        );
        Ok(())
    }

    #[test]
    fn repeated_failure_close_notifies_reset_once() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let context = worker
            .contexts
            .insert(Context::connection_with_listener(
                lower,
                None,
                ApplicationId::new(1, 1),
                None,
                None,
                None,
            ))
            .map(ContextId::from)
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        let connection_session = sessions.insert_session_for_test(QuicWorker::ID, context.into());
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .connection_session = Some(connection_session);
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .state = ConnectionState::Established;

        worker.close_connection(
            &mut sessions,
            context,
            Some(SessionConnectError::ConnectionReset),
        )?;
        worker.close_connection(
            &mut sessions,
            context,
            Some(SessionConnectError::ConnectionReset),
        )?;

        assert_eq!(
            worker
                .contexts
                .get(context.into())
                .and_then(Context::connection)
                .map(|connection| connection.state),
            Some(ConnectionState::PassiveClosingQuicClosed)
        );
        assert_eq!(
            sessions.session_transport(connection_session),
            Some((QuicWorker::ID, context.into()))
        );
        assert!(!sessions.session_app_closed(connection_session));
        Ok(())
    }

    #[test]
    fn lower_transport_close_resets_and_deletes_connection_immediately() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let context = worker
            .contexts
            .insert(Context::connection_with_listener(
                lower,
                None,
                ApplicationId::new(1, 1),
                None,
                None,
                None,
            ))
            .map(ContextId::from)
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        let connection_session = sessions.insert_session_for_test(QuicWorker::ID, context.into());
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .connection_session = Some(connection_session);
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .state = ConnectionState::Established;

        worker.transport_closed(&mut sessions, context)?;

        assert!(!worker.contexts.contains_key(context.into()));
        assert_eq!(sessions.session_transport(connection_session), None);
        let application =
            sessions
                .app_session(connection_session)
                .ok_or(QuicWorkerError::SessionMissing {
                    session: connection_session,
                })?;
        let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
        let event_count = application.poll_events(&mut events);
        assert!(
            events[..event_count]
                .iter()
                .any(|event| event.evt_type == SessionEvtType::Reset)
        );
        Ok(())
    }

    #[test]
    fn stream_data_error_resets_parent_and_retains_connection() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let context = worker
            .contexts
            .insert(Context::connection_with_listener(
                lower,
                None,
                ApplicationId::new(1, 1),
                None,
                None,
                None,
            ))
            .map(ContextId::from)
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        let connection_session = sessions.insert_session_for_test(QuicWorker::ID, context.into());
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .connection_session = Some(connection_session);
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .state = ConnectionState::Established;

        let stream = quinn_proto::StreamId::new(quinn_proto::Side::Server, quinn_proto::Dir::Bi, 0);
        let result = worker.handle_stream_data_error(
            &mut sessions,
            context,
            quinn_proto::StreamDataError::StreamMissing { stream },
        );

        assert!(result.is_err());
        assert!(worker.contexts.contains_key(context.into()));
        assert_eq!(
            worker
                .contexts
                .get(context.into())
                .and_then(Context::connection)
                .map(|connection| connection.state),
            Some(ConnectionState::PassiveClosingQuicClosed)
        );
        let application =
            sessions
                .app_session(connection_session)
                .ok_or(QuicWorkerError::SessionMissing {
                    session: connection_session,
                })?;
        let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
        let event_count = application.poll_events(&mut events);
        assert!(
            events[..event_count]
                .iter()
                .any(|event| event.evt_type == SessionEvtType::Reset)
        );
        assert_eq!(worker.stream_data_errors, 1);
        Ok(())
    }

    #[test]
    fn drained_connection_waits_for_application_confirmation() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let context = worker.allocate_client_connect(
            client_config(),
            "localhost".to_owned(),
            local,
            remote,
            ApplicationId::new(7, 1),
            None,
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(context, lower, now)?;
        let connection_session = sessions.insert_session_for_test(QuicWorker::ID, context.into());
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .connection_session = Some(connection_session);
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .state = ConnectionState::PassiveClosingQuicClosed;

        let close_deadline = {
            let connection = worker
                .contexts
                .get_mut(context.into())
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .and_then(|engine| engine.connection.as_mut())
                .ok_or(QuicWorkerError::ContextMissing { context })?;
            connection.close(now, quinn_proto::VarInt::from_u32(0), Bytes::new());
            connection
                .poll_timeout()
                .ok_or(QuicWorkerError::ContextMissing { context })?
        };
        worker
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.connection.as_mut())
            .ok_or(QuicWorkerError::ContextMissing { context })?
            .handle_timeout(close_deadline);

        worker.maybe_finalize_connection(&mut sessions, context)?;
        assert!(worker.contexts.contains_key(context.into()));

        let runtime = hammer_runtime::DataPlaneRuntime::new(
            hammer_runtime::DataPlaneRuntimeConfig::default(),
        );
        sessions.schedule_disconnect(connection_session);
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        dispatch_session_queue_events(
            &runtime,
            &mut sessions,
            &mut worker,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            now,
        )?;

        assert!(!worker.contexts.contains_key(context.into()));
        assert!(!sessions.has_session(connection_session));
        Ok(())
    }

    #[test]
    fn finalizing_connection_removes_all_child_stream_contexts() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let parent = worker
            .contexts
            .insert(Context::connection_with_listener(
                lower,
                None,
                ApplicationId::new(1, 1),
                None,
                None,
                None,
            ))
            .map(ContextId::from)
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        worker
            .contexts
            .insert(Context::stream(
                parent.into(),
                SessionId::from_raw(99),
                quinn_proto::StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0),
            ))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;

        worker.finalize_connection(&mut sessions, parent)?;

        assert!(worker.contexts.is_empty());
        Ok(())
    }

    #[test]
    fn receive_only_stream_close_and_tx_event_do_not_use_send_side() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let parent = worker.allocate_client_connect(
            client_config(),
            "localhost".to_owned(),
            local,
            remote,
            ApplicationId::new(7, 1),
            None,
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(parent, lower, now)?;
        let stream_session = sessions.insert_session_for_test(QuicWorker::ID, Index::new(1, 1));
        let stream =
            quinn_proto::StreamId::new(quinn_proto::Side::Server, quinn_proto::Dir::Uni, 0);
        let stream_context = worker
            .contexts
            .insert(Context::stream(parent.into(), stream_session, stream))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        let tx_fifo = sessions
            .app_session(stream_session)
            .ok_or(QuicWorkerError::SessionMissing {
                session: stream_session,
            })?
            .tx_fifo();
        assert_eq!(tx_fifo.enqueue(b"payload"), 7);

        worker.stream_tx_event(&mut sessions, stream_session, stream_context, now)?;
        worker.begin_stream_close(&mut sessions, ContextId::from(stream_context), now)?;

        assert!(worker.connection_tx_pending.is_empty());
        Ok(())
    }

    #[test]
    fn blocked_stream_close_waits_for_writable_retry() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let parent = worker.allocate_client_connect(
            client_config(),
            "localhost".to_owned(),
            local,
            remote,
            ApplicationId::new(7, 1),
            None,
            None,
            SessionConnectionId::from_raw(7),
        )?;
        let now = Instant::now();
        worker.connect_connection(parent, lower, now)?;
        let stream_session = sessions.insert_session_for_test(QuicWorker::ID, Index::new(1, 1));
        let stream = quinn_proto::StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0);
        let stream_context = worker
            .contexts
            .insert(Context::stream(parent.into(), stream_session, stream))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        let tx_fifo = sessions
            .app_session(stream_session)
            .ok_or(QuicWorkerError::SessionMissing {
                session: stream_session,
            })?
            .tx_fifo();
        assert_eq!(tx_fifo.enqueue(b"payload"), 7);
        worker
            .contexts
            .get_mut(parent.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.connection.as_mut())
            .ok_or(QuicWorkerError::ConnectionMissing)?
            .close(now, quinn_proto::VarInt::from_u32(0), Bytes::new());

        worker.begin_stream_close(&mut sessions, ContextId::from(stream_context), now)?;

        assert_ne!(
            worker
                .contexts
                .get_mut(stream_context)
                .and_then(Context::stream_mut)
                .map(|stream| stream.flags & STREAM_APP_CLOSE_PENDING),
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn local_stream_reset_sends_stop_and_reset_preserving_siblings() -> RuntimeResult<()> {
        let mut relay = peer_relay_established()?;
        let stream = worker_connection_mut(&mut relay.worker.contexts, relay.context)?
            .streams()
            .open(quinn_proto::Dir::Bi)
            .expect("open worker bidirectional stream");
        let sibling = worker_connection_mut(&mut relay.worker.contexts, relay.context)?
            .streams()
            .open(quinn_proto::Dir::Bi)
            .expect("open sibling bidirectional stream");
        let (stream_context, stream_session) = relay_stream_session(&mut relay, stream)?;
        let (sibling_context, sibling_session) = relay_stream_session(&mut relay, sibling)?;

        let app =
            relay
                .sessions
                .app_session(stream_session)
                .ok_or(QuicWorkerError::SessionMissing {
                    session: stream_session,
                })?;
        app.app_rx_mq()
            .enqueue_ctrl(SessionEvt::ctrl(
                app.session_index(),
                0,
                SessionEvtType::Reset,
            ))
            .expect("enqueue app reset");
        relay.sessions.poll_app().expect("stage app reset");
        let runtime = DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig::default());
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        dispatch_session_queue_events(
            &runtime,
            &mut relay.sessions,
            &mut relay.worker,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            relay.now,
        )?;

        // RX open: STOP_SENDING applied (recv side stopped).
        assert!(
            worker_connection_mut(&mut relay.worker.contexts, relay.context)?
                .recv_stream(stream)
                .received_reset()
                .is_err()
        );
        // TX open: RESET_STREAM applied (send side reset; a second reset errors).
        assert!(
            worker_connection_mut(&mut relay.worker.contexts, relay.context)?
                .send_stream(stream)
                .reset(quinn_proto::VarInt::from_u32(0))
                .is_err()
        );
        assert_ne!(
            relay
                .worker
                .contexts
                .get(stream_context)
                .and_then(|value| match &value.role {
                    ContextRole::Stream(stream) => Some(stream),
                    ContextRole::Listener(_) | ContextRole::Connection(_) => None,
                })
                .map(|stream| stream.flags & STREAM_APP_CLOSED_TX),
            Some(0)
        );
        // The reset frames reach the real peer: STOP_SENDING stops the
        // client's send side, RESET_STREAM resets the client's recv side.
        let mut reset_seen = false;
        let mut stop_seen = false;
        for _ in 0..256 {
            if reset_seen && stop_seen {
                break;
            }
            peer_relay_exchange(&mut relay)?;
            if relay
                .client_connection
                .recv_stream(stream)
                .received_reset()
                .is_ok_and(|code| code.is_some())
            {
                reset_seen = true;
            }
            if matches!(
                relay.client_connection.send_stream(stream).write(&[0u8; 1]),
                Err(quinn_proto::WriteError::Stopped(_))
            ) {
                stop_seen = true;
            }
        }
        assert!(reset_seen, "peer RESET_STREAM did not reach the client");
        assert!(
            stop_seen,
            "peer STOP_SENDING did not stop the client send side"
        );
        assert!(relay.sessions.session_app_closed(stream_session));
        // The stream context and Session stay live: VPP reset keeps the
        // context; only an engine-destroyed stream closes the child.
        assert!(relay.worker.contexts.contains_key(stream_context));
        assert!(relay.sessions.has_session(stream_session));
        // Sibling and parent remain untouched.
        assert_eq!(
            worker_connection_mut(&mut relay.worker.contexts, relay.context)?
                .recv_stream(sibling)
                .received_reset(),
            Ok(None)
        );
        assert!(relay.worker.contexts.contains_key(sibling_context));
        assert!(relay.worker.contexts.contains_key(relay.context.into()));
        assert!(relay.sessions.has_session(sibling_session));
        assert!(!relay.sessions.session_app_closed(sibling_session));
        Ok(())
    }

    #[test]
    fn local_stream_reset_on_destroyed_engine_stream_cleans_child() -> RuntimeResult<()> {
        let mut relay = peer_relay_established()?;
        let stream = worker_connection_mut(&mut relay.worker.contexts, relay.context)?
            .streams()
            .open(quinn_proto::Dir::Bi)
            .expect("open worker bidirectional stream");
        let (stream_context, stream_session) = relay_stream_session(&mut relay, stream)?;
        // The engine stream is already gone (VPP `!ctx->stream`): the io entry
        // was removed and the context records the engine close.
        relay
            .worker
            .contexts
            .get_mut(relay.context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .and_then(|engine| engine.io_table.remove_stream(stream));
        relay
            .worker
            .contexts
            .get_mut(stream_context)
            .and_then(Context::stream_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(stream_context),
            })?
            .flags |= STREAM_ENGINE_CLOSED;

        let app =
            relay
                .sessions
                .app_session(stream_session)
                .ok_or(QuicWorkerError::SessionMissing {
                    session: stream_session,
                })?;
        app.app_rx_mq()
            .enqueue_ctrl(SessionEvt::ctrl(
                app.session_index(),
                0,
                SessionEvtType::Reset,
            ))
            .expect("enqueue app reset");
        relay.sessions.poll_app().expect("stage app reset");
        let runtime = DataPlaneRuntime::new(hammer_runtime::DataPlaneRuntimeConfig::default());
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        dispatch_session_queue_events(
            &runtime,
            &mut relay.sessions,
            &mut relay.worker,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            relay.now,
        )?;

        assert!(!relay.worker.contexts.contains_key(stream_context));
        assert!(!relay.sessions.has_session(stream_session));
        assert!(relay.worker.contexts.contains_key(relay.context.into()));
        Ok(())
    }

    #[test]
    fn stream_half_close_then_close_is_idempotent() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let context = worker
            .contexts
            .insert(Context::connection_with_listener(
                lower,
                None,
                ApplicationId::new(1, 1),
                None,
                None,
                None,
            ))
            .map(ContextId::from)
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        let stream_id =
            quinn_proto::StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0);
        let stream_session = sessions.insert_session_for_test(QuicWorker::ID, Index::new(1, 1));
        let stream_context = worker
            .contexts
            .insert(Context::stream(context.into(), stream_session, stream_id))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        worker
            .contexts
            .get_mut(stream_context)
            .and_then(Context::stream_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(stream_context),
            })?
            .flags |= STREAM_APP_CLOSED_TX;

        let runtime = hammer_runtime::DataPlaneRuntime::new(
            hammer_runtime::DataPlaneRuntimeConfig::default(),
        );
        sessions.schedule_half_close(stream_session);
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        dispatch_session_queue_events(
            &runtime,
            &mut sessions,
            &mut worker,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;

        sessions.schedule_disconnect(stream_session);
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        dispatch_session_queue_events(
            &runtime,
            &mut sessions,
            &mut worker,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;

        worker.close_stream_context(&mut sessions, context, stream_id, false)?;
        assert!(!worker.contexts.contains_key(stream_context));
        assert!(!sessions.has_session(stream_session));
        Ok(())
    }

    #[test]
    fn finished_stream_context_waits_for_application_confirmation() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let stream_session = sessions.insert_session_for_test(QuicWorker::ID, Index::new(1, 1));
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let parent = worker
            .contexts
            .insert(Context::connection_with_listener(
                lower,
                None,
                ApplicationId::new(1, 1),
                None,
                None,
                None,
            ))
            .map(ContextId::from)
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        let stream = quinn_proto::StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0);
        let stream_context = worker
            .contexts
            .insert(Context::stream(parent.into(), stream_session, stream))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: worker.contexts.capacity(),
            })?;
        let app_session =
            sessions
                .app_session(stream_session)
                .ok_or(QuicWorkerError::SessionMissing {
                    session: stream_session,
                })?;
        let rx_fifo = Arc::clone(app_session.rx_fifo());
        let tx_fifo = Arc::clone(app_session.tx_fifo());
        worker
            .contexts
            .get_mut(parent.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .ok_or(QuicWorkerError::EngineMissing { context: parent })?
            .io_table
            .install_stream(
                stream,
                stream_context,
                stream_session,
                rx_fifo,
                tx_fifo,
                0,
                0,
            );

        worker.close_stream_context(&mut sessions, parent, stream, false)?;

        assert!(worker.contexts.contains_key(stream_context));
        assert_eq!(
            sessions.session_transport(stream_session),
            Some((QuicWorker::ID, stream_context)),
        );

        let runtime = hammer_runtime::DataPlaneRuntime::new(
            hammer_runtime::DataPlaneRuntimeConfig::default(),
        );
        sessions.schedule_disconnect(stream_session);
        let mut frame = BufferFrame::with_capacity(8);
        let mut output = SessionQueueOutput::default();
        dispatch_session_queue_events(
            &runtime,
            &mut sessions,
            &mut worker,
            SessionQueueNext::from_slot(0),
            &mut frame,
            &mut output,
            Instant::now(),
        )?;

        assert!(!worker.contexts.contains_key(stream_context));
        assert!(!sessions.has_session(stream_session));
        Ok(())
    }

    #[test]
    fn active_failure_publication_backpressure_retains_quic_context() -> RuntimeResult<()> {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        let mut roots = quinn_proto::rustls::RootCertStore::empty();
        roots
            .add(certified.cert.der().clone())
            .expect("add test trust anchor");
        let builder = quinn_proto::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let crypto = quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(builder))
            .expect("build QUIC client crypto");
        let mut config = quinn_proto::ClientConfig::new(Arc::new(crypto));
        config.transport_config(Arc::new(quinn_proto::TransportConfig::default()));

        let socket_path = unique_socket_path("hammer-quic-connect-failure");
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let applications = hammer_service::session::ApplicationMain::new(4);
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_application_mq_for_test(application)?;

        let published = sessions.construct_transport_session(
            SessionTransportId::new(1),
            Index::new(1, 1),
            unique_allocation_owner(),
            application,
            None,
            None,
            None,
            false,
        )?;
        sessions.connection_published(published)?;
        sessions.connected(published)?;

        let lower = sessions.construct_transport_session(
            SessionTransportId::new(1),
            Index::new(2, 1),
            2,
            application,
            None,
            None,
            None,
            false,
        )?;
        let application_connection = applications
            .register_connection(application, 0, None, None, None)
            .map_err(hammer_runtime::RuntimeError::from)?;
        let outer_connection = SessionConnectionId::from_raw(application_connection.raw());
        let local = "127.0.0.1:443".parse().expect("local endpoint");
        let remote = "127.0.0.1:444".parse().expect("remote endpoint");
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let context = worker.allocate_client_connect(
            Arc::new(config),
            "localhost".to_owned(),
            local,
            remote,
            application,
            None,
            None,
            outer_connection,
        )?;
        worker.connect_connection(context, lower, Instant::now())?;

        worker.close_connection(&mut sessions, context, Some(SessionConnectError::TimedOut))?;
        worker.transport_closed(&mut sessions, context)?;

        assert!(worker.contexts.contains_key(context.into()));
        let pending_error = worker
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .and_then(|engine| engine.pending_connect_error);
        assert_eq!(pending_error, Some(SessionConnectError::ConnectionReset));
        assert_eq!(worker.lower_session(context)?, lower);
        assert!(matches!(
            applications.reclaim_connection(application, application_connection),
            Err(hammer_service::session::application::ApplicationError::ConnectionNotConnected {
                connection,
            }) if connection == application_connection
        ));

        drop(server);
        let _ = std::fs::remove_file(socket_path);
        Ok(())
    }

    #[test]
    fn oversized_datagram_is_dropped_and_consumed_once() -> RuntimeResult<()> {
        let (mut sessions, lower) = test_lower_session()?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let listener_id = ContextId::from(0x1234u64);
        let listener = ListenerContext {
            outer_listener: SessionListenerId::new(1, 1),
            outer_application: ApplicationId::new(1, 1),
            inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(2, 1),
            inner_session_listener: SessionListenerId::new(3, 1),
            configuration: ConfigId::from_raw(4),
            connection_timeout: crate::config::DEFAULT_CONNECTION_TIMEOUT,
            server_config: None,
        };
        let context = worker.accept_connection(lower, listener_id, &listener)?;
        let server_addr = "127.0.0.1:443".parse().expect("server address");
        let client_addr = "127.0.0.1:444".parse().expect("client address");
        let payload_len = MAX_PACKET_SIZE + 1;
        let header = SessionDgramHeader::new(server_addr, client_addr, payload_len)
            .expect("oversized test header");
        {
            let (rx_fifo, _) = sessions
                .fifo_pair(lower)
                .ok_or_else(|| QuicWorkerError::SessionMissing { session: lower })?;
            // VPP-style segmented enqueue: header and payload as separate
            // FIFO segments.
            let header_bytes = header.to_bytes();
            assert_eq!(rx_fifo.enqueue(&header_bytes), header_bytes.len());
            let payload = vec![0u8; payload_len];
            assert_eq!(rx_fifo.enqueue(&payload), payload.len());
        }

        worker.process_udp_rx(&mut sessions, lower, context, Instant::now())?;

        let (rx_fifo, _) = sessions
            .fifo_pair(lower)
            .ok_or_else(|| QuicWorkerError::SessionMissing { session: lower })?;
        assert_eq!(rx_fifo.max_dequeue(), 0);
        assert_eq!(worker.rx_datagram_drops, 1);
        assert_eq!(worker.rx_packet_drops, 0);
        Ok(())
    }

    #[test]
    fn server_handshake_publishes_upper_connection_session_through_real_session_fifos()
    -> RuntimeResult<()> {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        let rustls_server = quinn_proto::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certified.cert.der().clone()],
                quinn_proto::rustls::pki_types::PrivateKeyDer::try_from(
                    certified.signing_key.serialize_der(),
                )
                .expect("encode QUIC server private key"),
            )
            .expect("build QUIC server rustls config");
        let crypto_server =
            quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
                .expect("build QUIC server crypto");
        let mut server_config = quinn_proto::ServerConfig::with_crypto(Arc::new(crypto_server));
        server_config.transport_config(Arc::new(quinn_proto::TransportConfig::default()));

        let mut roots = quinn_proto::rustls::RootCertStore::empty();
        roots
            .add(certified.cert.der().clone())
            .expect("add QUIC test trust anchor");
        let rustls_client = quinn_proto::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let crypto_client =
            quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
                .expect("build QUIC client crypto");
        let mut client_config = quinn_proto::ClientConfig::new(Arc::new(crypto_client));
        client_config.transport_config(Arc::new(quinn_proto::TransportConfig::default()));

        let applications = hammer_service::session::ApplicationMain::new(4);
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_application_mq_for_test(application)?;
        let lower = sessions.construct_transport_session(
            SessionTransportId::new(1),
            Index::new(1, 1),
            1,
            application,
            Some(hammer_runtime::app::SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        let outer_listener =
            register_test_outer_listener(&applications, application, &mut sessions)?;

        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let listener_id = ContextId::from(0x1234u64);
        let listener = ListenerContext {
            outer_listener,
            outer_application: application,
            inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(2, 1),
            inner_session_listener: SessionListenerId::new(3, 1),
            configuration: ConfigId::from_raw(4),
            connection_timeout: crate::config::DEFAULT_CONNECTION_TIMEOUT,
            server_config: Some(Arc::new(server_config)),
        };
        let context = worker.accept_connection(lower, listener_id, &listener)?;

        let server_addr = "127.0.0.1:443".parse().expect("server address");
        let client_addr = "127.0.0.1:444".parse().expect("client address");
        let now = Instant::now();
        let mut client_endpoint =
            Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None);
        let (client_handle, mut client_connection) = client_endpoint
            .connect(now, client_config, server_addr, "localhost")
            .map_err(|source| QuicWorkerError::ClientConnectFailed { context, source })?;
        let mut client_buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
        let initial = client_connection
            .poll_transmit(now, 1, &mut client_buf)
            .expect("client Initial datagram");
        while let Some(event) = client_connection.poll_endpoint_events() {
            client_endpoint.handle_event(client_handle, event);
        }
        let mut to_server = VecDeque::from([client_buf.split_to(initial.size)]);

        for _ in 0..16 {
            if worker
                .contexts
                .get(context.into())
                .and_then(Context::connection)
                .and_then(|connection| connection.connection_session)
                .is_some()
            {
                break;
            }
            while let Some(packet) = to_server.pop_front() {
                let (lower_rx, _) = sessions
                    .fifo_pair(lower)
                    .ok_or_else(|| QuicWorkerError::SessionMissing { session: lower })?;
                let header = SessionDgramHeader::new(server_addr, client_addr, packet.len())
                    .ok_or_else(|| QuicWorkerError::InvalidEndpoint {
                        context,
                        local: server_addr,
                        remote: client_addr,
                    })?;
                let header_bytes = header.to_bytes();
                assert_eq!(lower_rx.enqueue(&header_bytes), header_bytes.len());
                assert_eq!(lower_rx.enqueue(&packet), packet.len());
                worker.process_udp_rx(&mut sessions, lower, context, now)?;

                let mut responses = Vec::new();
                loop {
                    let (_, lower_tx) = sessions
                        .fifo_pair(lower)
                        .ok_or_else(|| QuicWorkerError::SessionMissing { session: lower })?;
                    if lower_tx.max_dequeue() < SessionDgramHeader::SIZE {
                        break;
                    }
                    let mut header_bytes = [0u8; SessionDgramHeader::SIZE];
                    assert_eq!(
                        lower_tx.peek(0, header_bytes.len(), &mut header_bytes),
                        header_bytes.len()
                    );
                    let Some(header) = SessionDgramHeader::from_bytes(&header_bytes) else {
                        break;
                    };
                    let payload_len = header.data_length() as usize;
                    let record_len =
                        header
                            .total_len()
                            .ok_or_else(|| QuicWorkerError::InvalidDatagram {
                                session: lower,
                                length: header.data_length(),
                            })?;
                    if lower_tx.max_dequeue() < record_len {
                        break;
                    }
                    let mut payload = vec![0; payload_len];
                    assert_eq!(
                        lower_tx.peek(SessionDgramHeader::SIZE, payload_len, &mut payload),
                        payload_len
                    );
                    assert_eq!(lower_tx.dequeue_drop(record_len), record_len);
                    responses.push(payload);
                }
                for response in responses {
                    let mut data = response;
                    let mut descriptors = Vec::new();
                    let mut endpoint_buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
                    if let Some(event) = client_endpoint.handle_scratch_with_descriptors(
                        now,
                        server_addr,
                        None,
                        None,
                        &mut data,
                        &mut descriptors,
                        &mut endpoint_buf,
                    ) {
                        match event {
                            DatagramEvent::ConnectionEvent(handle, event)
                                if handle == client_handle =>
                            {
                                client_connection.handle_event(event);
                            }
                            DatagramEvent::Response(transmit) => {
                                to_server.push_back(endpoint_buf.split_to(transmit.size));
                                endpoint_buf.reserve(MAX_PACKET_SIZE);
                            }
                            DatagramEvent::NewConnection(_)
                            | DatagramEvent::ConnectionEvent(_, _) => {}
                        }
                    }
                    while let Some(event) = client_connection.poll_endpoint_events() {
                        client_endpoint.handle_event(client_handle, event);
                    }
                    let mut client_buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
                    while let Some(transmit) =
                        client_connection.poll_transmit(now, 1, &mut client_buf)
                    {
                        to_server.push_back(client_buf.split_to(transmit.size));
                        client_buf.reserve(MAX_PACKET_SIZE);
                    }
                }
            }
        }

        let connection_session = worker
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.connection_session)
            .expect("QUIC handshake publishes one Connection Session");
        assert_eq!(
            sessions.session_transport(connection_session),
            Some((QuicWorker::ID, context.into()))
        );
        Ok(())
    }

    /// Server-side relay fixture: a QUIC client connects to the worker's
    /// listener through real Session FIFOs until the Connection Session is
    /// published, mirroring
    /// `server_handshake_publishes_upper_connection_session_through_real_session_fifos`.
    struct PeerRelay {
        worker: QuicWorker,
        sessions: SessionWorker<Index>,
        // Owns the AppServer whose publisher `sessions` holds as a weak
        // reference; dropping it would make `set_accepted` fail with
        // PublicationQueueClosed.
        server: hammer_runtime::attach::AppServer,
        lower: SessionId,
        context: ContextId,
        connection_session: SessionId,
        client_endpoint: Endpoint,
        client_handle: ConnectionHandle,
        client_connection: Connection,
        server_addr: SocketAddr,
        client_addr: SocketAddr,
        now: Instant,
        to_server: VecDeque<BytesMut>,
    }

    /// One relay exchange round: worker -> Session FIFO -> client endpoint ->
    /// client transmit -> back to the worker.
    fn peer_relay_exchange(relay: &mut PeerRelay) -> RuntimeResult<()> {
        let mut client_buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
        // Drain the client's pending transmits before processing server
        // input: a real peer sends STREAM frames as soon as it has them,
        // without waiting for a server datagram.
        while let Some(transmit) =
            relay
                .client_connection
                .poll_transmit(relay.now, 1, &mut client_buf)
        {
            relay
                .to_server
                .push_back(client_buf.split_to(transmit.size));
            client_buf.reserve(MAX_PACKET_SIZE);
        }
        while let Some(packet) = relay.to_server.pop_front() {
            let (lower_rx, _) = relay.sessions.fifo_pair(relay.lower).ok_or_else(|| {
                QuicWorkerError::SessionMissing {
                    session: relay.lower,
                }
            })?;
            let header =
                SessionDgramHeader::new(relay.server_addr, relay.client_addr, packet.len())
                    .ok_or_else(|| QuicWorkerError::InvalidEndpoint {
                        context: relay.context,
                        local: relay.server_addr,
                        remote: relay.client_addr,
                    })?;
            let header_bytes = header.to_bytes();
            assert_eq!(lower_rx.enqueue(&header_bytes), header_bytes.len());
            assert_eq!(lower_rx.enqueue(&packet), packet.len());
            relay.worker.process_udp_rx(
                &mut relay.sessions,
                relay.lower,
                relay.context,
                relay.now,
            )?;

            let mut responses = Vec::new();
            loop {
                let (_, lower_tx) = relay.sessions.fifo_pair(relay.lower).ok_or_else(|| {
                    QuicWorkerError::SessionMissing {
                        session: relay.lower,
                    }
                })?;
                if lower_tx.max_dequeue() < SessionDgramHeader::SIZE {
                    break;
                }
                let mut header_bytes = [0u8; SessionDgramHeader::SIZE];
                assert_eq!(
                    lower_tx.peek(0, header_bytes.len(), &mut header_bytes),
                    header_bytes.len()
                );
                let Some(header) = SessionDgramHeader::from_bytes(&header_bytes) else {
                    break;
                };
                let payload_len = header.data_length() as usize;
                let record_len =
                    header
                        .total_len()
                        .ok_or_else(|| QuicWorkerError::InvalidDatagram {
                            session: relay.lower,
                            length: header.data_length(),
                        })?;
                if lower_tx.max_dequeue() < record_len {
                    break;
                }
                let mut payload = vec![0; payload_len];
                assert_eq!(
                    lower_tx.peek(SessionDgramHeader::SIZE, payload_len, &mut payload),
                    payload_len
                );
                assert_eq!(lower_tx.dequeue_drop(record_len), record_len);
                responses.push(payload);
            }
            for response in responses {
                let mut data = response;
                let mut descriptors = Vec::new();
                let mut endpoint_buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
                if let Some(event) = relay.client_endpoint.handle_scratch_with_descriptors(
                    relay.now,
                    relay.server_addr,
                    None,
                    None,
                    &mut data,
                    &mut descriptors,
                    &mut endpoint_buf,
                ) {
                    match event {
                        DatagramEvent::ConnectionEvent(handle, event)
                            if handle == relay.client_handle =>
                        {
                            relay.client_connection.handle_event(event);
                        }
                        DatagramEvent::Response(transmit) => {
                            relay
                                .to_server
                                .push_back(endpoint_buf.split_to(transmit.size));
                            endpoint_buf.reserve(MAX_PACKET_SIZE);
                        }
                        DatagramEvent::NewConnection(_) | DatagramEvent::ConnectionEvent(_, _) => {}
                    }
                }
                while let Some(event) = relay.client_connection.poll_endpoint_events() {
                    relay
                        .client_endpoint
                        .handle_event(relay.client_handle, event);
                }
                client_buf.clear();
                while let Some(transmit) =
                    relay
                        .client_connection
                        .poll_transmit(relay.now, 1, &mut client_buf)
                {
                    relay
                        .to_server
                        .push_back(client_buf.split_to(transmit.size));
                    client_buf.reserve(MAX_PACKET_SIZE);
                }
            }
        }
        Ok(())
    }

    /// Inserts a worker-side stream context (with a child AppSession) on the
    /// relay connection and returns its pool slot and Session id.
    fn relay_stream_session(
        relay: &mut PeerRelay,
        stream: quinn_proto::StreamId,
    ) -> RuntimeResult<(Index, SessionId)> {
        let context: Index = relay
            .worker
            .contexts
            .insert(Context::stream(
                relay.context.into(),
                SessionId::from_raw(0),
                stream,
            ))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: relay.worker.contexts.capacity(),
            })?;
        let session = relay
            .sessions
            .insert_session_for_test(QuicWorker::ID, context);
        relay
            .worker
            .contexts
            .get_mut(context)
            .and_then(Context::stream_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(context),
            })?
            .session = session;
        Ok((context, session))
    }

    fn peer_relay_established() -> RuntimeResult<PeerRelay> {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        let rustls_server = quinn_proto::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certified.cert.der().clone()],
                quinn_proto::rustls::pki_types::PrivateKeyDer::try_from(
                    certified.signing_key.serialize_der(),
                )
                .expect("encode QUIC server private key"),
            )
            .expect("build QUIC server rustls config");
        let crypto_server =
            quinn_proto::crypto::rustls::QuicServerConfig::try_from(Arc::new(rustls_server))
                .expect("build QUIC server crypto");
        let mut server_config = quinn_proto::ServerConfig::with_crypto(Arc::new(crypto_server));
        server_config.transport_config(Arc::new(quinn_proto::TransportConfig::default()));

        let mut roots = quinn_proto::rustls::RootCertStore::empty();
        roots
            .add(certified.cert.der().clone())
            .expect("add QUIC test trust anchor");
        let rustls_client = quinn_proto::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let crypto_client =
            quinn_proto::crypto::rustls::QuicClientConfig::try_from(Arc::new(rustls_client))
                .expect("build QUIC client crypto");
        let mut client_config = quinn_proto::ClientConfig::new(Arc::new(crypto_client));
        client_config.transport_config(Arc::new(quinn_proto::TransportConfig::default()));

        let applications = hammer_service::session::ApplicationMain::new(4);
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let socket_path = unique_socket_path("hammer-quic-peer-relay");
        let server =
            hammer_runtime::attach::AppServer::bind(&socket_path, 1).expect("bind App server");
        let mut sessions = hammer_service::session::SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            Some(server.publisher()),
        )?;
        sessions.install_application_mq_for_test(application)?;
        let lower = sessions.construct_transport_session(
            SessionTransportId::new(1),
            Index::new(1, 1),
            unique_allocation_owner(),
            application,
            Some(hammer_runtime::app::SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        let outer_listener =
            register_test_outer_listener(&applications, application, &mut sessions)?;

        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let listener_id = ContextId::from(0x1234u64);
        let listener = ListenerContext {
            outer_listener,
            outer_application: application,
            inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(2, 1),
            inner_session_listener: SessionListenerId::new(3, 1),
            configuration: ConfigId::from_raw(4),
            connection_timeout: crate::config::DEFAULT_CONNECTION_TIMEOUT,
            server_config: Some(Arc::new(server_config)),
        };
        let context = worker.accept_connection(lower, listener_id, &listener)?;

        let server_addr = "127.0.0.1:443".parse().expect("server address");
        let client_addr = "127.0.0.1:444".parse().expect("client address");
        let now = Instant::now();
        let mut client_endpoint =
            Endpoint::new(Arc::new(EndpointConfig::default()), None, true, None);
        let (client_handle, mut client_connection) = client_endpoint
            .connect(now, client_config, server_addr, "localhost")
            .map_err(|source| QuicWorkerError::ClientConnectFailed { context, source })?;
        let mut client_buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
        let initial = client_connection
            .poll_transmit(now, 1, &mut client_buf)
            .expect("client Initial datagram");
        while let Some(event) = client_connection.poll_endpoint_events() {
            client_endpoint.handle_event(client_handle, event);
        }
        let to_server = VecDeque::from([client_buf.split_to(initial.size)]);

        let mut relay = PeerRelay {
            worker,
            sessions,
            server,
            lower,
            context,
            connection_session: SessionId::from_raw(0),
            client_endpoint,
            client_handle,
            client_connection,
            server_addr,
            client_addr,
            now,
            to_server,
        };
        // The handshake needs only a few flights, but parallel relay
        // fixtures on a loaded machine may take extra rounds (each round
        // is cheap and the loop breaks early once the Session is set).
        for _ in 0..256 {
            if relay
                .worker
                .contexts
                .get(relay.context.into())
                .and_then(Context::connection)
                .and_then(|connection| connection.connection_session)
                .is_some()
            {
                break;
            }
            peer_relay_exchange(&mut relay)?;
        }
        let connection_session = relay
            .worker
            .contexts
            .get(relay.context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.connection_session)
            .ok_or(QuicWorkerError::ConnectionMissing)?;
        relay.connection_session = connection_session;
        Ok(relay)
    }

    /// A peer-opened bidirectional stream allocates one child Session on the
    /// parent connection's worker with STREAM capability, a pinned accepting
    /// listener (the parent Connection Session handle, VPP
    /// `quic_quicly_on_stream_open` sets `stream_session->listener_handle =
    /// listen_session_get_handle (quic_session)`), and an ACCEPTED
    /// publication (VPP `app_worker_accept_notify`,
    /// application_worker.c:593).
    #[test]
    fn peer_bidirectional_stream_open_allocates_child_on_parent_worker() -> RuntimeResult<()> {
        let mut relay = peer_relay_established()?;
        let stream = relay
            .client_connection
            .streams()
            .open(quinn_proto::Dir::Bi)
            .expect("open peer bidirectional stream");
        relay
            .client_connection
            .send_stream(stream)
            .finish()
            .expect("finish peer stream data");
        for _ in 0..16 {
            let missing = relay
                .worker
                .contexts
                .get(relay.context.into())
                .and_then(Context::connection)
                .and_then(|connection| connection.engine.as_ref())
                .map(|engine| engine.io_table.stream_session(stream).is_none())
                .unwrap_or(true);
            if !missing {
                break;
            }
            peer_relay_exchange(&mut relay)?;
        }
        let child = relay
            .worker
            .contexts
            .get(relay.context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .and_then(|engine| engine.io_table.stream_session(stream))
            .ok_or(QuicWorkerError::StreamMissing { stream })?;
        let (_, child_context) = relay
            .sessions
            .session_transport(child)
            .ok_or(QuicWorkerError::SessionMissing { session: child })?;
        let stream_context = relay
            .worker
            .contexts
            .get(child_context)
            .and_then(|context| match &context.role {
                ContextRole::Stream(stream) => Some(stream),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(child_context),
            })?;

        assert_eq!(stream_context.parent, relay.context.into());
        assert_eq!(stream_context.stream.dir(), quinn_proto::Dir::Bi);
        assert_eq!(stream_context.session, child);
        assert_eq!(
            relay.sessions.session_flags(child),
            Some(SessionFlags::STREAM)
        );
        let accepted = relay
            .sessions
            .accepted_message(child)
            .expect("peer-open child carries a retained ACCEPTED message");
        assert_eq!(
            accepted.listener,
            SessionHandle::new(relay.connection_session.pool_index().slot(), 0)
        );
        assert_eq!(
            accepted.session,
            SessionHandle::new(child.pool_index().slot(), 0)
        );
        assert_eq!(accepted.flags, SessionFlags::STREAM);
        Ok(())
    }

    /// A peer-opened unidirectional stream allocates a send-only child
    /// Session carrying STREAM | UNIDIRECTIONAL capability on its ACCEPTED
    /// publication (VPP `quic_quicly_on_stream_open` flags SESSION_F_STREAM |
    /// SESSION_F_UNIDIRECTIONAL for a remote uni stream).
    #[test]
    fn peer_unidirectional_stream_open_allocates_send_only_child() -> RuntimeResult<()> {
        let mut relay = peer_relay_established()?;
        let stream = relay
            .client_connection
            .streams()
            .open(quinn_proto::Dir::Uni)
            .expect("open peer unidirectional stream");
        relay
            .client_connection
            .send_stream(stream)
            .finish()
            .expect("finish peer stream data");
        for _ in 0..16 {
            let missing = relay
                .worker
                .contexts
                .get(relay.context.into())
                .and_then(Context::connection)
                .and_then(|connection| connection.engine.as_ref())
                .map(|engine| engine.io_table.stream_session(stream).is_none())
                .unwrap_or(true);
            if !missing {
                break;
            }
            peer_relay_exchange(&mut relay)?;
        }
        let child = relay
            .worker
            .contexts
            .get(relay.context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .and_then(|engine| engine.io_table.stream_session(stream))
            .ok_or(QuicWorkerError::StreamMissing { stream })?;
        let (_, child_context) = relay
            .sessions
            .session_transport(child)
            .ok_or(QuicWorkerError::SessionMissing { session: child })?;
        let stream_context = relay
            .worker
            .contexts
            .get(child_context)
            .and_then(|context| match &context.role {
                ContextRole::Stream(stream) => Some(stream),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(child_context),
            })?;

        assert_eq!(stream_context.parent, relay.context.into());
        assert_eq!(stream_context.stream.dir(), quinn_proto::Dir::Uni);
        assert_eq!(stream_context.session, child);
        let flags = SessionFlags::STREAM | SessionFlags::UNIDIRECTIONAL;
        assert_eq!(relay.sessions.session_flags(child), Some(flags));
        let accepted = relay
            .sessions
            .accepted_message(child)
            .expect("peer-open uni child carries a retained ACCEPTED message");
        assert_eq!(
            accepted.listener,
            SessionHandle::new(relay.connection_session.pool_index().slot(), 0)
        );
        assert_eq!(
            accepted.session,
            SessionHandle::new(child.pool_index().slot(), 0)
        );
        assert_eq!(accepted.flags, flags);
        Ok(())
    }

    /// A peer that finishes its send side in the first frame of a stream —
    /// Quinn reports `Opened` with no later `Readable` — must notify the child
    /// AppSession exactly once with the transport-closing control event while
    /// the engine stream and child Session stay live. This is RX half-close,
    /// not stream teardown (VPP `quic_quicly_on_receive` check_eos ->
    /// `session_transport_closing_notify`).
    #[test]
    fn peer_receive_fin_with_initial_opened_notifies_child_transport_closing_once()
    -> RuntimeResult<()> {
        let mut relay = peer_relay_established()?;
        let stream = relay
            .client_connection
            .streams()
            .open(quinn_proto::Dir::Bi)
            .expect("open peer bidirectional stream");
        // Pre-create the child AppSession (Active) under the parent
        // connection, matching
        // `remote_close_retains_context_until_application_confirmation`, so a
        // FIN notification is immediately observable on its event queue.
        let child_context: Index = relay
            .worker
            .contexts
            .insert(Context::stream(
                relay.context.into(),
                SessionId::from_raw(0),
                stream,
            ))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: relay.worker.contexts.capacity(),
            })?;
        let child = relay
            .sessions
            .insert_session_for_test(QuicWorker::ID, child_context);
        relay
            .worker
            .contexts
            .get_mut(child_context)
            .and_then(Context::stream_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(child_context),
            })?
            .session = child;
        let (rx_fifo, tx_fifo) = relay
            .sessions
            .app_session(child)
            .map(|session| (Arc::clone(session.rx_fifo()), Arc::clone(session.tx_fifo())))
            .ok_or(QuicWorkerError::SessionMissing { session: child })?;
        let engine = relay
            .worker
            .contexts
            .get_mut(relay.context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .ok_or(QuicWorkerError::ConnectionMissing)?;
        engine
            .io_table
            .install_stream(stream, child_context, child, rx_fifo, tx_fifo, 0, 0);

        relay
            .client_connection
            .send_stream(stream)
            .finish()
            .expect("finish peer stream data");
        let mut notified = false;
        for _ in 0..16 {
            peer_relay_exchange(&mut relay)?;
            let child_app = relay
                .sessions
                .app_session(child)
                .ok_or(QuicWorkerError::SessionMissing { session: child })?;
            let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
            let event_count = child_app.poll_events(&mut events);
            if events[..event_count]
                .iter()
                .any(|event| event.evt_type == SessionEvtType::Disconnected)
            {
                notified = true;
                break;
            }
        }
        assert!(notified, "peer RX FIN did not notify the child AppSession");
        // Half-close only: the child Session and the engine stream stay live.
        assert!(relay.sessions.session_transport(child).is_some());
        assert!(
            relay
                .worker
                .contexts
                .get(relay.context.into())
                .and_then(Context::connection)
                .and_then(|connection| connection.engine.as_ref())
                .map(|engine| engine.io_table.stream_session(stream).is_some())
                .unwrap_or(false)
        );
        Ok(())
    }

    /// A peer FIN whose final offset arrives before an earlier range (packet
    /// loss): the final offset is known but the receive is not contiguous, so
    /// no transport-closing notify may fire; once the missing range fills the
    /// hole the child AppSession is notified exactly once (VPP
    /// `quicly_recvstate_transfer_complete` requires `data_off ==
    /// received.end`).
    #[test]
    fn peer_receive_fin_out_of_order_waits_for_contiguous_fill() -> RuntimeResult<()> {
        let mut relay = peer_relay_established()?;
        let stream = relay
            .client_connection
            .streams()
            .open(quinn_proto::Dir::Bi)
            .expect("open peer bidirectional stream");
        // Pre-create the child AppSession (Active) under the parent
        // connection so FIN notifications are observable on its event queue.
        let child_context: Index = relay
            .worker
            .contexts
            .insert(Context::stream(
                relay.context.into(),
                SessionId::from_raw(0),
                stream,
            ))
            .ok_or(QuicWorkerError::ContextCapacityExhausted {
                capacity: relay.worker.contexts.capacity(),
            })?;
        let child = relay
            .sessions
            .insert_session_for_test(QuicWorker::ID, child_context);
        relay
            .worker
            .contexts
            .get_mut(child_context)
            .and_then(Context::stream_mut)
            .ok_or(QuicWorkerError::ContextMissing {
                context: ContextId::from(child_context),
            })?
            .session = child;
        let (rx_fifo, tx_fifo) = relay
            .sessions
            .app_session(child)
            .map(|session| (Arc::clone(session.rx_fifo()), Arc::clone(session.tx_fifo())))
            .ok_or(QuicWorkerError::SessionMissing { session: child })?;
        let engine = relay
            .worker
            .contexts
            .get_mut(relay.context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .ok_or(QuicWorkerError::ConnectionMissing)?;
        engine
            .io_table
            .install_stream(stream, child_context, child, rx_fifo, tx_fifo, 0, 0);

        // The peer sends bytes 0..4 in one datagram and the FIN byte 4..5 in
        // the next; the first datagram is lost, so the FIN arrives first and
        // the server sees the final offset with a hole before it.
        relay
            .client_connection
            .send_stream(stream)
            .write(&[0u8; 4])
            .expect("write first peer range");
        let mut a_buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
        let a = relay
            .client_connection
            .poll_transmit(relay.now, 1, &mut a_buf)
            .expect("peer first-range datagram");
        let a_payload = a_buf.split_to(a.size);
        relay
            .client_connection
            .send_stream(stream)
            .write(&[1u8])
            .expect("write final peer byte");
        relay
            .client_connection
            .send_stream(stream)
            .finish()
            .expect("finish peer stream data");
        let mut b_buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
        let b = relay
            .client_connection
            .poll_transmit(relay.now, 1, &mut b_buf)
            .expect("peer final-range datagram");
        relay.to_server.push_back(b_buf.split_to(b.size));
        peer_relay_exchange(&mut relay)?;
        let child_app = relay
            .sessions
            .app_session(child)
            .ok_or(QuicWorkerError::SessionMissing { session: child })?;
        let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
        let event_count = child_app.poll_events(&mut events);
        assert!(
            !events[..event_count]
                .iter()
                .any(|event| event.evt_type == SessionEvtType::Disconnected),
            "a known final offset with a missing earlier range must not notify"
        );

        // The lost first-range datagram arrives and fills the hole.
        relay.to_server.push_back(a_payload);
        peer_relay_exchange(&mut relay)?;
        let child_app = relay
            .sessions
            .app_session(child)
            .ok_or(QuicWorkerError::SessionMissing { session: child })?;
        let mut events = [SessionEvt::io(0, SessionEvtType::Connect); 4];
        let event_count = child_app.poll_events(&mut events);
        assert_eq!(
            events[..event_count]
                .iter()
                .filter(|event| event.evt_type == SessionEvtType::Disconnected)
                .count(),
            1,
            "filling the hole notifies the child AppSession exactly once"
        );
        // Half-close only: the child Session and the engine stream stay live.
        assert!(relay.sessions.session_transport(child).is_some());
        assert!(
            relay
                .worker
                .contexts
                .get(relay.context.into())
                .and_then(Context::connection)
                .and_then(|connection| connection.engine.as_ref())
                .map(|engine| engine.io_table.stream_session(stream).is_some())
                .unwrap_or(false)
        );
        Ok(())
    }

    #[test]
    fn app_error_code_varint_conversion_is_checked() {
        let legal_min = QuicAppErrorCode(SessionApplicationErrorCode::from(0));
        assert_eq!(
            VarInt::try_from(legal_min),
            Ok(VarInt::from_u32(0)),
            "zero is the minimum legal application error code"
        );
        let legal_max = (1u64 << 62) - 1;
        assert_eq!(
            VarInt::try_from(QuicAppErrorCode(SessionApplicationErrorCode::from(
                legal_max
            ))),
            Ok(VarInt::try_from(legal_max).expect("max legal varint")),
            "2^62 - 1 is the maximum legal application error code"
        );
        let out_of_range = SessionApplicationErrorCode::from(1u64 << 62);
        assert_eq!(
            VarInt::try_from(QuicAppErrorCode(out_of_range)),
            Err(1u64 << 62),
            "1 << 62 exceeds the varint range and returns the original code"
        );
        assert_eq!(
            u64::from(out_of_range),
            1u64 << 62,
            "failed conversion leaves the input unchanged"
        );
    }

    #[test]
    fn app_error_code_varint_rejects_out_of_range_with_session_error() {
        let session = SessionId::from_raw(7);
        let error = QuicWorker::app_error_code_varint(
            session,
            SessionApplicationErrorCode::from(1u64 << 62),
        )
        .expect_err("1 << 62 exceeds the varint range");
        match error {
            QuicWorkerError::ApplicationErrorCodeInvalid {
                session: rejected,
                code,
            } => {
                assert_eq!(rejected, session);
                assert_eq!(code, 1u64 << 62, "original code is preserved");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    fn test_quic_session() -> RuntimeResult<(
        QuicWorker,
        SessionWorker<Index>,
        ApplicationId,
        SessionId,
        ContextId,
        Index,
    )> {
        let applications = hammer_service::session::ApplicationMain::new(4);
        let application = applications
            .attach()
            .map_err(hammer_runtime::RuntimeError::from)?;
        let mut sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )?;
        sessions.install_application_mq_for_test(application)?;
        let mut worker = QuicWorker::new(DataWorkerId::new(0));
        let listener = ListenerContext {
            outer_listener: SessionListenerId::new(1, 1),
            outer_application: ApplicationId::new(1, 1),
            inner_application_listener: hammer_runtime::app::ApplicationListenerId::new(2, 1),
            inner_session_listener: SessionListenerId::new(3, 1),
            configuration: ConfigId::from_raw(4),
            connection_timeout: 5,
            server_config: None,
        };
        let context = worker
            .accept_connection(
                SessionId::from_raw(5),
                ContextId::from(0x1234u64),
                &listener,
            )
            .expect("accept connection");
        let connection_index = Index::from(context);
        let session = sessions.construct_transport_session(
            QuicWorker::ID,
            connection_index,
            1,
            application,
            Some(hammer_runtime::app::SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        let listener_index = worker
            .contexts
            .insert(Context::listener(
                SessionListenerId::new(1, 1),
                ApplicationId::new(1, 1),
                hammer_runtime::app::ApplicationListenerId::new(2, 1),
                SessionListenerId::new(3, 1),
                ConfigId::from_raw(4),
                5,
                None,
            ))
            .expect("listener context capacity");
        Ok((
            worker,
            sessions,
            application,
            session,
            context,
            listener_index,
        ))
    }

    #[test]
    fn session_context_resolves_quic_session_to_exact_context() -> RuntimeResult<()> {
        let (worker, sessions, _, session, context, _) = test_quic_session()?;
        let resolved = worker
            .session_context(&sessions, session)
            .expect("QUIC Session resolves");
        assert_eq!(resolved, context);
        Ok(())
    }

    #[test]
    fn session_context_rejects_missing_session() -> RuntimeResult<()> {
        let (worker, sessions, _, _, _, _) = test_quic_session()?;
        let missing = SessionId::from_raw(0xDEAD);
        assert!(matches!(
            worker.session_context(&sessions, missing),
            Err(QuicWorkerError::SessionMissing { session }) if session == missing
        ));
        Ok(())
    }

    #[test]
    fn session_context_rejects_foreign_transport_session() -> RuntimeResult<()> {
        let worker = QuicWorker::new(DataWorkerId::new(0));
        let (sessions, foreign) = test_lower_session()?;
        assert!(matches!(
            worker.session_context(&sessions, foreign),
            Err(QuicWorkerError::SessionNotQuic { session }) if session == foreign
        ));
        Ok(())
    }

    #[test]
    fn session_context_rejects_non_stream_connection_context() -> RuntimeResult<()> {
        let (worker, mut sessions, application, _, _, listener_index) = test_quic_session()?;
        let non_quic = sessions.construct_transport_session(
            QuicWorker::ID,
            listener_index,
            1,
            application,
            Some(hammer_runtime::app::SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        assert!(matches!(
            worker.session_context(&sessions, non_quic),
            Err(QuicWorkerError::ContextMissing { context })
                if context == ContextId::from(listener_index)
        ));
        Ok(())
    }
}

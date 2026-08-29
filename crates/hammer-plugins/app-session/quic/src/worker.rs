//! QUIC's worker-local transport state and registration.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hammer_core::data_plane::BufferFrame;
use hammer_infra::bytes::BytesBuffer;
use hammer_infra::fifo::{Fifo, FifoError};
use hammer_infra::pool::Pool;
use hammer_infra::thread_owned::ThreadOwnedError;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::app::{SessionConnectError, SessionDgramHeader, SessionFlags, SessionHandle};
use hammer_runtime::session::SessionStreamDirection;
use hammer_runtime::{DataPlaneMain, DataWorkerId, NodeRuntimeData, RuntimeError, RuntimeResult};
use hammer_service::session::application::{ApplicationMain, application_main};
use hammer_service::session::node::{SessionQueueNext, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionTransport, SessionWorker, TransportInternalTransport, TransportInternalTx,
    dispatch_session_queue_events,
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

/// QUIC-local wrapper around `u64` (foreign) so the
/// checked `TryFrom` conversion into `VarInt` (foreign) can be implemented
/// under the orphan rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(super) struct QuicAppErrorCode(u64);

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

/// A stream has a local receive side when it is bidirectional or the peer
/// initiated it (RFC 9000 2.1); mirrors VPP `quicly_stream_has_receive_side`
/// guarding `quicly_request_stop` (quic_quicly.c:1253).
#[inline]
fn stream_has_receive_side(connection: &Connection, stream: StreamId) -> bool {
    stream.dir() == quinn_proto::Dir::Bi || stream.initiator() != connection.side()
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

/// Worker-local role state stored in one QUIC context slot.
#[repr(C)]
#[derive(Debug, Clone)]
pub(super) struct ListenerContext {
    pub(crate) outer_listener: SessionHandle,
    pub(crate) outer_application: u32,
    pub(crate) inner_application_listener: u32,
    pub(crate) inner_session_listener: SessionHandle,
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
    application: u32,
    app: Option<u32>,
    /// Application-supplied opaque for child Stream Sessions. Not VPP
    /// `quic_ctx_t.client_opaque`.
    app_opaque: Option<u64>,
    /// VPP `quic_ctx_t.client_opaque`: outer active Application Connection
    /// correlation retained through the handshake.
    client_opaque: Option<u32>,
    /// Outer Session listener retained through a passive handshake.
    outer_listener: Option<SessionHandle>,
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
        application: u32,
        app: Option<u32>,
        app_opaque: Option<u64>,
        client_opaque: Option<u32>,
        outer_listener: Option<SessionHandle>,
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
        application: u32,
        app: Option<u32>,
        app_opaque: Option<u64>,
        client_opaque: Option<u32>,
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
    lower_session: u32,
    connection_session: Option<u32>,
    listener: Option<u32>,
    state: ConnectionState,
    flags: u8,
    reserved: [u8; 6],
}

#[repr(C)]
struct StreamContext {
    parent: u32,
    session: u32,
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
#[repr(C)]
pub(super) struct Context {
    cacheline0: hammer_infra::align::CacheLineAlignMark,
    role: ContextRole,
}

impl Context {
    pub(super) fn listener(
        outer_listener: SessionHandle,
        outer_application: u32,
        inner_application_listener: u32,
        inner_session_listener: SessionHandle,
        configuration: ConfigId,
        connection_timeout: u32,
        server_config: Option<Arc<quinn_proto::ServerConfig>>,
    ) -> Self {
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
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
        lower_session: u32,
        listener: Option<u32>,
        application: u32,
        listener_context: Option<&ListenerContext>,
        app: Option<u32>,
        app_opaque: Option<u64>,
    ) -> Self {
        let server_config = listener_context.and_then(|listener| listener.server_config.clone());
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
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
        lower_session: u32,
        application_connection: u32,
        application: u32,
        app: Option<u32>,
        app_opaque: Option<u64>,
        config: Arc<quinn_proto::ClientConfig>,
        server_name: String,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Self {
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
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

    fn stream(parent: u32, session: u32, stream: quinn_proto::StreamId) -> Self {
        Self {
            cacheline0: hammer_infra::align::CacheLineAlignMark,
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

    fn lower_session(&self) -> Option<u32> {
        match &self.role {
            ContextRole::Connection(connection) => Some(connection.lower_session),
            ContextRole::Stream(_) | ContextRole::Listener(_) => None,
        }
    }

    fn transport_session(&self) -> Option<u32> {
        match &self.role {
            ContextRole::Connection(connection) => connection.connection_session,
            ContextRole::Stream(stream) => Some(stream.session),
            ContextRole::Listener(_) => None,
        }
    }

    fn connection_index(&self, index: u32) -> Option<u32> {
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

    fn engine_mut(&mut self, context: u32) -> RuntimeResult<&mut EngineConnection> {
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
    context: u32,
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

    fn set(&mut self, context: u32, kind: QuicTimerKind, interval: Duration) -> RuntimeResult<()> {
        self.wheel
            .arm_timer(context, 0, kind.id(), self.duration_ticks(interval))
            .map_err(|_| QuicWorkerError::TimerUpdateFailed { context }.into())
    }

    fn stop(&mut self, context: u32, kind: QuicTimerKind) {
        let _ = self.wheel.cancel_timer(context, 0, kind.id());
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
            let Some((index, _, kind_id)) = self.wheel.take_expired_timer(*payload) else {
                continue;
            };
            let Some(kind) = QuicTimerKind::from_id(kind_id) else {
                continue;
            };
            self.pending.push_back(QuicTimerToken {
                context: index,
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
pub struct QuicWorker {
    protocol: u8,
    endpoint: Endpoint,
    contexts: Pool<Context>,
    timers: QuicTimers,
    rx_datagrams: [Option<Box<RxDatagramScratch>>; RX_DATAGRAM_BURST],
    rx_packet_descriptors: Vec<PartialDecode>,
    stream_io_events: Vec<crate::stream_io::StreamIoEvent>,
    tx_bufs: BytesBuffer,
    connection_tx_pending: Vec<u32>,
    connection_tx_ready: Vec<u32>,
    rx_datagram_drops: u64,
    rx_packet_drops: u64,
    stream_data_errors: u64,
}

impl QuicWorker {
    pub fn new(_: DataWorkerId, protocol: u8) -> Self {
        Self {
            protocol,
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
        lower_session: u32,
        listener_id: u32,
        listener: &ListenerContext,
    ) -> RuntimeResult<u32> {
        let context = self.contexts.insert(Context::connection_with_listener(
            lower_session,
            Some(listener_id),
            listener.outer_application,
            Some(listener),
            None,
            None,
        ));
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
        application: u32,
        app: Option<u32>,
        app_opaque: Option<u64>,
        application_connection: u32,
    ) -> RuntimeResult<u32> {
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
        application: u32,
        app: Option<u32>,
        app_opaque: Option<u64>,
        application_connection: u32,
        connection_timeout: u32,
    ) -> RuntimeResult<u32> {
        let context = self.contexts.insert(Context::connection_with_client(
            0,
            application_connection,
            application,
            app,
            app_opaque,
            config,
            server_name,
            local,
            remote,
        ));
        if let Err(error) = self.timers.set(
            context,
            QuicTimerKind::Handshake,
            Duration::from_millis(connection_timeout.into()),
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
        context: u32,
        lower_session: u32,
        now: Instant,
    ) -> RuntimeResult<u32> {
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
        sessions: &mut SessionWorker,
        parent: SessionHandle,
        connection: u32,
        flags: SessionFlags,
    ) -> RuntimeResult<u32> {
        let parent_session = sessions
            .session_id_from_handle(parent)
            .ok_or(QuicWorkerError::ParentSessionMissing { parent })?;
        let parent_index = sessions
            .transport_connection_index(parent_session)
            .ok_or(QuicWorkerError::ParentSessionInvalid { parent })?;
        if !sessions.owns_transport_session(parent_session, self.protocol)
            || !self
                .contexts
                .get(parent_index)
                .is_some_and(|context| context.connection().is_some())
        {
            return Err(QuicWorkerError::ParentSessionInvalid { parent }.into());
        }

        let child_index = self.contexts.insert(Context::stream(
            parent_index,
            0,
            quinn_proto::StreamId::new(quinn_proto::Side::Client, quinn_proto::Dir::Bi, 0),
        ));
        let child_session =
            match sessions.stream_connect_pending(self.protocol, child_index, connection) {
                Ok(session) => session,
                Err(error) => {
                    let cleanup = self.remove_context(child_index).err();
                    return match cleanup {
                        Some(cleanup) => Err(QuicWorkerError::StreamConnectCleanupFailed {
                            context: child_index,
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
                context: parent_index,
            })?
            .engine
            .as_mut()
            .ok_or_else(|| QuicWorkerError::EngineMissing {
                context: parent_index,
            })?
            .connection_mut()?
            .streams()
            .open(direction)
            .ok_or_else(|| QuicWorkerError::StreamLimitReached {
                context: parent_index,
                direction,
            })
            .map_err(RuntimeError::from);
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                let session_cleanup = sessions.rollback_session_creation(child_session).err();
                let context_cleanup = self.remove_context(child_index).err();
                let cleanup = session_cleanup.or(context_cleanup);
                return match cleanup {
                    Some(cleanup) => Err(QuicWorkerError::StreamConnectCleanupFailed {
                        context: child_index,
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
                context: child_index,
            })?
            .stream = stream;
        self.contexts
            .get_mut(child_index)
            .and_then(Context::stream_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing {
                context: child_index,
            })?
            .session = child_session;
        self.contexts
            .get_mut(parent_index)
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing {
                context: parent_index,
            })?
            .engine
            .as_mut()
            .ok_or_else(|| QuicWorkerError::EngineMissing {
                context: parent_index,
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
            let _ = self.remove_context(child_index);
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
        sessions: &mut SessionWorker,
        parent: u32,
        direction: SessionStreamDirection,
        app_context: u64,
    ) -> RuntimeResult<u32> {
        let parent_handle = sessions.session_handle(parent);
        let parent_index = sessions.transport_connection_index(parent).ok_or(
            QuicWorkerError::ParentSessionMissing {
                parent: parent_handle,
            },
        )?;
        if !sessions.owns_transport_session(parent, self.protocol)
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
        let child = match self.connect_stream(sessions, parent_handle, connection, flags) {
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
            let child_index = sessions.transport_connection_index(child);
            let session_cleanup = sessions.rollback_session_creation(child).err();
            let context_cleanup = match child_index {
                Some(index) => self.remove_context(index).err(),
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
    fn session_context(
        &self,
        sessions: &SessionWorker,
        session: u32,
    ) -> Result<u32, QuicWorkerError> {
        let index = sessions
            .transport_connection_index(session)
            .ok_or(QuicWorkerError::SessionMissing { session })?;
        if !sessions.owns_transport_session(session, self.protocol) {
            return Err(QuicWorkerError::SessionNotQuic { session });
        }
        let context = index;
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
    fn app_error_code_varint(session: u32, code: u64) -> Result<VarInt, QuicWorkerError> {
        VarInt::try_from(QuicAppErrorCode(code)).map_err(|invalid| {
            QuicWorkerError::ApplicationErrorCodeInvalid {
                session,
                code: invalid,
            }
        })
    }

    /// Resets one stream Session with an application error code; the
    /// transport half of the generic `SessionWorker::reset_stream` dispatch,
    /// replacing the `transport_reset_unsupported` stub. Mirrors VPP
    /// `session_transport_reset` (session.c:1687-1703) notifying the
    /// transport, where the application error code recorded through the
    /// `APP_PROTO_ERR_CODE` transport endpoint attribute (quic.c:701) is
    /// carried on the RESET_STREAM frame. Resolution is O(1): `session_context`
    /// derives the worker-local context index from the Session entry, the
    /// stream context pins the exact quinn stream on the parent connection,
    /// and only then is the send stream reset and the connection scheduled
    /// for output so the frame transmits. Every validation precedes the
    /// quinn call, so a rejection never mutates transport state.
    pub(super) fn reset_stream(
        &mut self,
        sessions: &mut SessionWorker,
        session: u32,
        code: u64,
    ) -> RuntimeResult<()> {
        let context = self.session_context(sessions, session)?;
        let (stream_id, parent, engine_closed) = self
            .contexts
            .get(context.into())
            .and_then(Context::stream_ref)
            .map(|stream| {
                (
                    stream.stream,
                    stream.parent,
                    stream.flags & STREAM_ENGINE_CLOSED != 0,
                )
            })
            .ok_or(QuicWorkerError::SessionNotStream { session })?;
        if engine_closed {
            return Err(QuicWorkerError::StreamMissing { stream: stream_id }.into());
        }
        let error_code = Self::app_error_code_varint(session, code)?;
        let connection = self
            .contexts
            .get_mut(parent)
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context: parent })?
            .engine
            .as_mut()
            .ok_or_else(|| QuicWorkerError::EngineMissing { context: parent })?
            .connection_mut()?;
        if !stream_has_send_side(connection, stream_id) {
            return Err(QuicWorkerError::StreamSendSideMissing { stream: stream_id }.into());
        }
        connection
            .send_stream(stream_id)
            .reset(error_code)
            .map_err(|source| QuicWorkerError::StreamReset {
                context,
                stream: stream_id,
                source,
            })?;
        self.queue_connection_output(parent)?;
        Ok(())
    }

    /// Stops a stream Session's receive side with an application error code;
    /// the transport half of the generic `SessionWorker::stop_sending`
    /// dispatch, replacing the `transport_stop_sending_unsupported` stub.
    /// Mirrors VPP `quic_quicly_on_app_reset` (quic_quicly.c:1253-1259),
    /// where a stream that still has a receive side calls `quicly_request_stop`
    /// with the application error code, transmitting a STOP_SENDING frame,
    /// but only while the receive state transfer is not complete
    /// (`!quicly_recvstate_transfer_complete`): a receive side whose peer
    /// already finished sending (FIN and every byte received contiguously)
    /// gets no request. The wire gate needs an explicit query because quinn's
    /// `RecvStream::stop` only suppresses the frame after the application has
    /// read the stream out of `Recv` state (`is_receiving`,
    /// streams/recv.rs:244), which lags the wire condition: after the peer's
    /// FIN and all bytes arrive but before the app reads, quinn would still
    /// emit STOP_SENDING. The gate is the O(1) public
    /// `RecvStream::receive_transfer_complete` query (streams/mod.rs:262),
    /// the same `!quicly_recvstate_transfer_complete` condition VPP applies.
    /// The Session Worker's `stop_sending` guard (runtime.rs:1696) admits
    /// only an Active stream and half-close never changes Session state, so
    /// unlike `reset_stream` this action records no AppClosed and repeated
    /// dispatches keep reaching the transport. Resolution is O(1):
    /// `session_context` derives the worker-local context index from the
    /// Session entry, the stream context pins the exact quinn stream on the
    /// parent connection, and only then is that receive stream stopped and
    /// the connection scheduled for output so the STOP_SENDING frame
    /// transmits. Every validation precedes the quinn call, so a rejection
    /// never mutates transport state.
    pub(super) fn stop_sending(
        &mut self,
        sessions: &mut SessionWorker,
        session: u32,
        code: u64,
    ) -> RuntimeResult<()> {
        let context = self.session_context(sessions, session)?;
        let (stream_id, parent, engine_closed) = self
            .contexts
            .get(context.into())
            .and_then(Context::stream_ref)
            .map(|stream| {
                (
                    stream.stream,
                    stream.parent,
                    stream.flags & STREAM_ENGINE_CLOSED != 0,
                )
            })
            .ok_or(QuicWorkerError::SessionNotStream { session })?;
        if engine_closed {
            return Err(QuicWorkerError::StreamMissing { stream: stream_id }.into());
        }
        let error_code = Self::app_error_code_varint(session, code)?;
        let connection = self
            .contexts
            .get_mut(parent)
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context: parent })?
            .engine
            .as_mut()
            .ok_or_else(|| QuicWorkerError::EngineMissing { context: parent })?
            .connection_mut()?;
        if !stream_has_receive_side(connection, stream_id) {
            return Err(QuicWorkerError::StreamReceiveSideMissing { stream: stream_id }.into());
        }
        let mut recv = connection.recv_stream(stream_id);
        if recv.receive_transfer_complete() {
            return Ok(());
        }
        recv.stop(error_code)
            .map_err(|source| QuicWorkerError::StreamStop {
                context,
                stream: stream_id,
                source,
            })?;
        self.queue_connection_output(parent)?;
        Ok(())
    }

    /// Closes one connection Session with an application error code and an
    /// owned copy of the app reason; the transport half of the generic
    /// `SessionWorker::close_connection` dispatch, replacing the
    /// `transport_close_connection_unsupported` stub. Mirrors VPP
    /// `quic_quicly_on_app_closed` (quic_quicly.c:1086-1177): only a
    /// connection Session is admitted (O(1) `session_context` resolution),
    /// the application error code is checked into a varint before any
    /// transport side effect, and the connection-state machine follows VPP —
    /// OPENED/HANDSHAKE/READY become ACTIVE_CLOSING with `quicly_close`
    /// (Hammer's worker-local `Connection::close` with the checked code and
    /// the owned reason bytes), PASSIVE_CLOSING becomes
    /// PASSIVE_CLOSING_APP_CLOSED, PASSIVE_CLOSING_QUIC_CLOSED is cleaned up,
    /// and ACTIVE_CLOSING is left alone. The connection is queued for output
    /// and its deadline resynced only after the mutation, so the
    /// CONNECTION_CLOSE frame transmits. The Session Worker's app-close guard
    /// (runtime.rs:1740) already recorded AppClosed before dispatch, and the
    /// connection state itself makes repeated dispatch a no-op.
    pub(super) fn close_connection_action(
        &mut self,
        sessions: &mut SessionWorker,
        session: u32,
        code: u64,
        reason: &[u8],
    ) -> RuntimeResult<()> {
        let context = self.session_context(sessions, session)?;
        let state = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .map(|connection| connection.state)
            .ok_or(QuicWorkerError::SessionNotConnection { session })?;
        let error_code = Self::app_error_code_varint(session, code)?;
        let reason = Bytes::copy_from_slice(reason);
        let now = Instant::now();
        match state {
            ConnectionState::Handshaking | ConnectionState::Established => {
                self.contexts
                    .get_mut(context.into())
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?
                    .engine_mut(context)?
                    .connection_mut()?
                    .close(now, error_code, reason);
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
        }
    }

    pub(super) fn lower_session(&self, context: u32) -> RuntimeResult<u32> {
        self.contexts
            .get(context.into())
            .and_then(Context::lower_session)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context }.into())
    }

    pub(super) fn lower_session_if_present(&self, context: u32) -> Option<u32> {
        self.contexts
            .get(context.into())
            .and_then(Context::lower_session)
    }

    pub(super) fn listener_context_id(&self, context: u32) -> Option<u32> {
        self.contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.listener)
    }

    pub(super) fn connection_index(&self, index: u32) -> RuntimeResult<u32> {
        self.contexts
            .get(index)
            .and_then(|context| context.connection_index(index))
            .ok_or_else(|| QuicWorkerError::ContextMissing { context: index }.into())
    }

    pub(super) fn remove_context(&mut self, context: u32) -> RuntimeResult<()> {
        self.timers.stop(context, QuicTimerKind::Handshake);
        self.timers.stop(context, QuicTimerKind::Transmit);
        self.contexts.remove(context);
        Ok(())
    }

    pub(super) fn process_udp_rx(
        &mut self,
        sessions: &mut SessionWorker,
        lower_session: u32,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.drain_connection_events_inner(sessions, context, now)?;
        self.schedule_connection_outputs(sessions, now)
    }

    fn drain_connection_events_inner(
        &mut self,
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
                    sessions.stream_connect(self.protocol, context.into(), connection)?
                } else if listener.is_some() {
                    let outer_listener = outer_listener
                        .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                    let upper =
                        sessions.stream_accept(self.protocol, context.into(), outer_listener)?;
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
                            if stream_context.parent == context
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
                self.remove_context(stream_context)?;
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
            self.remove_context(stream_context)?;
        } else if reset {
            sessions.notify_transport_reset(stream_session, stream_context)?;
        } else {
            sessions.notify_transport_closing(None, stream_session, stream_context)?;
        }
        Ok(())
    }

    fn create_stream_context(
        &mut self,
        sessions: &mut SessionWorker,
        context: u32,
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
            .unwrap_or(((0), None, None));
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
        sessions: &mut SessionWorker,
        context: u32,
        stream: quinn_proto::StreamId,
        accepted: bool,
        application: u32,
        app: Option<u32>,
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
        sessions: &mut SessionWorker,
        context: u32,
        stream: quinn_proto::StreamId,
        accepted: bool,
        application: u32,
        app: Option<u32>,
        opaque: Option<u64>,
    ) -> RuntimeResult<(u32, u32, Arc<Fifo>, Arc<Fifo>, u64)> {
        let parent = context.into();
        let parent_session = self
            .contexts
            .get(parent)
            .and_then(Context::connection)
            .and_then(|connection| connection.connection_session);
        // VPP `quic_quicly_on_stream_open` inherits the parent connection's
        // app worker onto the accepted stream Session
        // (`sctx->parent_app_wrk_id = qctx->parent_app_wrk_id`,
        // `stream_session->app_wrk_index = sctx->parent_app_wrk_id`,
        // quic_quicly.c:831-853): an accepted child inherits the parent
        // Session's builtin app endpoint when it carries an app; app-less
        // and external parents keep external children (unchanged).
        let parent_endpoint =
            parent_session.and_then(|parent| sessions.session_app_endpoint(parent));
        let (child_application, child_app, child_opaque, child_server_name) = match parent_endpoint
        {
            Some((application, Some(app), opaque, server_name)) if accepted => (
                application,
                Some(app),
                opaque,
                server_name.map(str::to_owned),
            ),
            _ => (application, if accepted { None } else { app }, opaque, None),
        };
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
        let stream_context = self.contexts.insert(Context::stream(parent, 0, stream));
        let session_id = match sessions.construct_transport_session(
            self.protocol,
            stream_context,
            allocation_owner,
            child_application,
            child_app,
            child_opaque,
            child_server_name.as_deref(),
            accepted,
        ) {
            Ok(session_id) => session_id,
            Err(primary) => {
                let cleanup = self.remove_context(stream_context).err();
                return Err(match cleanup {
                    Some(cleanup) => QuicWorkerError::StreamConnectCleanupFailed {
                        context: stream_context,
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
                context: stream_context,
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
                .and_then(|()| {
                    if child_app.is_some() {
                        // VPP `quic_quicly_on_stream_open` fires the accept
                        // callback on the inherited app worker
                        // (`app_worker_accept_notify`, quic_quicly.c:873); the
                        // builtin child completes through the same
                        // `complete_stream_connect` as locally-initiated
                        // streams so the Session App accept callback fires.
                        sessions.complete_stream_connect(session_id)
                    } else {
                        sessions.publish_accepted_transport_session(session_id)
                    }
                });
            if let Err(primary) = publication {
                // Both publication paths already remove the Session on
                // failure, so roll back only a still-live child Session.
                let session_cleanup = if sessions.has_session(session_id) {
                    sessions.rollback_session_creation(session_id).err()
                } else {
                    None
                };
                let context_cleanup = self.remove_context(stream_context).err();
                let cleanup = session_cleanup.or(context_cleanup);
                return Err(match cleanup {
                    Some(cleanup) => QuicWorkerError::StreamConnectCleanupFailed {
                        context: stream_context,
                        primary,
                        cleanup,
                    }
                    .into(),
                    None => primary,
                });
            }
        }
        // Builtin and external Session FIFOs are owned by SessionEntry
        // (`construct_app_transport_session` / `construct_external_...`);
        // `fifo_pair` resolves both paths O(1) without AppSession lookup.
        let (rx_fifo, tx_fifo) = sessions
            .fifo_pair(session_id)
            .map(|(rx, tx)| (Arc::clone(rx), Arc::clone(tx)))
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

    fn drain_io_events(&mut self, sessions: &mut SessionWorker, context: u32) -> RuntimeResult<()> {
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
        sessions: &mut SessionWorker,
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
        context: u32,
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

    fn queue_connection_output(&mut self, context: u32) -> RuntimeResult<()> {
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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

    pub(super) fn app_rx_evt(&mut self, index: u32, rx_available: usize) -> RuntimeResult<bool> {
        let context = index;
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
        let parent_context = parent;
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

    fn update_time(&mut self, sessions: &mut SessionWorker, now: Instant) -> RuntimeResult<()> {
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

    fn stream_contexts(&self, parent: u32) -> Vec<(u32, u32)> {
        let parent = parent;
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

    fn connection_is_drained(&self, context: u32) -> bool {
        self.contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .and_then(|engine| engine.connection.as_ref())
            .is_some_and(Connection::is_drained)
    }

    fn sync_connection_deadline_from_engine(
        &mut self,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
    ) -> RuntimeResult<()> {
        let stream_contexts = self.stream_contexts(context);
        for (index, session) in stream_contexts {
            if sessions.has_session(session) {
                sessions.notify_transport_deleted(session, index)?;
            }
            if self.contexts.contains_key(index) {
                self.remove_context(index)?;
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        context: u32,
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
        sessions: &mut SessionWorker,
        session_id: u32,
        index: u32,
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
                .ok_or_else(|| QuicWorkerError::ContextMissing { context: index })?;
            (
                stream.stream,
                stream.parent,
                stream.bytes_written,
                stream.flags & STREAM_APP_CLOSE_PENDING != 0,
            )
        };
        let parent_context = parent;
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
    _runtime: &DataPlaneMain,
    sessions: &mut SessionWorker,
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
    runtime: &DataPlaneMain,
    sessions: &mut SessionWorker,
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

/// Opens one stream child of `parent` through the globally registered
/// transport VFT. Mirrors VPP resolving the transport VFT and invoking
/// `quic_connect_stream` (transport.c:500-505, quic.c:164): the QUIC shim
/// resolves the owning worker from the Session's `DataWorkerId` and uses the
/// process-global QUIC authority.
pub(crate) fn quic_transport_open_stream(
    sessions: &mut SessionWorker,
    parent: u32,
    direction: SessionStreamDirection,
    app_context: u64,
) -> RuntimeResult<u32> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    let applications = application_main();
    main.with_worker_and_sessions(sessions, |sessions, quic| {
        quic.open_stream(applications, sessions, parent, direction, app_context)
    })
}

/// Resets one QUIC stream Session through the globally registered transport
/// VFT. Mirrors VPP resolving the transport VFT and
/// invoking `transport_reset` (transport.h:138) from `session_transport_reset`
/// (session.c:1687-1703): the Session Worker resolves the owning worker from
/// its own `DataWorkerId` in O(1), and the QUIC Main comes from the same
/// QUIC_MAIN channel the session queue entry points use. No scan,
/// allocation, or lock on the dispatch path.
pub(crate) fn quic_transport_reset_stream(
    sessions: &mut SessionWorker,
    session: u32,
    code: u64,
) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    main.with_worker_and_sessions(sessions, |sessions, quic| {
        quic.reset_stream(sessions, session, code)
    })
}

/// Stops the receive side of one QUIC stream Session through the globally
/// registered transport VFT. Mirrors VPP resolving the transport VFT from
/// `session_transport_half_close` (session.c:1637-1648) and the
/// receive-side `quicly_request_stop` in `quic_quicly_on_app_reset`
/// (quic_quicly.c:1253-1259): the Session Worker resolves the owning worker
/// from its own `DataWorkerId` in O(1), and the QUIC Main comes from the same
/// QUIC_MAIN channel the session queue entry points use. No scan, allocation,
/// or lock on the dispatch path.
pub(crate) fn quic_transport_stop_sending(
    sessions: &mut SessionWorker,
    session: u32,
    code: u64,
) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    main.with_worker_and_sessions(sessions, |sessions, quic| {
        quic.stop_sending(sessions, session, code)
    })
}

/// Closes one connection Session through the globally registered transport
/// VFT. Mirrors VPP resolving the transport VFT and
/// invoking `transport_close` (transport.h:131) from `session_transport_close`
/// (session.c:1657-1682): the Session Worker resolves the owning worker from
/// its own `DataWorkerId` in O(1), the reason stays raw `&[u8]` (never
/// narrowed to `&str`), and the QUIC Main comes from the same QUIC_MAIN
/// channel the session queue entry points use. No scan, allocation, or lock
/// on the dispatch path; the reason copy and the quinn close happen on the
/// owning worker inside `close_connection_action`.
pub(crate) fn quic_transport_close_connection(
    sessions: &mut SessionWorker,
    connection: u32,
    code: u64,
    reason: &[u8],
) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    main.with_worker_and_sessions(sessions, |sessions, quic| {
        quic.close_connection_action(sessions, connection, code, reason)
    })
}

impl QuicWorker {
    fn begin_stream_close(
        &mut self,
        sessions: &mut SessionWorker,
        context: u32,
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
        let parent_context = parent;
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

impl SessionTransport for QuicWorker {
    type Tx = TransportInternalTx;

    #[inline]
    fn protocol(&self) -> u8 {
        self.protocol
    }

    fn connection_index(&self, index: u32) -> RuntimeResult<u32> {
        self.connection_index(index)
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker,
        _: &DataPlaneMain,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.update_time(sessions, now)
    }

    fn app_rx_evt(
        &mut self,
        index: u32,
        rx_available: usize,
        _: usize,
        _: &DataPlaneMain,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
    ) -> RuntimeResult<bool> {
        self.app_rx_evt(index, rx_available)
    }

    fn disconnect(
        &mut self,
        sessions: &mut SessionWorker,
        index: u32,
        _: &DataPlaneMain,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        let context = index;
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
        sessions: &mut SessionWorker,
        index: u32,
        _: &DataPlaneMain,
        _: SessionQueueNext,
        _: &mut BufferFrame,
        _: &mut SessionQueueOutput,
        now: Instant,
    ) -> RuntimeResult<()> {
        let context = index;
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
        let parent_context = parent;
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

impl TransportInternalTransport for QuicWorker {
    fn internal_tx(
        &mut self,
        sessions: &mut SessionWorker,
        session_id: u32,
        index: u32,
        _: &DataPlaneMain,
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
    ContextMissing { context: u32 },
    #[error("QUIC stream {stream:?} is missing")]
    StreamMissing { stream: quinn_proto::StreamId },
    #[error("QUIC engine is not installed for context {context:?}")]
    EngineMissing { context: u32 },
    #[error("QUIC protocol connection is not established")]
    ConnectionMissing,
    #[error("QUIC server configuration is missing for context {context:?}")]
    ServerConfigMissing { context: u32 },
    #[error("QUIC client configuration is missing for active connect")]
    ClientConfigurationMissing,
    #[error("QUIC client connect failed for context {context:?}: {source}")]
    ClientConnectFailed {
        context: u32,
        #[source]
        source: quinn_proto::ConnectError,
    },
    #[error(
        "QUIC client connect failed for context {context:?}: {connect}; context cleanup failed: {cleanup}"
    )]
    ClientConnectCleanupFailed {
        context: u32,
        #[source]
        connect: quinn_proto::ConnectError,
        cleanup: RuntimeError,
    },
    #[error("QUIC Session {session:?} is missing")]
    SessionMissing { session: u32 },
    #[error("QUIC datagram for Session {session:?} has invalid length {length}")]
    InvalidDatagram { session: u32, length: u32 },
    #[error("QUIC RX datagram scratch slot {slot} is busy for context {context:?}")]
    RxDatagramScratchBusy { context: u32, slot: usize },
    #[error("QUIC context {context:?} has incompatible endpoints {local} and {remote}")]
    InvalidEndpoint {
        context: u32,
        local: SocketAddr,
        remote: SocketAddr,
    },
    #[error("QUIC context {context:?} could not reserve {bytes} output bytes")]
    OutputReservationFailed {
        context: u32,
        bytes: usize,
        #[source]
        source: FifoError,
    },
    #[error(
        "QUIC connection TX queue is full while scheduling context {context:?} (capacity {capacity})"
    )]
    OutputQueueCapacityExceeded { context: u32, capacity: usize },
    #[error("QUIC engine produced an invalid packet for context {context:?}: {bytes} bytes")]
    EnginePacketTooLarge { context: u32, bytes: usize },
    #[error(
        "QUIC output commit length mismatch for context {context:?}: expected {expected}, got {actual}"
    )]
    OutputCommitLengthMismatch {
        context: u32,
        expected: usize,
        actual: usize,
    },
    #[error("QUIC timer update failed for context {context:?}")]
    TimerUpdateFailed { context: u32 },
    #[error(
        "QUIC timer update failed for context {context:?}: {timer}; context cleanup failed: {cleanup}"
    )]
    TimerUpdateCleanupFailed {
        context: u32,
        #[source]
        timer: RuntimeError,
        cleanup: RuntimeError,
    },
    #[error("QUIC Session FIFO stream data error: {error}")]
    StreamData {
        context: u32,
        #[source]
        error: quinn_proto::StreamDataError,
    },
    #[error("QUIC stream {stream:?} write sync failed for context {context:?}: {source}")]
    StreamWrite {
        context: u32,
        stream: quinn_proto::StreamId,
        #[source]
        source: quinn_proto::WriteError,
    },
    #[error("QUIC stream {stream:?} finish failed for context {context:?}: {source}")]
    StreamFinish {
        context: u32,
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
        context: u32,
        direction: quinn_proto::Dir,
    },
    #[error(
        "QUIC stream connect failed for parent context {context:?}: {primary}; context cleanup failed: {cleanup}"
    )]
    StreamConnectCleanupFailed {
        context: u32,
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
    SessionNotQuic { session: u32 },
    #[error("Session {session:?} is a QUIC connection, not a stream")]
    SessionNotStream { session: u32 },
    #[error("Session {session:?} is a QUIC stream, not a connection")]
    SessionNotConnection { session: u32 },
    #[error("QUIC stream {stream:?} has no local send side to reset")]
    StreamSendSideMissing { stream: quinn_proto::StreamId },
    #[error("QUIC stream {stream:?} has no local receive side to stop")]
    StreamReceiveSideMissing { stream: quinn_proto::StreamId },
    #[error("QUIC stream {stream:?} reset failed for context {context:?}: {source}")]
    StreamReset {
        context: u32,
        stream: quinn_proto::StreamId,
        #[source]
        source: quinn_proto::ClosedStream,
    },
    #[error("QUIC stream {stream:?} stop failed for context {context:?}: {source}")]
    StreamStop {
        context: u32,
        stream: quinn_proto::StreamId,
        #[source]
        source: quinn_proto::ClosedStream,
    },
    #[error("QUIC application error code {code} for Session {session:?} is not a valid varint")]
    ApplicationErrorCodeInvalid { session: u32, code: u64 },
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

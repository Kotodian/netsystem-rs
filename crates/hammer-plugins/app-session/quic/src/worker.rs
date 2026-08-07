//! QUIC's worker-local transport state and registration.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use hammer_core::data_plane::BufferFrame;
use hammer_infra::bytes::BytesBuffer;
use hammer_infra::pool::{Index, Pool};
use hammer_infra::thread_owned::ThreadOwnedError;
use hammer_infra::timer_wheel::TimerWheel1t2w2048sl;
use hammer_runtime::app::{ApplicationId, SessionAppId, SessionDgramHeader};
use hammer_runtime::{DataPlaneRuntime, DataWorkerId, RuntimeResult, SessionListenerId};
use hammer_service::session::SessionId;
use hammer_service::session::node::{SessionQueueNext, SessionQueueOutput};
use hammer_service::session::runtime::{
    SessionTransport, SessionTransportId, SessionWorker, TransportInternalTransport,
    TransportInternalTx,
};
use quinn_proto::{
    Connection, ConnectionHandle, DatagramEvent, Endpoint, EndpointConfig, Event, StreamEvent,
};

use crate::config::ConfigId;
use crate::stream_io::StreamIoTable;

pub(super) const QUIC_CONTEXT_CAPACITY: usize = 4_096;

const MAX_PACKET_SIZE: usize = 1280;
const RX_DATAGRAM_BURST: usize = 16;
const TX_PACKET_BURST: usize = 10;
const TIMER_RESOLUTION: Duration = Duration::from_millis(1);
const TIMER_MAX_TICKS_PER_UPDATE: u32 = 1_024;
const TIMER_EXPIRY_BUDGET: usize = 256;
const TIMER_WHEEL_MAX_INTERVAL_TICKS: u64 = 2048 * 2048 - 1;

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
    pub(crate) server_config: Option<Arc<quinn_proto::ServerConfig>>,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Handshaking,
    Established,
}

struct EngineConnection {
    handle: Option<ConnectionHandle>,
    connection: Option<Connection>,
    remote: Option<SocketAddr>,
    local: Option<SocketAddr>,
    server_config: Option<Arc<quinn_proto::ServerConfig>>,
    application: ApplicationId,
    app: Option<SessionAppId>,
    opaque: Option<u64>,
    io_table: Box<StreamIoTable>,
}

impl EngineConnection {
    fn pending(
        server_config: Option<Arc<quinn_proto::ServerConfig>>,
        application: ApplicationId,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
    ) -> Self {
        Self {
            handle: None,
            connection: None,
            remote: None,
            local: None,
            server_config,
            application,
            app,
            opaque,
            io_table: StreamIoTable::new(),
        }
    }

    fn connection_mut(&mut self) -> RuntimeResult<&mut Connection> {
        self.connection
            .as_mut()
            .ok_or_else(|| QuicWorkerError::EngineMissing.into())
    }
}

#[repr(C)]
struct ConnectionContext {
    engine: Option<Box<EngineConnection>>,
    lower_session: SessionId,
    upper_session: Option<SessionId>,
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
        server_config: Option<Arc<quinn_proto::ServerConfig>>,
    ) -> Self {
        Self {
            role: ContextRole::Listener(ListenerContext {
                outer_listener,
                outer_application,
                inner_application_listener,
                inner_session_listener,
                configuration,
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
        opaque: Option<u64>,
    ) -> Self {
        let server_config = listener_context.and_then(|listener| listener.server_config.clone());
        Self {
            role: ContextRole::Connection(ConnectionContext {
                engine: Some(Box::new(EngineConnection::pending(
                    server_config,
                    application,
                    app,
                    opaque,
                ))),
                lower_session,
                upper_session: None,
                listener,
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

    fn session(&self) -> Option<SessionId> {
        match &self.role {
            ContextRole::Connection(connection) => Some(connection.lower_session),
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
    Accept = 0,
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
            0 => Some(Self::Accept),
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
        let consumed_ticks = u32::try_from(self.wheel.current_tick() - tick_before)
            .expect("QUIC timer wheel consumes no more than the requested u32 ticks");
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

/// Data Worker-owned QUIC context pool and sans-I/O endpoint.
#[hammer_component_macros::session_transport(
    name = "quic",
    start_listen = crate::listener::start_listen,
    stop_listen = crate::listener::stop_listen,
)]
pub struct QuicWorker {
    endpoint: Endpoint,
    contexts: Pool<Context>,
    timers: QuicTimers,
    tx_bufs: BytesBuffer,
    connection_tx_pending: Vec<ContextId>,
}

impl QuicWorker {
    pub fn new(_: DataWorkerId) -> Self {
        Self {
            endpoint: Endpoint::new(Arc::new(EndpointConfig::default()), None, false, None),
            contexts: Pool::with_capacity(QUIC_CONTEXT_CAPACITY),
            timers: QuicTimers::new(Instant::now()),
            tx_bufs: BytesBuffer::with_capacity(TX_PACKET_BURST * MAX_PACKET_SIZE),
            connection_tx_pending: Vec::new(),
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
        self.timers
            .set(
                context,
                QuicTimerKind::Accept,
                Duration::from_millis(30_000),
            )
            .map_err(|error| {
                self.remove_context(context)
                    .expect("new QUIC context remains present when timer start fails");
                error
            })?;
        Ok(context)
    }

    pub(super) fn connect_connection(
        &mut self,
        lower_session: SessionId,
        application: ApplicationId,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
    ) -> RuntimeResult<ContextId> {
        Ok(self
            .contexts
            .insert(Context::connection_with_listener(
                lower_session,
                None,
                application,
                None,
                app,
                opaque,
            ))
            .map(ContextId::from)
            .ok_or_else(|| QuicWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            })?)
    }

    pub(super) fn context_session(&self, context: ContextId) -> RuntimeResult<SessionId> {
        self.contexts
            .get(context.into())
            .and_then(Context::session)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context }.into())
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
        self.timers.stop(context, QuicTimerKind::Accept);
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
        for _ in 0..RX_DATAGRAM_BURST {
            let (header, data, record_len) = {
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
                    break;
                };
                let payload_len = header.data_length() as usize;
                let record_len =
                    header
                        .total_len()
                        .ok_or_else(|| QuicWorkerError::InvalidDatagram {
                            session: lower_session,
                            length: header.data_length(),
                        })?;
                if rx_fifo.max_dequeue() < record_len {
                    break;
                }
                if payload_len > MAX_PACKET_SIZE {
                    rx_fifo.dequeue_drop(record_len);
                    consumed = consumed.saturating_add(record_len);
                    continue;
                }
                let mut data = BytesMut::with_capacity(payload_len);
                data.resize(payload_len, 0);
                let copied = rx_fifo.peek(SessionDgramHeader::SIZE, payload_len, &mut data[..]);
                if copied != payload_len {
                    break;
                }
                (header, data, record_len)
            };
            let remote = header.remote();
            let local = header.local();
            self.process_one_datagram(sessions, context, local, remote, data, now)?;
            sessions
                .fifo_pair(lower_session)
                .map(|(rx, _)| rx.dequeue_drop(record_len))
                .unwrap_or(0);
            consumed = consumed.saturating_add(record_len);
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
        data: BytesMut,
        now: Instant,
    ) -> RuntimeResult<()> {
        let has_connection = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .map(|engine| engine.connection.is_some())
            .unwrap_or(false);
        if !has_connection {
            return self.accept_first_datagram(sessions, context, local, remote, data, now);
        }
        let connection_context = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        let engine = connection_context
            .engine
            .as_mut()
            .expect("QUIC Connection context owns an EngineConnection");
        engine.remote = Some(remote);
        engine.local = Some(local);
        let connection = engine.connection_mut()?;
        connection.handle_datagram(now, remote, None, data);
        self.drain_connection_events(sessions, context, now)
    }

    fn accept_first_datagram(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        local: SocketAddr,
        remote: SocketAddr,
        data: BytesMut,
        now: Instant,
    ) -> RuntimeResult<()> {
        let server_config = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .and_then(|connection| connection.engine.as_ref())
            .and_then(|engine| engine.server_config.clone())
            .ok_or(QuicWorkerError::ServerConfigMissing { context })?;
        self.endpoint
            .set_server_config(Some(Arc::clone(&server_config)));
        let event = self.endpoint.handle(
            now,
            remote,
            Some(local.ip()),
            None,
            data,
            &mut *self.tx_bufs,
        );
        let Some(event) = event else {
            return Ok(());
        };
        match event {
            DatagramEvent::NewConnection(incoming) => {
                let (handle, mut connection) = self
                    .endpoint
                    .accept(
                        incoming,
                        now,
                        &mut *self.tx_bufs,
                        Some(Arc::clone(&server_config)),
                    )
                    .map_err(|error| QuicWorkerError::AcceptFailed {
                        context,
                        source: error.cause,
                    })?;
                let connection_context = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::connection_mut)
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                let engine = connection_context
                    .engine
                    .as_mut()
                    .expect("QUIC Connection context owns an EngineConnection");
                let io = engine.io_table.io();
                connection.set_stream_data_io(Some(io));
                engine.handle = Some(handle);
                engine.connection = Some(connection);
                engine.remote = Some(remote);
                engine.local = Some(local);
                self.timers.stop(context, QuicTimerKind::Accept);
                self.drain_connection_events(sessions, context, now)
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
                Ok(())
            }
            DatagramEvent::ConnectionEvent(_, _) => Ok(()),
        }
    }

    fn drain_connection_events(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        now: Instant,
    ) -> RuntimeResult<()> {
        for _ in 0..8 {
            let (events, endpoint_events, handled) = {
                let connection_context = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::connection_mut)
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                let engine = connection_context
                    .engine
                    .as_mut()
                    .expect("QUIC Connection context owns an EngineConnection");
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
        self.schedule_connection_outputs(sessions, now)
    }

    fn handle_connection_event(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        event: Event,
    ) -> RuntimeResult<()> {
        let mut close = false;
        let mut stream_event = None;
        match event {
            Event::Connected => {
                let connection_context = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::connection_mut)
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                if connection_context.upper_session.is_none() {
                    let upper = sessions
                        .create_upper_session(connection_context.lower_session, context.into())?;
                    connection_context.upper_session = Some(upper);
                }
                connection_context.state = ConnectionState::Established;
                connection_context.listener = None;
            }
            Event::HandshakeDataReady => {}
            Event::ConnectionLost { .. } => close = true,
            Event::Stream(event) => stream_event = Some(event),
            Event::DatagramReceived | Event::DatagramsUnblocked => {}
        }
        if close {
            self.close_connection(sessions, context)?;
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
        let mut to_close = None;
        match event {
            StreamEvent::Opened { dir } => {
                let streams = self
                    .contexts
                    .get_mut(context.into())
                    .and_then(Context::connection_mut)
                    .and_then(|connection| connection.engine.as_mut())
                    .and_then(|engine| engine.connection_mut().ok())
                    .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
                while let Some(stream) = streams.streams().accept(dir) {
                    to_create.push((stream, true));
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
            }
            StreamEvent::Finished { id } => to_close = Some((id, false)),
            StreamEvent::Stopped { id, .. } => to_close = Some((id, true)),
            StreamEvent::Available { .. } => {}
        }
        for (stream, accepted) in to_create {
            self.create_stream_context(sessions, context, stream, accepted)?;
        }
        if let Some((stream, reset)) = to_close {
            self.close_stream_context(sessions, context, stream, reset)?;
        }
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
            .ok_or_else(|| QuicWorkerError::StreamMissing { stream })?;
        let stream_session = self
            .contexts
            .get(stream_context)
            .and_then(|value| match &value.role {
                ContextRole::Stream(stream) => Some(stream.session),
                ContextRole::Listener(_) | ContextRole::Connection(_) => None,
            })
            .ok_or_else(|| QuicWorkerError::StreamMissing { stream })?;
        if reset {
            sessions.notify_transport_reset(stream_session, stream_context)?;
        } else {
            sessions.notify_transport_closed(stream_session, stream_context)?;
        }
        if let Some(engine) = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
        {
            engine.io_table.remove_stream(stream);
        }
        self.contexts.remove(stream_context);
        Ok(())
    }

    fn create_stream_context(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
        stream: quinn_proto::StreamId,
        accepted: bool,
    ) -> RuntimeResult<()> {
        let connection_context = self
            .contexts
            .get(context.into())
            .and_then(Context::connection)
            .ok_or_else(|| QuicWorkerError::ContextMissing { context })?;
        let (application, app, opaque) = connection_context
            .engine
            .as_ref()
            .map(|engine| (engine.application, engine.app, engine.opaque))
            .unwrap_or((ApplicationId::new(0, 0), None, None));
        let parent = context.into();
        let session_id = sessions.construct_transport_session(
            QuicWorker::ID,
            parent,
            application.raw(),
            application,
            app,
            opaque,
            None,
            accepted,
        )?;
        let (rx_fifo, tx_fifo) = sessions
            .app_session(session_id)
            .map(|session| (Arc::clone(session.rx_fifo()), Arc::clone(session.tx_fifo())))
            .ok_or_else(|| QuicWorkerError::SessionMissing {
                session: session_id,
            })?;
        let app_tx_data_len = tx_fifo.max_dequeue() as u64;
        let stream_context = self
            .contexts
            .insert(Context::stream(parent, session_id, stream))
            .ok_or_else(|| QuicWorkerError::ContextCapacityExhausted {
                capacity: self.contexts.capacity(),
            })?;
        let engine = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .expect("QUIC Connection context owns an EngineConnection");
        if let Err(_error) = engine.io_table.drain_pending(
            stream,
            stream_context,
            session_id,
            Arc::clone(&rx_fifo),
            Arc::clone(&tx_fifo),
            0,
            app_tx_data_len,
        ) {
            self.contexts.remove(stream_context);
            sessions.rollback_session_creation(session_id)?;
            return Err(QuicWorkerError::StreamSessionCreationFailed { context, stream }.into());
        }
        Ok(())
    }

    fn drain_io_events(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
    ) -> RuntimeResult<()> {
        let engine = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.engine.as_mut())
            .expect("QUIC Connection context owns an EngineConnection");
        let events = engine.io_table.take_events();
        for event in events {
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
        Ok(())
    }

    fn schedule_connection_outputs(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        now: Instant,
    ) -> RuntimeResult<()> {
        let contexts = std::mem::take(&mut self.connection_tx_pending);
        for context in contexts {
            self.send_packets(sessions, context, now)?;
        }
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
        let engine = connection_context
            .engine
            .as_mut()
            .expect("QUIC Connection context owns an EngineConnection");
        let (remote, local) = match (engine.remote, engine.local) {
            (Some(remote), Some(local)) => (remote, local),
            _ => return Ok(()),
        };
        let lower_session = connection_context.lower_session;
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
                return Ok(());
            }
            budget
        };
        let mut produced = 0usize;
        for _ in 0..packet_budget {
            let Some(transmit) = engine
                .connection_mut()?
                .poll_transmit(now, 1, &mut *self.tx_bufs)
            else {
                break;
            };
            let payload_len = transmit.size;
            if payload_len == 0 || payload_len > self.tx_bufs.len() {
                return Err(QuicWorkerError::OutputLengthInvalid {
                    context,
                    length: payload_len,
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
            let committed = {
                let (_, tx_fifo) = sessions.fifo_pair(lower_session).ok_or_else(|| {
                    QuicWorkerError::SessionMissing {
                        session: lower_session,
                    }
                })?;
                match tx_fifo.reserve_write(record_len) {
                    Ok(mut reservation) => {
                        reservation
                            .copy_from_segments([header_bytes.as_slice(), self.tx_bufs.as_slice()])
                            .map_err(|_| QuicWorkerError::OutputReservationFailed {
                                context,
                                bytes: record_len,
                            })?;
                        reservation.commit(record_len).map_err(|_| {
                            QuicWorkerError::OutputReservationFailed {
                                context,
                                bytes: record_len,
                            }
                        })?;
                        true
                    }
                    Err(_) => {
                        tx_fifo.want_deq_notification();
                        false
                    }
                }
            };
            if committed {
                produced = produced.saturating_add(record_len);
            } else {
                break;
            }
        }
        if produced != 0 {
            sessions.publish_tx_enqueue(lower_session, produced)?;
        }
        if let Some(error) = engine.connection_mut()?.take_stream_data_error() {
            return Err(QuicWorkerError::StreamData { context, error }.into());
        }
        if let Some(deadline) = engine.connection_mut()?.poll_timeout() {
            self.timers.set(
                context,
                QuicTimerKind::Transmit,
                deadline
                    .saturating_duration_since(now)
                    .max(TIMER_RESOLUTION),
            )?;
        } else {
            self.timers.stop(context, QuicTimerKind::Transmit);
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
        let lower_session = self.context_session(context)?;
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
            let mut reservation = tx_fifo.reserve_write(record_len).map_err(|_| {
                QuicWorkerError::OutputReservationFailed {
                    context,
                    bytes: record_len,
                }
            })?;
            reservation
                .copy_from_segments([header_bytes.as_slice(), payload])
                .map_err(|_| QuicWorkerError::OutputReservationFailed {
                    context,
                    bytes: record_len,
                })?;
            reservation.commit(record_len).map_err(|_| {
                QuicWorkerError::OutputReservationFailed {
                    context,
                    bytes: record_len,
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
            .and_then(|engine| engine.io_table.app_rx_consumed(stream_id, rx_available))
            .unwrap_or(0);
        if consumed != 0
            && let Ok(should_transmit) = self
                .contexts
                .get_mut(parent)
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .and_then(|engine| engine.connection_mut().ok())
                .map(|connection| connection.recv_stream(stream_id).credit_read(consumed))
                .unwrap_or(Ok(Default::default()))
            && should_transmit.should_transmit()
        {
            self.connection_tx_pending.push(parent_context);
        }
        Ok(false)
    }

    fn update_time(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        now: Instant,
    ) -> RuntimeResult<()> {
        self.timers.advance(now);
        while let Some(token) = self.timers.take_pending() {
            match token.kind {
                QuicTimerKind::Accept => {
                    if let Ok(lower_session) = self.context_session(token.context) {
                        sessions.set_app_session(lower_session, 0)?;
                        sessions.schedule_disconnect(lower_session);
                    }
                    self.remove_context(token.context)?;
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

    fn close_connection(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        context: ContextId,
    ) -> RuntimeResult<()> {
        let upper = self
            .contexts
            .get_mut(context.into())
            .and_then(Context::connection_mut)
            .and_then(|connection| connection.upper_session.take());
        if let Some(upper) = upper {
            sessions.notify_transport_closed(upper, context.into())?;
        }
        self.timers.stop(context, QuicTimerKind::Accept);
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
        self.remove_context(context)?;
        Ok(())
    }

    pub(super) fn stream_tx_event(
        &mut self,
        sessions: &mut SessionWorker<Index>,
        session_id: SessionId,
        index: Index,
        now: Instant,
    ) -> RuntimeResult<()> {
        let (stream_id, parent, bytes_written) = {
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
            (stream.stream, stream.parent, stream.bytes_written)
        };
        let parent_context = ContextId::from(parent);
        let pending = sessions.pending_send_len(session_id)?.unwrap_or(0);
        let end_offset = bytes_written.saturating_add(pending as u64);
        if pending != 0 {
            let connection = self
                .contexts
                .get_mut(parent)
                .and_then(Context::connection_mut)
                .and_then(|connection| connection.engine.as_mut())
                .and_then(|engine| engine.connection_mut().ok())
                .ok_or_else(|| QuicWorkerError::ContextMissing {
                    context: parent_context,
                })?;
            connection
                .send_stream(stream_id)
                .sync(end_offset)
                .map_err(|source| QuicWorkerError::StreamWrite {
                    context: parent_context,
                    stream: stream_id,
                    source,
                })?;
        }
        if let Some(stream) = self
            .contexts
            .get_mut(index)
            .and_then(|value| value.stream_mut())
        {
            stream.app_tx_data_len = end_offset;
        }
        self.connection_tx_pending.push(parent_context);
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
        _: Instant,
    ) -> RuntimeResult<()> {
        let context = ContextId::from(index);
        let session = self.context_session(context)?;
        self.remove_context(context)?;
        if sessions.has_session(session) {
            sessions.set_app_session(session, 0)?;
            sessions.notify_transport_deleted(session, index)?;
        }
        Ok(())
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
    #[error("QUIC engine is not installed")]
    EngineMissing,
    #[error("QUIC server configuration is missing for context {context:?}")]
    ServerConfigMissing { context: ContextId },
    #[error("QUIC first Initial accept failed for context {context:?}: {source}")]
    AcceptFailed {
        context: ContextId,
        #[source]
        source: quinn_proto::ConnectionError,
    },
    #[error("QUIC Session {session:?} is missing")]
    SessionMissing { session: SessionId },
    #[error("QUIC datagram for Session {session:?} has invalid length {length}")]
    InvalidDatagram { session: SessionId, length: u32 },
    #[error("QUIC context {context:?} has incompatible endpoints {local} and {remote}")]
    InvalidEndpoint {
        context: ContextId,
        local: SocketAddr,
        remote: SocketAddr,
    },
    #[error("QUIC context {context:?} produced an invalid output length {length}")]
    OutputLengthInvalid { context: ContextId, length: usize },
    #[error("QUIC context {context:?} could not reserve {bytes} output bytes")]
    OutputReservationFailed { context: ContextId, bytes: usize },
    #[error("QUIC stream Session creation failed for context {context:?} stream {stream:?}")]
    StreamSessionCreationFailed {
        context: ContextId,
        stream: quinn_proto::StreamId,
    },
    #[error("QUIC timer update failed for context {context:?}")]
    TimerUpdateFailed { context: ContextId },
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
            server_config: None,
        };
        let context = worker
            .accept_connection(SessionId::from_raw(5), listener_id, &listener)
            .expect("accept connection");
        assert_eq!(worker.listener_context_id(context), Some(listener_id));
        worker.remove_context(context).expect("remove context");
        assert_eq!(worker.listener_context_id(context), None);
    }
}

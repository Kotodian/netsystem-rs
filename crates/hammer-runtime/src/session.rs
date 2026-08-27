use std::net::SocketAddr;

use crate::app::{SessionFlags, SessionHandle};
use crate::{DataWorkerId, RuntimeResult};

/// Endpoint selected by the Session control plane for one transport listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SessionListenEndpoint {
    local: SocketAddr,
    worker: DataWorkerId,
}

/// Transport endpoint selected for one Session active-open request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConnectEndpoint {
    pub remote: SocketAddr,
    pub local: Option<SocketAddr>,
    pub worker: DataWorkerId,
    pub connection: u32,
    pub application: u32,
    pub parent_handle: Option<SessionHandle>,
    pub flags: SessionFlags,
    pub opaque: Option<u64>,
    pub server_name: Option<String>,
}

impl SessionConnectEndpoint {
    #[inline]
    pub const fn new(
        remote: SocketAddr,
        local: Option<SocketAddr>,
        worker: DataWorkerId,
        connection: u32,
        application: u32,
        opaque: Option<u64>,
        server_name: Option<String>,
    ) -> Self {
        Self {
            remote,
            local,
            worker,
            connection,
            application,
            parent_handle: None,
            flags: SessionFlags::empty(),
            opaque,
            server_name,
        }
    }
    #[inline]
    pub const fn new_stream(
        remote: SocketAddr,
        local: Option<SocketAddr>,
        worker: DataWorkerId,
        connection: u32,
        application: u32,
        parent_handle: SessionHandle,
        flags: SessionFlags,
        opaque: Option<u64>,
        server_name: Option<String>,
    ) -> Self {
        Self {
            remote,
            local,
            worker,
            connection,
            application,
            parent_handle: Some(parent_handle),
            flags: flags.union(SessionFlags::STREAM),
            opaque,
            server_name,
        }
    }
}

impl SessionListenEndpoint {
    #[inline]
    pub const fn new(local: SocketAddr, worker: DataWorkerId) -> Self {
        Self { local, worker }
    }

    #[inline]
    pub const fn local(self) -> SocketAddr {
        self.local
    }

    #[inline]
    pub const fn worker(self) -> DataWorkerId {
        self.worker
    }
}

/// Direction of one stream opened through the transport worker actions.
///
/// QUIC distinguishes bidirectional and unidirectional streams; the direction
/// is fixed at open time for the stream's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SessionStreamDirection {
    /// The peer may send on the opened stream as well.
    Bidi,
    /// Only the opener may send on the opened stream.
    Uni,
}

pub type SessionTransportStartListen =
    fn(SessionHandle, u32, Option<u64>, SessionListenEndpoint) -> RuntimeResult<()>;
pub type SessionTransportStopListen = fn(SessionHandle) -> RuntimeResult<()>;
pub type SessionTransportConnect = fn(SessionConnectEndpoint) -> RuntimeResult<()>;
pub type SessionTransportConnectStream = fn(SessionConnectEndpoint) -> RuntimeResult<()>;

/// Static operations registered by one transport plugin.
#[derive(Debug, Clone, Copy)]
pub struct SessionTransportRegistration {
    name: &'static str,
    start_listen: Option<SessionTransportStartListen>,
    stop_listen: Option<SessionTransportStopListen>,
    connect: Option<SessionTransportConnect>,
    connect_stream: Option<SessionTransportConnectStream>,
}

impl SessionTransportRegistration {
    #[doc(hidden)]
    #[inline]
    pub const fn new(
        name: &'static str,
        start_listen: Option<SessionTransportStartListen>,
        stop_listen: Option<SessionTransportStopListen>,
        connect: Option<SessionTransportConnect>,
    ) -> Self {
        Self {
            name,
            start_listen,
            stop_listen,
            connect,
            connect_stream: None,
        }
    }

    #[doc(hidden)]
    #[inline]
    pub const fn with_connect_stream(
        name: &'static str,
        start_listen: Option<SessionTransportStartListen>,
        stop_listen: Option<SessionTransportStopListen>,
        connect: Option<SessionTransportConnect>,
        connect_stream: Option<SessionTransportConnectStream>,
    ) -> Self {
        Self {
            name,
            start_listen,
            stop_listen,
            connect,
            connect_stream,
        }
    }

    #[inline]
    pub const fn name(self) -> &'static str {
        self.name
    }

    #[inline]
    pub const fn start_listen(self) -> Option<SessionTransportStartListen> {
        self.start_listen
    }

    #[inline]
    pub const fn stop_listen(self) -> Option<SessionTransportStopListen> {
        self.stop_listen
    }

    #[inline]
    pub const fn connect(self) -> Option<SessionTransportConnect> {
        self.connect
    }

    #[inline]
    pub const fn connect_stream(self) -> Option<SessionTransportConnectStream> {
        self.connect_stream
    }
}

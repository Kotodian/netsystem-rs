use std::net::SocketAddr;

use crate::{DataWorkerId, RuntimeResult};

/// Opaque Session-layer listener identity supplied to one selected transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[repr(transparent)]
pub struct SessionListenerId(u64);

impl SessionListenerId {
    #[inline]
    pub const fn new(slot: u32, generation: u32) -> Self {
        Self((slot as u64) | ((generation as u64) << 32))
    }

    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn slot(self) -> u32 {
        self.0 as u32
    }

    #[inline]
    pub const fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Opaque Session-layer identity for one active-open request.
///
/// A transport retains this identity only until it binds its worker-owned
/// connection to the Session worker. Application policy remains Session-owned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct SessionConnectionId(u64);

impl SessionConnectionId {
    #[doc(hidden)]
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    #[doc(hidden)]
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Endpoint selected by the Session control plane for one transport listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct SessionListenEndpoint {
    local: SocketAddr,
    worker: DataWorkerId,
}

/// Transport endpoint selected for one Session active-open request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionConnectEndpoint {
    remote: SocketAddr,
    local: Option<SocketAddr>,
    worker: DataWorkerId,
}

impl SessionConnectEndpoint {
    #[inline]
    pub const fn new(remote: SocketAddr, local: Option<SocketAddr>, worker: DataWorkerId) -> Self {
        Self {
            remote,
            local,
            worker,
        }
    }

    #[inline]
    pub const fn remote(self) -> SocketAddr {
        self.remote
    }

    #[inline]
    pub const fn local(self) -> Option<SocketAddr> {
        self.local
    }

    #[inline]
    pub const fn worker(self) -> DataWorkerId {
        self.worker
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

pub type SessionTransportStartListen = fn(
    SessionListenerId,
    crate::app::ApplicationId,
    Option<u64>,
    SessionListenEndpoint,
) -> RuntimeResult<()>;
pub type SessionTransportStopListen = fn(SessionListenerId) -> RuntimeResult<()>;
pub type SessionTransportConnect =
    fn(SessionConnectionId, SessionConnectEndpoint) -> RuntimeResult<()>;

/// Static operations registered by one transport plugin.
#[derive(Debug, Clone, Copy)]
pub struct SessionTransportRegistration {
    name: &'static str,
    start_listen: Option<SessionTransportStartListen>,
    stop_listen: Option<SessionTransportStopListen>,
    connect: Option<SessionTransportConnect>,
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
}

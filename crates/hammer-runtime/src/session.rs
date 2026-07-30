use std::net::SocketAddr;

use crate::app::AppSessionSemantics;
use crate::{DataWorkerId, RuntimeResult};

/// Opaque Session-layer listener identity supplied to one selected transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

/// Endpoint selected by the Session control plane for one transport listener.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionListenEndpoint {
    local: SocketAddr,
    worker: DataWorkerId,
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

pub type SessionTransportStartListen =
    fn(SessionListenerId, SessionListenEndpoint) -> RuntimeResult<()>;
pub type SessionTransportStopListen = fn(SessionListenerId) -> RuntimeResult<()>;

/// Static operations registered by one transport plugin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionTransportRegistration {
    name: &'static str,
    upper: AppSessionSemantics,
    start_listen: Option<SessionTransportStartListen>,
    stop_listen: Option<SessionTransportStopListen>,
}

impl SessionTransportRegistration {
    #[doc(hidden)]
    #[inline]
    pub const fn new(name: &'static str, upper: AppSessionSemantics) -> Self {
        Self {
            name,
            upper,
            start_listen: None,
            stop_listen: None,
        }
    }

    #[doc(hidden)]
    #[inline]
    pub const fn with_listener_operations(
        name: &'static str,
        upper: AppSessionSemantics,
        start_listen: SessionTransportStartListen,
        stop_listen: SessionTransportStopListen,
    ) -> Self {
        Self {
            name,
            upper,
            start_listen: Some(start_listen),
            stop_listen: Some(stop_listen),
        }
    }

    #[inline]
    pub const fn name(self) -> &'static str {
        self.name
    }

    #[inline]
    pub const fn upper(self) -> AppSessionSemantics {
        self.upper
    }

    #[inline]
    pub const fn start_listen(self) -> Option<SessionTransportStartListen> {
        self.start_listen
    }

    #[inline]
    pub const fn stop_listen(self) -> Option<SessionTransportStopListen> {
        self.stop_listen
    }
}

use std::net::SocketAddr;
use hammer_runtime::{DataWorkerId, SessionHandle};
use hammer_service::session::SessionId;

use crate::UdpIpVersion;

/// Worker-owned UDP listener state. A listener has no application Session of
/// its own; accepted remote tuples create one Session per exact peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpListener {
    local: SocketAddr,
    version: UdpIpVersion,
    session_listener: SessionHandle,
    owner_worker: DataWorkerId,
}

impl UdpListener {
    #[inline]
    pub const fn new(
        local: SocketAddr,
        session_listener: SessionHandle,
        owner_worker: DataWorkerId,
    ) -> Option<Self> {
        let version = match local {
            SocketAddr::V4(_) => UdpIpVersion::V4,
            SocketAddr::V6(_) => UdpIpVersion::V6,
        };
        Some(Self {
            local,
            version,
            session_listener,
            owner_worker,
        })
    }

    #[inline]
    pub const fn local(self) -> SocketAddr {
        self.local
    }

    #[inline]
    pub const fn session_listener(self) -> SessionHandle {
        self.session_listener
    }

    #[inline]
    pub fn accepts(self, local: SocketAddr) -> bool {
        if self.local.port() != local.port() || self.version != UdpIpVersion::from(local) {
            return false;
        }
        self.local.ip().is_unspecified() || self.local.ip() == local.ip()
    }
}

impl From<SocketAddr> for UdpIpVersion {
    #[inline]
    fn from(value: SocketAddr) -> Self {
        match value {
            SocketAddr::V4(_) => Self::V4,
            SocketAddr::V6(_) => Self::V6,
        }
    }
}

/// A connected UDP tuple embedded in a worker-local pool, mirroring VPP's
/// `udp_connection_t` whose transport identity is pool-local to one worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UdpConnection {
    owner_worker: DataWorkerId,
    local: SocketAddr,
    remote: SocketAddr,
    session: Option<SessionId>,
    listener: bool,
    closing: bool,
}

impl UdpConnection {
    #[inline]
    pub const fn connected(
        owner_worker: DataWorkerId,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<Self> {
        if local.is_ipv4() != remote.is_ipv4() || local.port() == 0 || remote.port() == 0 {
            return None;
        }
        Some(Self {
            owner_worker,
            local,
            remote,
            session: None,
            listener: false,
            closing: false,
        })
    }

    #[inline]
    pub const fn local(self) -> SocketAddr {
        self.local
    }

    #[inline]
    pub const fn remote(self) -> SocketAddr {
        self.remote
    }

    #[inline]
    pub const fn session(self) -> Option<SessionId> {
        self.session
    }

    #[inline]
    pub fn attach_session(&mut self, session: SessionId) -> bool {
        if self.session.is_some() {
            return false;
        }
        self.session = Some(session);
        true
    }
}

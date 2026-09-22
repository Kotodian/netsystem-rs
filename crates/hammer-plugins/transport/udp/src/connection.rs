use hammer_plugin_session::{IpTransportConnection, IpTransportConnectionId};
use hammer_runtime::DataWorkerId;
use hammer_runtime::app::SessionHandle;
use hammer_service::transport::{Pacer, TransportConnectionFlags};
use std::net::SocketAddr;

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
#[derive(Debug, Clone, Copy)]
pub struct UdpConnection {
    pub base: IpTransportConnection,
    owner_worker: DataWorkerId,
    local: SocketAddr,
    remote: SocketAddr,
    session: Option<u32>,
    listener: bool,
    closing: bool,
}

impl UdpConnection {
    #[inline]
    pub fn listener(
        owner_worker: DataWorkerId,
        transport_protocol: u8,
        local: SocketAddr,
        session: u32,
    ) -> Self {
        let remote = match local {
            SocketAddr::V4(_) => SocketAddr::from(([0, 0, 0, 0], 0)),
            SocketAddr::V6(_) => SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0)),
        };
        let endpoint = ip_transport_connection_id(local, remote, transport_protocol);
        Self {
            base: IpTransportConnection {
                session: hammer_service::session::SessionHandle {
                    worker_index: owner_worker.slot() as u32,
                    session_index: session,
                },
                connection_index: u32::MAX,
                worker_index: owner_worker.slot() as u32,
                flags: TransportConnectionFlags {
                    connectionless: true,
                    ..TransportConnectionFlags::default()
                },
                endpoint,
                pacer: Pacer::default(),
                opaque: 0,
            },
            owner_worker,
            local,
            remote,
            session: Some(session),
            listener: true,
            closing: false,
        }
    }

    #[inline]
    pub fn connected(
        owner_worker: DataWorkerId,
        transport_protocol: u8,
        local: SocketAddr,
        remote: SocketAddr,
    ) -> Option<Self> {
        if local.is_ipv4() != remote.is_ipv4() || local.port() == 0 || remote.port() == 0 {
            return None;
        }
        let endpoint = ip_transport_connection_id(local, remote, transport_protocol);
        Some(Self {
            base: IpTransportConnection {
                session: hammer_service::session::SessionHandle::invalid(),
                connection_index: u32::MAX,
                worker_index: owner_worker.slot() as u32,
                flags: TransportConnectionFlags {
                    connectionless: true,
                    ..TransportConnectionFlags::default()
                },
                endpoint,
                pacer: Pacer::default(),
                opaque: 0,
            },
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
    pub const fn session(self) -> Option<u32> {
        self.session
    }

    #[inline]
    pub fn attach_session(&mut self, session: u32) -> bool {
        if self.session.is_some() {
            return false;
        }
        self.session = Some(session);
        self.base.session = hammer_service::session::SessionHandle {
            worker_index: self.base.worker_index,
            session_index: session,
        };
        true
    }

    #[inline]
    pub fn close(&mut self) {
        self.closing = true;
    }
}

fn ip_transport_connection_id(
    local: SocketAddr,
    remote: SocketAddr,
    transport_protocol: u8,
) -> IpTransportConnectionId {
    match (local, remote) {
        (SocketAddr::V4(local), SocketAddr::V4(remote)) => {
            (0, local, remote, transport_protocol).into()
        }
        (SocketAddr::V6(local), SocketAddr::V6(remote)) => {
            (0, local, remote, transport_protocol).into()
        }
        _ => panic!("UDP connection endpoint family mismatch"),
    }
}

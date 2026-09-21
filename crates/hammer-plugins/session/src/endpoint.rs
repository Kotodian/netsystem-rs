use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};

use hammer_service::session::SessionEndpoint;

pub const ENDPOINT_INVALID_INDEX: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpTransportEndpoint {
    pub address: IpAddr,
    pub port: u16,
    pub sw_if_index: u32,
    pub fib_index: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpTransportEndpointConfig {
    pub local: IpTransportEndpoint,
    pub peer: IpTransportEndpoint,
    pub next_node_index: u32,
    pub next_node_opaque: u32,
    pub mss: u16,
    pub dscp: u8,
    pub transport_flags: u8,
}

pub type IpSessionEndpoint = SessionEndpoint<IpTransportEndpointConfig>;

impl From<IpSessionEndpoint> for IpTransportEndpointConfig {
    #[inline(always)]
    fn from(endpoint: IpSessionEndpoint) -> Self {
        *endpoint.transport()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IpTransportConnectionId {
    Ip4 {
        remote_address: Ipv4Addr,
        local_address: Ipv4Addr,
        fib_index: u32,
        remote_port: u16,
        local_port: u16,
        dscp: u8,
        transport_protocol: u8,
    },
    Ip6 {
        remote_address: Ipv6Addr,
        local_address: Ipv6Addr,
        fib_index: u32,
        remote_port: u16,
        local_port: u16,
        dscp: u8,
        transport_protocol: u8,
    },
}

impl From<(u32, SocketAddrV4, SocketAddrV4, u8)> for IpTransportConnectionId {
    #[inline(always)]
    fn from(
        (fib_index, local, remote, transport_protocol): (u32, SocketAddrV4, SocketAddrV4, u8),
    ) -> Self {
        Self::Ip4 {
            remote_address: *remote.ip(),
            local_address: *local.ip(),
            fib_index,
            remote_port: remote.port().to_be(),
            local_port: local.port().to_be(),
            dscp: 0,
            transport_protocol,
        }
    }
}

impl From<(u32, SocketAddrV6, SocketAddrV6, u8)> for IpTransportConnectionId {
    #[inline(always)]
    fn from(
        (fib_index, local, remote, transport_protocol): (u32, SocketAddrV6, SocketAddrV6, u8),
    ) -> Self {
        Self::Ip6 {
            remote_address: *remote.ip(),
            local_address: *local.ip(),
            fib_index,
            remote_port: remote.port().to_be(),
            local_port: local.port().to_be(),
            dscp: 0,
            transport_protocol,
        }
    }
}

impl IpTransportConnectionId {
    #[inline(always)]
    pub const fn fib_index(self) -> u32 {
        match self {
            Self::Ip4 { fib_index, .. } | Self::Ip6 { fib_index, .. } => fib_index,
        }
    }
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpHalfOpenHandle(u64);

impl IpHalfOpenHandle {
    pub const INVALID: Self = Self(u64::MAX);

    #[inline(always)]
    pub const fn new(handle: u64) -> Self {
        Self(handle)
    }

    #[inline(always)]
    pub const fn value(self) -> u64 {
        self.0
    }
}

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use hammer_infra::bihash::BihashKey;

#[derive(Clone, Copy)]
#[repr(C, packed)]
pub struct UdpHeader {
    source_port: [u8; 2],
    destination_port: [u8; 2],
    length: [u8; 2],
    checksum: [u8; 2],
}

impl UdpHeader {
    #[inline(always)]
    pub fn source_port(self) -> u16 {
        u16::from_be_bytes(self.source_port)
    }

    #[inline(always)]
    pub fn destination_port(self) -> u16 {
        u16::from_be_bytes(self.destination_port)
    }

    #[inline(always)]
    pub fn length(self) -> usize {
        usize::from(u16::from_be_bytes(self.length))
    }

    #[inline(always)]
    pub fn checksum(self) -> u16 {
        u16::from_be_bytes(self.checksum)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransportConnectionKey<A = IpAddr> {
    scope_id: u32,
    local_addr: A,
    remote_addr: A,
    ports: u32,
}

impl<A: Copy> TransportConnectionKey<A> {
    #[inline]
    pub const fn new(
        scope_id: u32,
        local_addr: A,
        local_port: u16,
        remote_addr: A,
        remote_port: u16,
    ) -> Self {
        Self {
            scope_id,
            local_addr,
            remote_addr,
            ports: (local_port as u32) << 16 | remote_port as u32,
        }
    }

    #[inline]
    pub const fn scope_id(self) -> u32 {
        self.scope_id
    }

    #[inline]
    pub const fn local_addr(self) -> A {
        self.local_addr
    }

    #[inline]
    pub const fn local_port(self) -> u16 {
        (self.ports >> 16) as u16
    }

    #[inline]
    pub const fn remote_addr(self) -> A {
        self.remote_addr
    }

    #[inline]
    pub const fn remote_port(self) -> u16 {
        self.ports as u16
    }

    #[inline]
    pub const fn reverse(self) -> Self {
        Self::new(
            self.scope_id,
            self.remote_addr,
            self.remote_port(),
            self.local_addr,
            self.local_port(),
        )
    }
}

impl Default for TransportConnectionKey<Ipv4Addr> {
    #[inline]
    fn default() -> Self {
        Self {
            scope_id: 0,
            local_addr: Ipv4Addr::UNSPECIFIED,
            remote_addr: Ipv4Addr::UNSPECIFIED,
            ports: 0,
        }
    }
}

impl Default for TransportConnectionKey<Ipv6Addr> {
    #[inline]
    fn default() -> Self {
        Self {
            scope_id: 0,
            local_addr: Ipv6Addr::UNSPECIFIED,
            remote_addr: Ipv6Addr::UNSPECIFIED,
            ports: 0,
        }
    }
}

impl Default for TransportConnectionKey<IpAddr> {
    #[inline]
    fn default() -> Self {
        Self {
            scope_id: 0,
            local_addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            remote_addr: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            ports: 0,
        }
    }
}

impl TransportConnectionKey<IpAddr> {
    #[inline]
    pub fn from_socket_addrs(scope_id: u32, local: SocketAddr, remote: SocketAddr) -> Option<Self> {
        match (local.ip(), remote.ip()) {
            (IpAddr::V4(local_addr), IpAddr::V4(remote_addr)) => Some(Self::new(
                scope_id,
                IpAddr::V4(local_addr),
                local.port(),
                IpAddr::V4(remote_addr),
                remote.port(),
            )),
            (IpAddr::V6(local_addr), IpAddr::V6(remote_addr)) => Some(Self::new(
                scope_id,
                IpAddr::V6(local_addr),
                local.port(),
                IpAddr::V6(remote_addr),
                remote.port(),
            )),
            _ => None,
        }
    }
}

impl BihashKey for TransportConnectionKey<Ipv4Addr> {
    #[inline(always)]
    fn hash(self) -> u64 {
        let packed = (u128::from(self.scope_id) << 96)
            | (u128::from(u32::from(self.local_addr)) << 64)
            | (u128::from(u32::from(self.remote_addr)) << 32)
            | u128::from(self.ports);
        splitmix64((packed ^ (packed >> 64)) as u64)
    }
}

impl BihashKey for TransportConnectionKey<Ipv6Addr> {
    #[inline(always)]
    fn hash(self) -> u64 {
        hash_words(&[
            fold_u128(u128::from(self.local_addr)),
            fold_u128(u128::from(self.remote_addr)),
            u64::from(self.scope_id),
            u64::from(self.ports),
        ])
    }
}

#[inline(always)]
fn fold_u128(value: u128) -> u64 {
    value as u64 ^ (value >> 64) as u64
}

#[inline(always)]
fn hash_words(words: &[u64]) -> u64 {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for word in words {
        state ^= splitmix64(*word ^ state);
        state = state.rotate_left(13);
    }
    splitmix64(state)
}

#[inline(always)]
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

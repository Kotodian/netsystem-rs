use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use crate::ds::FlatHashKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TcpState {
    Closed,
    Listen,
    SynSent,
    SynRcvd,
    Established,
    FinWait1,
    FinWait2,
    CloseWait,
    Closing,
    LastAck,
    TimeWait,
}

impl TcpState {
    pub const COUNT: usize = 11;

    #[inline]
    pub const fn index(self) -> usize {
        self as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TcpTimerKind {
    Connect,
    Retransmit,
    DelayedAck,
    Persist,
    KeepAlive,
    TimeWait,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct TcpTimerId(u64);

impl TcpTimerId {
    // Control plane assigns a fresh token on each arm so stale expiries can be ignored.
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TcpShutdownDirection {
    Read,
    Write,
    Both,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TcpCloseReason {
    LocalRequest,
    LocalShutdown,
    RemoteFin,
    RemoteReset,
    ConnectTimeout,
    RetransmitTimeout,
    KeepAliveTimeout,
    ProtocolError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct TcpSeq(u32);

impl TcpSeq {
    #[inline]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn raw(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn advance(self, by: u32) -> Self {
        Self(self.0.wrapping_add(by))
    }

    #[inline]
    pub const fn before(self, other: Self) -> bool {
        // TCP sequence ordering is only well-defined inside the RFC half-space.
        (self.0.wrapping_sub(other.0) as i32) < 0
    }

    #[inline]
    pub const fn after(self, other: Self) -> bool {
        (self.0.wrapping_sub(other.0) as i32) > 0
    }

    #[inline]
    pub const fn distance_to(self, other: Self) -> u32 {
        other.0.wrapping_sub(self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TcpCapabilities {
    pub max_segment_size: Option<u16>,
    pub window_scale: Option<u8>,
    pub sack: bool,
    pub timestamps: bool,
    pub ecn: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TcpNegotiatedOptions {
    pub send_max_segment_size: Option<u16>,
    pub receive_max_segment_size: Option<u16>,
    pub send_window_scale: Option<u8>,
    pub receive_window_scale: Option<u8>,
    pub sack: bool,
    pub timestamps: bool,
    pub ecn: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct TcpConnectionId(u64);

impl TcpConnectionId {
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct TcpListenerId(u64);

impl TcpListenerId {
    #[inline]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TcpV4ListenerKey(u128);

impl TcpV4ListenerKey {
    #[inline]
    pub fn new(scope_id: u32, local_addr: Ipv4Addr, local_port: u16) -> Self {
        Self(
            (u128::from(scope_id) << 48)
                | (u128::from(u32::from(local_addr)) << 16)
                | u128::from(local_port),
        )
    }

    #[inline]
    pub const fn scope_id(self) -> u32 {
        (self.0 >> 48) as u32
    }

    #[inline]
    pub fn local_addr(self) -> Ipv4Addr {
        Ipv4Addr::from((self.0 >> 16) as u32)
    }

    #[inline]
    pub const fn local_port(self) -> u16 {
        self.0 as u16
    }
}

impl FlatHashKey for TcpV4ListenerKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        self.0.hash_key()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TcpV4ConnectionKey(u128);

impl TcpV4ConnectionKey {
    #[inline]
    pub fn new(
        scope_id: u32,
        local_addr: Ipv4Addr,
        local_port: u16,
        remote_addr: Ipv4Addr,
        remote_port: u16,
    ) -> Self {
        Self(
            (u128::from(scope_id) << 96)
                | (u128::from(u32::from(local_addr)) << 64)
                | (u128::from(u32::from(remote_addr)) << 32)
                | (u128::from(local_port) << 16)
                | u128::from(remote_port),
        )
    }

    #[inline]
    pub const fn scope_id(self) -> u32 {
        (self.0 >> 96) as u32
    }

    #[inline]
    pub fn local_addr(self) -> Ipv4Addr {
        Ipv4Addr::from((self.0 >> 64) as u32)
    }

    #[inline]
    pub const fn local_port(self) -> u16 {
        (self.0 >> 16) as u16
    }

    #[inline]
    pub fn remote_addr(self) -> Ipv4Addr {
        Ipv4Addr::from((self.0 >> 32) as u32)
    }

    #[inline]
    pub const fn remote_port(self) -> u16 {
        self.0 as u16
    }

    #[inline]
    pub fn reverse(self) -> Self {
        Self::new(
            self.scope_id(),
            self.remote_addr(),
            self.remote_port(),
            self.local_addr(),
            self.local_port(),
        )
    }
}

impl FlatHashKey for TcpV4ConnectionKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        self.0.hash_key()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TcpV6ListenerKey {
    local_addr: u128,
    scope_port: u64,
}

impl TcpV6ListenerKey {
    #[inline]
    pub fn new(scope_id: u32, local_addr: Ipv6Addr, local_port: u16) -> Self {
        Self {
            local_addr: u128::from(local_addr),
            scope_port: (u64::from(scope_id) << 16) | u64::from(local_port),
        }
    }

    #[inline]
    pub const fn scope_id(self) -> u32 {
        (self.scope_port >> 16) as u32
    }

    #[inline]
    pub fn local_addr(self) -> Ipv6Addr {
        Ipv6Addr::from(self.local_addr)
    }

    #[inline]
    pub const fn local_port(self) -> u16 {
        self.scope_port as u16
    }
}

impl FlatHashKey for TcpV6ListenerKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        hash_words(&[fold_u128(self.local_addr), self.scope_port])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TcpV6ConnectionKey {
    local_addr: u128,
    remote_addr: u128,
    scope_ports: u64,
}

impl TcpV6ConnectionKey {
    #[inline]
    pub fn new(
        scope_id: u32,
        local_addr: Ipv6Addr,
        local_port: u16,
        remote_addr: Ipv6Addr,
        remote_port: u16,
    ) -> Self {
        Self {
            local_addr: u128::from(local_addr),
            remote_addr: u128::from(remote_addr),
            scope_ports: (u64::from(scope_id) << 32)
                | (u64::from(local_port) << 16)
                | u64::from(remote_port),
        }
    }

    #[inline]
    pub const fn scope_id(self) -> u32 {
        (self.scope_ports >> 32) as u32
    }

    #[inline]
    pub fn local_addr(self) -> Ipv6Addr {
        Ipv6Addr::from(self.local_addr)
    }

    #[inline]
    pub const fn local_port(self) -> u16 {
        (self.scope_ports >> 16) as u16
    }

    #[inline]
    pub fn remote_addr(self) -> Ipv6Addr {
        Ipv6Addr::from(self.remote_addr)
    }

    #[inline]
    pub const fn remote_port(self) -> u16 {
        self.scope_ports as u16
    }

    #[inline]
    pub fn reverse(self) -> Self {
        Self::new(
            self.scope_id(),
            self.remote_addr(),
            self.remote_port(),
            self.local_addr(),
            self.local_port(),
        )
    }
}

impl FlatHashKey for TcpV6ConnectionKey {
    #[inline(always)]
    fn hash_key(self) -> usize {
        hash_words(&[
            fold_u128(self.local_addr),
            fold_u128(self.remote_addr),
            self.scope_ports,
        ])
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TcpListenerKey {
    V4(TcpV4ListenerKey),
    V6(TcpV6ListenerKey),
}

impl TcpListenerKey {
    #[inline]
    pub fn v4(scope_id: u32, local_addr: Ipv4Addr, local_port: u16) -> Self {
        Self::V4(TcpV4ListenerKey::new(scope_id, local_addr, local_port))
    }

    #[inline]
    pub fn v6(scope_id: u32, local_addr: Ipv6Addr, local_port: u16) -> Self {
        Self::V6(TcpV6ListenerKey::new(scope_id, local_addr, local_port))
    }

    #[inline]
    pub const fn scope_id(self) -> u32 {
        match self {
            Self::V4(key) => key.scope_id(),
            Self::V6(key) => key.scope_id(),
        }
    }

    #[inline]
    pub fn local_addr(self) -> IpAddr {
        match self {
            Self::V4(key) => IpAddr::V4(key.local_addr()),
            Self::V6(key) => IpAddr::V6(key.local_addr()),
        }
    }

    #[inline]
    pub const fn local_port(self) -> u16 {
        match self {
            Self::V4(key) => key.local_port(),
            Self::V6(key) => key.local_port(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TcpConnectionKey {
    V4(TcpV4ConnectionKey),
    V6(TcpV6ConnectionKey),
}

impl TcpConnectionKey {
    #[inline]
    pub fn v4(
        scope_id: u32,
        local_addr: Ipv4Addr,
        local_port: u16,
        remote_addr: Ipv4Addr,
        remote_port: u16,
    ) -> Self {
        Self::V4(TcpV4ConnectionKey::new(
            scope_id,
            local_addr,
            local_port,
            remote_addr,
            remote_port,
        ))
    }

    #[inline]
    pub fn v6(
        scope_id: u32,
        local_addr: Ipv6Addr,
        local_port: u16,
        remote_addr: Ipv6Addr,
        remote_port: u16,
    ) -> Self {
        Self::V6(TcpV6ConnectionKey::new(
            scope_id,
            local_addr,
            local_port,
            remote_addr,
            remote_port,
        ))
    }

    #[inline]
    pub const fn scope_id(self) -> u32 {
        match self {
            Self::V4(key) => key.scope_id(),
            Self::V6(key) => key.scope_id(),
        }
    }

    #[inline]
    pub fn local_addr(self) -> IpAddr {
        match self {
            Self::V4(key) => IpAddr::V4(key.local_addr()),
            Self::V6(key) => IpAddr::V6(key.local_addr()),
        }
    }

    #[inline]
    pub const fn local_port(self) -> u16 {
        match self {
            Self::V4(key) => key.local_port(),
            Self::V6(key) => key.local_port(),
        }
    }

    #[inline]
    pub fn remote_addr(self) -> IpAddr {
        match self {
            Self::V4(key) => IpAddr::V4(key.remote_addr()),
            Self::V6(key) => IpAddr::V6(key.remote_addr()),
        }
    }

    #[inline]
    pub const fn remote_port(self) -> u16 {
        match self {
            Self::V4(key) => key.remote_port(),
            Self::V6(key) => key.remote_port(),
        }
    }

    #[inline]
    pub fn reverse(self) -> Self {
        match self {
            Self::V4(key) => Self::V4(key.reverse()),
            Self::V6(key) => Self::V6(key.reverse()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpControlPlaneAction {
    // These actions intentionally stop at shared state/timer contracts.
    InstallListener {
        listener_id: TcpListenerId,
        listener: TcpListenerKey,
        capabilities: TcpCapabilities,
    },
    RemoveListener {
        listener_id: TcpListenerId,
        reason: TcpCloseReason,
    },
    InstallConnection {
        connection_id: TcpConnectionId,
        key: TcpConnectionKey,
        state: TcpState,
        capabilities: TcpCapabilities,
        negotiated: TcpNegotiatedOptions,
    },
    UpsertConnectionState {
        connection_id: TcpConnectionId,
        key: TcpConnectionKey,
        state: TcpState,
        capabilities: TcpCapabilities,
        negotiated: TcpNegotiatedOptions,
    },
    TransitionConnection {
        connection_id: TcpConnectionId,
        state: TcpState,
    },
    ShutdownConnection {
        connection_id: TcpConnectionId,
        direction: TcpShutdownDirection,
        reason: TcpCloseReason,
    },
    CloseConnection {
        connection_id: TcpConnectionId,
        reason: TcpCloseReason,
    },
    ArmTimer {
        connection_id: TcpConnectionId,
        timer_id: TcpTimerId,
        kind: TcpTimerKind,
        timeout: Duration,
    },
    CancelTimer {
        connection_id: TcpConnectionId,
        kind: TcpTimerKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpWorkerEvent {
    IncomingConnection {
        listener_id: TcpListenerId,
        listener: TcpListenerKey,
        key: TcpConnectionKey,
        capabilities: TcpCapabilities,
    },
    StateChanged {
        connection_id: TcpConnectionId,
        key: TcpConnectionKey,
        state: TcpState,
    },
    TimerExpired {
        connection_id: TcpConnectionId,
        timer_id: TcpTimerId,
        kind: TcpTimerKind,
    },
    ShutdownObserved {
        connection_id: TcpConnectionId,
        direction: TcpShutdownDirection,
        reason: TcpCloseReason,
    },
    Closed {
        connection_id: TcpConnectionId,
        reason: TcpCloseReason,
    },
}

#[inline(always)]
fn fold_u128(value: u128) -> u64 {
    value as u64 ^ (value >> 64) as u64
}

#[inline(always)]
fn hash_words(words: &[u64]) -> usize {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for word in words {
        state ^= splitmix64(*word ^ state);
        state = state.rotate_left(13);
    }
    splitmix64(state) as usize
}

#[inline(always)]
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

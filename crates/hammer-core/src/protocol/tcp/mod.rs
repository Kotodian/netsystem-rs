use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::ds::FlatHashKey;
use crate::protocol::ip_ecn::IpEcnCodepoint;
use crate::protocol::transport::TransportConnectionKey;
use thiserror::Error;

pub mod options;
pub mod reset;
pub mod segment;

pub use options::{
    ParsedTcpOptions, TcpSackBlock, TcpTimestampOption, tcp_capabilities_from_options,
    tcp_options_from_bytes,
};
pub use segment::{TcpSegmentHeader, TcpWireHeader, tcp_header};

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum TcpError {
    #[error("wrong worker thread")]
    WrongThread,
    #[error("inconsistent ip/tcp lengths")]
    Length,
    #[error("no listener for dst port")]
    NoListener,
    #[error("lookup drops")]
    LookupDrops,
    #[error("dispatch error")]
    Dispatch,
    #[error("invalid segments")]
    SegmentInvalid,
    #[error("invalid ACK")]
    AckInvalid,
    #[error("invalid connection")]
    InvalidConnection,
    #[error("connection closed")]
    ConnectionClosed,
    #[error("could not parse options")]
    Options,
    #[error("PAWS check failed")]
    Paws,
    #[error("segment not in receive window")]
    RcvWnd,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum TcpControlPacketParseError {
    #[error("tcp control packet is empty")]
    EmptyPacket,
    #[error("tcp control packet uses unsupported IP version")]
    UnsupportedIpVersion,
    #[error("tcp control packet is too short")]
    PacketTooShort,
    #[error("tcp control packet cursor is invalid")]
    InvalidCursor,
    #[error("tcp control header length is invalid")]
    InvalidHeaderLength,
}

impl From<TcpControlPacketParseError> for TcpError {
    #[inline]
    fn from(error: TcpControlPacketParseError) -> Self {
        match error {
            TcpControlPacketParseError::EmptyPacket
            | TcpControlPacketParseError::PacketTooShort
            | TcpControlPacketParseError::InvalidCursor
            | TcpControlPacketParseError::InvalidHeaderLength => TcpError::Length,
            TcpControlPacketParseError::UnsupportedIpVersion => TcpError::SegmentInvalid,
        }
    }
}

pub const TCP_FLAG_FIN: u8 = 0x01;
pub const TCP_FLAG_SYN: u8 = 0x02;
pub const TCP_FLAG_RST: u8 = 0x04;
pub const TCP_FLAG_PSH: u8 = 0x08;
pub const TCP_FLAG_ACK: u8 = 0x10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct TcpControlFlags(pub u8);

impl TcpControlFlags {
    #[inline]
    pub const fn from_bits(bits: u8) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn bits(self) -> u8 {
        self.0
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TcpInputFlags: u8 {
        const FIN = 0x01;
        const SYN = 0x02;
        const RST = 0x04;
        const ACK = 0x10;
    }
}

#[derive(Debug, Clone)]
pub struct TcpPacket {
    pub local: std::net::SocketAddr,
    pub remote: std::net::SocketAddr,
    pub sequence: TcpSeq,
    pub acknowledgment: Option<TcpSeq>,
    pub advertised_window: u16,
    pub flags: TcpSegmentFlags,
    pub capabilities: TcpCapabilities,
    pub sack_blocks: hammer_infra::vec::Vec<TcpSackBlock>,
    pub timestamp: Option<TcpTimestampOption>,
    pub fast_open_cookie: Option<TcpFastOpenCookie>,
    pub ip_ecn: Option<IpEcnCodepoint>,
    pub payload_offset: usize,
    pub payload_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TcpFastOpenCookie {
    bytes: [u8; Self::MAX_LEN],
    len: u8,
}

impl TcpFastOpenCookie {
    pub const MIN_LEN: usize = 4;
    pub const MAX_LEN: usize = 16;

    #[inline]
    pub const fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len()]
    }

    #[inline]
    pub fn epoch(&self) -> Option<u32> {
        (self.len() == Self::MAX_LEN && self.bytes[0] == 1).then(|| {
            u32::from_be_bytes([self.bytes[1], self.bytes[2], self.bytes[3], self.bytes[4]])
        })
    }

    #[inline]
    pub fn constant_time_eq(&self, other: &Self) -> bool {
        let lhs = self.as_slice();
        let rhs = other.as_slice();
        if lhs.len() != rhs.len() {
            return false;
        }
        let mut diff = 0u8;
        let mut index = 0usize;
        while index < lhs.len() {
            diff |= lhs[index] ^ rhs[index];
            index += 1;
        }
        diff == 0
    }

    #[inline]
    pub const fn is_valid_len(len: usize) -> bool {
        len >= Self::MIN_LEN && len <= Self::MAX_LEN && len % 2 == 0
    }
}

impl AsRef<[u8]> for TcpFastOpenCookie {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::ops::Deref for TcpFastOpenCookie {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl TryFrom<&[u8]> for TcpFastOpenCookie {
    type Error = ();

    #[inline]
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if !Self::is_valid_len(value.len()) {
            return Err(());
        }
        let mut cookie = Self {
            bytes: [0; Self::MAX_LEN],
            len: value.len() as u8,
        };
        let mut index = 0usize;
        while index < value.len() {
            cookie.bytes[index] = value[index];
            index += 1;
        }
        Ok(cookie)
    }
}

impl From<[u8; TcpFastOpenCookie::MAX_LEN]> for TcpFastOpenCookie {
    #[inline]
    fn from(bytes: [u8; TcpFastOpenCookie::MAX_LEN]) -> Self {
        Self {
            bytes,
            len: TcpFastOpenCookie::MAX_LEN as u8,
        }
    }
}

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TcpSegmentFlags: u16 {
        const FIN = 0x01;
        const SYN = 0x02;
        const RST = 0x04;
        const PSH = 0x08;
        const ACK = 0x10;
        const URG = 0x20;
        const ECE = 0x40;
        const CWR = 0x80;
        const NS = 0x100;
    }
}

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

impl TcpTimerKind {
    #[inline]
    pub const fn close_reason_on_expiry(self) -> Option<TcpCloseReason> {
        match self {
            Self::Connect => Some(TcpCloseReason::ConnectTimeout),
            Self::Retransmit => Some(TcpCloseReason::RetransmitTimeout),
            Self::KeepAlive => Some(TcpCloseReason::KeepAliveTimeout),
            Self::DelayedAck | Self::Persist | Self::TimeWait => None,
        }
    }
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
    pub const fn distance_to(self, other: Self) -> u32 {
        other.0.wrapping_sub(self.0)
    }
}

impl From<u32> for TcpSeq {
    #[inline]
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl From<TcpSeq> for u32 {
    #[inline]
    fn from(value: TcpSeq) -> Self {
        value.0
    }
}

impl Ord for TcpSeq {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        if self == other {
            Ordering::Equal
        // TCP sequence ordering is only well-defined inside the RFC half-space.
        } else if (self.0.wrapping_sub(other.0) as i32) < 0 {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

impl PartialOrd for TcpSeq {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TcpCapabilities {
    pub max_segment_size: Option<u16>,
    pub window_scale: Option<u8>,
    pub sack: bool,
    pub timestamps: bool,
    pub ecn: bool,
    pub accurate_ecn: bool,
    pub fast_open: bool,
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
    pub accurate_ecn: bool,
    pub fast_open: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpControlPlaneAction {
    // The shared TCP control plane only owns listener registrations.
    InstallListener {
        listener_id: TcpListenerId,
        listener: TcpListenerKey,
        capabilities: TcpCapabilities,
    },
    RemoveListener {
        listener_id: TcpListenerId,
        reason: TcpCloseReason,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpWorkerEvent {
    IncomingConnection {
        listener_id: TcpListenerId,
        listener: TcpListenerKey,
        key: TransportConnectionKey<IpAddr>,
        capabilities: TcpCapabilities,
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
use std::cmp::Ordering;

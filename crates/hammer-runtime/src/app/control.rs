//! Application Session control messages and their fixed VPP-shaped wire
//! representation over the Session Message Queue control slot.
//!
//! Public payloads retain typed Rust errors; the private fixed-layout wire
//! structs use stable numeric retvals with private conversions. No aggregate
//! wire enum, no envelope, no heap allocation in the encode/decode path.

use std::mem::size_of;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use thiserror::Error;

use crate::{DataWorkerId, SessionListenEndpoint};

use super::session_msg_queue::SESSION_CTRL_MSG_MAX_SIZE;
use super::{SessionConnectError, SessionEvtType, SessionHandle};

bitflags::bitflags! {
    /// VPP Session flags carried by concrete control payloads.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    #[repr(transparent)]
    pub struct SessionFlags: u16 {
        const STREAM = 0x0001;
        const UNIDIRECTIONAL = 0x0002;
    }
}

/// Wire-level errors for concrete non-connect Session control messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SessionControlError {
    #[error("the Session control request is invalid")]
    InvalidRequest,
    #[error("the Application is missing")]
    ApplicationMissing,
    #[error("Application control must run on the main thread")]
    ApplicationControlWrongThread,
    #[error("the Session listener is missing")]
    ListenerMissing,
    #[error("the Session listener is owned by another Application")]
    ListenerNotOwned,
    #[error("the Session listener capacity is exhausted")]
    ListenerCapacityExhausted,
    #[error("the Session transport is missing")]
    TransportMissing,
    #[error("the Session transport is registered more than once")]
    TransportDuplicate,
    #[error("the Session transport operation is unsupported")]
    TransportUnsupported,
    #[error("the Session transport listen operation is unsupported")]
    TransportListenUnsupported,
    #[error("the Session transport connect operation is unsupported")]
    TransportConnectUnsupported,
    #[error("the Session transport operation failed")]
    TransportFailed,
    #[error("the Session resource capacity is exhausted")]
    CapacityExhausted,
    #[error("no bounded ext-config storage is published for the Application")]
    ExtConfigUnavailable,
    #[error("the ext-config chunk payload is not a valid server name")]
    ExtConfigInvalid,
    #[error("the ext-config chunk could not be read or freed")]
    ExtConfigFailed,
    #[error("CONNECT_STREAM requires a parent Session handle")]
    ConnectStreamParentMissing,
    #[error("CONNECT_STREAM arrived on the wrong Data Worker")]
    ConnectStreamWrongWorker,
    #[error("Session has no data workers configured")]
    NoDataWorkers,
    #[error("the Session connection is missing")]
    ConnectionMissing,
    #[error("the Session connection is owned by another Application")]
    ConnectionNotOwned,
}

impl SessionControlError {
    /// Stable private wire retval (VPP `i32 retval`; 0 means success).
    const fn retval(self) -> i32 {
        match self {
            Self::InvalidRequest => -1,
            Self::ApplicationMissing => -2,
            Self::ApplicationControlWrongThread => -3,
            Self::ListenerMissing => -4,
            Self::ListenerNotOwned => -5,
            Self::ListenerCapacityExhausted => -6,
            Self::TransportMissing => -7,
            Self::TransportDuplicate => -8,
            Self::TransportUnsupported => -9,
            Self::TransportListenUnsupported => -10,
            Self::TransportConnectUnsupported => -11,
            Self::TransportFailed => -12,
            Self::CapacityExhausted => -13,
            Self::ExtConfigUnavailable => -17,
            Self::ExtConfigInvalid => -18,
            Self::ExtConfigFailed => -19,
            Self::ConnectStreamParentMissing => -20,
            Self::ConnectStreamWrongWorker => -21,
            Self::NoDataWorkers => -22,
            Self::ConnectionMissing => -23,
            Self::ConnectionNotOwned => -24,
        }
    }

    fn from_retval(code: i32) -> Option<Self> {
        match code {
            -1 => Some(Self::InvalidRequest),
            -2 => Some(Self::ApplicationMissing),
            -3 => Some(Self::ApplicationControlWrongThread),
            -4 => Some(Self::ListenerMissing),
            -5 => Some(Self::ListenerNotOwned),
            -6 => Some(Self::ListenerCapacityExhausted),
            -7 => Some(Self::TransportMissing),
            -8 => Some(Self::TransportDuplicate),
            -9 => Some(Self::TransportUnsupported),
            -10 => Some(Self::TransportListenUnsupported),
            -11 => Some(Self::TransportConnectUnsupported),
            -12 => Some(Self::TransportFailed),
            -13 => Some(Self::CapacityExhausted),
            -17 => Some(Self::ExtConfigUnavailable),
            -18 => Some(Self::ExtConfigInvalid),
            -19 => Some(Self::ExtConfigFailed),
            -20 => Some(Self::ConnectStreamParentMissing),
            -21 => Some(Self::ConnectStreamWrongWorker),
            -22 => Some(Self::NoDataWorkers),
            -23 => Some(Self::ConnectionMissing),
            -24 => Some(Self::ConnectionNotOwned),
            _ => None,
        }
    }
}

/// Stable private wire retvals for [`SessionConnectError`].
///
/// The magnitude's high byte is a stable variant tag (no payload bits);
/// payload-bearing variants carry their full value in the reply's handle
/// field, which VPP leaves unused on error (`session_connected_msg_t.retval`
/// plus `handle`, application_interface.h:410-424). `SessionControlError`
/// codes are reused as-is.
fn session_connect_error_retval(error: SessionConnectError) -> i32 {
    match error {
        SessionConnectError::TlsAlert { .. } => -(0x01 << 16),
        SessionConnectError::QuicTransportError { .. } => -(0x02 << 16),
        SessionConnectError::PeerClosed { .. } => -(0x03 << 16),
        SessionConnectError::QuicVersionUnsupported => -(0x04 << 16),
        SessionConnectError::TimedOut => -(0x05 << 16),
        SessionConnectError::ConnectionRefused => -(0x06 << 16),
        SessionConnectError::ConnectionReset => -(0x07 << 16),
        SessionConnectError::LocalClosed => -(0x08 << 16),
        SessionConnectError::LocalResourceExhausted => -(0x09 << 16),
        SessionConnectError::Control { error } => error.retval(),
    }
}

/// Full error payload for a payload-bearing [`SessionConnectError`], carried
/// in the wire handle field (0 for variants without a payload).
fn session_connect_error_code(error: SessionConnectError) -> u64 {
    match error {
        SessionConnectError::TlsAlert { alert } => alert as u64,
        SessionConnectError::QuicTransportError { code } => code,
        SessionConnectError::PeerClosed { code } => code,
        SessionConnectError::QuicVersionUnsupported
        | SessionConnectError::TimedOut
        | SessionConnectError::ConnectionRefused
        | SessionConnectError::ConnectionReset
        | SessionConnectError::LocalClosed
        | SessionConnectError::LocalResourceExhausted
        | SessionConnectError::Control { .. } => 0,
    }
}

fn session_connect_error_from_retval(code: i32, payload: u64) -> Option<SessionConnectError> {
    if code >= 0 {
        return None;
    }
    let raw = -code as u32;
    if raw < 0x0100 {
        return SessionControlError::from_retval(code)
            .map(|error| SessionConnectError::Control { error });
    }
    match raw >> 16 {
        0x01 => Some(SessionConnectError::TlsAlert {
            alert: payload as u8,
        }),
        0x02 => Some(SessionConnectError::QuicTransportError { code: payload }),
        0x03 => Some(SessionConnectError::PeerClosed { code: payload }),
        0x04 => Some(SessionConnectError::QuicVersionUnsupported),
        0x05 => Some(SessionConnectError::TimedOut),
        0x06 => Some(SessionConnectError::ConnectionRefused),
        0x07 => Some(SessionConnectError::ConnectionReset),
        0x08 => Some(SessionConnectError::LocalClosed),
        0x09 => Some(SessionConnectError::LocalResourceExhausted),
        _ => None,
    }
}

/// Decoding failure for a concrete Session control payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SessionControlDecodeError {
    #[error(
        "Session control payload is truncated: {wire} wire bytes in a {available}-byte slot payload"
    )]
    Truncated { wire: usize, available: usize },
    #[error("Session control payload carries unknown error code {code}")]
    UnknownErrorCode { code: i32 },
}

/// VPP-shaped LISTEN control payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionListenMsg {
    pub context: u64,
    pub transport: u8,
    pub endpoint: SessionListenEndpoint,
    pub application: u32,
    pub app: Option<u32>,
    pub flags: SessionFlags,
    pub opaque: Option<u64>,
}

/// VPP-shaped BOUND control payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionBoundMsg {
    pub context: u64,
    pub result: Result<SessionHandle, SessionControlError>,
    pub local: Option<SocketAddr>,
    pub opaque: Option<u64>,
}

/// VPP-shaped UNLISTEN control payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionUnlistenMsg {
    pub context: u64,
    pub listener: SessionHandle,
}

/// VPP-shaped UNLISTEN_REPLY control payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionUnlistenReplyMsg {
    pub context: u64,
    pub listener: SessionHandle,
    pub result: Result<(), SessionControlError>,
}

/// VPP-shaped CONNECTED control payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConnectedMsg {
    pub context: u64,
    pub result: Result<SessionHandle, SessionConnectError>,
    pub local: Option<SocketAddr>,
    pub remote: Option<SocketAddr>,
    pub flags: SessionFlags,
    pub opaque: Option<u64>,
}

impl SessionConnectedMsg {
    /// Empty CONNECTED message (endpoints, flags and opaque unset).
    pub const fn new(context: u64, result: Result<SessionHandle, SessionConnectError>) -> Self {
        Self {
            context,
            result,
            local: None,
            remote: None,
            flags: SessionFlags::empty(),
            opaque: None,
        }
    }
}

/// VPP-shaped ACCEPTED control payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionAcceptedMsg {
    pub context: u64,
    pub listener: SessionHandle,
    pub session: SessionHandle,
    pub flags: SessionFlags,
    pub local: Option<SocketAddr>,
    pub remote: Option<SocketAddr>,
    pub opaque: Option<u64>,
}

impl SessionAcceptedMsg {
    /// ACCEPTED message (endpoints and opaque unset).
    pub const fn new(
        context: u64,
        listener: SessionHandle,
        session: SessionHandle,
        flags: SessionFlags,
    ) -> Self {
        Self {
            context,
            listener,
            session,
            flags,
            local: None,
            remote: None,
            opaque: None,
        }
    }
}

/// VPP-shaped ACCEPTED_REPLY control payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionAcceptedReplyMsg {
    pub context: u64,
    pub session: SessionHandle,
    pub result: Result<(), SessionControlError>,
}

impl SessionAcceptedReplyMsg {
    /// ACCEPTED_REPLY acknowledgment from the Application.
    pub const fn new(
        context: u64,
        session: SessionHandle,
        result: Result<(), SessionControlError>,
    ) -> Self {
        Self {
            context,
            session,
            result,
        }
    }
}

/// VPP-shaped CONNECT/CONNECT_STREAM control payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConnectMsg {
    pub context: u64,
    pub transport: u8,
    pub remote: SocketAddr,
    pub local: Option<SocketAddr>,
    pub application: u32,
    pub app: Option<u32>,
    /// Parent Session for a stream open; `None` for an ordinary CONNECT.
    ///
    /// CONNECT_STREAM is parent-handle pinned (VPP
    /// `session_mq_connect_stream_handler`, session_node.c:327-332). The
    /// Session owns the data-worker choice, so no external worker identity is
    /// accepted on this message.
    pub parent_handle: Option<SessionHandle>,
    pub flags: SessionFlags,
    pub opaque: Option<u64>,
    /// Opaque bounded ext-config reference: the absolute offset of one
    /// allocated ext-config chunk in the shared Application segment, or
    /// `None`. The daemon reads, validates, and frees the chunk exactly once
    /// (VPP `session_connect_msg_t.ext_config`, application_interface.h; the
    /// wire encodes `None` as 0).
    pub ext_config: Option<u64>,
}

impl SessionConnectMsg {
    #[allow(clippy::too_many_arguments)]
    pub fn connect(
        context: u64,
        transport: u8,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        application: u32,
        opaque: Option<u64>,
    ) -> Self {
        Self {
            context,
            transport,
            remote,
            local,
            application,
            app: None,
            parent_handle: None,
            flags: SessionFlags::empty(),
            opaque,
            ext_config: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn connect_stream(
        context: u64,
        transport: u8,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        application: u32,
        parent_handle: SessionHandle,
        flags: SessionFlags,
        opaque: Option<u64>,
    ) -> Self {
        Self {
            context,
            transport,
            remote,
            local,
            application,
            app: None,
            parent_handle: Some(parent_handle),
            flags: flags | SessionFlags::STREAM,
            opaque,
            ext_config: None,
        }
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Concrete Session control message selected by a control-slot event type.
///
/// Sealed: only the concrete messages in this module implement it. It is the
/// static-dispatch seam for [`SessionMsgQueue::enqueue_control`] and
/// [`SessionControlItem::decode`]; the wire representation stays private.
pub trait SessionControlPayload: sealed::Sealed + Sized {
    /// Control-slot event type for this message value. CONNECT and
    /// CONNECT_STREAM share one payload shape; the event type is derived
    /// from the parent handle.
    fn event_type(&self) -> SessionEvtType;

    /// True when `event` selects this concrete message type.
    fn is_event_type(event: SessionEvtType) -> bool;

    /// Fixed wire payload size in bytes (≤ SESSION_CTRL_MSG_MAX_SIZE - 1).
    const WIRE_BYTES: usize;

    /// Writes the private fixed-layout wire payload into the slot payload.
    #[doc(hidden)]
    fn encode_wire(&self, payload: &mut [u8]);

    /// Decodes the private fixed-layout wire payload from the slot payload.
    #[doc(hidden)]
    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError>;
}

impl sealed::Sealed for SessionListenMsg {}
impl sealed::Sealed for SessionBoundMsg {}
impl sealed::Sealed for SessionUnlistenMsg {}
impl sealed::Sealed for SessionUnlistenReplyMsg {}
impl sealed::Sealed for SessionConnectMsg {}
impl sealed::Sealed for SessionConnectedMsg {}
impl sealed::Sealed for SessionAcceptedMsg {}
impl sealed::Sealed for SessionAcceptedReplyMsg {}

// ---------------------------------------------------------------------------
// Private fixed-layout wire structs (VPP `__clib_packed` shapes).
//
// All sizes are asserted against SESSION_CTRL_MSG_MAX_SIZE = 86 at compile
// time below; the slot carries one event-type byte plus the payload.
// ---------------------------------------------------------------------------

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct ListenWire {
    context: u64,
    transport_proto: u8,
    is_ip4: u8,
    ip: [u8; 16],
    port: u16,
    worker: u32,
    application: u32,
    app: u32,
    flags: u16,
    opaque: u64,
    /// VPP `session_listen_msg_t.ext_config` (uword offset; 0 = none).
    ext_config: u64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct BoundWire {
    context: u64,
    retval: i32,
    session_index: u32,
    thread_index: u32,
    local_is_ip4: u8,
    local_ip: [u8; 16],
    local_port: u16,
    opaque: u64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct UnlistenWire {
    context: u64,
    listener_session_index: u32,
    listener_thread_index: u32,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct UnlistenReplyWire {
    context: u64,
    listener_session_index: u32,
    listener_thread_index: u32,
    retval: i32,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct ConnectWire {
    context: u64,
    transport_proto: u8,
    remote_is_ip4: u8,
    remote_ip: [u8; 16],
    remote_port: u16,
    local_is_ip4: u8,
    local_ip: [u8; 16],
    local_port: u16,
    application: u32,
    app: u32,
    /// `u32::MAX` fields identify an absent stream parent.
    parent_session_index: u32,
    parent_thread_index: u32,
    flags: u16,
    opaque: u64,
    /// VPP `session_connect_msg_t.ext_config` (uword offset; 0 = none).
    ext_config: u64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct ConnectedWire {
    context: u64,
    retval: i32,
    session_index: u32,
    thread_index: u32,
    error_payload: u64,
    local_is_ip4: u8,
    local_ip: [u8; 16],
    local_port: u16,
    remote_is_ip4: u8,
    remote_ip: [u8; 16],
    remote_port: u16,
    flags: u16,
    opaque: u64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct AcceptedWire {
    context: u64,
    listener_session_index: u32,
    listener_thread_index: u32,
    session_index: u32,
    session_thread_index: u32,
    flags: u16,
    local_is_ip4: u8,
    local_ip: [u8; 16],
    local_port: u16,
    remote_is_ip4: u8,
    remote_ip: [u8; 16],
    remote_port: u16,
    opaque: u64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct AcceptedReplyWire {
    context: u64,
    session_index: u32,
    thread_index: u32,
    retval: i32,
}

const _: () = assert!(size_of::<ListenWire>() + 1 <= SESSION_CTRL_MSG_MAX_SIZE);
const _: () = assert!(size_of::<BoundWire>() + 1 <= SESSION_CTRL_MSG_MAX_SIZE);
const _: () = assert!(size_of::<UnlistenWire>() + 1 <= SESSION_CTRL_MSG_MAX_SIZE);
const _: () = assert!(size_of::<UnlistenReplyWire>() + 1 <= SESSION_CTRL_MSG_MAX_SIZE);
const _: () = assert!(size_of::<ConnectWire>() + 1 <= SESSION_CTRL_MSG_MAX_SIZE);
const _: () = assert!(size_of::<ConnectedWire>() + 1 <= SESSION_CTRL_MSG_MAX_SIZE);
const _: () = assert!(size_of::<AcceptedWire>() + 1 <= SESSION_CTRL_MSG_MAX_SIZE);
const _: () = assert!(size_of::<AcceptedReplyWire>() + 1 <= SESSION_CTRL_MSG_MAX_SIZE);

fn write_wire<W: Copy>(wire: W, payload: &mut [u8]) {
    // SAFETY: `W` is a #[repr(C, packed)] wire struct with no padding; its
    // byte view has exactly size_of::<W>() bytes, and the compile-time size
    // assertions above keep every wire within the slot payload.
    let bytes =
        unsafe { std::slice::from_raw_parts((&wire as *const W).cast::<u8>(), size_of::<W>()) };
    payload[..bytes.len()].copy_from_slice(bytes);
}

fn read_wire<W: Copy>(payload: &[u8]) -> Result<W, SessionControlDecodeError> {
    let bytes = payload
        .get(..size_of::<W>())
        .ok_or(SessionControlDecodeError::Truncated {
            wire: size_of::<W>(),
            available: payload.len(),
        })?;
    // SAFETY: `bytes` has exactly size_of::<W>() bytes and `W` is a packed
    // wire struct (alignment 1), so unaligned reads are well-defined.
    Ok(unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<W>()) })
}

fn encode_endpoint(addr: SocketAddr) -> (u8, [u8; 16], u16) {
    match addr {
        SocketAddr::V4(v4) => {
            let mut ip = [0_u8; 16];
            ip[..4].copy_from_slice(&v4.ip().octets());
            (1, ip, v4.port())
        }
        SocketAddr::V6(v6) => (0, v6.ip().octets(), v6.port()),
    }
}

/// `None` is the VPP zero-endpoint sentinel (SESSION_IP46_ZERO).
fn encode_optional_endpoint(addr: Option<SocketAddr>) -> (u8, [u8; 16], u16) {
    match addr {
        Some(addr) => encode_endpoint(addr),
        None => (0, [0_u8; 16], 0),
    }
}

fn decode_endpoint(is_ip4: u8, ip: [u8; 16], port: u16) -> SocketAddr {
    if is_ip4 == 1 {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(ip[0], ip[1], ip[2], ip[3])), port)
    } else {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), port)
    }
}

fn decode_optional_endpoint(is_ip4: u8, ip: [u8; 16], port: u16) -> Option<SocketAddr> {
    if is_ip4 == 0 && port == 0 && ip == [0_u8; 16] {
        None
    } else {
        Some(decode_endpoint(is_ip4, ip, port))
    }
}

impl SessionControlPayload for SessionListenMsg {
    fn event_type(&self) -> SessionEvtType {
        SessionEvtType::Listen
    }

    fn is_event_type(event: SessionEvtType) -> bool {
        event == SessionEvtType::Listen
    }

    const WIRE_BYTES: usize = size_of::<ListenWire>();

    fn encode_wire(&self, payload: &mut [u8]) {
        let (is_ip4, ip, port) = encode_endpoint(self.endpoint.local());
        write_wire(
            ListenWire {
                context: self.context,
                transport_proto: self.transport as u8,
                is_ip4,
                ip,
                port,
                worker: self.endpoint.worker().slot() as u32,
                application: self.application,
                app: self.app.map_or(u32::MAX, |app| app),
                flags: self.flags.bits(),
                opaque: self.opaque.unwrap_or(u64::MAX),
                ext_config: 0,
            },
            payload,
        );
    }

    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError> {
        let wire = read_wire::<ListenWire>(payload)?;
        let transport = wire.transport_proto;
        let endpoint = SessionListenEndpoint::new(
            decode_endpoint(wire.is_ip4, wire.ip, wire.port),
            DataWorkerId::new(wire.worker),
        );
        Ok(Self {
            context: wire.context,
            transport,
            endpoint,
            application: wire.application,
            app: (wire.app != u32::MAX).then(|| wire.app),
            flags: SessionFlags::from_bits_retain(wire.flags),
            opaque: (wire.opaque != u64::MAX).then_some(wire.opaque),
        })
    }
}

impl SessionControlPayload for SessionBoundMsg {
    fn event_type(&self) -> SessionEvtType {
        SessionEvtType::Bound
    }

    fn is_event_type(event: SessionEvtType) -> bool {
        event == SessionEvtType::Bound
    }

    const WIRE_BYTES: usize = size_of::<BoundWire>();

    fn encode_wire(&self, payload: &mut [u8]) {
        let (retval, session_index, thread_index) = match self.result {
            Ok(handle) => (0, handle.session_index, handle.thread_index),
            Err(error) => (error.retval(), 0, 0),
        };
        let (local_is_ip4, local_ip, local_port) = encode_optional_endpoint(self.local);
        write_wire(
            BoundWire {
                context: self.context,
                retval,
                session_index,
                thread_index,
                local_is_ip4,
                local_ip,
                local_port,
                opaque: self.opaque.unwrap_or(u64::MAX),
            },
            payload,
        );
    }

    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError> {
        let wire = read_wire::<BoundWire>(payload)?;
        let result = if wire.retval == 0 {
            Ok(SessionHandle::new(wire.session_index, wire.thread_index))
        } else {
            Err(SessionControlError::from_retval(wire.retval)
                .ok_or(SessionControlDecodeError::UnknownErrorCode { code: wire.retval })?)
        };
        Ok(Self {
            context: wire.context,
            result,
            local: decode_optional_endpoint(wire.local_is_ip4, wire.local_ip, wire.local_port),
            opaque: (wire.opaque != u64::MAX).then_some(wire.opaque),
        })
    }
}

impl SessionControlPayload for SessionUnlistenMsg {
    fn event_type(&self) -> SessionEvtType {
        SessionEvtType::Unlisten
    }

    fn is_event_type(event: SessionEvtType) -> bool {
        event == SessionEvtType::Unlisten
    }

    const WIRE_BYTES: usize = size_of::<UnlistenWire>();

    fn encode_wire(&self, payload: &mut [u8]) {
        write_wire(
            UnlistenWire {
                context: self.context,
                listener_session_index: self.listener.session_index,
                listener_thread_index: self.listener.thread_index,
            },
            payload,
        );
    }

    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError> {
        let wire = read_wire::<UnlistenWire>(payload)?;
        Ok(Self {
            context: wire.context,
            listener: SessionHandle::new(wire.listener_session_index, wire.listener_thread_index),
        })
    }
}

impl SessionControlPayload for SessionUnlistenReplyMsg {
    fn event_type(&self) -> SessionEvtType {
        SessionEvtType::UnlistenReply
    }

    fn is_event_type(event: SessionEvtType) -> bool {
        event == SessionEvtType::UnlistenReply
    }

    const WIRE_BYTES: usize = size_of::<UnlistenReplyWire>();

    fn encode_wire(&self, payload: &mut [u8]) {
        let retval = match self.result {
            Ok(()) => 0,
            Err(error) => error.retval(),
        };
        write_wire(
            UnlistenReplyWire {
                context: self.context,
                listener_session_index: self.listener.session_index,
                listener_thread_index: self.listener.thread_index,
                retval,
            },
            payload,
        );
    }

    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError> {
        let wire = read_wire::<UnlistenReplyWire>(payload)?;
        let result = if wire.retval == 0 {
            Ok(())
        } else {
            Err(SessionControlError::from_retval(wire.retval)
                .ok_or(SessionControlDecodeError::UnknownErrorCode { code: wire.retval })?)
        };
        Ok(Self {
            context: wire.context,
            listener: SessionHandle::new(wire.listener_session_index, wire.listener_thread_index),
            result,
        })
    }
}

impl SessionControlPayload for SessionConnectMsg {
    fn event_type(&self) -> SessionEvtType {
        if self.parent_handle.is_some() {
            SessionEvtType::ConnectStream
        } else {
            SessionEvtType::Connect
        }
    }

    fn is_event_type(event: SessionEvtType) -> bool {
        matches!(
            event,
            SessionEvtType::Connect | SessionEvtType::ConnectStream
        )
    }

    const WIRE_BYTES: usize = size_of::<ConnectWire>();

    fn encode_wire(&self, payload: &mut [u8]) {
        let (remote_is_ip4, remote_ip, remote_port) = encode_endpoint(self.remote);
        let (local_is_ip4, local_ip, local_port) = encode_optional_endpoint(self.local);
        write_wire(
            ConnectWire {
                context: self.context,
                transport_proto: self.transport as u8,
                remote_is_ip4,
                remote_ip,
                remote_port,
                local_is_ip4,
                local_ip,
                local_port,
                application: self.application,
                app: self.app.map_or(u32::MAX, |app| app),
                parent_session_index: self
                    .parent_handle
                    .map_or(u32::MAX, |handle| handle.session_index),
                parent_thread_index: self
                    .parent_handle
                    .map_or(u32::MAX, |handle| handle.thread_index),
                flags: self.flags.bits(),
                opaque: self.opaque.unwrap_or(u64::MAX),
                ext_config: self.ext_config.unwrap_or(0),
            },
            payload,
        );
    }

    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError> {
        let wire = read_wire::<ConnectWire>(payload)?;
        let transport = wire.transport_proto;
        Ok(Self {
            context: wire.context,
            transport,
            remote: decode_endpoint(wire.remote_is_ip4, wire.remote_ip, wire.remote_port),
            local: decode_optional_endpoint(wire.local_is_ip4, wire.local_ip, wire.local_port),
            application: wire.application,
            app: (wire.app != u32::MAX).then(|| wire.app),
            parent_handle: (wire.parent_session_index != u32::MAX
                && wire.parent_thread_index != u32::MAX)
                .then_some(SessionHandle::new(
                    wire.parent_session_index,
                    wire.parent_thread_index,
                )),
            flags: SessionFlags::from_bits_retain(wire.flags),
            opaque: (wire.opaque != u64::MAX).then_some(wire.opaque),
            ext_config: (wire.ext_config != 0).then_some(wire.ext_config),
        })
    }
}

impl SessionControlPayload for SessionConnectedMsg {
    fn event_type(&self) -> SessionEvtType {
        SessionEvtType::Connected
    }

    fn is_event_type(event: SessionEvtType) -> bool {
        event == SessionEvtType::Connected
    }

    const WIRE_BYTES: usize = size_of::<ConnectedWire>();

    fn encode_wire(&self, payload: &mut [u8]) {
        // On error the handle field carries the full error payload (unused in
        // VPP's connected reply when `retval < 0`).
        let (retval, session_index, thread_index, error_payload) = match self.result {
            Ok(handle) => (0, handle.session_index, handle.thread_index, 0),
            Err(error) => (
                session_connect_error_retval(error),
                0,
                0,
                session_connect_error_code(error),
            ),
        };
        let (local_is_ip4, local_ip, local_port) = encode_optional_endpoint(self.local);
        let (remote_is_ip4, remote_ip, remote_port) = encode_optional_endpoint(self.remote);
        write_wire(
            ConnectedWire {
                context: self.context,
                retval,
                session_index,
                thread_index,
                error_payload,
                local_is_ip4,
                local_ip,
                local_port,
                remote_is_ip4,
                remote_ip,
                remote_port,
                flags: self.flags.bits(),
                opaque: self.opaque.unwrap_or(u64::MAX),
            },
            payload,
        );
    }

    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError> {
        let wire = read_wire::<ConnectedWire>(payload)?;
        let result = if wire.retval == 0 {
            Ok(SessionHandle::new(wire.session_index, wire.thread_index))
        } else {
            Err(
                session_connect_error_from_retval(wire.retval, wire.error_payload)
                    .ok_or(SessionControlDecodeError::UnknownErrorCode { code: wire.retval })?,
            )
        };
        Ok(Self {
            context: wire.context,
            result,
            local: decode_optional_endpoint(wire.local_is_ip4, wire.local_ip, wire.local_port),
            remote: decode_optional_endpoint(wire.remote_is_ip4, wire.remote_ip, wire.remote_port),
            flags: SessionFlags::from_bits_retain(wire.flags),
            opaque: (wire.opaque != u64::MAX).then_some(wire.opaque),
        })
    }
}

impl SessionControlPayload for SessionAcceptedMsg {
    fn event_type(&self) -> SessionEvtType {
        SessionEvtType::Accepted
    }

    fn is_event_type(event: SessionEvtType) -> bool {
        event == SessionEvtType::Accepted
    }

    const WIRE_BYTES: usize = size_of::<AcceptedWire>();

    fn encode_wire(&self, payload: &mut [u8]) {
        let (local_is_ip4, local_ip, local_port) = encode_optional_endpoint(self.local);
        let (remote_is_ip4, remote_ip, remote_port) = encode_optional_endpoint(self.remote);
        write_wire(
            AcceptedWire {
                context: self.context,
                listener_session_index: self.listener.session_index,
                listener_thread_index: self.listener.thread_index,
                session_index: self.session.session_index,
                session_thread_index: self.session.thread_index,
                flags: self.flags.bits(),
                local_is_ip4,
                local_ip,
                local_port,
                remote_is_ip4,
                remote_ip,
                remote_port,
                opaque: self.opaque.unwrap_or(u64::MAX),
            },
            payload,
        );
    }

    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError> {
        let wire = read_wire::<AcceptedWire>(payload)?;
        Ok(Self {
            context: wire.context,
            listener: SessionHandle::new(wire.listener_session_index, wire.listener_thread_index),
            session: SessionHandle::new(wire.session_index, wire.session_thread_index),
            flags: SessionFlags::from_bits_retain(wire.flags),
            local: decode_optional_endpoint(wire.local_is_ip4, wire.local_ip, wire.local_port),
            remote: decode_optional_endpoint(wire.remote_is_ip4, wire.remote_ip, wire.remote_port),
            opaque: (wire.opaque != u64::MAX).then_some(wire.opaque),
        })
    }
}

impl SessionControlPayload for SessionAcceptedReplyMsg {
    fn event_type(&self) -> SessionEvtType {
        SessionEvtType::AcceptedReply
    }

    fn is_event_type(event: SessionEvtType) -> bool {
        event == SessionEvtType::AcceptedReply
    }

    const WIRE_BYTES: usize = size_of::<AcceptedReplyWire>();

    fn encode_wire(&self, payload: &mut [u8]) {
        let retval = match self.result {
            Ok(()) => 0,
            Err(error) => error.retval(),
        };
        write_wire(
            AcceptedReplyWire {
                context: self.context,
                session_index: self.session.session_index,
                thread_index: self.session.thread_index,
                retval,
            },
            payload,
        );
    }

    fn decode_wire(payload: &[u8]) -> Result<Self, SessionControlDecodeError> {
        let wire = read_wire::<AcceptedReplyWire>(payload)?;
        let result = if wire.retval == 0 {
            Ok(())
        } else {
            Err(SessionControlError::from_retval(wire.retval)
                .ok_or(SessionControlDecodeError::UnknownErrorCode { code: wire.retval })?)
        };
        Ok(Self {
            context: wire.context,
            session: SessionHandle::new(wire.session_index, wire.thread_index),
            result,
        })
    }
}

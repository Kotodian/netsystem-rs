use hammer_app::AppSession;
use hammer_runtime::app::{SessionConnectError, SessionFlags, SessionHandle, TransportProtocol};

use crate::VclSessionHandle;

/// Which side initiated a Session: the local application (active open,
/// CONNECT_STREAM) or the peer (ACCEPTED).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VclInitiator {
    Local,
    Peer,
}

/// Direction of a capability-checked operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VclDirection {
    Read,
    Write,
}

/// VPP-shaped local Session states.
///
/// Maps `vcl_session_state_t` (vcl_private.h) plus the active-open interim
/// `SESSION_STATE_CONNECTING` (vnet/session/session.h).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VclSessionState {
    /// VCL_STATE_CLOSED: created, no wire handle yet.
    Closed,
    /// VCL_STATE_LISTEN: registered listener.
    Listen,
    /// SESSION_STATE_CONNECTING: the nonblocking active-open interim between
    /// `session_stream_connect` and the asynchronous CONNECTED event.
    Connecting,
    /// VCL_STATE_READY: established; data FIFOs attached.
    Ready,
    /// VCL_STATE_VPP_CLOSING: wire close in flight.
    VppClosing,
    /// VCL_STATE_DISCONNECT: closing; no further operations.
    Disconnect,
    /// VCL_STATE_DETACHED: connect failed; VPP retains `vpp_error`.
    Detached,
    /// VCL_STATE_UPDATED: an attribute update is in flight.
    Updated,
}

/// Derived Session attributes: VPP `SESSION_F_STREAM` /
/// `SESSION_F_UNIDIRECTIONAL` plus the creation-path initiator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VclSessionAttributes {
    pub stream: bool,
    pub unidirectional: bool,
    pub initiator: VclInitiator,
}

impl VclSessionAttributes {
    /// Read capability, derived as VPP derives it: a unidirectional Session
    /// is readable only when peer-initiated; bidirectional Sessions are
    /// always readable.
    pub fn readable(self) -> bool {
        !self.unidirectional || self.initiator == VclInitiator::Peer
    }

    /// Write capability, derived as VPP derives it: a unidirectional Session
    /// is writable only when locally initiated; bidirectional Sessions are
    /// always writable.
    pub fn writable(self) -> bool {
        !self.unidirectional || self.initiator == VclInitiator::Local
    }
}

/// One client-local VCL Session (VPP `vcl_session_t`).
///
/// The struct is public so callers can read attributes, but all fields are
/// private: sessions are created and mutated only through [`VclWorker`].
///
/// [`VclWorker`]: crate::VclWorker
pub struct VclSession {
    pub(crate) state: VclSessionState,
    pub(crate) proto: TransportProtocol,
    /// Wire Session attributes preserved from CONNECTED / ACCEPTED.
    pub(crate) flags: SessionFlags,
    pub(crate) initiator: VclInitiator,
    /// Local parent Session handle (stream parent or listener).
    pub(crate) parent: Option<VclSessionHandle>,
    /// Local child Session handles, for exactly-once parent cascade.
    pub(crate) children: Vec<VclSessionHandle>,
    /// VPP-shaped wire Session handle once established.
    pub(crate) wire_handle: Option<SessionHandle>,
    pub(crate) nonblocking: bool,
    /// Established data path (FIFO / MQ / event consumption).
    pub(crate) app: Option<AppSession>,
    /// Connect error retained on `Detached` (VPP `vpp_error`).
    pub(crate) vpp_error: Option<SessionConnectError>,
}

impl VclSession {
    /// Fresh CLOSED session (VPP `vppcom_session_create`).
    pub(crate) fn new(proto: TransportProtocol, nonblocking: bool) -> Self {
        Self {
            state: VclSessionState::Closed,
            proto,
            flags: SessionFlags::empty(),
            initiator: VclInitiator::Local,
            parent: None,
            children: Vec::new(),
            wire_handle: None,
            nonblocking,
            app: None,
            vpp_error: None,
        }
    }

    /// LISTEN session bound to a wire listener handle (set by the worker).
    pub(crate) fn listener(proto: TransportProtocol) -> Self {
        Self {
            state: VclSessionState::Listen,
            proto,
            flags: SessionFlags::empty(),
            initiator: VclInitiator::Local,
            parent: None,
            children: Vec::new(),
            wire_handle: None,
            nonblocking: false,
            app: None,
            vpp_error: None,
        }
    }

    /// Peer-open child: READY once the worker attaches its FIFOs.
    pub(crate) fn peer_child(flags: SessionFlags) -> Self {
        Self {
            state: VclSessionState::Ready,
            proto: TransportProtocol::Quic,
            flags,
            initiator: VclInitiator::Peer,
            parent: None,
            children: Vec::new(),
            wire_handle: None,
            nonblocking: false,
            app: None,
            vpp_error: None,
        }
    }

    /// VPP-shaped local state.
    pub fn state(&self) -> VclSessionState {
        self.state
    }

    /// Derived stream / unidirectional / initiator attributes and the
    /// readable / writable capability derived from them.
    pub fn attributes(&self) -> VclSessionAttributes {
        VclSessionAttributes {
            stream: self.flags.contains(SessionFlags::STREAM),
            unidirectional: self.flags.contains(SessionFlags::UNIDIRECTIONAL),
            initiator: self.initiator,
        }
    }

    /// Local parent Session handle, when this Session is a stream child or
    /// an accepted peer child.
    pub fn parent(&self) -> Option<VclSessionHandle> {
        self.parent
    }

    /// The wire Session handle (VPP-shaped `session_handle_t`), when
    /// established. The QUIC stream ID is never exposed.
    pub fn wire_handle(&self) -> Option<SessionHandle> {
        self.wire_handle
    }
}

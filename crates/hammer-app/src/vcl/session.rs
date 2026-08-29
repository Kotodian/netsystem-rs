use crate::AppSession;
use hammer_runtime::app::{SessionConnectError, SessionFlags, SessionHandle};

/// Which side initiated a Session: the local application (active open,
/// CONNECT_STREAM) or the peer (ACCEPTED).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Initiator {
    Local,
    Peer,
}

/// Direction of a capability-checked operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Read,
    Write,
}

/// VPP-shaped local Session states.
///
/// Maps `vcl_session_state_t` (vcl_private.h) plus the active-open interim
/// `SESSION_STATE_CONNECTING` (vnet/session/session.h).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
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
    Closing,
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
pub struct SessionAttributes {
    pub stream: bool,
    pub unidirectional: bool,
    pub initiator: Initiator,
}

impl SessionAttributes {
    /// Read capability, derived as VPP derives it: a unidirectional Session
    /// is readable only when peer-initiated; bidirectional Sessions are
    /// always readable.
    pub fn readable(self) -> bool {
        !self.unidirectional || self.initiator == Initiator::Peer
    }

    /// Write capability, derived as VPP derives it: a unidirectional Session
    /// is writable only when locally initiated; bidirectional Sessions are
    /// always writable.
    pub fn writable(self) -> bool {
        !self.unidirectional || self.initiator == Initiator::Local
    }
}

/// One client-local VCL Session (VPP `vcl_session_t`).
///
/// The struct is public so callers can read attributes, but all fields are
/// private: sessions are created and mutated only through [`Worker`].
///
/// [`Worker`]: crate::Worker
pub(super) struct Session {
    pub(super) state: SessionState,
    pub(crate) proto: u8,
    /// Wire Session attributes preserved from CONNECTED / ACCEPTED.
    pub(crate) flags: SessionFlags,
    pub(super) initiator: Initiator,
    /// Local parent Session handle (stream parent or listener).
    pub(crate) parent: Option<u32>,
    /// Local child Session handles, for exactly-once parent cascade.
    pub(crate) children: Vec<u32>,
    /// VPP-shaped wire Session handle once established.
    pub(crate) wire_handle: Option<SessionHandle>,
    pub(crate) nonblocking: bool,
    /// Established data path (FIFO / MQ / event consumption).
    pub(crate) app: Option<AppSession>,
    /// Connect error retained on `Detached` (VPP `vpp_error`).
    pub(crate) connect_error: Option<SessionConnectError>,
}

impl Session {
    /// Fresh CLOSED session (VPP `vppcom_session_create`).
    pub(crate) fn new(proto: u8, nonblocking: bool) -> Self {
        Self {
            state: SessionState::Closed,
            proto,
            flags: SessionFlags::empty(),
            initiator: Initiator::Local,
            parent: None,
            children: Vec::new(),
            wire_handle: None,
            nonblocking,
            app: None,
            connect_error: None,
        }
    }

    /// LISTEN session bound to a wire listener handle (set by the worker).
    pub(crate) fn listener(proto: u8) -> Self {
        Self {
            state: SessionState::Listen,
            proto,
            flags: SessionFlags::empty(),
            initiator: Initiator::Local,
            parent: None,
            children: Vec::new(),
            wire_handle: None,
            nonblocking: false,
            app: None,
            connect_error: None,
        }
    }

    /// Peer-open child: READY once the worker attaches its FIFOs. The
    /// transport is the listener's, inherited at allocation (VPP
    /// `vcl_session_accepted_handler`: `session->session_type =
    /// listen_session->session_type`, vppcom.c:365).
    pub(crate) fn peer_child(proto: u8, flags: SessionFlags) -> Self {
        Self {
            state: SessionState::Ready,
            proto,
            flags,
            initiator: Initiator::Peer,
            parent: None,
            children: Vec::new(),
            wire_handle: None,
            nonblocking: false,
            app: None,
            connect_error: None,
        }
    }

    /// VPP-shaped local state.
    pub fn state(&self) -> SessionState {
        self.state
    }

    /// Derived stream / unidirectional / initiator attributes and the
    /// readable / writable capability derived from them.
    pub fn attributes(&self) -> SessionAttributes {
        SessionAttributes {
            stream: self.flags.contains(SessionFlags::STREAM),
            unidirectional: self.flags.contains(SessionFlags::UNIDIRECTIONAL),
            initiator: self.initiator,
        }
    }
}

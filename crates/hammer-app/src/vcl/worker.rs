use std::collections::HashMap;
use std::net::SocketAddr;

use crate::AppSession;
use crate::attach::{AppClient, AppClientError, ControlReply};
use hammer_runtime::SessionListenEndpoint;
use hammer_runtime::app::{
    SessionAcceptedMsg, SessionAcceptedReplyMsg, SessionConnectError, SessionConnectedMsg,
    SessionFlags, SessionHandle,
};

use hammer_infra::pool::Pool;

use super::Error;
use super::session::{Direction, Initiator, Session, SessionAttributes, SessionState};

/// Notifications produced by [`Worker::session_poll`] as asynchronous
/// Session control events are consumed. In-memory only; never serialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// CONNECTED consumed: the Session transitioned Connecting -> Ready.
    Connected { session: u32 },
    /// ACCEPTED consumed: a peer-open child was allocated and is Ready.
    Accepted { session: u32, parent: u32 },
}

/// Decision for one CONNECTED event (VPP `vcl_session_connected_handler`
/// up to the segment attach).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectOutcome {
    /// The wire Session handle to attach; the caller then completes the
    /// Session with the CONNECTED flags.
    Established { wire: SessionHandle },
    /// Connect failed: the Session is Detached retaining its connect error.
    Failed { error: SessionConnectError },
}

/// Decision for one ACCEPTED event (VPP `vcl_session_accepted_handler` up to
/// the segment attach).
pub(crate) enum PeerOutcome {
    /// Unknown listener or non-listening parent: drop without allocation.
    Drop,
    /// A peer child was allocated; the caller attaches its FIFOs and
    /// completes it.
    Child { handle: u32, parent: u32 },
}

/// What a stream connect needs to enqueue and how it completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectBegin {
    pub(crate) parent_wire: SessionHandle,
    pub(crate) proto: u8,
    pub(crate) nonblocking: bool,
}

/// What an ordinary (non-stream) active open needs to enqueue and how it
/// completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectParams {
    pub(crate) proto: u8,
    pub(crate) nonblocking: bool,
}

/// Worker-local Session state: the Pool and its wire/control indexes remain
/// owned directly by the client worker.
///
/// The worker owns the Application attach, control queue, and these local
/// Session facts. No separate storage object sits between the owner and the
/// pool.
pub struct Worker {
    client: AppClient,
    pool: Pool<Session>,
    /// Local Session handle by wire Session handle (VPP `vcl_session_table`,
    /// session_table_add_vpp_handle): resolves ACCEPTED listeners and drops
    /// stale wire events.
    session_handles: HashMap<(u32, u32), u32>,
    /// In-flight generic (non-stream) connects by the client-owned
    /// connection identity, connection -> Session. `AppClient::connect`
    /// assigns the identity, so its CONNECTED event carries the connection
    /// identity rather than the Session identity (VPP instead uses
    /// `mp->context = s->session_index`, vcl_send_session_connect,
    /// vppcom.c:76); this map resolves it.
    ///
    /// Stream connects need no entry: their context is the Session handle
    /// itself. The two spaces are disjoint within the limits of their
    /// widths: generic connect contexts are client connection counters while
    /// stream connect contexts are direct local pool indexes.
    /// `resolve_connected` consults this map first, so a generic connect is
    /// unambiguous for any pool lifetime, where the counter remains far
    /// below that bound.
    pending_connects: HashMap<u32, u32>,
    /// Reverse index, Session -> connection, for exactly-once O(1) removal
    /// when the Session closes or its CONNECTED resolves: a daemon that
    /// never replies leaves no map residue.
    pending_by_handle: HashMap<u32, u32>,
}

impl Worker {
    /// Creates a CLOSED local Session (VPP `vppcom_session_create`).
    pub(crate) fn create(&mut self, proto: u8, nonblocking: bool) -> Result<u32, Error> {
        Ok(self.pool.insert(Session::new(proto, nonblocking)))
    }

    /// Allocates a LISTEN Session for a BOUND wire listener handle.
    pub(crate) fn bind_listener(&mut self, proto: u8, wire: SessionHandle) -> Result<u32, Error> {
        let handle = self.pool.insert(Session::listener(proto));
        self.pool
            .get_mut(handle)
            .ok_or(Error::InvalidHandle { handle })?
            .wire_handle = Some(wire);
        self.session_handles
            .insert((wire.session_index, wire.thread_index), handle);
        Ok(handle)
    }

    fn state(&self, handle: u32) -> Result<SessionState, Error> {
        self.pool
            .get(handle)
            .map(Session::state)
            .ok_or(Error::InvalidHandle { handle })
    }

    fn attributes(&self, handle: u32) -> Result<SessionAttributes, Error> {
        self.pool
            .get(handle)
            .map(Session::attributes)
            .ok_or(Error::InvalidHandle { handle })
    }

    fn get(&self, handle: u32) -> Result<&Session, Error> {
        self.pool.get(handle).ok_or(Error::InvalidHandle { handle })
    }

    /// Validates one two-step active open and marks the child Connecting
    /// (VPP `vppcom_session_stream_connect`): only a CLOSED child connects
    /// through an established READY parent, never to itself. Returns what
    /// the caller needs to enqueue CONNECT_STREAM and how to complete.
    pub(crate) fn begin_stream_connect(
        &mut self,
        child: u32,
        parent: u32,
        flags: SessionFlags,
    ) -> Result<ConnectBegin, Error> {
        if child == parent {
            return Err(Error::SelfParent { session: child });
        }
        let parent_session = self
            .pool
            .get(parent)
            .ok_or(Error::InvalidHandle { handle: parent })?;
        let parent_wire = parent_session
            .wire_handle
            .ok_or(Error::ParentNotEstablished { parent })?;
        if parent_session.state != SessionState::Ready {
            return Err(Error::ParentNotReady {
                parent,
                state: parent_session.state,
            });
        }
        let child_state = self.state(child)?;
        if child_state != SessionState::Closed {
            return Err(Error::NotConnectable {
                session: child,
                state: child_state,
            });
        }
        let (nonblocking, proto) = {
            let session = self.get(child)?;
            (session.nonblocking, session.proto)
        };
        {
            let session = self
                .pool
                .get_mut(child)
                .ok_or(Error::InvalidHandle { handle: child })?;
            session.parent = Some(parent);
            session.flags = flags | SessionFlags::STREAM;
            session.initiator = Initiator::Local;
            session.state = SessionState::Connecting;
        }
        // The child is tracked so the parent cascade closes it exactly once.
        self.pool
            .get_mut(parent)
            .ok_or(Error::InvalidHandle { handle: parent })?
            .children
            .push(child);
        Ok(ConnectBegin {
            parent_wire,
            proto,
            nonblocking,
        })
    }

    /// Validates one ordinary active open and marks the Session Connecting
    /// (VPP `vppcom_session_connect`, vppcom.c:2102: `vcl_send_session_connect`,
    /// vppcom.c:76): only a CLOSED Session connects, and the transport is
    /// the Session's own create-time protocol (`mp->proto = s->session_type`).
    /// Returns what the caller needs to enqueue CONNECT and how to complete.
    pub(crate) fn begin_connect(&mut self, session: u32) -> Result<ConnectParams, Error> {
        let state = self.state(session)?;
        if state != SessionState::Closed {
            return Err(Error::NotConnectable { session, state });
        }
        let (proto, nonblocking) = {
            let session = self
                .pool
                .get_mut(session)
                .ok_or(Error::InvalidHandle { handle: session })?;
            session.initiator = Initiator::Local;
            session.state = SessionState::Connecting;
            (session.proto, session.nonblocking)
        };
        Ok(ConnectParams { proto, nonblocking })
    }

    /// Registers an in-flight generic connect in both indexes. Infallible:
    /// both maps are keyed by owned values and the Session was validated by
    /// [`Self::begin_connect`] immediately prior — nothing in
    /// between can free it — so no Session-side write and no error path
    /// exist; the maps are only written after the enqueue succeeded.
    pub(crate) fn register_connect(&mut self, session: u32, connection: u32) {
        self.pending_connects.insert(connection, session);
        self.pending_by_handle.insert(session, connection);
    }

    /// Resolves the CONNECTED event of one in-flight generic connect, if
    /// any: removes the tracking in both indexes and returns its Session.
    /// Infallible; a context that names no in-flight connect yields `None`.
    pub(crate) fn resolve_connected(&mut self, context: u64) -> Option<u32> {
        let connection = context as u32;
        let handle = self.pending_connects.remove(&connection)?;
        self.pending_by_handle.remove(&handle);
        Some(handle)
    }

    /// Applies one CONNECTED event (VPP `vcl_session_connected_handler`): the
    /// context selects the local Session; only a Connecting Session
    /// transitions. Stale handles and non-Connecting Sessions drop without
    /// allocation, callback, or state mutation. A failure transitions the
    /// Session to Detached retaining its connect error.
    pub(crate) fn accept_connected(
        &mut self,
        handle: u32,
        result: Result<SessionHandle, SessionConnectError>,
    ) -> Result<Option<ConnectOutcome>, Error> {
        let Some(session) = self.pool.get_mut(handle) else {
            return Ok(None);
        };
        if session.state != SessionState::Connecting {
            return Ok(None);
        }
        match result {
            Ok(wire) => Ok(Some(ConnectOutcome::Established { wire })),
            Err(error) => {
                session.state = SessionState::Detached;
                session.connect_error = Some(error);
                session.wire_handle = None;
                Ok(Some(ConnectOutcome::Failed { error }))
            }
        }
    }

    /// Completes an established Session (VPP `vcl_session_connected_handler`
    /// after segment attach): READY with the connect-time flags preserved,
    /// the CONNECTED flags added, and the wire handle registered.
    pub(crate) fn complete_connected(
        &mut self,
        handle: u32,
        wire: SessionHandle,
        flags: SessionFlags,
        app: Option<AppSession>,
    ) -> Result<(), Error> {
        {
            let session = self
                .pool
                .get_mut(handle)
                .ok_or(Error::InvalidHandle { handle })?;
            session.wire_handle = Some(wire);
            // VPP `vcl_session_connected_handler` never touches session
            // flags: the connect-time flags set by CONNECT_STREAM persist
            // and the CONNECTED flags are additive (Hammer's wire carries
            // them; VPP's message does not).
            session.flags = session.flags.union(flags);
            session.initiator = Initiator::Local;
            session.connect_error = None;
            session.state = SessionState::Ready;
            session.app = app;
        }
        self.session_handles
            .insert((wire.session_index, wire.thread_index), handle);
        Ok(())
    }

    /// Applies one ACCEPTED event up to the child allocation (VPP
    /// `vcl_session_accepted_handler`): the listener wire must resolve to a
    /// LISTEN Session, otherwise the event drops without allocation.
    pub(crate) fn allocate_peer(
        &mut self,
        listener_wire: SessionHandle,
        flags: SessionFlags,
    ) -> Result<PeerOutcome, Error> {
        let Some(&parent) = self
            .session_handles
            .get(&(listener_wire.session_index, listener_wire.thread_index))
        else {
            return Ok(PeerOutcome::Drop);
        };
        if !matches!(self.state(parent), Ok(SessionState::Listen)) {
            return Ok(PeerOutcome::Drop);
        }
        // The child inherits the listener's transport (VPP
        // `vcl_session_accepted_handler`: `session->session_type =
        // listen_session->session_type`, vppcom.c:365).
        let proto = self.get(parent)?.proto;
        let handle = self.pool.insert(Session::peer_child(proto, flags));
        Ok(PeerOutcome::Child { handle, parent })
    }

    /// Completes a peer-open child after its FIFOs were attached (VPP
    /// `vcl_session_accepted_handler` after segment attach): READY with the
    /// ACCEPTED flags, the listener as parent, and the child tracked.
    pub(crate) fn complete_peer(
        &mut self,
        handle: u32,
        accepted: &SessionAcceptedMsg,
        app: Option<AppSession>,
    ) -> Result<(), Error> {
        let parent = self
            .session_handles
            .get(&(
                accepted.listener.session_index,
                accepted.listener.thread_index,
            ))
            .copied()
            .ok_or(Error::InvalidHandle { handle })?;
        {
            let session = self
                .pool
                .get_mut(handle)
                .ok_or(Error::InvalidHandle { handle })?;
            session.state = SessionState::Ready;
            session.wire_handle = Some(accepted.session);
            session.parent = Some(parent);
            session.flags = accepted.flags;
            session.initiator = Initiator::Peer;
            session.app = app;
        }
        self.pool
            .get_mut(parent)
            .ok_or(Error::InvalidHandle { handle: parent })?
            .children
            .push(handle);
        self.session_handles.insert(
            (
                accepted.session.session_index,
                accepted.session.thread_index,
            ),
            handle,
        );
        Ok(())
    }

    /// Closes one Session and cascades to every child exactly once (VPP
    /// `vcl_session_cleanup`). The exactly-once guard is the Session state: a
    /// Session already Closed or Disconnect is a no-op, so re-entry through
    /// any path cannot double-close; every child is attempted even when an
    /// individual step fails. An in-flight generic connect is untracked in
    /// O(1) so a daemon that never replies leaves no map residue. Returns
    /// whether the Session was freed.
    pub(crate) fn close_cascade(&mut self, handle: u32) -> Result<bool, Error> {
        let (parent, wire) = {
            let session = self.get(handle)?;
            match session.state {
                SessionState::Closed | SessionState::Disconnect => return Ok(false),
                _ => {}
            }
            (session.parent, session.wire_handle)
        };
        let children = {
            let session = self.get(handle)?;
            session.children.clone()
        };
        self.pool
            .get_mut(handle)
            .ok_or(Error::InvalidHandle { handle })?
            .state = SessionState::Disconnect;
        for child in children {
            let _ = self.close_cascade(child);
        }
        if self.pool.remove(handle).is_some() {
            if let Some(connection) = self.pending_by_handle.remove(&handle) {
                self.pending_connects.remove(&connection);
            }
            if let Some(wire) = wire {
                self.session_handles
                    .remove(&(wire.session_index, wire.thread_index));
            }
            if let Some(parent) = parent {
                if let Some(session) = self.pool.get_mut(parent) {
                    session.children.retain(|entry| *entry != handle);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Worker {
    /// Attaches to the Application daemon and creates a worker-local Session
    /// pool.
    pub fn attach(path: &str) -> Result<Self, Error> {
        let client = AppClient::attach(path)?;
        Ok(Self::with_client(client))
    }

    /// Builds a worker over an already-attached client.
    pub fn with_client(client: AppClient) -> Self {
        Self {
            client,
            pool: Pool::default(),
            session_handles: HashMap::new(),
            pending_connects: HashMap::new(),
            pending_by_handle: HashMap::new(),
        }
    }

    /// Creates a CLOSED local Session (VPP `vppcom_session_create`). The
    /// `is_nonblocking` attribute selects the behavior of the later
    /// `session_stream_connect` on this Session: blocking waits for the
    /// CONNECTED event; nonblocking returns immediately in `Connecting`.
    pub fn session_create(&mut self, proto: u8, is_nonblocking: bool) -> Result<u32, Error> {
        self.create(proto, is_nonblocking)
    }

    /// Registers a transport listener and returns the local LISTEN Session
    /// (VPP `vppcom_session_listen`). Blocks until the BOUND message; the
    /// wire listener handle is retained to parent ACCEPTED peer children.
    pub fn session_listen(
        &mut self,
        transport: u8,
        endpoint: SessionListenEndpoint,
        opaque: Option<u64>,
    ) -> Result<u32, Error> {
        let listener = self.client.listen(transport, endpoint, None, opaque)?;
        self.bind_listener(transport, listener)
    }

    /// Two-step active open: opens one stream on an established parent
    /// Session (VPP `vppcom_session_stream_connect`, CONNECT_STREAM).
    ///
    /// Only the child's state changes: it is marked Connecting and the
    /// CONNECTED event transitions exactly that child to Ready. Blocking
    /// Sessions wait on the single control inbox; nonblocking Sessions
    /// return immediately and complete through [`Self::session_poll`].
    pub fn session_stream_connect(
        &mut self,
        child: u32,
        parent: u32,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        flags: SessionFlags,
    ) -> Result<(), Error> {
        let begin = self.begin_stream_connect(child, parent, flags)?;
        // The control context carries the direct local pool index.
        self.client.connect_stream(
            child.into(),
            begin.proto,
            remote,
            local,
            None,
            begin.parent_wire,
            flags | SessionFlags::STREAM,
        )?;
        if begin.nonblocking {
            return Ok(());
        }
        self.wait_connected(child)
    }

    /// Generic two-step active open (VPP `vppcom_session_connect`,
    /// vppcom.c:2102 / `vcl_send_session_connect`, vppcom.c:76): opens an
    /// ordinary connection on a CLOSED Session using its create-time
    /// transport, forwarding the local/remote endpoint, server name (SNI),
    /// and opaque value without parsing transport-specific configuration.
    ///
    /// Registration happens only after the enqueue succeeded and is
    /// infallible (both tracking indexes take owned values), so a successful
    /// CONNECT is never left untracked.
    ///
    /// Blocking Sessions wait on the single control inbox and surface
    /// failure as [`Error::ConnectFailed`]; nonblocking Sessions return
    /// immediately in `Connecting` and complete through [`Self::session_poll`],
    /// with failure observable the same way every asynchronous outcome is:
    /// the Session reaches [`SessionState::Detached`], read through
    /// [`Self::session_state`]. No transport-specific case is added.
    pub fn session_connect(
        &mut self,
        session: u32,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        server_name: Option<&str>,
        opaque: Option<u64>,
    ) -> Result<(), Error> {
        let params = self.begin_connect(session)?;
        let connection =
            match self
                .client
                .connect(params.proto, remote, local, None, opaque, server_name)
            {
                Ok(connection) => connection,
                Err(error) => return Err(error.into()),
            };
        self.register_connect(session, connection);
        if params.nonblocking {
            return Ok(());
        }
        self.wait_connected(session)
    }

    /// Consumes all currently available Session control events, nonblocking.
    ///
    /// Never blocks and never spins: the inbox is drained once and
    /// asynchronous CONNECTED / ACCEPTED events are applied (Connecting ->
    /// Ready transitions, peer child allocation, ACCEPTED_REPLY).
    pub fn session_poll(&mut self) -> Result<Vec<Event>, Error> {
        let mut events = Vec::new();
        while let Some(reply) = self.client.poll_control()? {
            if let Some(event) = self.process_reply(reply)? {
                events.push(event);
            }
        }
        Ok(events)
    }

    /// Closes a Session; closing a parent cascades to every child exactly
    /// once (VPP `vcl_session_cleanup`). A listener is unlistened first.
    pub fn session_close(&mut self, handle: u32) -> Result<(), Error> {
        if matches!(self.state(handle)?, SessionState::Listen) {
            let wire = self
                .get(handle)?
                .wire_handle
                .ok_or(Error::SessionNotReady { session: handle })?;
            self.client.unlisten(wire)?;
        }
        self.close_cascade(handle)?;
        Ok(())
    }

    /// Sends bytes on an established writable Session. A direction-invalid
    /// operation returns a typed error and mutates nothing.
    pub fn session_send(&mut self, handle: u32, bytes: &[u8]) -> Result<usize, Error> {
        let session = self.get(handle)?;
        if !session.attributes().writable() {
            return Err(Error::DirectionInvalid {
                session: handle,
                direction: Direction::Write,
            });
        }
        let app = session
            .app
            .as_ref()
            .ok_or(Error::SessionNotReady { session: handle })?;
        Ok(app.send_bytes(bytes)?)
    }

    /// Receives bytes from an established readable Session (VPP
    /// `vppcom_session_read_internal`: peek then dequeue-drop).
    pub fn session_recv(&mut self, handle: u32, out: &mut [u8]) -> Result<usize, Error> {
        let session = self.get(handle)?;
        if !session.attributes().readable() {
            return Err(Error::DirectionInvalid {
                session: handle,
                direction: Direction::Read,
            });
        }
        let app = session
            .app
            .as_ref()
            .ok_or(Error::SessionNotReady { session: handle })?;
        let read = app.recv_bytes(out);
        let _ = app.consume_rx(read);
        Ok(read)
    }

    /// Current local state of one Session; a stale handle is a typed error.
    pub fn session_state(&self, handle: u32) -> Result<SessionState, Error> {
        self.state(handle)
    }

    /// Derived attributes and capabilities of one Session.
    pub fn session_attributes(&self, handle: u32) -> Result<SessionAttributes, Error> {
        self.attributes(handle)
    }

    /// Transport protocol of one Session: the create-time protocol for
    /// local Sessions, the listener-inherited protocol for accepted peer
    /// children (VPP `VPPCOM_ATTR_GET_PROTOCOL`, vppcom.h:143).
    pub fn session_proto(&self, handle: u32) -> Result<u8, Error> {
        Ok(self.get(handle)?.proto)
    }

    /// Blocking completion of one active open: drain the single control
    /// inbox until the CONNECTED event resolves `child`.
    fn wait_connected(&mut self, child: u32) -> Result<(), Error> {
        loop {
            if let Some(reply) = self.client.poll_control()? {
                self.process_reply(reply)?;
            } else {
                self.client.wait_control()?;
            }
            match self.state(child)? {
                SessionState::Ready => return Ok(()),
                SessionState::Detached => return Err(self.connect_failure(child)),
                SessionState::Closed => {
                    return Err(Error::SessionNotReady { session: child });
                }
                _ => {}
            }
        }
    }

    /// The retained connect error of a Detached Session.
    fn connect_failure(&self, child: u32) -> Error {
        let error = self
            .pool
            .get(child)
            .and_then(|session| session.connect_error);
        match error {
            Some(error) => Error::ConnectFailed {
                session: child,
                error,
            },
            None => Error::DetachedWithoutError { session: child },
        }
    }

    /// Applies one buffered control reply. Returns the event the reply
    /// produced, if any; stale or mismatched wire events are dropped.
    fn process_reply(&mut self, reply: ControlReply) -> Result<Option<Event>, Error> {
        let kind = reply.kind();
        match reply {
            ControlReply::Connected(connected) => self.process_connected(connected),
            ControlReply::Accepted(accepted) => self.process_accepted(accepted),
            ControlReply::Bound(_) | ControlReply::Unlisten(_) => {
                Err(Error::UnexpectedReply { kind })
            }
        }
    }

    /// VPP `vcl_session_connected_handler`: the CONNECTED context selects the
    /// local Session; only a Connecting Session transitions (to Ready, or to
    /// Detached retaining its connect error. Anything else drops without
    /// allocation, callback, or state mutation.
    fn process_connected(
        &mut self,
        connected: SessionConnectedMsg,
    ) -> Result<Option<Event>, Error> {
        // Generic connects resolve through their application connection id;
        // stream connects carry the direct local pool index.
        let handle = match self.resolve_connected(connected.context) {
            Some(handle) => handle,
            None => connected.context as u32,
        };
        let Some(outcome) = self.accept_connected(handle, connected.result)? else {
            return Ok(None);
        };
        match outcome {
            ConnectOutcome::Established { wire } => {
                let app = match self.client.accept_with_handle(Some(wire)) {
                    Ok(app) => app,
                    Err(AppClientError::SessionHandleMismatch { .. }) => {
                        // Stale/mismatched CONNECTED: the published
                        // descriptors belong to a newer Session. Drop the
                        // Session exactly as ACCEPTED handling does; the
                        // pending-connect tracking was already resolved.
                        self.close_cascade(handle)?;
                        return Ok(None);
                    }
                    Err(error) => return Err(error.into()),
                };
                self.complete_connected(handle, wire, connected.flags, Some(app))?;
                Ok(Some(Event::Connected { session: handle }))
            }
            ConnectOutcome::Failed { .. } => Ok(None),
        }
    }

    /// VPP `vcl_session_accepted_handler`: allocate a peer-open child, attach
    /// its FIFOs, transition it to Ready, and send ACCEPTED_REPLY. An
    /// unknown or non-listening parent, or a stale/mismatched publication,
    /// drops without allocation, callback, or state mutation.
    fn process_accepted(&mut self, accepted: SessionAcceptedMsg) -> Result<Option<Event>, Error> {
        let (child, parent) = match self.allocate_peer(accepted.listener, accepted.flags)? {
            PeerOutcome::Drop => return Ok(None),
            PeerOutcome::Child { handle, parent } => (handle, parent),
        };
        let app = match self.client.accept_with_handle(Some(accepted.session)) {
            Ok(app) => app,
            Err(AppClientError::SessionHandleMismatch { .. }) => {
                // Stale/mismatched ACCEPTED: the published descriptors
                // belong to a newer Session; drop the child.
                self.pool.remove(child);
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        self.complete_peer(child, &accepted, Some(app))?;
        let reply = SessionAcceptedReplyMsg::new(
            self.client.application().into(),
            accepted.session,
            Ok(()),
        );
        self.client.accepted_reply(&reply)?;
        Ok(Some(Event::Accepted {
            session: child,
            parent,
        }))
    }
}

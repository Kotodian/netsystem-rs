use std::collections::HashMap;
use std::net::SocketAddr;

use hammer_app::AppSession;
use hammer_app::attach::{AppClient, AppClientError, ControlReply};
use hammer_runtime::app::{
    SessionAcceptedMsg, SessionAcceptedReplyMsg, SessionConnectError, SessionConnectedMsg,
    SessionControlError, SessionFlags, SessionHandle, TransportProtocol,
};
use hammer_runtime::{SessionListenEndpoint, SessionListenerId};

use crate::pool::{SessionPool, VclSessionHandle};
use crate::session::{VclInitiator, VclSession, VclSessionAttributes, VclSessionState};
use crate::{VclDirection, VclError};

fn app_error(source: AppClientError) -> VclError {
    VclError::AppClient { source }
}

/// Notifications produced by [`VclWorker::session_poll`] as asynchronous
/// Session control events are consumed. In-memory only; never serialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VclEvent {
    /// CONNECTED consumed: the Session transitioned Connecting -> Ready.
    Connected { session: VclSessionHandle },
    /// ACCEPTED consumed: a peer-open child was allocated and is Ready.
    Accepted {
        session: VclSessionHandle,
        parent: VclSessionHandle,
    },
}

/// Decision for one CONNECTED event (VPP `vcl_session_connected_handler`
/// up to the segment attach).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectOutcome {
    /// The wire Session handle to attach; the caller then completes the
    /// Session with the CONNECTED flags.
    Established { wire: SessionHandle },
    /// Connect failed: the Session is Detached retaining `vpp_error`.
    Failed { error: SessionConnectError },
}

/// Decision for one ACCEPTED event (VPP `vcl_session_accepted_handler` up to
/// the segment attach).
pub(crate) enum PeerOutcome {
    /// Unknown listener or non-listening parent: drop without allocation.
    Drop,
    /// Pool full: reply ACCEPTED_REPLY with a capacity error.
    RejectCapacity,
    /// A peer child was allocated; the caller attaches its FIFOs and
    /// completes it.
    Child {
        handle: VclSessionHandle,
        parent: VclSessionHandle,
    },
}

/// What a stream connect needs to enqueue and how it completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ConnectBegin {
    pub(crate) parent_wire: SessionHandle,
    pub(crate) proto: TransportProtocol,
    pub(crate) nonblocking: bool,
}

/// Worker-local Session domain (VPP `vcl_worker_t` session table): the
/// fixed-capacity generation-safe pool plus the wire-handle lookup, and the
/// pure VCL state transitions. No IO: attach, control MQ, and descriptor
/// delivery live on [`VclWorker`], which applies the decisions returned
/// here.
pub(crate) struct SessionStore {
    pool: SessionPool,
    /// Local Session handle by wire Session handle (VPP `vcl_session_table`,
    /// session_table_add_vpp_handle): resolves ACCEPTED listeners and drops
    /// stale wire events.
    vpp_handles: HashMap<u64, VclSessionHandle>,
}

impl SessionStore {
    pub(crate) fn new(capacity: usize) -> Result<Self, VclError> {
        if capacity == 0 {
            return Err(VclError::PoolCapacityInvalid { capacity });
        }
        Ok(Self {
            pool: SessionPool::new(capacity),
            vpp_handles: HashMap::new(),
        })
    }

    /// Creates a CLOSED local Session (VPP `vppcom_session_create`).
    pub(crate) fn create(
        &mut self,
        proto: TransportProtocol,
        nonblocking: bool,
    ) -> Result<VclSessionHandle, VclError> {
        self.pool.alloc(VclSession::new(proto, nonblocking))
    }

    /// Allocates a LISTEN Session for a BOUND wire listener handle.
    pub(crate) fn bind_listener(
        &mut self,
        proto: TransportProtocol,
        wire: SessionHandle,
    ) -> Result<VclSessionHandle, VclError> {
        let handle = self.pool.alloc(VclSession::listener(proto))?;
        self.pool.get_mut(handle)?.wire_handle = Some(wire);
        self.vpp_handles.insert(wire.raw(), handle);
        Ok(handle)
    }

    /// Test seam: fabricates a READY Session carrying `wire` and `flags`.
    #[cfg(test)]
    pub(crate) fn seed_ready(
        &mut self,
        proto: TransportProtocol,
        wire: SessionHandle,
        flags: SessionFlags,
    ) -> Result<VclSessionHandle, VclError> {
        let handle = self.pool.alloc(VclSession::new(proto, false))?;
        {
            let session = self.pool.get_mut(handle)?;
            session.state = VclSessionState::Ready;
            session.wire_handle = Some(wire);
            session.flags = flags;
        }
        self.vpp_handles.insert(wire.raw(), handle);
        Ok(handle)
    }

    /// Local Session handle registered for a wire Session handle, if any.
    pub(crate) fn wire_handle(&self, wire: SessionHandle) -> Option<VclSessionHandle> {
        self.vpp_handles.get(&wire.raw()).copied()
    }

    pub(crate) fn state(&self, handle: VclSessionHandle) -> Result<VclSessionState, VclError> {
        self.pool.state(handle)
    }

    pub(crate) fn attributes(
        &self,
        handle: VclSessionHandle,
    ) -> Result<VclSessionAttributes, VclError> {
        Ok(self.pool.get(handle)?.attributes())
    }

    pub(crate) fn get(&self, handle: VclSessionHandle) -> Result<&VclSession, VclError> {
        self.pool.get(handle)
    }

    /// Validates one two-step active open and marks the child Connecting
    /// (VPP `vppcom_session_stream_connect`): only a CLOSED child connects
    /// through an established READY parent, never to itself. Returns what
    /// the caller needs to enqueue CONNECT_STREAM and how to complete.
    pub(crate) fn begin_stream_connect(
        &mut self,
        child: VclSessionHandle,
        parent: VclSessionHandle,
        flags: SessionFlags,
    ) -> Result<ConnectBegin, VclError> {
        if child == parent {
            return Err(VclError::SelfParent { session: child });
        }
        let parent_session = self.pool.get(parent)?;
        let parent_wire = parent_session
            .wire_handle
            .ok_or(VclError::ParentNotEstablished { parent })?;
        if parent_session.state != VclSessionState::Ready {
            return Err(VclError::ParentNotReady {
                parent,
                state: parent_session.state,
            });
        }
        let child_state = self.pool.state(child)?;
        if child_state != VclSessionState::Closed {
            return Err(VclError::NotConnectable {
                session: child,
                state: child_state,
            });
        }
        let (nonblocking, proto) = {
            let session = self.pool.get(child)?;
            (session.nonblocking, session.proto)
        };
        {
            let session = self.pool.get_mut(child)?;
            session.parent = Some(parent);
            session.flags = flags | SessionFlags::STREAM;
            session.initiator = VclInitiator::Local;
            session.state = VclSessionState::Connecting;
        }
        // The child is tracked so the parent cascade closes it exactly once.
        self.pool.get_mut(parent)?.children.push(child);
        Ok(ConnectBegin {
            parent_wire,
            proto,
            nonblocking,
        })
    }

    /// Applies one CONNECTED event (VPP `vcl_session_connected_handler`): the
    /// context selects the local Session; only a Connecting Session
    /// transitions. Stale handles and non-Connecting Sessions drop without
    /// allocation, callback, or state mutation. A failure transitions the
    /// Session to Detached retaining `vpp_error`.
    pub(crate) fn accept_connected(
        &mut self,
        handle: VclSessionHandle,
        result: Result<SessionHandle, SessionConnectError>,
    ) -> Result<Option<ConnectOutcome>, VclError> {
        let session = match self.pool.get_mut(handle) {
            Ok(session) => session,
            Err(_) => return Ok(None),
        };
        if session.state != VclSessionState::Connecting {
            return Ok(None);
        }
        match result {
            Ok(wire) => Ok(Some(ConnectOutcome::Established { wire })),
            Err(error) => {
                session.state = VclSessionState::Detached;
                session.vpp_error = Some(error);
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
        handle: VclSessionHandle,
        wire: SessionHandle,
        flags: SessionFlags,
        app: Option<AppSession>,
    ) -> Result<(), VclError> {
        {
            let session = self.pool.get_mut(handle)?;
            session.wire_handle = Some(wire);
            // VPP `vcl_session_connected_handler` never touches session
            // flags: the connect-time flags set by CONNECT_STREAM persist
            // and the CONNECTED flags are additive (Hammer's wire carries
            // them; VPP's message does not).
            session.flags = session.flags.union(flags);
            session.initiator = VclInitiator::Local;
            session.vpp_error = None;
            session.state = VclSessionState::Ready;
            session.app = app;
        }
        self.vpp_handles.insert(wire.raw(), handle);
        Ok(())
    }

    /// Applies one ACCEPTED event up to the child allocation (VPP
    /// `vcl_session_accepted_handler`): the listener wire must resolve to a
    /// LISTEN Session, otherwise the event drops without allocation. A full
    /// pool is reported for an ACCEPTED_REPLY error reply.
    pub(crate) fn allocate_peer(
        &mut self,
        listener_wire: u64,
        flags: SessionFlags,
    ) -> Result<PeerOutcome, VclError> {
        let Some(&parent) = self.vpp_handles.get(&listener_wire) else {
            return Ok(PeerOutcome::Drop);
        };
        if !matches!(self.pool.state(parent), Ok(VclSessionState::Listen)) {
            return Ok(PeerOutcome::Drop);
        }
        match self.pool.alloc(VclSession::peer_child(flags)) {
            Ok(handle) => Ok(PeerOutcome::Child { handle, parent }),
            Err(VclError::PoolFull { .. }) => Ok(PeerOutcome::RejectCapacity),
            Err(error) => Err(error),
        }
    }

    /// Completes a peer-open child after its FIFOs were attached (VPP
    /// `vcl_session_accepted_handler` after segment attach): READY with the
    /// ACCEPTED flags, the listener as parent, and the child tracked.
    pub(crate) fn complete_peer(
        &mut self,
        handle: VclSessionHandle,
        accepted: &SessionAcceptedMsg,
        app: Option<AppSession>,
    ) -> Result<(), VclError> {
        let parent = self
            .vpp_handles
            .get(&accepted.listener.raw())
            .copied()
            .ok_or(VclError::InvalidHandle { handle })?;
        {
            let session = self.pool.get_mut(handle)?;
            session.state = VclSessionState::Ready;
            session.wire_handle = Some(accepted.session);
            session.parent = Some(parent);
            session.flags = accepted.flags;
            session.initiator = VclInitiator::Peer;
            session.app = app;
        }
        self.pool.get_mut(parent)?.children.push(handle);
        self.vpp_handles.insert(accepted.session.raw(), handle);
        Ok(())
    }

    /// Closes one Session and cascades to every child exactly once (VPP
    /// `vcl_session_cleanup`). The exactly-once guard is the Session state: a
    /// Session already Closed or Disconnect is a no-op, so re-entry through
    /// any path cannot double-close; every child is attempted even when an
    /// individual step fails. Returns whether the Session was freed.
    pub(crate) fn close_cascade(&mut self, handle: VclSessionHandle) -> Result<bool, VclError> {
        let (parent, wire) = {
            let session = self.pool.get(handle)?;
            match session.state {
                VclSessionState::Closed | VclSessionState::Disconnect => return Ok(false),
                _ => {}
            }
            (session.parent, session.wire_handle)
        };
        let children = {
            let session = self.pool.get(handle)?;
            session.children.clone()
        };
        self.pool.get_mut(handle)?.state = VclSessionState::Disconnect;
        for child in children {
            let _ = self.close_cascade(child);
        }
        if self.pool.free(handle) {
            if let Some(wire) = wire {
                self.vpp_handles.remove(&wire.raw());
            }
            if let Some(parent) = parent {
                if let Ok(session) = self.pool.get_mut(parent) {
                    session.children.retain(|entry| *entry != handle);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

/// Client-local VCL worker (VPP `vcl_worker_t`).
///
/// Owns the Application attach, the fixed-capacity generation-safe local
/// Session pool, and the single control inbox. Single-threaded and
/// client-local: not `Sync`, no locks.
pub struct VclWorker {
    client: AppClient,
    store: SessionStore,
}

impl VclWorker {
    /// Attaches to the Application daemon and creates a worker-local Session
    /// pool of fixed `capacity` slots.
    pub fn attach(path: &str, capacity: usize) -> Result<Self, VclError> {
        let client = AppClient::attach(path).map_err(app_error)?;
        Self::with_client(client, capacity)
    }

    /// Builds a worker over an already-attached client (hammer-vcl MQ
    /// protocol tests construct the client over a real control queue pair).
    pub fn with_client(client: AppClient, capacity: usize) -> Result<Self, VclError> {
        Ok(Self {
            client,
            store: SessionStore::new(capacity)?,
        })
    }

    /// Creates a CLOSED local Session (VPP `vppcom_session_create`). The
    /// `is_nonblocking` attribute selects the behavior of the later
    /// `session_stream_connect` on this Session: blocking waits for the
    /// CONNECTED event; nonblocking returns immediately in `Connecting`.
    pub fn session_create(
        &mut self,
        proto: TransportProtocol,
        is_nonblocking: bool,
    ) -> Result<VclSessionHandle, VclError> {
        self.store.create(proto, is_nonblocking)
    }

    /// Registers a transport listener and returns the local LISTEN Session
    /// (VPP `vppcom_session_listen`). Blocks until the BOUND message; the
    /// wire listener handle is retained to parent ACCEPTED peer children.
    pub fn session_listen(
        &mut self,
        transport: TransportProtocol,
        endpoint: SessionListenEndpoint,
        opaque: Option<u64>,
    ) -> Result<VclSessionHandle, VclError> {
        let listener = self
            .client
            .listen(transport, endpoint, None, opaque)
            .map_err(app_error)?;
        self.store
            .bind_listener(transport, SessionHandle::from(listener.raw()))
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
        child: VclSessionHandle,
        parent: VclSessionHandle,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        flags: SessionFlags,
    ) -> Result<(), VclError> {
        let begin = self.store.begin_stream_connect(child, parent, flags)?;
        // The control context carries the local (slot, generation) identity
        // (VPP `mp->context = s->session_index`): the CONNECTED event
        // resolves directly to this Session and generation-staleness drops.
        self.client
            .connect_stream(
                child.raw(),
                begin.proto,
                remote,
                local,
                None,
                begin.parent_wire,
                flags | SessionFlags::STREAM,
            )
            .map_err(app_error)?;
        if begin.nonblocking {
            return Ok(());
        }
        self.wait_connected(child)
    }

    /// Consumes all currently available Session control events, nonblocking.
    ///
    /// Never blocks and never spins: the inbox is drained once and
    /// asynchronous CONNECTED / ACCEPTED events are applied (Connecting ->
    /// Ready transitions, peer child allocation, ACCEPTED_REPLY).
    pub fn session_poll(&mut self) -> Result<Vec<VclEvent>, VclError> {
        let mut events = Vec::new();
        while let Some(reply) = self.client.poll_control().map_err(app_error)? {
            if let Some(event) = self.process_reply(reply)? {
                events.push(event);
            }
        }
        Ok(events)
    }

    /// Closes a Session; closing a parent cascades to every child exactly
    /// once (VPP `vcl_session_cleanup`). A listener is unlistened first.
    pub fn session_close(&mut self, handle: VclSessionHandle) -> Result<(), VclError> {
        if matches!(self.store.state(handle)?, VclSessionState::Listen) {
            let wire = self
                .store
                .get(handle)?
                .wire_handle
                .ok_or(VclError::SessionNotReady { session: handle })?;
            self.client
                .unlisten(SessionListenerId::from_raw(wire.raw()))
                .map_err(app_error)?;
        }
        self.store.close_cascade(handle)?;
        Ok(())
    }

    /// Sends bytes on an established writable Session. A direction-invalid
    /// operation returns a typed error and mutates nothing.
    pub fn session_send(
        &mut self,
        handle: VclSessionHandle,
        bytes: &[u8],
    ) -> Result<usize, VclError> {
        let session = self.store.get(handle)?;
        if !session.attributes().writable() {
            return Err(VclError::DirectionInvalid {
                session: handle,
                direction: VclDirection::Write,
            });
        }
        let app = session
            .app
            .as_ref()
            .ok_or(VclError::SessionNotReady { session: handle })?;
        app.send_bytes(bytes)
            .map_err(|source| VclError::AppSession { source })
    }

    /// Receives bytes from an established readable Session (VPP
    /// `vppcom_session_read_internal`: peek then dequeue-drop).
    pub fn session_recv(
        &mut self,
        handle: VclSessionHandle,
        out: &mut [u8],
    ) -> Result<usize, VclError> {
        let session = self.store.get(handle)?;
        if !session.attributes().readable() {
            return Err(VclError::DirectionInvalid {
                session: handle,
                direction: VclDirection::Read,
            });
        }
        let app = session
            .app
            .as_ref()
            .ok_or(VclError::SessionNotReady { session: handle })?;
        let read = app.recv_bytes(out);
        let _ = app.consume_rx(read);
        Ok(read)
    }

    /// Current local state of one Session; a stale handle is a typed error.
    pub fn session_state(&self, handle: VclSessionHandle) -> Result<VclSessionState, VclError> {
        self.store.state(handle)
    }

    /// Derived attributes and capabilities of one Session.
    pub fn session_attributes(
        &self,
        handle: VclSessionHandle,
    ) -> Result<VclSessionAttributes, VclError> {
        self.store.attributes(handle)
    }

    /// Blocking completion of one active open: drain the single control
    /// inbox until the CONNECTED event resolves `child`.
    fn wait_connected(&mut self, child: VclSessionHandle) -> Result<(), VclError> {
        loop {
            if let Some(reply) = self.client.poll_control().map_err(app_error)? {
                self.process_reply(reply)?;
            } else {
                self.client.wait_control().map_err(app_error)?;
            }
            match self.store.state(child)? {
                VclSessionState::Ready => return Ok(()),
                VclSessionState::Detached => return Err(self.connect_failure(child)),
                VclSessionState::Closed => {
                    return Err(VclError::SessionNotReady { session: child });
                }
                _ => {}
            }
        }
    }

    /// The retained connect error of a Detached Session (VPP `vpp_error`).
    fn connect_failure(&self, child: VclSessionHandle) -> VclError {
        let error = self
            .store
            .get(child)
            .ok()
            .and_then(|session| session.vpp_error);
        match error {
            Some(error) => VclError::ConnectFailed {
                session: child,
                error,
            },
            None => VclError::DetachedWithoutError { session: child },
        }
    }

    /// Applies one buffered control reply. Returns the event the reply
    /// produced, if any; stale or mismatched wire events are dropped.
    fn process_reply(&mut self, reply: ControlReply) -> Result<Option<VclEvent>, VclError> {
        let kind = reply.kind();
        match reply {
            ControlReply::Connected(connected) => self.process_connected(connected),
            ControlReply::Accepted(accepted) => self.process_accepted(accepted),
            ControlReply::Bound(_) | ControlReply::Unlisten(_) => {
                Err(VclError::UnexpectedReply { kind })
            }
        }
    }

    /// VPP `vcl_session_connected_handler`: the CONNECTED context selects the
    /// local Session; only a Connecting Session transitions (to Ready, or to
    /// Detached retaining `vpp_error`). Anything else drops without
    /// allocation, callback, or state mutation.
    fn process_connected(
        &mut self,
        connected: SessionConnectedMsg,
    ) -> Result<Option<VclEvent>, VclError> {
        let handle = VclSessionHandle::from_raw(connected.context);
        let Some(outcome) = self.store.accept_connected(handle, connected.result)? else {
            return Ok(None);
        };
        match outcome {
            ConnectOutcome::Established { wire } => {
                let app = self
                    .client
                    .accept_with_handle(Some(wire))
                    .map_err(app_error)?;
                self.store
                    .complete_connected(handle, wire, connected.flags, Some(app))?;
                Ok(Some(VclEvent::Connected { session: handle }))
            }
            ConnectOutcome::Failed { .. } => Ok(None),
        }
    }

    /// VPP `vcl_session_accepted_handler`: allocate a peer-open child, attach
    /// its FIFOs, transition it to Ready, and send ACCEPTED_REPLY. An
    /// unknown or non-listening parent, or a stale/mismatched publication,
    /// drops without allocation, callback, or state mutation.
    fn process_accepted(
        &mut self,
        accepted: SessionAcceptedMsg,
    ) -> Result<Option<VclEvent>, VclError> {
        let (child, parent) = match self
            .store
            .allocate_peer(accepted.listener.raw(), accepted.flags)?
        {
            PeerOutcome::Drop => return Ok(None),
            PeerOutcome::RejectCapacity => {
                // VPP replies an error on the ACCEPTED_REPLY; no child is
                // allocated and no local state changes.
                let reply = SessionAcceptedReplyMsg::new(
                    self.client.application().raw(),
                    accepted.session,
                    Err(SessionControlError::CapacityExhausted),
                );
                self.client.accepted_reply(&reply).map_err(app_error)?;
                return Ok(None);
            }
            PeerOutcome::Child { handle, parent } => (handle, parent),
        };
        let app = match self.client.accept_with_handle(Some(accepted.session)) {
            Ok(app) => app,
            Err(AppClientError::SessionHandleMismatch { .. }) => {
                // Stale/mismatched ACCEPTED: the published descriptors
                // belong to a newer Session; drop the child.
                self.store.pool.free(child);
                return Ok(None);
            }
            Err(error) => return Err(app_error(error)),
        };
        self.store.complete_peer(child, &accepted, Some(app))?;
        let reply =
            SessionAcceptedReplyMsg::new(self.client.application().raw(), accepted.session, Ok(()));
        self.client.accepted_reply(&reply).map_err(app_error)?;
        Ok(Some(VclEvent::Accepted {
            session: child,
            parent,
        }))
    }
}

#[cfg(test)]
mod tests {
    use hammer_runtime::app::SessionAcceptedMsg;

    use super::*;

    const TCP: TransportProtocol = TransportProtocol::Tcp;

    fn wire(slot: u32) -> SessionHandle {
        SessionHandle::new(slot, 0)
    }

    fn store() -> SessionStore {
        SessionStore::new(8).expect("store")
    }

    /// A Ready parent carrying `wire` and `flags`, for stream-connect and
    /// cascade fixtures (the wire belongs to a daemon-created Session).
    fn seed_ready(
        store: &mut SessionStore,
        proto: TransportProtocol,
        wire: SessionHandle,
        flags: SessionFlags,
    ) -> VclSessionHandle {
        store.seed_ready(proto, wire, flags).expect("seed Ready")
    }

    fn seed_connecting(
        store: &mut SessionStore,
        parent_wire: SessionHandle,
    ) -> (VclSessionHandle, VclSessionHandle) {
        let parent = seed_ready(store, TCP, parent_wire, SessionFlags::empty());
        let child = store.create(TCP, true).expect("create child");
        store
            .begin_stream_connect(child, parent, SessionFlags::empty())
            .expect("begin stream connect");
        (parent, child)
    }

    #[test]
    fn begin_stream_connect_rejects_self_parent() {
        let mut store = store();
        let child = store.create(TCP, false).expect("create");
        let error = store
            .begin_stream_connect(child, child, SessionFlags::empty())
            .expect_err("self parent");
        assert!(matches!(error, VclError::SelfParent { session } if session == child));
    }

    #[test]
    fn begin_stream_connect_rejects_unestablished_parent() {
        let mut store = store();
        let parent = store.create(TCP, false).expect("create parent");
        let child = store.create(TCP, false).expect("create child");
        let error = store
            .begin_stream_connect(child, parent, SessionFlags::empty())
            .expect_err("unestablished parent");
        assert!(matches!(
            error,
            VclError::ParentNotEstablished { parent: p } if p == parent
        ));
    }

    #[test]
    fn begin_stream_connect_rejects_non_ready_parent() {
        let mut store = store();
        let parent = store.create(TCP, false).expect("create parent");
        store.pool.get_mut(parent).expect("parent").wire_handle = Some(wire(5));
        let child = store.create(TCP, false).expect("create child");
        let error = store
            .begin_stream_connect(child, parent, SessionFlags::empty())
            .expect_err("non-ready parent");
        assert!(matches!(
            error,
            VclError::ParentNotReady { parent: p, state } if p == parent && state == VclSessionState::Closed
        ));
    }

    #[test]
    fn begin_stream_connect_rejects_non_closed_child() {
        let mut store = store();
        let parent = seed_ready(&mut store, TCP, wire(1), SessionFlags::empty());
        let child = seed_ready(&mut store, TCP, wire(2), SessionFlags::empty());
        let error = store
            .begin_stream_connect(child, parent, SessionFlags::empty())
            .expect_err("non-closed child");
        assert!(matches!(
            error,
            VclError::NotConnectable { session, state } if session == child && state == VclSessionState::Ready
        ));
    }

    #[test]
    fn begin_stream_connect_marks_child_connecting_and_tracks_it() {
        let mut store = store();
        let parent_wire = wire(1);
        let parent = seed_ready(&mut store, TCP, parent_wire, SessionFlags::empty());
        let child = store.create(TCP, true).expect("create child");
        let begin = store
            .begin_stream_connect(child, parent, SessionFlags::UNIDIRECTIONAL)
            .expect("begin stream connect");
        assert_eq!(begin.parent_wire, parent_wire);
        assert_eq!(begin.proto, TCP);
        assert!(begin.nonblocking);
        let session = store.pool.get(child).expect("child");
        assert_eq!(session.state, VclSessionState::Connecting);
        assert_eq!(session.parent, Some(parent));
        assert_eq!(session.initiator, VclInitiator::Local);
        assert!(session.flags.contains(SessionFlags::STREAM));
        assert!(session.flags.contains(SessionFlags::UNIDIRECTIONAL));
        // The child is tracked so the parent cascade closes it exactly once.
        let parent_session = store.pool.get(parent).expect("parent");
        assert!(parent_session.children.contains(&child));
    }

    #[test]
    fn accept_connected_drops_stale_handle() {
        let mut store = store();
        let (_, child) = seed_connecting(&mut store, wire(1));
        let freed = store.close_cascade(child).expect("close child");
        assert!(freed);
        assert_eq!(
            store.accept_connected(child, Ok(wire(9))).expect("stale"),
            None
        );
        // An out-of-range handle is equally a drop.
        let stale = VclSessionHandle::new(63, 1);
        assert_eq!(
            store.accept_connected(stale, Ok(wire(9))).expect("stale"),
            None
        );
    }

    #[test]
    fn accept_connected_drops_non_connecting_session() {
        let mut store = store();
        let parent = seed_ready(&mut store, TCP, wire(1), SessionFlags::empty());
        assert_eq!(
            store.accept_connected(parent, Ok(wire(9))).expect("ready"),
            None
        );
        let session = store.pool.get(parent).expect("parent");
        assert_eq!(session.state, VclSessionState::Ready);
    }

    #[test]
    fn accept_connected_failure_detaches_with_retained_error() {
        let mut store = store();
        let (_, child) = seed_connecting(&mut store, wire(1));
        let outcome = store
            .accept_connected(child, Err(SessionConnectError::TimedOut))
            .expect("failure");
        assert!(matches!(
            outcome,
            Some(ConnectOutcome::Failed {
                error: SessionConnectError::TimedOut
            })
        ));
        let session = store.pool.get(child).expect("child");
        assert_eq!(session.state, VclSessionState::Detached);
        assert_eq!(session.vpp_error, Some(SessionConnectError::TimedOut));
        assert_eq!(session.wire_handle, None);
    }

    #[test]
    fn accept_connected_establishes_wire_then_complete_readies() {
        let mut store = store();
        let (_, child) = seed_connecting(&mut store, wire(1));
        let outcome = store
            .accept_connected(child, Ok(wire(7)))
            .expect("established");
        assert!(matches!(
            outcome,
            Some(ConnectOutcome::Established { wire: w }) if w == wire(7)
        ));
        store
            .complete_connected(child, wire(7), SessionFlags::UNIDIRECTIONAL, None)
            .expect("complete");
        let session = store.pool.get(child).expect("child");
        assert_eq!(session.state, VclSessionState::Ready);
        assert_eq!(session.wire_handle, Some(wire(7)));
        // Established-session flags preservation: the connect-time STREAM
        // flag persists (VPP CONNECTED never touches flags) and the
        // CONNECTED flags are additive; the initiator stays local.
        assert_eq!(
            session.flags,
            SessionFlags::STREAM.union(SessionFlags::UNIDIRECTIONAL)
        );
        assert_eq!(session.initiator, VclInitiator::Local);
        assert!(store.wire_handle(wire(7)) == Some(child));
    }

    #[test]
    fn allocate_peer_drops_unknown_listener() {
        let mut store = store();
        let outcome = store
            .allocate_peer(wire(99).raw(), SessionFlags::empty())
            .expect("unknown listener");
        assert!(matches!(outcome, PeerOutcome::Drop));
    }

    #[test]
    fn allocate_peer_drops_non_listening_session() {
        let mut store = store();
        seed_ready(&mut store, TCP, wire(1), SessionFlags::empty());
        let outcome = store
            .allocate_peer(wire(1).raw(), SessionFlags::empty())
            .expect("non-listening parent");
        assert!(matches!(outcome, PeerOutcome::Drop));
    }

    #[test]
    fn allocate_peer_rejects_capacity() {
        let mut store = SessionStore::new(1).expect("store");
        let listener = store.bind_listener(TCP, wire(1)).expect("listener");
        let outcome = store
            .allocate_peer(wire(1).raw(), SessionFlags::empty())
            .expect("capacity");
        assert!(matches!(outcome, PeerOutcome::RejectCapacity));
        assert!(matches!(
            store.pool.state(listener),
            Ok(VclSessionState::Listen)
        ));
    }

    #[test]
    fn allocate_peer_allocates_child_and_complete_readies_peer() {
        let mut store = store();
        let listener = store.bind_listener(TCP, wire(1)).expect("listener");
        let outcome = store
            .allocate_peer(wire(1).raw(), SessionFlags::UNIDIRECTIONAL)
            .expect("alloc");
        let (child, parent) = match outcome {
            PeerOutcome::Child { handle, parent } => (handle, parent),
            _ => panic!("expected child allocation"),
        };
        assert_eq!(parent, listener);
        store
            .complete_peer(
                child,
                &SessionAcceptedMsg {
                    context: 0,
                    listener: wire(1),
                    session: wire(2),
                    flags: SessionFlags::UNIDIRECTIONAL,
                    local: None,
                    remote: None,
                    opaque: None,
                },
                None,
            )
            .expect("complete peer");
        let session = store.pool.get(child).expect("child");
        assert_eq!(session.state, VclSessionState::Ready);
        assert_eq!(session.wire_handle, Some(wire(2)));
        assert_eq!(session.parent, Some(listener));
        assert_eq!(session.initiator, VclInitiator::Peer);
        assert!(session.flags.contains(SessionFlags::UNIDIRECTIONAL));
        // Peer-initiated unidirectional: readable, not writable.
        let attributes = session.attributes();
        assert!(attributes.readable());
        assert!(!attributes.writable());
        assert!(store.wire_handle(wire(2)) == Some(child));
        let listener_session = store.pool.get(listener).expect("listener");
        assert!(listener_session.children.contains(&child));
    }

    #[test]
    fn close_cascade_closes_children_exactly_once() {
        let mut store = store();
        let listener = store.bind_listener(TCP, wire(1)).expect("listener");
        let first = store
            .allocate_peer(wire(1).raw(), SessionFlags::empty())
            .expect("alloc");
        let second = store
            .allocate_peer(wire(1).raw(), SessionFlags::empty())
            .expect("alloc");
        let first = match first {
            PeerOutcome::Child { handle, .. } => handle,
            _ => panic!("expected child"),
        };
        let second = match second {
            PeerOutcome::Child { handle, .. } => handle,
            _ => panic!("expected child"),
        };
        for child in [first, second] {
            store
                .complete_peer(
                    child,
                    &SessionAcceptedMsg {
                        context: 0,
                        listener: wire(1),
                        session: wire(child.slot() as u32),
                        flags: SessionFlags::empty(),
                        local: None,
                        remote: None,
                        opaque: None,
                    },
                    None,
                )
                .expect("complete peer");
        }
        assert!(store.close_cascade(listener).expect("close listener"));
        for child in [first, second] {
            assert!(matches!(
                store.pool.state(child),
                Err(VclError::InvalidHandle { handle: h }) if h == child
            ));
        }
        assert!(matches!(
            store.pool.state(listener),
            Err(VclError::InvalidHandle { handle: h }) if h == listener
        ));
        // Exactly once: closing again on the freed handle is a stale-handle
        // error, never a double free.
        assert!(matches!(
            store.pool.state(listener),
            Err(VclError::InvalidHandle { handle: h }) if h == listener
        ));
    }

    #[test]
    fn close_cascade_removes_stream_child_from_parent() {
        let mut store = store();
        let (parent, child) = seed_connecting(&mut store, wire(1));
        store
            .complete_connected(child, wire(2), SessionFlags::empty(), None)
            .expect("complete child");
        assert!(store.close_cascade(child).expect("close child"));
        let parent_session = store.pool.get(parent).expect("parent");
        assert!(!parent_session.children.contains(&child));
        assert!(matches!(
            store.pool.state(child),
            Err(VclError::InvalidHandle { handle: h }) if h == child
        ));
        assert!(store.close_cascade(parent).expect("close parent"));
    }
}

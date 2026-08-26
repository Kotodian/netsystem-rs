use std::net::SocketAddr;

use hammer_runtime::app::{
    ApplicationConnectionId, SessionAcceptedMsg, SessionAcceptedReplyMsg, SessionAppId,
    SessionBoundMsg, SessionConnectMsg, SessionConnectedMsg, SessionControlItem,
    SessionControlPayload, SessionEvtType, SessionFlags, SessionHandle, SessionListenMsg,
    SessionUnlistenMsg, SessionUnlistenReplyMsg, TransportProtocol,
};
use hammer_runtime::SessionListenEndpoint;

use crate::attach::{AppClient, AppClientError, ControlReply, ControlReplyKind};

impl AppClient {
    /// Registers a transport listener (VPP `session_listen_msg_t`).
    ///
    /// The request carries the stable [`TransportProtocol`] and a
    /// Session-owned listen endpoint; the Session control plane selects the
    /// transport and owning worker. Blocks until the BOUND message.
    pub fn listen(
        &mut self,
        transport: TransportProtocol,
        endpoint: SessionListenEndpoint,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
    ) -> Result<SessionHandle, AppClientError> {
        let context = self.next_context();
        let request = SessionListenMsg {
            context,
            transport,
            endpoint,
            application: self.application(),
            app,
            flags: SessionFlags::empty(),
            opaque,
        };
        self.session_requests
            .borrow_mut()
            .enqueue_control(&request)
            .map_err(|source| AppClientError::SessionControl { source })?;
        loop {
            if let Some(reply) = self.take_bound(context) {
                return reply
                    .result
                    .map_err(|error| AppClientError::SessionRejected { error });
            }
            if !self.receive_control_event()? {
                self.session_replies
                    .borrow()
                    .wait()
                    .map_err(|source| AppClientError::SessionReplyWait { source })?;
            }
        }
    }

    /// Opens one ordinary transport connection (VPP `session_connect_msg_t`).
    ///
    /// The Session owns the data-worker choice, so no external worker
    /// identity is accepted; the returned [`ApplicationConnectionId`] selects
    /// the CONNECTED message on [`Self::wait_connection`].
    /// Starts an active-open connect. `server_name` (SNI / QUIC-TLS server
    /// name) is stored in one bounded ext-config chunk in the shared
    /// Application segment and carried on the fixed CONNECT message as an
    /// opaque reference only (VPP `session_connect_msg_t.ext_config`); the
    /// daemon reads, validates, and frees the chunk exactly once.
    pub fn connect(
        &mut self,
        transport: TransportProtocol,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
        server_name: Option<&str>,
    ) -> Result<ApplicationConnectionId, AppClientError> {
        let context = self.next_context();
        let connection = ApplicationConnectionId::from_raw(context as u32);
        let mut request = SessionConnectMsg::connect(
            context,
            transport,
            remote,
            local,
            self.application(),
            opaque,
        );
        request.app = app;
        if let Some(server_name) = server_name {
            let store = self
                .ext_config
                .as_ref()
                .ok_or(AppClientError::ExtConfigStoreMissing)?;
            let offset = store
                .alloc(server_name.as_bytes())
                .map_err(|source| AppClientError::ExtConfig { source })?;
            request.ext_config = Some(offset);
        }
        if let Err(source) = self.session_requests.borrow_mut().enqueue_control(&request) {
            // The daemon never saw the request, so the chunk is still owned
            // by this client: return it to the free list instead of leaking
            // the bounded allocation (the daemon frees it exactly once after
            // a successful read).
            if let Some(offset) = request.ext_config
                && let Some(store) = self.ext_config.as_ref()
            {
                // A failed free cannot be reported further: the enqueue
                // failure is the primary error, and the chunk stays
                // allocated (bounded) rather than being leaked twice.
                let _ = store.free(offset);
            }
            return Err(AppClientError::SessionControl { source });
        }
        Ok(connection)
    }

    /// Opens one stream on an established parent Session (VPP CONNECT_STREAM,
    /// `vcl_send_session_connect_stream`, vppcom.c:112).
    ///
    /// Nonblocking: this method only enqueues the request and returns the
    /// connection identity; the asynchronous CONNECTED message is consumed
    /// later through [`Self::poll_control`] (nonblocking) or
    /// [`Self::wait_control`] (blocking wait on the same inbox).
    ///
    /// `context` selects the CONNECTED message and must be unique among
    /// in-flight connects: it mirrors VPP's `mp->context = s->session_index`
    /// and is chosen by the caller (hammer-vcl passes its local (slot,
    /// generation) Session identity).
    #[allow(clippy::too_many_arguments)]
    pub fn connect_stream(
        &mut self,
        context: u64,
        transport: TransportProtocol,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        opaque: Option<u64>,
        parent: SessionHandle,
        flags: SessionFlags,
    ) -> Result<ApplicationConnectionId, AppClientError> {
        let request = SessionConnectMsg::connect_stream(
            context,
            transport,
            remote,
            local,
            self.application(),
            parent,
            flags,
            opaque,
        );
        self.session_requests
            .borrow_mut()
            .enqueue_control(&request)
            .map_err(|source| AppClientError::SessionControl { source })?;
        Ok(ApplicationConnectionId::from_raw(context as u32))
    }

    /// Returns the next buffered Session control reply, or `None` when the
    /// single inbox is empty. Drains the buffered inbox first; the reply
    /// queue is only read when no reply is buffered, so a reply already
    /// consumed by a blocking flow (e.g. an ACCEPTED buffered while `listen`
    /// waited for BOUND) is never skipped. Never blocks and never spins:
    /// reply readiness is not polled here.
    pub fn poll_control(&self) -> Result<Option<ControlReply>, AppClientError> {
        if self.pending_replies.borrow().is_empty() {
            self.receive_control_event()?;
        }
        Ok(self.pending_replies.borrow_mut().pop_front())
    }

    /// Blocks until a Session control reply is signaled on the reply queue.
    /// Pairs with [`Self::poll_control`] for blocking flows over the same
    /// single inbox.
    pub fn wait_control(&self) -> Result<(), AppClientError> {
        self.session_replies
            .borrow()
            .wait()
            .map_err(|source| AppClientError::SessionReplyWait { source })
    }

    /// Enqueues one ACCEPTED_REPLY acknowledgment (VPP
    /// `session_accepted_reply_msg_t`).
    pub fn accepted_reply(&self, reply: &SessionAcceptedReplyMsg) -> Result<(), AppClientError> {
        self.session_requests
            .borrow_mut()
            .enqueue_control(reply)
            .map_err(|source| AppClientError::SessionControl { source })
    }

    /// Removes a listener (VPP `session_unlisten_msg_t`). Blocks until the
    /// UNLISTEN_REPLY message.
    pub fn unlisten(&mut self, listener: SessionHandle) -> Result<(), AppClientError> {
        let context = self.next_context();
        let request = SessionUnlistenMsg { context, listener };
        self.session_requests
            .borrow_mut()
            .enqueue_control(&request)
            .map_err(|source| AppClientError::SessionControl { source })?;
        loop {
            if let Some(reply) = self.take_unlisten(context) {
                return reply
                    .result
                    .map_err(|error| AppClientError::SessionRejected { error });
            }
            if !self.receive_control_event()? {
                self.session_replies
                    .borrow()
                    .wait()
                    .map_err(|source| AppClientError::SessionReplyWait { source })?;
            }
        }
    }

    /// Waits for the CONNECTED message of one active-open
    /// ([`Self::connect`]) and returns the established Session.
    pub fn wait_connection(
        &self,
        connection: ApplicationConnectionId,
    ) -> Result<crate::AppSession, AppClientError> {
        loop {
            if let Some(reply) = self.take_connection_reply(connection) {
                return self.finish_connection(connection, reply);
            }
            if !self.receive_control_event()? {
                self.session_replies
                    .borrow()
                    .wait()
                    .map_err(|source| AppClientError::SessionReplyWait { source })?;
            }
        }
    }

    /// Accepts one published accepted Session and acknowledges it
    /// (VPP `session_accepted_reply_msg_t`).
    ///
    /// The Session descriptors arrive on the attach stream first; the ACCEPTED
    /// message follows on the reply control queue. Once it matches the
    /// received Session handle, ACCEPTED_REPLY is sent so the service
    /// transitions the Session to the ready state.
    pub fn accept_accepted(&self) -> Result<crate::AppSession, AppClientError> {
        let session = self.accept()?;
        let handle = session.session_handle();
        loop {
            if self.take_accepted(handle).is_some() {
                break;
            }
            if !self.receive_control_event()? {
                self.session_replies
                    .borrow()
                    .wait()
                    .map_err(|source| AppClientError::SessionReplyWait { source })?;
            }
        }
        let reply = SessionAcceptedReplyMsg::new(u64::from(self.application().raw()), handle, Ok(()));
        self.accepted_reply(&reply)?;
        Ok(session)
    }

    fn next_context(&mut self) -> u64 {
        let context = self.next_session_context;
        self.next_session_context = self.next_session_context.wrapping_add(1);
        context
    }

    fn finish_connection(
        &self,
        connection: ApplicationConnectionId,
        connected: SessionConnectedMsg,
    ) -> Result<crate::AppSession, AppClientError> {
        match connected.result {
            Ok(session) => self.accept_with_handle(Some(session)),
            Err(error) => Err(AppClientError::SessionConnectFailed { connection, error }),
        }
    }

    /// Drains one control-slot message from the reply queue into the
    /// client's typed reply inbox. Returns true when a message was
    /// buffered.
    fn receive_control_event(&self) -> Result<bool, AppClientError> {
        let mut replies = self.session_replies.borrow_mut();
        let Some(item) = replies
            .dequeue_control()
            .map_err(|source| AppClientError::SessionControl { source })?
        else {
            return Ok(false);
        };
        let event = item.event_type();
        let reply = match event {
            SessionEvtType::Bound => ControlReply::Bound(Self::decode_reply(&item)?),
            SessionEvtType::UnlistenReply => ControlReply::Unlisten(Self::decode_reply(&item)?),
            SessionEvtType::Connected => ControlReply::Connected(Self::decode_reply(&item)?),
            SessionEvtType::Accepted => ControlReply::Accepted(Self::decode_reply(&item)?),
            event => return Err(AppClientError::UnexpectedSessionEvent { event }),
        };
        self.pending_replies.borrow_mut().push_back(reply);
        Ok(true)
    }

    fn decode_reply<M: SessionControlPayload>(
        item: &SessionControlItem<'_>,
    ) -> Result<M, AppClientError> {
        item.decode::<M>()
            .ok_or(AppClientError::UnexpectedSessionEvent {
                event: item.event_type(),
            })?
            .map_err(|source| AppClientError::SessionControlDecode { source })
    }

    /// Consumes the first buffered message of `kind` carrying `context`.
    fn take_reply(&self, kind: ControlReplyKind, context: u64) -> Option<ControlReply> {
        let mut replies = self.pending_replies.borrow_mut();
        let position = replies
            .iter()
            .position(|reply| reply.kind() == kind && reply.context() == context)?;
        replies.remove(position)
    }

    fn take_bound(&self, context: u64) -> Option<SessionBoundMsg> {
        match self.take_reply(ControlReplyKind::Bound, context) {
            Some(ControlReply::Bound(reply)) => Some(reply),
            _ => None,
        }
    }

    fn take_unlisten(&self, context: u64) -> Option<SessionUnlistenReplyMsg> {
        match self.take_reply(ControlReplyKind::Unlisten, context) {
            Some(ControlReply::Unlisten(reply)) => Some(reply),
            _ => None,
        }
    }

    fn take_connection_reply(
        &self,
        connection: ApplicationConnectionId,
    ) -> Option<SessionConnectedMsg> {
        match self.take_reply(ControlReplyKind::Connected, u64::from(connection.raw())) {
            Some(ControlReply::Connected(reply)) => Some(reply),
            _ => None,
        }
    }

    fn take_accepted(&self, handle: SessionHandle) -> Option<SessionAcceptedMsg> {
        let mut replies = self.pending_replies.borrow_mut();
        let position = replies.iter().position(
            |reply| matches!(reply, ControlReply::Accepted(accepted) if accepted.session == handle),
        )?;
        match replies.remove(position) {
            Some(ControlReply::Accepted(reply)) => Some(reply),
            _ => None,
        }
    }
}

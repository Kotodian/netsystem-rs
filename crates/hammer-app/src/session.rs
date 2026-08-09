use std::net::SocketAddr;

use hammer_runtime::app::{
    ApplicationConnectionId, ApplicationSessionReply, ApplicationSessionRequest,
    ApplicationSessionStatus, SessionAppId, dequeue_application_session_reply,
    enqueue_application_session_request,
};
use hammer_runtime::{DataWorkerId, SessionListenEndpoint, SessionListenerId};

use crate::attach::{AppClient, AppClientError};

impl AppClient {
    pub fn listen(
        &mut self,
        transport: impl Into<String>,
        endpoint: SessionListenEndpoint,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
    ) -> Result<SessionListenerId, AppClientError> {
        let context = self.next_session_context();
        let reply = self.session_request(ApplicationSessionRequest::Listen {
            context,
            transport: transport.into(),
            endpoint,
            app,
            opaque,
        })?;
        Ok(reply.listener())
    }

    pub fn connect(
        &mut self,
        transport: impl Into<String>,
        remote: SocketAddr,
        local: Option<SocketAddr>,
        worker: DataWorkerId,
        server_name: Option<String>,
        app: Option<SessionAppId>,
        opaque: Option<u64>,
    ) -> Result<ApplicationConnectionId, AppClientError> {
        let context = self.next_session_context();
        let reply = self.session_request(ApplicationSessionRequest::Connect {
            context,
            transport: transport.into(),
            remote,
            local,
            worker,
            server_name,
            app,
            opaque,
        })?;
        Ok(reply.connection())
    }

    pub fn unlisten(&mut self, listener: SessionListenerId) -> Result<(), AppClientError> {
        let context = self.next_session_context();
        self.session_request(ApplicationSessionRequest::Unlisten { context, listener })?;
        Ok(())
    }

    pub fn wait_connection(
        &self,
        connection: ApplicationConnectionId,
    ) -> Result<crate::AppSession, AppClientError> {
        loop {
            if let Some(reply) = self.take_connection_reply(connection) {
                return match reply {
                    ApplicationSessionReply::Connected { session, .. } => {
                        self.accept_with_handle(Some(session))
                    }
                    ApplicationSessionReply::ConnectFailed { status, .. } => {
                        Err(AppClientError::SessionConnectFailed { connection, status })
                    }
                    ApplicationSessionReply::Response { .. } => unreachable!(
                        "connection reply cache only stores asynchronous connection completions"
                    ),
                };
            }
            let Some(reply) = dequeue_application_session_reply(&self.session_replies)
                .map_err(|source| AppClientError::SessionControl { source })?
            else {
                self.session_replies
                    .wait()
                    .map_err(|source| AppClientError::SessionReplyWait { source })?;
                continue;
            };
            if matches!(
                reply,
                ApplicationSessionReply::Connected {
                    connection: candidate,
                    ..
                }
                | ApplicationSessionReply::ConnectFailed {
                    connection: candidate,
                    ..
                } if candidate == connection
            ) {
                return match reply {
                    ApplicationSessionReply::Connected { session, .. } => {
                        self.accept_with_handle(Some(session))
                    }
                    ApplicationSessionReply::ConnectFailed { status, .. } => {
                        Err(AppClientError::SessionConnectFailed { connection, status })
                    }
                    ApplicationSessionReply::Response { .. } => unreachable!(),
                };
            }
            self.pending_replies.borrow_mut().push_back(reply);
        }
    }

    fn next_session_context(&mut self) -> u64 {
        let context = self.next_session_context;
        self.next_session_context = self.next_session_context.wrapping_add(1);
        context
    }

    fn session_request(
        &self,
        request: ApplicationSessionRequest,
    ) -> Result<hammer_runtime::app::ApplicationSessionReply, AppClientError> {
        let context = request.context();
        enqueue_application_session_request(&self.session_requests, &request)
            .map_err(|source| AppClientError::SessionControl { source })?;
        loop {
            if let Some(reply) = self.take_response(context) {
                return self.accept_response(reply);
            }
            if let Some(reply) = dequeue_application_session_reply(&self.session_replies)
                .map_err(|source| AppClientError::SessionControl { source })?
            {
                if matches!(reply, ApplicationSessionReply::Response { context: actual, .. } if actual == context)
                {
                    return self.accept_response(reply);
                }
                self.pending_replies.borrow_mut().push_back(reply);
            } else {
                self.session_replies
                    .wait()
                    .map_err(|source| AppClientError::SessionReplyWait { source })?;
            }
        }
    }

    fn accept_response(
        &self,
        reply: ApplicationSessionReply,
    ) -> Result<ApplicationSessionReply, AppClientError> {
        if reply.status() != ApplicationSessionStatus::Success {
            return Err(AppClientError::SessionRejected {
                status: reply.status(),
            });
        }
        Ok(reply)
    }

    fn take_response(&self, context: u64) -> Option<ApplicationSessionReply> {
        let mut replies = self.pending_replies.borrow_mut();
        let position = replies.iter().position(|reply| {
            matches!(
                reply,
                ApplicationSessionReply::Response {
                    context: actual, ..
                } if *actual == context
            )
        })?;
        replies.remove(position)
    }

    fn take_connection_reply(
        &self,
        connection: ApplicationConnectionId,
    ) -> Option<ApplicationSessionReply> {
        let mut replies = self.pending_replies.borrow_mut();
        let position = replies.iter().position(|reply| {
            matches!(
                reply,
                ApplicationSessionReply::Connected {
                    connection: candidate, ..
                }
                | ApplicationSessionReply::ConnectFailed {
                    connection: candidate, ..
                } if *candidate == connection
            )
        })?;
        replies.remove(position)
    }
}

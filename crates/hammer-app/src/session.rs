use std::net::SocketAddr;

use hammer_runtime::app::{
    ApplicationConnectionId, ApplicationSessionRequest, ApplicationSessionStatus, SessionAppId,
    dequeue_application_session_reply, enqueue_application_session_request,
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
            if let Some(reply) = dequeue_application_session_reply(&self.session_replies)
                .map_err(|source| AppClientError::SessionControl { source })?
            {
                if reply.context() != context {
                    return Err(AppClientError::SessionReplyContext {
                        expected: context,
                        actual: reply.context(),
                    });
                }
                if reply.status() != ApplicationSessionStatus::Success {
                    return Err(AppClientError::SessionRejected {
                        status: reply.status(),
                    });
                }
                return Ok(reply);
            }
            self.session_replies
                .wait()
                .map_err(|source| AppClientError::SessionReplyWait { source })?;
        }
    }
}

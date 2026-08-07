use hammer_runtime::app::{
    ApplicationId, ApplicationSessionReply, ApplicationSessionRequest, ApplicationSessionStatus,
    SessionMsgQueue, dequeue_application_session_request, enqueue_application_session_reply,
};
use hammer_runtime::plugin::PluginError;
use hammer_runtime::{
    Engine, RuntimeError, RuntimeResult, SessionConnectEndpoint, SessionConnectionId,
};

use super::application::ApplicationError;
use super::runtime::SessionMain;

impl SessionMain {
    pub fn dispatch_application_session_mq(
        &self,
        application: ApplicationId,
        requests: &SessionMsgQueue,
        replies: &SessionMsgQueue,
    ) -> RuntimeResult<()> {
        while let Some(request) =
            dequeue_application_session_request(requests).map_err(RuntimeError::from)?
        {
            let reply = self.application_session_request(application, request);
            enqueue_application_session_reply(replies, &reply).map_err(RuntimeError::from)?;
        }
        Ok(())
    }

    fn application_session_request(
        &self,
        application: ApplicationId,
        request: ApplicationSessionRequest,
    ) -> ApplicationSessionReply {
        let context = request.context();
        let result = match request {
            ApplicationSessionRequest::Listen {
                transport,
                endpoint,
                app,
                opaque,
                ..
            } => self.application_listen(application, &transport, endpoint, app, opaque),
            ApplicationSessionRequest::Connect {
                transport,
                remote,
                local,
                worker,
                server_name,
                app,
                opaque,
                ..
            } => self.application_connect(
                application,
                &transport,
                remote,
                local,
                worker,
                server_name,
                app,
                opaque,
            ),
            ApplicationSessionRequest::Unlisten { listener, .. } => {
                self.application_unlisten(application, listener).map(|()| 0)
            }
        };
        match result {
            Ok(handle) => ApplicationSessionReply::success(context, handle),
            Err(status) => ApplicationSessionReply::rejected(context, status),
        }
    }

    fn application_listen(
        &self,
        application: ApplicationId,
        transport_name: &str,
        endpoint: hammer_runtime::SessionListenEndpoint,
        app: Option<hammer_runtime::app::SessionAppId>,
        opaque: Option<u64>,
    ) -> Result<u64, ApplicationSessionStatus> {
        self.with_control_barrier(|| {
            let transport = session_transport(transport_name)?;
            if transport.start_listen().is_none() {
                return Err(ApplicationSessionStatus::TransportListenUnsupported);
            }
            let application_listener = self
                .applications()
                .register_listener(application, app, opaque)
                .map_err(application_status)?;
            match self.listen(application_listener, transport, endpoint) {
                Ok(listener) => Ok(listener.raw()),
                Err(_) => {
                    self.applications()
                        .remove_listener(application, application_listener)
                        .expect("failed Session listen leaves its Application listener available for rollback");
                    Err(ApplicationSessionStatus::TransportListenFailed)
                }
            }
        })
        .map_err(|_| ApplicationSessionStatus::ApplicationControlWrongThread)?
    }

    fn application_connect(
        &self,
        application: ApplicationId,
        transport_name: &str,
        remote: std::net::SocketAddr,
        local: Option<std::net::SocketAddr>,
        worker: hammer_runtime::DataWorkerId,
        server_name: Option<String>,
        app: Option<hammer_runtime::app::SessionAppId>,
        opaque: Option<u64>,
    ) -> Result<u64, ApplicationSessionStatus> {
        let transport = session_transport(transport_name)?;
        if transport.connect().is_none() {
            return Err(ApplicationSessionStatus::TransportConnectUnsupported);
        }
        let application_connection = self
            .applications()
            .register_connection(application, server_name.clone(), app, opaque)
            .map_err(application_status)?;
        let endpoint = SessionConnectEndpoint::new(
            remote,
            local,
            worker,
            SessionConnectionId::from_raw(application_connection.raw()),
            application,
            opaque,
            server_name,
        );
        match self.connect(transport, endpoint) {
            Ok(_) => {
                self.applications()
                    .reclaim_connection(application, application_connection)
                    .expect("completed Session connect leaves its Application connection available for reclamation");
                Ok(application_connection.raw())
            }
            Err(_) => {
                self.applications()
                    .remove_connection(application, application_connection)
                    .expect("failed Session connect leaves its Application connection available for rollback");
                Err(ApplicationSessionStatus::TransportConnectFailed)
            }
        }
    }

    fn application_unlisten(
        &self,
        application: ApplicationId,
        listener: hammer_runtime::SessionListenerId,
    ) -> Result<(), ApplicationSessionStatus> {
        self.with_control_barrier(|| {
            match self.applications().contains(application) {
                Ok(true) => {}
                Ok(false) => return Err(ApplicationSessionStatus::ApplicationMissing),
                Err(error) => return Err(application_status(error)),
            }
            let (owner, application_listener) = self
                .with_listener(listener, |listener| {
                    (listener.application(), listener.application_listener())
                })
                .map_err(|_| ApplicationSessionStatus::ListenerMissing)?;
            if owner != application {
                return Err(ApplicationSessionStatus::ListenerNotOwned);
            }
            self.applications()
                .with_listener(application_listener, |listener| listener.application())
                .map_err(application_status)?;
            self.unlisten(listener)
                .map_err(|_| ApplicationSessionStatus::TransportUnlistenFailed)?;
            self.applications()
                .remove_listener(application, application_listener)
                .expect("validated Application listener remains present after transport unlisten");
            Ok(())
        })
        .map_err(|_| ApplicationSessionStatus::ApplicationControlWrongThread)?
    }
}

fn session_transport(
    name: &str,
) -> Result<hammer_runtime::SessionTransportRegistration, ApplicationSessionStatus> {
    let result = Engine::with_current(|engine| engine.plugin_main().session_transport(name));
    match result {
        Some(Ok(transport)) => Ok(transport),
        Some(Err(PluginError::SessionTransportMissing { .. })) => {
            Err(ApplicationSessionStatus::TransportMissing)
        }
        Some(Err(PluginError::SessionTransportDuplicate { .. })) => {
            Err(ApplicationSessionStatus::TransportDuplicate)
        }
        Some(Err(error)) => panic!("session transport lookup returned unrelated error: {error}"),
        None => Err(ApplicationSessionStatus::ApplicationControlWrongThread),
    }
}

fn application_status(error: ApplicationError) -> ApplicationSessionStatus {
    match error {
        ApplicationError::CapacityExhausted { .. } => {
            ApplicationSessionStatus::ApplicationCapacityExhausted
        }
        ApplicationError::Missing { .. } => ApplicationSessionStatus::ApplicationMissing,
        ApplicationError::WrongThread => ApplicationSessionStatus::ApplicationControlWrongThread,
        ApplicationError::SessionAppMissing { .. } => ApplicationSessionStatus::SessionAppMissing,
        ApplicationError::SessionAppDuplicate { .. } => {
            ApplicationSessionStatus::SessionAppDuplicate
        }
        ApplicationError::SessionAppUnregistered { .. } => {
            ApplicationSessionStatus::SessionAppMissing
        }
        ApplicationError::ListenerCapacityExhausted { .. } => {
            ApplicationSessionStatus::ListenerCapacityExhausted
        }
        ApplicationError::ConnectionCapacityExhausted { .. } => {
            ApplicationSessionStatus::ConnectionCapacityExhausted
        }
        ApplicationError::ListenerMissing { .. } => ApplicationSessionStatus::ListenerMissing,
        ApplicationError::ListenerNotOwned { .. } => ApplicationSessionStatus::ListenerNotOwned,
        ApplicationError::ConnectionMissing { .. } => ApplicationSessionStatus::ConnectionMissing,
        ApplicationError::ConnectionNotOwned { .. } => ApplicationSessionStatus::ConnectionNotOwned,
        ApplicationError::ConnectionAlreadyCompleted { .. } => {
            ApplicationSessionStatus::ConnectionAlreadyCompleted
        }
        ApplicationError::ConnectionNotCompleted { .. } => {
            ApplicationSessionStatus::ConnectionNotCompleted
        }
        ApplicationError::MqCapacityInvalid { .. } => {
            ApplicationSessionStatus::ApplicationMqCapacityInvalid
        }
        ApplicationError::MqWorkerCountZero
        | ApplicationError::MqLayout { .. }
        | ApplicationError::MqLayoutOverflow
        | ApplicationError::MqSegmentCreate { .. }
        | ApplicationError::MqSegmentExhausted
        | ApplicationError::MqInit { .. }
        | ApplicationError::MqInstall { .. } => {
            ApplicationSessionStatus::ApplicationMqResourceUnavailable
        }
        ApplicationError::MqDetachFailed { .. } => {
            ApplicationSessionStatus::ApplicationMqResourceUnavailable
        }
        ApplicationError::MqPublication { .. } => {
            ApplicationSessionStatus::ApplicationMqResourceUnavailable
        }
        ApplicationError::SessionMainMissing => ApplicationSessionStatus::SessionMainUnavailable,
        ApplicationError::MqAlreadyAttached { .. } => {
            ApplicationSessionStatus::ApplicationMqAlreadyAttached
        }
    }
}

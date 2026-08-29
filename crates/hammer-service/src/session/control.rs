use hammer_runtime::app::{
    SessionAcceptedReplyMsg, SessionBoundMsg, SessionConnectError, SessionConnectMsg,
    SessionConnectedMsg, SessionControlError, SessionEvtType, SessionListenMsg, SessionMsgQueue,
    SessionProducer, SessionUnlistenMsg, SessionUnlistenReplyMsg, SingleProducer,
};
use hammer_runtime::{
    DataWorkerId, GlobalMain, RuntimeError, RuntimeResult, SessionConnectEndpoint,
};

use super::application::{ApplicationError, application_main};
use super::runtime::{SessionMain, schedule_worker_task};
use crate::transport::transport_vft;

impl SessionMain {
    pub fn dispatch_application_session_mq(
        &self,
        application: u32,
        requests: &mut SessionMsgQueue<SingleProducer>,
        replies: &mut SessionProducer,
    ) -> RuntimeResult<()> {
        // One bad element never strands later requests (VPP
        // `session_mq_handle_connects_rpc`, session_node.c:237-271 iterates
        // independent elements): decode failures are reported and skipped so
        // the drain continues. A reply is only produced when the decoded
        // request yields one (a failed decode has no context to reply with).
        let mut first_error = None;
        while let Some(item) = requests.dequeue_control()? {
            let event = item.event_type();
            match event {
                SessionEvtType::Listen => match item.decode::<SessionListenMsg>() {
                    Some(Ok(request)) => {
                        let reply = self.application_listen(application, request);
                        replies.enqueue_control(&reply)?;
                    }
                    _ => tracing::warn!(?event, ?application, "undecodable Session Listen request"),
                },
                SessionEvtType::Unlisten => match item.decode::<SessionUnlistenMsg>() {
                    Some(Ok(request)) => {
                        let reply = self.application_unlisten(application, request);
                        replies.enqueue_control(&reply)?;
                    }
                    _ => {
                        tracing::warn!(?event, ?application, "undecodable Session Unlisten request")
                    }
                },
                SessionEvtType::Connect | SessionEvtType::ConnectStream => {
                    match item.decode::<SessionConnectMsg>() {
                        Some(Ok(request)) => {
                            let stream = event == SessionEvtType::ConnectStream;
                            if let Err(error) =
                                self.application_connect(application, request.clone(), stream)
                            {
                                let connected = SessionConnectedMsg {
                                    context: request.context,
                                    result: Err(SessionConnectError::Control { error }),
                                    local: request.local,
                                    remote: Some(request.remote),
                                    flags: request.flags,
                                    opaque: request.opaque,
                                };
                                replies.enqueue_control(&connected)?;
                            }
                        }
                        _ => tracing::warn!(
                            ?event,
                            ?application,
                            "undecodable Session Connect request"
                        ),
                    }
                }
                SessionEvtType::AcceptedReply => match item.decode::<SessionAcceptedReplyMsg>() {
                    Some(Ok(request)) => {
                        if let Err(error) = self.accepted_reply(application, request) {
                            first_error.get_or_insert(error);
                        }
                    }
                    _ => {
                        tracing::warn!(
                            ?event,
                            ?application,
                            "undecodable Session AcceptedReply request"
                        )
                    }
                },
                SessionEvtType::RxEnq
                | SessionEvtType::TxDeq
                | SessionEvtType::Close
                | SessionEvtType::RxDeq
                | SessionEvtType::TxEnq
                | SessionEvtType::ProtocolOutput
                | SessionEvtType::HalfClose
                | SessionEvtType::Reset
                | SessionEvtType::Disconnected
                | SessionEvtType::TransportClosed
                | SessionEvtType::Bound
                | SessionEvtType::UnlistenReply
                | SessionEvtType::Accepted
                | SessionEvtType::Connected => {}
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    fn application_listen(&self, application: u32, request: SessionListenMsg) -> SessionBoundMsg {
        let result = self
            .with_control_barrier(|| {
                let transport = transport_vft(request.transport)
                    .ok_or(SessionControlError::TransportMissing)?;
                if transport.start_listen.is_none() {
                    return Err(SessionControlError::TransportListenUnsupported);
                }
                let application_listener = application_main()
                    .register_listener(application, request.app, request.opaque)
                    .map_err(SessionControlError::from)?;
                match self.listen(application_listener, request.transport, request.endpoint) {
                    Ok(listener) => Ok(listener),
                    Err(error) => {
                        let _ =
                            application_main().remove_listener(application, application_listener);
                        Err(SessionControlError::from(error))
                    }
                }
            })
            .map_err(|_| SessionControlError::ApplicationControlWrongThread)
            .and_then(|result| result);
        SessionBoundMsg {
            context: request.context,
            result,
            local: Some(request.endpoint.local()),
            opaque: request.opaque,
        }
    }

    fn application_connect(
        &self,
        application: u32,
        request: SessionConnectMsg,
        stream: bool,
    ) -> Result<(), SessionControlError> {
        let transport =
            transport_vft(request.transport).ok_or(SessionControlError::TransportMissing)?;
        let server_name = self.application_server_name(application, request.ext_config)?;
        if stream {
            if transport.connect_stream.is_none() {
                return Err(SessionControlError::TransportConnectUnsupported);
            }
            let parent = request
                .parent_handle
                .ok_or(SessionControlError::ConnectStreamParentMissing)?;
            let worker = DataWorkerId::try_from(parent.thread_index)
                .map_err(|_| SessionControlError::ConnectStreamParentMissing)?;
            let application_connection = application_main()
                .register_connection(
                    application,
                    request.context,
                    None,
                    request.app,
                    request.opaque,
                )
                .map_err(SessionControlError::from)?;
            let endpoint = SessionConnectEndpoint {
                remote: request.remote,
                local: request.local,
                worker,
                connection: application_connection,
                application,
                parent_handle: Some(parent),
                flags: request.flags,
                opaque: request.opaque,
                // CONNECT_STREAM inherits the TLS/QUIC configuration of its
                // parent Session; no per-stream ext-config is accepted.
                server_name: None,
            };
            if let Err(error) = self.connect_stream(request.transport, endpoint) {
                let _ = application_main().remove_connection(application, application_connection);
                // VPP notifies the app with the concrete connect rv
                // (`app_worker_connect_notify`, session_node.c:355-359).
                return Err(SessionControlError::from(error));
            }
            Ok(())
        } else {
            if transport.connect.is_none() {
                return Err(SessionControlError::TransportConnectUnsupported);
            }
            let application_connection = application_main()
                .register_connection(
                    application,
                    request.context,
                    None,
                    request.app,
                    request.opaque,
                )
                .map_err(SessionControlError::from)?;
            let endpoint = SessionConnectEndpoint::new(
                request.remote,
                request.local,
                DataWorkerId::new(0),
                application_connection,
                application,
                request.opaque,
                server_name,
            );
            if let Err(error) = self.connect(request.transport, endpoint) {
                let _ = application_main().remove_connection(application, application_connection);
                // VPP propagates the concrete connect rv to the app
                // (`session_mq_connect_one`, session_node.c:263-267).
                return Err(SessionControlError::from(error));
            }
            Ok(())
        }
    }

    /// Resolves the bounded ext-config chunk referenced by an ordinary
    /// CONNECT into an owned server name, then returns the chunk to the free
    /// list exactly once (VPP `session_mq_connect_handler` reads ext_config
    /// for the TLS/QUIC server name, session_node.c:327-348). A validation
    /// failure still frees the chunk; a read failure leaves ownership with
    /// the Application because the chunk is not in a readable allocated
    /// state.
    fn application_server_name(
        &self,
        application: u32,
        ext_config: Option<u64>,
    ) -> Result<Option<String>, SessionControlError> {
        let Some(offset) = ext_config else {
            return Ok(None);
        };
        let store = application_main()
            .with_application_mq(application, |resources| Ok(resources.ext_config_store()))
            .map_err(SessionControlError::from)?
            .ok_or(SessionControlError::ExtConfigUnavailable)?;
        let data = store
            .read(offset)
            .map_err(|_| SessionControlError::ExtConfigFailed)?;
        let name = match std::str::from_utf8(data) {
            Ok(name) if !name.is_empty() => name,
            _ => {
                // The chunk is still allocated; return it before failing so
                // a validation failure cannot leak the bounded allocation.
                let _ = store.free(offset);
                return Err(SessionControlError::ExtConfigInvalid);
            }
        };
        store
            .free(offset)
            .map_err(|_| SessionControlError::ExtConfigFailed)?;
        Ok(Some(name.to_owned()))
    }

    fn application_unlisten(
        &self,
        application: u32,
        request: SessionUnlistenMsg,
    ) -> SessionUnlistenReplyMsg {
        let listener = request.listener;
        let result = self
            .with_control_barrier(|| {
                match application_main().contains(application) {
                    Ok(true) => {}
                    Ok(false) => return Err(SessionControlError::ApplicationMissing),
                    Err(error) => return Err(SessionControlError::from(error)),
                }
                let (owner, application_listener) = self
                    .with_listener(listener, |listener| {
                        (listener.application(), listener.application_listener())
                    })
                    .map_err(|_| SessionControlError::ListenerMissing)?;
                if owner != application {
                    return Err(SessionControlError::ListenerNotOwned);
                }
                application_main()
                    .with_listener(application_listener, |listener| listener.application())
                    .map_err(SessionControlError::from)?;
                self.unlisten(listener).map_err(SessionControlError::from)?;
                application_main()
                    .remove_listener(application, application_listener)
                    .map_err(SessionControlError::from)?;
                Ok(())
            })
            .map_err(|_| SessionControlError::ApplicationControlWrongThread)
            .and_then(|result| result);
        SessionUnlistenReplyMsg {
            context: request.context,
            listener: request.listener,
            result,
        }
    }

    /// Applies an ACCEPTED_REPLY from the Application on the worker that owns
    /// the accepted Session. VPP redirects the main-thread arrival to a worker
    /// before running the handler (session_node.c:511-515); Hammer forwards it
    /// synchronously through the engine task queue — the same mechanism used
    /// for Application MQ install/removal — because the main thread cannot
    /// mutate the worker-owned Session table (`ThreadOwned::with_mut` would
    /// return WrongThread). The request drain records the first typed
    /// scheduling/worker failure and continues with later replies.
    fn accepted_reply(
        &self,
        application: u32,
        request: SessionAcceptedReplyMsg,
    ) -> RuntimeResult<()> {
        let worker = DataWorkerId::try_from(request.session.thread_index)?;
        let main = SessionMain::global()?;
        let result = GlobalMain::with_current(|engine| {
            schedule_worker_task(engine, worker, move || {
                hammer_runtime::with_data_plane_main(|runtime| {
                    main.with_worker_mut(runtime, |sessions| {
                        sessions.accept_reply(application, request.session, request.result)
                    })
                })
            })
        });
        result.ok_or(RuntimeError::WorkerControlRequiresGlobalMain)??;
        Ok(())
    }
}

impl From<ApplicationError> for SessionControlError {
    fn from(error: ApplicationError) -> Self {
        match error {
            ApplicationError::MqCapacityInvalid { .. } | ApplicationError::MqWorkerCountZero => {
                Self::TransportFailed
            }
            ApplicationError::Missing { .. } => Self::ApplicationMissing,
            ApplicationError::WrongThread => Self::ApplicationControlWrongThread,
            ApplicationError::ListenerMissing { .. } => Self::ListenerMissing,
            ApplicationError::ListenerNotOwned { .. } => Self::ListenerNotOwned,
            ApplicationError::ConnectionMissing { .. } => Self::ConnectionMissing,
            ApplicationError::ConnectionNotOwned { .. } => Self::ConnectionNotOwned,
            ApplicationError::ConnectionAlreadyConnected { .. }
            | ApplicationError::ConnectionNotConnected { .. }
            | ApplicationError::MqLayout { .. }
            | ApplicationError::MqLayoutOverflow
            | ApplicationError::MqSegmentCreate { .. }
            | ApplicationError::MqSegmentExhausted
            | ApplicationError::MqInit { .. }
            | ApplicationError::MqInstall { .. }
            | ApplicationError::MqDetachFailed { .. }
            | ApplicationError::MqPublication { .. } => Self::TransportFailed,
            ApplicationError::MqAlreadyAttached { .. } => Self::TransportFailed,
            ApplicationError::SessionAppAlreadyRegistered { .. } => Self::TransportFailed,
        }
    }
}

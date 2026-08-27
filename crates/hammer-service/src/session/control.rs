use hammer_runtime::app::{
    SessionAcceptedReplyMsg, SessionBoundMsg, SessionConnectError, SessionConnectMsg,
    SessionConnectedMsg, SessionControlError, SessionEvtType, SessionListenMsg, SessionMsgQueue,
    SessionProducer, SessionUnlistenMsg, SessionUnlistenReplyMsg, SingleProducer,
};
use hammer_runtime::{DataWorkerId, Engine, RuntimeError, RuntimeResult, SessionConnectEndpoint};
use std::sync::Arc;

use super::application::ApplicationError;
use super::runtime::{SessionMain, schedule_worker_task};

impl SessionMain {
    pub fn dispatch_application_session_mq(
        self: &Arc<Self>,
        application: u32,
        requests: &mut SessionMsgQueue<SingleProducer>,
        replies: &mut SessionProducer,
    ) -> RuntimeResult<()> {
        // One bad element never strands later requests (VPP
        // `session_mq_handle_connects_rpc`, session_node.c:237-271 iterates
        // independent elements): decode failures are reported and skipped so
        // the drain continues. A reply is only produced when the decoded
        // request yields one (a failed decode has no context to reply with).
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
                            tracing::warn!(%error, ?application, "ACCEPTED_REPLY was not applied");
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
        Ok(())
    }

    fn application_listen(&self, application: u32, request: SessionListenMsg) -> SessionBoundMsg {
        let result = self
            .with_control_barrier(|| {
                let transport = session_transport(request.transport)?;
                if transport.start_listen().is_none() {
                    return Err(SessionControlError::TransportListenUnsupported);
                }
                let application_listener = self
                    .applications()
                    .register_listener(application, request.app, request.opaque)
                    .map_err(SessionControlError::from)?;
                match self.listen(application_listener, transport, request.endpoint) {
                    Ok(listener) => Ok(listener),
                    Err(error) => {
                        let _ = self
                            .applications()
                            .remove_listener(application, application_listener);
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
        let transport = session_transport(request.transport)?;
        let server_name = self.application_server_name(application, request.ext_config)?;
        if stream {
            if transport.connect_stream().is_none() {
                return Err(SessionControlError::TransportConnectUnsupported);
            }
            let parent = request
                .parent_handle
                .ok_or(SessionControlError::ConnectStreamParentMissing)?;
            let application_connection = self
                .applications()
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
                worker: DataWorkerId::new(parent.thread_index),
                connection: application_connection,
                application,
                parent_handle: Some(parent),
                flags: request.flags,
                opaque: request.opaque,
                // CONNECT_STREAM inherits the TLS/QUIC configuration of its
                // parent Session; no per-stream ext-config is accepted.
                server_name: None,
            };
            if let Err(error) = self.connect_stream(transport, endpoint) {
                let _ = self
                    .applications()
                    .remove_connection(application, application_connection);
                // VPP notifies the app with the concrete connect rv
                // (`app_worker_connect_notify`, session_node.c:355-359).
                return Err(SessionControlError::from(error));
            }
            Ok(())
        } else {
            if transport.connect().is_none() {
                return Err(SessionControlError::TransportConnectUnsupported);
            }
            let application_connection = self
                .applications()
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
            if let Err(error) = self.connect(transport, endpoint) {
                let _ = self
                    .applications()
                    .remove_connection(application, application_connection);
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
        let store = self
            .applications()
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
                match self.applications().contains(application) {
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
                self.applications()
                    .with_listener(application_listener, |listener| listener.application())
                    .map_err(SessionControlError::from)?;
                self.unlisten(listener).map_err(SessionControlError::from)?;
                self.applications()
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
    /// return WrongThread). Hop or worker-side failures are logged and never
    /// fail the request drain, so later replies are still processed.
    fn accepted_reply(
        self: &Arc<Self>,
        application: u32,
        request: SessionAcceptedReplyMsg,
    ) -> RuntimeResult<()> {
        let worker = DataWorkerId::new(request.session.thread_index);
        let main = Arc::clone(self);
        let result = Engine::with_current(|engine| {
            schedule_worker_task(engine, worker, move || {
                Engine::with_current(|engine| {
                    let runtime = &mut engine.runtime;
                    main.with_worker_mut(runtime, |sessions| {
                        sessions.accept_reply(application, request.session, request.result)
                    })
                })
                .ok_or(RuntimeError::WorkerControlRequiresMainEngine)
            })
        });
        let Some(result) = result else {
            tracing::warn!(
                ?application,
                ?worker,
                "ACCEPTED_REPLY requires the main engine"
            );
            return Ok(());
        };
        if let Err(error) = result {
            tracing::warn!(%error, ?application, ?worker, "ACCEPTED_REPLY was not applied");
        }
        Ok(())
    }
}

fn session_transport(
    index: u8,
) -> Result<hammer_runtime::SessionTransportRegistration, SessionControlError> {
    match Engine::with_current(|engine| {
        engine
            .plugin_main()
            .session_transports()
            .get(index as usize)
            .copied()
    }) {
        Some(Some(transport)) => Ok(transport),
        Some(None) => Err(SessionControlError::TransportMissing),
        None => Err(SessionControlError::ApplicationControlWrongThread),
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
            ApplicationError::SessionAppMissing { .. }
            | ApplicationError::SessionAppUnregistered { .. } => Self::SessionAppMissing,
            ApplicationError::SessionAppDuplicate { .. } => Self::SessionAppDuplicate,
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
            ApplicationError::SessionMainMissing => Self::SessionMainUnavailable,
            ApplicationError::MqAlreadyAttached { .. } => Self::TransportFailed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::application::ApplicationMain;
    use hammer_runtime::attach::ExtConfigStore;

    /// One SessionMain with an attached Application whose bounded ext-config
    /// store is handed back for direct inspection.
    fn ext_config_fixture() -> (Arc<SessionMain>, u32, ExtConfigStore) {
        let applications = ApplicationMain::new(2);
        let application = applications
            .attach_local_for_test(1, 128)
            .expect("attach test Application");
        let main = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let store = applications
            .with_application_mq(application, |resources| Ok(resources.ext_config_store()))
            .expect("resolve Application MQ resources")
            .expect("Application carries an ext-config store");
        (main, application, store)
    }

    #[test]
    fn application_server_name_reads_and_frees_ext_config_exactly_once() {
        let (main, application, store) = ext_config_fixture();
        let offset = store.alloc(b"example.com").expect("alloc ext-config chunk");
        let name = main
            .application_server_name(application, Some(offset))
            .expect("valid hostname");
        assert_eq!(name.as_deref(), Some("example.com"));
        // The daemon freed the chunk exactly once after reading it.
        assert!(
            store.read(offset).is_err(),
            "chunk must be freed after the daemon read it"
        );
        assert!(
            store.free(offset).is_err(),
            "a second free must be rejected as a double free"
        );
    }

    #[test]
    fn application_server_name_frees_chunk_on_validation_error() {
        let (main, application, store) = ext_config_fixture();
        let offset = store
            .alloc(&[0xff, 0xfe, 0xfd])
            .expect("alloc ext-config chunk");
        let error = main
            .application_server_name(application, Some(offset))
            .expect_err("invalid UTF-8 server name");
        assert_eq!(error, SessionControlError::ExtConfigInvalid);
        // A validation failure still returns the chunk to the free list.
        assert!(
            store.read(offset).is_err(),
            "chunk must be freed on validation failure"
        );
    }

    #[test]
    fn application_server_name_rejects_empty_name_and_frees_chunk() {
        let (main, application, store) = ext_config_fixture();
        let offset = store.alloc(b"").expect("alloc ext-config chunk");
        let error = main
            .application_server_name(application, Some(offset))
            .expect_err("empty server name");
        assert_eq!(error, SessionControlError::ExtConfigInvalid);
        assert!(
            store.read(offset).is_err(),
            "chunk must be freed for an empty server name"
        );
    }

    #[test]
    fn application_server_name_without_ext_config_reference_is_none() {
        let (main, application, _store) = ext_config_fixture();
        let name = main
            .application_server_name(application, None)
            .expect("no ext-config reference");
        assert!(name.is_none());
    }

    #[test]
    fn application_server_name_fails_on_unreadable_offset() {
        let (main, application, store) = ext_config_fixture();
        // Far outside the store: the read fails and ownership stays with the
        // Application (nothing to free).
        let error = main
            .application_server_name(application, Some(0xffff_ffff))
            .expect_err("out-of-range ext-config offset");
        assert_eq!(error, SessionControlError::ExtConfigFailed);
        // The store is untouched: the next allocation still succeeds (chunk 0).
        let _ = store.alloc(b"after").expect("store still healthy");
    }

    /// A CONNECT_STREAM or CONNECT runtime failure must surface as its
    /// specific control error through the typed boundary, never as the
    /// opaque `TransportFailed` collapse (issue #222; VPP propagates the
    /// concrete rv via `app_worker_connect_notify`,
    /// session_node.c:263-267/355-359).
    #[test]
    fn connect_errors_survive_as_specific_control_errors() {
        use crate::session::error::SessionError;

        assert_eq!(
            SessionControlError::from(SessionError::ConnectStreamParentMissing),
            SessionControlError::ConnectStreamParentMissing
        );
        assert_eq!(
            SessionControlError::from(SessionError::ConnectStreamWrongWorker {
                parent: SessionHandle::new(1, 1),
                expected: DataWorkerId::new(2),
                actual: DataWorkerId::new(3),
            }),
            SessionControlError::ConnectStreamWrongWorker
        );
        assert_eq!(
            SessionControlError::from(SessionError::NoDataWorkers),
            SessionControlError::NoDataWorkers
        );
        assert_eq!(
            SessionControlError::from(SessionError::TransportConnectUnsupported {
                transport: "udp",
            }),
            SessionControlError::TransportConnectUnsupported
        );
        assert_eq!(
            SessionControlError::from(SessionError::TransportOpFailed {
                source: RuntimeError::service_closed(),
            }),
            SessionControlError::TransportFailed
        );
    }

    /// Connection ownership/not-found failures map to dedicated wire variants
    /// the same way the listener flow maps `ListenerMissing`/`ListenerNotOwned`.
    #[test]
    fn application_connection_errors_map_like_listener_errors() {
        let missing = ApplicationError::ConnectionMissing { connection: 1 };
        assert_eq!(
            SessionControlError::from(missing),
            SessionControlError::ConnectionMissing
        );
        let not_owned = ApplicationError::ConnectionNotOwned {
            application: (1),
            connection: 2,
        };
        assert_eq!(
            SessionControlError::from(not_owned),
            SessionControlError::ConnectionNotOwned
        );
    }
}

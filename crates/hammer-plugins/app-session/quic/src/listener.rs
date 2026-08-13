use std::cell::UnsafeCell;
use std::sync::{Arc, OnceLock, mpsc};
use std::thread::{self, ThreadId};

use hammer_core::data_plane::NodeState;
use hammer_infra::align::CacheLine;
use hammer_infra::pool::Index;
use hammer_infra::pool::Pool;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{ApplicationId, SessionAppId};
use hammer_runtime::{
    DataWorkerId, Engine, RuntimeError, RuntimeResult, SessionConnectEndpoint, SessionConnectionId,
    SessionListenEndpoint, SessionListenerId, SessionTransportRegistration,
};
use hammer_service::session::ApplicationMain;
use hammer_service::session::node::SessionQueueNode;
use hammer_service::session::runtime::{SessionMain, SessionTransport, SessionWorker};

use crate::config::{ConfigId, QUIC_CONFIG_CAPACITY, QuicConfigRegistry};
use crate::worker::ListenerContext;
use crate::worker::{Context, ContextId, QUIC_CONTEXT_CAPACITY, QuicWorker, QuicWorkerError};

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum QuicListenerError {
    #[error("required graph node `{name}` is not registered")]
    NodeMissing { name: &'static str },
    #[error("QUIC listener {listener:?} is not registered")]
    ListenerMissing { listener: SessionListenerId },
    #[error("QUIC listener capacity {capacity} is exhausted")]
    ListenerCapacityExhausted { capacity: usize },
    #[error("QUIC active connect requires an explicit local endpoint")]
    LocalEndpointMissing,
    #[error("QUIC active connect endpoint family mismatch between local and remote")]
    ConnectEndpointMismatch,
    #[error("QUIC stream connect requires a parent Session handle")]
    ParentSessionMissing,
    #[error("QUIC worker graph setup failed: {setup}; attachment rollback failed: {cleanup}")]
    WorkerGraphRollbackFailed {
        setup: RuntimeError,
        cleanup: RuntimeError,
    },
}

/// Main Thread-owned QUIC listener authority.
///
/// The inner Application Listener is deliberately generic: its `config` is
/// updated to the outer `SessionListenerId` while the same WorkerBarrier
/// transaction is active. QUIC interprets that opaque fact later; service and
/// runtime do not know its type.
pub struct QuicMain {
    owner: ThreadId,
    sessions: Arc<SessionMain>,
    inner_application: ApplicationId,
    session_app: SessionAppId,
    udp_transport: SessionTransportRegistration,
    pub(crate) configs: QuicConfigRegistry,
    contexts: Arc<QuicListenerContexts>,
    workers: Box<[CacheLine<ThreadOwned<QuicWorker>>]>,
}

struct QuicListenerContexts {
    value: UnsafeCell<Pool<Context>>,
}

impl QuicListenerContexts {
    fn new(capacity: usize) -> Self {
        Self {
            value: UnsafeCell::new(Pool::with_capacity(capacity)),
        }
    }

    #[inline]
    fn get(&self) -> &Pool<Context> {
        // SAFETY: Main Thread mutates this pool only while the worker barrier
        // is held. Data Workers read it only after that publication barrier
        // and never while a writer can access it.
        unsafe { &*self.value.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn get_mut(&self) -> &mut Pool<Context> {
        // SAFETY: callers must be the Main Thread and hold the worker barrier
        // (or run before Data Workers exist), which is enforced by
        // `QuicMain::with_contexts`.
        unsafe { &mut *self.value.get() }
    }
}

// SAFETY: access to the listener pool is ordered by the Session worker
// barrier; reads from Data Workers observe only barrier-published state.
unsafe impl Send for QuicListenerContexts {}
unsafe impl Sync for QuicListenerContexts {}

impl QuicMain {
    pub(crate) fn new(
        sessions: Arc<SessionMain>,
        inner_application: ApplicationId,
        session_app: SessionAppId,
        udp_transport: SessionTransportRegistration,
        worker_count: usize,
    ) -> Self {
        Self {
            owner: thread::current().id(),
            sessions,
            inner_application,
            session_app,
            udp_transport,
            configs: QuicConfigRegistry::new(QUIC_CONFIG_CAPACITY),
            contexts: Arc::new(QuicListenerContexts::new(QUIC_CONTEXT_CAPACITY)),
            workers: (0..worker_count)
                .map(|_| CacheLine::new(ThreadOwned::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    pub(crate) fn start_listen(
        &self,
        outer_listener: SessionListenerId,
        outer_application: ApplicationId,
        outer_config: Option<u64>,
        endpoint: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let (context_count, context_capacity) =
            self.with_contexts(|contexts| (contexts.len(), contexts.capacity()))?;
        if context_count == context_capacity {
            return Err(QuicListenerError::ListenerCapacityExhausted {
                capacity: context_capacity,
            }
            .into());
        }

        let config = outer_config
            .map(ConfigId::from_raw)
            .ok_or(crate::config::ConfigError::ConfigurationRequired)?;
        let server_config = self.configs.server_config(outer_application, config)?;
        let transport_config = self.configs.transport_config(outer_application, config)?;

        let applications = self.sessions.applications();
        let inner_application_listener = applications
            .register_listener(self.inner_application, Some(self.session_app), None)
            .map_err(RuntimeError::from)?;
        let inner_session_listener =
            match self
                .sessions
                .listen(inner_application_listener, self.udp_transport, endpoint)
            {
                Ok(listener) => listener,
                Err(error) => {
                    applications
                        .remove_listener(self.inner_application, inner_application_listener)
                        .expect("failed QUIC inner listen leaves Application listener available");
                    return Err(error.into());
                }
            };

        let context = self.with_contexts(|contexts| {
            // Capacity was checked before any Session state was published, so
            // a failed insert would be an impossible pool invariant violation.
            contexts
                .insert(Context::listener(
                    outer_listener,
                    outer_application,
                    inner_application_listener,
                    inner_session_listener,
                    config,
                    transport_config.connection_timeout,
                    Some(server_config),
                ))
                .map(ContextId::from)
                .expect("QUIC listener pool capacity remains after preflight")
        })?;

        // The lower UDP Session Listener must exist before its Application
        // Listener receives the context identity. This is the VPP
        // `udp_listen_session->opaque` publication point, and it remains in
        // the outer SessionMain barrier transaction.
        applications
            .update_listener_opaque(
                self.inner_application,
                inner_application_listener,
                Some(context.into()),
            )
            .expect("new QUIC inner Application Listener remains publishable");
        Ok(())
    }

    pub(crate) fn stop_listen(&self, outer_listener: SessionListenerId) -> RuntimeResult<()> {
        let (index, listener) = self
            .with_contexts(|contexts| {
                contexts.iter().find_map(|(index, context)| {
                    context.listener_context().and_then(|listener| {
                        (listener.outer_listener == outer_listener).then_some((index, listener))
                    })
                })
            })?
            .ok_or(QuicListenerError::ListenerMissing {
                listener: outer_listener,
            })?;

        self.sessions.unlisten(listener.inner_session_listener)?;
        self.sessions
            .applications()
            .remove_listener(self.inner_application, listener.inner_application_listener)
            .expect("validated QUIC inner Application listener remains during unlisten");
        self.with_contexts(|contexts| {
            contexts
                .remove(index)
                .expect("validated QUIC listener context remains during unlisten");
        })?;
        Ok(())
    }

    fn with_contexts<R>(
        &self,
        operation: impl FnOnce(&mut Pool<Context>) -> R,
    ) -> RuntimeResult<R> {
        if thread::current().id() != self.owner {
            return Err(RuntimeError::ControlRequiresMainThread);
        }
        hammer_runtime::ensure_main_thread_with_barrier()?;
        Ok(operation(self.contexts.get_mut()))
    }

    pub(crate) fn listener_context(&self, context: ContextId) -> Option<ListenerContext> {
        self.contexts
            .get()
            .get(context.into())
            .and_then(Context::listener_context)
    }

    pub(crate) fn install_worker(&self, worker: DataWorkerId) -> RuntimeResult<()> {
        let slot =
            self.workers
                .get(worker.slot())
                .ok_or_else(|| QuicWorkerError::WorkerOutOfRange {
                    worker: worker.slot(),
                })?;
        slot.install(QuicWorker::new(worker)).map_err(|_| {
            RuntimeError::from(QuicWorkerError::WorkerAlreadyInstalled {
                worker: worker.slot(),
            })
        })
    }

    pub(crate) fn with_worker<R>(
        &self,
        worker: DataWorkerId,
        operation: impl FnOnce(&mut QuicWorker) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let slot = self.workers.get(worker.slot()).ok_or_else(|| {
            RuntimeError::from(QuicWorkerError::WorkerOutOfRange {
                worker: worker.slot(),
            })
        })?;
        slot.with_mut(operation).map_err(|source| {
            RuntimeError::from(QuicWorkerError::WorkerAccess {
                worker: worker.slot(),
                source,
            })
        })?
    }

    pub(crate) fn with_worker_and_sessions<R>(
        &self,
        sessions: &mut SessionWorker<Index>,
        operation: impl FnOnce(&mut SessionWorker<Index>, &mut QuicWorker) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let worker = sessions.worker();
        let slot = self.workers.get(worker.slot()).ok_or_else(|| {
            RuntimeError::from(QuicWorkerError::WorkerOutOfRange {
                worker: worker.slot(),
            })
        })?;
        slot.with_mut(|quic| operation(sessions, quic))
            .map_err(|source| {
                RuntimeError::from(QuicWorkerError::WorkerAccess {
                    worker: worker.slot(),
                    source,
                })
            })?
    }

    pub(crate) fn applications(&self) -> &ApplicationMain {
        self.sessions.applications()
    }

    pub(crate) fn application_is_attached(
        &self,
        application: ApplicationId,
    ) -> Result<bool, hammer_service::session::ApplicationError> {
        self.sessions.applications().contains(application)
    }

    #[cfg(test)]
    fn listener_count(&self) -> usize {
        self.with_contexts(|contexts| contexts.len())
            .expect("test listener inspection runs on the QUIC Main Thread")
    }

    #[cfg(test)]
    fn listener(&self, outer_listener: SessionListenerId) -> Option<ListenerContext> {
        self.with_contexts(|contexts| {
            contexts.iter().find_map(|(_, context)| {
                context.listener_context().and_then(|listener| {
                    (listener.outer_listener == outer_listener).then_some(listener)
                })
            })
        })
        .expect("test listener inspection runs on the QUIC Main Thread")
    }
}

pub(crate) static QUIC_MAIN: OnceLock<Arc<QuicMain>> = OnceLock::new();

pub(crate) fn connect(endpoint: SessionConnectEndpoint) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    let config = endpoint
        .opaque
        .map(ConfigId::from_raw)
        .ok_or(crate::config::ConfigError::ConfigurationRequired)?;
    let client_config = main
        .client_config(endpoint.application, config)
        .map_err(RuntimeError::from)?;
    let connection_timeout = main
        .configs
        .transport_config(endpoint.application, config)?
        .connection_timeout;
    let local = endpoint
        .local
        .ok_or(QuicListenerError::LocalEndpointMissing)?;
    let remote = endpoint.remote;
    if local.is_ipv4() != remote.is_ipv4() {
        return Err(QuicListenerError::ConnectEndpointMismatch.into());
    }
    let server_name = endpoint.server_name.unwrap_or_else(|| remote.to_string());
    let worker = endpoint.worker;

    let (completion, completed) = mpsc::sync_channel(1);
    Engine::with_current(|engine| {
        engine.schedule_on_worker(worker, {
            let main = Arc::clone(main);
            let client_config = Arc::clone(&client_config);
            let server_name = server_name.clone();
            let local = local;
            let remote = remote;
            move || {
                let result = main.with_worker(worker, |quic| {
                    quic.allocate_client_connect_with_timeout(
                        client_config,
                        server_name,
                        local,
                        remote,
                        endpoint.application,
                        None,
                        endpoint.opaque,
                        endpoint.connection,
                        connection_timeout,
                    )
                });
                if completion.send(result).is_err() {
                    return;
                }
            }
        })
    })
    .ok_or(RuntimeError::WorkerControlRequiresMainEngine)??;
    let context = completed
        .recv()
        .map_err(|_| RuntimeError::DataWorkerCallCanceled {
            worker: worker.slot(),
        })??;

    let inner_connection = main
        .sessions
        .applications()
        .register_connection(
            main.inner_application,
            endpoint.connection.raw(),
            None,
            Some(main.session_app),
            Some(context.into()),
        )
        .map_err(RuntimeError::from)?;
    if let Err(error) = main.sessions.connect(
        main.udp_transport,
        SessionConnectEndpoint::new(
            endpoint.remote,
            endpoint.local,
            endpoint.worker,
            SessionConnectionId::from_raw(inner_connection.raw()),
            main.inner_application,
            Some(context.into()),
            None,
        ),
    ) {
        let _ = main
            .sessions
            .applications()
            .remove_connection(main.inner_application, inner_connection);
        let (completion, completed) = mpsc::sync_channel(1);
        Engine::with_current(|engine| {
            engine.schedule_on_worker(worker, {
                let main = Arc::clone(main);
                move || {
                    let result = main.with_worker(worker, |quic| quic.remove_context(context));
                    if completion.send(result).is_err() {
                        return;
                    }
                }
            })
        })
        .ok_or(RuntimeError::WorkerControlRequiresMainEngine)??;
        let _ = completed
            .recv()
            .map_err(|_| RuntimeError::DataWorkerCallCanceled {
                worker: worker.slot(),
            })?;
        return Err(error.into());
    }
    main.sessions
        .applications()
        .reclaim_connection(main.inner_application, inner_connection)
        .map_err(RuntimeError::from)?;
    Ok(())
}

pub(crate) fn connect_stream(endpoint: SessionConnectEndpoint) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    let parent = endpoint
        .parent_handle
        .ok_or(QuicListenerError::ParentSessionMissing)?;
    let worker = endpoint.worker;
    let connection = endpoint.connection;
    let flags = endpoint.flags;
    let (completion, completed) = mpsc::sync_channel(1);

    Engine::with_current(|engine| {
        engine.schedule_on_worker(worker, {
            let main = Arc::clone(main);
            move || {
                let result = hammer_runtime::with_data_plane_runtime(|runtime| {
                    main.sessions.with_worker_mut(runtime, |sessions| {
                        main.with_worker_and_sessions(sessions, |sessions, quic| {
                            quic.connect_stream(sessions, parent, connection, flags)
                                .map(|_| ())
                        })
                    })
                });
                let _ = completion.send(result);
            }
        })
    })
    .ok_or(RuntimeError::WorkerControlRequiresMainEngine)??;

    completed
        .recv()
        .map_err(|_| RuntimeError::DataWorkerCallCanceled {
            worker: worker.slot(),
        })??;
    Ok(())
}

pub(crate) fn start_listen(
    listener: SessionListenerId,
    application: ApplicationId,
    config: Option<u64>,
    endpoint: SessionListenEndpoint,
) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?
        .start_listen(listener, application, config, endpoint)
}

pub(crate) fn stop_listen(listener: SessionListenerId) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?
        .stop_listen(listener)
}

#[hammer_component_macros::init_function(
    name = "quic_init",
    runs_after = ["transport_init", "session_init", "udp_init"],
    runs_before = ["install_packet_graph"]
)]
fn init_quic(engine: &mut Engine, sessions: Arc<SessionMain>) -> RuntimeResult<Arc<QuicMain>> {
    let session_app = sessions
        .applications()
        .session_app_id(crate::session_app::NAME)
        .map_err(RuntimeError::from)?;
    let udp_transport = engine
        .plugin_main()
        .session_transport("udp")
        .map_err(RuntimeError::from)?;
    let inner_application = sessions
        .applications()
        .attach()
        .map_err(RuntimeError::from)?;
    let main = Arc::new(QuicMain::new(
        Arc::clone(&sessions),
        inner_application,
        session_app,
        udp_transport,
        engine.configured_worker_count(),
    ));
    if QUIC_MAIN.set(Arc::clone(&main)).is_err() {
        sessions
            .applications()
            .detach(inner_application)
            .expect("duplicate QUIC initialization leaves no published inner Application");
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "quic" });
    }
    Ok(main)
}

fn bind_worker_graph(engine: &mut Engine, main: &QuicMain) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let session_queue =
        engine
            .runtime
            .node_by_name("session-queue")
            .ok_or(QuicListenerError::NodeMissing {
                name: "session-queue",
            })?;
    let udp_output = engine
        .runtime
        .node_by_name("udp-output")
        .ok_or(QuicListenerError::NodeMissing { name: "udp-output" })?;
    let session_queue_data = engine.runtime.nodes().node_runtime_data(session_queue)?;
    let session_queue_output =
        SessionQueueNode::existing_output_next(&engine.runtime, session_queue, udp_output)?;
    let attachment_installed = SessionQueueNode::install_worker_attachment(
        &engine.runtime,
        session_queue_data,
        session_queue_output,
        crate::worker::quic_session_queue_update_time,
        crate::worker::quic_session_queue_dispatch,
    )?;
    let setup = main
        .install_worker(worker)
        .and_then(|()| {
            // Install the worker-local transport action table into the owning
            // Session Worker, mirroring VPP's per-worker transport VFT
            // publication (`session_register_transport`, session.c:1823, with
            // `.connect_stream = quic_connect_stream`, quic.c:902): the Session
            // Worker owns the transport keyed by `QuicWorker::ID` (O(1) slot,
            // synchronous worker-local, no lock or scan). Duplicate installation
            // for one worker is a typed error, never an overwrite.
            main.sessions.with_worker_mut(&engine.runtime, |sessions| {
                sessions
                    .install_transport_actions(
                        QuicWorker::ID,
                        crate::worker::quic_transport_actions(),
                    )
                    .map_err(|source| {
                        RuntimeError::from(QuicWorkerError::TransportActionsInstall { source })
                    })
            })
        })
        .and_then(|()| {
            engine
                .runtime
                .nodes()
                .set_node_state(session_queue, NodeState::Polling)
        });
    if let Err(setup) = setup {
        if attachment_installed {
            if let Err(cleanup) = SessionQueueNode::remove_worker_attachment(
                &engine.runtime,
                session_queue_data,
                session_queue_output,
                crate::worker::quic_session_queue_update_time,
                crate::worker::quic_session_queue_dispatch,
            ) {
                return Err(QuicListenerError::WorkerGraphRollbackFailed { setup, cleanup }.into());
            }
        }
        return Err(setup);
    }
    Ok(())
}

#[hammer_component_macros::worker_init_function(
    name = "quic_worker_init",
    runs_after = ["session_worker_init", "udp_worker_init"]
)]
fn init_quic_worker(engine: &mut Engine) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    bind_worker_graph(engine, main)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use hammer_runtime::app::SessionAppRegistration;
    use hammer_runtime::{
        DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine, RuntimeRegistry,
        SessionTransportRegistration,
    };
    use hammer_service::session::ApplicationMain;
    use hammer_service::session::error::SessionTransportActionError;
    use rcgen::generate_simple_self_signed;

    use crate::config::ServerConfig;

    use super::*;

    static LISTEN_APPLICATION: AtomicU64 = AtomicU64::new(0);
    static LISTEN_OPAQUE: AtomicU64 = AtomicU64::new(0);
    static LISTEN_BARRIER: AtomicBool = AtomicBool::new(false);
    static STOP_LISTENER: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn quic_transport_registration_exposes_connect_stream() {
        assert!(
            crate::worker::__SESSION_TRANSPORT_QUIC_WORKER
                .connect_stream()
                .is_some()
        );
    }

    #[test]
    fn quic_worker_bind_installs_transport_actions() -> RuntimeResult<()> {
        let mut engine = test_engine();
        engine.install_current();
        let applications = test_application_main();
        let inner_application = applications.attach().map_err(RuntimeError::from)?;
        let sessions = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let main = QuicMain::new(
            Arc::clone(&sessions),
            inner_application,
            SessionAppId::new(0),
            SessionTransportRegistration::new("udp-test", None, None, None),
            1,
        );
        main.install_worker(DataWorkerId::new(0))?;

        // Owning-worker Session receives exactly the registration
        // `bind_worker_graph` performs at the bind point.
        let mut worker_sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )?;
        worker_sessions.install_application_mq_for_test(inner_application)?;
        worker_sessions
            .install_transport_actions(QuicWorker::ID, crate::worker::quic_transport_actions())?;

        // Duplicate installation for one worker is a typed error, never a
        // silent overwrite.
        let duplicate = worker_sessions
            .install_transport_actions(QuicWorker::ID, crate::worker::quic_transport_actions());
        assert!(matches!(
            duplicate,
            Err(SessionTransportActionError::AlreadyRegistered { transport })
                if transport == QuicWorker::ID
        ));

        Engine::uninstall_current();
        Ok(())
    }

    fn install_session_app(_: &mut Engine) -> RuntimeResult<()> {
        Ok(())
    }

    fn destroy_session_app(_: DataWorkerId, _: u64) {}

    fn record_start(
        listener: SessionListenerId,
        application: ApplicationId,
        opaque: Option<u64>,
        _: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        LISTEN_APPLICATION.store(application.raw(), Ordering::SeqCst);
        LISTEN_OPAQUE.store(opaque.unwrap_or_default(), Ordering::SeqCst);
        LISTEN_BARRIER.store(
            Engine::with_current(|engine| engine.worker_barrier().is_pending()).unwrap_or(false),
            Ordering::SeqCst,
        );
        assert_ne!(listener.raw(), 0);
        Ok(())
    }

    fn record_stop(listener: SessionListenerId) -> RuntimeResult<()> {
        STOP_LISTENER.store(listener.raw(), Ordering::SeqCst);
        Ok(())
    }

    fn fail_start(
        _: SessionListenerId,
        _: ApplicationId,
        _: Option<u64>,
        _: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        Err(RuntimeError::config_validation(
            "test QUIC lower listen failure",
        ))
    }

    fn test_application_main() -> Arc<ApplicationMain> {
        ApplicationMain::with_session_apps(
            8,
            [SessionAppRegistration::new(
                crate::session_app::NAME,
                install_session_app,
                destroy_session_app,
            )],
        )
    }

    fn test_engine() -> Engine {
        Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        )
    }

    fn server_config() -> ServerConfig {
        let certified = generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("generate QUIC test certificate");
        ServerConfig::new(
            vec![certified.cert.der().to_vec()],
            certified.signing_key.serialize_der(),
        )
    }

    #[test]
    fn quic_listener_updates_inner_config_inside_service_transaction() -> RuntimeResult<()> {
        let mut engine = test_engine();
        engine.install_current();
        let applications = test_application_main();
        let outer_application = applications.attach().map_err(RuntimeError::from)?;
        let inner_application = applications.attach().map_err(RuntimeError::from)?;
        let sessions = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let transport = SessionTransportRegistration::new(
            "udp-test",
            Some(record_start),
            Some(record_stop),
            None,
        );
        let main = Arc::new(QuicMain::new(
            Arc::clone(&sessions),
            inner_application,
            SessionAppId::new(0),
            transport,
            1,
        ));
        assert!(QUIC_MAIN.set(Arc::clone(&main)).is_ok());
        let config = main
            .register_server_config(outer_application, server_config())
            .map_err(RuntimeError::from)?;
        LISTEN_APPLICATION.store(0, Ordering::SeqCst);
        LISTEN_OPAQUE.store(0, Ordering::SeqCst);
        LISTEN_BARRIER.store(false, Ordering::SeqCst);
        STOP_LISTENER.store(0, Ordering::SeqCst);

        let endpoint = SessionListenEndpoint::new(
            "127.0.0.1:443".parse().expect("test endpoint"),
            DataWorkerId::new(0),
        );
        let outer_application_listener = applications
            .register_listener(outer_application, None, Some(config.raw()))
            .map_err(RuntimeError::from)?;
        let outer_listener = sessions.listen(
            outer_application_listener,
            SessionTransportRegistration::new(
                "quic-test",
                Some(super::start_listen),
                Some(super::stop_listen),
                None,
            ),
            endpoint,
        )?;
        let stored = main
            .listener(outer_listener)
            .expect("QUIC listener is published");
        assert_eq!(stored.configuration, config);
        assert_ne!(
            stored.inner_application_listener,
            outer_application_listener
        );
        assert_ne!(stored.inner_session_listener, outer_listener);
        assert_eq!(
            LISTEN_APPLICATION.load(Ordering::SeqCst),
            inner_application.raw()
        );
        assert_eq!(LISTEN_OPAQUE.load(Ordering::SeqCst), 0);
        assert!(LISTEN_BARRIER.load(Ordering::SeqCst));

        // The Session generic dispatch reaches the QUIC `open_stream`
        // wrapper installed at the bind point, under this test's single
        // QUIC_MAIN owner; `MissingRegistration` would prove the bind never
        // installed the table.
        let mut worker_sessions = SessionWorker::<Index>::new(
            DataWorkerId::new(0),
            1,
            hammer_runtime::app::AppSessionConfig::default(),
            64,
            Arc::clone(&applications),
            None,
        )?;
        worker_sessions.install_application_mq_for_test(inner_application)?;
        worker_sessions
            .install_transport_actions(QuicWorker::ID, crate::worker::quic_transport_actions())?;
        let parent = worker_sessions.construct_transport_session(
            QuicWorker::ID,
            Index::new(1, 1),
            1,
            inner_application,
            Some(SessionAppId::new(0)),
            None,
            None,
            false,
        )?;
        let opened = worker_sessions.open_stream(
            parent,
            hammer_runtime::session::SessionStreamDirection::Uni,
            0xDEAD_BEEF,
        );
        assert!(matches!(
            opened,
            Err(SessionTransportActionError::TransportActionFailed {
                action: "open_stream",
                ..
            })
        ));

        sessions.unlisten(outer_listener)?;
        assert_eq!(main.listener_count(), 0);
        assert_ne!(STOP_LISTENER.load(Ordering::SeqCst), 0);
        applications
            .remove_listener(outer_application, outer_application_listener)
            .map_err(RuntimeError::from)?;

        Engine::uninstall_current();
        Ok(())
    }

    #[test]
    fn failed_inner_listen_rolls_back_application_listener() -> RuntimeResult<()> {
        let mut engine = test_engine();
        engine.install_current();
        let applications = test_application_main();
        let outer_application = applications.attach().map_err(RuntimeError::from)?;
        let inner_application = applications.attach().map_err(RuntimeError::from)?;
        let sessions = Arc::new(SessionMain::new(1, Arc::clone(&applications)));
        let main = QuicMain::new(
            Arc::clone(&sessions),
            inner_application,
            SessionAppId::new(0),
            SessionTransportRegistration::new("udp-failure", Some(fail_start), None, None),
            1,
        );
        let config = main
            .register_server_config(outer_application, server_config())
            .map_err(RuntimeError::from)?;

        let result = main.start_listen(
            SessionListenerId::new(23, 8),
            outer_application,
            Some(config.raw()),
            SessionListenEndpoint::new(
                "127.0.0.1:444".parse().expect("test endpoint"),
                DataWorkerId::new(0),
            ),
        );
        assert!(result.is_err());
        assert_eq!(main.listener_count(), 0);
        let probe = applications
            .register_listener(inner_application, Some(SessionAppId::new(0)), None)
            .map_err(RuntimeError::from)?;
        assert_eq!(probe.slot(), 0, "failed inner listener was rolled back");
        applications
            .remove_listener(inner_application, probe)
            .map_err(RuntimeError::from)?;

        Engine::uninstall_current();
        Ok(())
    }
}

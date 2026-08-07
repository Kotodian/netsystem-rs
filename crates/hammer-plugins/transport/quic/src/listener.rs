use std::sync::{Arc, OnceLock};
use std::thread::{self, ThreadId};

use hammer_infra::align::CacheLine;
use hammer_infra::pool::Pool;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{ApplicationId, SessionAppId};
use hammer_runtime::{
    Barrier, DataWorkerId, Engine, RuntimeError, RuntimeResult, SessionListenEndpoint,
    SessionListenerId, SessionTransportRegistration,
};
use hammer_service::session::runtime::SessionMain;

use crate::config::{ConfigId, QUIC_CONFIG_CAPACITY, QuicConfigRegistry};
#[cfg(test)]
use crate::worker::ListenerContext;
use crate::worker::{Context, ContextId, QUIC_CONTEXT_CAPACITY, QuicWorker, QuicWorkerError};

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum QuicListenerError {
    #[error("QUIC listener {listener:?} is not registered")]
    ListenerMissing { listener: SessionListenerId },
    #[error("QUIC listener capacity {capacity} is exhausted")]
    ListenerCapacityExhausted { capacity: usize },
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
    contexts: Barrier<Pool<Context>>,
    workers: Box<[CacheLine<ThreadOwned<QuicWorker>>]>,
}

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
            contexts: Barrier::new(Pool::with_capacity(QUIC_CONTEXT_CAPACITY)),
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

        let server_config = outer_config
            .map(ConfigId::from_raw)
            .ok_or(crate::config::ConfigError::ConfigurationRequired)?;
        self.configs
            .server_config(outer_application, server_config)?;
        self.configs
            .transport_config(outer_application, server_config)?;

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
                    return Err(error);
                }
            };

        let context = self.with_contexts(|contexts| {
            // Capacity was checked before any Session state was published, so
            // a failed insert would be an impossible pool invariant violation.
            contexts
                .insert(Context::listener(
                    outer_listener,
                    inner_application_listener,
                    inner_session_listener,
                    server_config,
                ))
                .map(ContextId::from)
                .expect("QUIC listener pool capacity remains after preflight")
        })?;

        // The lower UDP Session Listener must exist before its Application
        // Listener receives the context identity. This is the VPP
        // `udp_listen_session->opaque` publication point, and it remains in
        // the outer SessionMain barrier transaction.
        applications
            .update_listener_config(
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
                    context.listener_facts().and_then(|listener| {
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
        // SAFETY: this Main Thread owns the listener pool. Its caller either
        // holds SessionMain's WorkerBarrier or runs before Data Workers exist.
        Ok(unsafe { self.contexts.with_mut_unchecked(operation) })
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
                context.listener_facts().and_then(|listener| {
                    (listener.outer_listener == outer_listener).then_some(listener)
                })
            })
        })
        .expect("test listener inspection runs on the QUIC Main Thread")
    }
}

pub(crate) static QUIC_MAIN: OnceLock<Arc<QuicMain>> = OnceLock::new();

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

#[hammer_component_macros::worker_init_function(
    name = "quic_worker_init",
    runs_after = ["session_worker_init", "udp_worker_init"]
)]
fn init_quic_worker(engine: &mut Engine) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?
        .install_worker(worker)
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
    use rcgen::generate_simple_self_signed;

    use crate::config::ServerConfig;

    use super::*;

    static LISTEN_APPLICATION: AtomicU64 = AtomicU64::new(0);
    static LISTEN_CONFIG: AtomicU64 = AtomicU64::new(0);
    static LISTEN_BARRIER: AtomicBool = AtomicBool::new(false);
    static STOP_LISTENER: AtomicU64 = AtomicU64::new(0);

    fn install_session_app(_: &mut Engine) -> RuntimeResult<()> {
        Ok(())
    }

    fn destroy_session_app(_: DataWorkerId, _: u64) {}

    fn record_start(
        listener: SessionListenerId,
        application: ApplicationId,
        config: Option<u64>,
        _: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        LISTEN_APPLICATION.store(application.raw(), Ordering::SeqCst);
        LISTEN_CONFIG.store(config.unwrap_or_default(), Ordering::SeqCst);
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
        LISTEN_CONFIG.store(0, Ordering::SeqCst);
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
        assert_eq!(stored.application, outer_application);
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
        assert_eq!(LISTEN_CONFIG.load(Ordering::SeqCst), outer_listener.raw());
        assert!(LISTEN_BARRIER.load(Ordering::SeqCst));

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

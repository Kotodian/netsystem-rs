use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, mpsc};

use hammer_core::data_plane::NodeState;
use hammer_infra::align::CacheLine;
use hammer_infra::pool::Pool;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::SessionHandle;
use hammer_runtime::{
    DataPlaneMain, DataWorkerId, GlobalMain, RuntimeError, RuntimeResult, SessionConnectEndpoint,
    SessionListenEndpoint,
};
use hammer_service::session::application_main;
use hammer_service::session::node::SessionQueueNode;
use hammer_service::session::runtime::{SessionWorker, session_main};
use hammer_service::transport::{TransportVft, register_transport};

use crate::config::{ConfigId, QUIC_CONFIG_CAPACITY, QuicConfigRegistry};
use crate::worker::ListenerContext;
use crate::worker::{Context, QUIC_CONTEXT_CAPACITY, QuicWorker, QuicWorkerError};

#[hammer_component_macros::runtime_error(subsystem = "quic")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum QuicListenerError {
    #[error("required graph node `{name}` is not registered")]
    NodeMissing { name: &'static str },
    #[error("QUIC listener {listener:?} is not registered")]
    ListenerMissing { listener: SessionHandle },
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
/// updated to the outer `SessionHandle` while the same WorkerBarrier
/// transaction is active. QUIC interprets that opaque fact later; service and
/// runtime do not know its type.
pub struct QuicMain {
    protocol: u8,
    inner_application: u32,
    session_app: u32,
    pub(crate) configs: QuicConfigRegistry,
    contexts: Arc<QuicListenerContexts>,
    workers: Box<[CacheLine<ThreadOwned<QuicWorker>>]>,
}

struct QuicListenerContexts {
    value: UnsafeCell<Pool<Context>>,
    by_connection: UnsafeCell<HashMap<u32, u32>>,
    by_outer_listener: UnsafeCell<HashMap<u32, u32>>,
}

impl QuicListenerContexts {
    fn new(capacity: usize) -> Self {
        Self {
            value: UnsafeCell::new(Pool::with_capacity(capacity)),
            by_connection: UnsafeCell::new(HashMap::new()),
            by_outer_listener: UnsafeCell::new(HashMap::new()),
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
        // (or run before Data Workers exist), which is enforced at each
        // Main Thread call site.
        unsafe { &mut *self.value.get() }
    }

    #[inline]
    fn connection_context(&self, connection_index: u32) -> Option<u32> {
        // SAFETY: reads occur after the Main Thread publication barrier.
        unsafe { (&*self.by_connection.get()).get(&connection_index).copied() }
    }

    #[inline]
    fn publish_connection_context(&self, connection_index: u32, context: u32) {
        // SAFETY: writes occur only on the Main Thread under the worker barrier.
        unsafe {
            (&mut *self.by_connection.get()).insert(connection_index, context);
        }
    }

    #[inline]
    fn remove_connection_context(&self, connection_index: u32) {
        // SAFETY: writes occur only on the Main Thread under the worker barrier.
        unsafe {
            (&mut *self.by_connection.get()).remove(&connection_index);
        }
    }

    #[inline]
    fn outer_listener_context(&self, listener: u32) -> Option<u32> {
        // SAFETY: reads occur after the Main Thread publication barrier.
        unsafe { (&*self.by_outer_listener.get()).get(&listener).copied() }
    }

    #[inline]
    fn publish_outer_listener_context(&self, listener: u32, context: u32) {
        // SAFETY: writes occur only on the Main Thread under the worker barrier.
        unsafe {
            (&mut *self.by_outer_listener.get()).insert(listener, context);
        }
    }

    #[inline]
    fn remove_outer_listener_context(&self, listener: u32) {
        // SAFETY: writes occur only on the Main Thread under the worker barrier.
        unsafe {
            (&mut *self.by_outer_listener.get()).remove(&listener);
        }
    }
}

// SAFETY: access to the listener pool is ordered by the Session worker
// barrier; reads from Data Workers observe only barrier-published state.
unsafe impl Send for QuicListenerContexts {}
unsafe impl Sync for QuicListenerContexts {}

impl QuicMain {
    pub(crate) fn new(
        protocol: u8,
        inner_application: u32,
        session_app: u32,
        worker_count: usize,
    ) -> Self {
        Self {
            protocol,
            inner_application,
            session_app,
            configs: QuicConfigRegistry::new(QUIC_CONFIG_CAPACITY),
            contexts: Arc::new(QuicListenerContexts::new(QUIC_CONTEXT_CAPACITY)),
            workers: (0..worker_count)
                .map(|_| CacheLine::new(ThreadOwned::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    pub const fn protocol(&self) -> u8 {
        self.protocol
    }

    pub(crate) fn start_listen(
        &self,
        outer_listener: SessionHandle,
        outer_application: u32,
        outer_config: Option<u64>,
        endpoint: SessionListenEndpoint,
    ) -> RuntimeResult<u32> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let contexts = self.contexts.get();
        let context_count = contexts.len();
        let context_capacity = contexts.capacity();
        if context_count == context_capacity {
            return Err(QuicListenerError::ListenerCapacityExhausted {
                capacity: context_capacity,
            }
            .into());
        }

        let config = outer_config
            .map(ConfigId::try_from)
            .transpose()?
            .ok_or(crate::config::ConfigError::ConfigurationRequired)?;
        let server_config = self.configs.server_config(outer_application, config)?;
        let transport_config = self.configs.transport_config(outer_application, config)?;

        let applications = application_main();
        let udp_protocol = hammer_plugin_udp::protocol()?;
        let inner_application_listener = applications
            .register_listener(self.inner_application, Some(self.session_app), None)
            .map_err(RuntimeError::from)?;
        let inner_session_listener =
            match session_main().listen(inner_application_listener, udp_protocol, endpoint) {
                Ok(listener) => listener,
                Err(error) => {
                    applications
                        .remove_listener(self.inner_application, inner_application_listener)
                        .expect("failed QUIC inner listen leaves Application listener available");
                    return Err(error.into());
                }
            };

        // Capacity was checked before any Session state was published, so a
        // failed insert would be an impossible pool invariant violation.
        let context = self.contexts.get_mut().insert(Context::listener(
            outer_listener,
            outer_application,
            inner_application_listener,
            inner_session_listener,
            config,
            transport_config.connection_timeout,
            Some(server_config),
        ));
        self.contexts
            .publish_connection_context(inner_session_listener.session_index, context);
        self.contexts
            .publish_outer_listener_context(outer_listener.session_index, context);

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
        Ok(inner_session_listener.session_index)
    }

    pub(crate) fn stop_listen(&self, connection_index: u32) -> RuntimeResult<()> {
        hammer_runtime::ensure_main_thread_with_barrier()?;
        let context_id = self.contexts.connection_context(connection_index).ok_or(
            QuicListenerError::ListenerMissing {
                listener: SessionHandle::new(connection_index, 0),
            },
        )?;
        let index = context_id;
        let listener = self
            .contexts
            .get()
            .get(index)
            .and_then(Context::listener_context)
            .ok_or(QuicListenerError::ListenerMissing {
                listener: SessionHandle::new(connection_index, 0),
            })?;

        session_main().unlisten(listener.inner_session_listener)?;
        application_main()
            .remove_listener(self.inner_application, listener.inner_application_listener)
            .expect("validated QUIC inner Application listener remains during unlisten");
        self.contexts
            .get_mut()
            .remove(index)
            .expect("validated QUIC listener context remains during unlisten");
        self.contexts.remove_connection_context(connection_index);
        self.contexts
            .remove_outer_listener_context(listener.outer_listener.session_index);
        Ok(())
    }

    pub(crate) fn listener_context(&self, context: u32) -> Option<ListenerContext> {
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
        slot.install(QuicWorker::new(worker, self.protocol))
            .map_err(|_| {
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
        sessions: &mut SessionWorker,
        operation: impl FnOnce(&mut SessionWorker, &mut QuicWorker) -> RuntimeResult<R>,
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

    pub(crate) fn application_is_attached(
        &self,
        application: u32,
    ) -> Result<bool, hammer_service::session::ApplicationError> {
        application_main().contains(application)
    }
}

pub(crate) static QUIC_MAIN: OnceLock<QuicMain> = OnceLock::new();

pub fn protocol() -> RuntimeResult<u8> {
    QUIC_MAIN
        .get()
        .map(QuicMain::protocol)
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })
}

pub(crate) fn connect(endpoint: SessionConnectEndpoint) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    let config = endpoint
        .opaque
        .map(ConfigId::try_from)
        .transpose()?
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
    GlobalMain::with_current(|engine| {
        engine.schedule_on_worker(worker, {
            let main = main;
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
    .ok_or(RuntimeError::WorkerControlRequiresGlobalMain)??;
    let context = completed
        .recv()
        .map_err(|_| RuntimeError::DataWorkerCallCanceled {
            worker: worker.slot(),
        })??;

    let inner_connection = application_main()
        .register_connection(
            main.inner_application,
            endpoint.connection.into(),
            None,
            Some(main.session_app),
            Some(context.into()),
        )
        .map_err(RuntimeError::from)?;
    let udp_protocol = hammer_plugin_udp::protocol()?;
    if let Err(error) = session_main().connect(
        udp_protocol,
        SessionConnectEndpoint::new(
            endpoint.remote,
            endpoint.local,
            endpoint.worker,
            inner_connection,
            main.inner_application,
            Some(context.into()),
            None,
        ),
    ) {
        let _ = application_main().remove_connection(main.inner_application, inner_connection);
        let (completion, completed) = mpsc::sync_channel(1);
        GlobalMain::with_current(|engine| {
            engine.schedule_on_worker(worker, {
                let main = main;
                move || {
                    let result = main.with_worker(worker, |quic| quic.remove_context(context));
                    if completion.send(result).is_err() {
                        return;
                    }
                }
            })
        })
        .ok_or(RuntimeError::WorkerControlRequiresGlobalMain)??;
        let _ = completed
            .recv()
            .map_err(|_| RuntimeError::DataWorkerCallCanceled {
                worker: worker.slot(),
            })?;
        return Err(error.into());
    }
    application_main()
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

    GlobalMain::with_current(|engine| {
        engine.schedule_on_worker(worker, {
            let main = main;
            move || {
                let result = hammer_runtime::with_data_plane_main(|runtime| {
                    session_main().with_worker_mut(runtime, |sessions| {
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
    .ok_or(RuntimeError::WorkerControlRequiresGlobalMain)??;

    completed
        .recv()
        .map_err(|_| RuntimeError::DataWorkerCallCanceled {
            worker: worker.slot(),
        })??;
    Ok(())
}

pub(crate) fn start_listen(
    listener: SessionHandle,
    application: u32,
    config: Option<u64>,
    endpoint: SessionListenEndpoint,
) -> RuntimeResult<u32> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?
        .start_listen(listener, application, config, endpoint)
}

pub(crate) fn stop_listen(connection_index: u32) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?
        .stop_listen(connection_index)
}

#[hammer_component_macros::init_function(
    name = "quic_init",
    runs_after = ["transport_main_init", "session_init", "udp_init"],
    runs_before = ["install_packet_graph"]
)]
fn init_quic(engine: &mut GlobalMain) -> RuntimeResult<()> {
    if QUIC_MAIN.get().is_some() {
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "quic" });
    }
    let inner_application = application_main().attach().map_err(RuntimeError::from)?;
    let session_app = match hammer_service::session::register_session_app(
        inner_application,
        crate::session_app::VFT,
    ) {
        Ok(session_app) => session_app,
        Err(error) => {
            application_main()
                .detach(inner_application)
                .expect("failed QUIC Session App registration leaves no inner Application");
            return Err(error);
        }
    };
    let protocol = match register_transport(TransportVft::new(
        Some(start_listen),
        Some(stop_listen),
        Some(connect),
        Some(connect_stream),
        Some(crate::worker::quic_transport_open_stream),
        Some(crate::worker::quic_transport_reset_stream),
        Some(crate::worker::quic_transport_stop_sending),
        Some(crate::worker::quic_transport_close_connection),
    )) {
        Ok(protocol) => protocol,
        Err(error) => {
            application_main()
                .detach(inner_application)
                .expect("failed QUIC VFT registration leaves no inner Application");
            return Err(error.into());
        }
    };
    let main = QuicMain::new(
        protocol,
        inner_application,
        session_app,
        engine.configured_worker_count(),
    );
    if QUIC_MAIN.set(main).is_err() {
        application_main()
            .detach(inner_application)
            .expect("duplicate QUIC initialization leaves no published inner Application");
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "quic" });
    }
    Ok(())
}

fn bind_worker_graph(engine: &mut DataPlaneMain, main: &QuicMain) -> RuntimeResult<()> {
    let worker = engine.data_worker_id()?;
    let session_queue =
        engine
            .node_by_name("session-queue")
            .ok_or(QuicListenerError::NodeMissing {
                name: "session-queue",
            })?;
    let udp_output = engine
        .node_by_name("udp-output")
        .ok_or(QuicListenerError::NodeMissing { name: "udp-output" })?;
    let session_queue_data = engine.nodes().node_runtime_data(session_queue)?;
    let session_queue_output =
        SessionQueueNode::existing_output_next(engine, session_queue, udp_output)?;
    let attachment_installed = SessionQueueNode::install_worker_attachment(
        engine,
        session_queue_data,
        session_queue_output,
        crate::worker::quic_session_queue_update_time,
        crate::worker::quic_session_queue_dispatch,
    )?;
    let setup = main.install_worker(worker).and_then(|()| {
        engine
            .nodes()
            .set_node_state(session_queue, NodeState::Polling)
    });
    if let Err(setup) = setup {
        if attachment_installed {
            if let Err(cleanup) = SessionQueueNode::remove_worker_attachment(
                engine,
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
fn init_quic_worker(engine: &mut DataPlaneMain) -> RuntimeResult<()> {
    let main = QUIC_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "quic" })?;
    bind_worker_graph(engine, main)
}

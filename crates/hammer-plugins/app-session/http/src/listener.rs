//! Main Thread-owned HTTP listener authority: nested QUIC listen bootstrap.
//!
//! VPP owns the HTTP transport proto on the main thread: `http_transport_init`
//! (http.c:1867-1903) registers the proto against the session layer and
//! initializes the main-thread `http_main_t`, and `http_transport_enable`
//! (http.c:1023-1063) runs the main-thread attach of the builtin HTTP app.
//! `http_start_listen` (http.c:1287) then creates a real listener: it
//! allocates the `http_ctx_t` listener context, drives `vnet_listen` on the
//! lower QUIC transport (http.c:1381-1388), links the resulting transport
//! listener to the HTTP listener by writing `ts_listener->opaque = hl_index`
//! (`http_listener_link_with_tl`, http.c:1275-1284), and only at the end
//! links the outer Application listener. The listen path rolls back in
//! reverse order on failure: the lower QUIC listen is torn down with
//! `vnet_unlisten` before the `http_ctx_t` is freed (http.c:1405-1415).
//!
//! Hammer mirrors that authority with `HttpMain`, initialized once by
//! `init_http_transport` ordered after QUIC/session init, and nests the lower
//! QUIC listen inside the same Main Thread worker barrier transaction:
//! register an inner Application listener owned by HTTP's builtin Session
//! App, call `SessionMain::listen` on the fetched QUIC transport, then store
//! the direct listener context in the O(1) outer-listener slot map as the
//! final publication. The inner Application listener's opaque is deliberately
//! left as the forwarded config — QUIC's `start_listen` consumes it as its
//! registered server config (quic listener.rs:135-139) — so the context
//! identity never overwrites the config opaque (contrast QUIC, which
//! registers its own inner listener with a `None` opaque and publishes its
//! context id there, quic listener.rs:142-187). A missing outer config is a
//! typed prerequisite error before any side effect, and HTTP has no QUIC
//! config registration seam in this slice. `HttpMain` also owns the
//! per-data-worker HTTP worker
//! container, mirroring `QuicMain.workers` (quic listener.rs:58) and VPP's
//! `http_main.wrk` array (http.c:1073); each worker installs itself once
//! via the `http_worker_init` worker init function ordered after
//! session/QUIC worker init, exactly as QUIC binds its workers (quic
//! listener.rs:568-577). The HTTP3 engine, FIFO, QPACK, stop_listen, and
//! Session App lifecycle are later slices.

use std::cell::UnsafeCell;
use std::sync::{Arc, OnceLock};
use std::thread::{self, ThreadId};

use hammer_infra::align::CacheLine;
use hammer_infra::thread_owned::ThreadOwned;
use hammer_runtime::app::{ApplicationId, ApplicationListenerId, SessionAppId};
use hammer_runtime::{
    DataWorkerId, Engine, RuntimeError, RuntimeResult, SessionListenEndpoint, SessionListenerId,
    SessionTransportRegistration,
};
use hammer_service::session::runtime::{SessionMain, SessionTransportId};

use crate::http_app;
use crate::worker::{HttpWorker, HttpWorkerError};

/// Bounds the HTTP listener-context table.
///
/// The table is indexed by the outer `SessionListenerId`'s session-pool slot,
/// and the session pool is bounded by `DEFAULT_SESSION_POOL_CAPACITY`
/// (session/runtime.rs:49), so every live outer listener's slot fits. A
/// listener whose slot exceeds the bound is rejected by the capacity
/// preflight before any side effect.
const HTTP_LISTENER_CONTEXT_CAPACITY: usize = 1024;

#[hammer_component_macros::runtime_error(subsystem = "http")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum HttpListenerError {
    #[error("HTTP listener context capacity {capacity} is exhausted")]
    ListenerCapacityExhausted { capacity: usize },
    #[error(
        "HTTP listen requires a QUIC server config, but QUIC config registration \
         is not installed in this slice"
    )]
    ConfigRequired,
}

/// Direct HTTP listener context: outer session identity plus the nested
/// inner Application listener and the lower QUIC `SessionListener` it owns.
struct ListenerContext {
    outer_listener: SessionListenerId,
    outer_application: ApplicationId,
    outer_config: Option<u64>,
    inner_application_listener: ApplicationListenerId,
    inner_session_listener: SessionListenerId,
}

/// Bounded O(1) outer-listener to listener-context table.
///
/// Indexed by the outer `SessionListenerId`'s session-pool slot: live
/// listeners own their pool slot, so a slot index resolves the context in
/// O(1) with no pool scan, unlike QUIC's context `Pool`, whose `stop_listen`
/// must scan for the outer listener (quic listener.rs:190-201). The table is
/// a fixed contiguous `Vec` of `Option` slots, allocated up front; capacity
/// is preflighted before any Session state is published.
struct HttpListenerContexts {
    slots: UnsafeCell<Vec<Option<ListenerContext>>>,
}

impl HttpListenerContexts {
    fn new() -> Self {
        Self {
            slots: UnsafeCell::new((0..HTTP_LISTENER_CONTEXT_CAPACITY).map(|_| None).collect()),
        }
    }

    #[inline]
    fn get(&self) -> &Vec<Option<ListenerContext>> {
        // SAFETY: Main Thread mutates the table only while the worker barrier
        // is held. Later slices read it from Data Workers only after that
        // publication barrier and never while a writer can access it.
        unsafe { &*self.slots.get() }
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    fn get_mut(&self) -> &mut Vec<Option<ListenerContext>> {
        // SAFETY: callers must be the Main Thread and hold the worker barrier,
        // which is enforced by `HttpMain::with_contexts`.
        unsafe { &mut *self.slots.get() }
    }
}

// SAFETY: access to the context table is ordered by the Session worker
// barrier; future Data Worker reads observe only barrier-published state.
unsafe impl Send for HttpListenerContexts {}
unsafe impl Sync for HttpListenerContexts {}

/// Main Thread-owned HTTP listener authority.
///
/// VPP reference: `http_main_t` is owned by the main vlib thread
/// (http.c:1867-1903). HTTP rides over QUIC, so the authority retains the
/// QUIC transport registration it needs to nest inner listeners, mirroring
/// `QuicMain.udp_transport` (quic listener.rs:100), plus the inner
/// Application attached at init and the builtin HTTP Session App id.
pub struct HttpMain {
    owner: ThreadId,
    sessions: Arc<SessionMain>,
    quic_transport: SessionTransportRegistration,
    session_app: SessionAppId,
    inner_application: ApplicationId,
    contexts: HttpListenerContexts,
    /// One cache-line-aligned, thread-owned `HttpWorker` per configured data
    /// worker, mirroring `QuicMain.workers` (quic listener.rs:58).
    workers: Box<[CacheLine<ThreadOwned<HttpWorker>>]>,
}

impl HttpMain {
    /// Session-layer transport identity for HTTP.
    pub const TRANSPORT_ID: SessionTransportId = SessionTransportId::new(4);

    pub(crate) fn new(
        sessions: Arc<SessionMain>,
        quic_transport: SessionTransportRegistration,
        session_app: SessionAppId,
        inner_application: ApplicationId,
        worker_count: usize,
    ) -> Self {
        Self {
            owner: thread::current().id(),
            sessions,
            quic_transport,
            session_app,
            inner_application,
            contexts: HttpListenerContexts::new(),
            workers: (0..worker_count)
                .map(|_| CacheLine::new(ThreadOwned::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    /// Nests the lower QUIC listen inside the current Main Thread worker
    /// barrier transaction, then publishes the context only last.
    ///
    /// Mirrors `http_start_listen` (http.c:1287-1440): the HTTP listener
    /// context is created before the lower listen and linked to it only after
    /// the listen succeeds. Rollback is reverse order on failure — the lower
    /// QUIC `SessionListener` is removed by the session layer itself on a
    /// failed `SessionMain::listen` (session/runtime.rs:925-933), then the
    /// inner Application listener is removed here; the context is stored only
    /// after the listen succeeded, and the table capacity was preflighted, so
    /// the final store is invariant-guarded exactly as in QUIC
    /// (quic listener.rs:159-187). The inner Application listener's opaque is
    /// never overwritten: it remains the forwarded config consumed by the
    /// lower QUIC listen. The primary typed error is preserved; cleanup steps
    /// are infallible invariants, so no aggregate error variant is needed in
    /// this path (the `WorkerGraphRollbackFailed` aggregation pattern belongs
    /// to the worker-graph slice).
    pub(crate) fn start_listen(
        &self,
        outer_listener: SessionListenerId,
        outer_application: ApplicationId,
        outer_config: Option<u64>,
        endpoint: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        if thread::current().id() != self.owner {
            return Err(RuntimeError::ControlRequiresMainThread);
        }
        hammer_runtime::ensure_main_thread_with_barrier()?;

        // Capacity preflight before any Session state is published: a single
        // O(1) bounds check on the outer listener's session-pool slot.
        if outer_listener.slot() as usize >= HTTP_LISTENER_CONTEXT_CAPACITY {
            return Err(HttpListenerError::ListenerCapacityExhausted {
                capacity: HTTP_LISTENER_CONTEXT_CAPACITY,
            }
            .into());
        }
        // The lower QUIC listen consumes the forwarded config as its own
        // registered server config (quic listener.rs:135-139). HTTP has no
        // QUIC config registration seam in this slice, so a missing config is
        // a typed prerequisite error before any side effect — never a
        // reported listener that does not exist.
        let config = outer_config.ok_or(HttpListenerError::ConfigRequired)?;

        let applications = self.sessions.applications();
        let inner_application_listener = applications
            .register_listener(self.inner_application, Some(self.session_app), Some(config))
            .map_err(RuntimeError::from)?;
        let inner_session_listener =
            match self
                .sessions
                .listen(inner_application_listener, self.quic_transport, endpoint)
            {
                Ok(listener) => listener,
                Err(error) => {
                    applications
                        .remove_listener(self.inner_application, inner_application_listener)
                        .expect("failed QUIC inner listen leaves Application listener available");
                    return Err(error.into());
                }
            };

        self.with_contexts(|slots| {
            // Capacity was checked before any Session state was published, so
            // a failed store would be an impossible table invariant violation.
            slots[outer_listener.slot() as usize] = Some(ListenerContext {
                outer_listener,
                outer_application,
                outer_config,
                inner_application_listener,
                inner_session_listener,
            });
        })?;

        // The context store above is the final publication step, and it is
        // deliberately the only one: the inner Application listener's opaque
        // stays the forwarded config that the lower QUIC listen consumed as
        // its registered server config (quic listener.rs:135-139), so the
        // context identity never overwrites the config opaque.
        Ok(())
    }

    fn with_contexts<R>(
        &self,
        operation: impl FnOnce(&mut Vec<Option<ListenerContext>>) -> R,
    ) -> RuntimeResult<R> {
        if thread::current().id() != self.owner {
            return Err(RuntimeError::ControlRequiresMainThread);
        }
        hammer_runtime::ensure_main_thread_with_barrier()?;
        Ok(operation(self.contexts.get_mut()))
    }

    /// Installs the data worker's `HttpWorker` once, bound to the current
    /// thread.
    ///
    /// O(1). The slot is addressed by the exact `DataWorkerId`; out-of-range
    /// ids and duplicate or cross-thread installs are typed errors. Mirrors
    /// `QuicMain::install_worker` (quic listener.rs:234-246).
    pub(crate) fn install_worker(&self, worker: DataWorkerId) -> RuntimeResult<()> {
        let slot =
            self.workers
                .get(worker.slot())
                .ok_or_else(|| HttpWorkerError::WorkerOutOfRange {
                    worker: worker.slot(),
                })?;
        slot.install(HttpWorker::new(worker)).map_err(|_| {
            RuntimeError::from(HttpWorkerError::WorkerAlreadyInstalled {
                worker: worker.slot(),
            })
        })
    }

    /// Runs `operation` on the exact data worker's installed `HttpWorker`.
    ///
    /// O(1). Out-of-range ids fail with `WorkerOutOfRange`; access from any
    /// thread other than the installing one, or before install, fails with
    /// `WorkerAccess`. Mirrors `QuicMain::with_worker` (quic
    /// listener.rs:248-264).
    pub(crate) fn with_worker<R>(
        &self,
        worker: DataWorkerId,
        operation: impl FnOnce(&mut HttpWorker) -> RuntimeResult<R>,
    ) -> RuntimeResult<R> {
        let slot = self.workers.get(worker.slot()).ok_or_else(|| {
            RuntimeError::from(HttpWorkerError::WorkerOutOfRange {
                worker: worker.slot(),
            })
        })?;
        slot.with_mut(operation).map_err(|source| {
            RuntimeError::from(HttpWorkerError::WorkerAccess {
                worker: worker.slot(),
                source,
            })
        })?
    }
}

/// Process-wide authority published by `init_http_transport`; `pub(crate)` so
/// the Session App `accept` callback resolves the owning worker by Session
/// worker id (http_app.rs).
pub(crate) static HTTP_MAIN: OnceLock<Arc<HttpMain>> = OnceLock::new();

/// Registered `SessionTransportStartListen` callback.
///
/// Checks the main thread and worker barrier, then authority initialization,
/// returning typed errors before any side effect. Mirrors the QUIC transport
/// callback gate (quic listener.rs:466-477).
pub(crate) fn start_listen(
    listener: SessionListenerId,
    application: ApplicationId,
    config: Option<u64>,
    endpoint: SessionListenEndpoint,
) -> RuntimeResult<()> {
    hammer_runtime::ensure_main_thread_with_barrier()?;
    HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "http" })?
        .start_listen(listener, application, config, endpoint)
}

/// Static HTTP transport registration published by the plugin descriptor.
///
/// VPP registers the HTTP transport proto against the session layer in
/// `http_transport_init` (http.c:1867-1903). This slice registers a real
/// `start_listen` callback; stop/connect are later slices, so the entry is
/// written by hand rather than through the `session_transport` macro (which
/// requires `start_listen` and `stop_listen` together).
pub(crate) static __SESSION_TRANSPORT_HTTP_TRANSPORT: SessionTransportRegistration =
    SessionTransportRegistration::new("http", Some(start_listen), None, None);

/// Publish the main-thread HTTP listener authority.
///
/// Ordered after QUIC/session init so the QUIC transport registration is
/// already resolvable from the plugin main (mirroring `init_quic`,
/// quic listener.rs:487-520). Resolves the builtin HTTP Session App and
/// attaches the inner Application exactly as QUIC does for its inner
/// listener (quic listener.rs:493-504). Duplicate initialization is a typed
/// error and detaches the just-attached inner Application; the OnceLock keeps
/// the authority single-owner with no lock.
#[hammer_component_macros::init_function(
    name = "http_transport_init",
    runs_after = ["quic_init", "session_init"]
)]
fn init_http_transport(
    engine: &mut Engine,
    sessions: Arc<SessionMain>,
) -> RuntimeResult<Arc<HttpMain>> {
    let session_app = sessions
        .applications()
        .session_app_id(http_app::NAME)
        .map_err(RuntimeError::from)?;
    let quic_transport = engine
        .plugin_main()
        .session_transport("quic")
        .map_err(RuntimeError::from)?;
    let inner_application = sessions
        .applications()
        .attach()
        .map_err(RuntimeError::from)?;
    let main = Arc::new(HttpMain::new(
        Arc::clone(&sessions),
        quic_transport,
        session_app,
        inner_application,
        engine.configured_worker_count(),
    ));
    if HTTP_MAIN.set(Arc::clone(&main)).is_err() {
        sessions
            .applications()
            .detach(inner_application)
            .expect("duplicate HTTP initialization leaves no published inner Application");
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "http" });
    }
    Ok(main)
}

/// Install the current data worker's HTTP worker.
///
/// Runs once per data worker, ordered after session and QUIC worker init so
/// lower transport state exists before HTTP contexts can bind to it,
/// mirroring `init_quic_worker` (quic listener.rs:568-577) and VPP's
/// per-thread `http_worker_t` slot in `http_main.wrk` (http.c:1073).
#[hammer_component_macros::worker_init_function(
    name = "http_worker_init",
    runs_after = ["session_worker_init", "quic_worker_init"]
)]
fn init_http_worker(engine: &mut Engine) -> RuntimeResult<()> {
    let main = HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: "http" })?;
    main.install_worker(engine.data_worker_id()?)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::thread;

    use hammer_infra::pool::Index;
    use hammer_infra::thread_owned::ThreadOwnedError;
    use hammer_runtime::app::SessionAppId;
    use hammer_runtime::{
        DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine, PluginError, PluginMain,
        RuntimeRegistry,
    };
    use hammer_service::session::ApplicationMain;
    use hammer_service::session::SessionId;

    use super::*;

    fn test_engine() -> Engine {
        Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        )
    }

    /// SessionMain whose Application registry pre-registers the builtin HTTP
    /// Session App, so `init_http_transport` can resolve its id.
    ///
    /// Zero session workers: the duplicate-init rollback calls
    /// `application_detached` (runtime.rs:1115), which schedules per-worker
    /// cleanup a unit-test Engine has no worker control queues for; with no
    /// session workers the rollback resolves the SessionMain from the Engine
    /// registry and performs only its local teardown.
    fn test_sessions() -> Arc<SessionMain> {
        Arc::new(SessionMain::new(
            0,
            ApplicationMain::with_session_apps(8, [crate::http_app::HTTP_SESSION_APP]),
        ))
    }

    fn test_endpoint() -> SessionListenEndpoint {
        SessionListenEndpoint::new(
            "127.0.0.1:443".parse().expect("test endpoint"),
            DataWorkerId::new(0),
        )
    }

    fn init_error(result: RuntimeResult<Arc<HttpMain>>) -> RuntimeError {
        match result {
            Ok(_) => panic!("expected typed init error"),
            Err(error) => error,
        }
    }

    fn start_error(result: RuntimeResult<()>) -> RuntimeError {
        match result {
            Ok(()) => panic!("expected typed start_listen error"),
            Err(error) => error,
        }
    }

    fn causes_contain(error: &dyn std::error::Error, needle: &str) -> bool {
        std::iter::successors(Some(error), |cause| cause.source())
            .any(|cause| cause.to_string().contains(needle))
    }

    static LISTEN_APPLICATION: AtomicU64 = AtomicU64::new(0);
    static LISTEN_OPAQUE: AtomicU64 = AtomicU64::new(0);
    static LISTEN_BARRIER: AtomicBool = AtomicBool::new(false);
    static LISTEN_LISTENER: AtomicU64 = AtomicU64::new(0);

    /// Recording stub lower QUIC transport: succeeds and records the exact
    /// nested identities the session layer handed it.
    fn record_start(
        listener: SessionListenerId,
        application: ApplicationId,
        opaque: Option<u64>,
        _: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        LISTEN_LISTENER.store(listener.raw(), Ordering::SeqCst);
        LISTEN_APPLICATION.store(application.raw(), Ordering::SeqCst);
        LISTEN_OPAQUE.store(opaque.unwrap_or_default(), Ordering::SeqCst);
        LISTEN_BARRIER.store(
            Engine::with_current(|engine| engine.worker_barrier().is_pending()).unwrap_or(false),
            Ordering::SeqCst,
        );
        Ok(())
    }

    /// Failing stub lower QUIC transport: exercises the reverse-order rollback.
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

    static __FAKE_QUIC_TRANSPORT: SessionTransportRegistration =
        SessionTransportRegistration::new("quic", Some(record_start), None, None);

    hammer_runtime::__declare_registration_image!(
        init_functions = [];
        config_functions = [];
        early_config_functions = [];
        main_loop_enter_functions = [];
        main_loop_exit_functions = [];
        worker_init_functions = [];
        graph_nodes = [];
        node_functions = [];
        process_nodes = [];
        session_transports = [__FAKE_QUIC_TRANSPORT];
        session_apps = [];
        binary_api_methods = [];
    );

    #[test]
    fn http_listener_registration_exposes_start_listen() {
        let registration = __SESSION_TRANSPORT_HTTP_TRANSPORT;
        assert_eq!(registration.name(), "http");
        assert!(registration.start_listen().is_some());
        assert!(registration.stop_listen().is_none());
        assert!(registration.connect().is_none());
        assert_eq!(HttpMain::TRANSPORT_ID, SessionTransportId::new(4));

        // The plugin descriptor's registration image must resolve the HTTP
        // transport through PluginMain, exactly as `init_quic` resolves the
        // QUIC transport by name (quic listener.rs:497-500).
        let mut plugins = PluginMain::default();
        plugins.register_builtin_image(&crate::__HAMMER_REGISTRATION_IMAGE);
        let found = plugins
            .session_transport("http")
            .expect("plugin descriptor registers the HTTP transport");
        assert_eq!(found.name(), "http");
        assert!(found.start_listen().is_some());
    }

    #[test]
    fn http_listener_init_requires_registered_quic_transport() {
        let mut engine = test_engine();
        let error = init_error(init_http_transport(&mut engine, test_sessions()));
        assert!(matches!(
            error,
            RuntimeError::Plugin(PluginError::SessionTransportMissing { name }) if name == "quic"
        ));
    }

    #[test]
    fn http_listener_init_publishes_authority_and_nests_quic_listen() -> RuntimeResult<()> {
        let mut engine = test_engine();
        engine.install_current();
        let sessions = test_sessions();
        // The duplicate-init rollback detaches its inner Application, and
        // detach consults the current Engine's registry for the SessionMain
        // (application.rs:443-451); register the same instance the init path
        // receives before any attach/detach rollback can run.
        engine.registry.set(Arc::clone(&sessions));
        let outer_application = sessions
            .applications()
            .attach()
            .map_err(RuntimeError::from)?;
        let config = 42u64;

        // Init publishes the authority only after the QUIC transport is
        // resolvable and the builtin Session App id is resolved; it stores
        // exactly the SessionMain and the fetched Copy QUIC registration.
        engine
            .plugin_main_mut()
            .register_builtin_image(&__HAMMER_REGISTRATION_IMAGE);
        let main = match init_http_transport(&mut engine, Arc::clone(&sessions)) {
            Ok(main) => main,
            Err(error) => panic!("init failed: {error}"),
        };
        let published = HTTP_MAIN.get().expect("init publishes the HTTP authority");
        assert!(Arc::ptr_eq(&main, published));
        assert!(Arc::ptr_eq(&published.sessions, &sessions));
        assert_eq!(published.quic_transport.name(), "quic");

        // Duplicate init is a typed error and detaches its inner Application;
        // the rollback succeeds because the registry above resolves the
        // SessionMain (otherwise detach fails and init panics).
        let error = init_error(init_http_transport(&mut engine, Arc::clone(&sessions)));
        assert!(matches!(
            error,
            RuntimeError::PluginStateNotInitialized { plugin: "http" }
        ));

        LISTEN_APPLICATION.store(0, Ordering::SeqCst);
        LISTEN_OPAQUE.store(0, Ordering::SeqCst);
        LISTEN_BARRIER.store(false, Ordering::SeqCst);
        LISTEN_LISTENER.store(0, Ordering::SeqCst);

        let endpoint = test_endpoint();
        let outer_application_listener = sessions
            .applications()
            .register_listener(outer_application, None, Some(config))
            .map_err(RuntimeError::from)?;
        let outer_listener = sessions.listen(
            outer_application_listener,
            SessionTransportRegistration::new("http", Some(super::start_listen), None, None),
            endpoint,
        )?;

        // The lower QUIC transport ran synchronously inside the same worker
        // barrier transaction, with HTTP's inner Application owned listener
        // and the forwarded outer config consumed as its registered server
        // config (quic listener.rs:135-139).
        assert_eq!(
            LISTEN_APPLICATION.load(Ordering::SeqCst),
            main.inner_application.raw()
        );
        assert_eq!(LISTEN_OPAQUE.load(Ordering::SeqCst), config);
        assert!(LISTEN_BARRIER.load(Ordering::SeqCst));

        // The context is published in the outer-listener slot map at the
        // outer listener's session slot with the exact nested identities:
        // outer listener/application/config, inner Application listener, and
        // the lower QUIC SessionListener the session layer returned.
        let slot = outer_listener.slot() as usize;
        let context = main
            .contexts
            .get()
            .get(slot)
            .and_then(Option::as_ref)
            .expect("nested HTTP listener context is published");
        assert_eq!(context.outer_listener, outer_listener);
        assert_eq!(context.outer_application, outer_application);
        assert_eq!(context.outer_config, Some(config));
        assert_ne!(context.inner_session_listener.raw(), 0);
        assert_eq!(
            context.inner_session_listener.raw(),
            LISTEN_LISTENER.load(Ordering::SeqCst)
        );

        // The inner Application listener's opaque is preserved, never
        // overwritten with the context slot: re-listening through it hands
        // the lower QUIC transport the config fact (42) again, the same
        // opaque it consumed on the first listen. The context identity lives
        // only in the outer-listener slot map above.
        sessions.listen(
            context.inner_application_listener,
            SessionTransportRegistration::new("quic", Some(record_start), None, None),
            endpoint,
        )?;
        assert_eq!(LISTEN_OPAQUE.load(Ordering::SeqCst), config);

        Engine::uninstall_current();
        Ok(())
    }

    #[test]
    fn http_listener_start_listen_requires_config_before_side_effects() {
        let mut engine = test_engine();
        engine.install_current();
        let sessions = test_sessions();
        let main = Arc::new(HttpMain::new(
            Arc::clone(&sessions),
            SessionTransportRegistration::new("quic", Some(record_start), None, None),
            SessionAppId::new(0),
            sessions.applications().attach().expect("inner attach"),
            0,
        ));
        LISTEN_APPLICATION.store(0, Ordering::SeqCst);

        let error = start_error(main.start_listen(
            SessionListenerId::new(0, 0),
            sessions.applications().attach().expect("outer attach"),
            None,
            test_endpoint(),
        ));
        assert!(matches!(
            error,
            RuntimeError::Subsystem { subsystem, .. } if subsystem == "http"
        ));
        // The typed subsystem wrapper displays "{subsystem} subsystem failed";
        // the prerequisite message lives in its source chain.
        assert!(causes_contain(&error, "config"));
        // The lower QUIC transport was never reached and no context exists:
        // the prerequisite error fired before any side effect.
        assert_eq!(LISTEN_APPLICATION.load(Ordering::SeqCst), 0);
        assert!(main.contexts.get().iter().all(Option::is_none));

        Engine::uninstall_current();
    }

    #[test]
    fn http_listener_start_listen_rolls_back_inner_listener_on_quic_failure() {
        let mut engine = test_engine();
        engine.install_current();
        let sessions = Arc::new(SessionMain::new(
            1,
            ApplicationMain::with_session_apps(4, [crate::http_app::HTTP_SESSION_APP]),
        ));
        let outer_application = sessions.applications().attach().expect("outer attach");
        let main = Arc::new(HttpMain::new(
            Arc::clone(&sessions),
            SessionTransportRegistration::new("quic", Some(fail_start), None, None),
            SessionAppId::new(0),
            sessions.applications().attach().expect("inner attach"),
            0,
        ));

        // Repeated lower-listen failures must roll back the inner Application
        // listener: the Application listener pool is bounded at 4, so one
        // leaked listener per cycle would exhaust it by the fifth cycle and
        // surface a capacity error instead of the lower transport's typed
        // error.
        for cycle in 0..5 {
            let error = start_error(main.start_listen(
                SessionListenerId::new(cycle, 0),
                outer_application,
                Some(42),
                test_endpoint(),
            ));
            assert!(
                causes_contain(&error, "test QUIC lower listen failure"),
                "cycle {cycle} preserved the primary error: {error}"
            );
        }
        // No listener context was published on any failed path.
        assert!(main.contexts.get().iter().all(Option::is_none));

        Engine::uninstall_current();
    }

    #[test]
    fn http_listener_start_listen_requires_main_thread() {
        let endpoint = test_endpoint();
        let error = thread::spawn(move || {
            start_error(super::start_listen(
                SessionListenerId::new(2, 0),
                ApplicationId::new(0, 0),
                None,
                endpoint,
            ))
        })
        .join()
        .expect("start_listen thread completes");
        assert!(matches!(error, RuntimeError::ControlRequiresMainThread));
    }

    #[test]
    fn http_listener_authority_start_listen_enforces_owner_thread() {
        let sessions = test_sessions();
        let main = Arc::new(HttpMain::new(
            Arc::clone(&sessions),
            SessionTransportRegistration::new("quic", Some(record_start), None, None),
            SessionAppId::new(0),
            sessions.applications().attach().expect("inner attach"),
            0,
        ));
        let error = thread::spawn(move || {
            start_error(main.start_listen(
                SessionListenerId::new(3, 0),
                ApplicationId::new(0, 0),
                None,
                test_endpoint(),
            ))
        })
        .join()
        .expect("authority start_listen thread completes");
        assert!(matches!(error, RuntimeError::ControlRequiresMainThread));
    }

    /// Direct `HttpMain` for worker-container tests, bypassing the
    /// process-wide `HTTP_MAIN` OnceLock so each test owns its authority.
    fn test_http_main(worker_count: usize) -> Arc<HttpMain> {
        let sessions = test_sessions();
        let session_app = sessions
            .applications()
            .session_app_id(crate::http_app::NAME)
            .expect("test sessions register the builtin HTTP Session App");
        let inner_application = sessions
            .applications()
            .attach()
            .expect("test inner Application attaches");
        Arc::new(HttpMain::new(
            Arc::clone(&sessions),
            __FAKE_QUIC_TRANSPORT,
            session_app,
            inner_application,
            worker_count,
        ))
    }

    /// The typed `HttpWorkerError` inside a `RuntimeError` chain.
    fn http_error(error: &RuntimeError) -> &HttpWorkerError {
        std::error::Error::source(error)
            .and_then(|cause| cause.downcast_ref::<HttpWorkerError>())
            .expect("typed HTTP worker error in the chain")
    }

    #[test]
    fn http_worker_install_binds_exact_worker_id() {
        let main = test_http_main(2);
        main.install_worker(DataWorkerId::new(1))
            .expect("worker 1 installs");
        let context = main
            .with_worker(DataWorkerId::new(1), |worker| {
                worker
                    .allocate(SessionId::from_raw(1))
                    .map_err(RuntimeError::from)
            })
            .expect("exact worker lookup runs on the installing thread");
        assert_eq!(Index::from(context).slot(), 0);
        main.with_worker(DataWorkerId::new(1), |worker| {
            assert_eq!(worker.len(), 1);
            assert_eq!(worker.capacity(), crate::worker::HTTP_CONTEXT_CAPACITY);
            Ok(())
        })
        .expect("installed worker context observable");
    }

    #[test]
    fn http_worker_duplicate_install_is_typed_error() {
        let main = test_http_main(1);
        main.install_worker(DataWorkerId::new(0))
            .expect("first install");
        let error = main
            .install_worker(DataWorkerId::new(0))
            .expect_err("duplicate install rejected");
        assert!(matches!(
            http_error(&error),
            HttpWorkerError::WorkerAlreadyInstalled { worker: 0 }
        ));
    }

    #[test]
    fn http_worker_out_of_range_is_typed_error() {
        let main = test_http_main(2);
        let error = main
            .install_worker(DataWorkerId::new(2))
            .expect_err("install beyond the configured worker count");
        assert!(matches!(
            http_error(&error),
            HttpWorkerError::WorkerOutOfRange { worker: 2 }
        ));
        let error = main
            .with_worker(DataWorkerId::new(2), |_| Ok(()))
            .expect_err("lookup beyond the configured worker count");
        assert!(matches!(
            http_error(&error),
            HttpWorkerError::WorkerOutOfRange { worker: 2 }
        ));
    }

    #[test]
    fn http_worker_wrong_thread_access_is_rejected() {
        let main = test_http_main(2);
        main.install_worker(DataWorkerId::new(0))
            .expect("worker 0 installs on this thread");
        let (sender, receiver) = mpsc::channel();
        let other = Arc::clone(&main);
        let other_thread = thread::spawn(move || {
            other
                .install_worker(DataWorkerId::new(1))
                .expect("worker 1 installs on its own thread");
            let error = other
                .with_worker(DataWorkerId::new(0), |_| Ok(()))
                .expect_err("cross-thread access to worker 0 rejected");
            sender.send(error).expect("relay the typed error");
        });
        let error = receiver.recv().expect("worker thread reported");
        other_thread.join().expect("other thread completes");
        assert!(matches!(
            http_error(&error),
            HttpWorkerError::WorkerAccess {
                worker: 0,
                source: ThreadOwnedError::WrongThread
            }
        ));
        // The main thread still owns worker 0 exclusively.
        main.with_worker(DataWorkerId::new(0), |worker| {
            assert!(worker.is_empty());
            Ok(())
        })
        .expect("worker 0 remains owned by the installing thread");
    }

    #[test]
    fn http_worker_contexts_are_isolated_across_two_workers() {
        let main = test_http_main(2);
        main.install_worker(DataWorkerId::new(0))
            .expect("worker 0 installs on this thread");
        let worker0 = main
            .with_worker(DataWorkerId::new(0), |worker| {
                worker
                    .allocate(SessionId::from_raw(10))
                    .map_err(RuntimeError::from)
            })
            .expect("worker 0 allocates");
        let (sender, receiver) = mpsc::channel();
        let worker1_main = Arc::clone(&main);
        let worker1_thread = thread::spawn(move || {
            worker1_main
                .install_worker(DataWorkerId::new(1))
                .expect("worker 1 installs on its own thread");
            // Worker 1 sees exactly its own binding, never worker 0's.
            let (context, len) = worker1_main
                .with_worker(DataWorkerId::new(1), |worker| {
                    let context = worker
                        .allocate(SessionId::from_raw(20))
                        .map_err(RuntimeError::from)?;
                    assert_eq!(worker.len(), 1);
                    assert_eq!(
                        worker.get(context).expect("live context").session,
                        SessionId::from_raw(20)
                    );
                    Ok((context, worker.len()))
                })
                .expect("worker 1 allocates its own context");
            sender.send((context, len)).expect("relay worker 1 result");
        });
        let (_, worker1_len) = receiver.recv().expect("worker 1 result");
        worker1_thread.join().expect("worker 1 thread completes");
        assert_eq!(worker1_len, 1);
        // Worker 0's pool still holds only its own binding. (Both workers'
        // first allocation occupies slot 0 with generation 1, so the raw ids
        // are equal by design; identity is worker + id, which is why lookup
        // always goes through `with_worker`.)
        main.with_worker(DataWorkerId::new(0), |worker| {
            assert_eq!(worker.len(), 1);
            assert_eq!(
                worker.get(worker0).expect("worker 0 context").session,
                SessionId::from_raw(10)
            );
            Ok(())
        })
        .expect("worker 0 pool untouched by worker 1");
    }

    #[test]
    fn http_worker_init_registration_orders_after_session_and_quic() {
        let registration = __INIT_FN_HTTP_WORKER_INIT;
        assert_eq!(registration.name, "http_worker_init");
        assert_eq!(
            registration.runs_after,
            &["session_worker_init", "quic_worker_init"]
        );
        assert!(registration.runs_before.is_empty());
    }
}

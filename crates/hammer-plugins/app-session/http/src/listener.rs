//! Main Thread-owned HTTP listener authority bootstrap.
//!
//! VPP owns the HTTP transport proto on the main thread: `http_transport_init`
//! (http.c:1867-1903) registers the proto against the session layer and
//! initializes the main-thread `http_main_t`, and `http_transport_enable`
//! (http.c:1023-1063) runs the main-thread attach of the builtin HTTP app.
//! Hammer mirrors that ownership with `HttpMain`, initialized once by
//! `init_http_transport` ordered after QUIC/session init, and exposes the
//! transport through a `start_listen`-only `SessionTransportRegistration`.
//! No inner Application listener, QUIC listen, HTTP3 engine, FIFO, or QPACK
//! state is allocated here; those are later slices; until the nesting slice
//! installs inner listener publication, `start_listen` returns a typed
//! not-ready error instead of reporting a listener that does not exist.

use std::sync::{Arc, OnceLock};
use std::thread::{self, ThreadId};

use hammer_runtime::app::ApplicationId;
use hammer_runtime::{
    Engine, RuntimeError, RuntimeResult, SessionListenEndpoint, SessionListenerId,
    SessionTransportRegistration,
};
use hammer_service::session::runtime::{SessionMain, SessionTransportId};

/// Main Thread-owned HTTP listener authority.
///
/// VPP reference: `http_main_t` is owned by the main vlib thread
/// (http.c:1867-1903). HTTP rides over QUIC, so the authority retains the
/// QUIC transport registration it will need to publish inner listeners,
/// mirroring `QuicMain.udp_transport` (quic listener.rs:100). This slice
/// bootstraps the authority and its `start_listen` validation gate only.
pub struct HttpMain {
    owner: ThreadId,
    sessions: Arc<SessionMain>,
    quic_transport: SessionTransportRegistration,
}

impl HttpMain {
    /// Session-layer transport identity for HTTP.
    pub const TRANSPORT_ID: SessionTransportId = SessionTransportId::new(4);

    pub(crate) fn new(
        sessions: Arc<SessionMain>,
        quic_transport: SessionTransportRegistration,
    ) -> Self {
        Self {
            owner: thread::current().id(),
            sessions,
            quic_transport,
        }
    }

    /// Validates main-thread ownership before any side effect.
    ///
    /// The registered free `start_listen` already ran
    /// `ensure_main_thread_with_barrier`; this owner gate mirrors QUIC's
    /// `QuicMain::with_contexts` re-check (quic listener.rs:216-225) so the
    /// authority stays single-owner even when reached directly. No inner
    /// Application listener or QUIC listen publication exists in this slice
    /// (VPP `http_transport_enable`, http.c:1023-1063, is later work), so a
    /// validated call must not return `Ok`: the session layer keeps the
    /// inserted `SessionListener` record on success (session/runtime.rs:
    /// 900-935), which would leave an externally observable listener with no
    /// backing. VPP's `http_start_listen` (http.c:1287) creates a real
    /// listener; until the nesting slice installs the equivalent publication,
    /// listen attempts fail with a typed not-ready error instead.
    pub(crate) fn start_listen(
        &self,
        _listener: SessionListenerId,
        _application: ApplicationId,
        _config: Option<u64>,
        _endpoint: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        if thread::current().id() != self.owner {
            return Err(RuntimeError::ControlRequiresMainThread);
        }
        Err(RuntimeError::Lifecycle {
            stage: "http start_listen".to_string(),
            message: "inner Application listener publication is not installed yet".to_string(),
        })
    }
}

static HTTP_MAIN: OnceLock<Arc<HttpMain>> = OnceLock::new();

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
/// already resolvable from the plugin main (mirroring
/// `init_quic`, quic listener.rs:487-520). Duplicate initialization is a
/// typed error; the OnceLock keeps the authority single-owner with no lock.
#[hammer_component_macros::init_function(
    name = "http_transport_init",
    runs_after = ["quic_init", "session_init"]
)]
fn init_http_transport(
    engine: &mut Engine,
    sessions: Arc<SessionMain>,
) -> RuntimeResult<Arc<HttpMain>> {
    let quic_transport = engine
        .plugin_main()
        .session_transport("quic")
        .map_err(RuntimeError::from)?;
    let main = Arc::new(HttpMain::new(sessions, quic_transport));
    if HTTP_MAIN.set(Arc::clone(&main)).is_err() {
        return Err(RuntimeError::PluginStateNotInitialized { plugin: "http" });
    }
    Ok(main)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use hammer_runtime::{
        DataPlaneRuntime, DataPlaneRuntimeConfig, DataWorkerId, Engine, PluginError, PluginMain,
        RuntimeRegistry,
    };
    use hammer_service::session::ApplicationMain;

    use super::*;

    fn test_engine() -> Engine {
        Engine::new(
            DataPlaneRuntime::new(DataPlaneRuntimeConfig::default()),
            RuntimeRegistry::new(),
        )
    }

    fn test_sessions() -> Arc<SessionMain> {
        Arc::new(SessionMain::new(
            1,
            ApplicationMain::with_session_apps(8, []),
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

    fn fake_quic_start_listen(
        _: SessionListenerId,
        _: ApplicationId,
        _: Option<u64>,
        _: SessionListenEndpoint,
    ) -> RuntimeResult<()> {
        Ok(())
    }

    static __FAKE_QUIC_TRANSPORT: SessionTransportRegistration =
        SessionTransportRegistration::new("quic", Some(fake_quic_start_listen), None, None);

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
    fn http_listener_start_listen_validates_state_before_side_effects() {
        let mut engine = test_engine();
        engine.install_current();
        let sessions = test_sessions();

        // Missing authority: typed error before any side effect.
        let error = start_error(super::start_listen(
            SessionListenerId::new(1, 0),
            ApplicationId::new(0, 0),
            None,
            test_endpoint(),
        ));
        assert!(matches!(
            error,
            RuntimeError::PluginStateNotInitialized { plugin: "http" }
        ));

        // Init publishes the authority only after the QUIC transport is
        // resolvable, and stores exactly the SessionMain and the fetched
        // Copy QUIC registration.
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

        // Duplicate init is a typed error, not a panic or overwrite.
        let error = init_error(init_http_transport(&mut engine, sessions));
        assert!(matches!(
            error,
            RuntimeError::PluginStateNotInitialized { plugin: "http" }
        ));

        // A validated listen must not report a listener that does not exist:
        // the session layer keeps the SessionListener record on Ok
        // (session/runtime.rs:900-935), so the bootstrap authority returns a
        // typed not-ready error until the nesting slice installs publication.
        let error = start_error(super::start_listen(
            SessionListenerId::new(4, 0),
            ApplicationId::new(0, 0),
            None,
            test_endpoint(),
        ));
        assert!(matches!(
            error,
            RuntimeError::Lifecycle { stage, .. } if stage == "http start_listen"
        ));

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
        let main = Arc::new(HttpMain::new(
            test_sessions(),
            SessionTransportRegistration::new("quic", Some(fake_quic_start_listen), None, None),
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
}

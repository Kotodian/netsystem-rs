//! Builtin HTTP Session App registration over QUIC sessions.
//!
//! VPP reference: `third_party/vpp/src/plugins/http/http.c`. The HTTP
//! transport proto's `http_transport_enable` attaches the builtin Session App
//! named "http" with the static `session_cb_vft_t http_app_cb_vft`
//! (http.c:1004-1017, attach at http.c:1049-1050 with
//! `APP_OPTIONS_FLAGS_IS_BUILTIN`). This slice owns the plugin descriptor and
//! the Session App registration whose `install` path hands `SessionMain` a
//! static callback table, plus the `accept` callback: it resolves the owning
//! `ThreadOwned<HttpWorker>` by Session worker id, allocates exactly one O(1)
//! `ConnectionContext` bound to the accepted lower QUIC Session, and
//! publishes its `ContextId` through `SessionWorker::set_app_session`,
//! mirroring VPP `http_ts_accept_connection` (http.c:586-673). Upward
//! `SessionTransport` registration, listener/connect, the HTTP3 engine, FIFO
//! transfer/publication, QPACK, and worker contexts are later slices; no
//! callback that needs their lifecycle state is installed here.

use hammer_infra::pool::Index;
use hammer_runtime::app::{SessionAppContext, SessionAppRegistration};
use hammer_runtime::{DataWorkerId, Engine, RuntimeError, RuntimeResult};
use hammer_service::session::SessionId;
use hammer_service::session::protocol::SessionAppCallbacks;
use hammer_service::session::runtime::SessionWorker;

use crate::listener::{HTTP_MAIN, HttpMain};

pub(crate) const NAME: &str = "http";

/// Static callback table passed to `SessionMain::install_session_app`,
/// mirroring VPP's static `http_app_cb_vft` (http.c:1004-1017).
///
/// `accept` is wired: it needs nothing beyond the per-worker context pool
/// (`HttpWorker::allocate`/`remove`, listener.rs) and the Session worker's
/// `set_app_session` publication. Every other VPP callback routes through
/// HTTP worker connection state (`http_ts_*` over `http_ctx_t`) that its
/// owning slice does not provide yet; installing speculative no-ops would be
/// wrong without that state. Deferred to the HTTP3/lifecycle slice:
/// `connected`, `disconnect`, `reset`, `transport_closed`, `cleanup`,
/// `half_open_cleanup`, `builtin_rx`, `builtin_tx`. `add_segment` /
/// `del_segment` stay `None` too: VPP's builtin entries are no-ops that are
/// never invoked without shared-memory segments (http.c:997-1003), so the
/// `SessionApp` trait's `Ok(())` default is behaviorally identical.
pub(crate) static CALLBACKS: SessionAppCallbacks = SessionAppCallbacks {
    accept: Some(accept),
    ..SessionAppCallbacks::all_none()
};

/// Session App `accept` for an accepted lower QUIC Session.
///
/// Mirrors the ownership/order of VPP `http_ts_accept_connection`
/// (http.c:586-673): the Session resolves the owning worker, one context is
/// allocated bound to the Session, and only then is the connection state
/// published. Here the per-worker `HttpWorker` slot is resolved by the
/// Session's `DataWorkerId` (`HttpMain::with_worker`, listener.rs) and the
/// publication is the Session worker's opaque `set_app_session`, with the
/// allocated context removed directly when publication fails so the primary
/// typed error is preserved (QUIC publish rollback, quic
/// session_app.rs:37-49).
pub(crate) fn accept(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    let main = HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?;
    accept_on(&main, worker, session, context)
}

/// `accept` on a caller-resolved authority, so unit tests own their `HttpMain`
/// without the process-wide `HTTP_MAIN` OnceLock, exactly as the listener
/// tests bypass it (listener.rs:1071).
pub(crate) fn accept_on(
    main: &HttpMain,
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context != 0 {
        // A previous accept already published this Session's context; the
        // dispatch layer passes it back on every callback, so a duplicate
        // accept is idempotent.
        return Ok(());
    }
    let context = main.with_worker(worker.worker(), |http| {
        http.allocate(session).map_err(RuntimeError::from)
    })?;
    if let Err(error) = worker.set_app_session(session, u64::from(context)) {
        // Rollback is best effort and must not replace the primary
        // set_app_session error, mirroring the QUIC publish path
        // (quic session_app.rs:42-47).
        let _ = main.with_worker(worker.worker(), |http| {
            http.remove(context).map_err(RuntimeError::from)
        });
        return Err(error);
    }
    Ok(())
}

/// Installs the builtin HTTP Session App on every worker; mirrors VPP
/// `vnet_application_attach` of the builtin "http" app (http.c:1049-1062).
/// Errors are the typed `RuntimeError` values of the registry and the
/// Session App installer, propagated unchanged.
pub(crate) fn install(engine: &mut Engine) -> RuntimeResult<()> {
    let main = engine
        .registry
        .require::<hammer_service::session::runtime::SessionMain>()?;
    main.install_session_app(&engine.runtime, NAME, &CALLBACKS)
}

/// Teardown hook for the registration image. Per-Session context removal is
/// owned by the lifecycle slice (the `cleanup` callback); the hook stays a
/// no-op until then.
pub(crate) fn destroy(_worker: DataWorkerId, _context: SessionAppContext) {}

pub(crate) static HTTP_SESSION_APP: SessionAppRegistration =
    SessionAppRegistration::new(NAME, install, destroy);

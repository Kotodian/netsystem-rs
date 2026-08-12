//! Builtin HTTP Session App registration over QUIC sessions.
//!
//! VPP reference: `third_party/vpp/src/plugins/http/http.c`. The HTTP
//! transport proto's `http_transport_enable` attaches the builtin Session App
//! named "http" with the static `session_cb_vft_t http_app_cb_vft`
//! (http.c:1004-1017, attach at http.c:1049-1050 with
//! `APP_OPTIONS_FLAGS_IS_BUILTIN`). This slice owns only that attach seam:
//! the plugin descriptor and the Session App registration whose `install`
//! path hands `SessionMain` a static callback table. Upward
//! `SessionTransport` registration, listener/connect, the HTTP3 engine,
//! FIFO transfer/publication, QPACK, and worker contexts are later slices;
//! no callback that needs their lifecycle state is installed here.

use hammer_runtime::app::{SessionAppContext, SessionAppRegistration};
use hammer_runtime::{DataWorkerId, Engine, RuntimeResult};
use hammer_service::session::protocol::SessionAppCallbacks;

pub(crate) const NAME: &str = "http";

/// Static callback table passed to `SessionMain::install_session_app`,
/// mirroring VPP's static `http_app_cb_vft` (http.c:1004-1017).
///
/// Every entry is `None`. Each VPP callback routes through HTTP worker
/// connection state (`http_ts_*` over `http_ctx_t`) that its owning slice
/// does not provide yet; installing speculative no-ops would be wrong without
/// that state. Deferred to the listener/worker-context/HTTP3 slice: `accept`,
/// `connected`, `disconnect`, `reset`, `transport_closed`, `cleanup`,
/// `half_open_cleanup`, `builtin_rx`, `builtin_tx`. `add_segment` /
/// `del_segment` stay `None` too: VPP's builtin entries are no-ops that are
/// never invoked without shared-memory segments (http.c:997-1003), so the
/// `SessionApp` trait's `Ok(())` default is behaviorally identical.
pub(crate) static CALLBACKS: SessionAppCallbacks = SessionAppCallbacks::all_none();

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

/// Teardown hook for the registration image. No HTTP worker context exists
/// in this slice, so there is nothing to remove; the hook stays a no-op until
/// the worker-context slice owns contexts.
pub(crate) fn destroy(_worker: DataWorkerId, _context: SessionAppContext) {}

pub(crate) static HTTP_SESSION_APP: SessionAppRegistration =
    SessionAppRegistration::new(NAME, install, destroy);

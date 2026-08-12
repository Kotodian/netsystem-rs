//! Builtin HTTP Session App registration over QUIC sessions.
//!
//! VPP reference: `third_party/vpp/src/plugins/http/http.c`. The HTTP
//! transport proto's `http_transport_enable` attaches the builtin Session App
//! named "http" with the static `session_cb_vft_t http_app_cb_vft`
//! (http.c:1004-1017, attach at http.c:1049-1050 with
//! `APP_OPTIONS_FLAGS_IS_BUILTIN`). This slice owns the plugin descriptor and
//! the Session App registration whose `install` path hands `SessionMain` a
//! static callback table, plus the `accept` callback: it resolves the owning
//! `ThreadOwned<HttpWorker>` by Session worker id, branches on the accepted
//! Session's metadata exactly as VPP `http_ts_accept_callback`
//! (http.c:733-740) branches on `SESSION_F_STREAM`, and allocates exactly
//! one O(1) context — a `ConnectionContext` for a root
//! (`http_ts_accept_connection`, http.c:586-673) or a `StreamContext` bound
//! to its parent connection for a stream child (`http_ts_accept_stream`,
//! http.c:675-721) — then publishes its `ContextId`/`StreamContextId`
//! through `SessionWorker::set_app_session`. A root connection additionally
//! bootstraps its local uni control stream exactly once
//! (`HttpWorker::bootstrap_control_stream`, mirroring `http3_conn_init`,
//! http3.c:216-250), rolling the accept back on failure as
//! `http3_conn_terminate` does (http3.c:152-165). Upward
//! `SessionTransport` registration, listener/connect, the HTTP3 engine, FIFO
//! transfer/publication, QPACK, and worker contexts are later slices; no
//! callback that needs their lifecycle state is installed here.

use hammer_infra::pool::Index;
use hammer_runtime::app::{SessionAppContext, SessionAppRegistration, SessionFlags};
use hammer_runtime::session::{SessionApplicationErrorCode, SessionStreamDirection};
use hammer_runtime::{DataWorkerId, Engine, RuntimeError, RuntimeResult};
use hammer_service::session::SessionId;
use hammer_service::session::protocol::SessionAppCallbacks;
use hammer_service::session::runtime::{SessionAcceptMetadata, SessionWorker};

use crate::listener::{HTTP_MAIN, HttpMain};
use crate::worker::ContextId;

pub(crate) const NAME: &str = "http";

/// VPP `HTTP3_ERROR_INTERNAL_ERROR` (http3.h:17): the app error code
/// `http3_conn_init` carries on the connection close that rolls back a
/// failed control-stream open (http3.c:228).
const H3_INTERNAL_ERROR: u64 = 0x0102;

/// Static callback table passed to `SessionMain::install_session_app`,
/// mirroring VPP's static `http_app_cb_vft` (http.c:1004-1017).
///
/// `accept` is wired: it needs nothing beyond the per-worker context pools
/// (`HttpWorker::allocate`/`bootstrap_control_stream`/`allocate_stream`/
/// `remove`/`remove_stream`, worker.rs) and the Session worker's
/// `set_app_session` publication plus the typed transport actions
/// (`open_stream`/`close_connection`, runtime.rs). Every other VPP callback
/// routes through
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

/// Typed errors of the builtin HTTP Session App accept path that the Session
/// Worker and `HttpWorker` errors cannot express: a stream child whose accept
/// metadata carries no parent connection context, so no `ContextId` exists to
/// name in `HttpWorkerError::ParentContextMissing`.
#[hammer_component_macros::runtime_error(subsystem = "http")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HttpAppError {
    #[error("http stream accept requires a live parent connection context, session {session:?} has none")]
    StreamParentMissing { session: SessionId },
}

/// Session App `accept` for an accepted lower QUIC Session.
///
/// Mirrors the ownership/order of VPP `http_ts_accept_callback`
/// (http.c:733-740): the Session's role is read from its accept metadata and
/// dispatched to `http_ts_accept_connection` (http.c:586-673) for roots or
/// `http_ts_accept_stream` (http.c:675-721) for stream children. In both
/// paths the Session resolves the owning worker, one context is allocated
/// bound to the Session, and only then is the connection state published.
/// For roots the published `ConnectionContext` additionally bootstraps its
/// local uni control stream exactly once
/// (`HttpWorker::bootstrap_control_stream`, mirroring `http3_conn_init`,
/// http3.c:216-250), and a bootstrap failure rolls the accept back as
/// `http3_conn_terminate` does (http3.c:152-165).
/// Here the per-worker `HttpWorker` slot is resolved by the Session's
/// `DataWorkerId` (`HttpMain::with_worker`, listener.rs) and the publication
/// is the Session worker's opaque `set_app_session`, with the allocated
/// context removed directly when publication fails so the primary typed
/// error is preserved (QUIC publish rollback, quic session_app.rs:37-49).
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
        // accept is idempotent for both connection and stream roles.
        return Ok(());
    }
    // VPP `http_ts_accept_callback` (http.c:733-740) branches on
    // `SESSION_F_STREAM`; `worker.accept_metadata` supplies the flags and,
    // for stream children, the parent connection's published context
    // (runtime.rs, `SessionAcceptMetadata`).
    let Some(metadata) = worker.accept_metadata(session) else {
        // The Session is not live in the worker pool, so no role metadata
        // exists to branch on. Fall back to the connection path: the
        // allocation succeeds and `set_app_session` reports the typed
        // SessionMissing, rolling the context back — the same observable
        // behavior the connection path always had for unknown Sessions.
        return accept_connection(main, worker, session);
    };
    if metadata.flags.contains(SessionFlags::STREAM) {
        accept_stream(main, worker, session, metadata)
    } else {
        accept_connection(main, worker, session)
    }
}

/// `accept` dispatch for a root Session, mirroring VPP
/// `http_ts_accept_connection` (http.c:586-673) plus the control-stream
/// bootstrap of `http3_conn_accept_callback` (http3.c:2358-2374, via
/// `http3_conn_init` http3.c:216-250).
fn accept_connection(
    main: &HttpMain,
    worker: &mut SessionWorker<Index>,
    session: SessionId,
) -> RuntimeResult<()> {
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
    if let Err(error) = main.with_worker(worker.worker(), |http| {
        // The local uni control stream is opened exactly once per accepted
        // connection, with the published `ContextId` as its app context,
        // mirroring `http3_conn_init`'s first stream (http3.c:223-234).
        http.bootstrap_control_stream(context, worker, u64::from(context))
            .map_err(RuntimeError::from)
    }) {
        // VPP `http3_conn_terminate` (http3.c:152-165) after a failed
        // control-stream open (http3.c:228): roll the direct owner/index
        // paths back in reverse order, each best effort so the primary
        // bootstrap error survives. The typed open action owns rollback of
        // any child it created; then the published app context is cleared,
        // the connection context released, and the lower connection closed
        // with the VPP H3_INTERNAL_ERROR code.
        let _ = worker.set_app_session(session, 0);
        let _ = main.with_worker(worker.worker(), |http| {
            http.remove(context).map_err(RuntimeError::from)
        });
        let _ = worker.close_connection(
            session,
            SessionApplicationErrorCode::from(H3_INTERNAL_ERROR),
            &[],
        );
        return Err(error);
    }
    Ok(())
}

/// `accept` dispatch for one stream child, mirroring VPP
/// `http_ts_accept_stream` (http.c:675-721): the parent connection context is
/// required and converted to a generation-checked `ContextId`, then one
/// `StreamContext` is allocated in the worker's stream pool bound to the
/// child Session with the direction derived from `SESSION_F_UNIDIRECTIONAL`
/// (http.c:688-690), and finally the `StreamContextId` is published through
/// `set_app_session`. Parent liveness lives in `HttpWorker::allocate_stream`
/// (worker.rs), which rejects stale or foreign parent identities with
/// `ParentContextMissing`; on publication failure the allocated stream
/// context is removed directly, preserving the primary typed error exactly
/// as the connection path does.
fn accept_stream(
    main: &HttpMain,
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    metadata: SessionAcceptMetadata,
) -> RuntimeResult<()> {
    let parent = ContextId::from(
        metadata
            .parent_app_context
            .ok_or(HttpAppError::StreamParentMissing { session })?,
    );
    let direction = if metadata.flags.contains(SessionFlags::UNIDIRECTIONAL) {
        SessionStreamDirection::Uni
    } else {
        SessionStreamDirection::Bidi
    };
    let stream = main.with_worker(worker.worker(), |http| {
        http.allocate_stream(session, parent, direction)
            .map_err(RuntimeError::from)
    })?;
    if let Err(error) = worker.set_app_session(session, u64::from(stream)) {
        // Rollback is best effort and must not replace the primary
        // set_app_session error, mirroring the QUIC publish path
        // (quic session_app.rs:42-47).
        let _ = main.with_worker(worker.worker(), |http| {
            http.remove_stream(stream).map_err(RuntimeError::from)
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

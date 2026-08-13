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
//! transfer/publication, QPACK, and worker contexts are later slices; the
//! `disconnect` callback is installed because the worker slice already
//! provides the peer stream slots and `finish_peer_*`/`remove_stream`
//! helpers it needs, and the `reset` callback terminates the parent
//! connection on a peer uni stream RESET through the committed
//! `classify_peer_uni_stream_reset` classification and the generic
//! `close_connection` action, while every other callback still waits on
//! lifecycle state its owning slice has not landed.

use hammer_infra::pool::Index;
use hammer_runtime::app::{SessionAppContext, SessionAppRegistration, SessionFlags};
use hammer_runtime::session::{SessionApplicationErrorCode, SessionStreamDirection};
use hammer_runtime::{DataWorkerId, Engine, RuntimeError, RuntimeResult};
use hammer_service::session::{SessionEndpointRole, SessionId};
use hammer_service::session::protocol::SessionAppCallbacks;
use hammer_service::session::runtime::{SessionAcceptMetadata, SessionWorker};

use crate::http3::proto::error::ErrorCode;
use crate::http3::request::RequestPublishError;
use crate::http3::request_frame_reader::RequestFrameError;
use crate::http_common::PublishError;
use crate::listener::{HTTP_MAIN, HttpMain};
use crate::worker::{
    ContextId, PeerControlError, PeerUniStreamRole, RequestReadError, StreamContextId,
};

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
/// (`open_stream`/`close_connection`, runtime.rs). `disconnect` is wired too:
/// a peer HTTP/3 uni stream FIN resolves the worker and finishes the stream
/// by role through the worker helpers this slice already provides. `reset`
/// is wired as well: a peer HTTP/3 uni stream RESET terminates the parent
/// connection with `ClosedCriticalStream` (0x0104) through the worker's
/// committed `classify_peer_uni_stream_reset` classification and the generic
/// `close_connection` transport action, mutating no HTTP worker state here
/// (VPP `http3_transport_stream_reset_callback`). Every other VPP callback
/// routes through HTTP worker connection state
/// (`http_ts_*` over `http_ctx_t`) that its owning slice does not provide
/// yet; installing speculative no-ops would be wrong without that state.
/// Deferred to the HTTP3/lifecycle slice:
/// `connected`, `transport_closed`, `cleanup`,
/// `half_open_cleanup`, `builtin_rx`, `builtin_tx`. `add_segment` /
/// `del_segment` stay `None` too: VPP's builtin entries are no-ops that are
/// never invoked without shared-memory segments (http.c:997-1003), so the
/// `SessionApp` trait's `Ok(())` default is behaviorally identical.
pub(crate) static CALLBACKS: SessionAppCallbacks = SessionAppCallbacks {
    accept: Some(accept),
    disconnect: Some(disconnect),
    reset: Some(reset),
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
    #[error(
        "peer control stream finish reported a SETTINGS protocol error, which is structurally impossible; HTTP/3 code {code:?}"
    )]
    PeerControlFinishProtocol { code: ErrorCode },
    #[error(
        "bidi request FIN arrived before the declared body was fully received; HTTP/3 code {code:?}"
    )]
    RequestFinishProtocol { code: ErrorCode },
    #[error("HTTP/3 request publication failed: {error:?}")]
    RequestPublish { error: RequestPublishError },
    #[error("HTTP/3 request frame processing failed; code {code:?}")]
    RequestFrameProtocol { code: ErrorCode },
}

impl From<RequestPublishError> for HttpAppError {
    fn from(error: RequestPublishError) -> Self {
        Self::RequestPublish { error }
    }
}

impl From<RequestFrameError> for HttpAppError {
    fn from(error: RequestFrameError) -> Self {
        Self::RequestFrameProtocol { code: error.error_code() }
    }
}

/// Recovery decision for a failed HTTP/3 request publication, mirroring
/// VPP's stream-vs-connection error split (http3.c:1107-1113): QPACK
/// decompression failure is a connection error that closes the connection,
/// message/protocol/internal errors are stream errors that reset the
/// request stream, and FIFO capacity exhaustion is transient backpressure
/// the caller retries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestErrorAction {
    CloseConnection { code: ErrorCode },
    ResetStream { code: ErrorCode },
    Retry,
}

#[allow(dead_code)] // consumed by the builtin_rx wiring slice
pub(crate) fn request_publish_error_action(error: RequestPublishError) -> RequestErrorAction {
    match error {
        RequestPublishError::QpackDecompressionFailed => RequestErrorAction::CloseConnection {
            code: ErrorCode::QpackDecompressionFailed,
        },
        RequestPublishError::MessageError => RequestErrorAction::ResetStream {
            code: ErrorCode::MessageError,
        },
        RequestPublishError::GeneralProtocolError => RequestErrorAction::ResetStream {
            code: ErrorCode::GeneralProtocolError,
        },
        RequestPublishError::InternalError => RequestErrorAction::ResetStream {
            code: ErrorCode::InternalError,
        },
        RequestPublishError::Publish(PublishError::Capacity { .. }) => RequestErrorAction::Retry,
        RequestPublishError::Publish(PublishError::Encode(_) | PublishError::Fifo(_)) => {
            RequestErrorAction::ResetStream { code: ErrorCode::InternalError }
        }
    }
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
        // exists to branch on. Fall back to the connection path with role
        // `None`: the allocation succeeds and `set_app_session` reports the
        // typed SessionMissing, rolling the context back — the same
        // observable behavior the connection path always had for unknown
        // Sessions.
        return accept_connection(main, worker, session, None);
    };
    if metadata.flags.contains(SessionFlags::STREAM) {
        accept_stream(main, worker, session, metadata)
    } else {
        accept_connection(main, worker, session, metadata.role)
    }
}

/// `accept` dispatch for a root Session, mirroring VPP
/// `http_ts_accept_connection` (http.c:586-673) plus the control-stream
/// bootstrap of `http3_conn_accept_callback` (http3.c:2358-2374, via
/// `http3_conn_init` http3.c:216-250). The allocated connection context
/// records the accept-metadata endpoint role (worker.rs
/// `allocate_with_role`); the missing-metadata fallback passes `None`.
fn accept_connection(
    main: &HttpMain,
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    role: Option<SessionEndpointRole>,
) -> RuntimeResult<()> {
    let context = main.with_worker(worker.worker(), |http| {
        http.allocate_with_role(session, role).map_err(RuntimeError::from)
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

/// Session App `disconnect` for a peer HTTP/3 unidirectional stream FIN.
///
/// The lower QUIC transport dispatches a received FIN synchronously as this
/// callback with the Session's published `SessionAppContext` naming the
/// generation-checked `StreamContextId`; a RESET is dispatched separately
/// through the `reset` callback. The owning `HttpWorker`
/// is resolved by the Session worker id (`HttpMain::with_worker`,
/// listener.rs) and the stream context is generation/session-checked with
/// `HttpWorker::get_stream_for_session` before any mutation, exactly as
/// `accept` resolves the worker and checks its metadata.
///
/// This callback owns peer unidirectional streams only; the dispatch mirrors
/// VPP `http3_transport_stream_close_callback` → `http3_stream_close`:
/// EOF on the peer control stream clears its slot and SETTINGS reader
/// (`finish_peer_control_stream`), EOF on a peer QPACK encoder/decoder
/// clears that slot (`finish_peer_qpack_stream`), and EOF on an Unknown
/// stream is silent (`finish_peer_unknown_stream`). EOF on a Push or
/// type-not-yet-decoded stream owns no slot, so the stream context is
/// released directly through the existing generation-checked
/// `HttpWorker::remove_stream`; no new worker code is added here. A
/// bidi/request FIN completes the request: the declared body is validated
/// finished (`validate_request_finish`), the upper request Session is
/// removed, and the request stream context is released, in VPP
/// `http3_stream_cleanup_callback`'s delete-notify-then-free order
/// (http3.c:2459-2462). The
/// root connection's own Disconnected callback is a clean no-op: only a
/// stream child carries `SessionFlags::STREAM` in its accept metadata, and
/// the root's published `ConnectionContextId` shares the packed
/// slot|generation layout with a `StreamContextId` in an unrelated pool, so
/// it is never interpreted as a stream. No FIFO is drained — the peer's
/// final bytes stay readable — and the whole path is O(1) with no scan,
/// allocation, lock, channel, or async work. Errors propagate typed through
/// the existing `RuntimeError` conversion; there is no connection-close
/// action here.
pub(crate) fn disconnect(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    let main = HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?;
    disconnect_on(&main, worker, session, context)
}

/// `disconnect` on a caller-resolved authority, so unit tests own their
/// `HttpMain` without the process-wide `HTTP_MAIN` OnceLock, exactly as
/// `accept_on` bypasses it (listener.rs:1071).
pub(crate) fn disconnect_on(
    main: &HttpMain,
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context == 0 {
        // A zero context names no published stream; the callback is a
        // lifecycle fallback no-op, consistent with the accept and destroy
        // fallbacks.
        return Ok(());
    }
    // The root HTTP/3 connection's Disconnected callback carries the
    // published `ConnectionContextId`, which shares the packed
    // slot|generation layout with a `StreamContextId` in an unrelated pool;
    // the raw value is never a stream identity by itself. The accept
    // metadata distinguishes the roles exactly as VPP
    // `http_ts_accept_callback` branches on `SESSION_F_STREAM`
    // (http.c:733-740): only a stream child carries
    // `SessionFlags::STREAM`, so a root connection disconnect — and a
    // Session with no live metadata, which cannot own a stream context —
    // is a clean no-op in this peer-uni FIN seam.
    let Some(metadata) = worker.accept_metadata(session) else {
        return Ok(());
    };
    if !metadata.flags.contains(SessionFlags::STREAM) {
        return Ok(());
    }
    let stream = StreamContextId::from(context);
    let direction = main.with_worker(worker.worker(), |http| {
        let stream_context = http
            .get_stream_for_session(stream, session)
            .map_err(RuntimeError::from)?;
        Ok(stream_context.direction)
    })?;
    if direction != SessionStreamDirection::Uni {
        // Bidi/request FIN completes the request: the declared body must be
        // fully received before any cleanup (`validate_request_finish`, a
        // typed `RequestFinishProtocol` on a short body), then the upper
        // request Session is removed before the request stream context is
        // freed, matching VPP `http3_stream_cleanup_callback`'s
        // delete-notify-then-free order (http3.c:2459-2462). The upper
        // removal happens outside the worker borrow, so its typed error
        // aborts the cleanup before the stream is released.
        main.with_worker(worker.worker(), |http| {
            http.validate_request_finish(stream, session)
                .map_err(|error| match error {
                    RequestReadError::Worker(inner) => RuntimeError::from(inner),
                    RequestReadError::Protocol(error) => RuntimeError::from(
                        HttpAppError::RequestFinishProtocol { code: error.error_code() },
                    ),
                })
        })?;
        if let Some(upper) = worker.upper_session(session) {
            worker.remove_upper_session(upper)?;
        }
        main.with_worker(worker.worker(), |http| {
            http.release_request_stream(stream, session).map_err(RuntimeError::from)
        })?;
        return Ok(());
    }
    main.with_worker(worker.worker(), |http| {
        let peer_role = http
            .get_stream_for_session(stream, session)
            .map_err(RuntimeError::from)?
            .peer_role;
        match peer_role {
            PeerUniStreamRole::Control => http
                .finish_peer_control_stream(stream)
                .map_err(|error| match error {
                    // The finish path only reports worker liveness failures,
                    // preserved as the typed `HttpWorkerError` unchanged.
                    PeerControlError::Worker(inner) => RuntimeError::from(inner),
                    // `PeerControlError::Protocol` is shared with the
                    // byte-processing path (`process_peer_control_bytes`) but
                    // is structurally impossible from a FIN:
                    // `finish_peer_control_stream` never parses SETTINGS, it
                    // only clears the registered slot and reader. The
                    // exhaustive conversion stays typed — naming the HTTP/3
                    // connection error code rather than mislabeling the arm
                    // as a lifecycle failure — and adds no connection policy.
                    PeerControlError::Protocol(error) => {
                        let code = error.error_code();
                        RuntimeError::from(HttpAppError::PeerControlFinishProtocol { code })
                    }
                }),
            PeerUniStreamRole::QpackEncoder | PeerUniStreamRole::QpackDecoder => http
                .finish_peer_qpack_stream(stream)
                .map_err(RuntimeError::from),
            PeerUniStreamRole::Unknown => http
                .finish_peer_unknown_stream(stream)
                .map_err(RuntimeError::from),
            // VPP's default arm records no slot for these (http3.c:1723-1725
            // and the push policy http3.c:1712-1722), so EOF releases the
            // stream context directly; `remove_stream` is generation-checked
            // and frees the SETTINGS reader only if this stream was the
            // registered peer control stream, which it cannot be here.
            PeerUniStreamRole::Push | PeerUniStreamRole::Unclassified => http
                .remove_stream(stream)
                .map_err(RuntimeError::from),
        }
    })
}

/// Session App `reset` for a peer HTTP/3 unidirectional stream RESET.
///
/// The lower QUIC transport dispatches a received RESET synchronously as
/// this callback with the Session's published `SessionAppContext` naming the
/// generation-checked `StreamContextId`, and the owning `HttpWorker` is
/// resolved by the Session worker id exactly as `disconnect` does. The
/// stream context is generation/session-checked with
/// `HttpWorker::get_stream_for_session` before anything else; a bidi/request
/// stream RESET aborts the request: the upper request Session is removed and
/// the request stream context is released with no finish validation, no
/// connection close, and no stream reset — VPP
/// `http3_transport_stream_reset_callback` checks only unidirectional-ness.
/// For a peer uni stream the committed
/// `HttpWorker::classify_peer_uni_stream_reset` copies the stream/parent/
/// session identities plus the constant `ClosedCriticalStream` error code
/// (0x0104), mutating nothing, and the parent connection is then closed
/// exactly once through `SessionWorker::close_connection`; the Session
/// app-close guard (`on_app_close_dispatch`) makes repeated dispatch a
/// no-op, so no stream reset is sent and no HTTP worker state is mutated or
/// cleaned up here (VPP dispatches the connection error before any stream
/// cleanup). Errors from the metadata check, the classification, and the
/// transport action propagate typed through the existing `RuntimeError`
/// conversion. The whole path is O(1) with no scan, allocation, lock,
/// channel, or async work.
pub(crate) fn reset(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    let main = HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?;
    reset_on(&main, worker, session, context)
}

/// `reset` on a caller-resolved authority, so unit tests own their `HttpMain`
/// without the process-wide `HTTP_MAIN` OnceLock, exactly as `accept_on`
/// bypasses it (listener.rs:1071).
pub(crate) fn reset_on(
    main: &HttpMain,
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context == 0 {
        // A zero context names no published stream; the callback is a
        // lifecycle fallback no-op, consistent with the accept, disconnect,
        // and destroy fallbacks.
        return Ok(());
    }
    let stream = StreamContextId::from(context);
    let direction = main.with_worker(worker.worker(), |http| {
        let stream_context = http
            .get_stream_for_session(stream, session)
            .map_err(RuntimeError::from)?;
        Ok(stream_context.direction)
    })?;
    if direction != SessionStreamDirection::Uni {
        // Bidi/request RESET aborts the request: no finish validation, the
        // upper request Session is removed, and the request stream context
        // is released; the parent connection is never closed and no stream
        // reset is sent, matching VPP `http3_transport_stream_reset_callback`
        // returning before any error recording for non-uni streams.
        if let Some(upper) = worker.upper_session(session) {
            worker.remove_upper_session(upper)?;
        }
        main.with_worker(worker.worker(), |http| {
            http.release_request_stream(stream, session).map_err(RuntimeError::from)
        })?;
        return Ok(());
    }
    main.with_worker(worker.worker(), |http| {
        let reset = http
            .classify_peer_uni_stream_reset(stream)
            .map_err(RuntimeError::from)?;
        worker
            .close_connection(
                reset.session,
                SessionApplicationErrorCode::from(reset.error_code.value()),
                &[],
            )
            .map_err(RuntimeError::from)?;
        Ok(())
    })
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

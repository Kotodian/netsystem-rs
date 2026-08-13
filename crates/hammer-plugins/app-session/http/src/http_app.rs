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
//! `close_connection` action, and the `cleanup` callback frees the HTTP
//! context when the SessionWorker removes the Session entry, mirroring VPP
//! `http3_conn_cleanup_callback`/`http3_stream_cleanup_callback` — the root
//! connection context is removed before the published app session is
//! cleared, and a bidi request stream's upper request Session is removed
//! before its request stream context is released — and the `builtin_rx`
//! callback publishes completed request HEADERS sections and DATA payloads
//! from a bidi request stream's lower RX FIFO to its upper request Session
//! RX FIFO (VPP `http3_stream_transport_rx_req` feeding the transport
//! bytes, http3.c:1733-1799, and `http3_req_state_transport_io_more_data`
//! forwarding DATA to the app FIFO, http3.c:1184-1263), while every other
//! callback still waits on lifecycle state its owning slice has not landed.

use hammer_infra::pool::Index;
use hammer_runtime::app::{SessionAppContext, SessionAppRegistration, SessionFlags};
use hammer_runtime::session::{SessionApplicationErrorCode, SessionStreamDirection};
use hammer_runtime::{DataWorkerId, Engine, RuntimeError, RuntimeResult};
use hammer_service::session::error::SessionError;
use hammer_service::session::protocol::SessionAppCallbacks;
use hammer_service::session::runtime::{SessionAcceptMetadata, SessionWorker};
use hammer_service::session::{SessionEndpointRole, SessionId};

use crate::http_common::PublishError;
use crate::http3::proto::error::ErrorCode;
use crate::http3::request::{RequestPublishError, publish_request_field_section};
use crate::http3::request_frame_reader::{RequestFrameError, RequestFrameRead};
use crate::listener::{HTTP_MAIN, HttpMain};
use crate::worker::{
    ContextId, HttpWorkerError, PeerControlError, PeerUniStreamRole, PendingFieldSection,
    RequestReadError, StreamContextId,
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
/// (VPP `http3_transport_stream_reset_callback`). `cleanup` is wired too: it
/// frees the Session App's HTTP context when the SessionWorker removes the
/// Session entry (`remove_session`/`remove_upper_session`), mirroring VPP
/// `http3_conn_cleanup_callback` (http3.c:2425-2436) and
/// `http3_stream_cleanup_callback` (http3.c:2440-2462): the root connection
/// context is removed before the published app session is cleared, and a bidi
/// request stream's upper request Session is removed before the request
/// stream context is released; an upper request Session, a uni stream, and a
/// zero context are no-ops. `builtin_rx` is wired too: newly readable bytes
/// on a bidi request stream's lower RX FIFO are fed to the per-stream
/// request reader, and a completed HEADERS section — then each DATA frame's
/// payload — is published all-or-nothing to the upper request Session's RX
/// FIFO, with exactly the reader-consumed lower bytes dequeued after the
/// upper commit (VPP `http3_stream_transport_rx_req`, http3.c:1733-1799,
/// and `http3_req_state_transport_io_more_data`, http3.c:1184-1263); root,
/// peer-uni, and context-less dispatches are no-ops. Every other VPP
/// callback routes through HTTP worker connection state (`http_ts_*` over
/// `http_ctx_t`) that its owning slice does not provide yet; installing
/// speculative no-ops would be wrong without that state. Deferred to the
/// HTTP3/lifecycle slice: `connected`, `transport_closed`,
/// `half_open_cleanup`, `builtin_tx`. `add_segment` /
/// `del_segment` stay `None` too: VPP's builtin entries are no-ops that are
/// never invoked without shared-memory segments (http.c:997-1003), so the
/// `SessionApp` trait's `Ok(())` default is behaviorally identical.
pub(crate) static CALLBACKS: SessionAppCallbacks = SessionAppCallbacks {
    accept: Some(accept),
    disconnect: Some(disconnect),
    reset: Some(reset),
    cleanup: Some(cleanup),
    builtin_rx: Some(builtin_rx),
    ..SessionAppCallbacks::all_none()
};

/// Typed errors of the builtin HTTP Session App accept path that the Session
/// Worker and `HttpWorker` errors cannot express: a stream child whose accept
/// metadata carries no parent connection context, so no `ContextId` exists to
/// name in `HttpWorkerError::ParentContextMissing`.
#[hammer_component_macros::runtime_error(subsystem = "http")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum HttpAppError {
    #[error(
        "http stream accept requires a live parent connection context, session {session:?} has none"
    )]
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
    #[error(
        "retained request field section missing from its worker slot; request stream {stream:?}"
    )]
    PendingFieldSectionLost { stream: StreamContextId },
}

impl From<RequestPublishError> for HttpAppError {
    fn from(error: RequestPublishError) -> Self {
        Self::RequestPublish { error }
    }
}

impl From<RequestFrameError> for HttpAppError {
    fn from(error: RequestFrameError) -> Self {
        Self::RequestFrameProtocol {
            code: error.error_code(),
        }
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
            RequestErrorAction::ResetStream {
                code: ErrorCode::InternalError,
            }
        }
    }
}

/// Executes a recovery decision through the generic transport actions, after
/// the FIFO borrows of `builtin_rx_on` end (VPP's stream-vs-connection error
/// split, http3.c:1107-1113: "message error is only stream error, otherwise
/// connection error"): a connection error closes the lower connection, a
/// stream error resets the request stream, and capacity backpressure is a
/// no-op the caller already handled.
fn execute_request_error_action(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    action: RequestErrorAction,
) -> RuntimeResult<()> {
    match action {
        RequestErrorAction::CloseConnection { code } => worker
            .close_connection(
                session,
                SessionApplicationErrorCode::from(code.value()),
                &[],
            )
            .map_err(RuntimeError::from),
        RequestErrorAction::ResetStream { code } => worker
            .reset_stream(session, SessionApplicationErrorCode::from(code.value()))
            .map_err(RuntimeError::from),
        RequestErrorAction::Retry => Ok(()),
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
        http.allocate_with_role(session, role)
            .map_err(RuntimeError::from)
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
                    RequestReadError::Protocol(error) => {
                        RuntimeError::from(HttpAppError::RequestFinishProtocol {
                            code: error.error_code(),
                        })
                    }
                })
        })?;
        if let Some(upper) = worker.upper_session(session) {
            worker.remove_upper_session(upper)?;
        }
        main.with_worker(worker.worker(), |http| {
            http.release_request_stream(stream, session)
                .map_err(RuntimeError::from)
        })?;
        return Ok(());
    }
    main.with_worker(worker.worker(), |http| {
        let peer_role = http
            .get_stream_for_session(stream, session)
            .map_err(RuntimeError::from)?
            .peer_role;
        match peer_role {
            PeerUniStreamRole::Control => {
                http.finish_peer_control_stream(stream)
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
                    })
            }
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
            PeerUniStreamRole::Push | PeerUniStreamRole::Unclassified => {
                http.remove_stream(stream).map_err(RuntimeError::from)
            }
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
            http.release_request_stream(stream, session)
                .map_err(RuntimeError::from)
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

/// Session App `cleanup` for a Session whose SessionWorker entry is being
/// removed (`remove_session`/`remove_upper_session`, runtime.rs): the
/// Session App's HTTP context is freed before the entry, mirroring VPP's
/// delete-notify-before-free order (`session_cleanup_notify`,
/// session.c:304-318). The owning `HttpWorker` is resolved by the Session
/// worker id exactly as `disconnect` does. The dispatch passes the Session's
/// published `SessionAppContext`: a `ConnectionContextId` for a root
/// connection, a `StreamContextId` for a stream child. The root context is
/// removed before the published app session is cleared (delete-before-free),
/// and a bidi request stream's upper request Session is removed before the
/// request stream context is released, matching VPP
/// `http3_conn_cleanup_callback` (http3.c:2425-2436) and
/// `http3_stream_cleanup_callback` (http3.c:2440-2462); the Session entry
/// itself is freed by the SessionWorker after the callback. An upper request
/// Session (identified by its owner link, since it shares the packed stream
/// context of its lower), a uni stream (VPP owns no request state for it,
/// http3.c:2446-2448), and a Session without live role metadata are no-ops.
/// The whole path is O(1) with no scan, allocation, lock, channel, or async
/// work.
pub(crate) fn cleanup(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    let main = HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?;
    cleanup_on(&main, worker, session, context)
}

/// `cleanup` on a caller-resolved authority, so unit tests own their
/// `HttpMain` without the process-wide `HTTP_MAIN` OnceLock, exactly as
/// `accept_on` bypasses it (listener.rs:1071).
pub(crate) fn cleanup_on(
    main: &HttpMain,
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context == 0 {
        // A zero context names no published HTTP context; the callback is a
        // lifecycle fallback no-op, consistent with the accept, disconnect,
        // and reset fallbacks.
        return Ok(());
    }
    // An upper request Session is never a lower transport Session: its
    // removal is owned by `remove_upper_session`, which runs this callback
    // reentrantly on the upper with the same packed stream context, and the
    // stream context is bound to the lower Session, so this path is a no-op.
    if worker.lower_session(session).is_some() {
        return Ok(());
    }
    // The accept metadata distinguishes the roles exactly as `disconnect`
    // does (VPP `http_ts_accept_callback` branches on `SESSION_F_STREAM`,
    // http.c:733-740): only a stream child carries `SessionFlags::STREAM`.
    let Some(metadata) = worker.accept_metadata(session) else {
        // No live role metadata means no published HTTP context exists to
        // clean; the Session entry is freed by the SessionWorker.
        return Ok(());
    };
    if metadata.flags.contains(SessionFlags::STREAM) {
        // VPP `http3_stream_cleanup_callback` (http3.c:2440-2462): a bidi
        // request stream's cleanup removes the upper request Session first
        // (`session_transport_delete_notify`, http3.c:2459-2462) and then
        // releases the request stream context, in delete-notify-then-free
        // order. A uni stream owns no request state
        // (`http_req_index == SESSION_INVALID_INDEX`, http3.c:2446-2448),
        // so it returns early here; the peer uni stream seams own uni stream
        // context release.
        let stream = StreamContextId::from(context);
        let direction = main.with_worker(worker.worker(), |http| {
            let stream_context = http
                .get_stream_for_session(stream, session)
                .map_err(RuntimeError::from)?;
            Ok(stream_context.direction)
        })?;
        if direction != SessionStreamDirection::Uni {
            if let Some(upper) = worker.upper_session(session) {
                worker.remove_upper_session(upper)?;
            }
            main.with_worker(worker.worker(), |http| {
                http.release_request_stream(stream, session)
                    .map_err(RuntimeError::from)
            })?;
        }
        return Ok(());
    }
    // Root connection cleanup, mirroring VPP `http3_conn_cleanup_callback`
    // (http3.c:2425-2436): the HTTP root context is removed before the
    // published app session is cleared (delete-before-free); clearing makes
    // a later dispatch a no-op. The Session entry itself is freed by the
    // SessionWorker after the callback.
    main.with_worker(worker.worker(), |http| {
        http.remove(ContextId::from(context))
            .map_err(RuntimeError::from)
    })?;
    worker.set_app_session(session, 0)
}

/// Outcome of the worker-local feed phase of `builtin_rx_on`: whether a
/// completed HEADERS section is ready for publication, the lower bytes the
/// reader consumed into its state (or that an erroring frame accounted
/// for), the bytes already published to the upper RX FIFO, and the
/// request-stream error to dispatch after the FIFO borrows end.
struct FeedOutcome {
    ready: bool,
    consumed: usize,
    produced: usize,
    action: Option<RequestErrorAction>,
}

/// Session App `builtin_rx` for readable bytes on a lower Session.
///
/// The lower QUIC transport dispatches newly readable RX bytes synchronously
/// as this callback with the Session's published `SessionAppContext` naming
/// the generation-checked `StreamContextId`, exactly like `disconnect` and
/// `reset` resolve their contexts.
pub(crate) fn builtin_rx(
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    let main = HTTP_MAIN
        .get()
        .ok_or(RuntimeError::PluginStateNotInitialized { plugin: NAME })?;
    builtin_rx_on(&main, worker, session, context)
}

/// `builtin_rx` on a caller-resolved authority, so unit tests own their
/// `HttpMain` without the process-wide `HTTP_MAIN` OnceLock, exactly as
/// `accept_on` bypasses it (listener.rs:1071).
///
/// Publishes a completed request HEADERS section and then the DATA payload
/// from the lower transport RX FIFO to the upper Session RX FIFO, mirroring
/// VPP `http3_stream_transport_rx_req` feeding the transport bytes
/// (http3.c:1733-1799) through `qpack_parse_request` and
/// `http3_req_state_wait_transport_method` recording the received request
/// on the per-request `http_ctx_t` (http3.c:835-899), the app-dispatch
/// stage reading it off (http3.c:456-475), and
/// `http3_req_state_transport_io_more_data` forwarding each DATA frame's
/// payload to the app FIFO (http3.c:1184-1263). A valid completed HEADERS
/// section is retained in the worker's pending slot with its exact
/// lower-FIFO consumed count ([`HttpWorker::retain_pending_field_section`])
/// and published all-or-nothing into the upper RX FIFO; the declared
/// Content-Length/body state is installed and the pending slot cleared only
/// after that commit, and exactly the consumed lower bytes are dequeued
/// with the lower dequeue notification, then the upper RX enqueue
/// notification. A fragmented HEADERS frame feeds only the bytes each
/// callback dequeues into the reader's state, so the next callback starts
/// at the first byte the reader has not seen, and an empty HEADERS field
/// section resets the request stream with HTTP3_ERROR_MESSAGE_ERROR (VPP
/// `http3_stream_terminate`, http3.c:860-869) rather than taking the QPACK
/// connection-decompression close. A FIFO-capacity backpressure on a HEADERS
/// publication returns `Ok(())` with the retained pending section and the
/// unconsumed lower bytes intact, so the next RX callback retries the same
/// section; a FIFO-capacity backpressure on a DATA publication is never
/// retryable — the reader has already accounted for the frame's bytes, so a
/// retry would re-feed reader-consumed bytes — and resets the request
/// stream with HTTP3_ERROR_INTERNAL_ERROR after dequeuing exactly the
/// reader-accounted bytes (VPP never faces this: it checks the app FIFO
/// before draining any transport bytes, http3.c:1211-1218). Every other
/// failure propagates typed with no partial mutation. The whole path is
/// synchronous and worker-local with no lock, channel, allocation beyond
/// the single bounded HEADERS capture, or async work.
pub(crate) fn builtin_rx_on(
    main: &HttpMain,
    worker: &mut SessionWorker<Index>,
    session: SessionId,
    context: SessionAppContext,
) -> RuntimeResult<()> {
    if context == 0 {
        // A zero context names no published stream; the callback is a
        // lifecycle fallback no-op, consistent with the accept, disconnect,
        // and reset fallbacks.
        return Ok(());
    }
    // The RX dispatch names either the lower transport Session whose FIFO
    // gained readable bytes or the upper Session attached to it; the request
    // stream is always the lower Session.
    let lower = worker.lower_session(session).unwrap_or(session);
    // Only a bidi request stream owns request publication; a root
    // connection, a peer uni stream, or a Session without live metadata is a
    // no-op, exactly as the accept metadata distinguishes the roles (VPP
    // `http_ts_accept_callback` branches on `SESSION_F_STREAM`, http.c:733).
    let Some(metadata) = worker.accept_metadata(lower) else {
        return Ok(());
    };
    if !metadata.flags.contains(SessionFlags::STREAM)
        || metadata.flags.contains(SessionFlags::UNIDIRECTIONAL)
    {
        return Ok(());
    }
    let stream = StreamContextId::from(context);
    // The upper request Session a previous HEADERS publication attached, or
    // none before the first completed section: DATA publication needs its
    // FIFO before the feed borrows end, but the upper is created exactly
    // once at the first publication, never here.
    let upper = worker.upper_session(lower);
    // The feed phase is worker-local: it borrows the readable lower bytes
    // and reports whether a completed HEADERS section is ready for
    // publication, the lower bytes the reader consumed, the bytes published
    // to the upper FIFO, and the stream error to reset the request stream
    // with — dispatched below after the FIFO borrows end.
    let outcome = {
        let (lower_rx, _) = worker
            .fifo_pair(lower)
            .ok_or(SessionError::SessionMissing { session_id: lower })?;
        let upper_rx = match upper {
            Some(upper) => Some(
                worker
                    .fifo_pair(upper)
                    .ok_or(SessionError::SessionMissing { session_id: upper })?
                    .0,
            ),
            None => None,
        };
        let outcome = main.with_worker(worker.worker(), |http| {
            // The encoded block and its exact lower-FIFO consumed count: on
            // a publication retry the retained pending section is ready to
            // publish again (borrowed below, never re-fed); otherwise a
            // fresh feed of the borrowed lower RX bytes.
            let pending = http
                .pending_field_section(stream, lower)
                .map_err(RuntimeError::from)?;
            if pending.is_some() {
                return Ok(FeedOutcome {
                    ready: true,
                    consumed: 0,
                    produced: 0,
                    action: None,
                });
            }
            // Borrow the readable lower bytes without dequeuing and feed
            // them to the request reader. Each byte is fed exactly once
            // across fragmented callbacks: a partial frame leaves its bytes
            // in the reader's state and they are dequeued below, so the
            // next RX callback starts at the first byte the reader has not
            // seen (VPP drains each callback's processed bytes,
            // `max_deq - left_deq`, http3.c:1798, and never re-reads a
            // frame's bytes).
            Ok(lower_rx
                .peek_segments(0, lower_rx.max_dequeue(), |first, second| {
                    let mut total = 0usize;
                    let mut produced = 0usize;
                    let mut completed: Option<(Vec<u8>, usize)> = None;
                    for segment in [first, second] {
                        if completed.is_some() || segment.is_empty() {
                            continue;
                        }
                        let (read, used) = match http.process_request_bytes(stream, lower, segment)
                        {
                            Ok(feed) => feed,
                            Err(RequestReadError::Worker(inner)) => {
                                return Err(RuntimeError::from(inner));
                            }
                            Err(RequestReadError::Protocol(error)) => {
                                // A request-stream frame error kills the
                                // stream before any payload is consumed:
                                // only the bytes the reader consumed into
                                // earlier partial-frame state dequeue, and
                                // the request stream resets with the typed
                                // error code (VPP abandons the transport
                                // bytes on an RX error,
                                // http3.c:1791-1796; RFC 9114
                                // Section 4.1).
                                return Ok(FeedOutcome {
                                    ready: false,
                                    consumed: total,
                                    produced,
                                    action: Some(RequestErrorAction::ResetStream {
                                        code: error.error_code(),
                                    }),
                                });
                            }
                        };
                        total += used;
                        match read {
                            RequestFrameRead::Headers(encoded) => {
                                completed = Some((encoded, total));
                            }
                            RequestFrameRead::Data { chunk, .. } => {
                                // A DATA frame publishes its borrowed payload
                                // through the shared body publication
                                // (`HttpWorker::process_request_data`, VPP
                                // `http3_req_state_transport_io_more_data`
                                // writing each frame's payload to the app
                                // FIFO, http3.c:1184-1263): the body
                                // accumulator rejects body-less or
                                // overrunning DATA with the typed
                                // request-stream error before any FIFO
                                // mutation, the upper commit is
                                // all-or-nothing, and the lower dequeue
                                // happens only after it.
                                let Some(upper_rx) = upper_rx else {
                                    // Only reachable after a prior stream
                                    // error (no upper was ever created): the
                                    // stream is already dead, so the
                                    // reader-consumed bytes dequeue and
                                    // nothing publishes.
                                    continue;
                                };
                                match http.process_request_data(stream, lower, upper_rx, chunk) {
                                    Ok(()) => produced += chunk.len(),
                                    Err(RequestReadError::Worker(
                                        HttpWorkerError::BodyChunkPublishFailed {
                                            error: PublishError::Capacity { .. },
                                            ..
                                        },
                                    )) => {
                                        // FIFO-capacity backpressure on a DATA
                                        // publication is never retryable: the
                                        // reader has already advanced over the
                                        // rejected bytes (`payload_len` moved
                                        // past them, request_frame_reader.rs),
                                        // so a retry would re-feed
                                        // reader-consumed bytes into the
                                        // in-progress frame and re-publish
                                        // duplicated body bytes (VPP never
                                        // faces this: it checks the app FIFO
                                        // before draining any transport bytes,
                                        // http3.c:1211-1218). Terminate the
                                        // request stream with
                                        // HTTP3_ERROR_INTERNAL_ERROR instead:
                                        // dequeue exactly the
                                        // reader-accounted bytes, without
                                        // mutating the body accumulator or
                                        // the upper FIFO
                                        // (process_request_data keeps the
                                        // slot's accumulator at its pre-call
                                        // state on rejection, worker.rs).
                                        return Ok(FeedOutcome {
                                            ready: false,
                                            consumed: total,
                                            produced,
                                            action: Some(RequestErrorAction::ResetStream {
                                                code: ErrorCode::InternalError,
                                            }),
                                        });
                                    }
                                    Err(RequestReadError::Worker(inner)) => {
                                        return Err(RuntimeError::from(inner));
                                    }
                                    Err(RequestReadError::Protocol(error)) => {
                                        // Body-less or overrunning DATA is a
                                        // typed stream protocol error, not a
                                        // publish failure: the
                                        // reader-consumed bytes dequeue with
                                        // the stream reset.
                                        return Ok(FeedOutcome {
                                            ready: false,
                                            consumed: total,
                                            produced,
                                            action: Some(RequestErrorAction::ResetStream {
                                                code: error.error_code(),
                                            }),
                                        });
                                    }
                                }
                            }
                            RequestFrameRead::Incomplete | RequestFrameRead::Drained(..) => {}
                        }
                    }
                    let Some((encoded, total)) = completed else {
                        // The reader consumed every fed byte into its
                        // partial-frame state (or the payload of a DATA
                        // frame); dequeue exactly those bytes now so the
                        // next RX callback feeds only bytes the reader has
                        // not seen.
                        return Ok(FeedOutcome {
                            ready: false,
                            consumed: total,
                            produced,
                            action: None,
                        });
                    };
                    // An empty HEADERS field section is a request-stream
                    // error, not a QPACK connection decompression failure:
                    // VPP checks `req->fh.length == 0` before
                    // `qpack_parse_request` and terminates the request
                    // stream with HTTP3_ERROR_MESSAGE_ERROR (http3.c:860-869,
                    // RFC 9114 Section 4.1.2). The reset is dispatched
                    // below.
                    if encoded.is_empty() {
                        return Ok(FeedOutcome {
                            ready: false,
                            consumed: total,
                            produced,
                            action: Some(RequestErrorAction::ResetStream {
                                code: ErrorCode::MessageError,
                            }),
                        });
                    }
                    http.retain_pending_field_section(
                        stream,
                        lower,
                        PendingFieldSection {
                            encoded,
                            consumed: total,
                        },
                    )
                    .map_err(RuntimeError::from)?;
                    Ok(FeedOutcome {
                        ready: true,
                        consumed: 0,
                        produced,
                        action: None,
                    })
                })
                .transpose()?
                .unwrap_or(FeedOutcome {
                    ready: false,
                    consumed: 0,
                    produced: 0,
                    action: None,
                }))
        })?;
        // Dequeue now the bytes the reader consumed into partial-frame state
        // (or an erroring frame's fed bytes, or a committed DATA chunk's
        // frame); the bytes of a completed section dequeue only after the
        // upper commit below.
        lower_rx.dequeue_drop(outcome.consumed);
        outcome
    };
    // Request-stream errors (an empty HEADERS section, body-less or
    // overrunning DATA, an invalid request-stream frame, or DATA capacity
    // backpressure) reset the request stream with the typed error code (VPP
    // `http3_stream_terminate`, http3.c:140-165): a stream error that never
    // reaches the upper Session. The action executes only after the FIFO
    // borrows end.
    if let Some(action) = outcome.action {
        execute_request_error_action(worker, lower, action)?;
    }
    if !outcome.ready {
        // No completed HEADERS section to publish — a partial frame, no
        // readable bytes, a request-stream error, or a published DATA chunk
        // — so no upper request Session is created here (VPP records the
        // received request and dispatches it to the app only once the full
        // HEADERS block arrives, http3.c:835-899 then http3.c:456-475).
        // Notify the lower dequeue first, then the upper enqueue for any
        // published chunk, and return.
        worker.publish_rx_dequeue(lower, outcome.consumed)?;
        if outcome.produced > 0 {
            let upper = upper.ok_or(SessionError::SessionMissing { session_id: lower })?;
            worker.publish_rx_enqueue(upper, outcome.produced)?;
        }
        return Ok(());
    }
    // Resolve the upper request Session exactly once, and only now that a
    // valid completed HEADERS section is retained: the attached upper on a
    // publication retry, or a fresh upper Session publishing the same
    // generation-checked stream context. Creation precedes the upper FIFO
    // publication, mirroring VPP dispatching the recorded request to the
    // app (http3.c:456-475) after the transport method stage completed.
    let upper = match worker.upper_session(lower) {
        Some(upper) => upper,
        None => worker.create_upper_session(lower, context)?,
    };
    let (lower_rx, _) = worker
        .fifo_pair(lower)
        .ok_or(SessionError::SessionMissing { session_id: lower })?;
    let (upper_rx, _) = worker
        .fifo_pair(upper)
        .ok_or(SessionError::SessionMissing { session_id: upper })?;
    // Publish the retained section all-or-nothing into the upper RX FIFO
    // (QPACK decode, request validation, one `InboundRequest` write).
    let published = main.with_worker(worker.worker(), |http| {
        let produced_before = upper_rx.max_dequeue();
        // The slot owns the retained section; borrow it back for
        // publication. A successful retain always leaves it pending (the
        // slot rejects rather than replaces, worker.rs), so a missing slot
        // here is a worker-internal invariant breach reported typed rather
        // than panicked.
        let section = http
            .pending_field_section(stream, lower)
            .map_err(RuntimeError::from)?
            .ok_or(HttpAppError::PendingFieldSectionLost { stream })
            .map_err(RuntimeError::from)?;
        let section_consumed = section.consumed;
        match publish_request_field_section(&upper_rx, &section.encoded[..]) {
            Ok(declared) => {
                // Install the declared Content-Length/body state and clear
                // the pending slot only after the upper FIFO commit: a
                // section is either retained-pending or published, never
                // both.
                http.install_request_body_length(stream, lower, declared)
                    .map_err(RuntimeError::from)?;
                http.clear_pending_field_section(stream, lower)
                    .map_err(RuntimeError::from)?;
                let produced = upper_rx.max_dequeue() - produced_before;
                Ok(Some((produced, section_consumed, None)))
            }
            Err(error) => {
                // FIFO capacity is transient backpressure: the retained
                // pending section and the unconsumed lower bytes stay intact
                // and the next RX callback retries the same section; every
                // other failure executes its stream- or connection-level
                // action synchronously after the borrows end (VPP
                // stream-vs-connection error split, http3.c:1107-1113:
                // "message error is only stream error, otherwise connection
                // error"). The section stays retained: the stream is dead,
                // so nothing publishes it.
                let action = request_publish_error_action(error);
                if matches!(action, RequestErrorAction::Retry) {
                    return Ok(None);
                }
                Ok(Some((0, 0, Some(action))))
            }
        }
    })?;
    let Some((produced, section_consumed, action)) = published else {
        // FIFO capacity backpressure: the retained pending section and the
        // unconsumed lower bytes both stay intact — the exact dequeue
        // happens only after a successful upper commit — so the next RX
        // callback retries the same section against the attached upper.
        worker.publish_rx_dequeue(lower, outcome.consumed)?;
        return Ok(());
    };
    // A publication failure (QPACK decompression, message validation,
    // internal encoding) executes its stream- or connection-level action
    // here, after the FIFO borrows end; nothing is published or dequeued.
    if let Some(action) = action {
        execute_request_error_action(worker, lower, action)?;
        return Ok(());
    }
    // Dequeue exactly the consumed lower bytes after the upper commit, then
    // notify the lower dequeue; the remaining bytes stay readable for the
    // next RX callback.
    lower_rx.dequeue_drop(section_consumed);
    worker.publish_rx_dequeue(lower, section_consumed)?;
    // Notify the upper RX enqueue only after the commit.
    worker.publish_rx_enqueue(upper, produced)?;
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
/// owned by the `cleanup` callback, which removes the HTTP context before
/// the SessionWorker frees the entry; the registration-level hook fires only
/// for the lower-most Session after that removal, carries neither the
/// Session id nor an error channel, and stays a no-op.
pub(crate) fn destroy(_worker: DataWorkerId, _context: SessionAppContext) {}

pub(crate) static HTTP_SESSION_APP: SessionAppRegistration =
    SessionAppRegistration::new(NAME, install, destroy);
